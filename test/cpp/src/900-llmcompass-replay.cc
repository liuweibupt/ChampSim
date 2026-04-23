#include <catch.hpp>

#include <algorithm>
#include <array>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <string>
#include <unistd.h>
#include <nlohmann/json.hpp>

namespace
{
struct DriverResult {
  int exit_code;
  std::string stdout_text;
  std::string stderr_text;
  std::string output_text;
};

std::string make_temp_file(const std::string& name)
{
  std::array<char, 64> path_template{};
  const auto templ = "/tmp/" + name + ".XXXXXX";
  std::copy(templ.begin(), templ.end(), path_template.begin());

  const int fd = mkstemp(path_template.data());
  REQUIRE(fd != -1);
  close(fd);
  return {path_template.data()};
}

std::string read_file(const std::string& path)
{
  std::ifstream stream{path};
  REQUIRE(stream.is_open());
  return {std::istreambuf_iterator<char>{stream}, std::istreambuf_iterator<char>{}};
}

void write_json(const std::string& path, const nlohmann::json& content)
{
  std::ofstream stream{path};
  REQUIRE(stream.is_open());
  stream << content.dump(2);
}

DriverResult run_driver(const nlohmann::json& input, bool use_output_flag)
{
  const auto input_path = make_temp_file("llmcompass-replay-input");
  const auto stdout_path = make_temp_file("llmcompass-replay-stdout");
  const auto stderr_path = make_temp_file("llmcompass-replay-stderr");
  const auto output_path = make_temp_file("llmcompass-replay-output");

  write_json(input_path, input);

  auto command = std::string{"./bin/llmcompass_replay --input '"} + input_path + "'";
  if (use_output_flag)
    command += " --output '" + output_path + "'";
  command += " > '" + stdout_path + "' 2> '" + stderr_path + "'";

  const auto exit_code = std::system(command.c_str());
  const auto stdout_text = read_file(stdout_path);
  const auto stderr_text = read_file(stderr_path);
  const auto output_text = use_output_flag ? read_file(output_path) : std::string{};

  std::remove(input_path.c_str());
  std::remove(stdout_path.c_str());
  std::remove(stderr_path.c_str());
  std::remove(output_path.c_str());

  return DriverResult{exit_code, stdout_text, stderr_text, output_text};
}
} // namespace

TEST_CASE("An ordered LLC replay reports hits, misses, and total latency")
{
  const auto replay = nlohmann::json{{"cache",
                                      {{"name", "test-llc"},
                                       {"sets", 1},
                                       {"ways", 2},
                                       {"hit_latency", 2},
                                       {"fill_latency", 1},
                                       {"memory_latency", 5}}},
                                     {"accesses",
                                      nlohmann::json::array({nlohmann::json{{"address", "0x0"}}, nlohmann::json{{"address", "0x40"}},
                                                             nlohmann::json{{"address", "0x0"}}})}};

  const auto result = run_driver(replay, false);
  REQUIRE(result.exit_code == 0);
  REQUIRE(result.stderr_text.empty());

  const auto output = nlohmann::json::parse(result.stdout_text);
  REQUIRE(output["summary"]["accesses"] == 3);
  REQUIRE(output["summary"]["hits"] == 1);
  REQUIRE(output["summary"]["misses"] == 2);
  REQUIRE(output["summary"]["total_latency_cycles"] == 23);
  REQUIRE(output["accesses"][0]["result"] == "miss");
  REQUIRE(output["accesses"][1]["result"] == "miss");
  REQUIRE(output["accesses"][2]["result"] == "hit");
  REQUIRE(output["accesses"][2]["latency_cycles"] == 3);
  REQUIRE(output["addresses"]["0x0"]["misses"] == 1);
  REQUIRE(output["addresses"]["0x0"]["hits"] == 1);
  REQUIRE(output["addresses"]["0x40"]["misses"] == 1);
  REQUIRE(output["addresses"]["0x40"]["hits"] == 0);
}

TEST_CASE("The replay driver writes JSON to --output when requested")
{
  const auto replay = nlohmann::json{{"accesses", nlohmann::json::array({nlohmann::json{{"address", "0x0"}}})}};

  const auto result = run_driver(replay, true);
  REQUIRE(result.exit_code == 0);
  REQUIRE(result.stdout_text.empty());
  REQUIRE(result.stderr_text.empty());

  const auto output = nlohmann::json::parse(result.output_text);
  REQUIRE(output["summary"]["accesses"] == 1);
  REQUIRE(output["summary"]["misses"] == 1);
}

TEST_CASE("The replay driver rejects malformed integer strings")
{
  const auto replay = nlohmann::json{{"accesses", nlohmann::json::array({nlohmann::json{{"address", "123garbage"}}})}};

  const auto result = run_driver(replay, false);
  REQUIRE(result.exit_code != 0);
  REQUIRE_THAT(result.stderr_text, Catch::Matchers::ContainsSubstring("llmcompass_replay error:"));
  REQUIRE_THAT(result.stderr_text, Catch::Matchers::ContainsSubstring("Invalid unsigned integer string"));
}

TEST_CASE("The replay driver rejects negative integer strings")
{
  const auto replay = nlohmann::json{{"accesses", nlohmann::json::array({nlohmann::json{{"address", "-1"}}})}};

  const auto result = run_driver(replay, false);
  REQUIRE(result.exit_code != 0);
  REQUIRE_THAT(result.stderr_text, Catch::Matchers::ContainsSubstring("llmcompass_replay error:"));
  REQUIRE_THAT(result.stderr_text, Catch::Matchers::ContainsSubstring("must be non-negative"));
}

TEST_CASE("The replay driver rejects empty accesses")
{
  const auto replay = nlohmann::json{{"accesses", nlohmann::json::array()}};

  const auto result = run_driver(replay, false);
  REQUIRE(result.exit_code != 0);
  REQUIRE_THAT(result.stderr_text, Catch::Matchers::ContainsSubstring("non-empty 'accesses' array"));
}

TEST_CASE("The replay driver rejects accesses missing the address field")
{
  const auto replay = nlohmann::json{{"accesses", nlohmann::json::array({nlohmann::json{{"cpu", 0}}})}};

  const auto result = run_driver(replay, false);
  REQUIRE(result.exit_code != 0);
  REQUIRE_THAT(result.stderr_text, Catch::Matchers::ContainsSubstring("must contain an 'address' field"));
}

TEST_CASE("The replay driver rejects uint32 overflow in cpu fields")
{
  const auto replay = nlohmann::json{{"accesses", nlohmann::json::array({nlohmann::json{{"address", "0x0"}, {"cpu", "4294967296"}}})}};

  const auto result = run_driver(replay, false);
  REQUIRE(result.exit_code != 0);
  REQUIRE_THAT(result.stderr_text, Catch::Matchers::ContainsSubstring("out of range for uint32"));
}
