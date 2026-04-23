#include <catch.hpp>

#include <algorithm>
#include <array>
#include <cstdlib>
#include <fstream>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <nlohmann/json.hpp>

namespace
{
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
} // namespace

TEST_CASE("An ordered LLC replay reports hits, misses, and total latency")
{
  const auto input_path = make_temp_file("llmcompass-replay-input");
  const auto output_path = make_temp_file("llmcompass-replay-output");

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

  {
    std::ofstream stream{input_path};
    REQUIRE(stream.is_open());
    stream << replay.dump(2);
  }

  const auto command = "./bin/llmcompass_replay --input '" + input_path + "' > '" + output_path + "'";
  const auto exit_code = std::system(command.c_str());

  REQUIRE(exit_code == 0);

  const auto output = nlohmann::json::parse(read_file(output_path));

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

  std::remove(input_path.c_str());
  std::remove(output_path.c_str());
}
