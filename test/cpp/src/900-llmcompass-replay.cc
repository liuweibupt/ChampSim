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

std::string make_temp_dir(const std::string& name)
{
  std::array<char, 64> path_template{};
  const auto templ = "/tmp/" + name + ".XXXXXX";
  std::copy(templ.begin(), templ.end(), path_template.begin());

  const auto* path = mkdtemp(path_template.data());
  REQUIRE(path != nullptr);
  return {path};
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

DriverResult run_bridge_driver(const nlohmann::json& trace, const nlohmann::json& cache, uint64_t clock_frequency_hz)
{
  const auto trace_path = make_temp_file("llmcompass-bridge-trace");
  const auto stdout_path = make_temp_file("llmcompass-bridge-stdout");
  const auto stderr_path = make_temp_file("llmcompass-bridge-stderr");

  write_json(trace_path, trace);

  auto command = std::string{"./bin/llmcompass_replay --trace '"} + trace_path + "'"
                 + " --cache-name bridge-llc"
                 + " --l3-size-byte 256"
                 + " --l3-associativity 2"
                 + " --l3-line-size-byte 64"
                 + " --output-format json"
                 + " --bridge-cache-json '" + cache.dump() + "'"
                 + " --bridge-clock-frequency-hz " + std::to_string(clock_frequency_hz)
                 + " > '" + stdout_path + "' 2> '" + stderr_path + "'";

  const auto exit_code = std::system(command.c_str());
  const auto stdout_text = read_file(stdout_path);
  const auto stderr_text = read_file(stderr_path);

  std::remove(trace_path.c_str());
  std::remove(stdout_path.c_str());
  std::remove(stderr_path.c_str());

  return DriverResult{exit_code, stdout_text, stderr_text, {}};
}

DriverResult run_policy_driver(const nlohmann::json& trace, const nlohmann::json& config, const std::string& policy)
{
  const auto temp_dir = make_temp_dir("llmcompass-policy");
  const auto trace_path = temp_dir + "/bridge_trace.json";
  const auto config_path = temp_dir + "/sram_config.json";
  const auto output_path = temp_dir + "/champsim_stats.json";
  const auto stdout_path = make_temp_file("llmcompass-policy-stdout");
  const auto stderr_path = make_temp_file("llmcompass-policy-stderr");

  write_json(trace_path, trace);
  write_json(config_path, config);
  write_json(temp_dir + "/bridge_trace_config.json",
             nlohmann::json{{"rows", 2}, {"demand_rows", 2}, {"prefetch_rows", 0}, {"trace_payload_sha256", "abc"}});
  write_json(temp_dir + "/trace_config.json", nlohmann::json{{"tokens", 2}, {"warmup_tokens", 0}, {"trace_payload_sha256", "abc"}});

  auto command = std::string{"./bin/llmcompass_replay --config '"} + config_path + "'"
                 + " --trace '" + trace_path + "'"
                 + " --cache-name 3d_weight_sram"
                 + " --l3-size-byte 256"
                 + " --l3-associativity 2"
                 + " --l3-line-size-byte 64"
                 + " --sram_mib 1"
                 + " --policy " + policy
                 + " --replay-scheduler event_driven"
                 + " --output '" + output_path + "'"
                 + " > '" + stdout_path + "' 2> '" + stderr_path + "'";

  const auto exit_code = std::system(command.c_str());
  const auto stdout_text = read_file(stdout_path);
  const auto stderr_text = read_file(stderr_path);
  const auto output_text = exit_code == 0 ? read_file(output_path) : std::string{};

  std::remove(trace_path.c_str());
  std::remove(config_path.c_str());
  std::remove((temp_dir + "/bridge_trace_config.json").c_str());
  std::remove((temp_dir + "/trace_config.json").c_str());
  std::remove(output_path.c_str());
  std::remove(stdout_path.c_str());
  std::remove(stderr_path.c_str());
  rmdir(temp_dir.c_str());

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

TEST_CASE("The replay driver accepts replacement_policy and differentiates policy-sensitive traces")
{
  const auto accesses = nlohmann::json::array(
      {nlohmann::json{{"address", "0x0"}}, nlohmann::json{{"address", "0x0"}}, nlohmann::json{{"address", "0x40"}},
       nlohmann::json{{"address", "0x80"}}, nlohmann::json{{"address", "0x0"}}});
  const auto base_cache =
      nlohmann::json{{"name", "policy-llc"}, {"sets", 1}, {"ways", 2}, {"hit_latency", 2}, {"fill_latency", 1}, {"memory_latency", 10}};

  auto lru_replay = nlohmann::json{{"cache", base_cache}, {"accesses", accesses}};
  auto srrip_replay = nlohmann::json{{"cache", base_cache}, {"accesses", accesses}};
  srrip_replay["cache"]["replacement_policy"] = "srrip";

  const auto lru_result = run_driver(lru_replay, false);
  const auto srrip_result = run_driver(srrip_replay, false);

  REQUIRE(lru_result.exit_code == 0);
  REQUIRE(srrip_result.exit_code == 0);

  const auto lru_output = nlohmann::json::parse(lru_result.stdout_text);
  const auto srrip_output = nlohmann::json::parse(srrip_result.stdout_text);
  REQUIRE(lru_output["summary"]["hits"] == 1);
  REQUIRE(srrip_output["summary"]["hits"] == 2);
  REQUIRE(srrip_output["summary"]["total_latency_cycles"] < lru_output["summary"]["total_latency_cycles"]);
}

TEST_CASE("The replay driver accepts native LLMCompass bridge flags and expands sized trace entries")
{
  const auto trace = nlohmann::json::array({
      nlohmann::json{{"order", 0}, {"phase", "decode"}, {"op_id", "decode:0:qkv"}, {"tensor_role", "weights"},
                     {"stream_id", "wq"}, {"address", 0}, {"size", 128}, {"access_type", "read"}},
      nlohmann::json{{"order", 1}, {"phase", "decode"}, {"op_id", "decode:1:qkv"}, {"tensor_role", "weights"},
                     {"stream_id", "wq"}, {"address", 0}, {"size", 64}, {"access_type", "read"}},
  });
  const auto cache = nlohmann::json{{"hit_latency_cycles", 2}, {"fill_latency_cycles", 1}, {"memory_latency_cycles", 5},
                                    {"replacement_policy", "lru"}};

  const auto result = run_bridge_driver(trace, cache, 1000000000);

  REQUIRE(result.exit_code == 0);
  REQUIRE(result.stderr_text.empty());
  const auto output = nlohmann::json::parse(result.stdout_text);
  REQUIRE(output["hit_count"] == 1);
  REQUIRE(output["miss_count"] == 2);
  REQUIRE(output["hit_rate"] == Catch::Approx(1.0 / 3.0));
  REQUIRE(output["total_memory_time_sec"] == Catch::Approx(23e-9));
  REQUIRE(output["phase_stats"]["decode"]["accesses"] == 3);
  REQUIRE(output["phase_stats"]["decode"]["hits"] == 1);
  REQUIRE(output["phase_stats"]["decode"]["misses"] == 2);
}

TEST_CASE("The bridge replay driver supports explicit bypass no-allocate policy")
{
  const auto trace = nlohmann::json::array({
      nlohmann::json{{"phase", "decode"}, {"address", 0}, {"size", 128}, {"access_type", "read"}},
      nlohmann::json{{"phase", "decode"}, {"address", 0}, {"size", 64}, {"access_type", "read"}},
  });
  const auto cache = nlohmann::json{{"memory_latency_cycles", 5}, {"replacement_policy", "bypass"}};

  const auto result = run_bridge_driver(trace, cache, 1000000000);

  REQUIRE(result.exit_code == 0);
  REQUIRE(result.stderr_text.empty());
  const auto output = nlohmann::json::parse(result.stdout_text);
  REQUIRE(output["hit_count"] == 0);
  REQUIRE(output["miss_count"] == 3);
  REQUIRE(output["total_memory_time_sec"] == Catch::Approx(15e-9));
  REQUIRE(output["phase_stats"]["decode"]["hits"] == 0);
  REQUIRE(output["phase_stats"]["decode"]["misses"] == 3);
}

TEST_CASE("The policy replay driver emits native_result counters for G3 production commands")
{
  const auto trace = nlohmann::json::array({
      nlohmann::json{{"phase", "token_0/layer_0/q/demand"}, {"address", 0}, {"size", 64}, {"access_type", "read"},
                     {"token_id", 0}, {"layer_id", 0}, {"residency_policy_hint", "managed_pinned"}, {"pin_window_id", "pinned_layers_0_0"}},
      nlohmann::json{{"phase", "token_1/layer_0/q/demand"}, {"address", 0}, {"size", 64}, {"access_type", "read"},
                     {"token_id", 1}, {"layer_id", 0}, {"residency_policy_hint", "managed_pinned"}, {"pin_window_id", "pinned_layers_0_0"}},
  });
  const auto config = nlohmann::json{{"policy", "managed_pinned"},
                                     {"policy_implementation", "descriptor_managed_pin_window"},
                                     {"capacity", {{"sram_mib", 1}, {"capacity_bytes", 256}, {"line_bytes", 64}, {"associativity", 2}}},
                                     {"timing", {{"hit_latency_cycles", 2}, {"fill_latency_cycles", 1}, {"hbm_fallback_latency_cycles", 5}}}};

  const auto result = run_policy_driver(trace, config, "managed_pinned");

  REQUIRE(result.exit_code == 0);
  REQUIRE(result.stdout_text.empty());
  REQUIRE(result.stderr_text.empty());
  const auto output = nlohmann::json::parse(result.output_text);
  REQUIRE(output.contains("native_result"));
  REQUIRE(output["native_result"]["status"] == "ok");
  REQUIRE(output["native_result"]["demand_accesses"] == 2);
  REQUIRE(output["native_result"]["demand_hits"] == 1);
  REQUIRE(output["native_result"]["residency_saved_hits"] == 1);
  REQUIRE(output["native_result"]["external_read_bytes"] == 64);
  REQUIRE(output["native_result"]["prefetch_read_bytes"] == 0);
  REQUIRE(output["native_result"]["token_cycles"].get<uint64_t>() > 0);
  REQUIRE(output["native_result"]["simulator_commit"].get<std::string>().size() >= 12);
}

TEST_CASE("The policy replay driver keeps prefetch hits separate from residency savings")
{
  const auto trace = nlohmann::json::array({
      nlohmann::json{{"phase", "token_0/layer_1/q/prefetch"}, {"address", 64}, {"size", 64}, {"access_type", "prefetch"},
                     {"token_id", 0}, {"layer_id", 1}, {"residency_policy_hint", "next_layer_prefetch"}, {"pin_window_id", "NA"}},
      nlohmann::json{{"phase", "token_0/layer_1/q/demand"}, {"address", 64}, {"size", 64}, {"access_type", "read"},
                     {"token_id", 0}, {"layer_id", 1}, {"residency_policy_hint", "next_layer_prefetch"}, {"pin_window_id", "NA"}},
  });
  const auto config = nlohmann::json{{"policy", "next_layer_prefetch"},
                                     {"policy_implementation", "lookahead_prefetch_no_persistence"},
                                     {"capacity", {{"sram_mib", 1}, {"capacity_bytes", 256}, {"line_bytes", 64}, {"associativity", 2}}},
                                     {"timing", {{"hit_latency_cycles", 2}, {"fill_latency_cycles", 1}, {"hbm_fallback_latency_cycles", 5}}}};

  const auto result = run_policy_driver(trace, config, "next_layer_prefetch");

  REQUIRE(result.exit_code == 0);
  const auto output = nlohmann::json::parse(result.output_text);
  REQUIRE(output["native_result"]["demand_accesses"] == 1);
  REQUIRE(output["native_result"]["demand_hits"] == 1);
  REQUIRE(output["native_result"]["residency_saved_hits"] == 0);
  REQUIRE(output["native_result"]["external_read_bytes"] == 0);
  REQUIRE(output["native_result"]["prefetch_read_bytes"] == 64);
}

TEST_CASE("The policy replay driver preserves bypass no-allocate native counters")
{
  const auto trace = nlohmann::json::array({
      nlohmann::json{{"phase", "token_0/layer_0/q/demand"}, {"address", 0}, {"size", 64}, {"access_type", "read"},
                     {"token_id", 0}, {"layer_id", 0}, {"residency_policy_hint", "bypass"}, {"pin_window_id", "NA"}},
      nlohmann::json{{"phase", "token_1/layer_0/q/demand"}, {"address", 0}, {"size", 64}, {"access_type", "read"},
                     {"token_id", 1}, {"layer_id", 0}, {"residency_policy_hint", "bypass"}, {"pin_window_id", "NA"}},
  });
  const auto config = nlohmann::json{{"policy", "bypass"},
                                     {"policy_implementation", "streaming_no_allocate_bypass"},
                                     {"capacity", {{"sram_mib", 1}, {"capacity_bytes", 256}, {"line_bytes", 64}, {"associativity", 2}}},
                                     {"timing", {{"hit_latency_cycles", 2}, {"fill_latency_cycles", 1}, {"hbm_fallback_latency_cycles", 5}}}};

  const auto result = run_policy_driver(trace, config, "bypass");

  REQUIRE(result.exit_code == 0);
  const auto output = nlohmann::json::parse(result.output_text);
  REQUIRE(output["native_result"]["demand_accesses"] == 2);
  REQUIRE(output["native_result"]["demand_hits"] == 0);
  REQUIRE(output["native_result"]["residency_saved_hits"] == 0);
  REQUIRE(output["native_result"]["external_read_bytes"] == 128);
  REQUIRE(output["native_result"]["sram_write_bytes"] == 0);
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
