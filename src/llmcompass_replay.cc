/*
 *    Copyright 2026 The ChampSim Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <algorithm>
#include <array>
#include <charconv>
#include <cctype>
#include <cstdint>
#include <deque>
#include <fstream>
#include <limits>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <CLI/CLI.hpp>
#include <fmt/core.h>
#include <nlohmann/json.hpp>

#include "access_type.h"
#include "cache.h"
#include "channel.h"
#include "defaults.hpp"
#include "operable.h"
#include "../replacement/srrip/srrip.h"
#include "util/to_underlying.h"

#ifdef CHAMPSIM_LLMCOMPASS_REPLAY_MAIN
const std::size_t NUM_CPUS = 1;
const unsigned BLOCK_SIZE = 64;
const unsigned PAGE_SIZE = 4096;
const unsigned LOG2_BLOCK_SIZE = champsim::lg2(BLOCK_SIZE);
const unsigned LOG2_PAGE_SIZE = champsim::lg2(PAGE_SIZE);
#endif

namespace
{
using json = nlohmann::json;

struct ReplayAccess {
  champsim::address address;
  access_type type{access_type::LOAD};
  uint32_t cpu{0};
  std::string phase{"default"};
};

struct ReplayConfig {
  enum class replacement_policy_kind { lru, srrip };

  std::optional<std::string> name;
  std::optional<uint32_t> sets;
  std::optional<uint32_t> ways;
  std::optional<uint32_t> pq_size;
  std::optional<uint32_t> mshr_size;
  std::optional<uint64_t> hit_latency;
  std::optional<uint64_t> fill_latency;
  uint64_t memory_latency{1};
  replacement_policy_kind replacement_policy{replacement_policy_kind::lru};
};

struct ReplayObservation {
  std::string address;
  std::string result;
  std::string access_type;
  uint32_t cpu{0};
  uint64_t latency_cycles{0};
  std::string phase{"default"};
};

struct AddressSummary {
  uint64_t hits{0};
  uint64_t misses{0};
};

struct ReplayReport {
  std::vector<ReplayObservation> accesses;
  uint64_t hits{0};
  uint64_t misses{0};
  uint64_t total_latency_cycles{0};
  std::map<std::string, AddressSummary> addresses{};
  std::map<std::string, AddressSummary> phases{};
};

class ReplayProducer final : public champsim::operable
{
public:
  using request_type = champsim::channel::request_type;

  champsim::channel queues{};

  void issue(const ReplayAccess& access)
  {
    if (pending_.has_value()) {
      throw std::logic_error{"Cannot issue a new replay access while another access is in flight"};
    }

    request_type request{};
    request.address = access.address;
    request.v_address = access.address;
    request.type = access.type;
    request.cpu = access.cpu;
    request.instr_id = next_instr_id_++;

    pending_ = pending_access{request, cycle_count_, 0};
    if (!issue_request(request)) {
      throw std::runtime_error{"Replay producer could not enqueue the access"};
    }
  }

  [[nodiscard]] bool has_pending() const { return pending_.has_value(); }

  [[nodiscard]] std::optional<uint64_t> completed_latency() const
  {
    if (!pending_.has_value() || pending_->return_time == 0) {
      return std::nullopt;
    }

    return pending_->return_time - pending_->issue_time;
  }

  void clear_completed()
  {
    if (!completed_latency().has_value()) {
      throw std::logic_error{"No completed replay access is available"};
    }

    pending_.reset();
  }

  long operate() override
  {
    ++cycle_count_;

    if (!queues.returned.empty()) {
      if (!pending_.has_value()) {
        throw std::runtime_error{"Replay producer received a response without an in-flight access"};
      }

      pending_->return_time = cycle_count_;
      queues.returned.clear();
    }

    return 1;
  }

private:
  struct pending_access {
    request_type request;
    uint64_t issue_time;
    uint64_t return_time;
  };

  uint64_t cycle_count_{0};
  uint64_t next_instr_id_{1};
  std::optional<pending_access> pending_{};

  bool issue_request(const request_type& request)
  {
    switch (request.type) {
    case access_type::LOAD:
    case access_type::RFO:
    case access_type::TRANSLATION:
      return queues.add_rq(request);
    case access_type::WRITE:
      return queues.add_wq(request);
    case access_type::PREFETCH:
      return queues.add_pq(request);
    case access_type::NUM_TYPES:
      throw std::invalid_argument{"Replay access type NUM_TYPES is invalid"};
    }

    throw std::invalid_argument{"Replay access type is invalid"};
  }
};

class FixedLatencyMemory final : public champsim::operable
{
public:
  explicit FixedLatencyMemory(uint64_t latency) : latency_(latency) {}

  champsim::channel queues{};

  long operate() override
  {
    ++cycle_count_;
    drain_requests(queues.RQ);
    drain_requests(queues.WQ);
    drain_requests(queues.PQ);
    release_ready_packets();
    return 1;
  }

private:
  struct packet {
    champsim::channel::request_type request;
    uint64_t ready_cycle{0};
  };

  uint64_t latency_{0};
  uint64_t cycle_count_{0};
  champsim::address next_data_{0x11111111};
  std::deque<packet> inflight_{};

  void drain_requests(std::deque<champsim::channel::request_type>& queue)
  {
    for (auto request : queue) {
      request.data = next_data_;
      ++next_data_;
      inflight_.push_back(packet{request, cycle_count_ + latency_});
    }
    queue.clear();
  }

  void release_ready_packets()
  {
    while (!inflight_.empty() && inflight_.front().ready_cycle <= cycle_count_) {
      auto ready_packet = inflight_.front();
      inflight_.pop_front();

      if (ready_packet.request.response_requested) {
        queues.returned.push_back(champsim::channel::response_type{ready_packet.request});
      }
    }
  }
};

[[nodiscard]] auto upper_case(std::string value) -> std::string
{
  std::transform(value.begin(), value.end(), value.begin(), [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
  return value;
}

[[nodiscard]] auto parse_u64_string(std::string_view text) -> uint64_t
{
  if (text.empty()) {
    throw std::invalid_argument{"Invalid unsigned integer string: ''"};
  }

  if (text.front() == '-') {
    throw std::invalid_argument{fmt::format("Unsigned integer string '{}' must be non-negative", text)};
  }

  auto digits = text;
  auto base = 10;
  if (digits.size() >= 2 && digits[0] == "0"[0] && (digits[1] == "x"[0] || digits[1] == "X"[0])) {
    digits.remove_prefix(2);
    base = 16;
  }

  if (digits.empty()) {
    throw std::invalid_argument{fmt::format("Invalid unsigned integer string: '{}'", text)};
  }

  uint64_t parsed{};
  const auto* begin = digits.data();
  const auto* end = digits.data() + digits.size();
  const auto [ptr, ec] = std::from_chars(begin, end, parsed, base);
  if (ec == std::errc::result_out_of_range) {
    throw std::invalid_argument{fmt::format("Unsigned integer string '{}' is out of range for uint64", text)};
  }
  if (ec != std::errc{} || ptr != end) {
    throw std::invalid_argument{fmt::format("Invalid unsigned integer string: '{}'", text)};
  }

  return parsed;
}

[[nodiscard]] auto parse_u64(const json& value) -> uint64_t
{
  if (value.is_number_unsigned()) {
    return value.get<uint64_t>();
  }

  if (value.is_number_integer()) {
    const auto parsed = value.get<int64_t>();
    if (parsed < 0) {
      throw std::invalid_argument{"Replay values must be non-negative"};
    }
    return static_cast<uint64_t>(parsed);
  }

  if (value.is_string()) {
    return parse_u64_string(value.get<std::string>());
  }

  throw std::invalid_argument{"Replay values must be integers or integer-like strings"};
}

[[nodiscard]] auto parse_u32(const json& value, std::string_view field_name) -> uint32_t
{
  constexpr auto max_u32 = std::numeric_limits<uint32_t>::max();
  const auto parsed = parse_u64(value);
  if (parsed > max_u32) {
    throw std::invalid_argument{fmt::format("Field '{}' is out of range for uint32: {}", field_name, parsed)};
  }

  return static_cast<uint32_t>(parsed);
}

[[nodiscard]] auto parse_access_type(const json& value) -> access_type
{
  if (value.is_null()) {
    return access_type::LOAD;
  }

  const auto parsed = upper_case(value.get<std::string>());
  for (std::size_t i = 0; i < access_type_names.size(); ++i) {
    if (parsed == access_type_names.at(i)) {
      return static_cast<access_type>(i);
    }
  }

  throw std::invalid_argument{fmt::format("Unsupported replay access type '{}'", parsed)};
}

[[nodiscard]] auto parse_bridge_access_type(const json& value) -> access_type
{
  if (!value.is_string()) {
    throw std::invalid_argument{"Bridge replay access_type must be a string"};
  }

  const auto parsed = upper_case(value.get<std::string>());
  if (parsed == "READ")
    return access_type::LOAD;
  if (parsed == "WRITE")
    return access_type::WRITE;
  if (parsed == "PREFETCH")
    return access_type::PREFETCH;
  if (parsed == "RFO")
    return access_type::RFO;
  if (parsed == "TRANSLATION")
    return access_type::TRANSLATION;

  return parse_access_type(value);
}

[[nodiscard]] auto format_address(champsim::address address) -> std::string { return fmt::format("{:#x}", address.to<uint64_t>()); }

[[nodiscard]] auto read_json_file(const std::string& path) -> json
{
  std::ifstream stream{path};
  if (!stream.is_open()) {
    throw std::runtime_error{fmt::format("Unable to open replay file '{}'", path)};
  }

  return json::parse(stream);
}

template <typename Builder>
void apply_cache_overrides(Builder& builder, ReplayConfig config, champsim::channel* upper_level, champsim::channel* lower_level)
{
  builder.upper_levels({upper_level}).lower_level(lower_level);

  if (config.name.has_value())
    builder.name(*config.name);
  if (config.sets.has_value())
    builder.sets(*config.sets);
  if (config.ways.has_value())
    builder.ways(*config.ways);
  if (config.pq_size.has_value())
    builder.pq_size(*config.pq_size);
  if (config.mshr_size.has_value())
    builder.mshr_size(*config.mshr_size);
  if (config.hit_latency.has_value())
    builder.hit_latency(*config.hit_latency);
  if (config.fill_latency.has_value())
    builder.fill_latency(*config.fill_latency);
}

[[nodiscard]] auto parse_replacement_policy(std::string_view value) -> ReplayConfig::replacement_policy_kind
{
  const auto normalized = upper_case(std::string{value});
  if (normalized == "LRU")
    return ReplayConfig::replacement_policy_kind::lru;
  if (normalized == "SRRIP")
    return ReplayConfig::replacement_policy_kind::srrip;
  throw std::invalid_argument{fmt::format("Unsupported replay replacement policy '{}'", value)};
}

[[nodiscard]] auto parse_config(const json& document) -> ReplayConfig
{
  ReplayConfig config{};
  const auto cache_it = document.find("cache");
  if (cache_it == document.end()) {
    return config;
  }

  const auto& cache = *cache_it;
  if (const auto it = cache.find("name"); it != cache.end())
    config.name = it->get<std::string>();
  if (const auto it = cache.find("sets"); it != cache.end())
    config.sets = parse_u32(*it, "sets");
  if (const auto it = cache.find("ways"); it != cache.end())
    config.ways = parse_u32(*it, "ways");
  if (const auto it = cache.find("pq_size"); it != cache.end())
    config.pq_size = parse_u32(*it, "pq_size");
  if (const auto it = cache.find("mshr_size"); it != cache.end())
    config.mshr_size = parse_u32(*it, "mshr_size");
  if (const auto it = cache.find("hit_latency"); it != cache.end())
    config.hit_latency = parse_u64(*it);
  if (const auto it = cache.find("fill_latency"); it != cache.end())
    config.fill_latency = parse_u64(*it);
  if (const auto it = cache.find("memory_latency"); it != cache.end())
    config.memory_latency = parse_u64(*it);
  if (const auto it = cache.find("replacement_policy"); it != cache.end())
    config.replacement_policy = parse_replacement_policy(it->get<std::string>());

  return config;
}

[[nodiscard]] auto parse_accesses(const json& document) -> std::vector<ReplayAccess>
{
  const auto accesses_it = document.find("accesses");
  if (accesses_it == document.end() || !accesses_it->is_array() || accesses_it->empty()) {
    throw std::invalid_argument{"Replay file must contain a non-empty 'accesses' array"};
  }

  std::vector<ReplayAccess> accesses{};
  accesses.reserve(accesses_it->size());

  for (const auto& entry : *accesses_it) {
    if (!entry.is_object()) {
      throw std::invalid_argument{"Replay access entries must be JSON objects"};
    }

    const auto address_it = entry.find("address");
    if (address_it == entry.end()) {
      throw std::invalid_argument{"Replay access entries must contain an 'address' field"};
    }

    ReplayAccess access{};
    access.address = champsim::address{parse_u64(*address_it)};
    if (const auto it = entry.find("type"); it != entry.end())
      access.type = parse_access_type(*it);
    if (const auto it = entry.find("cpu"); it != entry.end())
      access.cpu = parse_u32(*it, "cpu");

    accesses.push_back(access);
  }

  return accesses;
}

[[nodiscard]] auto require_bridge_field(const json& entry, std::string_view field_name) -> const json&
{
  const auto it = entry.find(std::string{field_name});
  if (it == entry.end()) {
    throw std::invalid_argument{fmt::format("Bridge replay trace entries must contain '{}'", field_name)};
  }
  return *it;
}

void append_bridge_lines(std::vector<ReplayAccess>& accesses, const json& entry, uint64_t line_size)
{
  const auto address = parse_u64(require_bridge_field(entry, "address"));
  const auto size = parse_u64(require_bridge_field(entry, "size"));
  if (size == 0) {
    throw std::invalid_argument{"Bridge replay trace entry size must be positive"};
  }

  const auto first_line = (address / line_size) * line_size;
  const auto end_address = address + size - 1;
  if (end_address < address) {
    throw std::invalid_argument{"Bridge replay trace entry address + size overflows uint64"};
  }
  const auto last_line = (end_address / line_size) * line_size;
  const auto phase = entry.value("phase", "default");
  const auto cpu = entry.contains("cpu") ? parse_u32(entry["cpu"], "cpu") : uint32_t{0};
  const auto type = parse_bridge_access_type(require_bridge_field(entry, "access_type"));

  for (uint64_t line_address = first_line;; line_address += line_size) {
    accesses.push_back(ReplayAccess{champsim::address{line_address}, type, cpu, phase});
    if (line_address == last_line)
      break;
  }
}

[[nodiscard]] auto parse_bridge_accesses(const json& trace, uint64_t line_size) -> std::vector<ReplayAccess>
{
  if (line_size == 0) {
    throw std::invalid_argument{"Bridge replay line size must be positive"};
  }
  if (!trace.is_array() || trace.empty()) {
    throw std::invalid_argument{"Bridge replay trace must be a non-empty JSON array"};
  }

  std::vector<ReplayAccess> accesses{};
  for (const auto& entry : trace) {
    if (!entry.is_object()) {
      throw std::invalid_argument{"Bridge replay trace entries must be JSON objects"};
    }
    append_bridge_lines(accesses, entry, line_size);
  }
  return accesses;
}

[[nodiscard]] auto bridge_cache_config(const json& cache_json, const std::string& cache_name, uint64_t l3_size_byte, uint64_t associativity, uint64_t line_size) -> ReplayConfig
{
  if (l3_size_byte == 0 || associativity == 0 || line_size == 0) {
    throw std::invalid_argument{"Bridge replay cache geometry fields must be positive"};
  }
  const auto geometry_bytes = associativity * line_size;
  if (geometry_bytes == 0 || l3_size_byte % geometry_bytes != 0) {
    throw std::invalid_argument{"Bridge replay l3-size-byte must be divisible by l3-associativity * l3-line-size-byte"};
  }

  ReplayConfig config{};
  config.name = cache_name;
  config.sets = static_cast<uint32_t>(l3_size_byte / geometry_bytes);
  config.ways = static_cast<uint32_t>(associativity);
  if (const auto it = cache_json.find("hit_latency_cycles"); it != cache_json.end())
    config.hit_latency = parse_u64(*it);
  if (const auto it = cache_json.find("fill_latency_cycles"); it != cache_json.end())
    config.fill_latency = parse_u64(*it);
  if (const auto it = cache_json.find("memory_latency_cycles"); it != cache_json.end())
    config.memory_latency = parse_u64(*it);
  if (const auto it = cache_json.find("replacement_policy"); it != cache_json.end())
    config.replacement_policy = parse_replacement_policy(it->get<std::string>());
  return config;
}

[[nodiscard]] auto measure_count(const CACHE& cache, const ReplayAccess& access, bool hit) -> uint64_t
{
  const auto key = std::pair{access.type, access.cpu};
  return hit ? cache.sim_stats.hits.value_or(key, 0) : cache.sim_stats.misses.value_or(key, 0);
}

void record_address_observation(std::map<std::string, AddressSummary>& address_summary, const ReplayObservation& observation)
{
  auto& entry = address_summary[observation.address];
  if (observation.result == "hit")
    ++entry.hits;
  else
    ++entry.misses;
}

void record_phase_observation(std::map<std::string, AddressSummary>& phase_summary, const ReplayObservation& observation)
{
  auto& entry = phase_summary[observation.phase];
  if (observation.result == "hit")
    ++entry.hits;
  else
    ++entry.misses;
}

[[nodiscard]] auto observe_access(CACHE& cache, ReplayProducer& producer, FixedLatencyMemory& memory, const ReplayAccess& access) -> ReplayObservation
{
  const auto hits_before = measure_count(cache, access, true);
  const auto misses_before = measure_count(cache, access, false);

  producer.issue(access);

  std::array<champsim::operable*, 3> elements{{&cache, &memory, &producer}};
  while (!producer.completed_latency().has_value()) {
    for (auto* element : elements) {
      element->_operate();
    }
  }

  const auto hits_after = measure_count(cache, access, true);
  const auto misses_after = measure_count(cache, access, false);
  const auto latency = producer.completed_latency().value();
  producer.clear_completed();

  if (hits_after == hits_before + 1) {
    return ReplayObservation{format_address(access.address), "hit", std::string{access_type_names.at(champsim::to_underlying(access.type))}, access.cpu, latency,
                             access.phase};
  }

  if (misses_after == misses_before + 1) {
    return ReplayObservation{format_address(access.address), "miss", std::string{access_type_names.at(champsim::to_underlying(access.type))}, access.cpu, latency,
                             access.phase};
  }

  throw std::runtime_error{fmt::format("Replay access {} did not update cache hit/miss counters", format_address(access.address))};
}

template <typename Builder>
[[nodiscard]] auto run_replay_with_builder(Builder builder, const ReplayConfig& config, const std::vector<ReplayAccess>& accesses) -> ReplayReport
{
  ReplayProducer producer{};
  FixedLatencyMemory memory{config.memory_latency};

  apply_cache_overrides(builder, config, &producer.queues, &memory.queues);
  CACHE cache{builder};

  std::array<champsim::operable*, 3> elements{{&cache, &memory, &producer}};
  for (auto* element : elements) {
    element->initialize();
    element->warmup = false;
    element->begin_phase();
  }

  ReplayReport report{};
  report.accesses.reserve(accesses.size());

  for (const auto& access : accesses) {
    auto observation = observe_access(cache, producer, memory, access);
    report.total_latency_cycles += observation.latency_cycles;
    report.hits += (observation.result == "hit");
    report.misses += (observation.result == "miss");
    record_address_observation(report.addresses, observation);
    record_phase_observation(report.phases, observation);
    report.accesses.push_back(std::move(observation));
  }

  return report;
}

[[nodiscard]] auto run_replay(const ReplayConfig& config, const std::vector<ReplayAccess>& accesses) -> ReplayReport
{
  if (config.replacement_policy == ReplayConfig::replacement_policy_kind::srrip) {
    auto builder = champsim::cache_builder{champsim::defaults::default_llc}.replacement<srrip>();
    return run_replay_with_builder(builder, config, accesses);
  }

  auto builder = champsim::defaults::default_llc;
  return run_replay_with_builder(builder, config, accesses);
}

[[nodiscard]] auto to_json(const ReplayReport& report) -> json
{
  auto access_json = json::array();
  for (const auto& observation : report.accesses) {
    access_json.push_back(json{{"address", observation.address},
                               {"result", observation.result},
                               {"type", observation.access_type},
                               {"cpu", observation.cpu},
                               {"latency_cycles", observation.latency_cycles},
                               {"phase", observation.phase}});
  }

  return json{{"summary",
               {{"accesses", report.accesses.size()},
                {"hits", report.hits},
                {"misses", report.misses},
                {"total_latency_cycles", report.total_latency_cycles}}},
              {"accesses", std::move(access_json)},
              {"addresses", [&report] {
                auto address_json = json::object();
                for (const auto& [address, summary] : report.addresses) {
                  address_json[address] = json{{"hits", summary.hits}, {"misses", summary.misses}};
                }
                return address_json;
              }()},
              {"phase_stats", [&report] {
                auto phase_json = json::object();
                for (const auto& [phase, summary] : report.phases) {
                  phase_json[phase] = json{{"accesses", summary.hits + summary.misses}, {"hits", summary.hits}, {"misses", summary.misses}};
                }
                return phase_json;
              }()}};
}

[[nodiscard]] auto to_bridge_json(const ReplayReport& report, uint64_t clock_frequency_hz) -> json
{
  if (clock_frequency_hz == 0) {
    throw std::invalid_argument{"Bridge replay clock frequency must be positive"};
  }
  const auto access_count = report.hits + report.misses;
  auto output = json{{"total_memory_time_sec", static_cast<double>(report.total_latency_cycles) / static_cast<double>(clock_frequency_hz)},
                     {"hit_count", report.hits},
                     {"miss_count", report.misses},
                     {"hit_rate", access_count > 0 ? static_cast<double>(report.hits) / static_cast<double>(access_count) : 0.0},
                     {"phase_stats", [&report] {
                        auto phase_json = json::object();
                        for (const auto& [phase, summary] : report.phases) {
                          phase_json[phase] = json{{"accesses", summary.hits + summary.misses}, {"hits", summary.hits}, {"misses", summary.misses}};
                        }
                        return phase_json;
                      }()}};
  output["driver_summary"] = json{{"accesses", access_count},
                                  {"hits", report.hits},
                                  {"misses", report.misses},
                                  {"total_latency_cycles", report.total_latency_cycles}};
  return output;
}

void write_report(const json& report, const std::string& output_path)
{
  if (output_path.empty()) {
    fmt::print("{}\n", report.dump(2));
    return;
  }

  std::ofstream output{output_path};
  if (!output.is_open()) {
    throw std::runtime_error{fmt::format("Unable to open replay output '{}'", output_path)};
  }
  output << report.dump(2) << '\n';
}

[[nodiscard]] auto run_legacy_input(const std::string& input_path) -> json
{
  const auto document = read_json_file(input_path);
  const auto config = parse_config(document);
  const auto accesses = parse_accesses(document);
  return to_json(run_replay(config, accesses));
}

[[nodiscard]] auto parse_bridge_cache_json(const std::string& bridge_cache_json) -> json
{
  if (bridge_cache_json.empty()) {
    return json::object();
  }
  auto parsed = json::parse(bridge_cache_json);
  if (!parsed.is_object()) {
    throw std::invalid_argument{"--bridge-cache-json must be a JSON object"};
  }
  return parsed;
}

[[nodiscard]] auto run_bridge_input(const std::string& trace_path, const std::string& cache_name, const std::string& bridge_cache_json,
                                    uint64_t l3_size_byte, uint64_t l3_associativity, uint64_t l3_line_size_byte,
                                    uint64_t clock_frequency_hz, const std::string& output_format) -> json
{
  if (output_format != "json") {
    throw std::invalid_argument{"Only --output-format json is supported"};
  }
  const auto cache_json = parse_bridge_cache_json(bridge_cache_json);
  const auto config = bridge_cache_config(cache_json, cache_name, l3_size_byte, l3_associativity, l3_line_size_byte);
  const auto trace = read_json_file(trace_path);
  const auto accesses = parse_bridge_accesses(trace, l3_line_size_byte);
  return to_bridge_json(run_replay(config, accesses), clock_frequency_hz);
}
} // namespace

int llmcompass_replay_main(int argc, char** argv)
{
  CLI::App app{"ChampSim LLC ordered replay driver"};

  std::string input_path;
  std::string output_path;
  std::string trace_path;
  std::string cache_name;
  std::string output_format{"json"};
  std::string bridge_cache_json;
  uint64_t l3_size_byte{0};
  uint64_t l3_associativity{0};
  uint64_t l3_line_size_byte{0};
  uint64_t bridge_clock_frequency_hz{1000000000};

  app.add_option("--input", input_path, "Legacy replay JSON file to execute")->check(CLI::ExistingFile);
  app.add_option("--trace", trace_path, "LLMCompass bridge replay trace JSON")->check(CLI::ExistingFile);
  app.add_option("--output", output_path, "Optional JSON output path; stdout is used when omitted");
  app.add_option("--cache-name", cache_name, "LLMCompass bridge cache name");
  app.add_option("--l3-size-byte", l3_size_byte, "LLMCompass bridge L3 size in bytes");
  app.add_option("--l3-associativity", l3_associativity, "LLMCompass bridge L3 associativity");
  app.add_option("--l3-line-size-byte", l3_line_size_byte, "LLMCompass bridge line size in bytes");
  app.add_option("--output-format", output_format, "LLMCompass bridge output format");
  app.add_option("--bridge-cache-json", bridge_cache_json, "Optional LLMCompass bridge cache timing JSON");
  app.add_option("--bridge-clock-frequency-hz", bridge_clock_frequency_hz, "Clock frequency used to convert replay cycles to seconds");

  try {
    app.parse(argc, argv);

    if (!input_path.empty() && !trace_path.empty()) {
      throw std::invalid_argument{"Use either --input or --trace, not both"};
    }
    if (!input_path.empty()) {
      write_report(run_legacy_input(input_path), output_path);
      return 0;
    }
    if (!trace_path.empty()) {
      write_report(
          run_bridge_input(trace_path, cache_name, bridge_cache_json, l3_size_byte, l3_associativity, l3_line_size_byte,
                           bridge_clock_frequency_hz, output_format),
          output_path);
      return 0;
    }
    throw std::invalid_argument{"Either --input or --trace is required"};
  } catch (const CLI::ParseError& e) {
    return app.exit(e);
  } catch (const std::exception& e) {
    fmt::print(stderr, "llmcompass_replay error: {}\n", e.what());
    return 1;
  }
}

#ifdef CHAMPSIM_LLMCOMPASS_REPLAY_MAIN
int main(int argc, char** argv) { return llmcompass_replay_main(argc, argv); }
#endif
