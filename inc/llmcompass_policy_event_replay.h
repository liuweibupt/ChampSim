#ifndef LLMCOMPASS_POLICY_EVENT_REPLAY_H
#define LLMCOMPASS_POLICY_EVENT_REPLAY_H

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <utility>
#include <string_view>
#include <vector>

namespace llmcompass
{
enum class policy_kind { transparent_lru, managed_pinned, next_layer_prefetch, bypass };

struct policy_event_timing {
  uint64_t line_size{0};
  uint64_t sets{0};
  uint64_t ways{0};
  uint64_t hit_latency{0};
  uint64_t fill_latency{0};
  uint64_t memory_latency{0};
};

struct policy_event_access {
  uint64_t address{0};
  bool prefetch{false};
  uint64_t token_id{0};
  uint64_t layer_id{0};
  std::string residency_policy_hint{};
  std::string pin_window_id{"NA"};
};

struct policy_event_counters {
  uint64_t accesses{0};
  uint64_t hits{0};
  uint64_t misses{0};
  uint64_t total_latency_cycles{0};
  uint64_t demand_accesses{0};
  uint64_t demand_hits{0};
  uint64_t residency_saved_hits{0};
  uint64_t external_read_bytes{0};
  uint64_t prefetch_read_bytes{0};
  uint64_t sram_read_bytes{0};
  uint64_t sram_write_bytes{0};
};

inline auto policy_from_string(std::string_view value) -> policy_kind
{
  if (value == "transparent_lru")
    return policy_kind::transparent_lru;
  if (value == "managed_pinned")
    return policy_kind::managed_pinned;
  if (value == "next_layer_prefetch")
    return policy_kind::next_layer_prefetch;
  if (value == "bypass")
    return policy_kind::bypass;
  throw std::invalid_argument{"Unsupported G3 event-replay policy"};
}

inline auto policy_name(policy_kind policy) -> std::string_view
{
  switch (policy) {
  case policy_kind::transparent_lru:
    return "transparent_lru";
  case policy_kind::managed_pinned:
    return "managed_pinned";
  case policy_kind::next_layer_prefetch:
    return "next_layer_prefetch";
  case policy_kind::bypass:
    return "bypass";
  }
  throw std::invalid_argument{"Unknown G3 event-replay policy"};
}

inline auto is_pinned_window(const policy_event_access& access) -> bool
{
  return access.pin_window_id != "NA" && access.pin_window_id != "streaming" && !access.pin_window_id.empty();
}

inline auto hit_latency_cycles(const policy_event_timing& timing) -> uint64_t { return timing.hit_latency + 1; }

inline auto miss_latency_cycles(const policy_event_timing& timing) -> uint64_t
{
  return timing.hit_latency + timing.fill_latency + timing.memory_latency + 2;
}

class policy_event_cache
{
public:
  explicit policy_event_cache(policy_event_timing timing) : timing_(timing), sets_(timing.sets)
  {
    if (timing_.line_size == 0 || timing_.sets == 0 || timing_.ways == 0) {
      throw std::invalid_argument{"Event replay cache geometry fields must be positive"};
    }
  }

  [[nodiscard]] auto contains(uint64_t address) const -> bool
  {
    const auto [set_index, tag] = decode(address);
    const auto& set = sets_.at(set_index);
    return std::any_of(set.begin(), set.end(), [tag](const auto& line) { return line.tag == tag; });
  }

  void touch(uint64_t address)
  {
    const auto [set_index, tag] = decode(address);
    for (auto& line : sets_.at(set_index)) {
      if (line.tag == tag) {
        line.last_used = ++timestamp_;
        return;
      }
    }
    throw std::logic_error{"Cannot touch absent event-replay cache line"};
  }

  [[nodiscard]] auto insert(uint64_t address, bool pinned) -> bool
  {
    const auto [set_index, tag] = decode(address);
    auto& set = sets_.at(set_index);
    if (refresh_existing(set, tag, pinned)) {
      return true;
    }
    if (set.size() < timing_.ways) {
      set.push_back(cache_line{tag, ++timestamp_, pinned});
      return true;
    }
    const auto victim = lru_unpinned(set);
    if (victim == set.end()) {
      return false;
    }
    *victim = cache_line{tag, ++timestamp_, pinned};
    return true;
  }

private:
  struct cache_line {
    uint64_t tag;
    uint64_t last_used;
    bool pinned;
  };

  [[nodiscard]] auto decode(uint64_t address) const -> std::pair<std::size_t, uint64_t>
  {
    const auto line = address / timing_.line_size;
    return {static_cast<std::size_t>(line % timing_.sets), line / timing_.sets};
  }

  auto refresh_existing(std::vector<cache_line>& set, uint64_t tag, bool pinned) -> bool
  {
    for (auto& line : set) {
      if (line.tag == tag) {
        line.last_used = ++timestamp_;
        line.pinned = line.pinned || pinned;
        return true;
      }
    }
    return false;
  }

  static auto lru_unpinned(std::vector<cache_line>& set) -> std::vector<cache_line>::iterator
  {
    auto victim = set.end();
    for (auto it = set.begin(); it != set.end(); ++it) {
      if (it->pinned)
        continue;
      if (victim == set.end() || it->last_used < victim->last_used) {
        victim = it;
      }
    }
    return victim;
  }

  policy_event_timing timing_;
  std::vector<std::vector<cache_line>> sets_;
  uint64_t timestamp_{0};
};

inline auto should_allocate(policy_kind policy, const policy_event_access& access) -> bool
{
  if (policy == policy_kind::bypass)
    return false;
  if (policy == policy_kind::managed_pinned)
    return is_pinned_window(access) || !access.prefetch;
  return true;
}

inline void require_policy_metadata(policy_kind policy, const policy_event_access& access)
{
  if (!access.residency_policy_hint.empty() && access.residency_policy_hint != policy_name(policy)) {
    throw std::invalid_argument{"Event replay residency_policy_hint does not match the selected policy"};
  }
  if (policy != policy_kind::managed_pinned) {
    if (!access.pin_window_id.empty() && access.pin_window_id != "NA") {
      throw std::invalid_argument{"Only managed-pinned event replay may carry non-NA pin_window_id metadata"};
    }
    return;
  }
  if (access.residency_policy_hint != "managed_pinned") {
    throw std::invalid_argument{"Managed-pinned event replay requires residency_policy_hint=managed_pinned"};
  }
  if (access.pin_window_id.empty()) {
    throw std::invalid_argument{"Managed-pinned event replay requires pin_window_id metadata"};
  }
}

inline void record_measured_access(policy_event_counters& counters, const policy_event_access& access,
                                   policy_kind policy, bool hit, bool allocated, const policy_event_timing& timing)
{
  counters.accesses += 1;
  counters.hits += hit;
  counters.misses += !hit;
  counters.total_latency_cycles += hit ? hit_latency_cycles(timing) : (allocated ? miss_latency_cycles(timing) : timing.memory_latency);
  counters.sram_read_bytes += hit ? timing.line_size : 0;
  counters.sram_write_bytes += (!hit && allocated) ? timing.line_size : 0;
  counters.prefetch_read_bytes += (access.prefetch && !hit) ? timing.line_size : 0;
  if (!access.prefetch) {
    counters.demand_accesses += 1;
    counters.demand_hits += hit;
    counters.external_read_bytes += hit ? 0 : timing.line_size;
    counters.residency_saved_hits += (hit && policy == policy_kind::managed_pinned && is_pinned_window(access)) ? 1 : 0;
  }
}

inline void replay_event(policy_event_cache& cache, policy_event_counters& counters, const policy_event_access& access,
                         policy_kind policy, const policy_event_timing& timing, uint64_t warmup_tokens)
{
  require_policy_metadata(policy, access);
  const bool hit = policy != policy_kind::bypass && cache.contains(access.address);
  if (hit) {
    cache.touch(access.address);
  }
  const bool allocate = !hit && should_allocate(policy, access);
  const bool allocated = allocate && cache.insert(access.address, policy == policy_kind::managed_pinned && is_pinned_window(access));
  if (allocate && !allocated && policy == policy_kind::managed_pinned && is_pinned_window(access)) {
    throw std::runtime_error{"Managed-pinned event replay could not allocate a protected pinned line"};
  }
  if (access.token_id >= warmup_tokens) {
    record_measured_access(counters, access, policy, hit, allocated, timing);
  }
}

template <typename Source>
auto run_policy_event_replay(policy_kind policy, policy_event_timing timing, uint64_t warmup_tokens, Source source) -> policy_event_counters
{
  policy_event_cache cache{timing};
  policy_event_counters counters{};
  source([&](const policy_event_access& access) { replay_event(cache, counters, access, policy, timing, warmup_tokens); });
  return counters;
}
} // namespace llmcompass

#endif
