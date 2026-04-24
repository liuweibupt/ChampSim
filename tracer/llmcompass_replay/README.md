# llmcompass_replay

`bin/llmcompass_replay` 是一个最小化的 ChampSim LLC ordered replay driver。

## 输入格式

支持两种输入：

1. legacy `--input` JSON：用于直接调试 ordered LLC replay。
2. LLMCompass bridge `--trace` JSON array：用于接入 `software_model.champsim_bridge.run_replay`。

### Legacy `--input`

```json
{
  "cache": {
    "name": "test-llc",
    "sets": 1,
    "ways": 2,
    "hit_latency": 2,
    "fill_latency": 1,
    "memory_latency": 5
  },
  "accesses": [
    {"address": "0x0"},
    {"address": "0x40"},
    {"address": "0x0"}
  ]
}
```

#### `cache`

- `name`: 可选，cache 名称
- `sets`: 可选，set 数
- `ways`: 可选，way 数
- `pq_size`: 可选，prefetch queue 大小
- `mshr_size`: 可选，MSHR 数
- `hit_latency`: 可选，命中延迟（cycles）
- `fill_latency`: 可选，fill 延迟（cycles）
- `memory_latency`: 可选，下层固定内存延迟（cycles）
- `replacement_policy`: 可选，当前支持 `lru`（默认）和 `srrip`

未提供的 cache 参数会回退到 `champsim::defaults::default_llc`。

#### `accesses`

每个访问项支持：

- `address`: 必填，整数或形如 `"0x40"` 的字符串
- `type`: 可选，默认 `LOAD`，支持 `LOAD`/`RFO`/`PREFETCH`/`WRITE`/`TRANSLATION`
- `cpu`: 可选，默认 `0`

### LLMCompass bridge `--trace`

`--trace` 接收 LLMCompass replay trace array。每个 entry 至少包含：

- `address`: 起始 byte address
- `size`: 本次 tensor access 的 byte 数；driver 会按 `--l3-line-size-byte` 展开为 cache-line accesses
- `access_type`: `read`/`write`/`prefetch`/`rfo`/`translation`

可选字段：

- `phase`: 用于输出 `phase_stats`
- `cpu`: 默认 `0`

示例：

```bash
bin/llmcompass_replay \
  --trace trace.json \
  --cache-name LLC \
  --l3-size-byte 268435456 \
  --l3-associativity 16 \
  --l3-line-size-byte 64 \
  --output-format json \
  --bridge-cache-json '{"hit_latency_cycles":20,"fill_latency_cycles":1,"memory_latency_cycles":500}' \
  --bridge-clock-frequency-hz 1410000000
```

bridge 输出字段为 LLMCompass 直接消费的 JSON：

- `total_memory_time_sec`
- `hit_count`
- `miss_count`
- `hit_rate`
- `eviction_count`
- `phase_stats`

## 构建

```bash
make bin/llmcompass_replay -j2
```

## 运行

```bash
bin/llmcompass_replay --input replay.json
bin/llmcompass_replay --input replay.json --output summary.json
bin/llmcompass_replay --trace trace.json --cache-name LLC --l3-size-byte 268435456 --l3-associativity 16 --l3-line-size-byte 64
```

## 输出

输出为结构化 JSON，包含：

- `summary`: 总访问数、hit/miss 数、总 replay latency
- `latency_cycles`: 以 driver 端到端 round-trip cycles 统计
- `accesses`: 每条访问的 hit/miss 与 latency
- `addresses`: 按地址聚合的 hit/miss 统计
