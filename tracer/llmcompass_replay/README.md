# llmcompass_replay

`bin/llmcompass_replay` 是一个最小化的 ChampSim LLC ordered replay driver。

## 输入格式

当前仅支持 JSON：

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

### `cache`

- `name`: 可选，cache 名称
- `sets`: 可选，set 数
- `ways`: 可选，way 数
- `pq_size`: 可选，prefetch queue 大小
- `mshr_size`: 可选，MSHR 数
- `hit_latency`: 可选，命中延迟（cycles）
- `fill_latency`: 可选，fill 延迟（cycles）
- `memory_latency`: 可选，下层固定内存延迟（cycles）

未提供的 cache 参数会回退到 `champsim::defaults::default_llc`。

### `accesses`

每个访问项支持：

- `address`: 必填，整数或形如 `"0x40"` 的字符串
- `type`: 可选，默认 `LOAD`，支持 `LOAD`/`RFO`/`PREFETCH`/`WRITE`/`TRANSLATION`
- `cpu`: 可选，默认 `0`

## 构建

```bash
make bin/llmcompass_replay -j2
```

## 运行

```bash
bin/llmcompass_replay --input replay.json
bin/llmcompass_replay --input replay.json --output summary.json
```

## 输出

输出为结构化 JSON，包含：

- `summary`: 总访问数、hit/miss 数、总 replay latency
- `latency_cycles`: 以 driver 端到端 round-trip cycles 统计
- `accesses`: 每条访问的 hit/miss 与 latency
- `addresses`: 按地址聚合的 hit/miss 统计
