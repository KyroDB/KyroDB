# Testing and Reliability

## Phase Gates (Go/No-Go)

Before advancing phases, the following suites must be green:
- Property tests (proptest) for RMI correctness and epsilon bounds
- Chaos tests (failpoints) for crash recovery (WAL, snapshots, atomic swaps)
- Concurrency tests (loom) for critical paths (group commit, swaps, rotation)
- Fuzzing (cargo-fuzz) for parsers (WAL, snapshot indexes)
- Bench regression: p50/p99 latency, probe length histogram, rebuild duration

## Existing Coverage (engine/tests)
- WAL CRC tail corruption; rotation/retention; background compaction
- Snapshot atomicity and mmap read
- RMI build thresholds, epsilon bounds, probe metrics, rebuild crash/swap
- Router consistency; recovery; fast lookup chaos; enterprise-safe modes

## CI Matrix
- OSes: Linux/macOS
- Features:
  - Default with `learned-index`
  - Without learned index
  - `http-test` integration (server running)

## Fuzzing (cargo-fuzz)
- Targets: WAL and snapshot parsers
- Example:
```bash
cd engine && cargo fuzz run fuzz_target_1
```

## Concurrency (loom)
- Enable for small, critical sections (WAL rotate + manifest, index swap)
- Loom not on by default in CI due to runtime cost; run in nightly jobs or manually
