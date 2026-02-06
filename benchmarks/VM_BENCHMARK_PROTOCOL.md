# KyroDB VM Benchmark Protocol (ANN datasets)

This protocol is for producing benchmark numbers that are reproducible, defensible, and comparable.

## What we benchmark (be explicit)

KyroDB has two distinct benchmark interpretations:

1. **End-to-end database performance** (KyroDB server + RPC + serialization + engine). This is what real users experience.
2. **Core ANN index performance** (HNSW internals only). This is closer to what ann-benchmarks traditionally compares.

KyroDB’s current harness is primarily (1). That is acceptable for a database claim, but you must state it clearly.

## Reproducibility checklist (industry standard)

- **Pinned dependencies**
  - Rust: build with `cargo build --release --locked`.
  - Python: install from `benchmarks/requirements.txt`.
- **Dataset integrity**
  - Download a known dataset file and record SHA-256.
- **Environment capture**
  - Record CPU model, core count, RAM, kernel, and KyroDB git commit.
  - Write metadata into results JSON.
- **Warmup + repetitions**
  - Run a warmup phase (not measured).
  - Run multiple repetitions (default: 3) and report the mean (and optionally stdev).
- **Avoid measuring the wrong thing**
  - If reporting query latency/QPS, do not mix indexing time into the query measurement.
  - Separate phases: (A) build/load index, (B) warmup queries, (C) measured queries.

## VM tuning checklist (Linux)

These are standard practices for stable, high-signal benchmarks:

- Disable noisy background services where possible.
- Use a consistent CPU layout (pin processes):
  - `taskset -c 0-15 ./target/release/kyrodb_server ...`
- Prefer a consistent CPU frequency governor (if permitted):
  - `sudo cpupower frequency-set -g performance`
- Optional for strict runs:
  - Disable turbo boost.
  - Disable SMT (hyperthreading) if you want single-core determinism.
- Use `numactl` pinning on multi-socket machines:
  - `numactl --physcpubind=0-15 --membind=0 ...`

## Recommended run structure

1. **Build**
   - `cargo build --release --locked --bin kyrodb_server`
2. **Start server**
   - Use a config that matches dataset dimension and metric.
3. **Index/load**
   - Prefer `BulkLoadHnsw` for benchmark ingestion (fast; bypasses hot tier).
4. **Warmup**
   - Run `--warmup-queries 200` (or more if the dataset is large).
5. **Measured**
   - Run 3 repetitions and keep the mean.

## Commands (example: SIFT-1M)

- Start server:
  - `./target/release/kyrodb_server --config benchmarks/benchmark.toml`
- Run benchmark:
  - `python benchmarks/run_benchmark.py --dataset sift-128-euclidean --k 10 --ef-search 200 --warmup-queries 200 --repetitions 3`

## What to report

For each dataset + configuration:

- Dataset name + SHA-256
- HNSW parameters (`M`, `ef_construction`, `ef_search`)
- Concurrency and request mode (Search vs BulkSearch)
- Mean QPS and latency percentiles (P50/P99)
- Hardware + OS + KyroDB git commit

## Known limitations (current state)

- The local harness uses Python + gRPC; measured numbers include RPC and serialization overhead.
- BulkSearch latency is approximated via response gaps (streaming).
- Not all ann-benchmarks datasets are listed in `benchmarks/run_benchmark.py:DATASETS`.

If you want leaderboard-style comparability against in-process ANN libraries, the next step is a dedicated **embedded benchmark** (engine-in-process) that bypasses RPC.
