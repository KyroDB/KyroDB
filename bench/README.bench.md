# Reproducible Bench Workflow

This document describes how to reproduce KyroDB results for 1M / 10M / 50M keys, capture artifacts, and compare RMI vs B-Tree.

## Prereqs
- Rust stable, Linux/macOS
- Sufficient disk space for chosen scale

## Build
```
cargo build -p kyrodb-engine --release --features learned-index
```

## Run server for HTTP bench (raw/fast endpoints)
- Disable rebuilds during read phase to keep the index fixed:
```
./target/release/kyrodb-engine serve 127.0.0.1 3030 \
  --rmi-rebuild-appends 0 --rmi-rebuild-ratio 0.0
```
- Benchmark policy: do NOT configure auth or rate limiting; enable `bench-no-metrics` to avoid metrics overhead if supported.
- Optional warm start to avoid cold-start tails:
```
KYRODB_WARM_ON_START=1 ./target/release/kyrodb-engine serve 127.0.0.1 3030
```

## Load and run HTTP read test
```
# Load N keys with V-byte values, then build RMI and warm up before measuring
./target/release/bench \
  --base http://127.0.0.1:3030 \
  --load-n 1000000 \
  --val-bytes 64 \
  --load-concurrency 64 \
  --read-concurrency 64 \
  --read-seconds 30 \
  --dist uniform \
  --out-csv bench_latency.csv
```
- Prime before measurements: `POST /v1/rmi/build` then `POST /v1/warmup` (current stable API).
- Index-only vs full read: `/v1/lookup_fast/{k}` returns offset (index-only), `/v1/get_fast/{k}` returns value bytes.

## Warm vs Cold
- Cold starts incur page faults; expect higher first-hit latencies.
- For steady-state comparisons either warm on start or perform explicit priming and only record steady-state window.

## HTTP status and error codes (fast routes)
- `GET /v1/lookup_fast/{key}` → 200 with 8 bytes (offset) on hit; 404 on miss.
- `GET /v1/get_fast/{key}` → 200 with value bytes on hit; 404 on miss.
- `GET /v1/lookup_raw?key=...` → 204 No Content on hit; 404 on miss.
- `POST /v1/put` → 200 with `{ offset }` on success; 500 on error.

## In-process microbench (sub-HTTP)
```
cargo bench -p bench --bench kv_index -- --sample-size 20
```

## Collect artifacts
Run the helper script to gather outputs into bench/results/<commit>/:
```
./bench/scripts/capture.sh 1_000_000 64 uniform
```
Artifacts captured:
- Prometheus /metrics scrape (before/after)
- bench stdout (RPS, percentiles)
- Latency CSV
- Engine logs (if configured)
- Config (flags, env)

## Tuning knobs (RMI)
- Target leaf size: `KYRODB_RMI_TARGET_LEAF` (default 1024)
- Epsilon multiplier: `KYRODB_RMI_EPS_MULT` (>=1.0, default 1.0)
- Suggested starting points by scale:
  - 1M–10M keys: leaf 1024–2048, eps_mult 1.0–1.2
  - 50M keys: leaf 2048–4096, eps_mult 1.0–1.2

## Scales
- 1M, 10M, 50M recommended; ensure disk space and RAM are sufficient. Use release build.

> Note: Planned `/v2/*` hybrid endpoints are Phase 1; current benches use `/v1/*`.
