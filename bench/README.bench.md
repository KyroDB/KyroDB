# Reproducible Bench Workflow

This document describes how to reproduce KyroDB results for 1M / 10M / 50M keys, capture artifacts, and compare RMI vs B-Tree.

## Prereqs
- Rust stable, Linux/macOS
- Sufficient disk space for chosen scale

## Build
```
cargo build -p engine --release --features learned-index
```

## Run server for HTTP bench (raw/fast endpoints)
- Disable rebuilds during read phase to keep the index fixed:
```
./target/release/kyrodb-engine serve 127.0.0.1 3030 \
  --rmi-rebuild-appends 0 --rmi-rebuild-ratio 0.0
```

## Load and run HTTP read test
```
# Load N keys with V-byte values, hit lookup_fast (binary) or lookup_raw (204/404)
./target/release/bench \
  --base http://127.0.0.1:3030 \
  --endpoint lookup_fast \
  --load-n 1000000 \
  --val-bytes 64 \
  --read-concurrency 64 \
  --read-seconds 30
```

## In-process microbench (sub-HTTP)
```
cargo bench -p bench --bench kv_index -- --sample-size 20
```

## Collect artifacts
Run the helper script to gather outputs into bench/results/<commit>/:
```
./bench/scripts/capture.sh 1_000_000 64
```
Artifacts captured:
- Prometheus /metrics scrape
- bench stdout (RPS, durations)
- Engine logs
- Config (flags, env)

## Tuning knobs
- RMI target leaf size: `KYRODB_RMI_TARGET_LEAF` (default 1024)
- RMI epsilon multiplier: `KYRODB_RMI_EPS_MULT` (>=1.0)

## Scales
- 1M, 10M, 50M recommended; ensure disk space and RAM are sufficient. Use release build.
