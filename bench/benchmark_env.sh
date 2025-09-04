#!/bin/bash
# filepath: /Users/kishan/Desktop/Codes/Project/ProjectKyro/bench/benchmark_env.sh

# Optimal settings for HTTP benchmarks
export KYRODB_BENCHMARK_MODE=1
export KYRODB_RL_DATA_RPS_BENCH=100000
export KYRODB_RL_DATA_BURST_BENCH=100000
export KYRODB_WARM_ON_START=1
export KYRODB_DISABLE_HTTP_LOG=1
export KYRODB_GROUP_COMMIT_DELAY_MICROS=100
export KYRODB_FSYNC_POLICY=data
export RUST_LOG=error  # Minimal logging
export KYRODB_RMI_TARGET_LEAF=2048  # Optimize RMI for benchmarks

echo "Benchmark environment set. Run: source bench/benchmark_env.sh && cargo run -p bench --release -- [args]"
