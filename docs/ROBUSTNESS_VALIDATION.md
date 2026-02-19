# Robustness Validation

This guide defines the hardening gate for KyroDB before production promotion.

## Gate 1: Core Correctness Matrix

Run the complete Rust test matrix and lint gates:

```bash
cargo fmt --all --check
cargo clippy -p kyrodb-engine --all-targets -- -D warnings
cargo test -p kyrodb-engine --lib --tests --bins --no-fail-fast
cargo test -p kyrodb-engine --features loom --test loom_concurrency_tests --release
```

These include chaos/recovery, persistence, metadata filtering, mixed workload stress, and regression suites.

## Gate 2: Soak + Chaos Restart

Use the soak harness to run sustained load with periodic graceful and hard restarts:

```bash
BUILD_BINARIES=1 \
SOAK_CYCLES=12 \
CYCLE_DURATION_SECS=300 \
KILL_EVERY_N_CYCLES=3 \
QPS=1500 \
CONCURRENCY=48 \
scripts/qa/run_soak_chaos_recovery.sh
```

Artifacts:
- `target/qa/soak_<timestamp>/summary.csv`
- `target/qa/soak_<timestamp>/logs/server_cycle_*.log`
- `target/qa/soak_<timestamp>/logs/load_cycle_*.log`

Failure policy:
- Any failed load phase is a hard stop.
- Any startup timeout after restart is a hard stop.

## Gate 3: Sanitizer Matrix

Run memory/concurrency sanitizers on nightly Rust:

```bash
rustup toolchain install nightly
NIGHTLY_TOOLCHAIN=nightly-aarch64-apple-darwin \
SANITIZERS_CSV=address,thread \
scripts/qa/run_sanitizer_matrix.sh
```

Optional flags:
- `BUILD_STD=1` to rebuild `std` with sanitizer instrumentation (`rust-src` required).
- `BUILD_STD=0` keeps address/leak runs faster; thread sanitizer still auto-enables `-Zbuild-std`.
- `PANIC_STRATEGY=abort` to force abort mode (adds `-Zpanic_abort_tests` automatically).
- `HAS_STD_WORKAROUND=1` keeps the `cfg(has_std)` compatibility workaround enabled for nightly `-Zbuild-std`.
- `TARGET_TRIPLE=<triple>` to run against a non-default target.

Artifacts:
- `target/qa/sanitizers_<timestamp>/summary.txt`
- `target/qa/sanitizers_<timestamp>/*.log`

Notes:
- Sanitizer support is platform/toolchain dependent.
- Keep logs as release-blocking evidence.

## Gate 4: Official ANN-Benchmarks Campaign

Run the campaign through upstream ann-benchmarks using the KyroDB adapter:

```bash
bash benchmarks/ann-benchmarks/run_annbenchmarks_adapter.sh \
  --ann-root /path/to/ann-benchmarks \
  --datasets sift-128-euclidean,glove-100-angular,gist-960-euclidean,mnist-784-euclidean \
  --kyrodb-ref <COMMIT_SHA> \
  --sdk-version 0.1.0 \
  --plot
```

Artifacts:
- ann-benchmarks result JSON and generated plots for each dataset
- staged adapter files under `ann_benchmarks/algorithms/kyrodb/`
- pinned code references: core commit (`--kyrodb-ref`) and SDK version (`--sdk-version`)

## Evidence Package Rule

A candidate is promotable only when all four gates are green and artifacts are retained:

1. Core test and lint logs
2. Soak summary/logs
3. Sanitizer logs
4. ANN FP32 campaign evidence bundle
