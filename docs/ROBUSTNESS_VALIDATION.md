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

## Gate 4: ANN FP32 Campaign

Run the reproducible ANN-team FP32 matrix with deterministic run IDs:

```bash
BENCH_ROOT=target/ann_parity
DATA_DIR="$BENCH_ROOT/data"
OUT_ROOT="$BENCH_ROOT/out"

bash benchmarks/ann-benchmarks/run_inproc_parallel_matrix.sh \
  --run-id ann_team_release_candidate_fp32_hard \
  --run-label ann_team_fp32_hard \
  --data-dir "$DATA_DIR" \
  --out-root "$OUT_ROOT" \
  --datasets glove-100-angular,gist-960-euclidean \
  --m-values 48,56 \
  --ef-construction-values 800,1200 \
  --ef-search-values 256,384,512,768,1024,1536,2048,2560 \
  --recall-targets 0.95,0.99 \
  --k 10 \
  --repetitions 2 \
  --warmup-queries 100 \
  --max-queries 5000 \
  --parallel-configs 16 \
  --threads-per-job 2 \
  --resume \
  --check-gates \
  --allow-missing-gate-datasets

bash benchmarks/ann-benchmarks/run_inproc_parallel_matrix.sh \
  --run-id ann_team_release_candidate_fp32_guard \
  --run-label ann_team_fp32_guard \
  --data-dir "$DATA_DIR" \
  --out-root "$OUT_ROOT" \
  --datasets sift-128-euclidean,mnist-784-euclidean \
  --m-values 56 \
  --ef-construction-values 800 \
  --ef-search-values 64,128,192,256,384,512,768,1024 \
  --recall-targets 0.95,0.99 \
  --k 10 \
  --repetitions 2 \
  --warmup-queries 100 \
  --max-queries 5000 \
  --parallel-configs 16 \
  --threads-per-job 2 \
  --resume \
  --skip-build \
  --check-gates \
  --allow-missing-gate-datasets
```

Package artifacts:

```bash
bash benchmarks/ann-benchmarks/package_evidence_bundle.sh \
  --out-root target/ann_parity/out \
  --run-ids ann_team_release_candidate_fp32_hard,ann_team_release_candidate_fp32_guard \
  --output-dir target/ann_parity
```

Artifacts:
- per-run manifests/raw/summary/plots under `target/ann_parity/out/<run_id>/`
- final tarball + checksum + manifest under `target/ann_parity/`

## Evidence Package Rule

A candidate is promotable only when all four gates are green and artifacts are retained:

1. Core test and lint logs
2. Soak summary/logs
3. Sanitizer logs
4. ANN FP32 campaign evidence bundle
