# KyroDB ANN Benchmarking

This directory contains the ANN-focused benchmarking path:

- ANN-Benchmarks submission artifacts (`benchmarks/ann-benchmarks/*`)
- True in-process Rust benchmark binary (`engine/src/bin/ann_inproc_bench.rs`)
- Dataset converter helper (`benchmarks/ann-benchmarks/export_ann_hdf5_to_annbin.py`)

## Scope

This workflow targets ANN-style evaluation (recall/throughput/latency trade-offs), not full production database E2E behavior.

Two modes are available:

1. True in-process: direct `HnswVectorIndex` benchmark in a single process (no gRPC in measured path)
2. ANN-Benchmarks adapter: KyroDB service inside ann-benchmarks Docker runner (submission-compatible)

The submission adapter is hardened to minimize non-ANN effects:

- benchmark mode enabled
- durability disabled for benchmark speed
- cache/predictor effects minimized
- angular vectors normalized

## Install into ann-benchmarks

```bash
git clone https://github.com/erikbern/ann-benchmarks.git
cd ann-benchmarks

mkdir -p ann_benchmarks/algorithms/kyrodb
cp /path/to/KyroDB/benchmarks/ann-benchmarks/Dockerfile ann_benchmarks/algorithms/kyrodb/Dockerfile
cp /path/to/KyroDB/benchmarks/ann-benchmarks/module.py ann_benchmarks/algorithms/kyrodb/module.py
cp /path/to/KyroDB/benchmarks/ann-benchmarks/__init__.py ann_benchmarks/algorithms/kyrodb/__init__.py
cp /path/to/KyroDB/benchmarks/ann-benchmarks/config.yml ann_benchmarks/algorithms/kyrodb/config.yml
```

## Build algorithm image

```bash
python install.py --algorithm kyrodb \
  --build-arg KYRODB_GIT=https://github.com/vatskishan03/KyroDB.git KYRODB_REF=main
```

Use an immutable commit for publication runs:

```bash
python install.py --algorithm kyrodb \
  --build-arg KYRODB_GIT=https://github.com/vatskishan03/KyroDB.git KYRODB_REF=<COMMIT_SHA>
```

## Run benchmark datasets

```bash
python run.py --algorithm kyrodb --dataset sift-128-euclidean
python run.py --algorithm kyrodb --dataset glove-100-angular
python run.py --algorithm kyrodb --dataset gist-960-euclidean
python run.py --algorithm kyrodb --dataset mnist-784-euclidean
```

Generate plots:

```bash
python plot.py --dataset sift-128-euclidean
python plot.py --dataset glove-100-angular
python plot.py --dataset gist-960-euclidean
python plot.py --dataset mnist-784-euclidean
```

## True In-Process Benchmark (Recommended for Core ANN Capability)

Build the runner:

```bash
cargo build --release -p kyrodb-engine --bin ann_inproc_bench
```

Backend note (2026-02-08):

- `ann_inproc_bench` exercises `HnswVectorIndex` with backend `kyro_single_graph`.
- The backend keeps a single mutable/readable graph representation for both inserts and queries.

Download datasets (example: SIFT):

```bash
mkdir -p benchmarks/data
curl -L https://ann-benchmarks.com/sift-128-euclidean.hdf5 -o benchmarks/data/sift-128-euclidean.hdf5
```

Convert HDF5 -> ANNBIN:

```bash
python benchmarks/ann-benchmarks/export_ann_hdf5_to_annbin.py \
  --input benchmarks/data/sift-128-euclidean.hdf5 \
  --output benchmarks/data/sift-128-euclidean.annbin
```

Run true in-process sweep:

```bash
./target/release/ann_inproc_bench \
  --dataset-annbin benchmarks/data/sift-128-euclidean.annbin \
  --dataset-name sift-128-euclidean \
  --k 10 \
  --m 32 \
  --ef-construction 600 \
  --ef-search 64,128,256,512,768,1024 \
  --repetitions 3 \
  --warmup-queries 200 \
  --output-json benchmarks/data/sift-128-euclidean.inproc.json
```

Run the same flow for:

- `glove-100-angular`
- `gist-960-euclidean`
- `mnist-784-euclidean`

### Metric Semantics (`ann_inproc_bench`)

`ann_inproc_bench` now emits two throughput metrics per sweep:

- `qps_mean` (primary): **search-only** throughput (recall computation excluded).
- `end_to_end_qps_mean`: throughput including benchmark-side recall/evaluation work.
- `config.ann_backend`: backend identifier used for the run (currently `kyro_single_graph`).

Use `qps_mean` for ANN core performance claims. Keep `end_to_end_qps_mean` for harness cost visibility.

## Azure Multi-Dataset In-Process Suite (Recommended)

Use the suite runner to execute a reproducible matrix across:

- datasets: `sift-128-euclidean`, `glove-100-angular`, `gist-960-euclidean`, `mnist-784-euclidean`
- HNSW build params: `M x ef_construction`
- query params: `ef_search` sweep

Run:

```bash
bash benchmarks/ann-benchmarks/run_inproc_azure_suite.sh \
  --run-label kyrodb-ann-pilot \
  --m-values 16,24,32 \
  --ef-construction-values 200,400,600 \
  --ef-search-values 16,32,64,128,256,512,768,1024 \
  --recall-targets 0.90,0.95,0.99 \
  --repetitions 3 \
  --warmup-queries 200 \
  --threads 16
```

Resume an interrupted run (same run directory, skip completed JSON shards):

```bash
bash benchmarks/ann-benchmarks/run_inproc_azure_suite.sh \
  --run-id kyrodb-ann-pilot_20260214T010203Z \
  --resume \
  --datasets sift-128-euclidean,glove-100-angular,gist-960-euclidean,mnist-784-euclidean \
  --m-values 16,24,32 \
  --ef-construction-values 200,400,600 \
  --ef-search-values 16,32,64,128,256,512,768,1024 \
  --recall-targets 0.90,0.95,0.99
```

Outputs are written under:

- `target/ann_inproc/<run_id>/manifest.txt` environment and run metadata
- `target/ann_inproc/<run_id>/raw/*.json` raw benchmark outputs from `ann_inproc_bench`
- `target/ann_inproc/<run_id>/logs/*.stdout.log` per-run CSV metrics
- `target/ann_inproc/<run_id>/logs/*.stderr.log` per-run diagnostics
- `target/ann_inproc/<run_id>/summary/inproc_summary.json` machine-readable ranked summary
- `target/ann_inproc/<run_id>/summary/inproc_summary.md` publication-ready markdown
- `target/ann_inproc/<run_id>/summary/inproc_candidates.csv` flattened candidate table
- `target/ann_inproc/<run_id>/plots/*.png` ANN-style performance plots (recall/QPS/build/latency/memory)
- `target/ann_inproc/<run_id>/plots/index.md` rendered plot index

Summary artifacts expose both `qps_search` (primary) and `qps_e2e` columns.

To skip plot rendering (for minimal environments): add `--no-plots`.

Manual summary generation (if needed):

```bash
python benchmarks/ann-benchmarks/summarize_inproc_results.py \
  --input-glob "target/ann_inproc/<run_id>/raw/*.json" \
  --recall-targets 0.90,0.95,0.99 \
  --output-json target/ann_inproc/<run_id>/summary/inproc_summary.json \
  --output-md target/ann_inproc/<run_id>/summary/inproc_summary.md \
  --output-csv target/ann_inproc/<run_id>/summary/inproc_candidates.csv
```

Manual plot generation (if needed):

```bash
python benchmarks/ann-benchmarks/render_inproc_plots.py \
  --summary-json target/ann_inproc/<run_id>/summary/inproc_summary.json \
  --candidates-csv target/ann_inproc/<run_id>/summary/inproc_candidates.csv \
  --output-dir target/ann_inproc/<run_id>/plots \
  --recall-targets 0.95,0.99
```

## Parallel Matrix Runner (Resumable + Hardened)

For large sweeps across datasets/configs with controlled parallel shard workers:

```bash
bash benchmarks/ann-benchmarks/run_inproc_parallel_matrix.sh \
  --run-label fx32_final_fp32 \
  --run-id fx32_final_fp32_20260214 \
  --datasets sift-128-euclidean,glove-100-angular,gist-960-euclidean,mnist-784-euclidean \
  --m-values 48,56 \
  --ef-construction-values 800,1200 \
  --ef-search-values 256,384,512,768,1024,1536,2048,2560 \
  --parallel-configs 16 \
  --threads-per-job 2 \
  --job-timeout-seconds 21600 \
  --resume
```

Key reliability behaviors:

- run-level lock (`flock`) prevents overlapping matrix runs on the same output root
- deterministic shard run IDs enable safe resume
- `--resume` skips completed shard JSONs
- per-shard status file: `target/ann_inproc_parallel/<run_id>/shard_status.tsv`
- optional timeout guard per shard job: `--job-timeout-seconds`
- optional partial-gate mode: `--allow-missing-gate-datasets`

Parallel matrix outputs include aggregate plots under:

- `target/ann_inproc_parallel/<run_id>/aggregate/plots/*.png`

## ANN-Team FP32 Campaign + Evidence Bundle

Run the ANN-team FP32 campaign directly with the matrix runner using deterministic run IDs.

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

The two run IDs above are the campaign phases:

- `ann_team_release_candidate_fp32_hard` (glove + gist)
- `ann_team_release_candidate_fp32_guard` (sift + mnist)

Manual packaging (if needed):

```bash
bash benchmarks/ann-benchmarks/package_evidence_bundle.sh \
  --out-root target/ann_parity/out \
  --run-ids ann_team_release_candidate_fp32_hard,ann_team_release_candidate_fp32_guard \
  --output-dir target/ann_parity
```

## Pre-submit checklist

- For core ANN claims, include in-process results from `ann_inproc_bench`.
- For reproducibility, include `manifest.txt` + `inproc_summary.json` + raw per-run JSON files.
- Build with pinned commit (`KYRODB_REF=<sha>`), not floating branch.
- Re-run datasets at least once from a clean Docker cache.
- Confirm no wrapper edits between run and publish.
- Publish only ANN-Benchmarks plots/tables produced from those runs.
- Clearly separate in-process ANN claims from service/E2E database claims.
