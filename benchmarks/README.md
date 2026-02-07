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

Outputs are written under:

- `target/ann_inproc/<run_id>/manifest.txt` environment and run metadata
- `target/ann_inproc/<run_id>/raw/*.json` raw benchmark outputs from `ann_inproc_bench`
- `target/ann_inproc/<run_id>/logs/*.stdout.log` per-run CSV metrics
- `target/ann_inproc/<run_id>/logs/*.stderr.log` per-run diagnostics
- `target/ann_inproc/<run_id>/summary/inproc_summary.json` machine-readable ranked summary
- `target/ann_inproc/<run_id>/summary/inproc_summary.md` publication-ready markdown
- `target/ann_inproc/<run_id>/summary/inproc_candidates.csv` flattened candidate table

Manual summary generation (if needed):

```bash
python benchmarks/ann-benchmarks/summarize_inproc_results.py \
  --input-glob "target/ann_inproc/<run_id>/raw/*.json" \
  --recall-targets 0.90,0.95,0.99 \
  --output-json target/ann_inproc/<run_id>/summary/inproc_summary.json \
  --output-md target/ann_inproc/<run_id>/summary/inproc_summary.md \
  --output-csv target/ann_inproc/<run_id>/summary/inproc_candidates.csv
```

## Pre-submit checklist

- For core ANN claims, include in-process results from `ann_inproc_bench`.
- For reproducibility, include `manifest.txt` + `inproc_summary.json` + raw per-run JSON files.
- Build with pinned commit (`KYRODB_REF=<sha>`), not floating branch.
- Re-run datasets at least once from a clean Docker cache.
- Confirm no wrapper edits between run and publish.
- Publish only ANN-Benchmarks plots/tables produced from those runs.
- Clearly separate in-process ANN claims from service/E2E database claims.
