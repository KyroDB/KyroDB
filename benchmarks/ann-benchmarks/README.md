# KyroDB ANN-Benchmarks Adapter

This directory contains the ANN-Benchmarks algorithm adapter for KyroDB.

## Files

- `module.py`: ANN-Benchmarks `BaseANN` adapter implementation.
- `Dockerfile`: algorithm image build, including `kyrodb_server` and Python SDK.
- `config.yml`: ANN-Benchmarks run groups (`M`, `ef_construction`, `ef_search`).
- `__init__.py`: package marker for ann-benchmarks import path.

Auxiliary scripts in this same directory are for the in-process benchmark workflow (`ann_inproc_bench`) and result packaging.

## Integration with upstream ann-benchmarks

```bash
git clone https://github.com/erikbern/ann-benchmarks.git
cd ann-benchmarks

mkdir -p ann_benchmarks/algorithms/kyrodb
cp /path/to/KyroDB/benchmarks/ann-benchmarks/Dockerfile ann_benchmarks/algorithms/kyrodb/Dockerfile
cp /path/to/KyroDB/benchmarks/ann-benchmarks/module.py ann_benchmarks/algorithms/kyrodb/module.py
cp /path/to/KyroDB/benchmarks/ann-benchmarks/config.yml ann_benchmarks/algorithms/kyrodb/config.yml
cp /path/to/KyroDB/benchmarks/ann-benchmarks/__init__.py ann_benchmarks/algorithms/kyrodb/__init__.py
```

Fast path via wrapper script (recommended):

```bash
bash /path/to/KyroDB/benchmarks/ann-benchmarks/run_annbenchmarks_adapter.sh \
  --ann-root /path/to/ann-benchmarks \
  --datasets sift-128-euclidean \
  --kyrodb-ref <CORE_COMMIT_SHA> \
  --sdk-version 0.1.0
```

## Build algorithm image

Use pinned core and SDK refs for reproducibility:

```bash
python install.py --algorithm kyrodb \
  --build-arg KYRODB_GIT=https://github.com/KyroDB/KyroDB.git \
  KYRODB_REF=<CORE_COMMIT_SHA> \
  KYRODB_SDK_VERSION=0.1.0
```

## Run datasets

```bash
python run.py --algorithm kyrodb --dataset sift-128-euclidean
python run.py --algorithm kyrodb --dataset glove-100-angular
python run.py --algorithm kyrodb --dataset gist-960-euclidean
python run.py --algorithm kyrodb --dataset mnist-784-euclidean
```

Run the full 4-dataset campaign with one command:

```bash
bash /path/to/KyroDB/benchmarks/ann-benchmarks/run_annbenchmarks_adapter.sh \
  --ann-root /path/to/ann-benchmarks \
  --datasets sift-128-euclidean,glove-100-angular,gist-960-euclidean,mnist-784-euclidean \
  --kyrodb-ref <CORE_COMMIT_SHA> \
  --sdk-version 0.1.0 \
  --plot
```

## Notes

- The adapter starts one local `kyrodb_server` process per benchmark worker.
- All RPC calls go through the released `kyrodb` Python SDK.
- For `angular` datasets, vectors are normalized before insert/search.
- ANN output IDs are converted from KyroDB doc IDs (`1..N`) to ann-benchmarks row IDs (`0..N-1`).
