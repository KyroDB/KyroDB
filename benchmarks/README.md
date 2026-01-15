# KyroDB Benchmarks

This directory contains tools for benchmarking KyroDB against standard ANN benchmarks.

## Quick Start

### 1. Start KyroDB Server

```bash
# From repository root
cargo run --release --bin kyrodb_server
```

### 2. Run Local Benchmarks

```bash
# Install dependencies
pip install grpcio grpcio-tools numpy h5py

# Run on SIFT-128 dataset
python benchmarks/run_benchmark.py --dataset sift-128-euclidean --k 10
```

### 3. View Results

Results are saved to `benchmarks/results/` as JSON files.

---

## Directory Structure

```
benchmarks/
├── ann-benchmarks/          # Files for ann-benchmarks integration
│   ├── kyrodb.py           # BaseANN wrapper
│   ├── config.yaml         # Parameter configurations
│   ├── Dockerfile          # Docker build
│   ├── kyrodb_pb2.py       # Generated gRPC stub
│   └── kyrodb_pb2_grpc.py  # Generated gRPC stub
├── run_benchmark.py        # Local benchmark runner
├── data/                   # Downloaded datasets (auto-created)
└── results/                # Benchmark results (auto-created)
```

---

## Running on Azure VM

### 1. Setup VM (Standard FX32ms v2)

```bash
# SSH to VM
ssh azureuser@<vm-ip>

# Clone repository
git clone https://github.com/<your-org>/KyroDB.git -b benchmark
cd KyroDB

# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.cargo/env

# Build release binary (reproducible)
cargo build --release --locked --bin kyrodb_server
```

### 2. Run Server with Performance Tuning

```bash
# Increase file descriptors
ulimit -n 65536

# Set CPU affinity for server (use first 16 cores)
taskset -c 0-15 ./target/release/kyrodb_server --config benchmarks/benchmark.toml &
```

### 3. Run Benchmarks

```bash
# Install Python deps
pip install grpcio grpcio-tools numpy h5py matplotlib

# Run all datasets
for dataset in sift-128-euclidean glove-100-angular gist-960-euclidean; do
    python benchmarks/run_benchmark.py --dataset $dataset --k 10 --ef-search 10,20,50,100,200,400,800
done
```

---

## ANN-Benchmarks Integration

To add KyroDB to the official ann-benchmarks:

### 1. Fork ann-benchmarks

```bash
git clone https://github.com/erikbern/ann-benchmarks
cd ann-benchmarks
```

### 2. Copy KyroDB Algorithm

```bash
mkdir -p ann_benchmarks/algorithms/kyrodb
cp /path/to/KyroDB/benchmarks/ann-benchmarks/* ann_benchmarks/algorithms/kyrodb/
```

### 3. Build Docker Image

```bash
# Create Dockerfile entry
cp ann_benchmarks/algorithms/kyrodb/Dockerfile install/Dockerfile.kyrodb

# Build
python install.py --algorithm kyrodb
```

### 4. Run Benchmarks

```bash
python run.py --algorithm kyrodb --dataset sift-128-euclidean
python plot.py --dataset sift-128-euclidean
```

### 5. Submit PR

Submit a pull request to `erikbern/ann-benchmarks` with your algorithm.

---

## Performance Targets

| Dataset   | Recall@10 | Target QPS | Current Status |
| --------- | --------- | ---------- | -------------- |
| SIFT-1M   | 0.95      | >15,000    | 🔄 Testing     |
| GloVe-100 | 0.95      | >20,000    | 🔄 Testing     |
| GIST-960  | 0.95      | >5,000     | 🔄 Testing     |

---

## Datasets

| Name                | Dimension | Train Size | Test Size | Metric |
| ------------------- | --------- | ---------- | --------- | ------ |
| sift-128-euclidean  | 128       | 1,000,000  | 10,000    | L2     |
| glove-100-angular   | 100       | 1,183,514  | 10,000    | Cosine |
| gist-960-euclidean  | 960       | 1,000,000  | 1,000     | L2     |
| mnist-784-euclidean | 784       | 60,000     | 10,000    | L2     |

Datasets are automatically downloaded when you run the benchmark.
