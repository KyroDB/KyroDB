# KyroDB Azure VM Benchmark Guide

## Quick Start Commands

### Step 1: SSH into Azure VM and Setup

```bash
# SSH into your Azure VM
ssh your-user@your-azure-vm

# Clone/pull latest code
cd ~/KyroDB
git pull origin benchmark

# Install dependencies
sudo apt update && sudo apt install -y build-essential pkg-config libssl-dev python3-pip
pip3 install -r benchmarks/requirements.txt

# Build release binary
cargo build --release --bin kyrodb_server

# Regenerate Python stubs
python3 benchmarks/scripts/gen_grpc_stubs.py
```

---

## OPTIMIZED Benchmark Configurations

Based on analysis, here are the recommended HNSW parameters:

| Parameter         | Default | Optimized | Effect                                 |
| ----------------- | ------- | --------- | -------------------------------------- |
| `M`               | 16      | **32**    | More graph connections → faster search |
| `ef_construction` | 64      | **128**   | Better recall at build time            |
| `ef_search`       | 100     | **200**   | Better recall at query time            |

---

## Benchmark Commands for Each Scale

### 1M Vectors (SIFT-128) - ~15 min

```bash
# Create optimized config for 1M
cat > benchmark_1m.toml << 'EOF'
[server]
host = "0.0.0.0"
port = 50051
http_port = 51051

[cache]
capacity = 2000000
strategy = "lru"
enable_ab_testing = false
enable_query_clustering = false
enable_prefetching = false
auto_tune_threshold = false

[hnsw]
m = 32                       # Optimized (was 16)
ef_construction = 128        # Optimized (was 64)
ef_search = 200              # Optimized (was 100)
dimension = 128
max_elements = 1500000
distance = "euclidean"

[persistence]
data_dir = "./data_1m"
enable_wal = false
fsync_policy = "none"
snapshot_interval_secs = 0
enable_recovery = true
allow_fresh_start_on_recovery_failure = true

[logging]
level = "warn"
EOF

# Clean and run
rm -rf data_1m && mkdir -p data_1m
pkill -9 kyrodb_server 2>/dev/null || true
RUST_LOG=warn ./target/release/kyrodb_server --config benchmark_1m.toml &
sleep 5

# Run benchmark (SIFT-128: 1M vectors)
python3 benchmarks/run_benchmark.py \
    --dataset sift-128-euclidean \
    --k 10 \
    --ef-search 200 \
    --warmup-queries 200 \
    --repetitions 3

# Stop server
pkill -9 kyrodb_server
```

---

### 10M Vectors - ~2-3 hours

For 10M vectors, you need the **SIFT-10M** dataset or can use random vectors:

```bash
# Create config for 10M
cat > benchmark_10m.toml << 'EOF'
[server]
host = "0.0.0.0"
port = 50051
http_port = 51051

[cache]
capacity = 5000000
strategy = "lru"
enable_ab_testing = false
enable_query_clustering = false
enable_prefetching = false
auto_tune_threshold = false

[hnsw]
m = 32
ef_construction = 100        # Lower for faster build at scale
ef_search = 200
dimension = 128
max_elements = 12000000
distance = "euclidean"

[persistence]
data_dir = "./data_10m"
enable_wal = false
fsync_policy = "none"
snapshot_interval_secs = 0
enable_recovery = true
allow_fresh_start_on_recovery_failure = true

[logging]
level = "warn"
EOF

# Clean and run
rm -rf data_10m && mkdir -p data_10m
pkill -9 kyrodb_server 2>/dev/null || true
RUST_LOG=warn ./target/release/kyrodb_server --config benchmark_10m.toml &
sleep 5

# Note: `benchmarks/run_benchmark.py` only downloads datasets listed in
# `benchmarks/run_benchmark.py:DATASETS`.
python3 benchmarks/run_benchmark.py \
    --dataset deep-image-96-angular \
    --k 10 \
    --ef-search 200

# Stop server
pkill -9 kyrodb_server
```

---

### 100M Vectors - ~1 day

```bash
# Create config for 100M (requires ~64GB+ RAM)
cat > benchmark_100m.toml << 'EOF'
[server]
host = "0.0.0.0"
port = 50051
http_port = 51051

[cache]
capacity = 10000000
strategy = "lru"
enable_ab_testing = false
enable_query_clustering = false
enable_prefetching = false
auto_tune_threshold = false

[hnsw]
m = 24                       # Lower M for memory efficiency
ef_construction = 64         # Lower for faster build
ef_search = 150
dimension = 128
max_elements = 110000000
distance = "euclidean"

[persistence]
data_dir = "./data_100m"
enable_wal = false
fsync_policy = "none"
snapshot_interval_secs = 0
enable_recovery = true
allow_fresh_start_on_recovery_failure = true

[logging]
level = "warn"
EOF

# Clean and run
rm -rf data_100m && mkdir -p data_100m
pkill -9 kyrodb_server 2>/dev/null || true
RUST_LOG=warn ./target/release/kyrodb_server --config benchmark_100m.toml &
sleep 5

# For 100M: Use SIFT-1B (subset) or generate random
# Dataset preparation (SIFT1B / BIGANN)
# Download base/query/ground-truth files from IRISA TEXMEX:
#   ftp://ftp.irisa.fr/local/texmex/corpus/bigann_base.bvecs.gz
#   ftp://ftp.irisa.fr/local/texmex/corpus/bigann_query.bvecs.gz
#   ftp://ftp.irisa.fr/local/texmex/corpus/bigann_gnd.zip
mkdir -p data_sift1b
cd data_sift1b
wget -c ftp://ftp.irisa.fr/local/texmex/corpus/bigann_base.bvecs.gz
wget -c ftp://ftp.irisa.fr/local/texmex/corpus/bigann_query.bvecs.gz
wget -c ftp://ftp.irisa.fr/local/texmex/corpus/bigann_gnd.zip
gunzip -k bigann_base.bvecs.gz
gunzip -k bigann_query.bvecs.gz
unzip -o bigann_gnd.zip
cd ..

# NOTE: KyroDB's end-to-end benchmark runner expects an HDF5 dataset with
# train/test/neighbors. For 100M+ runs, generating ground-truth neighbors is
# expensive. If you need recall numbers, compute ground truth with FAISS and
# convert to HDF5 before running the Python benchmark.

# Throughput-only (synthetic) benchmark for 100M scale:
./target/release/kyrodb_load_tester \
    --server http://127.0.0.1:50051 \
    --dataset 100000000 \
    --dimension 128 \
    --qps 2000 \
    --duration 600 \
    --concurrency 32

pkill -9 kyrodb_server
```

---

### 500M Vectors - ~3-5 days

```bash
# Create config for 500M (requires ~256GB+ RAM)
cat > benchmark_500m.toml << 'EOF'
[server]
host = "0.0.0.0"
port = 50051
http_port = 51051

[cache]
capacity = 20000000
strategy = "lru"
enable_ab_testing = false
enable_query_clustering = false
enable_prefetching = false
auto_tune_threshold = false

[hnsw]
m = 16                       # Lower M for memory
ef_construction = 48         # Lower for faster build
ef_search = 100
dimension = 128
max_elements = 550000000
distance = "euclidean"

[persistence]
data_dir = "./data_500m"
enable_wal = false
fsync_policy = "none"
snapshot_interval_secs = 0
enable_recovery = true
allow_fresh_start_on_recovery_failure = true

[logging]
level = "warn"
EOF

rm -rf data_500m && mkdir -p data_500m
pkill -9 kyrodb_server 2>/dev/null || true
RUST_LOG=warn ./target/release/kyrodb_server --config benchmark_500m.toml &
sleep 5

# 500M requires SIFT-1B dataset subset
# Download steps are identical to the 100M section above. Use the same TEXMEX files
# and subset the first 500M vectors for base. Ground truth is required if you want
# recall metrics and should be computed with FAISS.

# Throughput-only (synthetic) benchmark for 500M scale:
./target/release/kyrodb_load_tester \
    --server http://127.0.0.1:50051 \
    --dataset 500000000 \
    --dimension 128 \
    --qps 2000 \
    --duration 600 \
    --concurrency 32
# Download from: http://corpus-texmex.irisa.fr/

pkill -9 kyrodb_server
```

---

## Memory Requirements by Scale

| Scale | RAM Required | Disk Space | Est. Time  |
| ----- | ------------ | ---------- | ---------- |
| 1M    | 4GB          | 1GB        | ~15 min    |
| 10M   | 16GB         | 10GB       | ~2-3 hours |
| 100M  | 64GB         | 100GB      | ~1 day     |
| 500M  | 256GB        | 500GB      | ~3-5 days  |

---

## Quick All-in-One Script

```bash
#!/bin/bash
# Run all benchmarks sequentially

for scale in 1m; do  # Add 10m 100m 500m as needed
    echo "Running $scale benchmark..."
    ./target/release/kyrodb_server --config benchmark_${scale}.toml &
    sleep 10
    python3 benchmarks/run_benchmark.py --dataset sift-128-euclidean --k 10
    pkill -9 kyrodb_server
    echo "Completed $scale"
done
```

---

## Monitoring During Benchmark

```bash
# Watch server logs
tail -f /tmp/kyrodb.log

# Monitor memory
watch -n 1 'free -h'

# Monitor CPU
htop

# Check health endpoint
curl http://localhost:51051/health

# Check metrics
curl http://localhost:51051/metrics
```
