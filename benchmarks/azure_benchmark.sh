#!/bin/bash
# ============================================================================
# KyroDB Azure VM Benchmark Script
# Run: bash benchmarks/azure_benchmark.sh
# ============================================================================

set -e

echo "============================================================"
echo "KyroDB Azure VM Benchmark Suite"
echo "============================================================"

# Configuration
export RUST_LOG=warn
DATA_DIR="${KYRODB_DATA_DIR:-./benchmark_data}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to run benchmark with specific config
run_benchmark() {
    local config_name=$1
    local dataset=$2
    local ef_construction=$3
    local ef_search=$4
    local m=$5
    local max_elements=$6
    
    echo ""
    echo -e "${YELLOW}============================================================${NC}"
    echo -e "${YELLOW}Running: $config_name${NC}"
    echo -e "${YELLOW}Dataset: $dataset, M=$m, ef_c=$ef_construction, ef_s=$ef_search${NC}"
    echo -e "${YELLOW}============================================================${NC}"
    
    # Create config file
    cat > /tmp/benchmark_${config_name}.toml << EOF
# Auto-generated benchmark config: $config_name

[server]
host = "0.0.0.0"
port = 50051
http_port = 51051

[cache]
capacity = $((max_elements * 2))
hot_tier_hard_limit = $((max_elements * 4))

[hnsw]
m = $m
ef_construction = $ef_construction
ef_search = $ef_search
dimension = 128
max_elements = $max_elements
distance = "euclidean"

[persistence]
enabled = true
data_dir = "$DATA_DIR/${config_name}"
wal_fsync = "never"
snapshot_interval = 1000000

[background]
flush_interval_ms = 5000
EOF
    
    # Clean previous data
    rm -rf "$DATA_DIR/${config_name}"
    mkdir -p "$DATA_DIR/${config_name}"
    
    # Kill any existing server
    pkill -9 kyrodb_server 2>/dev/null || true
    sleep 2
    
    # Start server
    echo "Starting KyroDB server..."
    ./target/release/kyrodb_server --config /tmp/benchmark_${config_name}.toml &
    SERVER_PID=$!
    sleep 5
    
    # Check server is running
    if ! curl -s http://localhost:51051/health > /dev/null; then
        echo -e "${RED}Server failed to start!${NC}"
        return 1
    fi
    
    # Run benchmark
    echo "Running benchmark..."
    python3 benchmarks/run_benchmark.py \
        --dataset $dataset \
        --k 10 \
        --ef-search $ef_search \
        --output-dir "benchmarks/results/${config_name}" \
        2>&1 | tee "benchmarks/results/${config_name}_log.txt"
    
    # Stop server
    kill $SERVER_PID 2>/dev/null || true
    sleep 2
    
    echo -e "${GREEN}Completed: $config_name${NC}"
}

# Ensure binary is built
echo "Building KyroDB (release mode)..."
cargo build --release --bin kyrodb_server

# Create results directory
mkdir -p benchmarks/results

# ============================================================================
# SCALE TESTS: Various dataset sizes
# ============================================================================

echo ""
echo "============================================================"
echo "PART 1: SCALE TESTS (1M, 10M, 100M, 500M)"
echo "============================================================"

# 1M vectors - SIFT dataset (baseline)
run_benchmark "scale_1m_optimized" \
    "sift-128-euclidean" \
    128 \    # ef_construction (was 64, now 128 for better recall)
    200 \    # ef_search (was 100, now 200 for better recall)
    32 \     # M (was 16, now 32 for better QPS)
    1500000  # max_elements

echo ""
echo "============================================================"
echo "SCALE TEST COMPLETE"
echo "============================================================"
echo ""
echo "Results saved in benchmarks/results/"
echo ""

# Summary
echo "============================================================"
echo "BENCHMARK SUMMARY"
echo "============================================================"
find benchmarks/results -name "*.json" -exec echo "- {}" \;
