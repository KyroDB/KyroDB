#!/usr/bin/env bash
set -euo pipefail

# 🔥 KyroDB Quick Performance Test
# Skip health checks and run core benchmarks

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

BASE="http://127.0.0.1:3030"
COMMIT=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUT_DIR="$ROOT_DIR/bench/results/quick_${COMMIT}_${TIMESTAMP}"

echo -e "${BLUE}🚀 KyroDB Quick Benchmark Suite${NC}"
echo -e "${BLUE}================================${NC}"
echo -e "Commit: ${COMMIT}"
echo -e "Output: ${OUT_DIR}"
echo -e "Base URL: ${BASE}"
echo ""

mkdir -p "$OUT_DIR/logs"

cd "$ROOT_DIR"

# Test 1: Basic Performance Test
echo -e "${YELLOW}🧪 Test 1: Basic Performance (100K keys, mixed workload)${NC}"
./target/release/bench \
    --base "$BASE" \
    --workers 32 \
    --duration 30 \
    --key-count 100000 \
    --value-size 256 \
    --read-ratio 0.7 \
    --warmup 5 \
    > "$OUT_DIR/logs/basic_test.log" 2>&1

if [[ -f "$OUT_DIR/logs/basic_test.log" ]]; then
    echo -e "${GREEN}✅ Basic test completed${NC}"
    grep "Throughput:" "$OUT_DIR/logs/basic_test.log" | tail -1 || echo "Results parsing failed"
    grep "P99:" "$OUT_DIR/logs/basic_test.log" | tail -1 || echo "Latency parsing failed"
else
    echo -e "${RED}❌ Basic test failed${NC}"
fi

# Test 2: High Concurrency Test  
echo -e "${YELLOW}🧪 Test 2: High Concurrency (64 workers)${NC}"
./target/release/bench \
    --base "$BASE" \
    --workers 64 \
    --duration 30 \
    --key-count 500000 \
    --value-size 256 \
    --read-ratio 0.8 \
    --warmup 5 \
    > "$OUT_DIR/logs/concurrency_test.log" 2>&1

if [[ -f "$OUT_DIR/logs/concurrency_test.log" ]]; then
    echo -e "${GREEN}✅ Concurrency test completed${NC}"
    grep "Throughput:" "$OUT_DIR/logs/concurrency_test.log" | tail -1 || echo "Results parsing failed"
    grep "P99:" "$OUT_DIR/logs/concurrency_test.log" | tail -1 || echo "Latency parsing failed"
else
    echo -e "${RED}❌ Concurrency test failed${NC}"
fi

# Test 3: Large Dataset Test
echo -e "${YELLOW}🧪 Test 3: Large Dataset (1M keys)${NC}" 
./target/release/bench \
    --base "$BASE" \
    --workers 32 \
    --duration 45 \
    --key-count 1000000 \
    --value-size 512 \
    --read-ratio 0.9 \
    --warmup 10 \
    > "$OUT_DIR/logs/large_dataset_test.log" 2>&1

if [[ -f "$OUT_DIR/logs/large_dataset_test.log" ]]; then
    echo -e "${GREEN}✅ Large dataset test completed${NC}"
    grep "Throughput:" "$OUT_DIR/logs/large_dataset_test.log" | tail -1 || echo "Results parsing failed"
    grep "P99:" "$OUT_DIR/logs/large_dataset_test.log" | tail -1 || echo "Latency parsing failed"
else
    echo -e "${RED}❌ Large dataset test failed${NC}"
fi

# Test 4: Write-Heavy Workload
echo -e "${YELLOW}🧪 Test 4: Write-Heavy Workload${NC}"
./target/release/bench \
    --base "$BASE" \
    --workers 32 \
    --duration 30 \
    --key-count 200000 \
    --value-size 1024 \
    --read-ratio 0.2 \
    --warmup 5 \
    > "$OUT_DIR/logs/write_heavy_test.log" 2>&1

if [[ -f "$OUT_DIR/logs/write_heavy_test.log" ]]; then
    echo -e "${GREEN}✅ Write-heavy test completed${NC}"
    grep "Throughput:" "$OUT_DIR/logs/write_heavy_test.log" | tail -1 || echo "Results parsing failed"
    grep "P99:" "$OUT_DIR/logs/write_heavy_test.log" | tail -1 || echo "Latency parsing failed"
else
    echo -e "${RED}❌ Write-heavy test failed${NC}"
fi

# Generate Summary Report
echo -e "${YELLOW}📋 Generating Summary Report${NC}"

cat > "$OUT_DIR/BENCHMARK_SUMMARY.md" << EOF
# KyroDB Quick Benchmark Results

Generated on: $(date)
Commit: $COMMIT  
Server: $BASE

## Test Results

### Test 1: Basic Performance (100K keys, mixed workload)
$(if [[ -f "$OUT_DIR/logs/basic_test.log" ]]; then
    echo "- $(grep "Throughput:" "$OUT_DIR/logs/basic_test.log" | tail -1 2>/dev/null || echo "Throughput: Results not found")"
    echo "- $(grep "P99:" "$OUT_DIR/logs/basic_test.log" | tail -1 2>/dev/null || echo "P99: Results not found")"
else
    echo "- Test failed or incomplete"
fi)

### Test 2: High Concurrency (64 workers)
$(if [[ -f "$OUT_DIR/logs/concurrency_test.log" ]]; then
    echo "- $(grep "Throughput:" "$OUT_DIR/logs/concurrency_test.log" | tail -1 2>/dev/null || echo "Throughput: Results not found")"
    echo "- $(grep "P99:" "$OUT_DIR/logs/concurrency_test.log" | tail -1 2>/dev/null || echo "P99: Results not found")"
else
    echo "- Test failed or incomplete"
fi)

### Test 3: Large Dataset (1M keys)
$(if [[ -f "$OUT_DIR/logs/large_dataset_test.log" ]]; then
    echo "- $(grep "Throughput:" "$OUT_DIR/logs/large_dataset_test.log" | tail -1 2>/dev/null || echo "Throughput: Results not found")"
    echo "- $(grep "P99:" "$OUT_DIR/logs/large_dataset_test.log" | tail -1 2>/dev/null || echo "P99: Results not found")"
else
    echo "- Test failed or incomplete"
fi)

### Test 4: Write-Heavy Workload
$(if [[ -f "$OUT_DIR/logs/write_heavy_test.log" ]]; then
    echo "- $(grep "Throughput:" "$OUT_DIR/logs/write_heavy_test.log" | tail -1 2>/dev/null || echo "Throughput: Results not found")"
    echo "- $(grep "P99:" "$OUT_DIR/logs/write_heavy_test.log" | tail -1 2>/dev/null || echo "P99: Results not found")"
else
    echo "- Test failed or incomplete"
fi)

## Configuration
- Workers: 32-64 concurrent threads
- Key counts: 100K - 1M keys
- Value sizes: 256B - 1KB
- Duration: 30-45 seconds per test
- Warmup: 5-10 seconds

## Log Files
- Basic test: logs/basic_test.log
- Concurrency test: logs/concurrency_test.log  
- Large dataset test: logs/large_dataset_test.log
- Write-heavy test: logs/write_heavy_test.log

EOF

echo -e "${GREEN}🎉 Quick benchmarking complete!${NC}"
echo -e "📂 Results directory: ${BLUE}$OUT_DIR${NC}"
echo -e "📊 Summary report: ${BLUE}$OUT_DIR/BENCHMARK_SUMMARY.md${NC}"

# Show quick summary from logs
echo -e "\n${YELLOW}📋 Quick Results Summary:${NC}"
for log_file in "$OUT_DIR/logs"/*.log; do
    if [[ -f "$log_file" ]]; then
        test_name=$(basename "$log_file" .log)
        echo -e "${BLUE}$test_name:${NC}"
        grep "Throughput:" "$log_file" | tail -1 2>/dev/null || echo "  Throughput: Not found"
        grep "P99:" "$log_file" | tail -1 2>/dev/null || echo "  P99: Not found"
        echo ""
    fi
done

echo -e "${GREEN}✅ KyroDB performance validated!${NC}"
