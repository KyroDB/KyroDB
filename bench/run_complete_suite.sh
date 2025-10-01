#!/usr/bin/env bash
set -euo pipefail

# 🔥 KyroDB Complete Performance Suite
# Tests: Raw Engine → HTTP Layer → Integration → Comparison

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Configuration
BASE_URL=${KYRODB_BENCH_URL:-http://127.0.0.1:3030}
PHASE=${1:-all}
WORKERS=${WORKERS:-64}
DURATION=${DURATION:-30}
WARMUP=${WARMUP:-10}

COMMIT=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTPUT_DIR="$ROOT_DIR/bench/results/complete_${COMMIT}_${TIMESTAMP}"

mkdir -p "$OUTPUT_DIR"

echo -e "${BLUE}🔥 KyroDB Complete Performance Suite${NC}"
echo -e "${BLUE}=====================================${NC}"
echo -e "Commit: ${COMMIT}"
echo -e "Phase: ${PHASE}"
echo -e "Server: ${BASE_URL}"
echo -e "Output: ${OUTPUT_DIR}"
echo ""

# Health check
echo -e "${YELLOW}🏥 Checking server health...${NC}"
if ! curl -s -f "${BASE_URL}/v1/health" > /dev/null 2>&1; then
    echo -e "${RED}❌ Server not responding at ${BASE_URL}${NC}"
    echo -e "${YELLOW}💡 Start the server with:${NC}"
    echo -e "   cd ${ROOT_DIR}"
    echo -e "   cargo build -p kyrodb-engine --release --features learned-index"
    echo -e "   ./target/release/kyrodb-engine serve 127.0.0.1 3030"
    exit 1
fi
echo -e "${GREEN}✅ Server is healthy${NC}"
echo ""

run_phase() {
    local phase=$1
    local description=$2
    
    echo -e "${YELLOW}🚀 ${description}${NC}"
    echo ""
    
    case "$phase" in
        micro)
            # Run Criterion microbenchmarks
            cd "$ROOT_DIR"
            echo "  Running WAL throughput benchmarks..."
            cargo bench -p kyrodb-engine --bench wal_throughput --features learned-index \
                > "$OUTPUT_DIR/microbench_wal.log" 2>&1 || true
            
            echo "  Running snapshot performance benchmarks..."
            cargo bench -p kyrodb-engine --bench snapshot_performance --features learned-index \
                > "$OUTPUT_DIR/microbench_snapshot.log" 2>&1 || true
            
            echo "  Running RMI rebuild benchmarks..."
            cargo bench -p kyrodb-engine --bench rmi_rebuild --features learned-index \
                > "$OUTPUT_DIR/microbench_rmi.log" 2>&1 || true
            
            echo "  Running index comparison benchmarks..."
            cargo bench -p kyrodb-engine --bench index_comparison --features learned-index \
                > "$OUTPUT_DIR/microbench_index.log" 2>&1 || true
            
            echo -e "${GREEN}✅ Microbenchmarks complete${NC}"
            ;;
            
        http)
            # Run HTTP layer benchmarks
            cd "$ROOT_DIR"
            
            echo "  Running read-heavy workload..."
            cargo run -p kyrodb-bench --release -- \
                --server-url "$BASE_URL" \
                --phase http \
                --workload read_heavy \
                --workers "$WORKERS" \
                --duration "$DURATION" \
                --warmup "$WARMUP" \
                --format csv \
                > "$OUTPUT_DIR/http_read_heavy.csv" 2>&1 || true
            
            echo "  Running write-heavy workload..."
            cargo run -p kyrodb-bench --release -- \
                --server-url "$BASE_URL" \
                --phase http \
                --workload write_heavy \
                --workers "$WORKERS" \
                --duration "$DURATION" \
                --warmup "$WARMUP" \
                --format csv \
                > "$OUTPUT_DIR/http_write_heavy.csv" 2>&1 || true
            
            echo "  Running mixed workload..."
            cargo run -p kyrodb-bench --release -- \
                --server-url "$BASE_URL" \
                --phase http \
                --workload mixed \
                --workers "$WORKERS" \
                --duration "$DURATION" \
                --warmup "$WARMUP" \
                --format csv \
                > "$OUTPUT_DIR/http_mixed.csv" 2>&1 || true
            
            echo -e "${GREEN}✅ HTTP benchmarks complete${NC}"
            ;;
            
        integration)
            # Run integration benchmarks
            cd "$ROOT_DIR"
            echo "  Running full integration test..."
            cargo run -p kyrodb-bench --release -- \
                --server-url "$BASE_URL" \
                --phase integration \
                --workload mixed \
                --workers "$WORKERS" \
                --duration 60 \
                --warmup "$WARMUP" \
                --key-count 1000000 \
                --value-size 512 \
                --distribution zipf \
                --format csv \
                > "$OUTPUT_DIR/integration_results.csv" 2>&1 || true
            
            echo -e "${GREEN}✅ Integration benchmarks complete${NC}"
            ;;
            
        comparison)
            # Run RMI vs BTree comparison
            echo -e "${YELLOW}⚖️  Building with RMI (learned-index feature)...${NC}"
            cd "$ROOT_DIR"
            cargo build -p kyrodb-engine --release --features learned-index
            
            # Kill any existing server
            pkill -f kyrodb-engine || true
            sleep 2
            
            # Start server with RMI
            echo "  Starting server with RMI..."
            rm -rf /tmp/kyrodb_bench_rmi
            ./target/release/kyrodb-engine serve 127.0.0.1 3030 --data-dir /tmp/kyrodb_bench_rmi > /dev/null 2>&1 &
            SERVER_PID=$!
            sleep 3
            
            # Run benchmarks with RMI
            echo "  Testing RMI performance..."
            cargo run -p kyrodb-bench --release -- \
                --server-url "$BASE_URL" \
                --phase http \
                --workload mixed \
                --workers "$WORKERS" \
                --duration 30 \
                --key-count 1000000 \
                --format csv \
                > "$OUTPUT_DIR/comparison_rmi.csv" 2>&1 || true
            
            # Stop RMI server
            kill $SERVER_PID 2>/dev/null || true
            sleep 2
            
            echo -e "${YELLOW}🌳 Building with BTree (no learned-index)...${NC}"
            cargo build -p kyrodb-engine --release # No learned-index feature
            
            # Start server with BTree
            echo "  Starting server with BTree..."
            rm -rf /tmp/kyrodb_bench_btree
            ./target/release/kyrodb-engine serve 127.0.0.1 3030 --data-dir /tmp/kyrodb_bench_btree > /dev/null 2>&1 &
            SERVER_PID=$!
            sleep 3
            
            # Run benchmarks with BTree
            echo "  Testing BTree performance..."
            cargo run -p kyrodb-bench --release -- \
                --server-url "$BASE_URL" \
                --phase http \
                --workload mixed \
                --workers "$WORKERS" \
                --duration 30 \
                --key-count 1000000 \
                --format csv \
                > "$OUTPUT_DIR/comparison_btree.csv" 2>&1 || true
            
            # Stop BTree server
            kill $SERVER_PID 2>/dev/null || true
            
            echo -e "${GREEN}✅ Comparison benchmarks complete${NC}"
            ;;
    esac
    
    echo ""
}

# Run requested phase(s)
case "$PHASE" in
    all)
        run_phase micro "Phase 1: Raw Engine Microbenchmarks"
        run_phase http "Phase 2: HTTP Layer Benchmarks"
        run_phase integration "Phase 3: Integration Benchmarks"
        run_phase comparison "Phase 4: RMI vs BTree Comparison"
        ;;
    micro)
        run_phase micro "Phase 1: Raw Engine Microbenchmarks"
        ;;
    http)
        run_phase http "Phase 2: HTTP Layer Benchmarks"
        ;;
    integration)
        run_phase integration "Phase 3: Integration Benchmarks"
        ;;
    comparison)
        run_phase comparison "Phase 4: RMI vs BTree Comparison"
        ;;
    *)
        echo -e "${RED}❌ Unknown phase: $PHASE${NC}"
        echo "Valid phases: all, micro, http, integration, comparison"
        exit 1
        ;;
esac

# Generate summary report
echo -e "${YELLOW}📊 Generating summary report...${NC}"

cat > "$OUTPUT_DIR/BENCHMARK_SUMMARY.md" << EOF
# KyroDB Benchmark Results

**Generated:** $(date)
**Commit:** ${COMMIT}
**Phase:** ${PHASE}

## Configuration
- Server: ${BASE_URL}
- Workers: ${WORKERS}
- Duration: ${DURATION}s
- Warmup: ${WARMUP}s

## Results

### Microbenchmarks
- WAL Throughput: \`microbench_wal.log\`
- Snapshot Performance: \`microbench_snapshot.log\`
- RMI Rebuild: \`microbench_rmi.log\`
- Index Comparison: \`microbench_index.log\`

### HTTP Layer Benchmarks
- Read Heavy: \`http_read_heavy.csv\`
- Write Heavy: \`http_write_heavy.csv\`
- Mixed Workload: \`http_mixed.csv\`

### Integration Tests
- Full Integration: \`integration_results.csv\`

### RMI vs BTree Comparison
- RMI Performance: \`comparison_rmi.csv\`
- BTree Performance: \`comparison_btree.csv\`

## Analysis

To view detailed results:
\`\`\`bash
# View microbenchmark results
cat microbench_*.log | grep -E "time:|throughput:"

# View HTTP benchmark results
column -t -s, http_*.csv

# Compare RMI vs BTree
paste comparison_rmi.csv comparison_btree.csv
\`\`\`

## Next Steps
1. Review latency distributions in CSV files
2. Compare RMI vs BTree throughput and latency
3. Analyze scaling behavior with different worker counts
4. Identify optimization opportunities

---
Generated by KyroDB Comprehensive Benchmark Suite
EOF

echo -e "${GREEN}✅ Summary report generated${NC}"
echo ""
echo -e "${BLUE}📂 Results directory: ${OUTPUT_DIR}${NC}"
echo -e "${BLUE}📊 Summary report: ${OUTPUT_DIR}/BENCHMARK_SUMMARY.md${NC}"
echo ""
echo -e "${GREEN}🎉 Benchmark suite complete!${NC}"
