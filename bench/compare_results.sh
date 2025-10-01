#!/usr/bin/env bash
#
# compare_results.sh
# Compare benchmark results before and after SIMD fix
#

set -euo pipefail

RESULTS_DIR="bench/results"

echo "========================================"
echo "BENCHMARK COMPARISON: BEFORE vs AFTER"
echo "========================================"
echo ""

# Find the broken run results
BROKEN_RUN="complete_6877bad_20251001_131132"
BROKEN_PATH="$RESULTS_DIR/$BROKEN_RUN"

if [ ! -d "$BROKEN_PATH" ]; then
    echo "❌ Cannot find broken run results at: $BROKEN_PATH"
    exit 1
fi

# Find the latest run results
LATEST_RUN=$(ls -t "$RESULTS_DIR" | grep "complete_" | head -1)
LATEST_PATH="$RESULTS_DIR/$LATEST_RUN"

if [ "$BROKEN_RUN" == "$LATEST_RUN" ]; then
    echo "⚠️  No new results found. Run benchmarks first:"
    echo "   cd /data/KyroDB"
    echo "   ./bench/run_complete_suite.sh"
    exit 1
fi

echo "Comparing:"
echo "  BEFORE (broken): $BROKEN_RUN"
echo "  AFTER  (fixed):  $LATEST_RUN"
echo ""

# Function to extract metrics from CSV
extract_metric() {
    local file=$1
    local field=$2
    tail -1 "$file" | cut -d',' -f"$field"
}

# Compare RMI results
echo "=========================================="
echo "RMI COMPARISON TEST"
echo "=========================================="

if [ -f "$BROKEN_PATH/comparison_rmi.csv" ] && [ -f "$LATEST_PATH/comparison_rmi.csv" ]; then
    echo ""
    echo "BEFORE (broken):"
    cat "$BROKEN_PATH/comparison_rmi.csv" | column -t -s','
    
    echo ""
    echo "AFTER (fixed):"
    cat "$LATEST_PATH/comparison_rmi.csv" | column -t -s','
    
    # Calculate improvement
    BEFORE_OPS=$(extract_metric "$BROKEN_PATH/comparison_rmi.csv" 5)
    AFTER_OPS=$(extract_metric "$LATEST_PATH/comparison_rmi.csv" 5)
    
    echo ""
    echo "Successful operations:"
    echo "  BEFORE: $BEFORE_OPS (100% failure)"
    echo "  AFTER:  $AFTER_OPS"
    
    if [ "$AFTER_OPS" -gt 0 ]; then
        echo "  ✅ RMI is now working!"
    else
        echo "  ❌ RMI still broken"
    fi
else
    echo "❌ Missing RMI comparison files"
fi

# Compare HTTP read-heavy
echo ""
echo "=========================================="
echo "HTTP READ-HEAVY TEST"
echo "=========================================="

if [ -f "$BROKEN_PATH/http_read_heavy.csv" ] && [ -f "$LATEST_PATH/http_read_heavy.csv" ]; then
    BEFORE_THROUGHPUT=$(extract_metric "$BROKEN_PATH/http_read_heavy.csv" 8)
    AFTER_THROUGHPUT=$(extract_metric "$LATEST_PATH/http_read_heavy.csv" 8)
    
    BEFORE_P99=$(extract_metric "$BROKEN_PATH/http_read_heavy.csv" 13)
    AFTER_P99=$(extract_metric "$LATEST_PATH/http_read_heavy.csv" 13)
    
    echo "Throughput (ops/sec):"
    echo "  BEFORE: $BEFORE_THROUGHPUT"
    echo "  AFTER:  $AFTER_THROUGHPUT"
    
    if [ "$AFTER_THROUGHPUT" -gt "$BEFORE_THROUGHPUT" ]; then
        IMPROVEMENT=$((AFTER_THROUGHPUT * 100 / BEFORE_THROUGHPUT - 100))
        echo "  ✅ Improved by ${IMPROVEMENT}%"
    else
        REGRESSION=$((BEFORE_THROUGHPUT * 100 / AFTER_THROUGHPUT - 100))
        echo "  ⚠️  Regressed by ${REGRESSION}%"
    fi
    
    echo ""
    echo "Latency P99 (ms):"
    echo "  BEFORE: $BEFORE_P99"
    echo "  AFTER:  $AFTER_P99"
    
    if (( $(echo "$AFTER_P99 < $BEFORE_P99" | bc -l) )); then
        echo "  ✅ Latency improved"
    else
        echo "  ⚠️  Latency regressed"
    fi
else
    echo "❌ Missing HTTP read-heavy files"
fi

# Compare HTTP write-heavy
echo ""
echo "=========================================="
echo "HTTP WRITE-HEAVY TEST"
echo "=========================================="

if [ -f "$BROKEN_PATH/http_write_heavy.csv" ] && [ -f "$LATEST_PATH/http_write_heavy.csv" ]; then
    BEFORE_THROUGHPUT=$(extract_metric "$BROKEN_PATH/http_write_heavy.csv" 8)
    AFTER_THROUGHPUT=$(extract_metric "$LATEST_PATH/http_write_heavy.csv" 8)
    
    echo "Throughput (ops/sec):"
    echo "  BEFORE: $BEFORE_THROUGHPUT"
    echo "  AFTER:  $AFTER_THROUGHPUT"
    
    if [ "$AFTER_THROUGHPUT" -gt "$BEFORE_THROUGHPUT" ]; then
        IMPROVEMENT=$((AFTER_THROUGHPUT * 100 / BEFORE_THROUGHPUT - 100))
        echo "  ✅ Improved by ${IMPROVEMENT}%"
    fi
else
    echo "❌ Missing HTTP write-heavy files"
fi

# Compare HTTP mixed
echo ""
echo "=========================================="
echo "HTTP MIXED WORKLOAD TEST"
echo "=========================================="

if [ -f "$BROKEN_PATH/http_mixed.csv" ] && [ -f "$LATEST_PATH/http_mixed.csv" ]; then
    BEFORE_THROUGHPUT=$(extract_metric "$BROKEN_PATH/http_mixed.csv" 8)
    AFTER_THROUGHPUT=$(extract_metric "$LATEST_PATH/http_mixed.csv" 8)
    
    BEFORE_P99=$(extract_metric "$BROKEN_PATH/http_mixed.csv" 13)
    AFTER_P99=$(extract_metric "$LATEST_PATH/http_mixed.csv" 13)
    
    echo "Throughput (ops/sec):"
    echo "  BEFORE: $BEFORE_THROUGHPUT"
    echo "  AFTER:  $AFTER_THROUGHPUT"
    
    if [ "$AFTER_THROUGHPUT" -gt "$BEFORE_THROUGHPUT" ]; then
        IMPROVEMENT=$((AFTER_THROUGHPUT * 100 / BEFORE_THROUGHPUT - 100))
        echo "  ✅ Improved by ${IMPROVEMENT}%"
    fi
    
    echo ""
    echo "Latency P99 (ms):"
    echo "  BEFORE: $BEFORE_P99"
    echo "  AFTER:  $AFTER_P99"
else
    echo "❌ Missing HTTP mixed files"
fi

# Overall summary
echo ""
echo "=========================================="
echo "OVERALL SUMMARY"
echo "=========================================="
echo ""

# Check if we hit targets
if [ -f "$LATEST_PATH/http_read_heavy.csv" ]; then
    THROUGHPUT=$(extract_metric "$LATEST_PATH/http_read_heavy.csv" 8)
    P99=$(extract_metric "$LATEST_PATH/http_read_heavy.csv" 13)
    
    echo "Performance vs Targets:"
    echo ""
    echo "Read Throughput:"
    echo "  Target:  100,000 ops/sec"
    echo "  Actual:  $THROUGHPUT ops/sec"
    
    if [ "$THROUGHPUT" -ge 100000 ]; then
        echo "  ✅ Target achieved!"
    elif [ "$THROUGHPUT" -ge 50000 ]; then
        PERCENT=$((THROUGHPUT * 100 / 100000))
        echo "  🟡 ${PERCENT}% of target (good progress)"
    else
        PERCENT=$((THROUGHPUT * 100 / 100000))
        echo "  ⚠️  ${PERCENT}% of target (needs more work)"
    fi
    
    echo ""
    echo "Latency P99:"
    echo "  Target:  <5ms"
    echo "  Actual:  ${P99}ms"
    
    if (( $(echo "$P99 < 5" | bc -l) )); then
        echo "  ✅ Target achieved!"
    elif (( $(echo "$P99 < 10" | bc -l) )); then
        echo "  🟡 Close to target"
    else
        echo "  ⚠️  Needs improvement"
    fi
fi

echo ""
echo "Next steps:"
echo "  1. Review detailed results in: $LATEST_PATH/"
echo "  2. Check server logs for SIMD usage"
echo "  3. Profile with perf to verify SIMD execution"
echo "  4. Iterate on remaining bottlenecks"
echo ""
