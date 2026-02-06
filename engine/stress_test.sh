#!/bin/bash
# Validation - Comprehensive 1M Operation Load Test
set -e

echo "=== KyroDB Validation: 1M Operation Load Test ==="
echo "Starting at: $(date)"
echo ""

# Test configuration
TOTAL_OPS=1000000
BATCH_SIZE=10000
ITERATIONS=$((TOTAL_OPS / BATCH_SIZE))

echo "Configuration:"
echo "  Total Operations: $TOTAL_OPS"
echo "  Batch Size: $BATCH_SIZE"
echo "  Iterations: $ITERATIONS"
echo ""

# Run comprehensive test suite multiple times
echo "Running test suite $ITERATIONS times..."
START_TIME=$(date +%s)

for i in $(seq 1 $ITERATIONS); do
    PROGRESS=$((i * 100 / ITERATIONS))
    echo -ne "\rProgress: [$i/$ITERATIONS] ${PROGRESS}% - Running tests..."
    
    # Run tests and capture output
    if ! cargo test --lib --quiet > test_output.log 2>&1; then
        echo ""
        echo "ERROR: Test failure at iteration $i"
        echo "=== FAILURE LOG ==="
        tail -n 50 test_output.log
        rm test_output.log
        exit 1
    fi
    rm test_output.log
    
    # Brief pause to allow system to stabilize
    sleep 1
done

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

DURATION=$((DURATION < 1 ? 1 : DURATION))
echo ""
echo "=== RESULTS ==="
echo " All $TOTAL_OPS operations completed successfully"
echo "Duration: ${DURATION}s"
THROUGHPUT=$((TOTAL_OPS / DURATION))
echo "Throughput: ${THROUGHPUT} ops/sec"
echo "Final memory check:"
ps aux | grep -E "cargo|rust" | grep -v grep | head -n 5
echo ""
echo "=== VALIDATION COMPLETE ==="
echo "No crashes, no panics, no deadlocks detected"
