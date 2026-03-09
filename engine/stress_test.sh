#!/bin/bash
# Repeated library-test runner
set -e

echo "=== KyroDB Validation: repeated library-test run ==="
echo "Starting at: $(date)"
echo ""

# Test configuration
TOTAL_TEST_RUNS=100

echo "Configuration:"
echo "  Total Test Runs: $TOTAL_TEST_RUNS"
echo ""

# Run comprehensive test suite multiple times
echo "Running test suite $TOTAL_TEST_RUNS times..."
START_TIME=$(date +%s)

for i in $(seq 1 $TOTAL_TEST_RUNS); do
    PROGRESS=$((i * 100 / TOTAL_TEST_RUNS))
    echo -ne "\rProgress: [$i/$TOTAL_TEST_RUNS] ${PROGRESS}% - Running tests..."
    
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
echo " Repeated test iterations completed successfully"
echo "Duration: ${DURATION}s"
THROUGHPUT=$((TOTAL_TEST_RUNS / DURATION))
echo "Throughput: ${THROUGHPUT} test runs/sec"
echo "Final memory check:"
ps aux | grep -E "cargo|rust" | grep -v grep | head -n 5
echo ""
echo "=== VALIDATION COMPLETE ==="
echo "No test failures were observed during this run"
