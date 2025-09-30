#!/bin/bash
# KyroDB Long-Running Stress Test Runner
# Runs long-running (ignored) stress tests for comprehensive validation

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RESULTS_DIR="${PROJECT_ROOT}/bench/results/stress_tests"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULTS_FILE="${RESULTS_DIR}/long_stress_test_${TIMESTAMP}.log"

echo -e "${BLUE}============================================${NC}"
echo -e "${BLUE}  KyroDB Long-Running Stress Tests${NC}"
echo -e "${BLUE}  (This will take 1-3 hours)${NC}"
echo -e "${BLUE}============================================${NC}"
echo ""

# Create results directory
mkdir -p "${RESULTS_DIR}"

# Function to run a long test
run_long_test() {
    local test_name=$1
    local description=$2
    local estimated_time=$3
    
    echo -e "${YELLOW}▶ Running ${description}...${NC}"
    echo -e "${YELLOW}  Estimated time: ${estimated_time}${NC}"
    
    START_TIME=$(date +%s)
    
    if cargo test --lib --features learned-index --release \
        "${test_name}" \
        -- --ignored --nocapture 2>&1 | tee -a "${RESULTS_FILE}"; then
        END_TIME=$(date +%s)
        DURATION=$((END_TIME - START_TIME))
        echo -e "${GREEN}✓ ${description} PASSED (${DURATION}s)${NC}"
        return 0
    else
        END_TIME=$(date +%s)
        DURATION=$((END_TIME - START_TIME))
        echo -e "${RED}✗ ${description} FAILED (${DURATION}s)${NC}"
        return 1
    fi
}

# Track results
PASSED=0
FAILED=0
TESTS=()

echo "Starting long-running stress tests at $(date)" | tee "${RESULTS_FILE}"
echo "" | tee -a "${RESULTS_FILE}"

# Warning
echo -e "${YELLOW}⚠ WARNING: These tests will run for several hours.${NC}"
echo -e "${YELLOW}⚠ Ensure stable power and network connection.${NC}"
echo -e "${YELLOW}⚠ Monitor system resources during execution.${NC}"
echo ""
read -p "Press Enter to continue or Ctrl+C to cancel..."
echo "" | tee -a "${RESULTS_FILE}"

# Run long tests
echo -e "${BLUE}===== Large Dataset Tests =====${NC}" | tee -a "${RESULTS_FILE}"
echo "" | tee -a "${RESULTS_FILE}"

if run_long_test "test_1_million_keys" "1 Million Keys Test" "~2-5 minutes"; then
    ((PASSED++))
else
    ((FAILED++))
fi
TESTS+=("1M_keys")
echo "" | tee -a "${RESULTS_FILE}"

if run_long_test "test_10_million_keys" "10 Million Keys Test" "~20-40 minutes"; then
    ((PASSED++))
else
    ((FAILED++))
fi
TESTS+=("10M_keys")
echo "" | tee -a "${RESULTS_FILE}"

if run_long_test "test_very_large_values_10kb" "Very Large Values Test" "~5-10 minutes"; then
    ((PASSED++))
else
    ((FAILED++))
fi
TESTS+=("large_values")
echo "" | tee -a "${RESULTS_FILE}"

# Run endurance tests
echo -e "${BLUE}===== Endurance Tests =====${NC}" | tee -a "${RESULTS_FILE}"
echo "" | tee -a "${RESULTS_FILE}"

if run_long_test "test_sustained_write_load_1hour" "1 Hour Sustained Write Load" "~60 minutes"; then
    ((PASSED++))
else
    ((FAILED++))
fi
TESTS+=("1hour_write")
echo "" | tee -a "${RESULTS_FILE}"

if run_long_test "test_sustained_mixed_30min" "30 Minute Mixed Workload" "~30 minutes"; then
    ((PASSED++))
else
    ((FAILED++))
fi
TESTS+=("30min_mixed")
echo "" | tee -a "${RESULTS_FILE}"

# Summary
echo "" | tee -a "${RESULTS_FILE}"
echo -e "${BLUE}============================================${NC}" | tee -a "${RESULTS_FILE}"
echo -e "${BLUE}  Long-Running Stress Test Summary${NC}" | tee -a "${RESULTS_FILE}"
echo -e "${BLUE}============================================${NC}" | tee -a "${RESULTS_FILE}"
echo "" | tee -a "${RESULTS_FILE}"
echo "Completed at: $(date)" | tee -a "${RESULTS_FILE}"
echo "Tests run: ${#TESTS[@]}" | tee -a "${RESULTS_FILE}"
echo -e "Results: ${GREEN}${PASSED} passed${NC}, ${RED}${FAILED} failed${NC}" | tee -a "${RESULTS_FILE}"
echo "" | tee -a "${RESULTS_FILE}"

# Detailed results
echo "Test Details:" | tee -a "${RESULTS_FILE}"
for test in "${TESTS[@]}"; do
    echo "  - ${test}" | tee -a "${RESULTS_FILE}"
done
echo "" | tee -a "${RESULTS_FILE}"

if [ $FAILED -eq 0 ]; then
    echo -e "${GREEN}✓ All long-running stress tests PASSED!${NC}" | tee -a "${RESULTS_FILE}"
    echo "" | tee -a "${RESULTS_FILE}"
    echo "Results saved to: ${RESULTS_FILE}"
    exit 0
else
    echo -e "${RED}✗ Some long-running stress tests FAILED. Check logs for details.${NC}" | tee -a "${RESULTS_FILE}"
    echo "" | tee -a "${RESULTS_FILE}"
    echo "Results saved to: ${RESULTS_FILE}"
    exit 1
fi
