#!/bin/bash
# KyroDB Stress Test Runner
# Runs comprehensive stress tests and reports results

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
RESULTS_FILE="${RESULTS_DIR}/stress_test_${TIMESTAMP}.log"

echo -e "${BLUE}============================================${NC}"
echo -e "${BLUE}  KyroDB Stress Test Suite${NC}"
echo -e "${BLUE}============================================${NC}"
echo ""

# Create results directory
mkdir -p "${RESULTS_DIR}"

# Function to run a test category
run_category() {
    local category=$1
    local description=$2
    
    echo -e "${YELLOW}▶ Running ${description}...${NC}"
    
    if cargo test --lib --features learned-index --release \
        "test::stress::${category}" \
        -- --test-threads=4 --nocapture 2>&1 | tee -a "${RESULTS_FILE}"; then
        echo -e "${GREEN}✓ ${description} PASSED${NC}"
        return 0
    else
        echo -e "${RED}✗ ${description} FAILED${NC}"
        return 1
    fi
}

# Function to run a single test
run_test() {
    local test_name=$1
    local description=$2
    
    echo -e "${YELLOW}▶ Running ${description}...${NC}"
    
    if cargo test --lib --features learned-index --release \
        "${test_name}" \
        -- --nocapture 2>&1 | tee -a "${RESULTS_FILE}"; then
        echo -e "${GREEN}✓ ${description} PASSED${NC}"
        return 0
    else
        echo -e "${RED}✗ ${description} FAILED${NC}"
        return 1
    fi
}

# Track results
PASSED=0
FAILED=0
CATEGORIES=()

echo "Starting stress tests at $(date)" | tee "${RESULTS_FILE}"
echo "" | tee -a "${RESULTS_FILE}"

# Run test categories
echo -e "${BLUE}===== Quick Stress Tests (Default) =====${NC}" | tee -a "${RESULTS_FILE}"
echo "" | tee -a "${RESULTS_FILE}"

if run_category "concurrent_operations" "Concurrent Operations Tests"; then
    ((PASSED++))
else
    ((FAILED++))
fi
CATEGORIES+=("concurrent_operations")
echo "" | tee -a "${RESULTS_FILE}"

if run_category "large_dataset" "Large Dataset Tests (non-ignored)"; then
    ((PASSED++))
else
    ((FAILED++))
fi
CATEGORIES+=("large_dataset")
echo "" | tee -a "${RESULTS_FILE}"

if run_category "memory_pressure" "Memory Pressure Tests"; then
    ((PASSED++))
else
    ((FAILED++))
fi
CATEGORIES+=("memory_pressure")
echo "" | tee -a "${RESULTS_FILE}"

if run_category "lock_contention" "Lock Contention Tests"; then
    ((PASSED++))
else
    ((FAILED++))
fi
CATEGORIES+=("lock_contention")
echo "" | tee -a "${RESULTS_FILE}"

if run_category "rmi_stress" "RMI Stress Tests"; then
    ((PASSED++))
else
    ((FAILED++))
fi
CATEGORIES+=("rmi_stress")
echo "" | tee -a "${RESULTS_FILE}"

if run_category "recovery_stress" "Recovery Stress Tests"; then
    ((PASSED++))
else
    ((FAILED++))
fi
CATEGORIES+=("recovery_stress")
echo "" | tee -a "${RESULTS_FILE}"

if run_category "endurance" "Endurance Tests (non-ignored)"; then
    ((PASSED++))
else
    ((FAILED++))
fi
CATEGORIES+=("endurance")
echo "" | tee -a "${RESULTS_FILE}"

# Summary
echo "" | tee -a "${RESULTS_FILE}"
echo -e "${BLUE}============================================${NC}" | tee -a "${RESULTS_FILE}"
echo -e "${BLUE}  Stress Test Summary${NC}" | tee -a "${RESULTS_FILE}"
echo -e "${BLUE}============================================${NC}" | tee -a "${RESULTS_FILE}"
echo "" | tee -a "${RESULTS_FILE}"
echo "Completed at: $(date)" | tee -a "${RESULTS_FILE}"
echo "Categories tested: ${#CATEGORIES[@]}" | tee -a "${RESULTS_FILE}"
echo -e "Results: ${GREEN}${PASSED} passed${NC}, ${RED}${FAILED} failed${NC}" | tee -a "${RESULTS_FILE}"
echo "" | tee -a "${RESULTS_FILE}"

if [ $FAILED -eq 0 ]; then
    echo -e "${GREEN}✓ All stress test categories PASSED!${NC}" | tee -a "${RESULTS_FILE}"
    echo "" | tee -a "${RESULTS_FILE}"
    echo "Results saved to: ${RESULTS_FILE}"
    exit 0
else
    echo -e "${RED}✗ Some stress tests FAILED. Check logs for details.${NC}" | tee -a "${RESULTS_FILE}"
    echo "" | tee -a "${RESULTS_FILE}"
    echo "Results saved to: ${RESULTS_FILE}"
    exit 1
fi
