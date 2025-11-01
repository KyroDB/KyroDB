#!/bin/bash
set -e

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║   KyroDB Week 1-4 Performance Test - M4 Mac                   ║"
echo "║   Testing: Query Clustering + Predictive Prefetching          ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Cleanup function
cleanup() {
    echo -e "\n${YELLOW}Cleaning up...${NC}"
    pkill -9 -f kyrodb_server 2>/dev/null || true
    pkill -9 -f validation_enterprise 2>/dev/null || true
}

trap cleanup EXIT

# Step 1: Clean data directory
echo -e "${BLUE}Step 1: Cleaning data directory...${NC}"
rm -rf ./data
mkdir -p ./data
echo -e "${GREEN}✓ Data directory cleaned${NC}\n"

# Step 2: Start server
echo -e "${BLUE}Step 2: Starting KyroDB server with Week 1-4 features...${NC}"
./target/release/kyrodb_server > kyrodb_server.log 2>&1 &
SERVER_PID=$!
echo "Server PID: $SERVER_PID"

# Wait for server to start
echo "Waiting for server to be ready..."
sleep 3

# Check if server is running
if ! ps -p $SERVER_PID > /dev/null; then
    echo -e "${RED}✗ Server failed to start!${NC}"
    cat kyrodb_server.log
    exit 1
fi

# Test health endpoint
if curl -s http://localhost:51051/health > /dev/null; then
    echo -e "${GREEN}✓ Server started successfully${NC}\n"
else
    echo -e "${RED}✗ Server health check failed!${NC}"
    exit 1
fi

# Step 3: Show configuration
echo -e "${BLUE}Step 3: Test Configuration${NC}"
cat performance_test_config.json | grep -E "(duration|target_qps|corpus_size|cache_capacity)" | head -4
echo ""

# Step 4: Run performance test
echo -e "${BLUE}Step 4: Running performance test (30 minutes)...${NC}"
echo "This will test:"
echo "  - 100K document corpus"
echo "  - 5K cache capacity (5% of corpus)"
echo "  - 1000 QPS target (~1.8M queries total)"
echo "  - Week 1-4 features: Auto-tuning, Clustering, Prefetching"
echo ""
echo "Progress will be shown below:"
echo "═══════════════════════════════════════════════════════════════"

./target/release/validation_enterprise --config performance_test_config.json 2>&1 | tee performance_test_output.log

echo ""
echo "═══════════════════════════════════════════════════════════════"

# Step 5: Show results
echo -e "\n${BLUE}Step 5: Test Results${NC}"
echo ""

if [ -f performance_test_m4.json ]; then
    echo -e "${GREEN}✓ Test completed successfully!${NC}\n"
    
    echo "PERFORMANCE METRICS:"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    # Parse results
    LRU_HIT_RATE=$(cat performance_test_m4.json | grep -o '"lru_hit_rate":[0-9.]*' | cut -d':' -f2)
    LEARNED_HIT_RATE=$(cat performance_test_m4.json | grep -o '"learned_hit_rate":[0-9.]*' | cut -d':' -f2)
    IMPROVEMENT=$(cat performance_test_m4.json | grep -o '"hit_rate_improvement":[0-9.]*' | cut -d':' -f2)
    TOTAL_QUERIES=$(cat performance_test_m4.json | grep -o '"total_queries":[0-9]*' | cut -d':' -f2)
    DURATION=$(cat performance_test_m4.json | grep -o '"test_duration_secs":[0-9]*' | cut -d':' -f2)
    
    # Convert to percentages
    LRU_PCT=$(echo "$LRU_HIT_RATE * 100" | bc -l | xargs printf "%.1f")
    LEARNED_PCT=$(echo "$LEARNED_HIT_RATE * 100" | bc -l | xargs printf "%.1f")
    IMPROVEMENT_X=$(echo "$IMPROVEMENT" | xargs printf "%.2f")
    
    echo "Cache Hit Rates:"
    echo "  LRU Baseline:        ${LRU_PCT}%"
    echo "  Learned (Week 1-4):  ${LEARNED_PCT}%"
    echo "  Improvement:         ${IMPROVEMENT_X}×"
    echo ""
    echo "Workload:"
    echo "  Total Queries:       $(echo $TOTAL_QUERIES | sed ':a;s/\B[0-9]\{3\}\>/,&/;ta')"
    echo "  Duration:            ${DURATION}s ($(echo "scale=1; $DURATION/60" | bc)min)"
    echo "  Avg QPS:             $(echo "scale=0; $TOTAL_QUERIES/$DURATION" | bc)"
    echo ""
    
    # Check success criteria
    echo "SUCCESS CRITERIA:"
    if (( $(echo "$LEARNED_HIT_RATE >= 0.60" | bc -l) )); then
        echo -e "  ${GREEN}✓ Learned hit rate ≥ 60%: ${LEARNED_PCT}%${NC}"
    else
        echo -e "  ${YELLOW}⚠ Learned hit rate < 60%: ${LEARNED_PCT}% (target: 60-65%)${NC}"
    fi
    
    if (( $(echo "$IMPROVEMENT >= 2.5" | bc -l) )); then
        echo -e "  ${GREEN}✓ Improvement ≥ 2.5×: ${IMPROVEMENT_X}×${NC}"
    else
        echo -e "  ${YELLOW}⚠ Improvement < 2.5×: ${IMPROVEMENT_X}× (target: 2.5-3.0×)${NC}"
    fi
    
    echo ""
    echo "DETAILED RESULTS:"
    echo "  Full JSON: performance_test_m4.json"
    echo "  CSV Stats: performance_test_m4.csv"
    echo "  Server Log: kyrodb_server.log"
    echo "  Test Log: performance_test_output.log"
    
else
    echo -e "${RED}✗ Test failed - results file not found${NC}"
    echo "Check performance_test_output.log for details"
    exit 1
fi

echo ""
echo -e "${GREEN}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}Test Complete!${NC}"
echo -e "${GREEN}═══════════════════════════════════════════════════════════════${NC}"
