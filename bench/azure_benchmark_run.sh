#!/usr/bin/env bash
# Azure VM Benchmark Execution Script
# Complete server startup and benchmark orchestration

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_DIR="/data/kyrodb_data"
LOG_FILE="/tmp/kyrodb_server.log"
PID_FILE="/tmp/kyrodb.pid"

cleanup() {
    if [[ -f "$PID_FILE" ]]; then
        SERVER_PID=$(cat "$PID_FILE")
        echo -e "${YELLOW}Stopping server (PID: $SERVER_PID)...${NC}"
        kill $SERVER_PID 2>/dev/null || true
        rm -f "$PID_FILE"
    fi
}

trap cleanup EXIT INT TERM

echo -e "${GREEN}═══════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  KyroDB Azure VM Benchmark Execution Suite${NC}"
echo -e "${GREEN}═══════════════════════════════════════════════════${NC}"
echo ""

# Clean previous run
cleanup
rm -rf "$DATA_DIR"
mkdir -p "$DATA_DIR"
chmod 755 "$DATA_DIR"

# Set optimal environment variables
echo -e "${BLUE}Setting environment variables...${NC}"
export KYRODB_WARM_ON_START=1
export KYRODB_DISABLE_HTTP_LOG=1
export KYRODB_GROUP_COMMIT_DELAY_MICROS=100
export KYRODB_FSYNC_POLICY=data
export KYRODB_RMI_TARGET_LEAF=2048
export KYRODB_USE_ADAPTIVE_RMI=true
export RUST_LOG=error

# Start server
echo -e "${BLUE}Starting KyroDB server...${NC}"
cd "$ROOT_DIR"

# Note: --data-dir must come BEFORE serve subcommand
./target/release/kyrodb-engine \
    --data-dir "$DATA_DIR" \
    serve 0.0.0.0 3030 \
    > "$LOG_FILE" 2>&1 &

SERVER_PID=$!
echo $SERVER_PID > "$PID_FILE"
echo -e "Server PID: ${GREEN}$SERVER_PID${NC}"
echo ""

# Wait for startup with health checks
echo -e "${YELLOW}Waiting for server startup...${NC}"
MAX_RETRIES=15
RETRY=0

while [[ $RETRY -lt $MAX_RETRIES ]]; do
    sleep 2
    
    if curl -sf http://127.0.0.1:3030/v1/health > /dev/null 2>&1; then
        echo -e "${GREEN}✅ Server is healthy!${NC}"
        echo ""
        break
    fi
    
    RETRY=$((RETRY + 1))
    echo -e "Health check attempt $RETRY/$MAX_RETRIES..."
done

if [[ $RETRY -eq $MAX_RETRIES ]]; then
    echo -e "${RED}❌ Server failed to start after $MAX_RETRIES attempts${NC}"
    echo ""
    echo -e "${YELLOW}Last 50 lines of server log:${NC}"
    tail -50 "$LOG_FILE"
    exit 1
fi

# Display server info
echo -e "${BLUE}Server Information:${NC}"
echo -e "  URL: http://0.0.0.0:3030"
echo -e "  Logs: $LOG_FILE"
echo -e "  Data: $DATA_DIR"
echo ""

# Show build info
echo -e "${YELLOW}Build Information:${NC}"
curl -s http://127.0.0.1:3030/build_info 2>/dev/null | jq . || echo "Build info not available"
echo ""

# Show health status
echo -e "${YELLOW}Health Status:${NC}"
curl -s http://127.0.0.1:3030/v1/health 2>/dev/null | jq . || echo "Health check failed"
echo ""

# Run benchmarks
echo -e "${GREEN}═══════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  Starting Comprehensive Benchmark Suite${NC}"
echo -e "${GREEN}═══════════════════════════════════════════════════${NC}"
echo ""

cd "$ROOT_DIR"
./bench/run_complete_suite.sh all

BENCHMARK_EXIT=$?

echo ""
echo -e "${GREEN}═══════════════════════════════════════════════════${NC}"
if [[ $BENCHMARK_EXIT -eq 0 ]]; then
    echo -e "${GREEN}  ✅ Benchmarks Complete Successfully!${NC}"
else
    echo -e "${RED}  ❌ Benchmarks Failed (exit code: $BENCHMARK_EXIT)${NC}"
fi
echo -e "${GREEN}═══════════════════════════════════════════════════${NC}"
echo ""

# Show results location
LATEST_RESULT=$(ls -td bench/results/complete_* 2>/dev/null | head -1)
if [[ -n "$LATEST_RESULT" ]]; then
    echo -e "${BLUE}Results saved to:${NC} $LATEST_RESULT"
    echo ""
fi

exit $BENCHMARK_EXIT
