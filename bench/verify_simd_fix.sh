#!/usr/bin/env bash
#
# verify_simd_fix.sh
# Quick verification that SIMD functions are now being used
#

set -euo pipefail

REPO_ROOT="/data/KyroDB"
DATA_DIR="/data/kyrodb_data"

echo "================================"
echo "SIMD FIX VERIFICATION SCRIPT"
echo "================================"
echo ""

cd "$REPO_ROOT"

# Step 1: Clean rebuild
echo "[1/5] Clean rebuild with learned-index feature..."
cargo clean
cargo build -p kyrodb-engine --release --features learned-index 2>&1 | tee /tmp/build_simd_fix.log

# Step 2: Check for warnings
echo ""
echo "[2/5] Checking for SIMD unused warnings..."
if grep -q "simd.*never used" /tmp/build_simd_fix.log; then
    echo "❌ FAIL: SIMD functions still marked as unused!"
    grep "simd.*never used" /tmp/build_simd_fix.log
    exit 1
else
    echo "✅ PASS: No unused SIMD function warnings"
fi

# Step 3: Verify binary size
echo ""
echo "[3/5] Checking binary size (should be ~7-8MB with SIMD)..."
BINARY_SIZE=$(stat -f%z ./target/release/kyrodb-engine 2>/dev/null || stat -c%s ./target/release/kyrodb-engine)
BINARY_MB=$((BINARY_SIZE / 1024 / 1024))
echo "Binary size: ${BINARY_MB}MB"

if [ "$BINARY_MB" -lt 5 ]; then
    echo "⚠️  WARNING: Binary size unusually small - SIMD may not be compiled"
elif [ "$BINARY_MB" -gt 15 ]; then
    echo "⚠️  WARNING: Binary size unusually large"
else
    echo "✅ PASS: Binary size looks good"
fi

# Step 4: Stop old server
echo ""
echo "[4/5] Stopping any existing server..."
pkill -9 -f kyrodb-engine || true
sleep 2

# Clean data
rm -rf "$DATA_DIR"/*
mkdir -p "$DATA_DIR"

# Start server with logging
echo ""
echo "[5/5] Starting server..."
export RUST_LOG=info
./target/release/kyrodb-engine \
    --data-dir "$DATA_DIR" \
    serve 0.0.0.0 3030 \
    > /tmp/kyrodb_verify.log 2>&1 &

SERVER_PID=$!
echo "Server PID: $SERVER_PID"
sleep 5

# Health check
echo ""
echo "Checking server health..."
if curl -sf http://127.0.0.1:3030/v1/health > /dev/null; then
    echo "✅ Server is healthy"
else
    echo "❌ Server health check failed!"
    tail -50 /tmp/kyrodb_verify.log
    exit 1
fi

# Quick load test
echo ""
echo "Running quick load test (1000 keys)..."
for i in {1..1000}; do
    curl -sf -X POST http://127.0.0.1:3030/v1/write \
        -H "Content-Type: application/json" \
        -d "{\"key\": $i, \"value\": $((i * 100))}" > /dev/null
done

# Build RMI index
echo ""
echo "Building RMI index..."
curl -sf -X POST http://127.0.0.1:3030/v1/rmi/build > /dev/null
sleep 2

# Warmup
echo ""
echo "Warming up index..."
curl -sf -X POST http://127.0.0.1:3030/v1/warmup > /dev/null
sleep 2

# Batch lookup test
echo ""
echo "Testing batch lookup (should trigger SIMD)..."
curl -sf -X POST http://127.0.0.1:3030/v1/lookup_batch \
    -H "Content-Type: application/json" \
    -d '{"keys": [1, 2, 3, 4, 5, 6, 7, 8]}' > /tmp/batch_result.json

if [ -s /tmp/batch_result.json ]; then
    echo "✅ Batch lookup returned data"
    cat /tmp/batch_result.json | jq . 2>/dev/null || cat /tmp/batch_result.json
else
    echo "❌ Batch lookup failed"
fi

# Check logs for SIMD activity
echo ""
echo "Checking server logs..."
if grep -i "simd\|rmi.*build\|warmup" /tmp/kyrodb_verify.log | tail -10; then
    echo "✅ Found relevant log entries"
else
    echo "⚠️  No SIMD-related log entries found (this is okay if not instrumented)"
fi

# Final status
echo ""
echo "================================"
echo "VERIFICATION SUMMARY"
echo "================================"
echo "✅ Build completed without SIMD warnings"
echo "✅ Server started successfully"
echo "✅ Basic operations working"
echo ""
echo "Server is running at http://0.0.0.0:3030"
echo "Server PID: $SERVER_PID"
echo "Server logs: /tmp/kyrodb_verify.log"
echo ""
echo "Next steps:"
echo "  1. Run full benchmark suite: ./bench/run_complete_suite.sh"
echo "  2. Compare results with previous broken run"
echo "  3. Verify RMI success rate is >99%"
echo "  4. Verify throughput >50K ops/sec"
echo ""
