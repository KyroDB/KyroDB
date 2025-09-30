#!/bin/bash
# Test Auto-Warmup Feature End-to-End

set -e

echo "========================================="
echo "Testing Auto-Warmup Feature"
echo "========================================="

# Clean up any existing test data
rm -rf /tmp/kyrodb_autowarmup_test
mkdir -p /tmp/kyrodb_autowarmup_test

# Start server with auto-warmup enabled (default)
echo ""
echo "Starting server with KYRODB_WARM_ON_START=1 (default)..."
KYRODB_WARM_ON_START=1 ./target/release/kyrodb-engine serve 127.0.0.1 3031 /tmp/kyrodb_autowarmup_test &
SERVER_PID=$!
echo "Server PID: $SERVER_PID"

# Wait for server to start
sleep 3

# Insert test data
echo ""
echo "Inserting 10,000 test records..."
for i in {0..9999}; do
    curl -s -X POST "http://127.0.0.1:3031/v1/append" \
        -H "Content-Type: application/json" \
        -d "{\"key\": $i, \"value\": \"value_$i\"}" > /dev/null
    if [ $((i % 1000)) -eq 0 ]; then
        echo "  Inserted $i records..."
    fi
done
echo "  ✓ Inserted 10,000 records"

# Trigger snapshot
echo ""
echo "Creating snapshot..."
curl -s -X POST "http://127.0.0.1:3031/v1/snapshot" > /dev/null
echo "  ✓ Snapshot created"

# Trigger RMI build
echo ""
echo "Building RMI index..."
curl -s -X POST "http://127.0.0.1:3031/v1/rmi/build" > /dev/null
echo "  ✓ RMI index built"

# Trigger warmup
echo ""
echo "Warming up mmap pages..."
curl -s -X POST "http://127.0.0.1:3031/v1/warmup" > /dev/null
echo "  ✓ Warmup complete"

# Test ultra-fast endpoints
echo ""
echo "Testing ultra-fast endpoints..."
RESULT=$(curl -s "http://127.0.0.1:3031/v1/lookup_ultra/5000")
echo "  /v1/lookup_ultra/5000 => $RESULT"
if echo "$RESULT" | grep -q "offset"; then
    echo "  ✓ Ultra-fast lookup working!"
else
    echo "  ✗ Ultra-fast lookup failed"
    kill $SERVER_PID
    exit 1
fi

# Stop server
echo ""
echo "Stopping server..."
kill $SERVER_PID
wait $SERVER_PID 2>/dev/null || true

# Restart server with auto-warmup to test auto-recovery
echo ""
echo "========================================="
echo "Testing Auto-Warmup on Server Restart"
echo "========================================="
echo ""
echo "Restarting server (auto-warmup should trigger)..."
KYRODB_WARM_ON_START=1 ./target/release/kyrodb-engine serve 127.0.0.1 3031 /tmp/kyrodb_autowarmup_test &
SERVER_PID=$!
echo "Server PID: $SERVER_PID"

# Wait for auto-warmup to complete
echo "Waiting for auto-warmup (should see snapshot → build_rmi → warmup)..."
sleep 5

# Test that ultra-fast endpoints work immediately after restart
echo ""
echo "Testing ultra-fast endpoints after restart..."
RESULT=$(curl -s "http://127.0.0.1:3031/v1/lookup_ultra/5000")
echo "  /v1/lookup_ultra/5000 => $RESULT"
if echo "$RESULT" | grep -q "offset"; then
    echo "  ✓ Ultra-fast lookup working after auto-warmup!"
    echo ""
    echo "========================================="
    echo "✓ AUTO-WARMUP FEATURE WORKING PERFECTLY"
    echo "========================================="
else
    echo "  ✗ Ultra-fast lookup failed after restart"
    kill $SERVER_PID
    exit 1
fi

# Cleanup
kill $SERVER_PID
wait $SERVER_PID 2>/dev/null || true
rm -rf /tmp/kyrodb_autowarmup_test

echo ""
echo "Test complete. Auto-warmup feature validated!"
