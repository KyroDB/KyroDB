#!/bin/bash
# Week 1-2 Validation Test Script
# Run this to validate 50-55% hit rate target

set -e  # Exit on error

echo "=================================================="
echo "Week 1-2 HSC Auto-Tuning Validation Test"
echo "Target: 50-55% cache hit rate (from 45.1% baseline)"
echo "=================================================="
echo ""

# Configuration
DURATION=3600        # 1 hour test
QPS=1000            # Queries per second
CORPUS_SIZE=10000   # MS MARCO corpus
QUERY_COUNT=71878   # MS MARCO queries
CACHE_STRATEGY="learned"

echo "Test Configuration:"
echo "  Duration: ${DURATION}s (1 hour)"
echo "  Query Rate: ${QPS} QPS"
echo "  Corpus: ${CORPUS_SIZE} documents"
echo "  Queries: ${QUERY_COUNT} total"
echo "  Strategy: ${CACHE_STRATEGY}"
echo ""

# Check if server is running
if ! lsof -i :50051 > /dev/null 2>&1; then
    echo "❌ ERROR: KyroDB server not running on port 50051"
    echo ""
    echo "Start server first:"
    echo "  ./target/release/kyrodb_server serve 127.0.0.1 50051"
    echo ""
    exit 1
fi

echo "✅ Server detected on port 50051"
echo ""

# Run validation
echo "Starting validation test..."
echo "This will take approximately 1 hour."
echo ""

./target/release/validation_enterprise \
    --duration ${DURATION} \
    --qps ${QPS} \
    --cache-strategy ${CACHE_STRATEGY} \
    --corpus-size ${CORPUS_SIZE} \
    --query-count ${QUERY_COUNT} \
    --metrics-interval 60

# Parse results (if validation binary outputs JSON or structured data)
echo ""
echo "=================================================="
echo "Validation Complete!"
echo "=================================================="
echo ""
echo "Next steps:"
echo "1. Check final learned hit rate (target: 50-55%)"
echo "2. Verify LRU control group (~20-25%)"
echo "3. Confirm P99 latency <5ms"
echo "4. Check memory growth <5%"
echo ""
echo "If targets met: Proceed to Week 3 (query clustering)"
echo "If targets missed: Tune parameters and re-run"
echo ""
