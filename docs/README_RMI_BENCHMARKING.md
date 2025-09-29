# KyroDB RMI Benchmarking Guide

## Critical: Testing the Actual System

**IMPORTANT**: KyroDB's core innovation is the **Adaptive RMI** (learned index) for ultra-fast reads. When benchmarking or testing, you MUST ensure you're actually using the RMI, not the legacy BTree fallback.

## Quick Start: How to Benchmark RMI

### 1. Build with learned-index feature
```bash
cargo build -p kyrodb-engine --release --features learned-index
cargo build -p bench --release
```

### 2. Start server with warmup enabled
```bash
# Enable RMI and warm-on-start for optimal performance
KYRODB_USE_ADAPTIVE_RMI=true \
KYRODB_WARM_ON_START=1 \
./target/release/kyrodb-engine serve 127.0.0.1 3030
```

### 3. Load data and build RMI
```bash
# Load test data (1M keys example)
./target/release/bench \
    --base http://127.0.0.1:3030 \
    --load-n 1000000 \
    --workers 32

# Build RMI index from loaded data
curl -X POST http://127.0.0.1:3030/v1/rmi/build

# Warm up caches (fault-in mmap pages)
curl -X POST http://127.0.0.1:3030/v1/warmup
```

### 4. Run benchmarks
```bash
# Now run read-heavy workload to measure RMI performance
./target/release/bench \
    --base http://127.0.0.1:3030 \
    --workers 64 \
    --duration 30 \
    --key-count 1000000 \
    --value-size 256 \
    --read-ratio 0.95 \
    --warmup 10
```

## Environment Variables

### Essential for RMI Testing

- **KYRODB_USE_ADAPTIVE_RMI=true** (default: true when learned-index feature enabled)
  - Forces use of RMI instead of BTree
  - MUST be set to "true" for RMI benchmarks
  
- **KYRODB_WARM_ON_START=1** (default: disabled)
  - Warms up mmap pages and RMI cache on server startup
  - Critical for accurate P95/P99 latency measurements
  - Without this, first reads will be slow (cold mmap reads)

### Optional Performance Tuning

- **KYRODB_DURABILITY_LEVEL** (default: "enterprise_safe")
  - "enterprise_safe": fsync on every write (safe, slower)
  - "enterprise_async": background fsync (fast, slightly less safe)
  - For benchmarks: use "enterprise_safe" for realistic performance
  - For max throughput tests: use "enterprise_async"

- **KYRODB_GROUP_COMMIT_ENABLED=1**
  - Batches multiple writes into single fsync
  - Improves write throughput significantly
  - Default delay: 100μs

- **AUTH_ENABLED=false** / **RATE_LIMIT_ENABLED=false**
  - Disables auth and rate limiting for pure engine benchmarks
  - Default: disabled (no overhead)

## API Endpoints for RMI

### Build and Manage RMI
```bash
# Build RMI from current data (required after loading data)
POST /v1/rmi/build

# Get RMI statistics
GET /v1/rmi/stats

# Warm up caches (mmap pages, CPU cache lines)
POST /v1/warmup
```

### Fast Lookup Endpoints
```bash
# Ultra-fast RMI lookup (returns offset only)
GET /v1/lookup_ultra/{key}

# Fast path (returns full value using RMI)
GET /v1/get_fast/{key}

# Batch lookup (uses RMI for all keys)
POST /v1/lookup_batch
# Body: {"keys": [key1, key2, ...]}
```

## Benchmark Script Template

```bash
#!/usr/bin/env bash
set -euo pipefail

BASE="http://127.0.0.1:3030"
KEYS=1000000

# 1. Build everything
cargo build -p kyrodb-engine --release --features learned-index
cargo build -p bench --release

# 2. Start server (in separate terminal or background)
# KYRODB_USE_ADAPTIVE_RMI=true \
# KYRODB_WARM_ON_START=1 \
# ./target/release/kyrodb-engine serve 127.0.0.1 3030 &
# SERVER_PID=$!

# 3. Wait for server
sleep 2
curl -f "$BASE/health" || { echo "Server not ready"; exit 1; }

# 4. Load data
echo "Loading $KEYS keys..."
./target/release/bench --base "$BASE" --load-n "$KEYS" --workers 32

# 5. Build RMI (CRITICAL)
echo "Building RMI index..."
curl -X POST "$BASE/v1/rmi/build"
sleep 1

# 6. Warm up (CRITICAL)
echo "Warming up caches..."
curl -X POST "$BASE/v1/warmup"
sleep 1

# 7. Run read benchmark
echo "Running read benchmark..."
./target/release/bench \
    --base "$BASE" \
    --workers 64 \
    --duration 30 \
    --key-count "$KEYS" \
    --value-size 256 \
    --read-ratio 0.95 \
    --warmup 10

# 8. Cleanup
# kill $SERVER_PID
```

## Common Mistakes

### ❌ WRONG: Testing BTree instead of RMI
```bash
# Missing learned-index feature
cargo build -p kyrodb-engine --release

# Or explicitly disabling RMI
KYRODB_USE_ADAPTIVE_RMI=false ./target/release/kyrodb-engine serve
```

**Fix**: Always use `--features learned-index` and ensure `KYRODB_USE_ADAPTIVE_RMI=true`

### ❌ WRONG: No RMI build after loading data
```bash
./target/release/bench --load-n 1000000
# Immediately run reads WITHOUT building RMI
./target/release/bench --read-ratio 1.0  # Uses BTree fallback!
```

**Fix**: Always call `POST /v1/rmi/build` after loading data

### ❌ WRONG: No warmup before benchmarking
```bash
curl -X POST /v1/rmi/build
# Immediately benchmark - first reads will be SLOW
./target/release/bench --read-ratio 0.95
```

**Fix**: Call `POST /v1/warmup` and add client-side `--warmup` period

### ❌ WRONG: Cold mmap reads polluting P99
```bash
# No KYRODB_WARM_ON_START
./target/release/kyrodb-engine serve 127.0.0.1 3030
# First reads hit cold mmap pages -> P99 spikes to 10ms+
```

**Fix**: Use `KYRODB_WARM_ON_START=1` or call `/v1/warmup` before benchmarks

## Verifying RMI is Active

### Check server logs
```bash
# Should see on startup:
# "Loaded optimized RMI configuration: SIMD width=4, batch size=16, cache buffer size=64"
```

### Check RMI stats
```bash
curl http://127.0.0.1:3030/v1/rmi/stats

# Should return:
# {
#   "index_type": "adaptive_rmi",
#   "keys_indexed": 1000000,
#   "segments": [...],
#   "cache_stats": {...}
# }
```

### Compare performance
```bash
# RMI should be 5-10x faster than BTree for sorted lookups
# Typical results:
# - RMI: P50=1-2μs, P99=5-10μs, 200K+ ops/sec
# - BTree: P50=10-20μs, P99=50-100μs, 30K ops/sec
```

## Test Infrastructure

### Tests Use RMI by Default
All tests in `engine/src/test/` now use RMI when compiled with `--features learned-index`:

```bash
# Run tests with RMI enabled
cargo test -p kyrodb-engine --features learned-index --lib test::background

# Tests automatically:
# 1. Set KYRODB_USE_ADAPTIVE_RMI=true (when feature enabled)
# 2. Build RMI after writes (via log.build_rmi().await)
# 3. Validate RMI fast path
```

### TestServer Helper
The `TestServer` helper in tests now:
- Enables RMI when `learned-index` feature is active
- Calls `build_rmi()` automatically after opening log
- Only falls back to BTree when feature is disabled

## Performance Expectations

With properly warmed RMI:

### Read Performance (95% read ratio)
- **Throughput**: 100K-300K ops/sec (64 workers)
- **P50 Latency**: 1-3μs
- **P95 Latency**: 5-15μs  
- **P99 Latency**: 10-30μs

### Write Performance (95% write ratio)
- **Throughput**: 10K-50K ops/sec (enterprise_safe)
- **Throughput**: 50K-100K ops/sec (enterprise_async)

### Mixed Workload (50/50)
- **Throughput**: 50K-100K ops/sec
- **P99 Read Latency**: 10-20μs
- **P99 Write Latency**: 50-200μs

## Next Steps

1. **Validate Phase 0**: Run full benchmark suite with RMI
2. **Compare RMI vs BTree**: Document performance improvements
3. **Tune Parameters**: Adjust SIMD width, batch size, cache size
4. **Stress Test**: 10M+ keys, sustained load, edge cases

## References

- [Vision Document](../docs/visiondocument.md) - Phase 0 goals
- [ADR 0001](../docs/adr/0001-rmi-learned-index.md) - RMI design decisions

