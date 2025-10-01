# KyroDB Benchmark Suite

Comprehensive performance testing infrastructure for KyroDB's read-optimized, learned-index architecture.

## Quick Start

### Prerequisites
- Rust stable toolchain (1.75+)
- KyroDB server running at `http://127.0.0.1:3030`
- Sufficient disk space (10GB+ for large datasets)
- 8GB+ RAM recommended

### Run Complete Suite

```bash
# Run all benchmark phases (2-3 hours on typical hardware)
./bench/run_complete_suite.sh all

# Run specific phase
./bench/run_complete_suite.sh micro      # Raw engine microbenchmarks (~30 min)
./bench/run_complete_suite.sh http       # HTTP layer benchmarks (~20 min)
./bench/run_complete_suite.sh integration # Integration tests (~10 min)
./bench/run_complete_suite.sh comparison  # RMI vs BTree comparison (~30 min)
```

### Quick Smoke Test

```bash
# Fast validation (~30 seconds)
cargo run -p kyrodb-bench --release -- --phase quick
```

## Architecture

```
bench/
├── src/
│   ├── lib.rs              # Core benchmark library
│   ├── main.rs             # CLI entry point
│   ├── client/             # HTTP and binary protocol clients
│   │   ├── mod.rs          # Client trait and factory
│   │   ├── http.rs         # HTTP client implementation
│   │   └── ultra_fast.rs   # Binary protocol client
│   ├── workload/           # Workload patterns
│   │   ├── mod.rs          # Workload trait and distributions
│   │   ├── generator.rs    # Key/value generation
│   │   └── patterns.rs     # Read/write/mixed/scan workloads
│   ├── metrics/            # Metrics collection
│   │   ├── mod.rs          # Metrics collector
│   │   └── reporter.rs     # Results formatting
│   └── runner/             # Benchmark orchestration
│       ├── mod.rs          # Benchmark runner
│       ├── warmup.rs       # Server warmup logic
│       └── executor.rs     # Workload execution
├── benches/
│   └── kv_index.rs         # Criterion microbenchmarks
├── scripts/
│   ├── comprehensive_benchmark.sh  # Legacy comprehensive suite
│   └── generate_plot.py            # Results visualization
└── run_complete_suite.sh   # Unified orchestration script

engine/benches/              # Raw engine benchmarks
├── index_comparison.rs      # RMI vs BTree comparison
├── raw_index_scale.rs       # Scaling behavior (1M-50M keys)
├── wal_throughput.rs        # WAL append throughput
├── snapshot_performance.rs  # Snapshot creation/load time
└── rmi_rebuild.rs           # RMI build time and atomic swap
```

## Benchmark Structure

### 1. Raw Engine Microbenchmarks (`engine/benches/`)

**Purpose:** Test core engine components without HTTP overhead.

**Benchmarks:**
- **`wal_throughput.rs`**: WAL append throughput
  - Sequential append (100, 1K, 10K batches)
  - Concurrent append (4, 8, 16, 32, 64 workers)
  - Group commit effectiveness
  - Fsync policy impact

- **`snapshot_performance.rs`**: Snapshot operations
  - Creation time vs dataset size (10K, 100K, 1M keys)
  - Load time from disk
  - Mmap warmup time
  - Concurrent operations during snapshot

- **`rmi_rebuild.rs`**: RMI index operations (requires `learned-index` feature)
  - Build time vs dataset size (100K, 1M, 5M keys)
  - Rebuild time under concurrent load
  - Atomic index swap overhead
  - Prediction accuracy with different distributions

- **`index_comparison.rs`**: RMI vs BTree
  - Lookup performance (1K, 10K, 100K keys)
  - Insert performance
  - Memory usage
  - Scaling behavior

- **`raw_index_scale.rs`**: Large-scale testing
  - 1M, 10M, 50M key datasets
  - Memory-mapped lookups
  - SIMD-accelerated search

**Run:**
```bash
# All engine benchmarks
cargo bench -p kyrodb-engine --features learned-index

# Specific benchmark
cargo bench -p kyrodb-engine --bench wal_throughput --features learned-index
```

### 2. HTTP Layer Benchmarks (`bench/src/`)

**Purpose:** Test HTTP API performance with realistic workloads.

**Workloads:**
- **Read-heavy (95% reads)**: Simulates cache/analytics workloads
- **Write-heavy (10% reads)**: Simulates logging/ingestion workloads
- **Mixed (50/50)**: Balanced OLTP workloads
- **Scan-heavy (99% reads with scans)**: Range query workloads

**Distributions:**
- **Uniform**: Even access across all keys
- **Zipf (skew=1.2)**: 80/20 hot/cold data pattern
- **Sequential**: Time-series-like access
- **Hotspot**: Concentrated access on 10% of keys

**Run:**
```bash
# Run specific workload
cargo run -p kyrodb-bench --release -- \
    --phase http \
    --workload read_heavy \
    --workers 64 \
    --duration 30 \
    --key-count 1000000 \
    --distribution zipf

# Output as CSV
cargo run -p kyrodb-bench --release -- \
    --phase http \
    --workload mixed \
    --format csv > results.csv
```

### 3. Integration Benchmarks

**Purpose:** Test complete data flow: PUT → Snapshot → RMI Build → GET

**Scenarios:**
- Multi-phase workflows
- Concurrent operations during maintenance
- Recovery patterns
- Realistic production workloads

**Run:**
```bash
cargo run -p kyrodb-bench --release -- \
    --phase integration \
    --workers 64 \
    --duration 60 \
    --key-count 1000000 \
    --value-size 512 \
    --distribution zipf
```

### 4. RMI vs BTree Comparison

**Purpose:** Quantify RMI performance advantages.

**Metrics:**
- Throughput (ops/sec)
- Latency (p50, p95, p99, p999)
- Scaling behavior
- Memory usage

**Run:**
```bash
./bench/run_complete_suite.sh comparison
```

This will:
1. Build with RMI (`--features learned-index`)
2. Start server and run benchmarks
3. Build with BTree (no learned-index feature)
4. Start server and run benchmarks
5. Generate comparison report

## Configuration

### Environment Variables

```bash
# Server URL
export KYRODB_BENCH_URL="http://127.0.0.1:3030"

# Worker count
export WORKERS=64

# Test duration (seconds)
export DURATION=30

# Warmup duration (seconds)
export WARMUP=10
```

### Server Configuration for Benchmarking

**Critical:** Disable auth and rate limiting for accurate benchmarks.

```bash
# Optimal benchmark settings
export KYRODB_WARM_ON_START=1
export KYRODB_DISABLE_HTTP_LOG=1
export KYRODB_GROUP_COMMIT_DELAY_MICROS=100
export KYRODB_FSYNC_POLICY=data
export KYRODB_RMI_TARGET_LEAF=2048
export RUST_LOG=error

# Build and start server
cargo build -p kyrodb-engine --release --features learned-index
./target/release/kyrodb-engine serve 127.0.0.1 3030
```

## Results

### Output Structure

```
bench/results/complete_<commit>_<timestamp>/
├── BENCHMARK_SUMMARY.md           # Human-readable summary
├── microbench_wal.log             # WAL throughput results
├── microbench_snapshot.log        # Snapshot performance
├── microbench_rmi.log             # RMI rebuild times
├── microbench_index.log           # Index comparison
├── http_read_heavy.csv            # Read-heavy workload
├── http_write_heavy.csv           # Write-heavy workload
├── http_mixed.csv                 # Mixed workload
├── integration_results.csv        # Integration test results
├── comparison_rmi.csv             # RMI benchmark results
└── comparison_btree.csv           # BTree benchmark results
```

### Interpreting Results

**Throughput:**
- Target: >100K ops/sec for reads (RMI)
- Target: >50K ops/sec for writes
- Target: >75K ops/sec for mixed workload

**Latency:**
- P50 target: <500μs
- P95 target: <2ms
- P99 target: <5ms
- P99.9 target: <10ms

**RMI vs BTree:**
- Expected RMI advantage: 2-4x throughput on large datasets (>1M keys)
- Expected RMI advantage: 30-50% lower latency
- Expected advantage increases with dataset size

**Example CSV output:**
```csv
phase,workload,duration_secs,total_ops,successful_ops,failed_ops,throughput_ops_sec,min_us,mean_us,p50_us,p95_us,p99_us,p999_us,max_us
HttpLayer,read_heavy,30.0,3000000,3000000,0,100000,85,450,420,1200,2400,8500,15000
```

## Troubleshooting

### Low Throughput

1. **Check RMI is enabled:**
   ```bash
   curl http://127.0.0.1:3030/metrics | grep rmi
   ```

2. **Verify warmup completed:**
   Check server logs for "Warmup complete"

3. **Increase workers:**
   ```bash
   export WORKERS=128
   ```

4. **Check system resources:**
   ```bash
   top
   iostat -x 1
   ```

### High Latency

1. **Check disk I/O bottlenecks:**
   ```bash
   iostat -x 1
   iotop
   ```

2. **Verify fsync settings:**
   ```bash
   export KYRODB_FSYNC_POLICY=data  # Not 'always'
   ```

3. **Check CPU throttling:**
   ```bash
   cat /proc/cpuinfo | grep MHz
   ```

4. **Review server logs:**
   ```bash
   tail -f logs/kyrodb.log
   ```

### Inconsistent Results

1. **Consistent server configuration:**
   - Same fsync policy
   - Same feature flags
   - Same environment variables

2. **Run longer tests:**
   ```bash
   export DURATION=60
   export WARMUP=20
   ```

3. **Close other applications:**
   - Stop unnecessary services
   - Disable background tasks
   - Isolate test environment

4. **Use dedicated hardware:**
   - Avoid shared VMs
   - Disable CPU frequency scaling
   - Use SSD/NVMe storage

## Development

### Adding New Benchmarks

1. **Engine Microbenchmarks:**
   ```bash
   touch engine/benches/my_feature.rs
   # Add to engine/Cargo.toml [[bench]] section
   ```

2. **HTTP Workloads:**
   Add to `bench/src/workload/patterns.rs`:
   ```rust
   pub struct MyCustomWorkload { /* ... */ }
   impl Workload for MyCustomWorkload { /* ... */ }
   ```

3. **Integration Tests:**
   Add to `bench/src/runner/mod.rs`:
   ```rust
   pub async fn run_my_integration_test(&mut self) -> Result<()> {
       // Test logic
   }
   ```

### Benchmark Best Practices

- **Always warmup** before measurement (10s minimum)
- **Use sufficient duration** (30s minimum for stability)
- **Run multiple iterations** and compare results
- **Document expected results** in benchmark comments
- **Use realistic data distributions** (zipf, hotspot, not just uniform)
- **Test both hot and cold paths** (with/without warmup)
- **Measure both throughput and latency**
- **Check for memory leaks** with long-running tests

## CI Integration

Benchmarks run in CI on:
- Every merge to `main`
- Manual trigger for PR validation
- Nightly performance regression tracking

**Performance Gates:**
- No >10% regression in throughput
- No >20% regression in P99 latency
- RMI must be >2x faster than BTree on large datasets (>1M keys)

## Azure VM Deployment

### VM Specifications (Recommended)

- **Instance:** Standard_D8s_v5 (8 vCPUs, 32GB RAM)
- **Storage:** Premium SSD (P30, 1TB, 5000 IOPS)
- **OS:** Ubuntu 22.04 LTS
- **Network:** Accelerated networking enabled

### Setup Script

```bash
#!/bin/bash
# setup_bench_vm.sh

# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source $HOME/.cargo/env

# Install dependencies
sudo apt update
sudo apt install -y build-essential pkg-config libssl-dev

# Clone repository
git clone https://github.com/vatskishan03/KyroDB.git
cd KyroDB

# Build
cargo build -p kyrodb-engine --release --features learned-index
cargo build -p kyrodb-bench --release

# Start server
./target/release/kyrodb-engine serve 0.0.0.0 3030 &
sleep 5

# Run benchmarks
./bench/run_complete_suite.sh all

# Results will be in bench/results/
```

## References

- [Performance Analysis Guide](../docs/PERFORMANCE.md) *(to be created)*
- [Testing Strategy](../docs/TESTING.md)
- [Vision Document](../docs/visiondocument.md)
- [Implementation Roadmap](../Implementation.md)
- [RMI ADR](../docs/adr/0001-rmi-learned-index.md)

## Performance Targets (Phase 0)

| Metric | Target | Current | Status |
|--------|--------|---------|--------|
| Read Throughput (1M keys) | 100K ops/sec | TBD | 🔄 |
| Write Throughput | 50K ops/sec | TBD | 🔄 |
| P99 Read Latency | <5ms | TBD | 🔄 |
| P99 Write Latency | <10ms | TBD | 🔄 |
| RMI vs BTree Advantage | >2x | TBD | 🔄 |
| Memory Usage (1M keys) | <2GB | TBD | 🔄 |

---

**Next Steps:**
1. Run complete benchmark suite on local development machine
2. Deploy to Azure VM and run production-scale tests
3. Document results and performance characteristics
4. Identify optimization opportunities
5. Track performance over time with CI integration
