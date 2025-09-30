# KyroDB Stress Testing Suite

Complete stress testing framework to validate KyroDB under extreme conditions.

## Test Categories

### 1. Concurrent Operations (`concurrent_operations.rs`)
Tests extreme concurrent load with hundreds to thousands of simultaneous operations.

**Tests:**
- `test_1000_concurrent_writers` - 1000 threads writing simultaneously
- `test_1000_concurrent_readers` - 1000 threads reading simultaneously  
- `test_mixed_workload_1000_threads` - 500 writers + 500 readers
- `test_hot_buffer_overflow_stress` - Force hot buffer overflow (200K concurrent writes)
- `test_snapshot_under_concurrent_load` - Snapshots during heavy writes
- `test_rmi_rebuild_under_load` - RMI rebuilds during concurrent operations

### 2. Large Dataset (`large_dataset.rs`)
Tests system behavior with millions of keys and gigabytes of data.

**Tests:**
- `test_1_million_keys` - Insert and verify 1M keys (⚠️ ignored, run explicitly)
- `test_10_million_keys` - Insert 10M keys with parallel workers (⚠️ ignored)
- `test_large_values_1kb` - 100K keys with 1KB values each (~100MB)
- `test_very_large_values_10kb` - 10K keys with 10KB values (~100MB) (⚠️ ignored)
- `test_sparse_key_space` - Large gaps between keys (tests RMI segmentation)
- `test_sequential_scan_large_dataset` - Full sequential scan of 500K keys

### 3. Memory Pressure (`memory_pressure.rs`)
Tests system behavior under memory constraints.

**Tests:**
- `test_buffer_overflow_recovery` - Buffer overflow and recovery validation
- `test_memory_intensive_workload` - 50K ops with 4KB values
- `test_snapshot_under_memory_pressure` - Snapshot 100K keys with large values
- `test_concurrent_snapshot_attempts` - 10 concurrent snapshot attempts
- `test_rmi_rebuild_memory_pressure` - RMI rebuild with 200K keys
- `test_wal_growth_under_pressure` - WAL growth with repeated updates

### 4. Lock Contention (`lock_contention.rs`)
Tests lock ordering and contention under extreme concurrent access.

**Tests:**
- `test_hot_buffer_lock_contention` - 100 threads hitting hot buffer
- `test_overflow_buffer_lock_contention` - Force overflow buffer contention
- `test_reader_writer_lock_contention` - 80 readers + 20 writers for 10s
- `test_snapshot_lock_contention` - Concurrent snapshots and writes
- `test_no_deadlocks_extreme_contention` - 40R + 30W + 5S + 3RMI (deadlock detection)

### 5. RMI Stress (`rmi_stress.rs`)
Tests RMI accuracy and performance under stress (requires `learned-index` feature).

**Tests:**
- `test_rmi_skewed_distribution_stress` - Zipfian (80/20) distribution
- `test_rmi_frequent_rebuilds` - 20 RMI rebuilds with 5K keys each
- `test_rmi_with_continuous_updates` - 10 rounds of updates on same keys
- `test_rmi_many_segments_stress` - Force many segments with random keys

### 6. Recovery Stress (`recovery_stress.rs`)
Tests recovery after crashes and failures.

**Tests:**
- `test_recovery_after_write_crash` - Crash simulation during writes
- `test_recovery_with_multiple_snapshots` - Recover from 5 snapshots
- `test_rapid_restart_stress` - 10 rapid open/close cycles
- `test_wal_replay_recovery` - Recovery via WAL replay

### 7. Endurance (`endurance.rs`)
Long-running tests for stability validation.

**Tests:**
- `test_sustained_write_load_1hour` - 1 hour of continuous writes (⚠️ ignored)
- `test_sustained_mixed_30min` - 30 min mixed workload (⚠️ ignored)
- `test_continuous_snapshots_10min` - 10 min with snapshots every 30s
- `test_memory_stability_15min` - 15 min stability test

## Running Stress Tests

### Quick Stress Test (default tests, ~5-10 minutes)
```bash
cargo test --lib --features learned-index --release test::stress -- --test-threads=4
```

### Run Specific Category
```bash
# Concurrent operations
cargo test --lib --features learned-index --release test::stress::concurrent_operations

# Large datasets
cargo test --lib --features learned-index --release test::stress::large_dataset

# Memory pressure
cargo test --lib --features learned-index --release test::stress::memory_pressure

# Lock contention
cargo test --lib --features learned-index --release test::stress::lock_contention

# RMI stress
cargo test --lib --features learned-index --release test::stress::rmi_stress

# Recovery
cargo test --lib --features learned-index --release test::stress::recovery_stress

# Endurance (quick endurance tests)
cargo test --lib --features learned-index --release test::stress::endurance
```

### Run Long-Running Tests (ignored by default)
```bash
# 1 million keys
cargo test --lib --features learned-index --release test_1_million_keys -- --ignored

# 10 million keys (VERY LONG)
cargo test --lib --features learned-index --release test_10_million_keys -- --ignored

# 1 hour endurance
cargo test --lib --features learned-index --release test_sustained_write_load_1hour -- --ignored

# 30 minute mixed workload
cargo test --lib --features learned-index --release test_sustained_mixed_30min -- --ignored
```

### Full Stress Test Suite (includes ignored tests, ~2-3 hours)
```bash
cargo test --lib --features learned-index --release test::stress -- --ignored --test-threads=4
```

## Performance Expectations

### Concurrent Operations
- **1000 concurrent writers:** >50K ops/sec aggregate
- **1000 concurrent readers:** >100K ops/sec aggregate
- **Mixed workload:** >70K ops/sec aggregate
- **No deadlocks:** All operations complete

### Large Datasets
- **1M keys insertion:** <60 seconds
- **10M keys insertion:** <600 seconds (10 min)
- **Sequential scan (500K):** >50K ops/sec

### Memory Pressure
- **Buffer overflow:** Graceful handling, >80% success rate
- **Snapshot under pressure:** Completes successfully
- **Recovery:** System resumes normal operation

### Lock Contention
- **No deadlocks:** All tests must complete within timeout
- **Throughput degradation:** <30% under extreme contention
- **Fairness:** All threads make progress

### RMI Performance
- **Build time:** <1s per 100K keys
- **Lookup accuracy:** 100% after rebuild
- **Performance:** <10μs P50 lookup

### Recovery
- **Data recovery:** >95% of committed data
- **Recovery time:** <5s per 100K keys
- **Consistency:** No corruption

### Endurance
- **Sustained load:** Stable throughput over time
- **Memory stability:** No memory leaks
- **No crashes:** System remains available

## Test Configuration

### Thread Pool Sizes
- **Light tests:** 4-8 threads
- **Heavy tests:** 16 threads
- **Endurance tests:** 8 threads

### Timeouts
- **Quick tests:** Complete in <60s
- **Medium tests:** Complete in <5 min
- **Long tests:** Up to 1 hour

### Data Sizes
- **Small:** <10K keys
- **Medium:** 10K-100K keys
- **Large:** 100K-1M keys
- **Very large:** >1M keys (explicit opt-in)

## Troubleshooting

### Tests Timing Out
```bash
# Increase test timeout
RUST_TEST_TIME_UNIT=60000 cargo test --release ...
```

### Out of Memory
```bash
# Reduce parallelism
cargo test --release -- --test-threads=1

# Run smaller dataset variants
cargo test --release test::stress -- --skip large_dataset
```

### Test Failures
1. Check system resources (CPU, memory, disk)
2. Review test output for specific errors
3. Run with `RUST_LOG=debug` for detailed logs
4. Run tests individually to isolate issues

## CI Integration

### Recommended CI Test Set (< 15 minutes)
```bash
cargo test --lib --features learned-index --release \
  test::stress::concurrent_operations \
  test::stress::memory_pressure \
  test::stress::lock_contention \
  test::stress::recovery_stress \
  -- --test-threads=4
```

### Nightly Stress Test (full suite)
```bash
cargo test --lib --features learned-index --release \
  test::stress -- --ignored --test-threads=4
```

## Metrics and Monitoring

Each test reports:
- **Operations completed:** Total successful operations
- **Duration:** Test execution time
- **Throughput:** ops/sec
- **Success rate:** Percentage of successful operations
- **Error counts:** Failed operations by type

### Example Output
```
📊 1000 Concurrent Writers Results:
   Total writes: 125,432
   Errors: 0
   Duration: 2.45s
   Throughput: 51,192.24 ops/sec
```

## Best Practices

1. **Always run in release mode** - Debug builds are too slow
2. **Monitor system resources** - Use `htop`, `iostat`, etc.
3. **Run on dedicated hardware** - Avoid shared CI runners for benchmarks
4. **Warm up the system** - Run a small test first
5. **Check for regressions** - Compare against baseline
6. **Document failures** - Include system info and logs

## Related Documentation

- [Testing Guide](TESTING.md) - Overall testing strategy
- [Concurrency](CONCURRENCY.md) - Lock ordering and thread safety
- [RMI Documentation](README_RMI_BENCHMARKING.md) - RMI-specific testing

## Contributing

When adding new stress tests:
1. Choose appropriate category module
2. Use descriptive test names
3. Add `#[ignore]` for long-running tests (>5 min)
4. Document expected performance
5. Include cleanup logic
6. Report comprehensive metrics
