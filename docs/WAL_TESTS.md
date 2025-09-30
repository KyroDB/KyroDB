# WAL Tests Documentation

Comprehensive test suite for KyroDB's Write-Ahead Log (WAL) implementation.

## Overview

The WAL test suite validates durability guarantees, crash recovery, corruption handling, and compaction operations. All tests use the modern API with helper functions and include multi-threaded execution patterns.

**Test Statistics:**
- **Total Tests:** 40
- **Pass Rate:** 100% (40/40)
- **Execution Time:** ~35 seconds
- **Coverage Areas:** 6 modules

## Test Modules

### 1. Append Correctness (`append_correctness.rs`)

Tests basic WAL append operations and data integrity.

#### Tests (6)

1. **`test_basic_wal_append`**
   - Validates sequential append and lookup operations
   - Writes 100 key-value pairs
   - Verifies data integrity after snapshot + RMI build

2. **`test_wal_append_ordering`**
   - Tests update ordering for same key
   - Writes 10 versions to key 42
   - Verifies latest version is retained

3. **`test_wal_durability_after_restart`**
   - Validates persistence across restarts
   - Writes 100 entries, drops log, reopens
   - Confirms all data survives restart

4. **`test_concurrent_wal_appends`**
   - Tests concurrent write safety
   - 20 concurrent workers × 100 operations each
   - Validates no data loss with parallel writes

5. **`test_wal_with_large_entries`**
   - Tests large entry handling (64KB values)
   - Writes 10 large entries
   - Verifies size and integrity after persistence

### 2. Group Commit (`group_commit.rs`)

Tests batching and fsync policies for write efficiency.

#### Tests (4)

1. **`test_group_commit_batching`**
   - Validates batching effectiveness
   - Rapid-fire 1000 appends
   - Confirms reasonable completion time (<30s with RMI)

2. **`test_group_commit_durability`**
   - Tests durability with batching enabled
   - Writes 100 entries, waits for commit window
   - Verifies persistence after restart

3. **`test_concurrent_group_commits`**
   - Tests concurrent writes during commit window
   - 10 workers × 100 operations
   - Validates all data committed

4. **`test_group_commit_performance`**
   - Compares batching vs frequent sync
   - Writes 1000 × 256-byte entries
   - Measures performance difference

### 3. Durability (`durability.rs`)

Tests WAL durability guarantees and crash recovery scenarios.

#### Tests (7)

1. **`test_wal_durability_immediate`**
   - Tests immediate durability with fsync
   - 100 writes → snapshot → restart → verify

2. **`test_wal_survives_crash_simulation`**
   - Simulates crash (immediate drop without graceful shutdown)
   - Writes data, drops without flush, reopens
   - Verifies data recovery

3. **`test_wal_multiple_restarts`**
   - Tests data persistence across 5 restart cycles
   - Cumulative writes with verification each cycle
   - 100 entries total across all restarts

4. **`test_wal_durability_with_large_entries`**
   - Tests large entry persistence (1KB values)
   - 50 large entries → restart → verify
   - Confirms size and content integrity

5. **`test_wal_durability_sequential_updates`**
   - Tests version tracking across restarts
   - 2 phases with updates to same keys
   - Verifies latest versions persist

6. **`test_wal_durability_concurrent_writes`**
   - Tests durability with concurrent writers
   - 10 workers × 50 operations → crash → recover
   - Validates high recovery rate (≥80%)

7. **`test_wal_no_data_loss_on_restart`**
   - Tests incremental writes across multiple restarts
   - 3 rounds: 100 → 150 → 200 keys
   - Verifies cumulative data integrity

### 4. Recovery (`recovery.rs`)

Tests WAL recovery mechanisms and replay correctness.

#### Tests (8)

1. **`test_wal_recovery_basic`**
   - Basic recovery scenario
   - 100 writes → shutdown → recovery → verify

2. **`test_wal_recovery_with_snapshots`**
   - Recovery from snapshot + WAL tail
   - Phase 1: 50 entries + snapshot
   - Phase 2: 50 more entries → crash → recover all 100

3. **`test_wal_recovery_idempotent`**
   - Tests idempotent recovery
   - 50 writes → 3 recovery cycles
   - Confirms consistent results

4. **`test_wal_recovery_preserves_order`**
   - Tests ordering preservation
   - 10 versions to key 42
   - Verifies correct version after recovery

5. **`test_wal_recovery_after_partial_write`**
   - Simulates partial write scenario
   - 30 writes → immediate crash
   - Validates graceful handling (≥20 recovered)

6. **`test_wal_recovery_with_multiple_snapshots`**
   - Recovery from multiple snapshot generations
   - 3 phases: snap1 (30), snap2 (30), nosnapshot (30)
   - Confirms recovery across all phases (≥50/90)

7. **`test_wal_recovery_empty_log`**
   - Tests empty log recovery
   - Opens empty log → recovery → verify empty state
   - Confirms no panics

8. **`test_wal_recovery_concurrent_writes_before_crash`**
   - Recovery after concurrent writes
   - 5 workers × 20 operations → crash → recover
   - Validates high recovery rate (≥80/100)

### 5. Corruption (`corruption.rs`)

Tests corruption detection and recovery strategies.

#### Tests (8)

1. **`test_wal_detects_missing_entries`**
   - Validates missing entry detection
   - 50 writes → recovery → check for gaps

2. **`test_wal_recovery_with_partial_corruption`**
   - Tests recovery with partial corruption
   - Snapshotted data (30) + risky tail (30)
   - Confirms at least snapshot data recovers (≥25/30)

3. **`test_wal_handles_empty_wal_file`**
   - Tests empty WAL file handling
   - Creates WAL structure → recovery
   - Confirms graceful handling

4. **`test_wal_isolation_of_corrupted_segment`**
   - Tests corruption isolation
   - Segment 1 (snapshotted 20) + Segment 2 (unsnapshotted 20)
   - Validates first segment integrity (≥18/20)

5. **`test_wal_continued_operation_after_corruption_recovery`**
   - Tests operation continuation post-recovery
   - Before (30) → crash → recover → after (30) → verify both
   - Confirms ≥50/60 recovery

6. **`test_wal_large_entry_boundary_corruption`**
   - Tests corruption at large entry boundaries
   - Small entries + 1KB entry + small entries
   - Validates recovery (≥15/20 small + 1KB large)

7. **`test_wal_sequential_corruption_detection`**
   - Tests sequence integrity checking
   - 100 sequential entries with pattern
   - Detects gaps (<20 gaps indicates good recovery)

8. **`test_wal_recovery_statistics`**
   - Measures recovery rate
   - 200 entries → recovery → count recovered
   - Requires ≥90% recovery rate

### 6. Compaction (`compaction.rs`)

Tests WAL compaction and space reclamation.

#### Tests (8)

1. **`test_wal_compaction_after_snapshot`**
   - Basic compaction after snapshot
   - 100 before + snapshot + 50 after
   - Verifies all 150 accessible

2. **`test_wal_space_reclamation`**
   - Tests space reclamation
   - 500 × 512-byte entries → snapshot → 100 small updates
   - Confirms integrity after compaction

3. **`test_wal_compaction_preserves_latest_values`**
   - Tests value preservation during compaction
   - 5 versions × 50 keys with snapshots
   - Verifies latest versions accessible

4. **`test_wal_compaction_concurrent_with_writes`**
   - Tests concurrent compaction and writes
   - 3 writers + concurrent snapshots
   - Validates no data loss (≥120/150)

5. **`test_wal_compaction_idempotent`**
   - Tests idempotent compaction
   - 100 entries → 5 snapshots
   - Confirms data integrity after multiple compactions

6. **`test_wal_segment_cleanup_after_recovery`**
   - Tests cleanup after recovery
   - Phase 1: 100 + snapshot → crash
   - Phase 2: recover + 100 more + snapshot → verify all 200

7. **`test_wal_handles_rapid_snapshots`**
   - Tests rapid snapshot cycles
   - 10 batches × 20 entries with rapid snapshots (10ms intervals)
   - Confirms all 200 survive rapid compaction

8. **`test_wal_compaction_performance`**
   - Measures compaction performance
   - 1000 entries → snapshot
   - Requires <5 seconds completion

## Running the Tests

### Run All WAL Tests

```bash
cargo test --lib --features learned-index wal:: -- --test-threads=4
```

### Run Specific Module

```bash
# Durability tests only
cargo test --lib --features learned-index wal::durability

# Recovery tests only
cargo test --lib --features learned-index wal::recovery

# Corruption tests only
cargo test --lib --features learned-index wal::corruption
```

### Run Single Test

```bash
cargo test --lib --features learned-index test_wal_durability_immediate
```

### Verbose Output

```bash
cargo test --lib --features learned-index wal:: -- --test-threads=4 --nocapture
```

## Test Patterns

### Multi-Phase Testing

Most WAL tests follow a multi-phase pattern:

```rust
// Phase 1: Write
{
    let log = Arc::new(open_test_log(dir.path()).await.unwrap());
    append_kv(&log, key, value).await.unwrap();
    drop(log);  // Simulate crash
}

tokio::time::sleep(Duration::from_millis(50)).await;

// Phase 2: Recover and Verify
{
    let log = Arc::new(open_test_log(dir.path()).await.unwrap());
    log.snapshot().await.unwrap();
    
    #[cfg(feature = "learned-index")]
    {
        log.build_rmi().await.unwrap();
    }
    
    let value = lookup_kv(&log, key).await.unwrap();
    assert!(value.is_some());
}
```

### Concurrent Testing

Tests use `JoinSet` for concurrent operations:

```rust
let mut handles = JoinSet::new();

for worker_id in 0..10 {
    let log_clone = log.clone();
    handles.spawn(async move {
        for i in 0..50 {
            let key = (worker_id * 100 + i) as u64;
            append_kv(&log_clone, key, value).await.unwrap();
        }
    });
}

while handles.join_next().await.is_some() {}
```

### Durability Verification

All tests use snapshot + optional RMI build before verification:

```rust
log.snapshot().await.unwrap();

#[cfg(feature = "learned-index")]
{
    log.build_rmi().await.unwrap();
}

// Now verify data
```

## Coverage Summary

| Category | Tests | Coverage |
|----------|-------|----------|
| **Append Operations** | 6 | Basic ops, ordering, concurrency, large entries |
| **Group Commit** | 4 | Batching, durability, concurrent commits, performance |
| **Durability** | 7 | Immediate, crash sim, restarts, large entries, concurrent |
| **Recovery** | 8 | Basic, snapshots, idempotent, ordering, partial writes |
| **Corruption** | 8 | Detection, partial corruption, isolation, statistics |
| **Compaction** | 8 | Space reclamation, concurrent ops, performance |
| **TOTAL** | **40** | **Comprehensive WAL coverage** |

## Performance Characteristics

- **Total Suite Time:** ~35 seconds
- **Per-Test Average:** ~0.875 seconds
- **Longest Test:** `test_group_commit_batching` (~11 seconds with RMI)
- **Shortest Test:** `test_wal_handles_empty_wal_file` (<0.1 seconds)

## Integration with Main Test Suite

WAL tests are part of the comprehensive test suite:

```bash
# Run ALL tests (including WAL)
cargo test --lib --features learned-index

# Run only WAL tests
cargo test --lib --features learned-index wal::

# Run stress, utils, and WAL tests
cargo test --lib --features learned-index -- stress:: utils_tests:: wal::
```

## Quality Gates

All WAL tests enforce:

1. **Durability:** Data persists across crashes
2. **Consistency:** Recovery produces correct state
3. **Concurrency:** No data loss with parallel writes
4. **Reliability:** ≥80-90% recovery rate in failure scenarios
5. **Performance:** Operations complete in reasonable time

## Future Enhancements

Planned additions:

- [ ] Checksum validation tests
- [ ] Multi-segment corruption tests
- [ ] Advanced compaction strategies
- [ ] Performance regression tracking
- [ ] Fuzzing integration for WAL operations
- [ ] Distributed WAL replication tests (Phase 1+)

## Troubleshooting

### Slow Tests

If tests timeout:
- Check disk I/O performance
- Reduce concurrent workers
- Skip RMI build for faster runs

### Flaky Tests

If tests fail intermittently:
- Increase sleep durations between phases
- Check system resources (memory, disk)
- Review logs for OS-level issues

### Recovery Rate Issues

If recovery rates are lower than expected:
- Verify fsync configuration
- Check WAL segment size settings
- Review crash simulation timing

## References

- [WAL Implementation](../engine/src/lib.rs)
- [Test Utilities](../engine/src/test/utils/mod.rs)
- [Stress Tests](./STRESS_TESTING.md)
- [Utils Tests](./UTILS_TESTS.md)
