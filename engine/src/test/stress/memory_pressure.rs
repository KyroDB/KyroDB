//! Memory Pressure Stress Tests
//!
//! Tests system behavior under memory constraints and high memory usage scenarios

use crate::test::utils::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::time::Instant;

/// Test buffer overflow recovery
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_buffer_overflow_recovery() {
    let data_dir = test_data_dir();
    let log = Arc::new(open_test_log(data_dir.path()).await.unwrap());

    const RAPID_WRITES: usize = 100_000;

    println!(
        "\n📝 Testing buffer overflow with {} rapid writes...",
        RAPID_WRITES
    );
    let start_time = Instant::now();

    let success_count = Arc::new(AtomicUsize::new(0));
    let overflow_count = Arc::new(AtomicUsize::new(0));

    let mut handles = vec![];

    for i in 0..RAPID_WRITES {
        let log_clone = log.clone();
        let success = success_count.clone();
        let overflow = overflow_count.clone();

        let handle = tokio::spawn(async move {
            let key = i as u64;
            let value = vec![0u8; 256]; // 256 bytes per value

            match append_kv(&log_clone, key, value).await {
                Ok(_) => {
                    success.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    let err_str = e.to_string();
                    if err_str.contains("overflow") || err_str.contains("buffer") {
                        overflow.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        });

        handles.push(handle);
    }

    // Wait for all writes
    for handle in handles {
        handle.await.unwrap();
    }

    let elapsed = start_time.elapsed();
    let total_success = success_count.load(Ordering::Relaxed);
    let total_overflow = overflow_count.load(Ordering::Relaxed);

    println!("\n📊 Buffer Overflow Recovery Results:");
    println!("   Successful writes: {}", total_success);
    println!("   Overflow events: {}", total_overflow);
    println!("   Duration: {:?}", elapsed);
    println!(
        "   Throughput: {:.2} ops/sec",
        total_success as f64 / elapsed.as_secs_f64()
    );

    // System should handle gracefully even if some writes fail
    assert!(
        total_success >= RAPID_WRITES * 60 / 100,
        "Should have at least 60% success rate under extreme pressure"
    );

    // Allow system to recover
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Verify writes can continue after overflow
    println!("\n🔍 Verifying recovery...");
    for i in 0..100 {
        let key = (RAPID_WRITES + i) as u64;
        let value = format!("recovery_value_{}", i).into_bytes();

        assert!(
            append_kv(&log, key, value).await.is_ok(),
            "System should recover after overflow"
        );
    }

    println!("   ✅ System recovered successfully");
}

/// Test memory-intensive mixed workload
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_memory_intensive_workload() {
    let data_dir = test_data_dir();
    let log = Arc::new(open_test_log(data_dir.path()).await.unwrap());

    const NUM_OPERATIONS: usize = 50_000;
    const LARGE_VALUE_SIZE: usize = 4096; // 4KB values

    println!(
        "\n📝 Running memory-intensive workload with {} ops...",
        NUM_OPERATIONS
    );
    let start_time = Instant::now();

    let success_count = Arc::new(AtomicUsize::new(0));

    for i in 0..NUM_OPERATIONS {
        let key = i as u64;
        let value = vec![(i % 256) as u8; LARGE_VALUE_SIZE];

        if append_kv(&log, key, value).await.is_ok() {
            success_count.fetch_add(1, Ordering::Relaxed);
        }

        if i % 5000 == 0 && i > 0 {
            let current = success_count.load(Ordering::Relaxed);
            println!("   Progress: {}/{} ops completed", current, NUM_OPERATIONS);
        }
    }

    let elapsed = start_time.elapsed();
    let total_success = success_count.load(Ordering::Relaxed);
    let total_mb = (total_success * LARGE_VALUE_SIZE) / (1024 * 1024);

    println!("\n📊 Memory Intensive Results:");
    println!("   Successful writes: {}", total_success);
    println!("   Total data: ~{}MB", total_mb);
    println!("   Duration: {:?}", elapsed);
    println!(
        "   Throughput: {:.2} ops/sec",
        total_success as f64 / elapsed.as_secs_f64()
    );
    println!(
        "   Data throughput: {:.2} MB/sec",
        total_mb as f64 / elapsed.as_secs_f64()
    );

    assert!(
        total_success >= NUM_OPERATIONS * 90 / 100,
        "Should complete at least 90% of operations"
    );
}

/// Test snapshot creation under memory pressure
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_snapshot_under_memory_pressure() {
    let data_dir = test_data_dir();
    let log = Arc::new(open_test_log(data_dir.path()).await.unwrap());

    const NUM_KEYS: usize = 100_000;
    const VALUE_SIZE: usize = 512;

    println!(
        "\n📝 Pre-populating {} keys with {}B values...",
        NUM_KEYS, VALUE_SIZE
    );

    for i in 0..NUM_KEYS {
        let key = i as u64;
        let value = vec![0xAB; VALUE_SIZE];
        append_kv(&log, key, value).await.unwrap();

        if i % 10_000 == 0 && i > 0 {
            println!("   Progress: {}/{} keys", i, NUM_KEYS);
        }
    }

    println!("\n📸 Creating snapshot under memory pressure...");
    let snapshot_start = Instant::now();

    assert!(
        log.snapshot().await.is_ok(),
        "Snapshot should succeed under memory pressure"
    );

    let snapshot_elapsed = snapshot_start.elapsed();

    println!("\n📊 Snapshot Under Pressure Results:");
    println!("   Keys snapshotted: {}", NUM_KEYS);
    println!("   Snapshot duration: {:?}", snapshot_elapsed);
    println!(
        "   Throughput: {:.2} keys/sec",
        NUM_KEYS as f64 / snapshot_elapsed.as_secs_f64()
    );

    // Verify snapshot integrity
    println!("\n🔍 Verifying snapshot integrity...");
    for i in (0..NUM_KEYS).step_by(1000) {
        let value = lookup_kv(&log, i as u64).await.unwrap();
        assert!(value.is_some(), "Key {} should be found after snapshot", i);
        assert_eq!(
            value.unwrap().len(),
            VALUE_SIZE,
            "Value size mismatch for key {}",
            i
        );
    }

    println!("   ✅ Snapshot integrity verified");
}

/// Test concurrent snapshots (stress test snapshot locking)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_snapshot_attempts() {
    let data_dir = test_data_dir();
    let log = Arc::new(open_test_log(data_dir.path()).await.unwrap());

    // Pre-populate some data
    for i in 0..10_000 {
        append_kv(&log, i, format!("value_{}", i).into_bytes())
            .await
            .unwrap();
    }

    const NUM_CONCURRENT_SNAPSHOTS: usize = 10;

    println!(
        "\n📸 Attempting {} concurrent snapshots...",
        NUM_CONCURRENT_SNAPSHOTS
    );
    let start_time = Instant::now();

    let success_count = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for i in 0..NUM_CONCURRENT_SNAPSHOTS {
        let log_clone = log.clone();
        let counter = success_count.clone();

        let handle = tokio::spawn(async move {
            match log_clone.snapshot().await {
                Ok(_) => {
                    counter.fetch_add(1, Ordering::Relaxed);
                    println!("   Snapshot {} completed", i);
                }
                Err(e) => {
                    println!("   Snapshot {} failed: {}", i, e);
                }
            }
        });

        handles.push(handle);
    }

    // Wait for all attempts
    for handle in handles {
        handle.await.unwrap();
    }

    let elapsed = start_time.elapsed();
    let total_success = success_count.load(Ordering::Relaxed);

    println!("\n📊 Concurrent Snapshots Results:");
    println!(
        "   Successful snapshots: {}/{}",
        total_success, NUM_CONCURRENT_SNAPSHOTS
    );
    println!("   Duration: {:?}", elapsed);

    // At least some should succeed (system may serialize them)
    assert!(total_success > 0, "At least one snapshot should succeed");
}

/// Test RMI rebuild under memory pressure
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[cfg(feature = "learned-index")]
async fn test_rmi_rebuild_memory_pressure() {
    let data_dir = test_data_dir();
    let log = Arc::new(open_test_log(data_dir.path()).await.unwrap());

    const NUM_KEYS: usize = 200_000;

    println!("\n📝 Pre-populating {} keys...", NUM_KEYS);

    for i in 0..NUM_KEYS {
        let key = i as u64;
        let value = format!("pressure_value_{}", i).into_bytes();
        append_kv(&log, key, value).await.unwrap();

        if i % 20_000 == 0 && i > 0 {
            println!("   Progress: {}/{} keys", i, NUM_KEYS);
        }
    }

    println!("\n🔧 Building RMI under memory pressure...");
    log.snapshot().await.unwrap();

    let rmi_start = Instant::now();
    assert!(
        log.build_rmi().await.is_ok(),
        "RMI build should succeed under memory pressure"
    );
    let rmi_elapsed = rmi_start.elapsed();

    println!("\n📊 RMI Build Under Pressure Results:");
    println!("   Keys indexed: {}", NUM_KEYS);
    println!("   Build duration: {:?}", rmi_elapsed);
    println!(
        "   Throughput: {:.2} keys/sec",
        NUM_KEYS as f64 / rmi_elapsed.as_secs_f64()
    );

    // Verify RMI accuracy
    println!("\n🔍 Verifying RMI accuracy...");
    let mut found_count = 0;
    for i in (0..NUM_KEYS).step_by(100) {
        if lookup_kv(&log, i as u64).await.unwrap().is_some() {
            found_count += 1;
        }
    }

    let expected = NUM_KEYS / 100;
    println!("   Found {}/{} sampled keys", found_count, expected);
    assert_eq!(
        found_count, expected,
        "RMI should maintain accuracy under pressure"
    );
}

/// Test WAL growth and compaction under pressure
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_wal_growth_under_pressure() {
    let data_dir = test_data_dir();
    let log = Arc::new(open_test_log(data_dir.path()).await.unwrap());

    const NUM_KEYS: usize = 50_000;
    const UPDATES_PER_KEY: usize = 5;

    println!(
        "\n📝 Testing WAL growth with {} keys, {} updates each...",
        NUM_KEYS, UPDATES_PER_KEY
    );

    let start_time = Instant::now();

    for update_round in 0..UPDATES_PER_KEY {
        println!(
            "   Update round {}/{}...",
            update_round + 1,
            UPDATES_PER_KEY
        );

        for i in 0..NUM_KEYS {
            let key = i as u64;
            let value = format!("update_{}_{}", update_round, i).into_bytes();
            append_kv(&log, key, value).await.unwrap();
        }
    }

    let elapsed = start_time.elapsed();
    let total_ops = NUM_KEYS * UPDATES_PER_KEY;

    println!("\n📊 WAL Growth Results:");
    println!("   Total operations: {}", total_ops);
    println!("   Duration: {:?}", elapsed);
    println!(
        "   Throughput: {:.2} ops/sec",
        total_ops as f64 / elapsed.as_secs_f64()
    );

    // Check WAL size
    let wal_size = log.wal_size_bytes().await;
    println!("   WAL size: {:.2} MB", wal_size as f64 / (1024.0 * 1024.0));

    // Trigger snapshot to compact WAL
    println!("\n📸 Creating snapshot to trigger WAL compaction...");
    let snapshot_start = Instant::now();
    log.snapshot().await.unwrap();
    let snapshot_elapsed = snapshot_start.elapsed();

    println!("   Snapshot duration: {:?}", snapshot_elapsed);

    // Verify latest values
    println!("\n🔍 Verifying latest values...");
    for i in (0..NUM_KEYS).step_by(100) {
        let value = lookup_kv(&log, i as u64).await.unwrap().unwrap();
        let expected = format!("update_{}_{}", UPDATES_PER_KEY - 1, i);
        assert_eq!(
            String::from_utf8(value).unwrap(),
            expected,
            "Should have latest value for key {}",
            i
        );
    }

    println!("   ✅ All values verified");
}
