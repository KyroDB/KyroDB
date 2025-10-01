//! Lock Contention Stress Tests
//!
//! Tests lock ordering and contention under extreme concurrent access patterns

use crate::test::utils::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::time::{Duration, Instant};

/// Test hot buffer lock contention with rapid concurrent access
#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn test_hot_buffer_lock_contention() {
    let data_dir = test_data_dir();
    let log = Arc::new(open_test_log(data_dir.path()).await.unwrap());

    const NUM_THREADS: usize = 100;
    const OPS_PER_THREAD: usize = 1000;
    const DURATION_SECS: u64 = 5;

    println!(
        "\n🔒 Testing hot buffer lock contention with {} threads...",
        NUM_THREADS
    );

    let success_count = Arc::new(AtomicUsize::new(0));
    let running = Arc::new(AtomicBool::new(true));
    let start_time = Instant::now();

    let mut handles = vec![];

    for thread_id in 0..NUM_THREADS {
        let log_clone = log.clone();
        let counter = success_count.clone();
        let running_flag = running.clone();

        let handle = tokio::spawn(async move {
            let mut local_count = 0;

            while running_flag.load(Ordering::Relaxed) {
                let key = (thread_id as u64 * 1_000_000) + local_count;
                let value = vec![0u8; 64];

                if append_kv(&log_clone, key, value).await.is_ok() {
                    counter.fetch_add(1, Ordering::Relaxed);
                    local_count += 1;
                }

                // Yield to create contention
                if local_count % 10 == 0 {
                    tokio::task::yield_now().await;
                }

                if local_count >= OPS_PER_THREAD as u64 {
                    break;
                }
            }
        });

        handles.push(handle);
    }

    // Run for duration
    tokio::time::sleep(Duration::from_secs(DURATION_SECS)).await;
    running.store(false, Ordering::Relaxed);

    // Wait for completion
    for handle in handles {
        handle.await.unwrap();
    }

    let elapsed = start_time.elapsed();
    let total_ops = success_count.load(Ordering::Relaxed);

    println!("\n📊 Hot Buffer Lock Contention Results:");
    println!("   Operations completed: {}", total_ops);
    println!("   Duration: {:?}", elapsed);
    println!(
        "   Throughput: {:.2} ops/sec",
        total_ops as f64 / elapsed.as_secs_f64()
    );
    println!(
        "   Avg ops/thread: {:.2}",
        total_ops as f64 / NUM_THREADS as f64
    );

    assert!(
        total_ops > 0,
        "Should complete operations despite contention"
    );
}

/// Test overflow buffer lock contention
#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn test_overflow_buffer_lock_contention() {
    let data_dir = test_data_dir();
    let log = Arc::new(open_test_log(data_dir.path()).await.unwrap());

    // Fill hot buffer first to force overflow
    println!("\n📝 Pre-filling hot buffer to trigger overflow...");
    for i in 0..5000 {
        append_kv(&log, i, vec![0u8; 128]).await.ok();
    }

    const NUM_THREADS: usize = 50;
    const OPS_PER_THREAD: usize = 500;

    println!(
        "\n🔒 Testing overflow buffer lock contention with {} threads...",
        NUM_THREADS
    );

    let success_count = Arc::new(AtomicUsize::new(0));
    let overflow_count = Arc::new(AtomicUsize::new(0));
    let start_time = Instant::now();

    let mut handles = vec![];

    for thread_id in 0..NUM_THREADS {
        let log_clone = log.clone();
        let success = success_count.clone();
        let overflow = overflow_count.clone();

        let handle = tokio::spawn(async move {
            for i in 0..OPS_PER_THREAD {
                let key = (thread_id as u64 * 1_000_000) + (i as u64) + 10_000;
                let value = vec![0xFF; 128];

                match append_kv(&log_clone, key, value).await {
                    Ok(_) => {
                        success.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        if e.to_string().contains("overflow") {
                            overflow.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }

                if i % 50 == 0 {
                    tokio::task::yield_now().await;
                }
            }
        });

        handles.push(handle);
    }

    // Wait for completion
    for handle in handles {
        handle.await.unwrap();
    }

    let elapsed = start_time.elapsed();
    let total_success = success_count.load(Ordering::Relaxed);
    let total_overflow = overflow_count.load(Ordering::Relaxed);

    println!("\n📊 Overflow Buffer Lock Contention Results:");
    println!("   Successful ops: {}", total_success);
    println!("   Overflow events: {}", total_overflow);
    println!("   Duration: {:?}", elapsed);
    println!(
        "   Throughput: {:.2} ops/sec",
        total_success as f64 / elapsed.as_secs_f64()
    );

    assert!(total_success > 0, "Should complete some operations");
}

/// Test reader-writer lock contention
#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
#[cfg(feature = "learned-index")]
async fn test_reader_writer_lock_contention() {
    let data_dir = test_data_dir();
    let log = Arc::new(open_test_log(data_dir.path()).await.unwrap());

    // Pre-populate data
    println!("\n📝 Pre-populating data...");
    for i in 0..10_000 {
        append_kv(&log, i, format!("value_{}", i).into_bytes())
            .await
            .unwrap();
    }

    log.snapshot().await.unwrap();
    log.build_rmi().await.unwrap();

    const NUM_READERS: usize = 80;
    const NUM_WRITERS: usize = 20;
    const DURATION_SECS: u64 = 10;

    println!(
        "\n🔒 Testing reader-writer contention: {} readers, {} writers...",
        NUM_READERS, NUM_WRITERS
    );

    let read_count = Arc::new(AtomicUsize::new(0));
    let write_count = Arc::new(AtomicUsize::new(0));
    let running = Arc::new(AtomicBool::new(true));
    let start_time = Instant::now();

    let mut handles = vec![];

    // Spawn readers
    for reader_id in 0..NUM_READERS {
        let log_clone = log.clone();
        let counter = read_count.clone();
        let running_flag = running.clone();

        let handle = tokio::spawn(async move {
            while running_flag.load(Ordering::Relaxed) {
                let key = (reader_id % 10_000) as u64;

                if lookup_kv(&log_clone, key).await.is_ok() {
                    counter.fetch_add(1, Ordering::Relaxed);
                }

                if reader_id % 10 == 0 {
                    tokio::task::yield_now().await;
                }
            }
        });

        handles.push(handle);
    }

    // Spawn writers
    for writer_id in 0..NUM_WRITERS {
        let log_clone = log.clone();
        let counter = write_count.clone();
        let running_flag = running.clone();

        let handle = tokio::spawn(async move {
            let mut local_count = 0u64;

            while running_flag.load(Ordering::Relaxed) {
                let key = (writer_id as u64 * 1_000_000) + local_count + 20_000;
                let value = format!("writer_{}_{}", writer_id, local_count).into_bytes();

                if append_kv(&log_clone, key, value).await.is_ok() {
                    counter.fetch_add(1, Ordering::Relaxed);
                    local_count += 1;
                }

                tokio::task::yield_now().await;
            }
        });

        handles.push(handle);
    }

    // Run for duration
    tokio::time::sleep(Duration::from_secs(DURATION_SECS)).await;
    running.store(false, Ordering::Relaxed);

    // Wait for completion
    for handle in handles {
        handle.await.unwrap();
    }

    let elapsed = start_time.elapsed();
    let total_reads = read_count.load(Ordering::Relaxed);
    let total_writes = write_count.load(Ordering::Relaxed);
    let total_ops = total_reads + total_writes;

    println!("\n📊 Reader-Writer Lock Contention Results:");
    println!("   Reads: {}", total_reads);
    println!("   Writes: {}", total_writes);
    println!("   Total ops: {}", total_ops);
    println!("   Duration: {:?}", elapsed);
    println!(
        "   Read throughput: {:.2} ops/sec",
        total_reads as f64 / elapsed.as_secs_f64()
    );
    println!(
        "   Write throughput: {:.2} ops/sec",
        total_writes as f64 / elapsed.as_secs_f64()
    );
    println!(
        "   Total throughput: {:.2} ops/sec",
        total_ops as f64 / elapsed.as_secs_f64()
    );

    assert!(total_reads > 0, "Should complete reads under contention");
    assert!(total_writes > 0, "Should complete writes under contention");
}

/// Test snapshot lock contention with concurrent operations
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_snapshot_lock_contention() {
    let data_dir = test_data_dir();
    let log = Arc::new(open_test_log(data_dir.path()).await.unwrap());

    // Pre-populate
    for i in 0..5_000 {
        append_kv(&log, i, format!("initial_{}", i).into_bytes())
            .await
            .unwrap();
    }

    const NUM_SNAPSHOT_THREADS: usize = 10;
    const NUM_WRITER_THREADS: usize = 20;
    const DURATION_SECS: u64 = 8;

    println!("\n🔒 Testing snapshot lock contention...");

    let snapshot_count = Arc::new(AtomicUsize::new(0));
    let write_count = Arc::new(AtomicUsize::new(0));
    let running = Arc::new(AtomicBool::new(true));
    let start_time = Instant::now();

    let mut handles = vec![];

    // Spawn snapshot threads
    for _ in 0..NUM_SNAPSHOT_THREADS {
        let log_clone = log.clone();
        let counter = snapshot_count.clone();
        let running_flag = running.clone();

        let handle = tokio::spawn(async move {
            while running_flag.load(Ordering::Relaxed) {
                if log_clone.snapshot().await.is_ok() {
                    counter.fetch_add(1, Ordering::Relaxed);
                }

                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        });

        handles.push(handle);
    }

    // Spawn writers
    for thread_id in 0..NUM_WRITER_THREADS {
        let log_clone = log.clone();
        let counter = write_count.clone();
        let running_flag = running.clone();

        let handle = tokio::spawn(async move {
            let mut local_count = 0u64;

            while running_flag.load(Ordering::Relaxed) {
                let key = (thread_id as u64 * 1_000_000) + local_count + 10_000;
                let value = format!("concurrent_{}", local_count).into_bytes();

                if append_kv(&log_clone, key, value).await.is_ok() {
                    counter.fetch_add(1, Ordering::Relaxed);
                    local_count += 1;
                }

                if local_count % 10 == 0 {
                    tokio::task::yield_now().await;
                }
            }
        });

        handles.push(handle);
    }

    // Run for duration
    tokio::time::sleep(Duration::from_secs(DURATION_SECS)).await;
    running.store(false, Ordering::Relaxed);

    // Wait for completion
    for handle in handles {
        handle.await.unwrap();
    }

    let elapsed = start_time.elapsed();
    let total_snapshots = snapshot_count.load(Ordering::Relaxed);
    let total_writes = write_count.load(Ordering::Relaxed);

    println!("\n📊 Snapshot Lock Contention Results:");
    println!("   Snapshots completed: {}", total_snapshots);
    println!("   Writes completed: {}", total_writes);
    println!("   Duration: {:?}", elapsed);
    println!(
        "   Snapshot rate: {:.2}/sec",
        total_snapshots as f64 / elapsed.as_secs_f64()
    );
    println!(
        "   Write throughput: {:.2} ops/sec",
        total_writes as f64 / elapsed.as_secs_f64()
    );

    assert!(
        total_snapshots > 0,
        "Should complete snapshots despite contention"
    );
    assert!(
        total_writes > 0,
        "Should complete writes despite contention"
    );
}

/// Test no deadlocks under extreme contention
#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
#[cfg(feature = "learned-index")]
async fn test_no_deadlocks_extreme_contention() {
    let data_dir = test_data_dir();
    let log = Arc::new(open_test_log(data_dir.path()).await.unwrap());

    // Pre-populate
    for i in 0..5_000 {
        append_kv(&log, i, format!("initial_{}", i).into_bytes())
            .await
            .unwrap();
    }

    log.snapshot().await.unwrap();
    log.build_rmi().await.unwrap();

    const NUM_READERS: usize = 40;
    const NUM_WRITERS: usize = 30;
    const NUM_SNAPSHOT_THREADS: usize = 5;
    const NUM_RMI_REBUILDERS: usize = 3;
    const DURATION_SECS: u64 = 15;

    println!("\n🔒 Testing for deadlocks under extreme contention...");
    println!(
        "   {} readers, {} writers, {} snapshotters, {} RMI rebuilders",
        NUM_READERS, NUM_WRITERS, NUM_SNAPSHOT_THREADS, NUM_RMI_REBUILDERS
    );

    let read_count = Arc::new(AtomicUsize::new(0));
    let write_count = Arc::new(AtomicUsize::new(0));
    let snapshot_count = Arc::new(AtomicUsize::new(0));
    let rebuild_count = Arc::new(AtomicUsize::new(0));
    let running = Arc::new(AtomicBool::new(true));

    let start_time = Instant::now();
    let mut handles = vec![];

    // Readers
    for reader_id in 0..NUM_READERS {
        let log_clone = log.clone();
        let counter = read_count.clone();
        let running_flag = running.clone();

        let handle = tokio::spawn(async move {
            let mut i = 0;
            while running_flag.load(Ordering::Relaxed) {
                let key = (i * 997 + reader_id * 31) as u64 % 10_000;
                i += 1;
                if lookup_kv(&log_clone, key).await.is_ok() {
                    counter.fetch_add(1, Ordering::Relaxed);
                }
            }
        });

        handles.push(handle);
    }

    // Writers
    for thread_id in 0..NUM_WRITERS {
        let log_clone = log.clone();
        let counter = write_count.clone();
        let running_flag = running.clone();

        let handle = tokio::spawn(async move {
            let mut local_count = 0u64;
            while running_flag.load(Ordering::Relaxed) {
                let key = (thread_id as u64 * 1_000_000) + local_count;
                let value = vec![0u8; 128];

                if append_kv(&log_clone, key, value).await.is_ok() {
                    counter.fetch_add(1, Ordering::Relaxed);
                    local_count += 1;
                }
            }
        });

        handles.push(handle);
    }

    // Snapshotters
    for _ in 0..NUM_SNAPSHOT_THREADS {
        let log_clone = log.clone();
        let counter = snapshot_count.clone();
        let running_flag = running.clone();

        let handle = tokio::spawn(async move {
            while running_flag.load(Ordering::Relaxed) {
                if log_clone.snapshot().await.is_ok() {
                    counter.fetch_add(1, Ordering::Relaxed);
                }
                tokio::time::sleep(Duration::from_millis(800)).await;
            }
        });

        handles.push(handle);
    }

    // RMI rebuilders
    for _ in 0..NUM_RMI_REBUILDERS {
        let log_clone = log.clone();
        let counter = rebuild_count.clone();
        let running_flag = running.clone();

        let handle = tokio::spawn(async move {
            while running_flag.load(Ordering::Relaxed) {
                if log_clone.build_rmi().await.is_ok() {
                    counter.fetch_add(1, Ordering::Relaxed);
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        });

        handles.push(handle);
    }

    // Run for duration
    tokio::time::sleep(Duration::from_secs(DURATION_SECS)).await;
    running.store(false, Ordering::Relaxed);

    // Wait with timeout to detect deadlocks
    let timeout = Duration::from_secs(5);
    let wait_start = Instant::now();

    for handle in handles {
        let remaining = timeout.saturating_sub(wait_start.elapsed());

        match tokio::time::timeout(remaining, handle).await {
            Ok(_) => {} // Thread completed
            Err(_) => {
                panic!("DEADLOCK DETECTED: Thread did not complete within timeout");
            }
        }
    }

    let elapsed = start_time.elapsed();
    let total_reads = read_count.load(Ordering::Relaxed);
    let total_writes = write_count.load(Ordering::Relaxed);
    let total_snapshots = snapshot_count.load(Ordering::Relaxed);
    let total_rebuilds = rebuild_count.load(Ordering::Relaxed);

    println!("\n📊 No Deadlocks Extreme Contention Results:");
    println!("   ✅ NO DEADLOCKS DETECTED");
    println!("   Reads: {}", total_reads);
    println!("   Writes: {}", total_writes);
    println!("   Snapshots: {}", total_snapshots);
    println!("   RMI rebuilds: {}", total_rebuilds);
    println!("   Duration: {:?}", elapsed);
    println!(
        "   Total throughput: {:.2} ops/sec",
        (total_reads + total_writes) as f64 / elapsed.as_secs_f64()
    );

    // All operations should complete
    assert!(total_reads > 0, "Reads should complete");
    assert!(total_writes > 0, "Writes should complete");
    assert!(total_snapshots > 0, "Snapshots should complete");
}
