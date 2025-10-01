//! Concurrent Operations Stress Tests
//!
//! Tests extreme concurrent load scenarios with hundreds to thousands of simultaneous operations

use crate::test::utils::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::time::{Duration, Instant};

/// Test 1000 concurrent writers with hot buffer check
#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn test_1000_concurrent_writers() {
    let data_dir = test_data_dir();
    let log = Arc::new(open_test_log(data_dir.path()).await.unwrap());

    const NUM_THREADS: usize = 1000;
    const WRITES_PER_THREAD: usize = 100;

    let success_count = Arc::new(AtomicUsize::new(0));
    let error_count = Arc::new(AtomicUsize::new(0));
    let start_time = Instant::now();

    let mut handles = vec![];

    for thread_id in 0..NUM_THREADS {
        let log_clone = log.clone();
        let success = success_count.clone();
        let errors = error_count.clone();

        let handle = tokio::spawn(async move {
            for i in 0..WRITES_PER_THREAD {
                let key = (thread_id as u64 * 1000000) + i as u64;
                let value = format!("thread_{}_value_{}", thread_id, i).into_bytes();

                match append_kv(&log_clone, key, value).await {
                    Ok(_) => {
                        success.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        eprintln!("Write error: {}", e);
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        });

        handles.push(handle);
    }

    // Wait for all writes to complete
    for handle in handles {
        handle.await.unwrap();
    }

    let elapsed = start_time.elapsed();
    let total_ops = success_count.load(Ordering::Relaxed);
    let errors = error_count.load(Ordering::Relaxed);

    println!("\n📊 1000 Concurrent Writers Results:");
    println!("   Total writes: {}", total_ops);
    println!("   Errors: {}", errors);
    println!("   Duration: {:?}", elapsed);
    println!(
        "   Throughput: {:.2} ops/sec",
        total_ops as f64 / elapsed.as_secs_f64()
    );

    assert!(
        total_ops >= (NUM_THREADS * WRITES_PER_THREAD * 95) / 100,
        "Should have at least 95% success rate"
    );
}

/// Test 1000 concurrent readers with immediate consistency
#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn test_1000_concurrent_readers() {
    let data_dir = test_data_dir();
    let log = Arc::new(open_test_log(data_dir.path()).await.unwrap());

    // Pre-populate data
    const NUM_KEYS: usize = 10000;
    println!("📝 Pre-populating {} keys...", NUM_KEYS);
    for i in 0..NUM_KEYS {
        append_kv(&log, i as u64, format!("value_{}", i).into_bytes())
            .await
            .unwrap();
    }

    // Build RMI for reads
    #[cfg(feature = "learned-index")]
    {
        log.snapshot().await.unwrap();
        log.build_rmi().await.unwrap();
    }

    const NUM_THREADS: usize = 1000;
    const READS_PER_THREAD: usize = 100;

    let success_count = Arc::new(AtomicUsize::new(0));
    let not_found_count = Arc::new(AtomicUsize::new(0));
    let start_time = Instant::now();

    let mut handles = vec![];

    for thread_id in 0..NUM_THREADS {
        let log_clone = log.clone();
        let success = success_count.clone();
        let not_found = not_found_count.clone();

        let handle = tokio::spawn(async move {
            for i in 0..READS_PER_THREAD {
                let key = (i * NUM_THREADS + thread_id) as u64 % NUM_KEYS as u64;

                match lookup_kv(&log_clone, key).await {
                    Ok(Some(_)) => {
                        success.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(None) => {
                        not_found.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        eprintln!("Read error: {}", e);
                    }
                }
            }
        });

        handles.push(handle);
    }

    // Wait for all reads
    for handle in handles {
        handle.await.unwrap();
    }

    let elapsed = start_time.elapsed();
    let total_ops = success_count.load(Ordering::Relaxed);
    let not_found = not_found_count.load(Ordering::Relaxed);

    println!("\n📊 1000 Concurrent Readers Results:");
    println!("   Successful reads: {}", total_ops);
    println!("   Not found: {}", not_found);
    println!("   Duration: {:?}", elapsed);
    println!(
        "   Throughput: {:.2} ops/sec",
        total_ops as f64 / elapsed.as_secs_f64()
    );

    assert!(
        total_ops >= (NUM_THREADS * READS_PER_THREAD * 95) / 100,
        "Should have at least 95% success rate"
    );
}

/// Test mixed workload: 500 writers + 500 readers
#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn test_mixed_workload_1000_threads() {
    let data_dir = test_data_dir();
    let log = Arc::new(open_test_log(data_dir.path()).await.unwrap());

    const NUM_WRITER_THREADS: usize = 500;
    const NUM_READER_THREADS: usize = 500;
    const OPS_PER_THREAD: usize = 100;

    let write_count = Arc::new(AtomicUsize::new(0));
    let read_count = Arc::new(AtomicUsize::new(0));
    let start_time = Instant::now();

    let mut handles = vec![];

    // Spawn writers
    for thread_id in 0..NUM_WRITER_THREADS {
        let log_clone = log.clone();
        let counter = write_count.clone();

        let handle = tokio::spawn(async move {
            for i in 0..OPS_PER_THREAD {
                let key = (thread_id as u64 * 1000000) + i as u64;
                let value = format!("write_{}", i).into_bytes();

                if append_kv(&log_clone, key, value).await.is_ok() {
                    counter.fetch_add(1, Ordering::Relaxed);
                }
            }
        });

        handles.push(handle);
    }

    // Spawn readers
    for thread_id in 0..NUM_READER_THREADS {
        let log_clone = log.clone();
        let counter = read_count.clone();

        let handle = tokio::spawn(async move {
            for i in 0..OPS_PER_THREAD {
                let key = (thread_id as u64 * 1000000) + i as u64;

                if lookup_kv(&log_clone, key).await.is_ok() {
                    counter.fetch_add(1, Ordering::Relaxed);
                }
            }
        });

        handles.push(handle);
    }

    // Wait for all operations
    for handle in handles {
        handle.await.unwrap();
    }

    let elapsed = start_time.elapsed();
    let total_writes = write_count.load(Ordering::Relaxed);
    let total_reads = read_count.load(Ordering::Relaxed);
    let total_ops = total_writes + total_reads;

    println!("\n📊 Mixed Workload (1000 threads) Results:");
    println!("   Writes: {}", total_writes);
    println!("   Reads: {}", total_reads);
    println!("   Total ops: {}", total_ops);
    println!("   Duration: {:?}", elapsed);
    println!(
        "   Throughput: {:.2} ops/sec",
        total_ops as f64 / elapsed.as_secs_f64()
    );

    assert!(
        total_writes >= (NUM_WRITER_THREADS * OPS_PER_THREAD * 90) / 100,
        "Writers should have at least 90% success rate"
    );
}

/// Test hot buffer overflow under extreme concurrent writes
#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn test_hot_buffer_overflow_stress() {
    let data_dir = test_data_dir();
    let log = Arc::new(open_test_log(data_dir.path()).await.unwrap());

    const NUM_THREADS: usize = 200;
    const WRITES_PER_THREAD: usize = 1000; // Total: 200K writes

    let success_count = Arc::new(AtomicUsize::new(0));
    let overflow_count = Arc::new(AtomicUsize::new(0));
    let start_time = Instant::now();

    let mut handles = vec![];

    for thread_id in 0..NUM_THREADS {
        let log_clone = log.clone();
        let success = success_count.clone();
        let overflow = overflow_count.clone();

        let handle = tokio::spawn(async move {
            for i in 0..WRITES_PER_THREAD {
                let key = (thread_id as u64 * 1000000) + i as u64;
                let value = vec![0u8; 100]; // 100 bytes per value

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

    println!("\n📊 Hot Buffer Overflow Stress Results:");
    println!("   Successful writes: {}", total_success);
    println!("   Overflow errors: {}", total_overflow);
    println!("   Duration: {:?}", elapsed);
    println!(
        "   Throughput: {:.2} ops/sec",
        total_success as f64 / elapsed.as_secs_f64()
    );

    // Should handle gracefully even if some writes overflow
    assert!(
        total_success >= (NUM_THREADS * WRITES_PER_THREAD * 80) / 100,
        "Should have at least 80% success rate even under extreme load"
    );
}

/// Test lock contention with rapid snapshot operations
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_snapshot_under_concurrent_load() {
    let data_dir = test_data_dir();
    let log = Arc::new(open_test_log(data_dir.path()).await.unwrap());

    const NUM_WRITER_THREADS: usize = 100;
    const NUM_SNAPSHOT_THREADS: usize = 10;
    const DURATION_SECS: u64 = 10;

    let write_count = Arc::new(AtomicUsize::new(0));
    let snapshot_count = Arc::new(AtomicUsize::new(0));
    let running = Arc::new(AtomicUsize::new(1));

    let start_time = Instant::now();
    let mut handles = vec![];

    // Spawn writer threads
    for thread_id in 0..NUM_WRITER_THREADS {
        let log_clone = log.clone();
        let counter = write_count.clone();
        let running_flag = running.clone();

        let handle = tokio::spawn(async move {
            let mut local_count = 0u64;
            while running_flag.load(Ordering::Relaxed) == 1 {
                let key = (thread_id as u64 * 1000000) + local_count;
                let value = format!("value_{}", local_count).into_bytes();

                if append_kv(&log_clone, key, value).await.is_ok() {
                    counter.fetch_add(1, Ordering::Relaxed);
                    local_count += 1;
                }

                // Small yield to allow other threads
                tokio::task::yield_now().await;
            }
        });

        handles.push(handle);
    }

    // Spawn snapshot threads
    for _ in 0..NUM_SNAPSHOT_THREADS {
        let log_clone = log.clone();
        let counter = snapshot_count.clone();
        let running_flag = running.clone();

        let handle = tokio::spawn(async move {
            while running_flag.load(Ordering::Relaxed) == 1 {
                if log_clone.snapshot().await.is_ok() {
                    counter.fetch_add(1, Ordering::Relaxed);
                }

                // Wait between snapshots
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        });

        handles.push(handle);
    }

    // Run for specified duration
    tokio::time::sleep(Duration::from_secs(DURATION_SECS)).await;
    running.store(0, Ordering::Relaxed);

    // Wait for all threads to finish
    for handle in handles {
        handle.await.unwrap();
    }

    let elapsed = start_time.elapsed();
    let total_writes = write_count.load(Ordering::Relaxed);
    let total_snapshots = snapshot_count.load(Ordering::Relaxed);

    println!("\n📊 Snapshot Under Load Results:");
    println!("   Total writes: {}", total_writes);
    println!("   Total snapshots: {}", total_snapshots);
    println!("   Duration: {:?}", elapsed);
    println!(
        "   Write throughput: {:.2} ops/sec",
        total_writes as f64 / elapsed.as_secs_f64()
    );
    println!(
        "   Snapshots/sec: {:.2}",
        total_snapshots as f64 / elapsed.as_secs_f64()
    );

    assert!(total_writes > 0, "Should complete some writes");
    assert!(total_snapshots > 0, "Should complete some snapshots");
}

/// Test RMI rebuild under concurrent operations
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[cfg(feature = "learned-index")]
async fn test_rmi_rebuild_under_load() {
    let data_dir = test_data_dir();
    let log = Arc::new(open_test_log(data_dir.path()).await.unwrap());

    // Pre-populate some data
    for i in 0..1000 {
        append_kv(&log, i, format!("initial_{}", i).into_bytes())
            .await
            .unwrap();
    }

    const NUM_WRITER_THREADS: usize = 50;
    const NUM_READER_THREADS: usize = 50;
    const NUM_RMI_REBUILDS: usize = 5;
    const DURATION_SECS: u64 = 15;

    let write_count = Arc::new(AtomicUsize::new(0));
    let read_count = Arc::new(AtomicUsize::new(0));
    let rebuild_count = Arc::new(AtomicUsize::new(0));
    let running = Arc::new(AtomicUsize::new(1));

    let start_time = Instant::now();
    let mut handles = vec![];

    // Spawn writers
    for thread_id in 0..NUM_WRITER_THREADS {
        let log_clone = log.clone();
        let counter = write_count.clone();
        let running_flag = running.clone();

        let handle = tokio::spawn(async move {
            let mut local_count = 0u64;
            while running_flag.load(Ordering::Relaxed) == 1 {
                let key = (thread_id as u64 * 1000000) + local_count;
                let value = format!("concurrent_{}", local_count).into_bytes();

                if append_kv(&log_clone, key, value).await.is_ok() {
                    counter.fetch_add(1, Ordering::Relaxed);
                    local_count += 1;
                }
            }
        });

        handles.push(handle);
    }

    // Spawn readers
    for _thread_id in 0..NUM_READER_THREADS {
        let log_clone = log.clone();
        let counter = read_count.clone();
        let running_flag = running.clone();

        let handle = tokio::spawn(async move {
            let mut local_count = 0u64;
            while running_flag.load(Ordering::Relaxed) == 1 {
                let key = local_count % 1000;

                if lookup_kv(&log_clone, key).await.is_ok() {
                    counter.fetch_add(1, Ordering::Relaxed);
                }

                local_count += 1;
            }
        });

        handles.push(handle);
    }

    // Spawn RMI rebuilder
    let log_rebuild = log.clone();
    let rebuild_counter = rebuild_count.clone();
    let running_rebuild = running.clone();

    let rebuild_handle = tokio::spawn(async move {
        for _ in 0..NUM_RMI_REBUILDS {
            if running_rebuild.load(Ordering::Relaxed) == 0 {
                break;
            }

            // Snapshot, rebuild, warmup
            if log_rebuild.snapshot().await.is_ok() {
                if log_rebuild.build_rmi().await.is_ok() {
                    rebuild_counter.fetch_add(1, Ordering::Relaxed);
                }
            }

            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    });

    // Run for specified duration
    tokio::time::sleep(Duration::from_secs(DURATION_SECS)).await;
    running.store(0, Ordering::Relaxed);

    // Wait for all threads
    for handle in handles {
        handle.await.unwrap();
    }
    rebuild_handle.await.unwrap();

    let elapsed = start_time.elapsed();
    let total_writes = write_count.load(Ordering::Relaxed);
    let total_reads = read_count.load(Ordering::Relaxed);
    let total_rebuilds = rebuild_count.load(Ordering::Relaxed);

    println!("\n📊 RMI Rebuild Under Load Results:");
    println!("   Total writes: {}", total_writes);
    println!("   Total reads: {}", total_reads);
    println!("   RMI rebuilds: {}", total_rebuilds);
    println!("   Duration: {:?}", elapsed);
    println!(
        "   Write throughput: {:.2} ops/sec",
        total_writes as f64 / elapsed.as_secs_f64()
    );
    println!(
        "   Read throughput: {:.2} ops/sec",
        total_reads as f64 / elapsed.as_secs_f64()
    );

    assert!(total_writes > 0, "Should complete writes during rebuilds");
    assert!(total_reads > 0, "Should complete reads during rebuilds");
    assert!(
        total_rebuilds >= NUM_RMI_REBUILDS - 1,
        "Should complete most RMI rebuilds"
    );
}
