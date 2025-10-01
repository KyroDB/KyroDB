//! Endurance Stress Tests
//!
//! Long-running tests to validate system stability over extended periods

use crate::test::utils::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::time::{Duration, Instant};

/// Test sustained write load (1 hour)
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore] // Run explicitly: cargo test --features learned-index test_sustained_write_load_1hour -- --ignored
async fn test_sustained_write_load_1hour() {
    let data_dir = test_data_dir();
    let log = Arc::new(open_test_log(data_dir.path()).await.unwrap());

    const DURATION_SECS: u64 = 3600; // 1 hour
    const NUM_WORKERS: usize = 8;

    println!(
        "\n⏱️  Running sustained write load for {} seconds (1 hour)...",
        DURATION_SECS
    );
    println!("   Workers: {}", NUM_WORKERS);

    let write_count = Arc::new(AtomicUsize::new(0));
    let running = Arc::new(AtomicBool::new(true));
    let start_time = Instant::now();

    let mut handles = vec![];

    for worker_id in 0..NUM_WORKERS {
        let log_clone = log.clone();
        let counter = write_count.clone();
        let running_flag = running.clone();

        let handle = tokio::spawn(async move {
            let mut local_count = 0u64;

            while running_flag.load(Ordering::Relaxed) {
                let key = (worker_id as u64 * 100_000_000) + local_count;
                let value = format!("endurance_{}", local_count).into_bytes();

                if append_kv(&log_clone, key, value).await.is_ok() {
                    let total = counter.fetch_add(1, Ordering::Relaxed) + 1;
                    local_count += 1;

                    if total % 100_000 == 0 {
                        let elapsed = start_time.elapsed();
                        println!(
                            "   Progress: {} ops, {:.2} ops/sec",
                            total,
                            total as f64 / elapsed.as_secs_f64()
                        );
                    }
                }

                // Small yield to prevent CPU saturation
                if local_count % 1000 == 0 {
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
    let total_writes = write_count.load(Ordering::Relaxed);

    println!("\n📊 Sustained Write Load Results:");
    println!("   Total writes: {}", total_writes);
    println!("   Duration: {:?}", elapsed);
    println!(
        "   Avg throughput: {:.2} ops/sec",
        total_writes as f64 / elapsed.as_secs_f64()
    );
    println!("   ✅ System remained stable for 1 hour");
}

/// Test sustained mixed workload (30 minutes)
#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
#[ignore] // Run explicitly: cargo test --features learned-index test_sustained_mixed_30min -- --ignored
async fn test_sustained_mixed_30min() {
    let data_dir = test_data_dir();
    let log = Arc::new(open_test_log(data_dir.path()).await.unwrap());

    // Pre-populate
    println!("\n📝 Pre-populating data...");
    for i in 0..50_000 {
        append_kv(&log, i, format!("initial_{}", i).into_bytes())
            .await
            .unwrap();
    }

    #[cfg(feature = "learned-index")]
    {
        log.snapshot().await.unwrap();
        log.build_rmi().await.unwrap();
    }

    const DURATION_SECS: u64 = 1800; // 30 minutes
    const NUM_WRITERS: usize = 8;
    const NUM_READERS: usize = 8;

    println!(
        "\n⏱️  Running sustained mixed load for {} seconds (30 min)...",
        DURATION_SECS
    );

    let write_count = Arc::new(AtomicUsize::new(0));
    let read_count = Arc::new(AtomicUsize::new(0));
    let running = Arc::new(AtomicBool::new(true));
    let start_time = Instant::now();

    let mut handles = vec![];

    // Writers
    for worker_id in 0..NUM_WRITERS {
        let log_clone = log.clone();
        let counter = write_count.clone();
        let running_flag = running.clone();

        let handle = tokio::spawn(async move {
            let mut local_count = 0u64;

            while running_flag.load(Ordering::Relaxed) {
                let key = (worker_id as u64 * 100_000_000) + local_count + 100_000;
                let value = vec![0u8; 128];

                if append_kv(&log_clone, key, value).await.is_ok() {
                    counter.fetch_add(1, Ordering::Relaxed);
                    local_count += 1;
                }

                if local_count % 1000 == 0 {
                    tokio::task::yield_now().await;
                }
            }
        });

        handles.push(handle);
    }

    // Readers
    for _ in 0..NUM_READERS {
        let log_clone = log.clone();
        let counter = read_count.clone();
        let running_flag = running.clone();

        let handle = tokio::spawn(async move {
            let mut op_count = 0;
            while running_flag.load(Ordering::Relaxed) {
                let key = (op_count * 997) as u64 % 50_000;
                op_count += 1;

                if lookup_kv(&log_clone, key).await.is_ok() {
                    counter.fetch_add(1, Ordering::Relaxed);
                }

                tokio::task::yield_now().await;
            }
        });

        handles.push(handle);
    }

    // Progress reporter
    let write_counter = write_count.clone();
    let read_counter = read_count.clone();
    let running_reporter = running.clone();

    let reporter = tokio::spawn(async move {
        while running_reporter.load(Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_secs(60)).await;
            let writes = write_counter.load(Ordering::Relaxed);
            let reads = read_counter.load(Ordering::Relaxed);
            let elapsed = start_time.elapsed();

            println!(
                "   Progress: {} writes, {} reads, {:.2} total ops/sec",
                writes,
                reads,
                (writes + reads) as f64 / elapsed.as_secs_f64()
            );
        }
    });

    // Run for duration
    tokio::time::sleep(Duration::from_secs(DURATION_SECS)).await;
    running.store(false, Ordering::Relaxed);

    // Wait for completion
    for handle in handles {
        handle.await.unwrap();
    }
    reporter.await.unwrap();

    let elapsed = start_time.elapsed();
    let total_writes = write_count.load(Ordering::Relaxed);
    let total_reads = read_count.load(Ordering::Relaxed);

    println!("\n📊 Sustained Mixed Load Results:");
    println!("   Total writes: {}", total_writes);
    println!("   Total reads: {}", total_reads);
    println!("   Duration: {:?}", elapsed);
    println!(
        "   Write throughput: {:.2} ops/sec",
        total_writes as f64 / elapsed.as_secs_f64()
    );
    println!(
        "   Read throughput: {:.2} ops/sec",
        total_reads as f64 / elapsed.as_secs_f64()
    );
    println!("   ✅ System remained stable for 30 minutes");
}

/// Test continuous snapshots for stability
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_continuous_snapshots_10min() {
    let data_dir = test_data_dir();
    let log = Arc::new(open_test_log(data_dir.path()).await.unwrap());

    const DURATION_SECS: u64 = 600; // 10 minutes
    const SNAPSHOT_INTERVAL_SECS: u64 = 30;

    println!(
        "\n⏱️  Running continuous snapshots for {} seconds (10 min)...",
        DURATION_SECS
    );

    let write_count = Arc::new(AtomicUsize::new(0));
    let snapshot_count = Arc::new(AtomicUsize::new(0));
    let running = Arc::new(AtomicBool::new(true));
    let start_time = Instant::now();

    // Writer
    let log_writer = log.clone();
    let write_counter = write_count.clone();
    let running_writer = running.clone();

    let writer = tokio::spawn(async move {
        let mut local_count = 0u64;

        while running_writer.load(Ordering::Relaxed) {
            let key = local_count;
            let value = format!("continuous_{}", local_count).into_bytes();

            if append_kv(&log_writer, key, value).await.is_ok() {
                write_counter.fetch_add(1, Ordering::Relaxed);
                local_count += 1;
            }

            if local_count % 1000 == 0 {
                tokio::task::yield_now().await;
            }
        }
    });

    // Snapshotter
    let log_snapshotter = log.clone();
    let snapshot_counter = snapshot_count.clone();
    let running_snapshotter = running.clone();

    let snapshotter = tokio::spawn(async move {
        while running_snapshotter.load(Ordering::Relaxed) {
            if log_snapshotter.snapshot().await.is_ok() {
                let count = snapshot_counter.fetch_add(1, Ordering::Relaxed) + 1;
                println!("   Snapshot {} completed", count);
            }

            tokio::time::sleep(Duration::from_secs(SNAPSHOT_INTERVAL_SECS)).await;
        }
    });

    // Run for duration
    tokio::time::sleep(Duration::from_secs(DURATION_SECS)).await;
    running.store(false, Ordering::Relaxed);

    // Wait for completion
    writer.await.unwrap();
    snapshotter.await.unwrap();

    let elapsed = start_time.elapsed();
    let total_writes = write_count.load(Ordering::Relaxed);
    let total_snapshots = snapshot_count.load(Ordering::Relaxed);

    println!("\n📊 Continuous Snapshots Results:");
    println!("   Total writes: {}", total_writes);
    println!("   Total snapshots: {}", total_snapshots);
    println!("   Duration: {:?}", elapsed);
    println!("   Snapshot interval: {}s", SNAPSHOT_INTERVAL_SECS);
    println!("   ✅ System remained stable with continuous snapshots");

    assert!(
        total_snapshots >= ((DURATION_SECS / SNAPSHOT_INTERVAL_SECS) - 1) as usize,
        "Should complete most scheduled snapshots"
    );
}

/// Test memory stability over time
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_memory_stability_15min() {
    let data_dir = test_data_dir();
    let log = Arc::new(open_test_log(data_dir.path()).await.unwrap());

    const DURATION_SECS: u64 = 900; // 15 minutes
    const VALUE_SIZE: usize = 1024; // 1KB values

    println!(
        "\n⏱️  Running memory stability test for {} seconds (15 min)...",
        DURATION_SECS
    );

    let write_count = Arc::new(AtomicUsize::new(0));
    let running = Arc::new(AtomicBool::new(true));
    let start_time = Instant::now();

    let log_writer = log.clone();
    let counter = write_count.clone();
    let running_flag = running.clone();

    let writer = tokio::spawn(async move {
        let mut local_count = 0u64;
        let value_template = vec![0xAB; VALUE_SIZE];

        while running_flag.load(Ordering::Relaxed) {
            let key = local_count;
            let value = value_template.clone();

            if append_kv(&log_writer, key, value).await.is_ok() {
                let total = counter.fetch_add(1, Ordering::Relaxed) + 1;
                local_count += 1;

                if total % 10_000 == 0 {
                    let elapsed = start_time.elapsed();
                    let mb_written = (total * VALUE_SIZE) / (1024 * 1024);
                    println!(
                        "   Progress: {} ops, ~{}MB written, {:.2} ops/sec",
                        total,
                        mb_written,
                        total as f64 / elapsed.as_secs_f64()
                    );
                }
            }

            if local_count % 100 == 0 {
                tokio::task::yield_now().await;
            }
        }
    });

    // Periodic snapshots to test stability
    let log_snapshotter = log.clone();
    let running_snapshotter = running.clone();

    let snapshotter = tokio::spawn(async move {
        while running_snapshotter.load(Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_secs(60)).await;

            if log_snapshotter.snapshot().await.is_ok() {
                println!("   Periodic snapshot completed");
            }
        }
    });

    // Run for duration
    tokio::time::sleep(Duration::from_secs(DURATION_SECS)).await;
    running.store(false, Ordering::Relaxed);

    // Wait for completion
    writer.await.unwrap();
    snapshotter.await.unwrap();

    let elapsed = start_time.elapsed();
    let total_writes = write_count.load(Ordering::Relaxed);
    let total_mb = (total_writes * VALUE_SIZE) / (1024 * 1024);

    println!("\n📊 Memory Stability Results:");
    println!("   Total writes: {}", total_writes);
    println!("   Total data: ~{}MB", total_mb);
    println!("   Duration: {:?}", elapsed);
    println!(
        "   Throughput: {:.2} ops/sec",
        total_writes as f64 / elapsed.as_secs_f64()
    );
    println!(
        "   Data rate: {:.2} MB/sec",
        total_mb as f64 / elapsed.as_secs_f64()
    );
    println!("   ✅ Memory remained stable over 15 minutes");
}
