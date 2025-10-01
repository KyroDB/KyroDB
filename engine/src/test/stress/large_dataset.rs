//! Large Dataset Stress Tests
//!
//! Tests system behavior with millions of keys and gigabytes of data

use crate::test::utils::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::time::Instant;

/// Test 1 million keys insertion
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore] // Run explicitly: cargo test --features learned-index test_1_million_keys -- --ignored
async fn test_1_million_keys() {
    let data_dir = test_data_dir();
    let log = Arc::new(open_test_log(data_dir.path()).await.unwrap());

    const NUM_KEYS: usize = 1_000_000;
    const BATCH_SIZE: usize = 10_000;

    println!(
        "\n📝 Inserting {} keys in batches of {}...",
        NUM_KEYS, BATCH_SIZE
    );
    let start_time = Instant::now();

    let inserted_count = Arc::new(AtomicUsize::new(0));

    for batch_start in (0..NUM_KEYS).step_by(BATCH_SIZE) {
        let batch_end = (batch_start + BATCH_SIZE).min(NUM_KEYS);
        let mut handles = vec![];

        for i in batch_start..batch_end {
            let log_clone = log.clone();
            let counter = inserted_count.clone();

            let handle = tokio::spawn(async move {
                let key = i as u64;
                let value = format!("large_dataset_value_{}", i).into_bytes();

                if append_kv(&log_clone, key, value).await.is_ok() {
                    counter.fetch_add(1, Ordering::Relaxed);
                }
            });

            handles.push(handle);
        }

        // Wait for batch to complete
        for handle in handles {
            handle.await.unwrap();
        }

        let current_count = inserted_count.load(Ordering::Relaxed);
        if current_count % 100_000 == 0 && current_count > 0 {
            let elapsed = start_time.elapsed();
            println!(
                "   Progress: {}/{} keys ({:.1}%), {:.2} ops/sec",
                current_count,
                NUM_KEYS,
                (current_count as f64 / NUM_KEYS as f64) * 100.0,
                current_count as f64 / elapsed.as_secs_f64()
            );
        }
    }

    let insert_elapsed = start_time.elapsed();
    let total_inserted = inserted_count.load(Ordering::Relaxed);

    println!("\n📊 Insertion Results:");
    println!("   Keys inserted: {}", total_inserted);
    println!("   Duration: {:?}", insert_elapsed);
    println!(
        "   Throughput: {:.2} ops/sec",
        total_inserted as f64 / insert_elapsed.as_secs_f64()
    );

    assert_eq!(total_inserted, NUM_KEYS, "Should insert all keys");

    // Build RMI for large dataset
    println!("\n🔧 Building snapshot and RMI index...");
    let snapshot_start = Instant::now();
    log.snapshot().await.unwrap();

    #[cfg(feature = "learned-index")]
    {
        log.build_rmi().await.unwrap();
        let snapshot_elapsed = snapshot_start.elapsed();
        println!("   Snapshot + RMI build: {:?}", snapshot_elapsed);
    }

    // Random lookup verification
    println!("\n🔍 Verifying random lookups...");
    const NUM_LOOKUPS: usize = 10_000;
    let lookup_start = Instant::now();
    let mut found_count = 0;

    for i in 0..NUM_LOOKUPS {
        let key = (i * 97) % NUM_KEYS; // Prime number for better distribution
        if lookup_kv(&log, key as u64).await.unwrap().is_some() {
            found_count += 1;
        }
    }

    let lookup_elapsed = lookup_start.elapsed();

    println!("\n📊 Lookup Results:");
    println!("   Lookups attempted: {}", NUM_LOOKUPS);
    println!("   Found: {}", found_count);
    println!("   Duration: {:?}", lookup_elapsed);
    println!(
        "   Lookup throughput: {:.2} ops/sec",
        NUM_LOOKUPS as f64 / lookup_elapsed.as_secs_f64()
    );

    assert!(
        found_count >= NUM_LOOKUPS * 95 / 100,
        "Should find at least 95% of keys"
    );
}

/// Test 10 million keys (long-running test)
#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
#[ignore] // Run explicitly: cargo test --features learned-index test_10_million_keys -- --ignored
async fn test_10_million_keys() {
    let data_dir = test_data_dir();
    let log = Arc::new(open_test_log(data_dir.path()).await.unwrap());

    const NUM_KEYS: usize = 10_000_000;
    const BATCH_SIZE: usize = 50_000;
    const PARALLEL_WORKERS: usize = 16;

    println!(
        "\n📝 Inserting {} keys with {} parallel workers...",
        NUM_KEYS, PARALLEL_WORKERS
    );
    let start_time = Instant::now();

    let inserted_count = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for worker_id in 0..PARALLEL_WORKERS {
        let log_clone = log.clone();
        let counter = inserted_count.clone();
        let keys_per_worker = NUM_KEYS / PARALLEL_WORKERS;
        let start_key = worker_id * keys_per_worker;
        let end_key = start_key + keys_per_worker;

        let handle = tokio::spawn(async move {
            for i in start_key..end_key {
                let key = i as u64;
                let value = format!("val_{}", i).into_bytes();

                if append_kv(&log_clone, key, value).await.is_ok() {
                    let count = counter.fetch_add(1, Ordering::Relaxed) + 1;

                    if count % 500_000 == 0 {
                        println!("   Worker {} progress: {} keys", worker_id, count);
                    }
                }
            }
        });

        handles.push(handle);
    }

    // Wait for all workers
    for handle in handles {
        handle.await.unwrap();
    }

    let insert_elapsed = start_time.elapsed();
    let total_inserted = inserted_count.load(Ordering::Relaxed);

    println!("\n📊 10M Keys Insertion Results:");
    println!("   Keys inserted: {}", total_inserted);
    println!("   Duration: {:?}", insert_elapsed);
    println!(
        "   Throughput: {:.2} ops/sec",
        total_inserted as f64 / insert_elapsed.as_secs_f64()
    );

    assert!(
        total_inserted >= NUM_KEYS * 95 / 100,
        "Should insert at least 95% of keys"
    );
}

/// Test large values (1KB each)
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_large_values_1kb() {
    let data_dir = test_data_dir();
    let log = Arc::new(open_test_log(data_dir.path()).await.unwrap());

    const NUM_KEYS: usize = 100_000;
    const VALUE_SIZE: usize = 1024; // 1KB

    println!(
        "\n📝 Inserting {} keys with {}B values (total: ~{}MB)...",
        NUM_KEYS,
        VALUE_SIZE,
        (NUM_KEYS * VALUE_SIZE) / (1024 * 1024)
    );

    let start_time = Instant::now();
    let value_template = vec![0xAB; VALUE_SIZE];

    for i in 0..NUM_KEYS {
        let key = i as u64;
        let mut value = value_template.clone();
        // Make values unique
        let suffix = i.to_le_bytes();
        value[..8].copy_from_slice(&suffix);

        append_kv(&log, key, value).await.unwrap();

        if i % 10_000 == 0 && i > 0 {
            println!("   Progress: {}/{} keys", i, NUM_KEYS);
        }
    }

    let insert_elapsed = start_time.elapsed();

    println!("\n📊 Large Values Results:");
    println!("   Keys inserted: {}", NUM_KEYS);
    println!("   Duration: {:?}", insert_elapsed);
    println!(
        "   Throughput: {:.2} ops/sec",
        NUM_KEYS as f64 / insert_elapsed.as_secs_f64()
    );
    println!(
        "   Data throughput: {:.2} MB/sec",
        (NUM_KEYS * VALUE_SIZE) as f64 / (1024.0 * 1024.0) / insert_elapsed.as_secs_f64()
    );

    // Verify a sample of keys
    println!("\n🔍 Verifying sample of large values...");
    for i in (0..NUM_KEYS).step_by(1000) {
        let value = lookup_kv(&log, i as u64).await.unwrap().unwrap();
        assert_eq!(value.len(), VALUE_SIZE, "Value size mismatch for key {}", i);

        // Verify unique suffix
        let mut expected_suffix = [0u8; 8];
        expected_suffix.copy_from_slice(&i.to_le_bytes());
        assert_eq!(
            &value[..8],
            &expected_suffix,
            "Value content mismatch for key {}",
            i
        );
    }

    println!("   ✅ All sampled values verified");
}

/// Test very large values (10KB each)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore] // Run explicitly due to large memory usage
async fn test_very_large_values_10kb() {
    let data_dir = test_data_dir();
    let log = Arc::new(open_test_log(data_dir.path()).await.unwrap());

    const NUM_KEYS: usize = 10_000;
    const VALUE_SIZE: usize = 10 * 1024; // 10KB

    println!(
        "\n📝 Inserting {} keys with {}KB values (total: ~{}MB)...",
        NUM_KEYS,
        VALUE_SIZE / 1024,
        (NUM_KEYS * VALUE_SIZE) / (1024 * 1024)
    );

    let start_time = Instant::now();
    let value_template = vec![0xCD; VALUE_SIZE];

    for i in 0..NUM_KEYS {
        let key = i as u64;
        let value = value_template.clone();

        append_kv(&log, key, value).await.unwrap();

        if i % 1000 == 0 && i > 0 {
            println!("   Progress: {}/{} keys", i, NUM_KEYS);
        }
    }

    let insert_elapsed = start_time.elapsed();

    println!("\n📊 Very Large Values Results:");
    println!("   Keys inserted: {}", NUM_KEYS);
    println!("   Duration: {:?}", insert_elapsed);
    println!(
        "   Throughput: {:.2} ops/sec",
        NUM_KEYS as f64 / insert_elapsed.as_secs_f64()
    );
    println!(
        "   Data throughput: {:.2} MB/sec",
        (NUM_KEYS * VALUE_SIZE) as f64 / (1024.0 * 1024.0) / insert_elapsed.as_secs_f64()
    );
}

/// Test sparse key space (large gaps between keys)
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_sparse_key_space() {
    let data_dir = test_data_dir();
    let log = Arc::new(open_test_log(data_dir.path()).await.unwrap());

    const NUM_KEYS: usize = 100_000;
    const KEY_SPACING: u64 = 1_000_000; // 1M gap between keys

    println!(
        "\n📝 Inserting {} keys with {}M spacing...",
        NUM_KEYS,
        KEY_SPACING / 1_000_000
    );

    let start_time = Instant::now();

    for i in 0..NUM_KEYS {
        let key = (i as u64) * KEY_SPACING;
        let value = format!("sparse_value_{}", i).into_bytes();

        append_kv(&log, key, value).await.unwrap();

        if i % 10_000 == 0 && i > 0 {
            println!("   Progress: {}/{} keys", i, NUM_KEYS);
        }
    }

    let insert_elapsed = start_time.elapsed();

    println!("\n📊 Sparse Key Space Results:");
    println!("   Keys inserted: {}", NUM_KEYS);
    println!("   Key range: 0 to {}", (NUM_KEYS as u64 - 1) * KEY_SPACING);
    println!("   Duration: {:?}", insert_elapsed);
    println!(
        "   Throughput: {:.2} ops/sec",
        NUM_KEYS as f64 / insert_elapsed.as_secs_f64()
    );

    // Build RMI for sparse keys
    println!("\n🔧 Building RMI for sparse keys...");
    #[cfg(feature = "learned-index")]
    {
        log.snapshot().await.unwrap();
        let rmi_start = Instant::now();
        log.build_rmi().await.unwrap();
        println!("   RMI build: {:?}", rmi_start.elapsed());
    }

    // Verify sparse lookups
    println!("\n🔍 Verifying sparse lookups...");
    let mut found_count = 0;
    for i in (0..NUM_KEYS).step_by(100) {
        let key = (i as u64) * KEY_SPACING;
        if lookup_kv(&log, key).await.unwrap().is_some() {
            found_count += 1;
        }
    }

    println!(
        "   ✅ Found {}/{} sampled keys",
        found_count,
        NUM_KEYS / 100
    );
    assert_eq!(found_count, NUM_KEYS / 100, "Should find all sampled keys");
}

/// Test sequential scan of large dataset
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_sequential_scan_large_dataset() {
    let data_dir = test_data_dir();
    let log = Arc::new(open_test_log(data_dir.path()).await.unwrap());

    const NUM_KEYS: usize = 500_000;

    println!("\n📝 Inserting {} sequential keys...", NUM_KEYS);
    for i in 0..NUM_KEYS {
        append_kv(&log, i as u64, format!("value_{}", i).into_bytes())
            .await
            .unwrap();

        if i % 50_000 == 0 && i > 0 {
            println!("   Progress: {}/{} keys", i, NUM_KEYS);
        }
    }

    #[cfg(feature = "learned-index")]
    {
        log.snapshot().await.unwrap();
        log.build_rmi().await.unwrap();
    }

    println!("\n🔍 Performing sequential scan...");
    let scan_start = Instant::now();
    let mut found_count = 0;

    for i in 0..NUM_KEYS {
        if lookup_kv(&log, i as u64).await.unwrap().is_some() {
            found_count += 1;
        }

        if i % 50_000 == 0 && i > 0 {
            let elapsed = scan_start.elapsed();
            println!(
                "   Scan progress: {}/{} keys ({:.2} ops/sec)",
                i,
                NUM_KEYS,
                i as f64 / elapsed.as_secs_f64()
            );
        }
    }

    let scan_elapsed = scan_start.elapsed();

    println!("\n📊 Sequential Scan Results:");
    println!("   Keys scanned: {}", NUM_KEYS);
    println!("   Found: {}", found_count);
    println!("   Duration: {:?}", scan_elapsed);
    println!(
        "   Scan throughput: {:.2} ops/sec",
        NUM_KEYS as f64 / scan_elapsed.as_secs_f64()
    );

    assert_eq!(
        found_count, NUM_KEYS,
        "Should find all keys in sequential scan"
    );
}
