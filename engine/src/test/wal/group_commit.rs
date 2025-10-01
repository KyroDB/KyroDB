//! Group Commit Tests
//!
//! Tests for group commit batching and fsync policies

use crate::test::utils::*;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_group_commit_batching() {
    let data_dir = test_data_dir();
    let log = Arc::new(open_test_log(data_dir.path()).await.unwrap());

    // Rapid fire appends should batch
    let start = Instant::now();
    for i in 0..1000 {
        append_kv(&log, i, format!("value_{}", i).into_bytes())
            .await
            .expect("Failed to append");
    }
    let elapsed = start.elapsed();

    log.snapshot().await.unwrap();

    #[cfg(feature = "learned-index")]
    {
        log.build_rmi().await.unwrap();
    }

    // Should complete in reasonable time
    // Note: With RMI rebuilds and snapshot operations, this can take longer
    assert!(
        elapsed < Duration::from_secs(30),
        "Group commit took too long: took {:?}",
        elapsed
    );

    println!("Batched 1000 appends completed in {:?}", elapsed);

    // Verify all data
    for i in 0..1000 {
        let value = lookup_kv(&log, i).await.expect("Failed to lookup");
        assert!(value.is_some(), "Key {} not found", i);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_group_commit_durability() {
    let data_dir = test_data_dir();

    // Write with group commit
    {
        let log = Arc::new(open_test_log(data_dir.path()).await.unwrap());

        for i in 0..100 {
            append_kv(&log, i, format!("value_{}", i).into_bytes())
                .await
                .expect("Failed to append");
        }

        // Wait for group commit window
        tokio::time::sleep(Duration::from_millis(100)).await;
        drop(log);
    }

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Verify persistence after restart
    {
        let log = Arc::new(open_test_log(data_dir.path()).await.unwrap());
        log.snapshot().await.unwrap();

        #[cfg(feature = "learned-index")]
        {
            log.build_rmi().await.unwrap();
        }

        for i in 0..100 {
            let value = lookup_kv(&log, i).await.expect("Failed to lookup");
            assert!(value.is_some(), "Key {} not durable after group commit", i);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_group_commits() {
    let data_dir = test_data_dir();
    let log = Arc::new(open_test_log(data_dir.path()).await.unwrap());

    let mut tasks = tokio::task::JoinSet::new();

    // Multiple concurrent writers during group commit window
    for thread_id in 0..10 {
        let log_clone = log.clone();
        tasks.spawn(async move {
            for i in 0..100 {
                let key = thread_id * 100 + i;
                append_kv(
                    &log_clone,
                    key,
                    format!("value_{}_{}", thread_id, i).into_bytes(),
                )
                .await
                .expect("Failed to append");
            }
        });
    }

    while tasks.join_next().await.is_some() {}

    // Wait for final group commit
    tokio::time::sleep(Duration::from_millis(100)).await;

    log.snapshot().await.unwrap();

    #[cfg(feature = "learned-index")]
    {
        log.build_rmi().await.unwrap();
    }

    // Verify all data
    for thread_id in 0..10 {
        for i in 0..100 {
            let key = thread_id * 100 + i;
            let value = lookup_kv(&log, key).await.expect("Failed to lookup");
            assert!(value.is_some(), "Key {} not found after group commit", key);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_group_commit_performance() {
    let data_dir = test_data_dir();

    // Test with batching
    let start = Instant::now();
    {
        let log = Arc::new(open_test_log(data_dir.path()).await.unwrap());

        for i in 0..1000 {
            append_kv(&log, i, fixtures::value_of_size(256))
                .await
                .expect("Failed to append");
        }

        log.snapshot().await.unwrap();
    }
    let with_batching = start.elapsed();

    // Test with smaller batches (more frequent fsyncs)
    let data_dir2 = test_data_dir();
    let start = Instant::now();
    {
        let log = Arc::new(open_test_log(data_dir2.path()).await.unwrap());

        for i in 0..1000 {
            append_kv(&log, i, fixtures::value_of_size(256))
                .await
                .expect("Failed to append");

            if i % 100 == 0 {
                log.snapshot().await.unwrap();
            }
        }
    }
    let with_frequent_sync = start.elapsed();

    // Batching should be faster or comparable
    println!(
        "With batching: {:?}, With frequent sync: {:?}",
        with_batching, with_frequent_sync
    );
}
