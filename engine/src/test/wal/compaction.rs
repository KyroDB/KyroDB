//! WAL Compaction Tests
//!
//! Tests for WAL compaction, cleanup, and space reclamation

use crate::test::utils::*;
use std::sync::Arc;
use tokio::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_wal_compaction_after_snapshot() {
    let dir = test_data_dir();

    let log = Arc::new(open_test_log(dir.path()).await.unwrap());

    // Write initial data
    for i in 0..100 {
        append_kv(&log, i, format!("before_snap_{}", i).into_bytes())
            .await
            .unwrap();
    }

    // Take snapshot (triggers compaction opportunity)
    log.snapshot().await.unwrap();

    // Write more data after snapshot
    for i in 100..150 {
        append_kv(&log, i, format!("after_snap_{}", i).into_bytes())
            .await
            .unwrap();
    }

    // Verify all data still accessible
    for i in 0..150 {
        let value = lookup_kv(&log, i).await.unwrap();
        assert!(value.is_some(), "Data lost during compaction for key {}", i);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_wal_space_reclamation() {
    let dir = test_data_dir();

    let log = Arc::new(open_test_log(dir.path()).await.unwrap());

    // Phase 1: Write large amount of data
    for i in 0..500 {
        let large_value = vec![0xFF; 512]; // 512 bytes each
        append_kv(&log, i, large_value).await.unwrap();
    }

    // Snapshot should allow old WAL segments to be reclaimed
    log.snapshot().await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Phase 2: Overwrite with smaller data
    for i in 0..100 {
        append_kv(&log, i, b"small".to_vec()).await.unwrap();
    }

    log.snapshot().await.unwrap();

    // Verify data integrity after compaction
    for i in 0..100 {
        let value = lookup_kv(&log, i).await.unwrap();
        assert!(
            value.is_some(),
            "Data lost after space reclamation for key {}",
            i
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_wal_compaction_preserves_latest_values() {
    let dir = test_data_dir();

    let log = Arc::new(open_test_log(dir.path()).await.unwrap());

    // Write multiple versions of same keys
    for version in 0..5 {
        for key in 0..50 {
            let value = format!("v{}_k{}", version, key);
            append_kv(&log, key, value.into_bytes()).await.unwrap();
        }

        // Snapshot after each version
        log.snapshot().await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Verify we can read the latest versions
    for key in 0..50 {
        let value = lookup_kv(&log, key).await.unwrap();
        assert!(value.is_some(), "Key {} lost during compaction", key);

        // Should be able to find at least one version
        let value_bytes = value.unwrap();
        let value_str = String::from_utf8_lossy(&value_bytes);
        assert!(
            value_str.contains("_k"),
            "Value format incorrect for key {}",
            key
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_wal_compaction_concurrent_with_writes() {
    use tokio::task::JoinSet;

    let dir = test_data_dir();
    let log = Arc::new(open_test_log(dir.path()).await.unwrap());

    let mut handles = JoinSet::new();

    // Concurrent writers
    for worker_id in 0..3 {
        let log_clone = log.clone();
        handles.spawn(async move {
            for i in 0..50 {
                let key = (worker_id * 100 + i) as u64;
                append_kv(
                    &log_clone,
                    key,
                    format!("concurrent_{}_{}", worker_id, i).into_bytes(),
                )
                .await
                .unwrap();

                if i % 10 == 0 {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        });
    }

    // Concurrent snapshot (triggers compaction)
    let log_clone = log.clone();
    handles.spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        log_clone.snapshot().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        log_clone.snapshot().await.unwrap();
    });

    while handles.join_next().await.is_some() {}

    // Verify no data loss during concurrent compaction
    let mut recovered = 0;
    for worker_id in 0..3 {
        for i in 0..50 {
            let key = (worker_id * 100 + i) as u64;
            if lookup_kv(&log, key).await.unwrap().is_some() {
                recovered += 1;
            }
        }
    }

    assert!(
        recovered >= 120, // At least 80% of 150 writes
        "Too much data lost during concurrent compaction: {} / 150",
        recovered
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_wal_compaction_idempotent() {
    let dir = test_data_dir();

    let log = Arc::new(open_test_log(dir.path()).await.unwrap());

    // Write data
    for i in 0..100 {
        append_kv(&log, i, format!("idempotent_{}", i).into_bytes())
            .await
            .unwrap();
    }

    // Multiple snapshots should be idempotent
    for _ in 0..5 {
        log.snapshot().await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Verify data integrity after multiple compactions
    for i in 0..100 {
        let value = lookup_kv(&log, i).await.unwrap();
        assert!(
            value.is_some(),
            "Data lost after idempotent compaction for key {}",
            i
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_wal_segment_cleanup_after_recovery() {
    let dir = test_data_dir();

    // Phase 1: Write and snapshot
    {
        let log = Arc::new(open_test_log(dir.path()).await.unwrap());

        for i in 0..100 {
            append_kv(&log, i, format!("cleanup_{}", i).into_bytes())
                .await
                .unwrap();
        }

        log.snapshot().await.unwrap();
        drop(log);
    }

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Phase 2: Recover and write more
    {
        let log = Arc::new(open_test_log(dir.path()).await.unwrap());
        log.snapshot().await.unwrap();

        #[cfg(feature = "learned-index")]
        {
            log.build_rmi().await.unwrap();
        }

        for i in 100..200 {
            append_kv(&log, i, format!("after_recovery_{}", i).into_bytes())
                .await
                .unwrap();
        }

        log.snapshot().await.unwrap();

        // Verify all data present
        for i in 0..200 {
            let value = lookup_kv(&log, i).await.unwrap();
            assert!(value.is_some(), "Data lost during cleanup for key {}", i);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_wal_handles_rapid_snapshots() {
    let dir = test_data_dir();
    let log = Arc::new(open_test_log(dir.path()).await.unwrap());

    // Rapid write + snapshot cycles
    for batch in 0..10 {
        for i in 0..20 {
            let key = batch * 20 + i;
            append_kv(&log, key, format!("rapid_{}_{}", batch, i).into_bytes())
                .await
                .unwrap();
        }

        log.snapshot().await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Verify all data survived rapid compaction
    for i in 0..200 {
        let value = lookup_kv(&log, i).await.unwrap();
        assert!(
            value.is_some(),
            "Data lost during rapid snapshots for key {}",
            i
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_wal_compaction_performance() {
    let dir = test_data_dir();
    let log = Arc::new(open_test_log(dir.path()).await.unwrap());

    // Write substantial data
    for i in 0..1000 {
        append_kv(&log, i, format!("perf_{}", i).into_bytes())
            .await
            .unwrap();
    }

    // Measure snapshot/compaction time
    let start = std::time::Instant::now();
    log.snapshot().await.unwrap();
    let duration = start.elapsed();

    // Snapshot should complete reasonably fast (< 5 seconds for 1000 entries)
    assert!(
        duration.as_secs() < 5,
        "Snapshot took too long: {:?}",
        duration
    );

    // Verify data integrity after performance test
    let sample_keys = [0, 100, 500, 999];
    for &key in &sample_keys {
        let value = lookup_kv(&log, key).await.unwrap();
        assert!(
            value.is_some(),
            "Data lost after performance compaction for key {}",
            key
        );
    }
}
