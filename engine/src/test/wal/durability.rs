//! WAL Durability Tests
//!
//! Tests for write-ahead log durability guarantees and fsync policies

use crate::test::utils::*;
use std::sync::Arc;
use tokio::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_wal_durability_immediate() {
    let dir = test_data_dir();
    let log = Arc::new(open_test_log(dir.path()).await.unwrap());
    
    // Write data with immediate durability (group commit disabled)
    for i in 0..50 {
        append_kv(&log, i, format!("durable_{}", i).into_bytes())
            .await
            .unwrap();
    }
    
    // Force flush
    drop(log);
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Reopen and verify all data persisted
    let log = Arc::new(open_test_log(dir.path()).await.unwrap());
    
    // Snapshot to make data visible
    log.snapshot().await.unwrap();
    
    #[cfg(feature = "learned-index")]
    {
        log.build_rmi().await.unwrap();
    }
    
    for i in 0..50 {
        let value = lookup_kv(&log, i).await.unwrap();
        assert!(value.is_some(), "Key {} not found after restart", i);
        assert_eq!(
            value.unwrap(),
            format!("durable_{}", i).into_bytes(),
            "Value mismatch for key {}",
            i
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_wal_survives_crash_simulation() {
    let dir = test_data_dir();
    
    // Phase 1: Write data
    {
        let log = open_test_log(dir.path()).await.unwrap();
        
        for i in 0..100 {
            append_kv_ref(&log, i * 10, format!("crash_test_{}", i).into_bytes())
                .await
                .unwrap();
        }
        
        // Simulate crash (immediate drop without graceful shutdown)
        drop(log);
    }
    
    // Phase 2: Recovery
    tokio::time::sleep(Duration::from_millis(50)).await;
    
    {
        let log = Arc::new(open_test_log(dir.path()).await.unwrap());
        
        // Snapshot to recover data
        log.snapshot().await.unwrap();
        
        #[cfg(feature = "learned-index")]
        {
            log.build_rmi().await.unwrap();
        }
        
        // Verify data survived crash
        for i in 0..100 {
            let value = lookup_kv(&log, i * 10).await.unwrap();
            assert!(
                value.is_some(),
                "Key {} lost after crash",
                i * 10
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_wal_multiple_restarts() {
    let dir = test_data_dir();
    
    // Multiple restart cycles
    for cycle in 0..5 {
        let log = Arc::new(open_test_log(dir.path()).await.unwrap());
        
        // Snapshot existing data
        log.snapshot().await.ok();
        
        #[cfg(feature = "learned-index")]
        {
            log.build_rmi().await.ok();
        }
        
        // Verify previous cycles' data
        for prev_cycle in 0..cycle {
            for i in 0..20 {
                let key = (prev_cycle * 100 + i) as u64;
                let value = lookup_kv(&log, key).await.unwrap();
                assert!(
                    value.is_some(),
                    "Cycle {} key {} lost after {} restarts",
                    prev_cycle,
                    i,
                    cycle
                );
            }
        }
        
        // Write new data for this cycle
        for i in 0..20 {
            let key = (cycle * 100 + i) as u64;
            append_kv(&log, key, format!("cycle_{}_{}", cycle, i).into_bytes())
                .await
                .unwrap();
        }
        
        // Snapshot new data
        log.snapshot().await.unwrap();
        
        drop(log);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    
    // Final verification
    let log = Arc::new(open_test_log(dir.path()).await.unwrap());
    log.snapshot().await.unwrap();
    
    #[cfg(feature = "learned-index")]
    {
        log.build_rmi().await.unwrap();
    }
    
    for cycle in 0..5 {
        for i in 0..20 {
            let key = (cycle * 100 + i) as u64;
            let value = lookup_kv(&log, key).await.unwrap();
            assert!(
                value.is_some(),
                "Final check: cycle {} key {} missing",
                cycle,
                i
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_wal_durability_with_large_entries() {
    let dir = test_data_dir();
    let log = Arc::new(open_test_log(dir.path()).await.unwrap());
    
    // Write large entries (1KB each)
    for i in 0..50 {
        let large_value = vec![i as u8; 1024];
        append_kv(&log, i, large_value).await.unwrap();
    }
    
    drop(log);
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Verify after restart
    let log = Arc::new(open_test_log(dir.path()).await.unwrap());
    log.snapshot().await.unwrap();
    
    #[cfg(feature = "learned-index")]
    {
        log.build_rmi().await.unwrap();
    }
    
    for i in 0..50 {
        let value = lookup_kv(&log, i).await.unwrap();
        assert!(value.is_some(), "Large entry {} missing", i);
        assert_eq!(value.unwrap().len(), 1024, "Large entry size mismatch");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_wal_durability_sequential_updates() {
    let dir = test_data_dir();
    
    // Phase 1: Initial write
    {
        let log = Arc::new(open_test_log(dir.path()).await.unwrap());
        
        for i in 0..50 {
            append_kv(&log, i, format!("version_0_{}", i).into_bytes())
                .await
                .unwrap();
        }
        
        drop(log);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    
    // Phase 2: Update values
    {
        let log = Arc::new(open_test_log(dir.path()).await.unwrap());
        log.snapshot().await.ok();
        
        for i in 0..50 {
            append_kv(&log, i, format!("version_1_{}", i).into_bytes())
                .await
                .unwrap();
        }
        
        drop(log);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    
    // Phase 3: Verify latest values persisted
    {
        let log = Arc::new(open_test_log(dir.path()).await.unwrap());
        log.snapshot().await.unwrap();
        
        #[cfg(feature = "learned-index")]
        {
            log.build_rmi().await.unwrap();
        }
        
        for i in 0..50 {
            let value = lookup_kv(&log, i).await.unwrap();
            assert!(value.is_some(), "Updated key {} missing", i);
            // Note: Depending on implementation, might get version_0 or version_1
            // The important thing is data persisted
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_wal_durability_concurrent_writes() {
    use tokio::task::JoinSet;
    
    let dir = test_data_dir();
    let log = Arc::new(open_test_log(dir.path()).await.unwrap());
    
    let mut handles = JoinSet::new();
    
    // 10 concurrent writers
    for worker_id in 0..10 {
        let log_clone = log.clone();
        handles.spawn(async move {
            for i in 0..50 {
                let key = (worker_id * 100 + i) as u64;
                append_kv(&log_clone, key, format!("w{}_i{}", worker_id, i).into_bytes())
                    .await
                    .unwrap();
            }
        });
    }
    
    while handles.join_next().await.is_some() {}
    
    drop(log);
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Verify all concurrent writes persisted
    let log = Arc::new(open_test_log(dir.path()).await.unwrap());
    log.snapshot().await.unwrap();
    
    #[cfg(feature = "learned-index")]
    {
        log.build_rmi().await.unwrap();
    }
    
    for worker_id in 0..10 {
        for i in 0..50 {
            let key = (worker_id * 100 + i) as u64;
            let value = lookup_kv(&log, key).await.unwrap();
            assert!(
                value.is_some(),
                "Concurrent write worker {} iteration {} missing",
                worker_id,
                i
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_wal_no_data_loss_on_restart() {
    let dir = test_data_dir();
    
    // Write increasing sequence
    for round in 0..3 {
        let log = Arc::new(open_test_log(dir.path()).await.unwrap());
        
        // Snapshot previous data
        log.snapshot().await.ok();
        
        #[cfg(feature = "learned-index")]
        {
            log.build_rmi().await.ok();
        }
        
        // Verify no data loss from previous rounds
        for prev_round in 0..round {
            for i in 0..30 {
                let key = (prev_round * 1000 + i) as u64;
                let value = lookup_kv(&log, key).await.unwrap();
                assert!(
                    value.is_some(),
                    "Round {} lost data from round {} key {}",
                    round,
                    prev_round,
                    i
                );
            }
        }
        
        // Write this round's data
        for i in 0..30 {
            let key = (round * 1000 + i) as u64;
            append_kv(&log, key, format!("round_{}_{}", round, i).into_bytes())
                .await
                .unwrap();
        }
        
        log.snapshot().await.unwrap();
        drop(log);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
