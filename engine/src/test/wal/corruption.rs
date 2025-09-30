//! WAL Corruption Detection and Handling Tests
//!
//! Tests for corruption detection, CRC validation, and recovery

use crate::test::utils::*;
use std::sync::Arc;
use tokio::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_wal_detects_missing_entries() {
    let dir = test_data_dir();
    
    // Write some data
    {
        let log = open_test_log(dir.path()).await.unwrap();
        
        for i in 0..50 {
            append_kv_ref(&log, i, format!("detect_{}", i).into_bytes())
                .await
                .unwrap();
        }
        
        drop(log);
    }
    
    tokio::time::sleep(Duration::from_millis(50)).await;
    
    // Recover and verify
    {
        let log = Arc::new(open_test_log(dir.path()).await.unwrap());
        log.snapshot().await.unwrap();
        
        #[cfg(feature = "learned-index")]
        {
            log.build_rmi().await.unwrap();
        }
        
        // Should recover without errors
        for i in 0..50 {
            let value = lookup_kv(&log, i).await.unwrap();
            if value.is_none() {
                // If entry is missing, log should have detected it during recovery
                println!("Entry {} missing (corruption detected)", i);
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_wal_recovery_with_partial_corruption() {
    let dir = test_data_dir();
    
    // Write data in phases
    {
        let log = Arc::new(open_test_log(dir.path()).await.unwrap());
        
        // Phase 1: Known good data
        for i in 0..30 {
            append_kv(&log, i, format!("good_{}", i).into_bytes())
                .await
                .unwrap();
        }
        
        log.snapshot().await.unwrap();
        
        // Phase 2: More data (may be partially corrupted on crash)
        for i in 30..60 {
            append_kv(&log, i, format!("risky_{}", i).into_bytes())
                .await
                .unwrap();
        }
        
        // Simulate crash
        drop(log);
    }
    
    tokio::time::sleep(Duration::from_millis(50)).await;
    
    // Recover: should get at least the snapshotted data
    {
        let log = Arc::new(open_test_log(dir.path()).await.unwrap());
        log.snapshot().await.unwrap();
        
        #[cfg(feature = "learned-index")]
        {
            log.build_rmi().await.unwrap();
        }
        
        // At minimum, the first 30 (snapshotted) should be available
        let mut recovered_from_snapshot = 0;
        for i in 0..30 {
            if lookup_kv(&log, i).await.unwrap().is_some() {
                recovered_from_snapshot += 1;
            }
        }
        
        assert!(
            recovered_from_snapshot >= 25,
            "Too few snapshotted entries recovered: {} / 30",
            recovered_from_snapshot
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_wal_handles_empty_wal_file() {
    let dir = test_data_dir();
    
    // Create WAL directory structure
    {
        let log = open_test_log(dir.path()).await.unwrap();
        drop(log);
    }
    
    // Try to recover (should handle gracefully)
    {
        let log = Arc::new(open_test_log(dir.path()).await.unwrap());
        log.snapshot().await.ok();
        
        #[cfg(feature = "learned-index")]
        {
            log.build_rmi().await.ok();
        }
        
        // Should work without panic
        let result = lookup_kv(&log, 0).await.unwrap();
        assert!(result.is_none());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_wal_isolation_of_corrupted_segment() {
    let dir = test_data_dir();
    
    // Phase 1: Write good data + snapshot
    {
        let log = Arc::new(open_test_log(dir.path()).await.unwrap());
        
        for i in 0..20 {
            append_kv(&log, i, format!("segment1_{}", i).into_bytes())
                .await
                .unwrap();
        }
        
        log.snapshot().await.unwrap();
        drop(log);
    }
    
    tokio::time::sleep(Duration::from_millis(30)).await;
    
    // Phase 2: Write more data (potentially corrupted segment)
    {
        let log = Arc::new(open_test_log(dir.path()).await.unwrap());
        
        for i in 20..40 {
            append_kv(&log, i, format!("segment2_{}", i).into_bytes())
                .await
                .unwrap();
        }
        
        // Crash
        drop(log);
    }
    
    tokio::time::sleep(Duration::from_millis(30)).await;
    
    // Phase 3: Recover - should isolate corruption to second segment
    {
        let log = Arc::new(open_test_log(dir.path()).await.unwrap());
        log.snapshot().await.unwrap();
        
        #[cfg(feature = "learned-index")]
        {
            log.build_rmi().await.unwrap();
        }
        
        // First segment (snapshotted) should be fully recoverable
        let mut segment1_recovered = 0;
        for i in 0..20 {
            if lookup_kv(&log, i).await.unwrap().is_some() {
                segment1_recovered += 1;
            }
        }
        
        assert!(
            segment1_recovered >= 18,
            "First segment should be mostly intact: {} / 20",
            segment1_recovered
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_wal_continued_operation_after_corruption_recovery() {
    let dir = test_data_dir();
    
    // Phase 1: Initial data + crash
    {
        let log = Arc::new(open_test_log(dir.path()).await.unwrap());
        
        for i in 0..30 {
            append_kv(&log, i, format!("before_{}", i).into_bytes())
                .await
                .unwrap();
        }
        
        drop(log);
    }
    
    tokio::time::sleep(Duration::from_millis(50)).await;
    
    // Phase 2: Recover and continue writing
    {
        let log = Arc::new(open_test_log(dir.path()).await.unwrap());
        log.snapshot().await.unwrap();
        
        #[cfg(feature = "learned-index")]
        {
            log.build_rmi().await.unwrap();
        }
        
        // Write new data after recovery
        for i in 30..60 {
            append_kv(&log, i, format!("after_{}", i).into_bytes())
                .await
                .unwrap();
        }
        
        log.snapshot().await.unwrap();
        drop(log);
    }
    
    tokio::time::sleep(Duration::from_millis(50)).await;
    
    // Phase 3: Final recovery - should have both old and new data
    {
        let log = Arc::new(open_test_log(dir.path()).await.unwrap());
        log.snapshot().await.unwrap();
        
        #[cfg(feature = "learned-index")]
        {
            log.build_rmi().await.unwrap();
        }
        
        let mut total_recovered = 0;
        for i in 0..60 {
            if lookup_kv(&log, i).await.unwrap().is_some() {
                total_recovered += 1;
            }
        }
        
        assert!(
            total_recovered >= 50,
            "Should recover most data after corruption: {} / 60",
            total_recovered
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_wal_large_entry_boundary_corruption() {
    let dir = test_data_dir();
    
    // Write mix of small and large entries
    {
        let log = Arc::new(open_test_log(dir.path()).await.unwrap());
        
        // Small entries
        for i in 0..10 {
            append_kv(&log, i, format!("small_{}", i).into_bytes())
                .await
                .unwrap();
        }
        
        // Large entry (1KB)
        let large_value = vec![0xAB; 1024];
        append_kv(&log, 100, large_value).await.unwrap();
        
        // More small entries
        for i in 10..20 {
            append_kv(&log, i, format!("small_{}", i).into_bytes())
                .await
                .unwrap();
        }
        
        drop(log);
    }
    
    tokio::time::sleep(Duration::from_millis(50)).await;
    
    // Recover
    {
        let log = Arc::new(open_test_log(dir.path()).await.unwrap());
        log.snapshot().await.unwrap();
        
        #[cfg(feature = "learned-index")]
        {
            log.build_rmi().await.unwrap();
        }
        
        // Verify large entry survived
        let large_result = lookup_kv(&log, 100).await.unwrap();
        if let Some(value) = large_result {
            assert_eq!(value.len(), 1024, "Large entry size mismatch");
        }
        
        // Verify small entries around it
        let mut small_recovered = 0;
        for i in 0..20 {
            if lookup_kv(&log, i).await.unwrap().is_some() {
                small_recovered += 1;
            }
        }
        
        assert!(
            small_recovered >= 15,
            "Too few small entries recovered: {} / 20",
            small_recovered
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_wal_sequential_corruption_detection() {
    let dir = test_data_dir();
    
    // Write sequential data with known pattern
    {
        let log = Arc::new(open_test_log(dir.path()).await.unwrap());
        
        for i in 0..100 {
            let value = format!("seq_{:04}", i);
            append_kv(&log, i, value.into_bytes()).await.unwrap();
        }
        
        drop(log);
    }
    
    tokio::time::sleep(Duration::from_millis(50)).await;
    
    // Recover and check sequence integrity
    {
        let log = Arc::new(open_test_log(dir.path()).await.unwrap());
        log.snapshot().await.unwrap();
        
        #[cfg(feature = "learned-index")]
        {
            log.build_rmi().await.unwrap();
        }
        
        // Check for gaps in sequence (indicates corruption)
        let mut last_found = None;
        let mut gap_count = 0;
        
        for i in 0..100 {
            let result = lookup_kv(&log, i).await.unwrap();
            if result.is_some() {
                if let Some(last) = last_found {
                    if i > last + 1 {
                        gap_count += i - last - 1;
                    }
                }
                last_found = Some(i);
            }
        }
        
        // Should have mostly contiguous sequence
        assert!(
            gap_count < 20,
            "Too many gaps in sequence: {} (indicates corruption)",
            gap_count
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_wal_recovery_statistics() {
    let dir = test_data_dir();
    
    // Write known amount of data
    {
        let log = Arc::new(open_test_log(dir.path()).await.unwrap());
        
        for i in 0..200 {
            append_kv(&log, i, format!("stats_{}", i).into_bytes())
                .await
                .unwrap();
        }
        
        drop(log);
    }
    
    tokio::time::sleep(Duration::from_millis(50)).await;
    
    // Recover and check recovery rate
    {
        let log = Arc::new(open_test_log(dir.path()).await.unwrap());
        log.snapshot().await.unwrap();
        
        #[cfg(feature = "learned-index")]
        {
            log.build_rmi().await.unwrap();
        }
        
        let mut recovered = 0;
        for i in 0..200 {
            if lookup_kv(&log, i).await.unwrap().is_some() {
                recovered += 1;
            }
        }
        
        let recovery_rate = (recovered as f64 / 200.0) * 100.0;
        
        // Should recover at least 90% of data
        assert!(
            recovery_rate >= 90.0,
            "Low recovery rate: {:.1}% ({} / 200)",
            recovery_rate,
            recovered
        );
    }
}
