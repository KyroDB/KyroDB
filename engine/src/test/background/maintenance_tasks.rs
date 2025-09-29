//! Background Maintenance Task Tests
//!
//! Tests for background compaction, group commit batching, and fsync operations

use crate::test::utils::{append_kv, lookup_kv, TestServer};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn test_group_commit_batching() {
    // Test that group commit batches multiple writes efficiently
    let server = TestServer::start_with_group_commit(5000).await.unwrap(); // 5ms delay
    let log = &server.log;

    let start = std::time::Instant::now();

    // Write 100 keys
    for i in 0..100 {
        append_kv(log, i, vec![i as u8; 100]).await.unwrap();
    }

    let elapsed = start.elapsed();

    // ✅ Build RMI after writes for lookups to work
    #[cfg(feature = "learned-index")]
    log.build_rmi().await.ok();

    // Verify data is readable
    for i in 0..100 {
        let value = lookup_kv(log, i).await.unwrap();
        assert!(value.is_some(), "Key {} should exist", i);
    }

    // With group commit, this should be reasonably fast
    // (not 100 individual fsyncs)
    assert!(
        elapsed.as_secs() < 10,
        "Group commit should be efficient, took {:?}",
        elapsed
    );

    server.cleanup().await;
}


#[tokio::test]
async fn test_compaction_reduces_wal_size() {
    // Test that compaction reduces WAL size by removing old versions
    let server = TestServer::start_default().await.unwrap();
    let log = &server.log;

    // Write initial data
    for i in 0..1000 {
        append_kv(log, i, vec![i as u8; 1000]).await.unwrap();
    }

    // Get initial WAL file size
    let wal_path = server.data_dir.join("wal.0000");
    let size_before = std::fs::metadata(&wal_path).unwrap().len();

    // Overwrite same keys multiple times (creates versions)
    for _round in 0..5 {
        for i in 0..1000 {
            append_kv(log, i, vec![(i + 1) as u8; 1000]).await.unwrap();
        }
    }

    // Check WAL size grew
    let size_after_writes = std::fs::metadata(&wal_path).unwrap().len();
    assert!(
        size_after_writes > size_before * 5,
        "WAL should grow significantly with overwrites"
    );

    // Compact - should keep only latest versions
    let stats = log.compact_keep_latest_and_snapshot_stats().await.unwrap();

    // Wait a moment for compaction to finish
    tokio::time::sleep(Duration::from_millis(100)).await;

    // ✅ Build RMI after compaction
    #[cfg(feature = "learned-index")]
    log.build_rmi().await.ok();

    // Verify data integrity after compaction
    for i in 0..1000 {
        let value = lookup_kv(log, i).await.unwrap();
        assert!(value.is_some(), "Key {} should exist after compaction", i);
    }

    // Verify stats show reduction
    assert!(stats.before_bytes > 0, "Should have data before compaction");
    assert!(
        stats.after_bytes < stats.before_bytes,
        "Should have less data after compaction"
    );

    server.cleanup().await;
}

#[tokio::test]
async fn test_compaction_creates_snapshot() {
    // Test that compaction creates a valid snapshot file
    let server = TestServer::start_default().await.unwrap();
    let log = &server.log;

    // Write data
    for i in 0..500 {
        append_kv(log, i, vec![i as u8; 512]).await.unwrap();
    }

    // Trigger compaction
    log.compact().await.unwrap();

    // Wait for snapshot creation
    sleep(Duration::from_millis(100)).await;

    // Check that snapshot file exists
    let snapshot_path = server.data_dir.join("snapshot.bin");
    assert!(
        snapshot_path.exists(),
        "Snapshot file should exist after compaction"
    );

    // Verify snapshot has content
    let snapshot_size = std::fs::metadata(&snapshot_path).unwrap().len();
    assert!(snapshot_size > 0, "Snapshot should have content");

    // ✅ Build RMI after compaction for lookups
    #[cfg(feature = "learned-index")]
    log.build_rmi().await.ok();

    // Verify data integrity
    for i in 0..500 {
        let value = lookup_kv(log, i).await.unwrap();
        assert!(value.is_some(), "Key {} should exist", i);
    }

    server.cleanup().await;
}

#[tokio::test]
async fn test_background_fsync_async_mode() {
    // Test that async durability mode uses background fsync
    let server = TestServer::start_with_async_durability().await.unwrap();
    let log = &server.log;

    // Write data (should return immediately, fsync in background)
    let start = std::time::Instant::now();

    for i in 0..100 {
        append_kv(log, i, vec![i as u8; 100]).await.unwrap();
    }

    let write_time = start.elapsed();

    // Writes should be relatively fast with async mode
    assert!(
        write_time.as_millis() < 2000,
        "Async writes should be reasonably fast, took {}ms",
        write_time.as_millis()
    );

    // Wait for background fsync to complete (interval is 10ms)
    tokio::time::sleep(Duration::from_millis(50)).await;

    // ✅ Build RMI after writes
    #[cfg(feature = "learned-index")]
    log.build_rmi().await.ok();

    // Verify all data is readable
    for i in 0..100 {
        let value = lookup_kv(log, i).await.unwrap();
        assert!(value.is_some(), "Key {} should exist", i);
    }

    server.cleanup().await;
}

#[tokio::test]
async fn test_maintenance_doesnt_block_reads() {
    // Test that background maintenance doesn't block read operations
    let server = TestServer::start_default().await.unwrap();
    let log = &server.log;

    // Write initial data
    for i in 0..1000 {
        append_kv(log, i, vec![i as u8; 512]).await.unwrap();
    }

    // ✅ Build RMI before reads
    #[cfg(feature = "learned-index")]
    log.build_rmi().await.ok();

    // Start compaction in background
    let log_clone = Arc::clone(log);
    let compaction_handle = tokio::spawn(async move {
        log_clone.compact().await.unwrap();
    });

    // While compaction runs, reads should still work
    sleep(Duration::from_millis(10)).await; // Let compaction start

    for i in 0..100 {
        let value = lookup_kv(log, i).await.unwrap();
        assert!(value.is_some(), "Read should work during compaction");
    }

    // Wait for compaction to finish
    compaction_handle.await.unwrap();

    // ✅ Rebuild RMI after compaction
    #[cfg(feature = "learned-index")]
    log.build_rmi().await.ok();

    // Reads should still work after compaction
    for i in 0..100 {
        let value = lookup_kv(log, i).await.unwrap();
        assert!(value.is_some(), "Read should work after compaction");
    }

    server.cleanup().await;
}

#[tokio::test]
async fn test_maintenance_doesnt_block_writes() {
    // Test that background maintenance doesn't block write operations
    let server = TestServer::start_default().await.unwrap();
    let log = &server.log;

    // Write initial data
    for i in 0..1000 {
        append_kv(log, i, vec![i as u8; 512]).await.unwrap();
    }

    // Start compaction in background
    let log_clone = Arc::clone(log);
    let compaction_handle = tokio::spawn(async move {
        log_clone.compact().await.unwrap();
    });

    // While compaction runs, writes should still work
    sleep(Duration::from_millis(10)).await; // Let compaction start

    for i in 1000..1100 {
        append_kv(log, i, vec![i as u8; 512]).await.unwrap();
    }

    // Wait for compaction to finish
    compaction_handle.await.unwrap();

    // ✅ Build RMI after all writes and compaction
    #[cfg(feature = "learned-index")]
    log.build_rmi().await.ok();

    // Verify all writes succeeded
    for i in 1000..1100 {
        let value = lookup_kv(log, i).await.unwrap();
        assert!(value.is_some(), "Write during compaction should persist");
    }

    server.cleanup().await;
}

#[tokio::test]
async fn test_concurrent_compactions_are_serialized() {
    // Test that multiple concurrent compaction requests are serialized
    let server = TestServer::start_default().await.unwrap();
    let log = &server.log;

    // Write data
    for i in 0..500 {
        append_kv(log, i, vec![i as u8; 256]).await.unwrap();
    }

    // Start multiple compactions concurrently
    let log1 = Arc::clone(log);
    let log2 = Arc::clone(log);
    let log3 = Arc::clone(log);

    let h1 = tokio::spawn(async move { log1.compact().await });
    let h2 = tokio::spawn(async move { log2.compact().await });
    let h3 = tokio::spawn(async move { log3.compact().await });

    // All should complete without error (serialized internally)
    let r1 = h1.await.unwrap();
    let r2 = h2.await.unwrap();
    let r3 = h3.await.unwrap();

    assert!(r1.is_ok(), "First compaction should succeed");
    assert!(r2.is_ok(), "Second compaction should succeed");
    assert!(r3.is_ok(), "Third compaction should succeed");

    // ✅ Build RMI after compactions
    #[cfg(feature = "learned-index")]
    log.build_rmi().await.ok();

    // Data should still be intact
    for i in 0..500 {
        let value = lookup_kv(log, i).await.unwrap();
        assert!(value.is_some(), "Data should survive concurrent compactions");
    }

    server.cleanup().await;
}

#[tokio::test]
async fn test_group_commit_respects_delay() {
    // Test that group commit respects the configured delay
    let delay_micros = 500; // 500µs = 0.5ms
    let server = TestServer::start_with_group_commit(delay_micros)
        .await
        .unwrap();
    let log = &server.log;

    // Single write should wait for delay
    let start = std::time::Instant::now();
    append_kv(log, 1, vec![1; 100]).await.unwrap();
    let elapsed = start.elapsed();

    // Should take at least the delay time (but not too much more)
    assert!(
        elapsed.as_micros() >= delay_micros as u128,
        "Group commit should wait for delay"
    );
    assert!(
        elapsed.as_millis() < 10,
        "Should not wait too long: {}ms",
        elapsed.as_millis()
    );

    server.cleanup().await;
}
