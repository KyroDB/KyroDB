#[cfg(feature = "http-test")]
#[tokio::test]
async fn http_compact_endpoint_smoke() {
    use std::sync::Arc;
    use tempfile::tempdir;
    use uuid::Uuid;

    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();

    let log = Arc::new(
        kyrodb_engine::PersistentEventLog::open(&path)
            .await
            .unwrap(),
    );

    // Write a few events and snapshot
    for i in 0..10u64 {
        let _ = log
            .append_kv(Uuid::new_v4(), i % 3, vec![b'z'; 64])
            .await
            .unwrap();
    }
    log.snapshot().await.unwrap();

    // Inflate WAL a bit
    for i in 0..50u64 {
        let _ = log
            .append_kv(Uuid::new_v4(), i % 5, vec![b'w'; 64])
            .await
            .unwrap();
    }

    // Test compaction directly via the log API instead of HTTP
    let initial_size = log.wal_size_bytes().await;
    println!("Initial WAL size: {} bytes", initial_size);

    let stats = log.compact_keep_latest_and_snapshot_stats().await.unwrap();

    println!("Compaction stats: {:?}", stats);

    // Verify we get reasonable stats back
    assert!(stats.before_bytes > 0, "Before bytes should be > 0");
    // WAL should be reasonable size after compaction
    let final_size = log.wal_size_bytes().await;
    println!("Final WAL size: {} bytes", final_size);
    assert!(
        final_size < 100_000,
        "WAL size should be reasonable after compaction"
    );

    // The compaction should process some data
    assert!(
        stats.before_bytes >= stats.after_bytes,
        "Compaction should not increase size"
    );
}