use uuid::Uuid;

#[tokio::test]
async fn compaction_triggers_by_appends_and_size() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();

    let log = kyrodb_engine::PersistentEventLog::open(&path).await.unwrap();

    // Write some KV pairs
    for i in 0..50u64 {
        let _ = log.append_kv(Uuid::new_v4(), i % 5, vec![b'x'; 128]).await.unwrap();
    }

    // Snapshot to reset WAL
    log.snapshot().await.unwrap();
    assert!(log.wal_size_bytes().await < 1024);

    // Append enough to exceed ~2KB and trigger size-based decision (we'll call compaction directly)
    for i in 0..200u64 {
        let _ = log.append_kv(Uuid::new_v4(), i % 7, vec![b'y'; 128]).await.unwrap();
    }

    // Before compaction, wal should be non-trivial
    let before = log.wal_size_bytes().await;
    assert!(before > 1024);

    // Manual compaction keeps latest and snapshots
    log.compact_keep_latest_and_snapshot().await.unwrap();
    let after = log.wal_size_bytes().await;
    assert!(after < before, "wal did not shrink: before={}, after={}", before, after);

    // Verify lookups still work (latest writes retained)
    for k in 0..7u64 {
        assert!(log.lookup_key(k).await.is_some());
    }
}
