use uuid::Uuid;

#[tokio::test]
async fn background_compaction_shrinks_wal_and_preserves_reads() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();

    let log = kyrodb_engine::PersistentEventLog::open(&path)
        .await
        .unwrap();

    // Grow WAL with overwrites so compaction has effect
    for i in 0..10_000u64 {
        let _ = log
            .append_kv(Uuid::new_v4(), i % 100, vec![b'x'; 64])
            .await
            .unwrap();
    }
    let before = log.wal_size_bytes();
    assert!(before > 0, "wal should be non-zero before compaction");

    // Manual compaction
    let stats = log
        .compact_keep_latest_and_snapshot_stats()
        .await
        .expect("compaction should succeed");

    let after = log.wal_size_bytes();
    assert!(after <= stats.after_bytes);
    assert!(after < before, "wal did not shrink, before={} after={}", before, after);

    // Latest values survive
    for k in 0..100u64 {
        assert!(log.lookup_key(k).await.is_some());
    }
}
