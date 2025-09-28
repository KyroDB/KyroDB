#[cfg(feature = "learned-index")]
#[tokio::test]
async fn rmi_rebuild_by_appends_threshold() {
    use uuid::Uuid;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let log = kyrodb_engine::PersistentEventLog::open(&path)
        .await
        .unwrap();

    // Prime some keys
    for i in 0..1000u64 {
        let _ = log.append_kv(Uuid::new_v4(), i, vec![0u8]).await.unwrap();
    }
    log.snapshot().await.unwrap();

    // Build initial RMI
    let pairs = log.collect_key_offset_pairs().await;
    let tmp = path.join("index-rmi.tmp");
    let dst = path.join("index-rmi.bin");
    kyrodb_engine::index::RmiIndex::write_from_pairs(&tmp, &pairs).unwrap();
    std::fs::rename(&tmp, &dst).unwrap();

    // Append more than threshold distinct keys
    for i in 1000..1105u64 {
        let _ = log.append_kv(Uuid::new_v4(), i, vec![1u8]).await.unwrap();
    }

    // Ensure all appends are persisted before rebuild
    log.snapshot().await.unwrap();

    // Manually trigger rebuild logic from main would require running server; instead emulate by writing new file again
    let pairs2 = log.collect_key_offset_pairs().await;
    kyrodb_engine::index::RmiIndex::write_from_pairs(&tmp, &pairs2).unwrap();
    std::fs::rename(&tmp, &dst).unwrap();

    // Reload and ensure index still answers
    drop(log);
    let log2 = kyrodb_engine::PersistentEventLog::open(&path)
        .await
        .unwrap();
    assert!(log2.lookup_key(0).await.is_some());
    assert!(log2.lookup_key(1104).await.is_some());
}
