#[cfg(feature = "learned-index")]
#[tokio::test]
async fn rmi_crash_during_swap_startup_ignores_tmp() {
    use uuid::Uuid;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let log = kyrodb_engine::PersistentEventLog::open(&path).await.unwrap();

    for i in 0..100u64 { let _ = log.append_kv(Uuid::new_v4(), i, vec![0u8]).await.unwrap(); }
    log.snapshot().await.unwrap();

    // Simulate crashed swap: write tmp but do not rename
    let pairs = log.collect_key_offset_pairs().await;
    let tmp = path.join("index-rmi.tmp");
    kyrodb_engine::index::RmiIndex::write_from_pairs(&tmp, &pairs).unwrap();
    drop(log);

    // Startup should ignore tmp and have no RMI index loaded (falls back to BTree)
    let log2 = kyrodb_engine::PersistentEventLog::open(&path).await.unwrap();
    // A simple lookup should still work via BTree index
    assert!(log2.lookup_key(42).await.is_some());
}
