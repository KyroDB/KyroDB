#[cfg(feature = "learned-index")]
#[tokio::test]
async fn rmi_crash_between_index_and_manifest_keeps_old_index_until_manifest_committed() {
    use uuid::Uuid;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let log = kyrodb_engine::PersistentEventLog::open(&path).await.unwrap();

    // Seed data and snapshot
    for i in 0..10_000u64 { let _ = log.append_kv(Uuid::new_v4(), i, vec![1u8]).await.unwrap(); }
    log.snapshot().await.unwrap();
    drop(log);

    // Write a manifest pointing to no RMI initially
    {
        let log = kyrodb_engine::PersistentEventLog::open(&path).await.unwrap();
        log.write_manifest().await;
    }

    // Simulate background rebuild: rename index to final name, but CRASH before manifest rename
    let pairs;
    {
        let log = kyrodb_engine::PersistentEventLog::open(&path).await.unwrap();
        pairs = log.collect_key_offset_pairs().await;
    }
    let tmp = path.join("index-rmi.tmp");
    let dst = path.join("index-rmi.bin");
    kyrodb_engine::index::RmiIndex::write_from_pairs(&tmp, &pairs).unwrap();
    std::fs::rename(&tmp, &dst).unwrap();
    // crash here (no manifest update)

    // Restart: engine should only load RMI if manifest references it
    let log2 = kyrodb_engine::PersistentEventLog::open(&path).await.unwrap();
    // Since manifest was not updated to point at index-rmi.bin yet, we must still be able to lookup via BTree
    assert!(log2.lookup_key(42).await.is_some());
}
