#![cfg(feature = "learned-index")]
use uuid::Uuid;

#[tokio::test]
async fn rmi_property_bound_window_finds_key() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let log = kyrodb_engine::PersistentEventLog::open(&path).await.unwrap();

    // Insert monotonic keys spread out to make per-leaf models meaningful
    for i in 0..10_000u64 {
        let _ = log.append_kv(Uuid::new_v4(), i * 10, vec![0u8]).await.unwrap();
    }
    log.snapshot().await.unwrap();

    // Build RMI
    let pairs = log.collect_key_offset_pairs().await;
    let tmp = path.join("index-rmi.tmp");
    let dst = path.join("index-rmi.bin");
    kyrodb_engine::index::RmiIndex::write_from_pairs(&tmp, &pairs).unwrap();
    std::fs::rename(&tmp, &dst).unwrap();
    drop(log);

    let log2 = kyrodb_engine::PersistentEventLog::open(&path).await.unwrap();

    // Random keys from the set should be found using bounded window
    for i in (0..10_000u64).step_by(111) {
        let key = i * 10;
        assert!(log2.lookup_key(key).await.is_some(), "missing key {}", key);
    }
}
