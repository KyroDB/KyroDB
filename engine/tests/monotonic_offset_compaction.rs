use kyrodb_engine::PersistentEventLog;
use tempfile::tempdir;
use uuid::Uuid;

#[tokio::test]
async fn monotonic_offset_across_compaction() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();

    let log = PersistentEventLog::open(&path).await.unwrap();

    // Write two versions for the same key
    let o1 = log.append_kv(Uuid::new_v4(), 42, b"v1".to_vec()).await.unwrap();
    let o2 = log.append_kv(Uuid::new_v4(), 42, b"v2".to_vec()).await.unwrap();
    assert!(o2 > o1);

    // Compact and snapshot
    log.compact_keep_latest_and_snapshot().await.unwrap();

    // Offsets must be monotonic; get_offset reflects next monotonic value
    let next = log.get_offset().await;
    assert!(next > o2);

    // Lookup returns latest
    let off = log.lookup_key(42).await.unwrap();
    assert_eq!(off, o2);

    drop(log);

    // Restart and verify
    let log2 = PersistentEventLog::open(&path).await.unwrap();
    assert_eq!(log2.lookup_key(42).await, Some(o2));
    assert_eq!(log2.get_offset().await, next);
}
