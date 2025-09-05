use kyrodb_engine::PersistentEventLog;
use uuid::Uuid;

#[tokio::test]
async fn kv_append_lookup_across_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();

    // First run: append KV and snapshot
    let key = 42u64;
    let value = b"hello".to_vec();
    {
        let log = PersistentEventLog::open(&path).await.unwrap();
        let off = log.append_kv(Uuid::new_v4(), key, value.clone()).await.unwrap();
        assert_eq!(log.lookup_key(key).await, Some(off));
        log.snapshot().await.unwrap();
    }

    // Second run: WAL should be truncated; index rebuilt from snapshot
    {
        let log = PersistentEventLog::open(&path).await.unwrap();
        assert_eq!(log.lookup_key(key).await.is_some(), true);
        let off = log.lookup_key(key).await.unwrap();
        let evs = log.replay(off, Some(off + 1)).await;
        let rec = bincode::deserialize::<kyrodb_engine::Record>(&evs[0].payload).unwrap();
        assert_eq!(rec.key, key);
        assert_eq!(rec.value, value);
    }
}