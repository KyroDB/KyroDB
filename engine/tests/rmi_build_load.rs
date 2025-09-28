#[cfg(feature = "learned-index")]
use kyrodb_engine::PersistentEventLog;
#[cfg(feature = "learned-index")]
use tempfile::tempdir;
#[cfg(feature = "learned-index")]
use uuid::Uuid;

#[cfg(feature = "learned-index")]
#[tokio::test]
async fn rmi_build_and_load_lookup_works() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();

    // write KV
    let log = PersistentEventLog::open(&path).await.unwrap();
    let o1 = log.append_kv(Uuid::new_v4(), 10, b"a".to_vec()).await.unwrap();
    let o2 = log.append_kv(Uuid::new_v4(), 20, b"b".to_vec()).await.unwrap();
    log.snapshot().await.unwrap();

    // rebuild adaptive index directly
    log.build_rmi().await.unwrap();

    drop(log);

    let log2 = PersistentEventLog::open(&path).await.unwrap();
    // Should be able to lookup via RMI (or delta)
    assert_eq!(log2.lookup_key(10).await, Some(o1));
    assert_eq!(log2.lookup_key(20).await, Some(o2));
}
