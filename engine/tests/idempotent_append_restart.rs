use kyrodb_engine::PersistentEventLog;
use tempfile::tempdir;
use uuid::Uuid;

#[tokio::test]
async fn idempotent_append_is_preserved_across_restart() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();

    let req = Uuid::new_v4();
    let off_saved = {
        let log = PersistentEventLog::open(&path).await.unwrap();
        log.append(req, b"hello".to_vec()).await.unwrap()
    };

    // Second append with same request should return same offset even after restart
    let log2 = PersistentEventLog::open(&path).await.unwrap();
    let off2 = log2.append(req, b"hello".to_vec()).await.unwrap();
    assert_eq!(off2, off_saved);
}
