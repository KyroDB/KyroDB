use kyrodb_engine::PersistentEventLog;
use std::io::Write;
use tempfile::tempdir;
use uuid::Uuid;

#[tokio::test]
async fn snapshot_tmp_is_ignored_if_not_renamed() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();

    {
        let log = PersistentEventLog::open(&path).await.unwrap();
        let _ = log.append(Uuid::new_v4(), b"x".to_vec()).await.unwrap();
        log.snapshot().await.unwrap();
    }

    // Write a snapshot.tmp manually but don't rename
    let tmp = path.join("snapshot.tmp");
    std::fs::File::create(&tmp)
        .unwrap()
        .write_all(b"garbage")
        .unwrap();

    // Restart: engine should ignore tmp and still recover from WAL
    let log2 = PersistentEventLog::open(&path).await.unwrap();
    assert_eq!(log2.get_offset().await, 1);
}
