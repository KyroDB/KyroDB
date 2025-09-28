use kyrodb_engine::PersistentEventLog;
use std::io::Write;
use tempfile::tempdir;
use uuid::Uuid;

#[tokio::test]
async fn wal_tail_corruption_ignored() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();

    // initial run
    let log = PersistentEventLog::open(&path).await.unwrap();
    let _ = log.append(Uuid::new_v4(), b"a".to_vec()).await.unwrap();
    let _ = log.append(Uuid::new_v4(), b"b".to_vec()).await.unwrap();
    let _ = log.append(Uuid::new_v4(), b"c".to_vec()).await.unwrap();
    let expected = log.get_offset().await;
    drop(log);

    // Corrupt WAL tail by appending incomplete bytes
    let wal_path = path.join("wal.bin");
    let mut f = std::fs::OpenOptions::new()
        .append(true)
        .open(&wal_path)
        .unwrap();
    let _ = f.write_all(&[0xFF, 0xFF]);
    let _ = f.flush();

    // Restart and verify recovery ignores the corrupt tail
    let log2 = PersistentEventLog::open(&path).await.unwrap();
    assert_eq!(log2.get_offset().await, expected);
}
