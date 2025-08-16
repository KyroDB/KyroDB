use kyrodb_engine::PersistentEventLog;
use tempfile::tempdir;
use uuid::Uuid;

#[tokio::test]
async fn subscribe_streams_past_and_live_after_restart() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();

    // First run: write a few events
    {
        let log = PersistentEventLog::open(&path).await.unwrap();
        for _ in 0..3 { let _ = log.append(Uuid::new_v4(), b"x".to_vec()).await.unwrap(); }
    }

    // Second run: subscribe from 0, ensure past delivered, then live gets one more
    let log = PersistentEventLog::open(&path).await.unwrap();
    let (past, mut rx) = log.subscribe(0).await;
    assert_eq!(past.len(), 3);

    let _ = log.append(Uuid::new_v4(), b"y".to_vec()).await.unwrap();
    let evt = rx.recv().await.expect("live event");
    assert_eq!(evt.payload, b"y".to_vec());
}
