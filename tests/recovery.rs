use ngdb_engine::PersistentEventLog;
use uuid::Uuid;
use std::fs;

#[tokio::test]
async fn test_recovery_after_restart() {
    // Prepare a temp directory
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();

    // 1st run: append two events, write snapshot
    {
        let log = PersistentEventLog::open(&path).await.unwrap();
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();
        assert_eq!(log.append(id1, b"a".to_vec()).await.unwrap(), 0);
        assert_eq!(log.append(id2, b"b".to_vec()).await.unwrap(), 1);
        log.snapshot().await.unwrap();
    }

    // 2nd run: append one more without snapshot
    {
        let log = PersistentEventLog::open(&path).await.unwrap();
        // The in-memory should have loaded 2 events
        assert_eq!(log.get_offset().await, 2);
        let id3 = Uuid::new_v4();
        assert_eq!(log.append(id3, b"c".to_vec()).await.unwrap(), 2);
    }

    // 3rd run: recover full state
    {
        let log = PersistentEventLog::open(&path).await.unwrap();
        let events = log.replay(0, None).await;
        let payloads: Vec<_> = events.iter().map(|e| e.payload.clone()).collect();
        assert_eq!(payloads, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    }
}
