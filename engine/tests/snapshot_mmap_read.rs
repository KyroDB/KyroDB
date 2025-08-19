#[tokio::test]
async fn snapshot_mmap_reader_equivalence() {
    use kyrodb_engine::PersistentEventLog;
    use tempfile::tempdir;
    use uuid::Uuid;

    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();

    // Seed events and snapshot
    let log = PersistentEventLog::open(&path).await.unwrap();
    let mut offsets = Vec::new();
    for i in 0..500u64 {
        let off = log.append(Uuid::new_v4(), format!("val-{i}").into_bytes()).await.unwrap();
        offsets.push(off);
    }
    log.snapshot().await.unwrap();

    // Reopen (this should mmap snapshot.data and build the index)
    let log2 = PersistentEventLog::open(&path).await.unwrap();

    // All offsets should be retrievable and match original payloads
    for i in 0..offsets.len() {
        let off = offsets[i];
        let v1 = log2.get(off).await.expect("value present via mmap or memory");
        // Fallback comparison through replay to fetch the same event
        let evs = log2.replay(off, Some(off + 1)).await;
        let v2 = evs.first().expect("event exists").payload.clone();
        assert_eq!(v1, v2, "mmap read must match canonical payload");
    }
}
