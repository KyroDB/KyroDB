use uuid::Uuid;

#[tokio::test]
async fn wal_rotation_and_retention_enforced() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let log = kyrodb_engine::PersistentEventLog::open(&path)
        .await
        .unwrap();

    // Configure small segment size and retain only 2 segments
    log.set_wal_rotation(Some(2_000), 2).await; // ~2KB per segment

    // Write enough to rotate a few times
    for i in 0..2_000u64 {
        let _ = log
            .append_kv(Uuid::new_v4(), i, vec![b'a'; 256])
            .await
            .unwrap();
        if i % 200 == 0 {
            // let rotation catch up
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }

    // Allow async rotation/retention to run and manifest to update
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Count wal.* files on disk (retention should keep at most 2)
    let mut seg_count = 0usize;
    for e in std::fs::read_dir(&path).unwrap() {
        let e = e.unwrap();
        let name = e.file_name();
        if let Some(s) = name.to_str() {
            if s.starts_with("wal.") {
                seg_count += 1;
            }
        }
    }
    assert!(seg_count <= 2, "expected <=2 segments, found {}", seg_count);

    // Compaction should reset to a single fresh segment and shrink total bytes
    let before = log.wal_size_bytes().await;
    let stats = log.compact_keep_latest_and_snapshot_stats().await.unwrap();
    let after = log.wal_size_bytes().await;
    assert_eq!(stats.before_bytes, before, "stat before bytes should match measurement");
    assert_eq!(stats.after_bytes, after, "stat after bytes should match measurement");
    assert!(after <= before, "compaction should not increase WAL size");

    // After snapshot+reset, segments should be 1
    let mut seg_count_after = 0usize;
    for e in std::fs::read_dir(&path).unwrap() {
        let e = e.unwrap();
        let name = e.file_name();
        if let Some(s) = name.to_str() {
            if s.starts_with("wal.") {
                seg_count_after += 1;
            }
        }
    }
    assert_eq!(seg_count_after, 1);
}
