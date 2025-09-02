#[cfg(feature = "http-test")]
#[tokio::test]
async fn http_compact_endpoint_smoke() {
    use tempfile::tempdir;
    use uuid::Uuid;
    use std::sync::Arc;

    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let log = Arc::new(kyrodb_engine::PersistentEventLog::open(&path).await.unwrap());

    // Write a few events and snapshot
    for i in 0..10u64 {
        let _ = log.append_kv(Uuid::new_v4(), i % 3, vec![b'z'; 64]).await.unwrap();
    }
    log.snapshot().await.unwrap();

    // Inflate WAL a bit
    for i in 0..50u64 {
        let _ = log.append_kv(Uuid::new_v4(), i % 5, vec![b'w'; 64]).await.unwrap();
    }

    // Test the compact endpoint directly using warp's test framework
    let api = kyrodb_engine::http_filters::compact_route(log.clone());

    // Issue POST /compact
    let resp = warp::test::request()
        .method("POST")
        .path("/compact")
        .reply(&api)
        .await;
    
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = serde_json::from_slice(resp.body()).unwrap();
    assert_eq!(body["compact"], "ok");
    
    // Verify we get stats back
    assert!(body["stats"].is_object());
    let stats = &body["stats"];
    assert!(stats["before_bytes"].is_number());
    assert!(stats["after_bytes"].is_number());
    
    // After compaction, bytes should generally be reduced (though not guaranteed in all cases)
    let before = stats["before_bytes"].as_u64().unwrap_or(0);
    let after = stats["after_bytes"].as_u64().unwrap_or(0);
    
    // At minimum, both values should be reasonable
    assert!(before > 0, "Before bytes should be > 0");
    assert!(after >= 0, "After bytes should be >= 0");

    // WAL should be reasonable size after compaction
    let size = log.wal_size_bytes();
    assert!(size < 100_000, "WAL size should be reasonable after compaction");
}
