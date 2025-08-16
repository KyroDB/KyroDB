#[cfg(feature = "http-test")]
#[tokio::test]
async fn http_compact_endpoint_smoke() {
    use warp::Filter;
    use uuid::Uuid;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let log = std::sync::Arc::new(kyrodb_engine::PersistentEventLog::open(&path).await.unwrap());

    // write a few events and snapshot
    for i in 0..10u64 {
        let _ = log.append_kv(Uuid::new_v4(), i % 3, vec![b'z'; 64]).await.unwrap();
    }
    log.snapshot().await.unwrap();

    // inflate wal a bit
    for i in 0..50u64 {
        let _ = log.append_kv(Uuid::new_v4(), i % 5, vec![b'w'; 64]).await.unwrap();
    }

    let api = kyrodb_engine::http_filters::compact_route(log.clone());

    // Issue POST /compact
    let resp = warp::test::request()
        .method("POST")
        .path("/compact")
        .reply(&api)
        .await;
    assert_eq!(resp.status(), 200);

    // WAL should shrink after compaction
    let size = log.wal_size_bytes();
    assert!(size < 10_000);
}
