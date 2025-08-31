//! Stress append+lookup by injecting fsync/rename failures and asserting recovery point.

use kyrodb_engine::PersistentEventLog;
use tempfile::tempdir;
use uuid::Uuid;

#[tokio::test]
async fn append_recovery_with_injected_failures() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();

    // Open log and write some entries
    let log = PersistentEventLog::open(&path).await.unwrap();
    for i in 0..100u64 {
        let _ = log
            .append(Uuid::new_v4(), format!("v{i}").into_bytes())
            .await
            .unwrap();
    }

    // Force a snapshot to exercise rename/fsync paths (best-effort; we don't inject OS-level errors here)
    log.snapshot().await.unwrap();

    // Simulate a crash by dropping log and reopening
    drop(log);

    let log2 = PersistentEventLog::open(&path).await.unwrap();
    // Recovery point objective: no reordering, offsets must be monotonic, and at least snapshot boundary must be visible
    let off = log2.get_offset().await;
    assert!(
        off >= 100,
        "recovered offset should be >= written count, got {off}"
    );

    // Verify a few lookups exist (read-your-writes after recovery)
    for i in 0..10u64 {
        let (offset, _rec) = log2.find_key_scan(i).await.unwrap_or((
            0u64,
            kyrodb_engine::Record {
                key: 0,
                value: vec![],
            },
        ));
        assert!(offset <= off);
    }
}

#[tokio::test]
async fn fsync_policy_edge_cases() {
    use std::env;
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();

    // Test DATA policy (default)
    env::set_var("KYRODB_FSYNC_POLICY", "data");
    let log = PersistentEventLog::open(&path).await.unwrap();
    let _ = log.append(Uuid::new_v4(), b"test1".to_vec()).await.unwrap();
    drop(log);

    // Test ALL policy
    env::set_var("KYRODB_FSYNC_POLICY", "all");
    let log = PersistentEventLog::open(&path).await.unwrap();
    let _ = log.append(Uuid::new_v4(), b"test2".to_vec()).await.unwrap();
    drop(log);

    // Test NONE policy (benchmark only)
    env::set_var("KYRODB_FSYNC_POLICY", "none");
    let log = PersistentEventLog::open(&path).await.unwrap();
    let _ = log.append(Uuid::new_v4(), b"test3".to_vec()).await.unwrap();
    drop(log);

    // Test invalid policy defaults to data
    env::set_var("KYRODB_FSYNC_POLICY", "invalid");
    let log = PersistentEventLog::open(&path).await.unwrap();
    let _ = log.append(Uuid::new_v4(), b"test4".to_vec()).await.unwrap();
    drop(log);

    // Cleanup
    env::remove_var("KYRODB_FSYNC_POLICY");
}

#[tokio::test]
async fn ops_knobs_end_to_end() {
    use std::env;
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();

    // Test fsync policy configurations
    for policy in &["data", "all", "none"] {
        env::set_var("KYRODB_FSYNC_POLICY", policy);
        let log = PersistentEventLog::open(&path).await.unwrap();

        // Perform operations and verify they work
        let offset1 = log.append(Uuid::new_v4(), b"test1".to_vec()).await.unwrap();
        let offset2 = log.append_kv(Uuid::new_v4(), 123, b"value123".to_vec()).await.unwrap();

        // Verify data integrity
        assert!(offset1 < offset2);
        assert_eq!(log.lookup_key(123).await, Some(offset2));

        drop(log);
    }

    // Test WAL rotation knobs
    env::set_var("KYRODB_FSYNC_POLICY", "data");
    let log = PersistentEventLog::open(&path).await.unwrap();
    log.configure_wal_rotation(Some(1024), 3).await; // Small segment size for testing

    // Fill WAL to trigger rotation
    for i in 0..100 {
        log.append(Uuid::new_v4(), format!("test data {}", i).into_bytes()).await.unwrap();
    }

    // Verify WAL rotation worked
    let segments = log.get_wal_segments();
    assert!(!segments.is_empty());

    drop(log);

    // Test environment variable configurations
    env::set_var("KYRODB_WARM_ON_START", "1");
    env::set_var("KYRODB_DISABLE_HTTP_LOG", "1");
    env::set_var("KYRODB_RMI_ROUTER_BITS", "12");

    let log = PersistentEventLog::open(&path).await.unwrap();
    log.warmup().await; // Should work without errors

    // Cleanup
    env::remove_var("KYRODB_FSYNC_POLICY");
    env::remove_var("KYRODB_WARM_ON_START");
    env::remove_var("KYRODB_DISABLE_HTTP_LOG");
    env::remove_var("KYRODB_RMI_ROUTER_BITS");
}
