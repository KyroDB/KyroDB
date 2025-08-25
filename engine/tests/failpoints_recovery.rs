#![cfg(feature = "failpoints")]

use kyrodb_engine::PersistentEventLog;
use tempfile::tempdir;
use uuid::Uuid;

async fn seed(log: &PersistentEventLog, n: u64) {
    for i in 0..n { let _ = log.append_kv(Uuid::new_v4(), i, vec![1]).await.unwrap(); }
}

#[tokio::test(flavor = "current_thread")]
async fn snapshot_fail_before_write_recoverable() {
    let _sc = fail::FailScenario::setup();
    // Trigger early return path in fail_point! closure
    fail::cfg("snapshot_before_write", "return").unwrap();
    let dir = tempdir().unwrap();
    let log = PersistentEventLog::open(dir.path()).await.unwrap();
    seed(&log, 10).await;
    // snapshot should error but not corrupt state
    let _ = log.snapshot().await.err().expect("failpoint must error");
    fail::remove("snapshot_before_write");
    // retry works
    log.snapshot().await.unwrap();
    // verify recovery
    let log2 = PersistentEventLog::open(dir.path()).await.unwrap();
    for i in 0..10 { assert!(log2.lookup_key(i).await.is_some()); }
}

#[tokio::test(flavor = "current_thread")]
async fn manifest_fail_before_rename_recoverable() {
    let _sc = fail::FailScenario::setup();
    let dir = tempdir().unwrap();
    let log = PersistentEventLog::open(dir.path()).await.unwrap();
    seed(&log, 5).await;
    // Trigger early return path in fail_point! closure
    fail::cfg("manifest_before_rename", "return").unwrap();
    let _ = log.write_manifest().await.err().expect("failpoint must error");
    fail::remove("manifest_before_rename");
    log.write_manifest().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn wal_rotate_fail_between_rotate_and_retention_is_safe() {
    let _sc = fail::FailScenario::setup();
    let dir = tempdir().unwrap();
    let log = PersistentEventLog::open(dir.path()).await.unwrap();
    // Small segments and low retention
    log.set_wal_rotation(Some(1_024), 2).await;
    // Seed to force rotation
    seed(&log, 200).await;
    // Arm failpoint just after rotate and before retention cleanup
    fail::cfg("wal_after_rotate_before_retention", "return").unwrap();
    // Append to trigger rotate path again
    for i in 0..100u64 { let _ = log.append_kv(Uuid::new_v4(), 10_000 + i, vec![0u8; 64]).await.unwrap(); }
    // Drop and reopen (simulate crash)
    drop(log);
    fail::remove("wal_after_rotate_before_retention");
    let log2 = PersistentEventLog::open(dir.path()).await.unwrap();
    // Data should still be recoverable
    for i in 0..200 { assert!(log2.lookup_key(i).await.is_some()); }
}
