#![cfg(feature = "learned-index")]

use kyrodb_engine::index::{PrimaryIndex, RmiIndex};
use kyrodb_engine::PersistentEventLog;
use tempfile::tempdir;
use uuid::Uuid;

#[tokio::test]
async fn rmi_swap_preserves_visibility_of_recent_writes() {
    let dir = tempdir().unwrap();
    let log = PersistentEventLog::open(dir.path()).await.unwrap();

    // Seed some keys
    for k in 0..100u64 {
        let _ = log.append_kv(Uuid::new_v4(), k, vec![1]).await.unwrap();
    }

    // Force learned index selection by writing a small RMI file and swapping it in
    // Collect pairs from current state
    let pairs = log.collect_key_offset_pairs().await;
    let rmi_path = dir.path().join("index-rmi.bin");
    RmiIndex::write_from_pairs_auto(&rmi_path, &pairs).unwrap();
    let new_rmi = RmiIndex::load_from_file(&rmi_path).expect("load rmi");

    // Concurrently write hot keys while swapping index
    let writer = {
        let log = log.clone();
        tokio::spawn(async move {
            for k in 0..100u64 {
                let _ = log.append_kv(Uuid::new_v4(), k, vec![2]).await.unwrap();
            }
        })
    };

    log.swap_primary_index(PrimaryIndex::Rmi(new_rmi)).await;
    let _ = writer.await;

    // All hot keys must be visible with their latest offsets
    for k in 0..100u64 {
        let off = log.lookup_key(k).await.expect("key must exist");
        // the latest append should be at highest offset; quick sanity: lookup resolves to some offset >= first batch
        assert!(off >= 100, "key {k} resolved to stale offset {off}");
    }
}
