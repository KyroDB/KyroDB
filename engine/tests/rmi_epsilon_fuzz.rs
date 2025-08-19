#![cfg(feature = "learned-index")]
use rand::distributions::{Distribution, Uniform};
use rand::SeedableRng;
use uuid::Uuid;

async fn build_and_check(dist_keys: Vec<u64>) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let log = kyrodb_engine::PersistentEventLog::open(&path)
        .await
        .unwrap();

    for k in dist_keys.iter().copied() {
        let _ = log.append_kv(Uuid::new_v4(), k, vec![0u8]).await.unwrap();
    }
    log.snapshot().await.unwrap();
    let pairs = log.collect_key_offset_pairs().await;
    let tmp = path.join("index-rmi.tmp");
    let dst = path.join("index-rmi.bin");
    kyrodb_engine::index::RmiIndex::write_from_pairs(&tmp, &pairs).unwrap();
    std::fs::rename(&tmp, &dst).unwrap();
    drop(log);

    let log2 = kyrodb_engine::PersistentEventLog::open(&path)
        .await
        .unwrap();
    // sample 100 keys that are present and assert lookup works
    let mut cnt = 0;
    for (i, (k, _)) in pairs.iter().enumerate() {
        if i % 100 == 0 {
            cnt += 1;
            assert!(log2.lookup_key(*k).await.is_some(), "missing key {}", k);
            if cnt >= 100 {
                break;
            }
        }
    }
}

#[tokio::test]
async fn fuzz_uniform_keys() {
    let mut rng = rand::rngs::StdRng::seed_from_u64(42);
    let uni = Uniform::from(0u64..1_000_000);
    let mut keys: Vec<u64> = (0..50_000).map(|_| uni.sample(&mut rng)).collect();
    keys.sort();
    keys.dedup();
    build_and_check(keys).await;
}

#[tokio::test]
async fn fuzz_zipf_like_keys() {
    // approximate zipf by sampling many small numbers and occasional large jumps
    let mut rng = rand::rngs::StdRng::seed_from_u64(43);
    let uni_small = Uniform::from(0u64..1_000);
    let uni_large = Uniform::from(1_000_000u64..2_000_000);
    let mut keys = Vec::with_capacity(50_000);
    for i in 0..50_000u32 {
        if i % 1000 == 0 {
            keys.push(uni_large.sample(&mut rng));
        } else {
            keys.push(uni_small.sample(&mut rng));
        }
    }
    keys.sort();
    keys.dedup();
    build_and_check(keys).await;
}
