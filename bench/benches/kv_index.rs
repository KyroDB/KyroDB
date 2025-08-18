use criterion::{criterion_group, criterion_main, Criterion, BatchSize, black_box};
use kyrodb_engine as engine;
use rand::Rng;

fn bench_btree_get(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    c.bench_function("btree_get", |b| {
        b.to_async(&rt).iter_batched(
            || {
                rt.block_on(async {
                    let dir = tempfile::tempdir().unwrap();
                    let log = engine::PersistentEventLog::open(dir.path()).await.unwrap();
                    for i in 0..100_000u64 {
                        let _ = log.append_kv(uuid::Uuid::new_v4(), i, vec![0u8; 8]).await.unwrap();
                    }
                    (log, dir)
                })
            },
            |(log, _dir)| async move {
                let k = rand::thread_rng().gen_range(0..100_000);
                let _ = log.get(black_box(k)).await;
            },
            BatchSize::SmallInput,
        );
    });
}

#[cfg(feature = "learned-index")]
fn bench_rmi_predict_get(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    c.bench_function("rmi_predict_get", |b| {
        b.to_async(&rt).iter_batched(
            || {
                rt.block_on(async {
                    let dir = tempfile::tempdir().unwrap();
                    let log = engine::PersistentEventLog::open(dir.path()).await.unwrap();
                    for i in 0..100_000u64 {
                        let _ = log.append_kv(uuid::Uuid::new_v4(), i, vec![0u8; 8]).await.unwrap();
                    }
                    // build RMI and swap it in
                    let pairs = log.collect_key_offset_pairs().await;
                    let tmp = dir.path().join("index-rmi.tmp");
                    let dst = dir.path().join("index-rmi.bin");
                    let leaf_target = 1024usize;
                    engine::index::RmiIndex::write_from_pairs(&tmp, &pairs, leaf_target).unwrap();
                    std::fs::rename(&tmp, &dst).unwrap();
                    if let Some(rmi) = engine::index::RmiIndex::load_from_file(&dst) {
                        log.swap_primary_index(engine::index::PrimaryIndex::Rmi(rmi)).await;
                    }
                    (log, dir)
                })
            },
            |(log, _dir)| async move {
                let k = rand::thread_rng().gen_range(0..100_000);
                let _ = log.get(black_box(k)).await;
            },
            BatchSize::SmallInput,
        );
    });
}

#[cfg(not(feature = "learned-index"))]
fn bench_rmi_predict_get(_c: &mut Criterion) {}

criterion_group!(benches, bench_btree_get, bench_rmi_predict_get);
criterion_main!(benches);
