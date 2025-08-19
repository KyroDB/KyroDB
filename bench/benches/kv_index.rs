use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use kyrodb_engine as engine;
use std::sync::Arc;
use uuid::Uuid;

fn bench_engine_indexes(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Build identical datasets in two separate logs: one will use RMI, one B-Tree
    let (log_rmi, log_btree) = {
        let dir_rmi = std::env::temp_dir().join(format!("kyrobench-rmi-{}", Uuid::new_v4()));
        let dir_bt = std::env::temp_dir().join(format!("kyrobench-bt-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir_rmi).unwrap();
        std::fs::create_dir_all(&dir_bt).unwrap();
        rt.block_on(async move {
            let rmi = Arc::new(engine::PersistentEventLog::open(&dir_rmi).await.unwrap());
            let bt = Arc::new(engine::PersistentEventLog::open(&dir_bt).await.unwrap());

            // Load the same dataset into both
            for i in 0..100_000u64 {
                let payload = vec![0u8; 16];
                let _ = rmi
                    .append_kv(Uuid::new_v4(), i, payload.clone())
                    .await
                    .unwrap();
                let _ = bt
                    .append_kv(Uuid::new_v4(), i, payload.clone())
                    .await
                    .unwrap();
            }
            rmi.snapshot().await.unwrap();
            bt.snapshot().await.unwrap();

            // Build RMI for the rmi log only
            let pairs = rmi.collect_key_offset_pairs().await;
            let tmp = dir_rmi.join("index-rmi.tmp");
            let dst = dir_rmi.join("index-rmi.bin");
            engine::index::RmiIndex::write_from_pairs(&tmp, &pairs).unwrap();
            std::fs::rename(&tmp, &dst).unwrap();
            if let Some(rindex) = engine::index::RmiIndex::load_from_file(&dst) {
                rmi.swap_primary_index(engine::index::PrimaryIndex::Rmi(rindex))
                    .await;
            }

            // B-Tree stays as default primary
            (rmi, bt)
        })
    };

    let mut group = c.benchmark_group("engine_lookup_key");
    for (name, handle) in [("rmi", log_rmi), ("btree", log_btree)] {
        group.bench_function(BenchmarkId::from_parameter(name), |b| {
            b.to_async(&rt).iter(|| async {
                let k = 77_777u64;
                let _ = handle.lookup_key(black_box(k)).await;
            })
        });
    }
    group.finish();
}

criterion_group!(benches, bench_engine_indexes);
criterion_main!(benches);
