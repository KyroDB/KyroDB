use criterion::{black_box, criterion_group, criterion_main, Criterion};
use kyrodb_engine as engine;
use uuid::Uuid;
use std::sync::Arc;

fn bench_engine_rmi_lookup(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    // One-time setup outside the timed loop
    let log: Arc<engine::PersistentEventLog> = {
        let dir = std::env::temp_dir().join(format!("kyrobench-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        rt.block_on(async move {
            let log = Arc::new(engine::PersistentEventLog::open(&dir).await.unwrap());
            for i in 0..100_000u64 {
                let _ = log.append_kv(Uuid::new_v4(), i, vec![0u8; 16]).await.unwrap();
            }
            log.snapshot().await.unwrap();
            let pairs = log.collect_key_offset_pairs().await;
            let tmp = dir.join("index-rmi.tmp");
            let dst = dir.join("index-rmi.bin");
            engine::index::RmiIndex::write_from_pairs(&tmp, &pairs).unwrap();
            std::fs::rename(&tmp, &dst).unwrap();
            if let Some(rmi) = engine::index::RmiIndex::load_from_file(&dst) {
                log.swap_primary_index(engine::index::PrimaryIndex::Rmi(rmi)).await;
            }
            log
        })
    };

    c.bench_function("engine_rmi_lookup_key", |b| {
        b.to_async(&rt).iter(|| async {
            let k = 77_777u64;
            let _ = log.lookup_key(black_box(k)).await;
        })
    });
}

criterion_group!(benches, bench_engine_rmi_lookup);
criterion_main!(benches);
