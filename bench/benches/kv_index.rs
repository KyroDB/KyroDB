use criterion::{criterion_group, criterion_main, Criterion, black_box};
use kyrodb_engine as engine;
use rand::Rng;

fn bench_btree_get(c: &mut Criterion) {
    // Build a single Tokio runtime and reuse it without nesting runtimes.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    // One-time setup: create a temp log, bulk load, snapshot for mmap-backed reads
    let (log, _dir) = rt.block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let log = engine::PersistentEventLog::open(dir.path()).await.unwrap();
        for i in 0..100_000u64 {
            let _ = log
                .append_kv(uuid::Uuid::new_v4(), i, vec![0u8; 8])
                .await
                .unwrap();
        }
        // Ensure reads hit the mmap snapshot path, not WAL scan (not used in this bench)
        log.snapshot().await.unwrap();
        (log, dir)
    });

    c.bench_function("btree_lookup_key", |b| {
        b.iter(|| {
            let k = rand::thread_rng().gen_range(0..100_000);
            let _ = rt.block_on(log.lookup_key(black_box(k)));
        });
    });
}

// Ultra-fast sync lookup benchmark (no async overhead)
fn bench_btree_lookup_sync(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let (log, _dir) = rt.block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let log = engine::PersistentEventLog::open(dir.path()).await.unwrap();
        for i in 0..100_000u64 {
            let _ = log
                .append_kv(uuid::Uuid::new_v4(), i, vec![0u8; 8])
                .await
                .unwrap();
        }
        log.snapshot().await.unwrap();
        (log, dir)
    });

    c.bench_function("btree_lookup_sync", |b| {
        b.iter(|| {
            let k = rand::thread_rng().gen_range(0..100_000);
            let _ = log.lookup_key_sync(black_box(k));
        });
    });
}

// New: apples-to-apples full get() for BTree index (includes record fetch)
fn bench_btree_get_full(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let (log, _dir) = rt.block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let log = engine::PersistentEventLog::open(dir.path()).await.unwrap();
        for i in 0..100_000u64 {
            let _ = log
                .append_kv(uuid::Uuid::new_v4(), i, vec![0u8; 8])
                .await
                .unwrap();
        }
        // Snapshot to stabilize storage
        log.snapshot().await.unwrap();
        (log, dir)
    });

    c.bench_function("btree_get_full", |b| {
        b.iter(|| {
            let k = rand::thread_rng().gen_range(0..100_000);
            let _ = rt.block_on(log.get(black_box(k)));
        });
    });
}

// Ultra-fast sync get benchmark
fn bench_btree_get_sync(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let (log, _dir) = rt.block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let log = engine::PersistentEventLog::open(dir.path()).await.unwrap();
        for i in 0..100_000u64 {
            let _ = log
                .append_kv(uuid::Uuid::new_v4(), i, vec![0u8; 8])
                .await
                .unwrap();
        }
        log.snapshot().await.unwrap();
        (log, dir)
    });

    c.bench_function("btree_get_sync", |b| {
        b.iter(|| {
            let k = rand::thread_rng().gen_range(0..100_000);
            let _ = log.get_sync(black_box(k));
        });
    });
}

#[cfg(feature = "learned-index")]
fn bench_rmi_predict_get(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    // One-time setup: create a temp log, bulk load, snapshot, build+swap RMI
    let (log, _dir) = rt.block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let log = engine::PersistentEventLog::open(dir.path()).await.unwrap();
        for i in 0..100_000u64 {
            let _ = log
                .append_kv(uuid::Uuid::new_v4(), i, vec![0u8; 8])
                .await
                .unwrap();
        }
        // Snapshot to stabilize storage and mmap reads
        log.snapshot().await.unwrap();

        // Build RMI and swap it in
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
    });

    c.bench_function("rmi_lookup_key", |b| {
        b.iter(|| {
            let k = rand::thread_rng().gen_range(0..100_000);
            let _ = rt.block_on(log.lookup_key(black_box(k)));
        });
    });
}

#[cfg(feature = "learned-index")]
fn bench_rmi_lookup_sync(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let (log, _dir) = rt.block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let log = engine::PersistentEventLog::open(dir.path()).await.unwrap();
        for i in 0..100_000u64 {
            let _ = log
                .append_kv(uuid::Uuid::new_v4(), i, vec![0u8; 8])
                .await
                .unwrap();
        }
        log.snapshot().await.unwrap();

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
    });

    c.bench_function("rmi_lookup_sync", |b| {
        b.iter(|| {
            let k = rand::thread_rng().gen_range(0..100_000);
            let _ = log.lookup_key_sync(black_box(k));
        });
    });
}

#[cfg(feature = "learned-index")]
fn bench_rmi_get_sync(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let (log, _dir) = rt.block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let log = engine::PersistentEventLog::open(dir.path()).await.unwrap();
        for i in 0..100_000u64 {
            let _ = log
                .append_kv(uuid::Uuid::new_v4(), i, vec![0u8; 8])
                .await
                .unwrap();
        }
        log.snapshot().await.unwrap();

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
    });

    c.bench_function("rmi_get_sync", |b| {
        b.iter(|| {
            let k = rand::thread_rng().gen_range(0..100_000);
            let _ = log.get_sync(black_box(k));
        });
    });
}

#[cfg(not(feature = "learned-index"))]
fn bench_rmi_predict_get(_c: &mut Criterion) {}
#[cfg(not(feature = "learned-index"))]
fn bench_rmi_lookup_sync(_c: &mut Criterion) {}
#[cfg(not(feature = "learned-index"))]
fn bench_rmi_get_sync(_c: &mut Criterion) {}

criterion_group!(benches, bench_btree_get, bench_btree_lookup_sync, bench_btree_get_full, bench_btree_get_sync, bench_rmi_predict_get, bench_rmi_lookup_sync, bench_rmi_get_sync);
criterion_main!(benches);
