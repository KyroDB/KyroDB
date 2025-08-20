use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use kyrodb_engine::index::{BTreeIndex, RmiIndex};
use rand::{rngs::StdRng, Rng, SeedableRng};

fn build_pairs(n: usize) -> Vec<(u64, u64)> {
    // Monotone keys 0..n with offsets equal to index for simplicity
    (0..n as u64).map(|k| (k, k)).collect()
}

fn bench_raw_indexes(c: &mut Criterion) {
    let n = std::env::var("KYRO_BENCH_N").ok().and_then(|s| s.parse().ok()).unwrap_or(1_000_000usize);

    // Build dataset
    let pairs = build_pairs(n);

    // Build B-Tree index
    let mut bt = BTreeIndex::new();
    for (k, o) in &pairs {
        bt.insert(*k, *o);
    }

    // Build RMI index (owned) via file path then load
    let dir = tempfile::tempdir().unwrap();
    let tmp = dir.path().join("index-rmi.tmp");
    let dst = dir.path().join("index-rmi.bin");
    RmiIndex::write_from_pairs(&tmp, &pairs).unwrap();
    std::fs::rename(&tmp, &dst).unwrap();
    let rmi = RmiIndex::load_from_file(&dst).expect("load rmi");

    // Randomized keys per iteration
    let mut group = c.benchmark_group(format!("raw_index_lookup_n{}", n));

    group.bench_function(BenchmarkId::from_parameter("btree"), |b| {
        let mut rng = StdRng::seed_from_u64(0xBEEF);
        b.iter(|| {
            let k = rng.gen_range(0..n as u64);
            let _ = black_box(bt.get(&k));
        })
    });

    group.bench_function(BenchmarkId::from_parameter("rmi"), |b| {
        let mut rng = StdRng::seed_from_u64(0xBEEF);
        b.iter(|| {
            let k = rng.gen_range(0..n as u64);
            let _ = black_box(rmi.predict_get(&k));
        })
    });

    group.finish();
}

criterion_group!(benches, bench_raw_indexes);
criterion_main!(benches);
