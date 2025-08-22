use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use kyrodb_engine::index::{BTreeIndex, RmiIndex};
use kyrodb_engine::Index; // bring trait into scope for insert/get
use rand::{rngs::StdRng, Rng, SeedableRng};

fn build_pairs(n: usize) -> Vec<(u64, u64)> {
    // Monotone keys 0..n with offsets equal to index for simplicity
    (0..n as u64).map(|k| (k, k)).collect()
}

fn bench_raw_indexes(c: &mut Criterion) {
    let n = std::env::var("KYRO_BENCH_N").ok().and_then(|s| s.parse().ok()).unwrap_or(5_000_000usize);
    let sample = std::env::var("KYRO_BENCH_SAMPLES").ok().and_then(|s| s.parse().ok()).unwrap_or(100_000usize);

    // Build dataset
    let pairs = build_pairs(n);

    // Build B-Tree index
    let mut bt = BTreeIndex::new();
    for (k, o) in &pairs {
        bt.insert(*k, *o);
    }

    // Build RMI index v4 (SoA) via file path then load
    let dir = tempfile::tempdir().unwrap();
    let v4_tmp = dir.path().join("index-rmi.v4.tmp");
    let v4_bin = dir.path().join("index-rmi.v4.bin");
    RmiIndex::write_from_pairs(&v4_tmp, &pairs).unwrap();
    std::fs::rename(&v4_tmp, &v4_bin).unwrap();
    let rmi_v4 = RmiIndex::load_from_file(&v4_bin).expect("load rmi v4");

    // Build RMI index v5 (AoS, offsets u32 when possible)
    let v5_tmp = dir.path().join("index-rmi.v5.tmp");
    let v5_bin = dir.path().join("index-rmi.v5.bin");
    let pack_u32 = (n as u64) <= (u32::MAX as u64);
    let _ = RmiIndex::write_from_pairs_v5(&v5_tmp, &pairs, pack_u32).unwrap();
    std::fs::rename(&v5_tmp, &v5_bin).unwrap();
    let rmi_v5 = RmiIndex::load_from_file(&v5_bin).expect("load rmi v5");

    // Randomized keys for iterations (shared seed)
    let mut rng = StdRng::seed_from_u64(0xBEEF);
    let keys: Vec<u64> = (0..sample).map(|_| rng.gen_range(0..n as u64)).collect();

    // Precompute predict windows for probe-only stage (v4)
    let mut triples_v4: Vec<(u64, usize, usize)> = Vec::with_capacity(sample);
    for &k in &keys {
        if let Some((lo, hi)) = rmi_v4.debug_predict_clamp(k) {
            triples_v4.push((k, lo, hi));
        } else {
            triples_v4.push((k, 0, 0));
        }
    }
    // Precompute predict windows for probe-only stage (v5)
    let mut triples_v5: Vec<(u64, usize, usize)> = Vec::with_capacity(sample);
    for &k in &keys {
        if let Some((lo, hi)) = rmi_v5.debug_predict_clamp(k) {
            triples_v5.push((k, lo, hi));
        } else {
            triples_v5.push((k, 0, 0));
        }
    }

    let mut group = c.benchmark_group(format!("raw_index_lookup_n{}", n));

    // BTree full
    group.bench_function(BenchmarkId::from_parameter("btree"), |b| {
        let mut i = 0usize;
        b.iter(|| {
            let k = keys[i % keys.len()];
            i = i.wrapping_add(1);
            let _ = black_box(bt.get(&k));
        })
    });

    // RMI v4 full
    group.bench_function(BenchmarkId::from_parameter("rmi_full_v4"), |b| {
        let mut i = 0usize;
        b.iter(|| {
            let k = keys[i % keys.len()];
            i = i.wrapping_add(1);
            let _ = black_box(rmi_v4.predict_get(&k));
        })
    });

    // RMI v5 full
    group.bench_function(BenchmarkId::from_parameter("rmi_full_v5"), |b| {
        let mut i = 0usize;
        b.iter(|| {
            let k = keys[i % keys.len()];
            i = i.wrapping_add(1);
            let _ = black_box(rmi_v5.predict_get(&k));
        })
    });

    // RMI v4: leaf-find only
    group.bench_function(BenchmarkId::from_parameter("rmi_leaf_find_v4"), |b| {
        let mut i = 0usize;
        b.iter(|| {
            let k = keys[i % keys.len()];
            i = i.wrapping_add(1);
            let _ = black_box(rmi_v4.debug_find_leaf_index(k));
        })
    });

    // RMI v4: predict + clamp
    group.bench_function(BenchmarkId::from_parameter("rmi_predict_clamp_v4"), |b| {
        let mut i = 0usize;
        b.iter(|| {
            let k = keys[i % keys.len()];
            i = i.wrapping_add(1);
            let _ = black_box(rmi_v4.debug_predict_clamp(k));
        })
    });

    // RMI v4: probe-only within [lo, hi]
    group.bench_function(BenchmarkId::from_parameter("rmi_probe_only_v4"), |b| {
        let mut i = 0usize;
        b.iter(|| {
            let (k, lo, hi) = triples_v4[i % triples_v4.len()];
            i = i.wrapping_add(1);
            let _ = black_box(rmi_v4.debug_probe_only(k, lo, hi));
        })
    });

    // RMI v5: leaf-find only
    group.bench_function(BenchmarkId::from_parameter("rmi_leaf_find_v5"), |b| {
        let mut i = 0usize;
        b.iter(|| {
            let k = keys[i % keys.len()];
            i = i.wrapping_add(1);
            let _ = black_box(rmi_v5.debug_find_leaf_index(k));
        })
    });

    // RMI v5: predict + clamp
    group.bench_function(BenchmarkId::from_parameter("rmi_predict_clamp_v5"), |b| {
        let mut i = 0usize;
        b.iter(|| {
            let k = keys[i % keys.len()];
            i = i.wrapping_add(1);
            let _ = black_box(rmi_v5.debug_predict_clamp(k));
        })
    });

    // RMI v5: probe-only within [lo, hi]
    group.bench_function(BenchmarkId::from_parameter("rmi_probe_only_v5"), |b| {
        let mut i = 0usize;
        b.iter(|| {
            let (k, lo, hi) = triples_v5[i % triples_v5.len()];
            i = i.wrapping_add(1);
            let _ = black_box(rmi_v5.debug_probe_only(k, lo, hi));
        })
    });

    group.finish();
}

criterion_group!(benches, bench_raw_indexes);
criterion_main!(benches);
