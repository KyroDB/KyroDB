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
    // Use full working set by default so we don't overfit to warmed caches
    let sample = std::env::var("KYRO_BENCH_SAMPLES").ok().and_then(|s| s.parse().ok()).unwrap_or(n);
    // Optional cache-busting to induce LLC/dTLB misses (MiB)
    let cache_bust_mb = std::env::var("KYRO_BENCH_CACHE_BUST_MB").ok().and_then(|s| s.parse().ok()).unwrap_or(0usize);
    // Simulated value size (bytes) and arena memory to map offsets into (MiB)
    let val_bytes = std::env::var("KYRO_BENCH_VAL_BYTES").ok().and_then(|s| s.parse().ok()).unwrap_or(64usize);
    let arena_mb = std::env::var("KYRO_BENCH_VALUE_ARENA_MB").ok().and_then(|s| s.parse().ok()).unwrap_or(512usize);
    // Hotset parameters to mimic skew: fraction of accesses and size of hot set
    let hot_frac: f64 = std::env::var("KYRO_BENCH_HOT_FRAC").ok().and_then(|s| s.parse().ok()).unwrap_or(0.2);
    let hot_set_frac: f64 = std::env::var("KYRO_BENCH_HOT_SET_FRAC").ok().and_then(|s| s.parse().ok()).unwrap_or(0.01);

    // Build dataset
    let pairs = build_pairs(n);

    // Build B-Tree index
    let mut bt = BTreeIndex::new();
    for (k, o) in &pairs { bt.insert(*k, *o); }

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

    // Randomized keys for iterations with hot/cold skew
    let mut rng = StdRng::seed_from_u64(0xBEEF);
    let hot_set = ((n as f64) * hot_set_frac).max(1.0) as u64;
    let mut keys: Vec<u64> = Vec::with_capacity(sample);
    for _ in 0..sample {
        let roll: f64 = rng.gen();
        if roll < hot_frac {
            keys.push(rng.gen_range(0..hot_set));
        } else {
            keys.push(rng.gen_range(0..n as u64));
        }
    }

    // Precompute predict windows for probe-only stage (v4)
    let mut triples_v4: Vec<(u64, usize, usize)> = Vec::with_capacity(sample);
    for &k in &keys { if let Some((lo, hi)) = rmi_v4.debug_predict_clamp(k) { triples_v4.push((k, lo, hi)); } else { triples_v4.push((k, 0, 0)); } }
    // Precompute predict windows for probe-only stage (v5)
    let mut triples_v5: Vec<(u64, usize, usize)> = Vec::with_capacity(sample);
    for &k in &keys { if let Some((lo, hi)) = rmi_v5.debug_predict_clamp(k) { triples_v5.push((k, lo, hi)); } else { triples_v5.push((k, 0, 0)); } }

    // Cache-busting buffer and value arena to simulate value fetch
    let mut poison: Vec<u8> = if cache_bust_mb > 0 { vec![0u8; cache_bust_mb * 1024 * 1024] } else { Vec::new() };
    let mut poison_idx: usize = 0;
    let mut arena: Vec<u8> = if arena_mb > 0 { vec![0u8; arena_mb * 1024 * 1024] } else { vec![0u8; 1] };
    // stride ensure we touch a few cachelines per value read
    let val_stride = 64usize.max(val_bytes.next_power_of_two().min(4096));

    #[inline(always)]
    fn cache_bust(buf: &mut [u8], idx: &mut usize) {
        if buf.is_empty() { return; }
        // Touch ~1 MiB: 256 steps * 4 KiB
        let step = 4096;
        for _ in 0..256 { let i = *idx; unsafe { *buf.get_unchecked_mut(i) = buf.get_unchecked(i).wrapping_add(1); } *idx = (i + step) % buf.len(); }
    }

    #[inline(always)]
    fn simulate_value_read(arena: &mut [u8], base_off: u64, val_bytes: usize, val_stride: usize) {
        if arena.is_empty() { return; }
        let len = arena.len();
        // Map logical offset to arena bounds
        let start = ((base_off as usize).wrapping_mul(val_stride)) % (len.saturating_sub(val_stride).max(1));
        // Touch first byte in each cache line up to val_bytes
        let mut off = start;
        let end = start + val_bytes.min(val_stride);
        while off < end { unsafe { let v = *arena.get_unchecked(off); black_box(v); } off += 64; }
    }

    let mut group = c.benchmark_group(format!("raw_index_lookup_n{}", n));

    // BTree full (warm CPU-level lookup only)
    group.bench_function(BenchmarkId::from_parameter("btree"), |b| {
        let mut i = 0usize;
        b.iter(|| {
            cache_bust(&mut poison, &mut poison_idx);
            let k = keys[i % keys.len()];
            i = i.wrapping_add(1);
            let _ = black_box(bt.get(&k));
        })
    });

    // RMI v4 full (lookup only)
    group.bench_function(BenchmarkId::from_parameter("rmi_full_v4"), |b| {
        let mut i = 0usize;
        b.iter(|| {
            cache_bust(&mut poison, &mut poison_idx);
            let k = keys[i % keys.len()];
            i = i.wrapping_add(1);
            let _ = black_box(rmi_v4.predict_get(&k));
        })
    });

    // RMI v5 full (lookup only)
    group.bench_function(BenchmarkId::from_parameter("rmi_full_v5"), |b| {
        let mut i = 0usize;
        b.iter(|| {
            cache_bust(&mut poison, &mut poison_idx);
            let k = keys[i % keys.len()];
            i = i.wrapping_add(1);
            let _ = black_box(rmi_v5.predict_get(&k));
        })
    });

    // Add variants that include a simulated value read after offset lookup
    group.bench_function(BenchmarkId::from_parameter("btree_plus_value"), |b| {
        let mut i = 0usize;
        b.iter(|| {
            cache_bust(&mut poison, &mut poison_idx);
            let k = keys[i % keys.len()];
            i = i.wrapping_add(1);
            if let Some(off) = bt.get(&k) { simulate_value_read(&mut arena, off, val_bytes, val_stride); }
        })
    });

    group.bench_function(BenchmarkId::from_parameter("rmi_full_v4_plus_value"), |b| {
        let mut i = 0usize;
        b.iter(|| {
            cache_bust(&mut poison, &mut poison_idx);
            let k = keys[i % keys.len()];
            i = i.wrapping_add(1);
            if let Some(off) = rmi_v4.predict_get(&k) { simulate_value_read(&mut arena, off, val_bytes, val_stride); }
        })
    });

    group.bench_function(BenchmarkId::from_parameter("rmi_full_v5_plus_value"), |b| {
        let mut i = 0usize;
        b.iter(|| {
            cache_bust(&mut poison, &mut poison_idx);
            let k = keys[i % keys.len()];
            i = i.wrapping_add(1);
            if let Some(off) = rmi_v5.predict_get(&k) { simulate_value_read(&mut arena, off, val_bytes, val_stride); }
        })
    });

    // RMI v4: leaf-find only
    group.bench_function(BenchmarkId::from_parameter("rmi_leaf_find_v4"), |b| {
        let mut i = 0usize;
        b.iter(|| {
            cache_bust(&mut poison, &mut poison_idx);
            let k = keys[i % keys.len()];
            i = i.wrapping_add(1);
            let _ = black_box(rmi_v4.debug_find_leaf_index(k));
        })
    });

    // RMI v4: predict + clamp
    group.bench_function(BenchmarkId::from_parameter("rmi_predict_clamp_v4"), |b| {
        let mut i = 0usize;
        b.iter(|| {
            cache_bust(&mut poison, &mut poison_idx);
            let k = keys[i % keys.len()];
            i = i.wrapping_add(1);
            let _ = black_box(rmi_v4.debug_predict_clamp(k));
        })
    });

    // RMI v4: probe-only within [lo, hi]
    group.bench_function(BenchmarkId::from_parameter("rmi_probe_only_v4"), |b| {
        let mut i = 0usize;
        b.iter(|| {
            cache_bust(&mut poison, &mut poison_idx);
            let (k, lo, hi) = triples_v4[i % triples_v4.len()];
            i = i.wrapping_add(1);
            let _ = black_box(rmi_v4.debug_probe_only(k, lo, hi));
        })
    });

    // RMI v5: leaf-find only
    group.bench_function(BenchmarkId::from_parameter("rmi_leaf_find_v5"), |b| {
        let mut i = 0usize;
        b.iter(|| {
            cache_bust(&mut poison, &mut poison_idx);
            let k = keys[i % keys.len()];
            i = i.wrapping_add(1);
            let _ = black_box(rmi_v5.debug_find_leaf_index(k));
        })
    });

    // RMI v5: predict + clamp
    group.bench_function(BenchmarkId::from_parameter("rmi_predict_clamp_v5"), |b| {
        let mut i = 0usize;
        b.iter(|| {
            cache_bust(&mut poison, &mut poison_idx);
            let k = keys[i % keys.len()];
            i = i.wrapping_add(1);
            let _ = black_box(rmi_v5.debug_predict_clamp(k));
        })
    });

    // RMI v5: probe-only within [lo, hi]
    group.bench_function(BenchmarkId::from_parameter("rmi_probe_only_v5"), |b| {
        let mut i = 0usize;
        b.iter(|| {
            cache_bust(&mut poison, &mut poison_idx);
            let (k, lo, hi) = triples_v5[i % triples_v5.len()];
            i = i.wrapping_add(1);
            let _ = black_box(rmi_v5.debug_probe_only(k, lo, hi));
        })
    });

    group.finish();
}

criterion_group!(benches, bench_raw_indexes);
criterion_main!(benches);
