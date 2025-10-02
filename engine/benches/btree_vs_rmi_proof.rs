use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use kyrodb_engine::index::{BTreeIndex, Index, PrimaryIndex};
use rand::prelude::*;
use std::sync::Arc;

/// FOCUSED TEST: Prove RMI is 3x faster than BTree
/// Tests smaller scales (100K, 1M, 5M) with shorter sample counts
/// Provides clear performance comparison without hours of benchmarking

fn benchmark_btree_vs_rmi_focused(c: &mut Criterion) {
    let mut group = c.benchmark_group("btree_vs_rmi_focused");
    group.sample_size(20); // Faster convergence
    group.measurement_time(std::time::Duration::from_secs(10)); // 10s per test

    // Realistic test sizes: 100K, 1M, 5M
    let sizes = vec![
        100_000,   // 100K - quick validation
        1_000_000, // 1M - realistic production scale
        5_000_000, // 5M - stress test
    ];

    for &size in &sizes {
        println!("\n🔥 Testing {} keys", size);

        // Generate sorted realistic data
        let keys: Vec<u64> = (0..size).map(|i| (i as u64) * 100 + (i as u64 % 1000)).collect();

        // Generate lookup pattern (90% hits, realistic cache behavior)
        let mut rng = StdRng::seed_from_u64(42);
        let lookup_keys: Vec<u64> = (0..100)
            .map(|_| {
                if rng.gen_bool(0.9) {
                    keys[rng.gen_range(0..keys.len())]
                } else {
                    rng.gen_range(0..size as u64 * 200)
                }
            })
            .collect();

        // ============ BTREE BENCHMARK ============
        println!("🌳 Building BTree...");
        let mut btree = BTreeIndex::new();
        for (i, &key) in keys.iter().enumerate() {
            btree.insert(key, i as u64);
        }

        group.bench_with_input(
            BenchmarkId::new("btree", format!("{}K", size / 1000)),
            &size,
            |b, _| {
                let mut idx = 0;
                b.iter(|| {
                    let key = lookup_keys[idx % lookup_keys.len()];
                    idx += 1;
                    black_box(btree.get(black_box(&key)))
                })
            },
        );

        // ============ RMI BENCHMARK ============
        #[cfg(feature = "learned-index")]
        {
            println!("🧠 Building RMI...");
            let key_offset_pairs: Vec<(u64, u64)> = keys
                .iter()
                .enumerate()
                .map(|(i, &key)| (key, i as u64))
                .collect();
            let adaptive_rmi =
                kyrodb_engine::adaptive_rmi::AdaptiveRMI::build_from_pairs_sync(&key_offset_pairs);
            let rmi_index = PrimaryIndex::AdaptiveRmi(Arc::new(adaptive_rmi));

            group.bench_with_input(
                BenchmarkId::new("rmi", format!("{}K", size / 1000)),
                &size,
                |b, _| {
                    let mut idx = 0;
                    b.iter(|| {
                        let key = lookup_keys[idx % lookup_keys.len()];
                        idx += 1;
                        if let PrimaryIndex::AdaptiveRmi(ref adaptive) = rmi_index {
                            black_box(adaptive.lookup(black_box(key)))
                        } else {
                            unreachable!()
                        }
                    })
                },
            );
        }

        println!("✅ Completed {} keys", size);
    }

    group.finish();
}

fn benchmark_single_lookup_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_lookup_comparison");
    group.sample_size(50);

    let size = 1_000_000; // 1M keys for single lookup test
    let keys: Vec<u64> = (0..size).map(|i| i as u64 * 10).collect();
    let target_key = keys[size / 2]; // Middle key

    // BTree single lookup
    println!("🌳 Building BTree for single lookup test...");
    let mut btree = BTreeIndex::new();
    for (i, &key) in keys.iter().enumerate() {
        btree.insert(key, i as u64);
    }

    group.bench_function("btree_1M_single", |b| {
        b.iter(|| black_box(btree.get(&target_key)))
    });

    // RMI single lookup
    #[cfg(feature = "learned-index")]
    {
        println!("🧠 Building RMI for single lookup test...");
        let key_offset_pairs: Vec<(u64, u64)> = keys
            .iter()
            .enumerate()
            .map(|(i, &key)| (key, i as u64))
            .collect();
        let adaptive_rmi =
            kyrodb_engine::adaptive_rmi::AdaptiveRMI::build_from_pairs_sync(&key_offset_pairs);

        group.bench_function("rmi_1M_single", |b| {
            b.iter(|| black_box(adaptive_rmi.lookup(target_key)))
        });
    }

    group.finish();
}

fn benchmark_batch_lookup_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_lookup_comparison");
    group.sample_size(20);

    let size = 1_000_000;
    let batch_size = 1000;

    let keys: Vec<u64> = (0..size).map(|i| i as u64 * 10).collect();

    // Random batch of lookups
    let mut rng = StdRng::seed_from_u64(42);
    let lookup_batch: Vec<u64> = (0..batch_size)
        .map(|_| keys[rng.gen_range(0..keys.len())])
        .collect();

    // BTree batch
    let mut btree = BTreeIndex::new();
    for (i, &key) in keys.iter().enumerate() {
        btree.insert(key, i as u64);
    }

    group.bench_function("btree_batch_1000", |b| {
        b.iter(|| {
            let mut count = 0;
            for &key in &lookup_batch {
                if let Some(_) = btree.get(&key) {
                    count += 1;
                }
            }
            black_box(count)
        })
    });

    // RMI batch
    #[cfg(feature = "learned-index")]
    {
        let key_offset_pairs: Vec<(u64, u64)> = keys
            .iter()
            .enumerate()
            .map(|(i, &key)| (key, i as u64))
            .collect();
        let adaptive_rmi =
            kyrodb_engine::adaptive_rmi::AdaptiveRMI::build_from_pairs_sync(&key_offset_pairs);

        group.bench_function("rmi_batch_1000", |b| {
            b.iter(|| {
                let mut count = 0;
                for &key in &lookup_batch {
                    if let Some(_) = adaptive_rmi.lookup(key) {
                        count += 1;
                    }
                }
                black_box(count)
            })
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    benchmark_btree_vs_rmi_focused,
    benchmark_single_lookup_comparison,
    benchmark_batch_lookup_comparison
);
criterion_main!(benches);
