use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use kyrodb_engine::index::{BTreeIndex, Index, PrimaryIndex};
use rand::prelude::*;
use std::sync::Arc;

/// Pure RMI vs BTree lookup performance at massive scale
/// Tests: 1M, 10M, 50M key datasets with random lookup patterns
/// Zero overhead - just raw index performance

fn benchmark_raw_lookup_scale(c: &mut Criterion) {
    let mut group = c.benchmark_group("raw_index_scale");

    // Test sizes: 1M, 10M, 50M keys
    let sizes = vec![
        1_000_000,  // 1M
        10_000_000, // 10M
        50_000_000, // 50M
    ];

    for &size in &sizes {
        println!("🔥 Preparing dataset: {} keys", size);

        // Generate sorted keys for realistic data distribution
        let mut keys: Vec<u64> = (0..size)
            .map(|i| (i as u64) * 100 + (i as u64 % 1000)) // Add some variance
            .collect();

        // Generate random lookup keys (mix of hits and misses)
        let mut rng = StdRng::seed_from_u64(42);
        let lookup_keys: Vec<u64> = (0..1000)
            .map(|_| {
                if rng.gen_bool(0.8) {
                    // 80% hit rate - pick existing key
                    keys[rng.gen_range(0..keys.len())]
                } else {
                    // 20% miss rate - pick non-existing key
                    rng.gen_range(0..size as u64 * 200)
                }
            })
            .collect();

        // Create BTree index
        println!("🌳 Building BTree index...");
        let mut btree = BTreeIndex::new();
        for (i, &key) in keys.iter().enumerate() {
            btree.insert(key, i as u64);
        }

        // Create RMI index
        #[cfg(feature = "learned-index")]
        {
            println!("🧠 Building RMI index...");
            let key_offset_pairs: Vec<(u64, u64)> = keys
                .iter()
                .enumerate()
                .map(|(i, &key)| (key, i as u64))
                .collect();
            let adaptive_rmi =
                kyrodb_engine::adaptive_rmi::AdaptiveRMI::build_from_pairs(&key_offset_pairs);
            let rmi_index = PrimaryIndex::AdaptiveRmi(Arc::new(adaptive_rmi));

            // Benchmark RMI
            group.bench_with_input(
                BenchmarkId::new("rmi_lookup", format!("{}M", size / 1_000_000)),
                &size,
                |b, _| {
                    let mut lookup_idx = 0;
                    b.iter(|| {
                        let key = lookup_keys[lookup_idx % lookup_keys.len()];
                        lookup_idx += 1;
                        if let PrimaryIndex::AdaptiveRmi(ref adaptive) = rmi_index {
                            black_box(adaptive.lookup(black_box(key)))
                        } else {
                            unreachable!()
                        }
                    })
                },
            );
        }

        // Benchmark BTree
        group.bench_with_input(
            BenchmarkId::new("btree_lookup", format!("{}M", size / 1_000_000)),
            &size,
            |b, _| {
                let mut lookup_idx = 0;
                b.iter(|| {
                    let key = lookup_keys[lookup_idx % lookup_keys.len()];
                    lookup_idx += 1;
                    black_box(btree.get(black_box(&key)))
                })
            },
        );

        println!("✅ Completed benchmarks for {} keys", size);
    }

    group.finish();
}

fn benchmark_sequential_vs_random_patterns(c: &mut Criterion) {
    let mut group = c.benchmark_group("access_patterns_scale");

    let size = 10_000_000; // 10M keys for pattern testing
    println!("🔥 Preparing 10M key dataset for pattern analysis...");

    let keys: Vec<u64> = (0..size).map(|i| i as u64 * 10).collect();

    // Sequential access pattern
    let sequential_keys: Vec<u64> = (0..1000).map(|i| (i * 10000) as u64 * 10).collect();

    // Random access pattern
    let mut rng = StdRng::seed_from_u64(42);
    let random_keys: Vec<u64> = (0..1000)
        .map(|_| keys[rng.gen_range(0..keys.len())])
        .collect();

    // Zipf distribution (hotspot pattern)
    let hotspot_keys: Vec<u64> = {
        let mut hotspot = Vec::new();
        for i in 0..1000 {
            // 80% of accesses to first 20% of keys
            if i < 800 {
                hotspot.push(keys[rng.gen_range(0..keys.len() / 5)]);
            } else {
                hotspot.push(keys[rng.gen_range(0..keys.len())]);
            }
        }
        hotspot
    };

    // Build indexes
    let mut btree = BTreeIndex::new();
    for (i, &key) in keys.iter().enumerate() {
        btree.insert(key, i as u64);
    }

    #[cfg(feature = "learned-index")]
    {
        let key_offset_pairs: Vec<(u64, u64)> = keys
            .iter()
            .enumerate()
            .map(|(i, &key)| (key, i as u64))
            .collect();
        let adaptive_rmi =
            kyrodb_engine::adaptive_rmi::AdaptiveRMI::build_from_pairs(&key_offset_pairs);
        let rmi_index = PrimaryIndex::AdaptiveRmi(Arc::new(adaptive_rmi));

        for (pattern_name, pattern_keys) in [
            ("sequential", &sequential_keys),
            ("random", &random_keys),
            ("hotspot", &hotspot_keys),
        ] {
            // RMI pattern test
            group.bench_with_input(
                BenchmarkId::new("rmi", pattern_name),
                pattern_name,
                |b, _| {
                    let mut idx = 0;
                    b.iter(|| {
                        let key = pattern_keys[idx % pattern_keys.len()];
                        idx += 1;
                        if let PrimaryIndex::AdaptiveRmi(ref adaptive) = rmi_index {
                            black_box(adaptive.lookup(black_box(key)))
                        } else {
                            unreachable!()
                        }
                    })
                },
            );

            // BTree pattern test
            group.bench_with_input(
                BenchmarkId::new("btree", pattern_name),
                pattern_name,
                |b, _| {
                    let mut idx = 0;
                    b.iter(|| {
                        let key = pattern_keys[idx % pattern_keys.len()];
                        idx += 1;
                        black_box(btree.get(black_box(&key)))
                    })
                },
            );
        }
    }

    #[cfg(not(feature = "learned-index"))]
    {
        for (pattern_name, pattern_keys) in [
            ("sequential", &sequential_keys),
            ("random", &random_keys),
            ("hotspot", &hotspot_keys),
        ] {
            // BTree pattern test only
            group.bench_with_input(
                BenchmarkId::new("btree", pattern_name),
                pattern_name,
                |b, _| {
                    let mut idx = 0;
                    b.iter(|| {
                        let key = pattern_keys[idx % pattern_keys.len()];
                        idx += 1;
                        black_box(btree.get(black_box(&key)))
                    })
                },
            );
        }
    }

    group.finish();
}

fn benchmark_scaling_behavior(c: &mut Criterion) {
    let mut group = c.benchmark_group("scaling_behavior");

    // Test multiple sizes to see scaling behavior
    let sizes = vec![100_000, 500_000, 1_000_000, 5_000_000, 10_000_000];

    for &size in &sizes {
        let keys: Vec<u64> = (0..size).map(|i| i as u64 * 7).collect(); // Prime multiplier for distribution
        let lookup_key = keys[size / 2]; // Middle key

        // BTree scaling
        let mut btree = BTreeIndex::new();
        for (i, &key) in keys.iter().enumerate() {
            btree.insert(key, i as u64);
        }

        group.bench_with_input(BenchmarkId::new("btree_scale", size), &size, |b, _| {
            b.iter(|| black_box(btree.get(black_box(&lookup_key))))
        });

        #[cfg(feature = "learned-index")]
        {
            // RMI scaling
            let key_offset_pairs: Vec<(u64, u64)> = keys
                .iter()
                .enumerate()
                .map(|(i, &key)| (key, i as u64))
                .collect();
            let adaptive_rmi =
                kyrodb_engine::adaptive_rmi::AdaptiveRMI::build_from_pairs(&key_offset_pairs);

            group.bench_with_input(BenchmarkId::new("rmi_scale", size), &size, |b, _| {
                b.iter(|| black_box(adaptive_rmi.lookup(black_box(lookup_key))))
            });
        }
    }

    group.finish();
}

criterion_group!(
    benches,
    benchmark_raw_lookup_scale,
    benchmark_sequential_vs_random_patterns,
    benchmark_scaling_behavior
);
criterion_main!(benches);
