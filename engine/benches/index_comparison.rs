use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use std::collections::BTreeMap;
use rand::prelude::*;
use rand::seq::SliceRandom;

#[cfg(feature = "learned-index")]
use kyrodb_engine::adaptive_rmi::AdaptiveRMI;

// Direct index performance benchmark - no HTTP, no WAL, just pure index operations
fn bench_index_lookups(c: &mut Criterion) {
    let sizes = vec![1000, 10000, 100000];
    
    for size in sizes {
        let mut group = c.benchmark_group(format!("index_lookup_{}", size));
        
        // Generate sorted test data (realistic scenario)
        let mut keys: Vec<u64> = (0..size).map(|i| i as u64 * 10).collect();
        let values: Vec<u64> = (0..size).map(|i| i as u64 * 100).collect();
        
        // Prepare random lookup keys
        let mut rng = thread_rng();
        let lookup_keys: Vec<u64> = (0..1000)
            .map(|_| keys[rng.gen_range(0..keys.len())])
            .collect();
        
        // BTree benchmark
        {
            let mut btree = BTreeMap::new();
            for (i, &key) in keys.iter().enumerate() {
                btree.insert(key, values[i]);
            }
            
            group.bench_with_input(
                BenchmarkId::new("btree", size),
                &lookup_keys,
                |b, lookup_keys| {
                    b.iter(|| {
                        for &key in lookup_keys.iter().take(100) {
                            black_box(btree.get(&key));
                        }
                    })
                },
            );
        }
        
        // RMI benchmark (if feature is enabled)
        #[cfg(feature = "learned-index")]
        {
            let pairs: Vec<(u64, u64)> = keys.iter().zip(values.iter()).map(|(&k, &v)| (k, v)).collect();
            let rmi = AdaptiveRMI::build_from_pairs(&pairs);
            
            group.bench_with_input(
                BenchmarkId::new("rmi", size),
                &lookup_keys,
                |b, lookup_keys| {
                    b.iter(|| {
                        for &key in lookup_keys.iter().take(100) {
                            black_box(rmi.lookup(key));
                        }
                    })
                },
            );
        }
        
        group.finish();
    }
}

fn bench_index_inserts(c: &mut Criterion) {
    let sizes = vec![1000, 10000];
    
    for size in sizes {
        let mut group = c.benchmark_group(format!("index_insert_{}", size));
        
        // Generate test data
        let keys: Vec<u64> = (0..size).map(|i| i as u64 * 10).collect();
        let values: Vec<u64> = (0..size).map(|i| i as u64 * 100).collect();
        
        // BTree insert benchmark
        group.bench_with_input(
            BenchmarkId::new("btree", size),
            &(keys.clone(), values.clone()),
            |b, (keys, values)| {
                b.iter(|| {
                    let mut btree = BTreeMap::new();
                    for (i, &key) in keys.iter().enumerate() {
                        black_box(btree.insert(key, values[i]));
                    }
                    black_box(btree)
                })
            },
        );
        
        // RMI insert benchmark (if feature is enabled)
        #[cfg(feature = "learned-index")]
        {
            group.bench_with_input(
                BenchmarkId::new("rmi", size),
                &(keys.clone(), values.clone()),
                |b, (keys, values)| {
                    b.iter(|| {
                        let rmi = AdaptiveRMI::new();
                        for (i, &key) in keys.iter().enumerate() {
                            black_box(rmi.insert(key, values[i]));
                        }
                        black_box(rmi)
                    })
                },
            );
        }
        
        group.finish();
    }
}

fn bench_lookup_patterns(c: &mut Criterion) {
    let size = 100000;
    
    // Generate test data
    let keys: Vec<u64> = (0..size).map(|i| i as u64 * 10).collect();
    let values: Vec<u64> = (0..size).map(|i| i as u64 * 100).collect();
    
    // Setup BTree
    let mut btree = BTreeMap::new();
    for (i, &key) in keys.iter().enumerate() {
        btree.insert(key, values[i]);
    }
    
    // Setup RMI
    #[cfg(feature = "learned-index")]
    let rmi = {
        let pairs: Vec<(u64, u64)> = keys.iter().zip(values.iter()).map(|(&k, &v)| (k, v)).collect();
        AdaptiveRMI::build_from_pairs(&pairs)
    };
    
    // Test different lookup patterns
    let patterns = vec![
        ("sequential", (0..1000).map(|i| i as u64 * 10).collect::<Vec<_>>()),
        ("random", {
            let mut rng = thread_rng();
            let mut random_keys: Vec<u64> = keys.choose_multiple(&mut rng, 1000).cloned().collect();
            random_keys.shuffle(&mut rng);
            random_keys
        }),
        ("hotspot", vec![5000u64; 1000]), // Same key repeated
    ];
    
    for (pattern_name, lookup_keys) in patterns {
        let mut group = c.benchmark_group(format!("lookup_pattern_{}", pattern_name));
        
        // BTree pattern benchmark
        group.bench_function("btree", |b| {
            b.iter(|| {
                for &key in &lookup_keys {
                    black_box(btree.get(&key));
                }
            })
        });
        
        // RMI pattern benchmark (if feature is enabled)
        #[cfg(feature = "learned-index")]
        {
            group.bench_function("rmi", |b| {
                b.iter(|| {
                    for &key in &lookup_keys {
                        black_box(rmi.lookup(key));
                    }
                })
            });
        }
        
        group.finish();
    }
}

fn bench_sequential_vs_random_access(c: &mut Criterion) {
    let size = 50000;
    
    // Generate test data
    let keys: Vec<u64> = (0..size).map(|i| i as u64 * 10).collect();
    let values: Vec<u64> = (0..size).map(|i| i as u64 * 100).collect();
    
    // Setup BTree
    let mut btree = BTreeMap::new();
    for (i, &key) in keys.iter().enumerate() {
        btree.insert(key, values[i]);
    }
    
    // Setup RMI
    #[cfg(feature = "learned-index")]
    let rmi = {
        let pairs: Vec<(u64, u64)> = keys.iter().zip(values.iter()).map(|(&k, &v)| (k, v)).collect();
        AdaptiveRMI::build_from_pairs(&pairs)
    };
    
    // Sequential access (best case for RMI)
    let sequential_keys: Vec<u64> = keys.iter().step_by(50).take(1000).cloned().collect();
    
    // Random access (worst case for RMI)
    let mut rng = thread_rng();
    let mut random_keys: Vec<u64> = keys.choose_multiple(&mut rng, 1000).cloned().collect();
    random_keys.shuffle(&mut rng);
    
    let mut group = c.benchmark_group("access_patterns");
    
    // Sequential access benchmarks
    group.bench_function("btree_sequential", |b| {
        b.iter(|| {
            for &key in &sequential_keys {
                black_box(btree.get(&key));
            }
        })
    });
    
    group.bench_function("btree_random", |b| {
        b.iter(|| {
            for &key in &random_keys {
                black_box(btree.get(&key));
            }
        })
    });
    
    #[cfg(feature = "learned-index")]
    {
        group.bench_function("rmi_sequential", |b| {
            b.iter(|| {
                for &key in &sequential_keys {
                    black_box(rmi.lookup(key));
                }
            })
        });
        
        group.bench_function("rmi_random", |b| {
            b.iter(|| {
                for &key in &random_keys {
                    black_box(rmi.lookup(key));
                }
            })
        });
    }
    
    group.finish();
}

// MASSIVE SCALE BENCHMARKS - 1M, 10M, 50M datasets
fn bench_massive_scale_lookups(c: &mut Criterion) {
    let sizes = vec![
        1_000_000,   // 1M
        10_000_000,  // 10M
        50_000_000,  // 50M
    ];
    
    for size in sizes {
        println!("🔥 Testing {} key dataset", size);
        let mut group = c.benchmark_group(format!("massive_scale_{}", size / 1_000_000));
        
        // Generate realistic data distribution
        let keys: Vec<u64> = (0..size).map(|i| i as u64 * 7 + (i as u64 % 1000)).collect();
        let values: Vec<u64> = (0..size).map(|i| i as u64).collect();
        
        // Generate lookup pattern (80% hits, 20% misses)
        let mut rng = thread_rng();
        let lookup_keys: Vec<u64> = (0..1000).map(|_| {
            if rng.gen_bool(0.8) {
                keys[rng.gen_range(0..keys.len())]
            } else {
                rng.gen_range(0..size as u64 * 10) // Potential miss
            }
        }).collect();
        
        // BTree benchmark
        {
            println!("🌳 Building BTree for {} keys...", size);
            let mut btree = BTreeMap::new();
            for (i, &key) in keys.iter().enumerate() {
                btree.insert(key, values[i]);
            }
            
            group.bench_with_input(
                BenchmarkId::new("btree", format!("{}M", size / 1_000_000)),
                &lookup_keys,
                |b, lookup_keys| {
                    b.iter(|| {
                        let mut total = 0u64;
                        for &key in lookup_keys {
                            if let Some(&value) = btree.get(&key) {
                                total = total.wrapping_add(value);
                            }
                        }
                        black_box(total)
                    })
                },
            );
        }
        
        // RMI benchmark
        #[cfg(feature = "learned-index")]
        {
            println!("🧠 Building RMI for {} keys...", size);
            let pairs: Vec<(u64, u64)> = keys.iter().zip(values.iter()).map(|(&k, &v)| (k, v)).collect();
            let rmi = AdaptiveRMI::build_from_pairs(&pairs);
            
            group.bench_with_input(
                BenchmarkId::new("rmi", format!("{}M", size / 1_000_000)),
                &lookup_keys,
                |b, lookup_keys| {
                    b.iter(|| {
                        let mut total = 0u64;
                        for &key in lookup_keys {
                            if let Some(value) = rmi.lookup(key) {
                                total = total.wrapping_add(value);
                            }
                        }
                        black_box(total)
                    })
                },
            );
        }
        
        group.finish();
        println!("✅ Completed {} key benchmark", size);
    }
}

// Pure single-lookup latency test
fn bench_single_lookup_latency(c: &mut Criterion) {
    let sizes = vec![1_000_000, 10_000_000, 50_000_000];
    
    for size in sizes {
        let mut group = c.benchmark_group(format!("single_lookup_{}", size / 1_000_000));
        
        let keys: Vec<u64> = (0..size).map(|i| i as u64 * 10).collect();
        let values: Vec<u64> = (0..size).map(|i| i as u64).collect();
        let target_key = keys[size / 2]; // Middle key for consistent access
        
        // BTree single lookup
        {
            let mut btree = BTreeMap::new();
            for (i, &key) in keys.iter().enumerate() {
                btree.insert(key, values[i]);
            }
            
            group.bench_function(
                &format!("btree_{}M", size / 1_000_000),
                |b| {
                    b.iter(|| {
                        black_box(btree.get(&target_key))
                    })
                },
            );
        }
        
        // RMI single lookup
        #[cfg(feature = "learned-index")]
        {
            let pairs: Vec<(u64, u64)> = keys.iter().zip(values.iter()).map(|(&k, &v)| (k, v)).collect();
            let rmi = AdaptiveRMI::build_from_pairs(&pairs);
            
            group.bench_function(
                &format!("rmi_{}M", size / 1_000_000),
                |b| {
                    b.iter(|| {
                        black_box(rmi.lookup(target_key))
                    })
                },
            );
        }
        
        group.finish();
    }
}

criterion_group!(
    benches,
    bench_index_lookups,
    bench_index_inserts,
    bench_lookup_patterns,
    bench_sequential_vs_random_access,
    bench_massive_scale_lookups,
    bench_single_lookup_latency
);
criterion_main!(benches);
