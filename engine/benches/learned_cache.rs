//! Benchmarks for Hybrid Semantic Cache Predictor
//!
//!
//! Targets:
//! - Hotness prediction: <100ns P99
//! - Training: <100ms for 10M doc_ids
//! - Memory: <100MB for 10M doc_ids

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use kyrodb_engine::learned_cache::{AccessEvent, AccessType, LearnedCachePredictor};
use std::time::{Duration, SystemTime};

/// Generate random access events for benchmarking
fn generate_access_events(
    count: usize,
    unique_docs: usize,
    time_window_secs: u64,
) -> Vec<AccessEvent> {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let now = SystemTime::now();

    (0..count)
        .map(|_| {
            let doc_id = rng.gen_range(0..unique_docs) as u64;
            let seconds_ago = rng.gen_range(0..time_window_secs);
            AccessEvent {
                doc_id,
                timestamp: now - Duration::from_secs(seconds_ago),
                access_type: AccessType::Read,
            }
        })
        .collect()
}

/// Generate Zipf-distributed access events (realistic workload)
fn generate_zipf_accesses(
    count: usize,
    unique_docs: usize,
    time_window_secs: u64,
) -> Vec<AccessEvent> {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let now = SystemTime::now();

    // Zipf distribution: 20% of docs get 80% of accesses
    let hot_docs = unique_docs / 5; // 20% hot docs
    let hot_probability = 0.8; // 80% of accesses go to hot docs

    (0..count)
        .map(|_| {
            let doc_id = if rng.gen::<f64>() < hot_probability {
                // Access hot doc (0..hot_docs)
                rng.gen_range(0..hot_docs) as u64
            } else {
                // Access cold doc (hot_docs..unique_docs)
                rng.gen_range(hot_docs..unique_docs) as u64
            };

            let seconds_ago = rng.gen_range(0..time_window_secs);
            AccessEvent {
                doc_id,
                timestamp: now - Duration::from_secs(seconds_ago),
                access_type: AccessType::Read,
            }
        })
        .collect()
}

/// Benchmark: Training Hybrid Semantic Cache predictor
fn bench_learned_cache_training(c: &mut Criterion) {
    let mut group = c.benchmark_group("learned_cache_training");

    for unique_docs in [1_000, 10_000, 100_000] {
        group.throughput(Throughput::Elements(unique_docs as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}docs", unique_docs)),
            &unique_docs,
            |b, &unique_docs| {
                // Generate access events (10x accesses vs unique docs)
                let accesses = generate_access_events(unique_docs * 10, unique_docs, 3600);
                let mut predictor = LearnedCachePredictor::new(unique_docs).unwrap();

                b.iter(|| {
                    predictor.train_from_accesses(&accesses).unwrap();
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: Hotness prediction latency
fn bench_learned_cache_prediction(c: &mut Criterion) {
    let mut group = c.benchmark_group("learned_cache_prediction");

    for unique_docs in [1_000, 10_000, 100_000] {
        group.throughput(Throughput::Elements(1));

        // Train predictor once
        let accesses = generate_access_events(unique_docs * 10, unique_docs, 3600);
        let mut predictor = LearnedCachePredictor::new(unique_docs).unwrap();
        predictor.train_from_accesses(&accesses).unwrap();

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}docs", unique_docs)),
            &unique_docs,
            |b, _| {
                b.iter(|| {
                    let doc_id = black_box(42);
                    black_box(predictor.predict_hotness(doc_id))
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: Cache admission decision (predict_hotness + should_cache)
fn bench_cache_admission(c: &mut Criterion) {
    let mut group = c.benchmark_group("cache_admission");

    let unique_docs = 100_000;
    let accesses = generate_zipf_accesses(unique_docs * 10, unique_docs, 3600);
    let mut predictor = LearnedCachePredictor::new(unique_docs).unwrap();
    predictor.train_from_accesses(&accesses).unwrap();

    group.bench_function("should_cache", |b| {
        b.iter(|| {
            let doc_id = black_box(42);
            black_box(predictor.should_cache(doc_id))
        });
    });

    group.finish();
}

/// Benchmark: Hit rate on Zipf workload
fn bench_learned_cache_hit_rate(c: &mut Criterion) {
    let mut group = c.benchmark_group("learned_cache_hit_rate");

    let unique_docs = 100_000;
    let training_accesses = generate_zipf_accesses(unique_docs * 10, unique_docs, 3600);
    let mut predictor = LearnedCachePredictor::new(unique_docs).unwrap();
    predictor.train_from_accesses(&training_accesses).unwrap();

    // Measure hit rate on test workload
    let test_accesses = generate_zipf_accesses(10_000, unique_docs, 3600);

    group.bench_function("zipf_hit_rate", |b| {
        b.iter(|| {
            let mut hits = 0;
            let mut total = 0;

            for access in &test_accesses {
                total += 1;
                if predictor.should_cache(access.doc_id) {
                    hits += 1;
                }
            }

            black_box((hits, total))
        });
    });

    group.finish();
}

/// Benchmark: Training with different cache thresholds
fn bench_cache_threshold_impact(c: &mut Criterion) {
    let mut group = c.benchmark_group("cache_threshold_impact");

    let unique_docs = 100_000;
    let accesses = generate_zipf_accesses(unique_docs * 10, unique_docs, 3600);

    for threshold in [0.5, 0.7, 0.9] {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("threshold_{}", threshold)),
            &threshold,
            |b, &threshold| {
                let mut predictor = LearnedCachePredictor::new(unique_docs).unwrap();
                predictor.train_from_accesses(&accesses).unwrap();
                predictor.set_cache_threshold(threshold);

                b.iter(|| {
                    let doc_id = black_box(42);
                    black_box(predictor.should_cache(doc_id))
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: Memory usage tracking
fn bench_memory_usage(c: &mut Criterion) {
    let mut group = c.benchmark_group("learned_cache_memory");

    for unique_docs in [10_000, 100_000, 1_000_000] {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}docs", unique_docs)),
            &unique_docs,
            |b, &unique_docs| {
                let accesses = generate_access_events(unique_docs * 10, unique_docs, 3600);

                b.iter(|| {
                    let mut predictor = LearnedCachePredictor::new(unique_docs).unwrap();
                    predictor.train_from_accesses(&accesses).unwrap();
                    black_box(predictor.stats())
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_learned_cache_training,
    bench_learned_cache_prediction,
    bench_cache_admission,
    bench_learned_cache_hit_rate,
    bench_cache_threshold_impact,
    bench_memory_usage,
);

criterion_main!(benches);
