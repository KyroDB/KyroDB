//! Benchmarks for Access Pattern Logger
//!
//! Phase 0 Week 5-8: Measure logging overhead and flush performance
//!
//! Targets:
//! - Log overhead: <10ns per access
//! - Flush time: <1ms for 10k events
//! - Memory: 240MB for 10M events (24 bytes/event)

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use kyrodb_engine::access_logger::{hash_embedding, AccessPatternLogger};
use kyrodb_engine::learned_cache::{AccessEvent, AccessType};
use std::time::{Duration, SystemTime};

/// Generate random embeddings for benchmarking
fn generate_random_embedding(dim: usize) -> Vec<f32> {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    (0..dim).map(|_| rng.gen::<f32>()).collect()
}

/// Benchmark: Log access overhead
fn bench_log_access_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("log_access_overhead");
    group.throughput(Throughput::Elements(1));

    let mut logger = AccessPatternLogger::new(10_000_000);
    let embedding = generate_random_embedding(128);

    group.bench_function("single_log", |b| {
        let mut doc_id = 0u64;
        b.iter(|| {
            logger.log_access(black_box(doc_id), black_box(&embedding));
            doc_id = doc_id.wrapping_add(1);
        });
    });

    group.finish();
}

/// Benchmark: Batch logging throughput
fn bench_batch_logging(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_logging");

    for batch_size in [100, 1_000, 10_000] {
        group.throughput(Throughput::Elements(batch_size as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_accesses", batch_size)),
            &batch_size,
            |b, &batch_size| {
                let mut logger = AccessPatternLogger::new(10_000_000);
                let embedding = generate_random_embedding(128);

                b.iter(|| {
                    for i in 0..batch_size {
                        logger.log_access(i as u64, &embedding);
                    }
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: Get recent window (training data extraction)
fn bench_get_recent_window(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_recent_window");

    for event_count in [1_000, 10_000, 100_000] {
        group.throughput(Throughput::Elements(event_count as u64));

        // Pre-populate logger
        let mut logger = AccessPatternLogger::new(event_count * 2);
        let embedding = generate_random_embedding(128);
        for i in 0..event_count {
            logger.log_access(i as u64, &embedding);
        }

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}events", event_count)),
            &event_count,
            |b, _| {
                b.iter(|| {
                    let window = logger.get_recent_window(Duration::from_secs(3600 * 24));
                    black_box(window)
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: Ring buffer wraparound performance
fn bench_ring_buffer_wraparound(c: &mut Criterion) {
    let mut group = c.benchmark_group("ring_buffer_wraparound");

    let capacity = 10_000;
    let mut logger = AccessPatternLogger::new(capacity);
    let embedding = generate_random_embedding(128);

    // Fill buffer to capacity
    for i in 0..capacity {
        logger.log_access(i as u64, &embedding);
    }

    group.throughput(Throughput::Elements(1));
    group.bench_function("wraparound_write", |b| {
        let mut doc_id = capacity as u64;
        b.iter(|| {
            logger.log_access(black_box(doc_id), black_box(&embedding));
            doc_id = doc_id.wrapping_add(1);
        });
    });

    group.finish();
}

/// Benchmark: Hash embedding performance
fn bench_hash_embedding(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash_embedding");

    for dim in [128, 384, 768, 1536] {
        group.throughput(Throughput::Elements(dim as u64));

        let embedding = generate_random_embedding(dim);

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}dim", dim)),
            &dim,
            |b, _| {
                b.iter(|| black_box(hash_embedding(black_box(&embedding))));
            },
        );
    }

    group.finish();
}

/// Benchmark: Logger stats retrieval
fn bench_logger_stats(c: &mut Criterion) {
    let mut logger = AccessPatternLogger::new(100_000);
    let embedding = generate_random_embedding(128);

    // Populate with some data
    for i in 0..10_000 {
        logger.log_access(i, &embedding);
    }

    c.bench_function("logger_stats", |b| {
        b.iter(|| black_box(logger.stats()));
    });
}

/// Benchmark: Log event (pre-constructed) vs log access (construct + log)
fn bench_log_event_vs_log_access(c: &mut Criterion) {
    let mut group = c.benchmark_group("log_event_comparison");

    let mut logger = AccessPatternLogger::new(100_000);
    let embedding = generate_random_embedding(128);

    // Pre-construct event
    let event = AccessEvent {
        doc_id: 42,
        timestamp: SystemTime::now(),
        access_type: AccessType::Read,
    };

    group.bench_function("log_event_preconstructed", |b| {
        b.iter(|| {
            logger.log_event(black_box(event));
        });
    });

    group.bench_function("log_access_construct_inline", |b| {
        b.iter(|| {
            logger.log_access(black_box(42), black_box(&embedding));
        });
    });

    group.finish();
}

/// Benchmark: Memory usage scaling
fn bench_memory_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_scaling");

    for capacity in [10_000, 100_000, 1_000_000] {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}capacity", capacity)),
            &capacity,
            |b, &capacity| {
                b.iter(|| {
                    let logger = AccessPatternLogger::new(capacity);
                    black_box(logger)
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: Concurrent-like access pattern (single thread, high frequency)
fn bench_high_frequency_logging(c: &mut Criterion) {
    let mut group = c.benchmark_group("high_frequency_logging");
    group.throughput(Throughput::Elements(100_000));

    let mut logger = AccessPatternLogger::new(10_000_000);
    let embedding = generate_random_embedding(128);

    group.bench_function("100k_rapid_fire", |b| {
        b.iter(|| {
            for i in 0..100_000 {
                logger.log_access(i % 1000, &embedding);
            }
        });
    });

    group.finish();
}

/// Benchmark: Flush detection overhead
fn bench_needs_flush_check(c: &mut Criterion) {
    let logger = AccessPatternLogger::with_flush_interval(100_000, Duration::from_secs(600));

    c.bench_function("needs_flush_check", |b| {
        b.iter(|| black_box(logger.needs_flush()));
    });
}

criterion_group!(
    benches,
    bench_log_access_overhead,
    bench_batch_logging,
    bench_get_recent_window,
    bench_ring_buffer_wraparound,
    bench_hash_embedding,
    bench_logger_stats,
    bench_log_event_vs_log_access,
    bench_memory_scaling,
    bench_high_frequency_logging,
    bench_needs_flush_check,
);

criterion_main!(benches);
