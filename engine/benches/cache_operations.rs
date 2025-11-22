//! Validates cache performance targets:
//! - Cache lookup: < 50ns (hash table O(1))
//! - Cache insertion: < 100ns (with LRU update)
//! - Cache eviction: < 200ns (VecDeque removal)
//! - Learned prediction: < 50ns (RMI inference)

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use kyrodb_engine::{
    AccessEvent, AccessType, CacheStrategy, CachedVector, LearnedCachePredictor,
    LearnedCacheStrategy, LruCacheStrategy, VectorCache,
};
use std::time::{Instant, SystemTime};

/// Generate random embedding for testing
fn generate_embedding(dim: usize, seed: u64) -> Vec<f32> {
    (0..dim)
        .map(|i| ((seed * 31 + i as u64) % 1000) as f32 / 1000.0)
        .collect()
}

/// Benchmark: VectorCache.get() - target < 50ns
fn bench_cache_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("cache_lookup");
    group.throughput(Throughput::Elements(1));

    for size in [1_000, 10_000, 100_000] {
        let cache = VectorCache::new(size);

        // Pre-populate cache
        for i in 0..size.min(1000) {
            cache.insert(CachedVector {
                doc_id: i as u64,
                embedding: generate_embedding(768, i as u64),
                distance: 0.5,
                cached_at: Instant::now(),
            });
        }

        group.bench_with_input(BenchmarkId::new("hit", size), &cache, |b, cache| {
            b.iter(|| black_box(cache.get(black_box(42))));
        });

        group.bench_with_input(BenchmarkId::new("miss", size), &cache, |b, cache| {
            b.iter(|| black_box(cache.get(black_box(999_999))));
        });
    }

    group.finish();
}

/// Benchmark: VectorCache.insert() - target < 100ns
fn bench_cache_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("cache_insert");
    group.throughput(Throughput::Elements(1));

    for capacity in [1_000, 10_000] {
        let cache = VectorCache::new(capacity);
        let embedding = generate_embedding(768, 42);

        group.bench_with_input(
            BenchmarkId::new("cold_insert", capacity),
            &cache,
            |b, cache| {
                let mut doc_id = 0u64;
                b.iter(|| {
                    let cached = CachedVector {
                        doc_id,
                        embedding: embedding.clone(),
                        distance: 0.5,
                        cached_at: Instant::now(),
                    };
                    cache.insert(black_box(cached));
                    doc_id = doc_id.wrapping_add(1);
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: LRU eviction overhead - target < 200ns
fn bench_lru_eviction(c: &mut Criterion) {
    let mut group = c.benchmark_group("lru_eviction");
    group.throughput(Throughput::Elements(1));

    let strategy = LruCacheStrategy::new(1000);
    let embedding = generate_embedding(768, 42);

    // Fill cache to capacity
    for i in 0..1000 {
        strategy.insert_cached(CachedVector {
            doc_id: i,
            embedding: embedding.clone(),
            distance: 0.5,
            cached_at: Instant::now(),
        });
    }

    group.bench_function("evict_and_insert", |b| {
        let mut doc_id = 1000u64;
        b.iter(|| {
            let cached = CachedVector {
                doc_id,
                embedding: embedding.clone(),
                distance: 0.5,
                cached_at: Instant::now(),
            };
            strategy.insert_cached(black_box(cached));
            doc_id = doc_id.wrapping_add(1);
        });
    });

    group.finish();
}

/// Benchmark: Hybrid Semantic Cache prediction - target < 50ns
fn bench_learned_cache_prediction(c: &mut Criterion) {
    let mut group = c.benchmark_group("learned_cache_prediction");
    group.throughput(Throughput::Elements(1));

    // Create and train predictor with Zipf distribution
    let mut predictor = LearnedCachePredictor::new(10_000).unwrap();

    let mut accesses = Vec::new();
    for doc_id in 0..1000 {
        // Hot documents (0-199): 40 accesses each
        // Cold documents (200-999): 2 accesses each
        let frequency = if doc_id < 200 { 40 } else { 2 };
        for _ in 0..frequency {
            accesses.push(AccessEvent {
                doc_id,
                timestamp: SystemTime::now(),
                access_type: AccessType::Read,
            });
        }
    }

    predictor.train_from_accesses(&accesses).unwrap();

    let strategy = LearnedCacheStrategy::new(10_000, predictor);
    let dummy_embedding = vec![0.5; 768];

    group.bench_function("should_cache_hot", |b| {
        b.iter(|| black_box(strategy.should_cache(black_box(42), black_box(&dummy_embedding))));
    });

    group.bench_function("should_cache_cold", |b| {
        b.iter(|| black_box(strategy.should_cache(black_box(999), black_box(&dummy_embedding))));
    });

    group.finish();
}

/// Benchmark: Cache hit rate calculation overhead
fn bench_cache_stats(c: &mut Criterion) {
    let mut group = c.benchmark_group("cache_stats");

    let cache = VectorCache::new(10_000);

    // Simulate some operations
    for i in 0..1000 {
        cache.insert(CachedVector {
            doc_id: i,
            embedding: generate_embedding(768, i),
            distance: 0.5,
            cached_at: Instant::now(),
        });
        cache.get(i / 2); // Some hits, some misses
    }

    group.bench_function("stats_calculation", |b| {
        b.iter(|| black_box(cache.stats()));
    });

    group.finish();
}

/// Benchmark: AbTestSplitter routing overhead
fn bench_ab_test_routing(c: &mut Criterion) {
    use kyrodb_engine::AbTestSplitter;

    let mut group = c.benchmark_group("ab_test_routing");
    group.throughput(Throughput::Elements(1));

    let lru_strategy = std::sync::Arc::new(LruCacheStrategy::new(10_000));
    let predictor = LearnedCachePredictor::new(10_000).unwrap();
    let learned_strategy = std::sync::Arc::new(LearnedCacheStrategy::new(10_000, predictor));

    let splitter = AbTestSplitter::new(lru_strategy, learned_strategy);

    group.bench_function("get_strategy", |b| {
        let mut doc_id = 0u64;
        b.iter(|| {
            black_box(splitter.get_strategy(black_box(doc_id)));
            doc_id = doc_id.wrapping_add(1);
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_cache_lookup,
    bench_cache_insert,
    bench_lru_eviction,
    bench_learned_cache_prediction,
    bench_cache_stats,
    bench_ab_test_routing,
);

criterion_main!(benches);
