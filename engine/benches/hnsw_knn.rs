//! HNSW k-NN search benchmarks
//!
//!
//! Run with: cargo bench --bench hnsw_knn

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use kyrodb_engine::hnsw_index::HnswVectorIndex;
use rand::Rng;

/// Generate random normalized vectors for benchmarking
fn generate_random_vectors(count: usize, dimension: usize) -> Vec<Vec<f32>> {
    let mut rng = rand::thread_rng();
    (0..count)
        .map(|_| {
            let mut vec: Vec<f32> = (0..dimension).map(|_| rng.gen_range(-1.0..1.0)).collect();
            // Normalize for cosine similarity
            let norm: f32 = vec.iter().map(|x| x * x).sum::<f32>().sqrt();
            vec.iter_mut().for_each(|x| *x /= norm);
            vec
        })
        .collect()
}

/// Benchmark HNSW insertion throughput
fn bench_hnsw_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("hnsw_insert");

    for size in [1_000, 10_000, 100_000] {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            let vectors = generate_random_vectors(size, 128);

            b.iter(|| {
                let mut index = HnswVectorIndex::new(128, size).unwrap();
                for (id, vec) in vectors.iter().enumerate() {
                    index.add_vector(id as u64, vec).unwrap();
                }
                black_box(index);
            });
        });
    }

    group.finish();
}

/// Benchmark HNSW k-NN search latency (P50, P99 targets)
fn bench_hnsw_search(c: &mut Criterion) {
    let mut group = c.benchmark_group("hnsw_search");

    // Target: P99 < 1ms on 10M vectors (warm index)
    for (size, k) in [(1_000, 10), (10_000, 10), (100_000, 10)] {
        // Build index
        let vectors = generate_random_vectors(size, 128);
        let mut index = HnswVectorIndex::new(128, size).unwrap();
        for (id, vec) in vectors.iter().enumerate() {
            index.add_vector(id as u64, vec).unwrap();
        }

        // Warm up index (important for accurate P99 measurements)
        let query = &vectors[0];
        for _ in 0..100 {
            let _ = index.knn_search(query, k);
        }

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}vectors_k{}", size, k)),
            &(&index, query, k),
            |b, (idx, q, k)| {
                b.iter(|| {
                    let results = idx.knn_search(q, *k).unwrap();
                    black_box(results);
                });
            },
        );
    }

    group.finish();
}

/// Benchmark HNSW recall vs brute force
fn bench_hnsw_recall(c: &mut Criterion) {
    let mut group = c.benchmark_group("hnsw_recall");

    // Target: >95% recall@10
    let size = 10_000;
    let k = 10;
    let vectors = generate_random_vectors(size, 128);

    let mut index = HnswVectorIndex::new(128, size).unwrap();
    for (id, vec) in vectors.iter().enumerate() {
        index.add_vector(id as u64, vec).unwrap();
    }

    let query = &vectors[0];

    group.bench_function("hnsw_vs_brute_force", |b| {
        b.iter(|| {
            // HNSW search
            let hnsw_results = index.knn_search(query, k).unwrap();

            // Brute force (for comparison)
            let mut brute_force: Vec<(usize, f32)> = vectors
                .iter()
                .enumerate()
                .map(|(id, vec)| {
                    let dist = cosine_distance(query, vec);
                    (id, dist)
                })
                .collect();
            brute_force.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
            let brute_force_top_k: Vec<usize> =
                brute_force.iter().take(k).map(|(id, _)| *id).collect();

            // Calculate recall
            let hnsw_ids: Vec<u64> = hnsw_results.iter().map(|r| r.doc_id).collect();
            let recall = hnsw_ids
                .iter()
                .filter(|id| brute_force_top_k.contains(&(**id as usize)))
                .count() as f32
                / k as f32;

            black_box((hnsw_results, brute_force_top_k, recall));
        });
    });

    group.finish();
}

/// Cosine distance (1 - cosine similarity)
fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    1.0 - (dot / (norm_a * norm_b))
}

criterion_group!(
    benches,
    bench_hnsw_insert,
    bench_hnsw_search,
    bench_hnsw_recall
);
criterion_main!(benches);
