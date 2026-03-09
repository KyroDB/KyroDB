//! HNSW hot-path benchmarks.
//!
//! Run with: `cargo bench -p kyrodb-engine --bench hnsw_knn`

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use kyrodb_engine::hnsw_index::HnswVectorIndex;
use rand::{rngs::StdRng, Rng, SeedableRng};

fn generate_normalized_vectors(seed: u64, count: usize, dimension: usize) -> Vec<Vec<f32>> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..count)
        .map(|_| {
            let mut vector: Vec<f32> = (0..dimension).map(|_| rng.gen_range(-1.0..1.0)).collect();
            let norm_sq: f32 = vector.iter().map(|x| x * x).sum();
            if norm_sq > 0.0 {
                let inv_norm = norm_sq.sqrt().recip();
                for value in &mut vector {
                    *value *= inv_norm;
                }
            }
            vector
        })
        .collect()
}

fn batch_refs(vectors: &[Vec<f32>]) -> Vec<(&[f32], usize)> {
    vectors
        .iter()
        .enumerate()
        .map(|(id, vector)| (vector.as_slice(), id))
        .collect()
}

fn bench_hnsw_build_single(c: &mut Criterion) {
    let mut group = c.benchmark_group("hnsw_build_single");

    for size in [1_000usize, 10_000, 50_000] {
        let vectors = generate_normalized_vectors(0xC0FF_EE00 + size as u64, size, 128);
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &_size| {
            b.iter(|| {
                let mut index = HnswVectorIndex::new(128, size).unwrap();
                for (id, vector) in vectors.iter().enumerate() {
                    index.add_vector(id as u64, vector).unwrap();
                }
                black_box(index);
            });
        });
    }

    group.finish();
}

fn bench_hnsw_build_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("hnsw_build_batch");

    for size in [1_000usize, 10_000, 50_000] {
        let vectors = generate_normalized_vectors(0xFACE_FEED + size as u64, size, 128);
        let refs = batch_refs(&vectors);
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &_size| {
            b.iter(|| {
                let mut index = HnswVectorIndex::new(128, size).unwrap();
                index.parallel_insert_batch(&refs).unwrap();
                black_box(index);
            });
        });
    }

    group.finish();
}

fn bench_hnsw_search(c: &mut Criterion) {
    let mut group = c.benchmark_group("hnsw_search");

    for (size, k) in [(1_000usize, 10usize), (10_000, 10), (100_000, 10)] {
        let vectors = generate_normalized_vectors(0x1234_5678 + size as u64, size, 128);
        let refs = batch_refs(&vectors);

        let mut index = HnswVectorIndex::new(128, size).unwrap();
        index.parallel_insert_batch(&refs).unwrap();

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

criterion_group!(
    benches,
    bench_hnsw_build_single,
    bench_hnsw_build_batch,
    bench_hnsw_search
);
criterion_main!(benches);
