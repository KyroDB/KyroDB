//! Persistence overhead benchmarks
//!
//! Measures WAL logging overhead for different fsync policies.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use kyrodb_engine::{DistanceMetric, FsyncPolicy, HnswBackend};
use tempfile::TempDir;

fn bench_insert_with_persistence(c: &mut Criterion) {
    let mut group = c.benchmark_group("persistence_insert");

    for policy in &[FsyncPolicy::Never, FsyncPolicy::Always] {
        let policy_name = match policy {
            FsyncPolicy::Never => "never",
            FsyncPolicy::Always => "always",
            _ => "periodic",
        };

        group.bench_with_input(
            BenchmarkId::new("fsync", policy_name),
            policy,
            |b, policy| {
                let dir = TempDir::new().unwrap();
                let data_dir = dir.path();

                let backend = HnswBackend::with_persistence(
                    128,
                    DistanceMetric::Euclidean,
                    vec![],
                    vec![],
                    100,
                    data_dir,
                    *policy,
                    16,
                )
                .unwrap();

                let mut doc_id = 100;

                b.iter(|| {
                    let embedding = vec![0.1; 128];
                    let metadata = std::collections::HashMap::new();
                    backend
                        .insert(black_box(doc_id), embedding, metadata)
                        .unwrap();
                    doc_id += 1;
                });
            },
        );
    }

    group.finish();
}

fn bench_snapshot_creation(c: &mut Criterion) {
    let dir = TempDir::new().unwrap();

    // Create backend with different sizes
    let mut group = c.benchmark_group("snapshot_creation");

    for size in &[100, 1000, 10000] {
        group.bench_with_input(BenchmarkId::new("docs", size), size, |b, size| {
            let embeddings: Vec<Vec<f32>> = (0..*size)
                .map(|i| vec![(i as f32) / *size as f32; 128])
                .collect();
            let metadata_vec: Vec<std::collections::HashMap<String, String>> = (0..*size)
                .map(|_| std::collections::HashMap::new())
                .collect();

            let backend = HnswBackend::with_persistence(
                128,
                DistanceMetric::Euclidean,
                embeddings.clone(),
                metadata_vec,
                *size * 2,
                dir.path(),
                FsyncPolicy::Never,
                100,
            )
            .unwrap();

            b.iter(|| {
                backend.create_snapshot().unwrap();
            });
        });
    }

    group.finish();
}

fn bench_recovery(c: &mut Criterion) {
    let mut group = c.benchmark_group("recovery");

    for size in &[100, 1000, 10000] {
        let size_val = *size;
        let dir = TempDir::new().unwrap();

        // Setup: Create backend and snapshot
        {
            let embeddings: Vec<Vec<f32>> = (0..size_val)
                .map(|i| vec![(i as f32) / size_val as f32; 128])
                .collect();
            let metadata_vec: Vec<std::collections::HashMap<String, String>> = (0..size_val)
                .map(|_| std::collections::HashMap::new())
                .collect();

            let backend = HnswBackend::with_persistence(
                128,
                DistanceMetric::Euclidean,
                embeddings.clone(),
                metadata_vec,
                size_val * 2,
                dir.path(),
                FsyncPolicy::Never,
                100,
            )
            .unwrap();

            backend.create_snapshot().unwrap();
        }

        group.bench_with_input(
            BenchmarkId::new("docs", size),
            &(dir, size_val),
            |b, (dir, size_val)| {
                b.iter(|| {
                    let _recovered = HnswBackend::recover(
                        128,
                        DistanceMetric::Euclidean,
                        dir.path(),
                        *size_val * 2,
                        FsyncPolicy::Never,
                        100,
                        kyrodb_engine::MetricsCollector::new(),
                    )
                    .unwrap();
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_insert_with_persistence,
    bench_snapshot_creation,
    bench_recovery
);
criterion_main!(benches);
