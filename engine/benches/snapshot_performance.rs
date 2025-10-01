use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use kyrodb_engine::PersistentEventLog;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::runtime::Runtime;

/// Benchmark snapshot creation time vs dataset size
fn bench_snapshot_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("snapshot_creation");

    for dataset_size in [10_000, 100_000, 1_000_000] {
        group.throughput(Throughput::Elements(dataset_size as u64));
        group.sample_size(10); // Reduce sample size for large datasets

        group.bench_with_input(
            BenchmarkId::from_parameter(dataset_size),
            &dataset_size,
            |b, &dataset_size| {
                let rt = Runtime::new().unwrap();

                b.to_async(&rt).iter(|| async {
                    let dir = TempDir::new().unwrap();
                    let log = Arc::new(PersistentEventLog::open(dir.path()).await.unwrap());

                    // Populate data
                    for i in 0..dataset_size {
                        let key = i as u64;
                        let value = vec![0u8; 256];
                        log.put(key, value).await.unwrap();
                    }

                    // Measure snapshot creation
                    black_box(log.snapshot().await.unwrap());
                });
            },
        );
    }

    group.finish();
}

/// Benchmark snapshot load time
fn bench_snapshot_load(c: &mut Criterion) {
    let mut group = c.benchmark_group("snapshot_load");

    for dataset_size in [10_000, 100_000, 1_000_000] {
        group.throughput(Throughput::Elements(dataset_size as u64));
        group.sample_size(10);

        group.bench_with_input(
            BenchmarkId::from_parameter(dataset_size),
            &dataset_size,
            |b, &dataset_size| {
                let rt = Runtime::new().unwrap();

                // Setup: Create snapshot
                let dir = TempDir::new().unwrap();
                let dir_path = dir.path().to_path_buf();

                rt.block_on(async {
                    let log = Arc::new(PersistentEventLog::open(&dir_path).await.unwrap());

                    for i in 0..dataset_size {
                        let key = i as u64;
                        let value = vec![0u8; 256];
                        log.put(key, value).await.unwrap();
                    }

                    log.snapshot().await.unwrap();
                });

                // Measure: Load snapshot
                b.to_async(&rt).iter(|| async {
                    let log = Arc::new(PersistentEventLog::open(&dir_path).await.unwrap());

                    // Access some data to ensure snapshot is loaded
                    for i in (0..dataset_size).step_by(1000) {
                        black_box(log.get(i as u64).await.ok());
                    }
                });
            },
        );
    }

    group.finish();
}

/// Benchmark mmap warmup time
fn bench_mmap_warmup(c: &mut Criterion) {
    let mut group = c.benchmark_group("mmap_warmup");

    for dataset_size in [100_000, 1_000_000] {
        group.sample_size(10);

        group.bench_with_input(
            BenchmarkId::from_parameter(dataset_size),
            &dataset_size,
            |b, &dataset_size| {
                let rt = Runtime::new().unwrap();

                // Setup: Create snapshot
                let dir = TempDir::new().unwrap();
                let dir_path = dir.path().to_path_buf();

                rt.block_on(async {
                    let log = Arc::new(PersistentEventLog::open(&dir_path).await.unwrap());

                    for i in 0..dataset_size {
                        let key = i as u64;
                        let value = vec![0u8; 512];
                        log.put(key, value).await.unwrap();
                    }

                    log.snapshot().await.unwrap();
                });

                // Measure: Warmup operation
                b.to_async(&rt).iter(|| async {
                    let log = Arc::new(PersistentEventLog::open(&dir_path).await.unwrap());

                    // Warmup mmap pages
                    black_box(log.warmup().await.ok());
                });
            },
        );
    }

    group.finish();
}

/// Benchmark snapshot with concurrent operations
fn bench_snapshot_concurrent_ops(c: &mut Criterion) {
    let mut group = c.benchmark_group("snapshot_concurrent_ops");
    group.sample_size(10);

    group.bench_function("snapshot_during_writes", |b| {
        let rt = Runtime::new().unwrap();

        b.to_async(&rt).iter(|| async {
            let dir = TempDir::new().unwrap();
            let log = Arc::new(PersistentEventLog::open(dir.path()).await.unwrap());

            // Populate initial data
            for i in 0..10000 {
                log.put(i as u64, vec![0u8; 256]).await.unwrap();
            }

            // Start concurrent writes
            let log_clone = log.clone();
            let write_handle = tokio::spawn(async move {
                for i in 10000..20000 {
                    log_clone.put(i as u64, vec![0u8; 256]).await.ok();
                }
            });

            // Create snapshot while writes are happening
            black_box(log.snapshot().await.unwrap());

            write_handle.await.unwrap();
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_snapshot_creation,
    bench_snapshot_load,
    bench_mmap_warmup,
    bench_snapshot_concurrent_ops,
);

criterion_main!(benches);
