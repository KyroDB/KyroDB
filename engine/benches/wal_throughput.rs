use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use kyrodb_engine::PersistentEventLog;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::runtime::Runtime;

/// Benchmark sequential WAL append throughput
fn bench_sequential_append(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_sequential_append");

    for batch_size in [100, 1000, 10000] {
        group.throughput(Throughput::Elements(batch_size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            &batch_size,
            |b, &batch_size| {
                let rt = Runtime::new().unwrap();

                b.to_async(&rt).iter(|| async {
                    let dir = TempDir::new().unwrap();
                    let log = Arc::new(PersistentEventLog::open(dir.path()).await.unwrap());

                    for i in 0..batch_size {
                        let key = i as u64;
                        let value = format!("value_{}", i).into_bytes();
                        black_box(log.put(key, value).await.unwrap());
                    }
                });
            },
        );
    }

    group.finish();
}

/// Benchmark concurrent WAL append throughput
fn bench_concurrent_append(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_concurrent_append");

    for workers in [4, 8, 16, 32, 64] {
        let total_ops = workers * 1000;
        group.throughput(Throughput::Elements(total_ops as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(workers),
            &workers,
            |b, &workers| {
                let rt = Runtime::new().unwrap();

                b.to_async(&rt).iter(|| async {
                    let dir = TempDir::new().unwrap();
                    let log = Arc::new(PersistentEventLog::open(dir.path()).await.unwrap());

                    let mut handles = Vec::new();

                    for worker_id in 0..workers {
                        let log_clone = log.clone();
                        handles.push(tokio::spawn(async move {
                            for i in 0..1000 {
                                let key = (worker_id * 1000 + i) as u64;
                                let value =
                                    format!("worker_{}_value_{}", worker_id, i).into_bytes();
                                log_clone.put(key, value).await.unwrap();
                            }
                        }));
                    }

                    for handle in handles {
                        handle.await.unwrap();
                    }
                });
            },
        );
    }

    group.finish();
}

/// Benchmark group commit effectiveness
fn bench_group_commit(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_group_commit");
    group.throughput(Throughput::Elements(10000));

    group.bench_function("group_commit_enabled", |b| {
        let rt = Runtime::new().unwrap();

        b.to_async(&rt).iter(|| async {
            let dir = TempDir::new().unwrap();
            let log = Arc::new(PersistentEventLog::open(dir.path()).await.unwrap());

            // Simulate burst of writes (group commit should batch fsyncs)
            let mut handles = Vec::new();
            for i in 0..10000 {
                let log_clone = log.clone();
                handles.push(tokio::spawn(async move {
                    let key = i as u64;
                    let value = vec![0u8; 256];
                    log_clone.put(key, value).await.unwrap();
                }));
            }

            for handle in handles {
                black_box(handle.await.unwrap());
            }
        });
    });

    group.finish();
}

/// Benchmark fsync frequency impact
fn bench_fsync_policy(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_fsync_policy");
    group.throughput(Throughput::Elements(1000));

    group.bench_function("with_fsync", |b| {
        let rt = Runtime::new().unwrap();

        b.to_async(&rt).iter(|| async {
            let dir = TempDir::new().unwrap();
            // In production, fsync policy would be configured via env vars
            let log = Arc::new(PersistentEventLog::open(dir.path()).await.unwrap());

            for i in 0..1000 {
                let key = i as u64;
                let value = vec![0u8; 256];
                black_box(log.put(key, value).await.unwrap());
            }
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_sequential_append,
    bench_concurrent_append,
    bench_group_commit,
    bench_fsync_policy,
);

criterion_main!(benches);
