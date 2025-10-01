#[cfg(feature = "learned-index")]
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
#[cfg(feature = "learned-index")]
use kyrodb_engine::PersistentEventLog;
#[cfg(feature = "learned-index")]
use std::sync::Arc;
#[cfg(feature = "learned-index")]
use tempfile::TempDir;
#[cfg(feature = "learned-index")]
use tokio::runtime::Runtime;

#[cfg(feature = "learned-index")]
/// Benchmark RMI build time vs dataset size
fn bench_rmi_build_time(c: &mut Criterion) {
    let mut group = c.benchmark_group("rmi_build");
    
    for dataset_size in [100_000, 1_000_000, 5_000_000] {
        group.throughput(Throughput::Elements(dataset_size as u64));
        group.sample_size(10); // Reduce sample size for large datasets
        
        group.bench_with_input(
            BenchmarkId::from_parameter(dataset_size),
            &dataset_size,
            |b, &dataset_size| {
                let rt = Runtime::new().unwrap();
                
                b.to_async(&rt).iter(|| async {
                    let dir = TempDir::new().unwrap();
                    let log = Arc::new(
                        PersistentEventLog::open(dir.path()).await.unwrap()
                    );
                    
                    // Populate data
                    for i in 0..dataset_size {
                        let key = i as u64;
                        let value = vec![0u8; 64];
                        log.put(key, value).await.unwrap();
                    }
                    
                    // Create snapshot (required before RMI build)
                    log.snapshot().await.unwrap();
                    
                    // Measure RMI build time
                    black_box(log.build_rmi().await.unwrap());
                });
            },
        );
    }
    
    group.finish();
}

#[cfg(feature = "learned-index")]
/// Benchmark RMI rebuild time under concurrent load
fn bench_rmi_rebuild_concurrent(c: &mut Criterion) {
    let mut group = c.benchmark_group("rmi_rebuild_concurrent");
    group.sample_size(10);
    
    group.bench_function("rebuild_during_reads", |b| {
        let rt = Runtime::new().unwrap();
        
        b.to_async(&rt).iter(|| async {
            let dir = TempDir::new().unwrap();
            let log = Arc::new(
                PersistentEventLog::open(dir.path()).await.unwrap()
            );
            
            // Populate initial dataset
            for i in 0..100_000 {
                log.put(i as u64, vec![0u8; 256]).await.unwrap();
            }
            
            log.snapshot().await.unwrap();
            log.build_rmi().await.unwrap();
            
            // Start concurrent reads
            let log_clone = log.clone();
            let read_handle = tokio::spawn(async move {
                for i in 0..10000 {
                    let key = (i % 100_000) as u64;
                    black_box(log_clone.get(key).await.ok());
                }
            });
            
            // Add more data and rebuild RMI
            for i in 100_000..150_000 {
                log.put(i as u64, vec![0u8; 256]).await.ok();
            }
            
            log.snapshot().await.unwrap();
            black_box(log.build_rmi().await.unwrap());
            
            read_handle.await.unwrap();
        });
    });
    
    group.finish();
}

#[cfg(feature = "learned-index")]
/// Benchmark RMI index swap overhead
fn bench_rmi_index_swap(c: &mut Criterion) {
    let mut group = c.benchmark_group("rmi_index_swap");
    group.sample_size(20);
    
    group.bench_function("atomic_swap", |b| {
        let rt = Runtime::new().unwrap();
        
        // Setup: Prepare log with data and initial RMI
        let dir = TempDir::new().unwrap();
        let dir_path = dir.path().to_path_buf();
        
        rt.block_on(async {
            let log = Arc::new(
                PersistentEventLog::open(&dir_path).await.unwrap()
            );
            
            for i in 0..100_000 {
                log.put(i as u64, vec![0u8; 128]).await.unwrap();
            }
            
            log.snapshot().await.unwrap();
            log.build_rmi().await.unwrap();
        });
        
        // Measure: Index swap during concurrent reads
        b.to_async(&rt).iter(|| async {
            let log = Arc::new(
                PersistentEventLog::open(&dir_path).await.unwrap()
            );
            
            // Simulate concurrent reads during swap
            let log_clone = log.clone();
            let read_handle = tokio::spawn(async move {
                for i in 0..1000 {
                    let key = (i % 100_000) as u64;
                    black_box(log_clone.get(key).await.ok());
                }
            });
            
            // Perform index rebuild (includes atomic swap)
            black_box(log.build_rmi().await.unwrap());
            
            read_handle.await.unwrap();
        });
    });
    
    group.finish();
}

#[cfg(feature = "learned-index")]
/// Benchmark RMI prediction accuracy impact on performance
fn bench_rmi_prediction_accuracy(c: &mut Criterion) {
    let mut group = c.benchmark_group("rmi_prediction_accuracy");
    
    // Test with different data distributions
    for pattern in ["sequential", "uniform", "skewed"] {
        group.bench_with_input(
            BenchmarkId::from_parameter(pattern),
            &pattern,
            |b, &pattern| {
                let rt = Runtime::new().unwrap();
                
                b.to_async(&rt).iter(|| async {
                    let dir = TempDir::new().unwrap();
                    let log = Arc::new(
                        PersistentEventLog::open(dir.path()).await.unwrap()
                    );
                    
                    // Insert data based on pattern
                    match pattern {
                        "sequential" => {
                            for i in 0..100_000 {
                                log.put(i as u64, vec![0u8; 128]).await.unwrap();
                            }
                        }
                        "uniform" => {
                            use rand::Rng;
                            let mut rng = rand::thread_rng();
                            for _ in 0..100_000 {
                                let key = rng.gen_range(0..1_000_000);
                                log.put(key, vec![0u8; 128]).await.ok();
                            }
                        }
                        "skewed" => {
                            // Zipf-like distribution
                            for i in 0..50_000 {
                                log.put(i as u64, vec![0u8; 128]).await.unwrap();
                            }
                            for i in 0..50_000 {
                                let key = i % 5000; // Skewed to first 5K keys
                                log.put(key, vec![0u8; 128]).await.ok();
                            }
                        }
                        _ => {}
                    }
                    
                    log.snapshot().await.unwrap();
                    log.build_rmi().await.unwrap();
                    
                    // Measure lookup performance
                    for i in 0..1000 {
                        let key = (i % 100_000) as u64;
                        black_box(log.get(key).await.ok());
                    }
                });
            },
        );
    }
    
    group.finish();
}

#[cfg(feature = "learned-index")]
criterion_group!(
    benches,
    bench_rmi_build_time,
    bench_rmi_rebuild_concurrent,
    bench_rmi_index_swap,
    bench_rmi_prediction_accuracy,
);

#[cfg(not(feature = "learned-index"))]
criterion_group!(benches,);

criterion_main!(benches);
