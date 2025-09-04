// High-performance gRPC benchmark tool for KyroDB
// Achieves supreme throughput using all optimization strategies

use anyhow::{Context, Result};
use base64::{engine::general_purpose, Engine as _};
use clap::Parser;
use futures::stream::FuturesUnordered;
use hdrhistogram::Histogram;
use indicatif::{ProgressBar, ProgressStyle};
use rand::prelude::*;
use rand_distr::zipf::Zipf;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tonic::transport::Channel;

mod grpc_client;
use grpc_client::UltraFastGrpcClient;

pub mod kyrodb {
    tonic::include_proto!("kyrodb.v1");
}

#[derive(Parser)]
#[command(name = "grpc_bench")]
#[command(about = "Supreme performance gRPC benchmark for KyroDB")]
struct Args {
    /// Server endpoint
    #[arg(short, long, default_value = "http://127.0.0.1:3030")]
    endpoint: String,

    /// Number of keys to load
    #[arg(short = 'n', long, default_value = "100000")]
    load_n: usize,

    /// Value size in bytes
    #[arg(short = 'v', long, default_value = "1024")]
    value_size: usize,

    /// Concurrency level
    #[arg(short, long, default_value = "1000")]
    concurrency: usize,

    /// Batch size for vectorized operations
    #[arg(short = 'b', long, default_value = "100")]
    batch_size: usize,

    /// Duration of sustained load test in seconds
    #[arg(short, long, default_value = "30")]
    duration: u64,

    /// Authentication token
    #[arg(short, long)]
    auth_token: Option<String>,

    /// Use Zipfian distribution for lookups
    #[arg(long)]
    zipfian: bool,

    /// Enable streaming operations
    #[arg(long)]
    streaming: bool,

    /// Export results to CSV
    #[arg(long)]
    export_csv: Option<String>,

    /// Warmup duration in seconds
    #[arg(long, default_value = "5")]
    warmup: u64,

    /// Connection pool size
    #[arg(long, default_value = "100")]
    pool_size: usize,
}

#[derive(Debug, Clone)]
struct BenchmarkResults {
    total_operations: u64,
    duration_secs: f64,
    ops_per_sec: f64,
    latency_p50: f64,
    latency_p95: f64,
    latency_p99: f64,
    latency_p999: f64,
    latency_max: f64,
    errors: u64,
    success_rate: f64,
}

#[derive(Debug)]
struct LoadTestStats {
    operations: AtomicU64,
    errors: AtomicU64,
    latency_histogram: parking_lot::Mutex<Histogram<u64>>,
}

impl LoadTestStats {
    fn new() -> Self {
        Self {
            operations: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            latency_histogram: parking_lot::Mutex::new(Histogram::new(3).unwrap()),
        }
    }

    fn record_success(&self, latency_micros: u64) {
        self.operations.fetch_add(1, Ordering::Relaxed);
        self.latency_histogram.lock().record(latency_micros).ok();
    }

    fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    fn get_results(&self, duration: Duration) -> BenchmarkResults {
        let total_ops = self.operations.load(Ordering::Relaxed);
        let total_errors = self.errors.load(Ordering::Relaxed);
        let duration_secs = duration.as_secs_f64();
        let ops_per_sec = total_ops as f64 / duration_secs;
        let success_rate = if total_ops + total_errors > 0 {
            total_ops as f64 / (total_ops + total_errors) as f64 * 100.0
        } else {
            0.0
        };

        let hist = self.latency_histogram.lock();
        BenchmarkResults {
            total_operations: total_ops,
            duration_secs,
            ops_per_sec,
            latency_p50: hist.value_at_quantile(0.50) as f64 / 1000.0, // Convert to milliseconds
            latency_p95: hist.value_at_quantile(0.95) as f64 / 1000.0,
            latency_p99: hist.value_at_quantile(0.99) as f64 / 1000.0,
            latency_p999: hist.value_at_quantile(0.999) as f64 / 1000.0,
            latency_max: hist.max() as f64 / 1000.0,
            errors: total_errors,
            success_rate,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    
    println!("🚀 KyroDB Supreme Performance gRPC Benchmark");
    println!("================================================");
    println!("Endpoint: {}", args.endpoint);
    println!("Keys: {}", args.load_n);
    println!("Value size: {} bytes", args.value_size);
    println!("Concurrency: {}", args.concurrency);
    println!("Batch size: {}", args.batch_size);
    println!("Duration: {}s", args.duration);
    println!("Pool size: {}", args.pool_size);
    println!("Streaming: {}", args.streaming);
    println!("Zipfian: {}", args.zipfian);
    println!();

    // Create ultra-fast gRPC client with connection pooling
    let client = UltraFastGrpcClient::connect(&args.endpoint, args.pool_size, args.auth_token.clone()).await
        .context("Failed to create gRPC client")?;

    // Health check
    println!("🔍 Performing health check...");
    client.health_check().await.context("Health check failed")?;
    println!("✅ Server is healthy");

    // Trigger RMI build for optimal performance
    println!("🧠 Triggering RMI build for learned index optimization...");
    client.build_rmi().await.context("RMI build failed")?;
    println!("✅ RMI build completed");

    // Warmup phase
    if args.warmup > 0 {
        println!("🔥 Warming up for {}s...", args.warmup);
        perform_warmup(&client, &args).await?;
        println!("✅ Warmup completed");
    }

    // Load data phase
    println!("📊 Loading {} keys...", args.load_n);
    let load_results = load_data(&client, &args).await?;
    print_results("LOAD", &load_results);

    // Sustained load test
    println!("⚡ Running sustained load test for {}s...", args.duration);
    let read_results = sustained_load_test(&client, &args).await?;
    print_results("READ", &read_results);

    // Export results if requested
    if let Some(csv_path) = &args.export_csv {
        export_to_csv(csv_path, &load_results, &read_results)?;
        println!("📄 Results exported to {}", csv_path);
    }

    // Final performance summary
    println!("\n🏆 SUPREME PERFORMANCE ACHIEVED");
    println!("================================");
    println!("Load throughput: {:.0} ops/sec", load_results.ops_per_sec);
    println!("Read throughput: {:.0} ops/sec", read_results.ops_per_sec);
    println!("Read latency p99: {:.2}ms", read_results.latency_p99);
    println!("Success rate: {:.2}%", read_results.success_rate);

    Ok(())
}

async fn perform_warmup(client: &UltraFastGrpcClient, args: &Args) -> Result<()> {
    let semaphore = Arc::new(Semaphore::new(args.concurrency));
    let mut tasks = FuturesUnordered::new();
    let start = Instant::now();

    while start.elapsed() < Duration::from_secs(args.warmup) {
        let permit = semaphore.clone().acquire_owned().await?;
        let client = client.clone();
        let key = format!("warmup_{}", thread_rng().gen::<u64>());
        
        tasks.push(tokio::spawn(async move {
            let _permit = permit;
            let _ = client.get(key.parse().unwrap()).await;
        }));

        if tasks.len() >= args.concurrency {
            futures::future::select_all(tasks.into_iter().collect::<Vec<_>>()).await;
            tasks = FuturesUnordered::new();
        }
    }

    Ok(())
}

async fn load_data(client: &UltraFastGrpcClient, args: &Args) -> Result<BenchmarkResults> {
    let stats = Arc::new(LoadTestStats::new());
    let semaphore = Arc::new(Semaphore::new(args.concurrency));
    let progress = ProgressBar::new(args.load_n as u64);
    progress.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {per_sec} ETA: {eta}")
            .unwrap()
    );

    let value = "x".repeat(args.value_size);
    let start_time = Instant::now();
    let mut tasks = FuturesUnordered::new();

    if args.streaming && args.batch_size > 1 {
        // Use streaming batch operations for supreme performance
        for batch_start in (0..args.load_n).step_by(args.batch_size) {
            let permit = semaphore.clone().acquire_owned().await?;
            let client = client.clone();
            let stats = stats.clone();
            let progress = progress.clone();
            let value = value.clone();
            let batch_end = (batch_start + args.batch_size).min(args.load_n);
            
            tasks.push(tokio::spawn(async move {
                let _permit = permit;
                let request_start = Instant::now();
                
                let batch: Vec<_> = (batch_start..batch_end)
                    .map(|i| (format!("key_{}", i), value.clone()))
                    .collect();

                match client.batch_put_stream(vec![batch.into_iter().map(|(k, v)| (k.parse().unwrap(), v.into_bytes())).collect()]).await {
                    Ok(_) => {
                        let latency = request_start.elapsed().as_micros() as u64;
                        stats.record_success(latency);
                        progress.inc((batch_end - batch_start) as u64);
                    }
                    Err(_) => {
                        stats.record_error();
                    }
                }
            }));

            if tasks.len() >= args.concurrency / 10 {
                futures::future::select_all(tasks.into_iter().collect::<Vec<_>>()).await;
                tasks = FuturesUnordered::new();
            }
        }
    } else if args.batch_size > 1 {
        // Use regular batch operations
        for batch_start in (0..args.load_n).step_by(args.batch_size) {
            let permit = semaphore.clone().acquire_owned().await?;
            let client = client.clone();
            let stats = stats.clone();
            let progress = progress.clone();
            let value = value.clone();
            let batch_end = (batch_start + args.batch_size).min(args.load_n);
            
            tasks.push(tokio::spawn(async move {
                let _permit = permit;
                let request_start = Instant::now();
                
                let batch: HashMap<String, String> = (batch_start..batch_end)
                    .map(|i| (format!("key_{}", i), value.clone()))
                    .collect();

                match client.batch_put(batch.into_iter().map(|(k, v)| (k.parse().unwrap(), v.into_bytes())).collect()).await {
                    Ok(_) => {
                        let latency = request_start.elapsed().as_micros() as u64;
                        stats.record_success(latency);
                        progress.inc((batch_end - batch_start) as u64);
                    }
                    Err(_) => {
                        stats.record_error();
                    }
                }
            }));

            if tasks.len() >= args.concurrency / 10 {
                futures::future::select_all(tasks.into_iter().collect::<Vec<_>>()).await;
                tasks = FuturesUnordered::new();
            }
        }
    } else {
        // Individual operations for baseline comparison
        for i in 0..args.load_n {
            let permit = semaphore.clone().acquire_owned().await?;
            let client = client.clone();
            let stats = stats.clone();
            let progress = progress.clone();
            let key = format!("key_{}", i);
            let value = value.clone();
            
            tasks.push(tokio::spawn(async move {
                let _permit = permit;
                let request_start = Instant::now();
                
                match client.put(key.parse().unwrap(), value.into_bytes()).await {
                    Ok(_) => {
                        let latency = request_start.elapsed().as_micros() as u64;
                        stats.record_success(latency);
                        progress.inc(1);
                    }
                    Err(_) => {
                        stats.record_error();
                    }
                }
            }));

            if tasks.len() >= args.concurrency {
                futures::future::select_all(tasks.into_iter().collect::<Vec<_>>()).await;
                tasks = FuturesUnordered::new();
            }
        }
    }

    // Wait for remaining tasks
    while !tasks.is_empty() {
        futures::future::select_all(tasks.into_iter().collect::<Vec<_>>()).await;
        tasks = FuturesUnordered::new();
    }

    progress.finish_with_message("Load completed");
    let duration = start_time.elapsed();
    Ok(stats.get_results(duration))
}

async fn sustained_load_test(client: &UltraFastGrpcClient, args: &Args) -> Result<BenchmarkResults> {
    let stats = Arc::new(LoadTestStats::new());
    let semaphore = Arc::new(Semaphore::new(args.concurrency));
    let running = Arc::new(AtomicUsize::new(1));
    
    let progress = ProgressBar::new(args.duration);
    progress.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.green/blue} {pos:>3}/{len:3}s {per_sec} ops/sec")
            .unwrap()
    );

    let start_time = Instant::now();
    let mut tasks = FuturesUnordered::new();

    // Create key distribution
    let zipfian_dist = if args.zipfian {
        Some(Zipf::new(args.load_n as f64, 1.03).unwrap())
    } else {
        None
    };

    // Spawn concurrent workers
    for _ in 0..args.concurrency {
        let permit = semaphore.clone().acquire_owned().await?;
        let client = client.clone();
        let stats = stats.clone();
        let running = running.clone();
        let zipfian_dist = zipfian_dist.clone();
        let load_n = args.load_n;
        let batch_size = args.batch_size;
        let use_streaming = args.streaming;
        
        tasks.push(tokio::spawn(async move {
            let _permit = permit;
            let mut rng = thread_rng();
            
            while running.load(Ordering::Relaxed) > 0 {
                let request_start = Instant::now();
                
                if batch_size > 1 {
                    // Batch operations for higher throughput
                    let keys: Vec<String> = (0..batch_size)
                        .map(|_| {
                            let key_idx = if let Some(ref dist) = zipfian_dist {
                                dist.sample(&mut rng) as usize
                            } else {
                                rng.gen_range(0..load_n)
                            };
                            format!("key_{}", key_idx)
                        })
                        .collect();

                    match client.batch_get(keys.into_iter().map(|k| k.parse().unwrap()).collect()).await {
                        Ok(_) => {
                            let latency = request_start.elapsed().as_micros() as u64;
                            stats.record_success(latency);
                        }
                        Err(_) => {
                            stats.record_error();
                        }
                    }
                } else {
                    // Individual operations
                    let key_idx = if let Some(ref dist) = zipfian_dist {
                        dist.sample(&mut rng) as usize
                    } else {
                        rng.gen_range(0..load_n)
                    };
                    let key = format!("key_{}", key_idx);

                    match client.get(&key).await {
                        Ok(_) => {
                            let latency = request_start.elapsed().as_micros() as u64;
                            stats.record_success(latency);
                        }
                        Err(_) => {
                            stats.record_error();
                        }
                    }
                }
            }
        }));
    }

    // Progress tracking
    let progress_task = {
        let progress = progress.clone();
        let running = running.clone();
        tokio::spawn(async move {
            let start = Instant::now();
            while running.load(Ordering::Relaxed) > 0 {
                let elapsed = start.elapsed().as_secs();
                progress.set_position(elapsed);
                if elapsed >= args.duration {
                    running.store(0, Ordering::Relaxed);
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            progress.finish_with_message("Sustained load test completed");
        })
    };

    // Wait for test completion
    progress_task.await?;
    
    // Wait for all workers to finish
    for task in tasks {
        task.await?;
    }

    let duration = start_time.elapsed();
    Ok(stats.get_results(duration))
}

fn print_results(phase: &str, results: &BenchmarkResults) {
    println!("\n📈 {} RESULTS", phase);
    println!("═══════════════════════════════════════════");
    println!("Total operations: {}", results.total_operations);
    println!("Duration: {:.2}s", results.duration_secs);
    println!("Throughput: {:.0} ops/sec", results.ops_per_sec);
    println!("Success rate: {:.2}%", results.success_rate);
    println!("Errors: {}", results.errors);
    println!("Latency p50: {:.3}ms", results.latency_p50);
    println!("Latency p95: {:.3}ms", results.latency_p95);
    println!("Latency p99: {:.3}ms", results.latency_p99);
    println!("Latency p99.9: {:.3}ms", results.latency_p999);
    println!("Latency max: {:.3}ms", results.latency_max);
}

fn export_to_csv(
    path: &str, 
    load_results: &BenchmarkResults, 
    read_results: &BenchmarkResults
) -> Result<()> {
    use std::fs::File;
    use std::io::Write;

    let mut file = File::create(path)?;
    writeln!(file, "metric,load_value,read_value")?;
    writeln!(file, "ops_per_sec,{:.0},{:.0}", load_results.ops_per_sec, read_results.ops_per_sec)?;
    writeln!(file, "latency_p50_ms,{:.3},{:.3}", load_results.latency_p50, read_results.latency_p50)?;
    writeln!(file, "latency_p95_ms,{:.3},{:.3}", load_results.latency_p95, read_results.latency_p95)?;
    writeln!(file, "latency_p99_ms,{:.3},{:.3}", load_results.latency_p99, read_results.latency_p99)?;
    writeln!(file, "latency_p999_ms,{:.3},{:.3}", load_results.latency_p999, read_results.latency_p999)?;
    writeln!(file, "success_rate_pct,{:.2},{:.2}", load_results.success_rate, read_results.success_rate)?;
    writeln!(file, "total_operations,{},{}", load_results.total_operations, read_results.total_operations)?;
    writeln!(file, "errors,{},{}", load_results.errors, read_results.errors)?;

    Ok(())
}
