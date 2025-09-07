use anyhow::Result;
use clap::Parser;
use hdrhistogram::Histogram;
use indicatif::{ProgressBar, ProgressStyle};
use rand::{rngs::StdRng, Rng, SeedableRng};
use std::sync::atomic::{AtomicU64, Ordering};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{sync::Semaphore, task::JoinSet};

mod client;
use client::BenchClient;

#[derive(Parser, Debug, Clone)]
struct Args {
    /// Engine base URL for HTTP
    #[arg(long, default_value = "http://127.0.0.1:3030")]
    base: String,
    
    /// Protocol to use: http only for Phase 0
    #[arg(long, default_value = "http")]
    protocol: String,

    /// Number of concurrent workers for load testing
    #[arg(long, default_value = "64")]
    workers: usize,

    /// Target operations per second (0 = unlimited)
    #[arg(long, default_value = "0")]
    rate: u64,

    /// Duration of the benchmark in seconds
    #[arg(long, default_value = "30")]
    duration: u64,

    /// Key prefix for generated keys
    #[arg(long, default_value = "bench")]
    key_prefix: String,

    /// Value size in bytes for PUT operations
    #[arg(long, default_value = "256")]
    value_size: usize,

    /// Read-write ratio (0.0 = all writes, 1.0 = all reads)
    #[arg(long, default_value = "0.5")]
    read_ratio: f64,

    /// Use zipf distribution for key selection (vs uniform)
    #[arg(long)]
    zipf: bool,

    /// Zipf skew parameter (1.0 = uniform, higher = more skewed)
    #[arg(long, default_value = "1.2")]
    zipf_skew: f64,

    /// Number of unique keys in the dataset
    #[arg(long, default_value = "100000")]
    key_count: u64,

    /// Warmup time in seconds before measuring
    #[arg(long, default_value = "10")]
    warmup: u64,

    /// Enable streaming operations for batch workloads (HTTP only)
    #[arg(long)]
    streaming: bool,

    /// Batch size for streaming operations
    #[arg(long, default_value = "100")]
    batch_size: usize,

    /// Authentication token
    #[arg(long)]
    auth_token: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    println!("🔥 KyroDB Performance Benchmark - Phase 0 Foundation Testing");
    println!("📊 Testing core KV performance with HTTP protocol");

    // Validate arguments
    if !(0.0..=1.0).contains(&args.read_ratio) {
        anyhow::bail!("Read ratio must be between 0.0 and 1.0");
    }

    // Create benchmark client
    let mut client = BenchClient::new(
        &args.protocol,
        &args.base,
        "",  // No gRPC address needed
        args.auth_token.clone(),
    ).await?;

    match args.protocol.as_str() {
        "http" => {
            if args.streaming {
                println!("🚀 Using HTTP batch operations for maximum throughput");
                run_streaming_benchmark(&mut client, &args).await?;
            } else {
                println!("🚀 Using HTTP point operations");
                run_workload(&mut client, &args).await?;
            }
        }
        _ => {
            anyhow::bail!("Only 'http' protocol supported in Phase 0");
        }
    }

    Ok(())
}

async fn run_streaming_benchmark(client: &mut BenchClient, args: &Args) -> Result<()> {
    println!("🏁 Running streaming benchmark for {} seconds", args.duration);
    
    let value = "x".repeat(args.value_size);
    let mut rng = StdRng::seed_from_u64(42);
    
    let warmup_end = Instant::now() + Duration::from_secs(args.warmup);
    let test_end = warmup_end + Duration::from_secs(args.duration);
    
    let mut batch_items = Vec::with_capacity(args.batch_size);
    let mut ops_count = 0u64;
    let mut start_time = None;
    
    println!("🔥 Warming up for {} seconds...", args.warmup);
    
    while Instant::now() < test_end {
        // Build batch
        batch_items.clear();
        for _ in 0..args.batch_size {
            let key = format!("{}_{}", args.key_prefix, rng.gen_range(0..args.key_count));
            batch_items.push((key, value.clone()));
        }
        
        // Execute batch
        let batch_start = Instant::now();
        client.batch_put(&batch_items, None, &args.base).await?;
        
        // Track metrics after warmup
        if Instant::now() > warmup_end {
            if start_time.is_none() {
                start_time = Some(Instant::now());
                println!("📈 Starting measurement phase...");
            }
            ops_count += args.batch_size as u64;
        }
        
        // Rate limiting
        if args.rate > 0 {
            let target_duration = Duration::from_micros(
                (args.batch_size as u64 * 1_000_000) / args.rate
            );
            let elapsed = batch_start.elapsed();
            if elapsed < target_duration {
                tokio::time::sleep(target_duration - elapsed).await;
            }
        }
    }
    
    let test_duration = start_time.unwrap().elapsed();
    let throughput = ops_count as f64 / test_duration.as_secs_f64();
    
    println!("🎯 Streaming Benchmark Results:");
    println!("   Operations: {}", ops_count);
    println!("   Duration: {:.2}s", test_duration.as_secs_f64());
    println!("   Throughput: {:.2} ops/sec", throughput);
    println!("   Batch Size: {}", args.batch_size);
    
    Ok(())
}

async fn run_workload(client: &mut BenchClient, args: &Args) -> Result<()> {
    println!("🏁 Starting load test with {} workers for {} seconds", 
             args.workers, args.duration);

    let completed_ops = Arc::new(AtomicU64::new(0));
    let semaphore = Arc::new(Semaphore::new(args.workers));
    let mut histogram = Histogram::<u64>::new(3)?;
    
    let progress_bar = ProgressBar::new(args.duration);
    progress_bar.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
            .unwrap()
            .progress_chars("#>-"),
    );

    let warmup_end = Instant::now() + Duration::from_secs(args.warmup);
    let test_end = warmup_end + Duration::from_secs(args.duration);
    
    println!("🔥 Warming up for {} seconds...", args.warmup);
    
    let mut tasks = JoinSet::new();
    let mut measurement_started = false;
    let mut measurement_start = Instant::now();
    
    while Instant::now() < test_end {
        let _permit = semaphore.clone().acquire_owned().await?;
        let mut client_clone = client.clone();
        let completed = completed_ops.clone();
        let args_clone = args.clone();
        
        if Instant::now() > warmup_end && !measurement_started {
            measurement_started = true;
            measurement_start = Instant::now();
            println!("📈 Starting measurement phase...");
        }
        
        tasks.spawn(async move {
            let mut rng = StdRng::from_entropy();
            let key = format!("{}_{}", args_clone.key_prefix, 
                             rng.gen_range(0..args_clone.key_count));
            
            let start = Instant::now();
            let result = if rng.gen::<f64>() < args_clone.read_ratio {
                match client_clone.get(&key, None, &args_clone.base).await {
                    Ok(_) => Ok(()),
                    Err(e) => Err(e),
                }
            } else {
                let value = "x".repeat(args_clone.value_size);
                client_clone.put(key, value, None, &args_clone.base).await
            };
            
            let latency = start.elapsed();
            
            if measurement_started {
                completed.fetch_add(1, Ordering::Relaxed);
            }
            
            (result, latency.as_micros() as u64)
        });
        
        // Rate limiting
        if args.rate > 0 {
            let delay = Duration::from_micros(1_000_000 / args.rate);
            tokio::time::sleep(delay).await;
        }
        
        // Update progress
        let elapsed = measurement_start.elapsed().as_secs();
        if measurement_started && elapsed <= args.duration {
            progress_bar.set_position(elapsed);
        }
    }
    
    progress_bar.finish_with_message("Collecting results...");
    
    // Collect all results
    let mut success_count = 0;
    let mut error_count = 0;
    
    while let Some(result) = tasks.try_join_next() {
        match result {
            Ok((Ok(_), latency)) => {
                success_count += 1;
                histogram.record(latency)?;
            }
            Ok((Err(_), _)) => error_count += 1,
            Err(_) => error_count += 1,
        }
    }
    
    let total_ops = completed_ops.load(Ordering::Relaxed);
    let test_duration = measurement_start.elapsed();
    let throughput = total_ops as f64 / test_duration.as_secs_f64();
    
    println!("\n🎯 Benchmark Results:");
    println!("   Operations: {}", total_ops);
    println!("   Successful: {}", success_count);
    println!("   Errors: {}", error_count);
    println!("   Duration: {:.2}s", test_duration.as_secs_f64());
    println!("   Throughput: {:.2} ops/sec", throughput);
    println!("   Workers: {}", args.workers);
    
    if histogram.len() > 0 {
        println!("\n📊 Latency Distribution (μs):");
        println!("   Min: {}", histogram.min());
        println!("   Mean: {:.2}", histogram.mean());
        println!("   P50: {}", histogram.value_at_quantile(0.5));
        println!("   P95: {}", histogram.value_at_quantile(0.95));
        println!("   P99: {}", histogram.value_at_quantile(0.99));
        println!("   Max: {}", histogram.max());
    }
    
    Ok(())
}

async fn run_protocol_comparison(_args: &Args) -> Result<()> {
    // This function was for comparing HTTP vs gRPC - not needed in Phase 0
    println!("Protocol comparison not available in Phase 0 (HTTP-only)");
    Ok(())
}
