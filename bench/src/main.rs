use anyhow::Result;
use clap::Parser;
use hdrhistogram::Histogram;
use indicatif::{ProgressBar, ProgressStyle};
use rand::{rngs::StdRng, Rng, SeedableRng};
use rand_distr::{Distribution, Zipf};
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

    /// Data distribution pattern
    #[arg(long, default_value = "uniform")]
    distribution: String,

    /// Test mode: read, write, or mixed
    #[arg(long, default_value = "mixed")]
    test_mode: String,

    /// Read percentage for mixed workload (0-100)
    #[arg(long, default_value = "80")]
    read_percentage: u8,

    /// Enable RMI index build before test
    #[arg(long, action)]
    enable_rmi: bool,

    /// Benchmark name for reporting
    #[arg(long, default_value = "kyrodb_benchmark")]
    benchmark_name: String,

    /// Output format: json, csv, or console
    #[arg(long, default_value = "console")]
    output_format: String,

    /// Minimum value size (bytes)
    #[arg(long, default_value = "64")]
    min_value_size: usize,

    /// Maximum value size (bytes)  
    #[arg(long, default_value = "1024")]
    max_value_size: usize,
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
    
    if args.read_percentage > 100 {
        anyhow::bail!("Read percentage must be between 0 and 100");
    }

    // Create benchmark client
    let mut client = BenchClient::new(
        &args.protocol,
        &args.base,
        "",  // No gRPC address needed
        args.auth_token.clone(),
    ).await?;

    // Pre-benchmark setup
    if args.enable_rmi {
        println!("🧠 Building RMI index for optimal performance...");
        build_rmi_index(&mut client, &args).await?;
        println!("✅ RMI index built successfully");
    }

    // Warmup server
    println!("🔥 Warming up server...");
    warmup_server(&mut client, &args).await?;
    println!("✅ Server warmed up");

    match args.protocol.as_str() {
        "http" => {
            match args.test_mode.as_str() {
                "write" => run_write_only_test(&mut client, &args).await?,
                "read" => run_read_only_test(&mut client, &args).await?,
                "mixed" => {
                    if args.streaming {
                        println!("🚀 Using HTTP batch operations for maximum throughput");
                        run_streaming_benchmark(&mut client, &args).await?;
                    } else {
                        println!("🚀 Using HTTP point operations");
                        run_workload(&mut client, &args).await?;
                    }
                }
                _ => anyhow::bail!("Invalid test mode. Use: read, write, or mixed"),
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
            let key = generate_key(&args_clone.key_prefix, &args_clone.distribution, 
                                 args_clone.key_count, &mut rng, args_clone.zipf_skew);
            
            let start = Instant::now();
            let result = if rng.gen_range(0..100) < args_clone.read_percentage {
                match client_clone.get(&key, None, &args_clone.base).await {
                    Ok(_) => Ok(()),
                    Err(e) => {
                        eprintln!("GET error for key {}: {}", key, e);
                        Err(e)
                    }
                }
            } else {
                let value_size = if args_clone.min_value_size == args_clone.max_value_size {
                    args_clone.value_size
                } else {
                    rng.gen_range(args_clone.min_value_size..=args_clone.max_value_size)
                };
                let value = "x".repeat(value_size);
                match client_clone.put(key, value, None, &args_clone.base).await {
                    Ok(_) => Ok(()),
                    Err(e) => {
                        eprintln!("PUT error: {}", e);
                        Err(e)
                    }
                }
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
    
    print_results("MIXED", &args, total_ops, success_count, error_count, &histogram, test_duration);
    
    Ok(())
}

async fn run_protocol_comparison(_args: &Args) -> Result<()> {
    // This function was for comparing HTTP vs gRPC - not needed in Phase 0
    println!("Protocol comparison not available in Phase 0 (HTTP-only)");
    Ok(())
}

async fn build_rmi_index(client: &mut BenchClient, args: &Args) -> Result<()> {
    let url = format!("{}/v1/rmi/build", args.base);
    let response = client.http.post(&url).send().await?;
    
    if !response.status().is_success() {
        anyhow::bail!("Failed to build RMI index: {}", response.status());
    }
    
    Ok(())
}

async fn warmup_server(client: &mut BenchClient, args: &Args) -> Result<()> {
    let url = format!("{}/v1/warmup", args.base);
    let response = client.http.post(&url).send().await?;
    
    if !response.status().is_success() {
        println!("⚠️ Warmup endpoint not available, skipping...");
    }
    
    // Do some basic operations to warm up the server
    let warmup_ops = 1000;
    for i in 0..warmup_ops {
        let key = i.to_string(); // Use numeric keys
        let value = "x".repeat(args.value_size);
        let _ = client.put(key.clone(), value, None, &args.base).await;
        if i % 100 == 0 {
            let _ = client.get(&key, None, &args.base).await;
        }
    }
    
    Ok(())
}

async fn run_write_only_test(client: &mut BenchClient, args: &Args) -> Result<()> {
    println!("✍️ Running write-only performance test");
    
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

    let test_end = Instant::now() + Duration::from_secs(args.duration);
    let measurement_start = Instant::now();
    
    let mut tasks = JoinSet::new();
    let mut rng = StdRng::seed_from_u64(42);
    
    while Instant::now() < test_end {
        let _permit = semaphore.clone().acquire_owned().await?;
        let mut client_clone = client.clone();
        let completed = completed_ops.clone();
        let args_clone = args.clone();
        
        // Generate variable value sizes if specified
        let value_size = if args.min_value_size == args.max_value_size {
            args.value_size
        } else {
            rng.gen_range(args.min_value_size..=args.max_value_size)
        };
        
        tasks.spawn(async move {
            let mut rng = StdRng::from_entropy();
            let key = generate_key(&args_clone.key_prefix, &args_clone.distribution, 
                                 args_clone.key_count, &mut rng, args_clone.zipf_skew);
            let value = "x".repeat(value_size);
            
            let start = Instant::now();
            let result = client_clone.put(key, value, None, &args_clone.base).await;
            let latency = start.elapsed();
            
            completed.fetch_add(1, Ordering::Relaxed);
            (result, latency.as_micros() as u64)
        });
        
        // Rate limiting
        if args.rate > 0 {
            let delay = Duration::from_micros(1_000_000 / args.rate);
            tokio::time::sleep(delay).await;
        }
        
        // Update progress
        let elapsed = measurement_start.elapsed().as_secs();
        if elapsed <= args.duration {
            progress_bar.set_position(elapsed);
        }
    }
    
    progress_bar.finish_with_message("Collecting write results...");
    
    // Collect results
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
    
    print_results("WRITE-ONLY", &args, completed_ops.load(Ordering::Relaxed), 
                  success_count, error_count, &histogram, measurement_start.elapsed());
    
    Ok(())
}

async fn run_read_only_test(client: &mut BenchClient, args: &Args) -> Result<()> {
    println!("📖 Running read-only performance test");
    
    // First, populate some data for reading
    println!("📝 Populating test data...");
    let populate_count = args.key_count.min(10000); // Limit population for faster test
    let mut rng = StdRng::seed_from_u64(42);
    
    for i in 0..populate_count {
        let key = i.to_string(); // Use numeric keys
        let value_size = if args.min_value_size == args.max_value_size {
            args.value_size
        } else {
            rng.gen_range(args.min_value_size..=args.max_value_size)
        };
        let value = "x".repeat(value_size);
        let _ = client.put(key, value, None, &args.base).await;
        
        if i % 1000 == 0 {
            print!(".");
        }
    }
    println!(" ✅ Data populated");
    
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

    let test_end = Instant::now() + Duration::from_secs(args.duration);
    let measurement_start = Instant::now();
    
    let mut tasks = JoinSet::new();
    
    while Instant::now() < test_end {
        let _permit = semaphore.clone().acquire_owned().await?;
        let mut client_clone = client.clone();
        let completed = completed_ops.clone();
        let args_clone = args.clone();
        
        tasks.spawn(async move {
            let mut rng = StdRng::from_entropy();
            let key = generate_key(&args_clone.key_prefix, &args_clone.distribution, 
                                 populate_count, &mut rng, args_clone.zipf_skew);
            
            let start = Instant::now();
            let result = client_clone.get(&key, None, &args_clone.base).await;
            let latency = start.elapsed();
            
            completed.fetch_add(1, Ordering::Relaxed);
            (result, latency.as_micros() as u64)
        });
        
        // Rate limiting
        if args.rate > 0 {
            let delay = Duration::from_micros(1_000_000 / args.rate);
            tokio::time::sleep(delay).await;
        }
        
        // Update progress
        let elapsed = measurement_start.elapsed().as_secs();
        if elapsed <= args.duration {
            progress_bar.set_position(elapsed);
        }
    }
    
    progress_bar.finish_with_message("Collecting read results...");
    
    // Collect results
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
    
    print_results("READ-ONLY", &args, completed_ops.load(Ordering::Relaxed), 
                  success_count, error_count, &histogram, measurement_start.elapsed());
    
    Ok(())
}

fn generate_key(_prefix: &str, distribution: &str, key_count: u64, rng: &mut StdRng, zipf_skew: f64) -> String {
    let key_id = match distribution {
        "uniform" => rng.gen_range(0..key_count),
        "zipf" => {
            let zipf = Zipf::new(key_count, zipf_skew).unwrap();
            zipf.sample(rng) as u64
        }
        "sequential" => {
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            COUNTER.fetch_add(1, Ordering::Relaxed) % key_count
        }
        _ => rng.gen_range(0..key_count),
    };
    
    // Return just the numeric key as string for KyroDB
    key_id.to_string()
}

fn print_results(
    test_type: &str,
    args: &Args,
    total_ops: u64,
    success_count: u64,
    error_count: u64,
    histogram: &Histogram<u64>,
    duration: Duration,
) {
    let throughput = total_ops as f64 / duration.as_secs_f64();
    
    match args.output_format.as_str() {
        "json" => {
            let result = serde_json::json!({
                "benchmark_name": args.benchmark_name,
                "test_type": test_type,
                "total_operations": total_ops,
                "successful_operations": success_count,
                "failed_operations": error_count,
                "duration_seconds": duration.as_secs_f64(),
                "throughput_ops_per_sec": throughput,
                "workers": args.workers,
                "latency_stats": {
                    "min_microseconds": histogram.min(),
                    "mean_microseconds": histogram.mean(),
                    "p50_microseconds": histogram.value_at_quantile(0.5),
                    "p95_microseconds": histogram.value_at_quantile(0.95),
                    "p99_microseconds": histogram.value_at_quantile(0.99),
                    "max_microseconds": histogram.max()
                },
                "config": {
                    "value_size": args.value_size,
                    "key_count": args.key_count,
                    "distribution": args.distribution,
                    "rmi_enabled": args.enable_rmi
                }
            });
            println!("{}", serde_json::to_string_pretty(&result).unwrap());
        }
        "csv" => {
            println!("{},{},{},{},{},{:.2},{:.2},{},{},{},{},{},{},{},{},{},{},{},{}",
                args.benchmark_name, test_type, total_ops, success_count, error_count,
                duration.as_secs_f64(), throughput, args.workers,
                histogram.min(), histogram.mean(), histogram.value_at_quantile(0.5),
                histogram.value_at_quantile(0.95), histogram.value_at_quantile(0.99),
                histogram.max(), args.value_size, args.key_count, args.distribution,
                args.enable_rmi, args.base);
        }
        _ => {
            println!("\n🎯 {} Benchmark Results:", test_type);
            println!("   Operations: {}", total_ops);
            println!("   Successful: {}", success_count);
            println!("   Errors: {}", error_count);
            println!("   Duration: {:.2}s", duration.as_secs_f64());
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
        }
    }
}
