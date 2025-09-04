use anyhow::Result;
use clap::Parser;
use hdrhistogram::Histogram;
use indicatif::{ProgressBar, ProgressStyle};
use rand::{rngs::StdRng, Rng, SeedableRng};
use rand_distr::{Distribution, Zipf};
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{sync::Semaphore, task::JoinSet};

mod grpc_client;
mod client;
use grpc_client::GrpcBenchClient;
use client::BenchClient;

#[derive(Parser, Debug, Clone)]
struct Args {
    /// Engine base URL for HTTP
    #[arg(long, default_value = "http://127.0.0.1:3030")]
    base: String,
    
    /// gRPC endpoint address  
    #[arg(long, default_value = "http://127.0.0.1:50051")]
    grpc_addr: String,
    
    /// Protocol to use: http, grpc, or both
    #[arg(long, default_value = "both")]
    protocol: String,
    
    /// Optional bearer token for protected endpoints
    #[arg(long)]
    auth_token: Option<String>,
    /// Total keys to load
    #[arg(long, default_value_t = 10_000)]
    load_n: usize,
    /// Value size bytes
    #[arg(long, default_value_t = 32)]
    val_bytes: usize,
    /// Concurrency for load phase
    #[arg(long, default_value_t = 32)]
    load_concurrency: usize,
    /// Concurrency for read phase
    #[arg(long, default_value_t = 64)]
    read_concurrency: usize,
    /// Duration for read phase (seconds)
    #[arg(long, default_value_t = 30)]
    read_seconds: u64,
    /// Distribution: uniform|zipf
    #[arg(long, default_value = "uniform")]
    dist: String,
    /// Zipf exponent (theta)
    #[arg(long, default_value_t = 1.1)]
    zipf_theta: f64,
    /// Seed for RNG
    #[arg(long, default_value_t = 42)]
    seed: u64,
    /// CSV output path for latency histogram percentiles
    #[arg(long, default_value = "bench_results.csv")]
    out_csv: String,
    /// Label for regime: warm|cold (used in outputs); when 'warm', prebuild+warm runs
    #[arg(long, default_value = "warm")]
    regime: String,
    
    /// Use streaming operations for batch workloads (gRPC only)
    #[arg(long)]
    use_streaming: bool,
    
    /// Batch size for streaming operations
    #[arg(long, default_value_t = 1000)]
    stream_batch_size: usize,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct OffsetResp {
    offset: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    
    // Create client based on protocol selection
    let mut client = BenchClient::new(
        &args.protocol,
        &args.base,
        &args.grpc_addr,
        args.auth_token.clone()
    ).await?;

    // Helper to attach auth header if provided
    let auth_header = args.auth_token.as_ref().map(|t| format!("Bearer {}", t));

    // Wait for server health
    wait_for_health(&mut client, &args.base, auth_header.as_deref()).await?;

    // Protocol-specific optimizations
    match args.protocol.as_str() {
        "grpc" | "both" => {
            #[cfg(feature = "grpc")]
            if args.use_streaming {
                println!("🚀 Using gRPC streaming for maximum throughput");
                run_streaming_benchmark(&mut client, &args).await?;
            } else {
                run_standard_benchmark(&mut client, &args).await?;
            }

            #[cfg(not(feature = "grpc"))]
            {
                println!("⚠️ gRPC streaming requested but gRPC feature not enabled");
                run_standard_benchmark(&mut client, &args).await?;
            }
        }
        "http" => {
            println!("🌐 Using HTTP/JSON protocol");
            run_standard_benchmark(&mut client, &args).await?;
        }
        _ => unreachable!(),
    }

    Ok(())
}

#[cfg(feature = "grpc")]
async fn run_streaming_benchmark(client: &mut BenchClient, args: &Args) -> Result<()> {
    println!("[streaming] Preparing {} batches of {} items each", 
             args.load_n / args.stream_batch_size, args.stream_batch_size);

    // Prepare batches
    let batches: Vec<Vec<(u64, Vec<u8>)>> = (0..args.load_n)
        .collect::<Vec<_>>()
        .chunks(args.stream_batch_size)
        .map(|chunk| {
            chunk
                .iter()
                .map(|&i| (i as u64, "A".repeat(args.val_bytes).into_bytes()))
                .collect()
        })
        .collect();

    let start = Instant::now();
    let offsets = client.batch_put_stream(batches).await?;
    let duration = start.elapsed();

    println!("✅ Streaming benchmark complete:");
    println!("   📊 Inserted {} keys in {:.2}ms", offsets.len(), duration.as_secs_f64() * 1000.0);
    println!("   📈 Throughput: {:.0} ops/sec", args.load_n as f64 / duration.as_secs_f64());

    Ok(())
}

#[cfg(not(feature = "grpc"))]
async fn run_streaming_benchmark(_client: &mut BenchClient, _args: &Args) -> Result<()> {
    anyhow::bail!("Streaming benchmark requires gRPC feature")
}

async fn run_standard_benchmark(client: &mut BenchClient, args: &Args) -> Result<()> {
    let auth_header = args.auth_token.as_ref().map(|t| format!("Bearer {}", t));

    // 1) Bulk load N keys
    println!("[load] inserting {} keys", args.load_n);
    let pb = ProgressBar::new(args.load_n as u64);
    pb.set_style(
        ProgressStyle::with_template("{spinner} {msg} {bar:40.cyan/blue} {pos}/{len}")?
            .progress_chars("##-"),
    );
    let sem = Arc::new(Semaphore::new(args.load_concurrency));
    let mut join = JoinSet::new();
    for i in 0..args.load_n {
        let permit = sem.clone().acquire_owned().await?;
        let mut client_clone = client.clone();
        let base = args.base.clone();
        let val_bytes = args.val_bytes;
        let auth = auth_header.clone();
        join.spawn(async move {
            let k = i as u64;
            let value = "A".repeat(val_bytes).into_bytes();
            let _ = client_clone.put(k, value, auth.as_deref(), &base).await;
            drop(permit);
        });
        pb.inc(1);
    }
    
    while join.join_next().await.is_some() {}
    pb.finish_with_message("Load complete");

    // Continue with the rest of the standard benchmark...
    run_read_benchmark(client, args).await
}

async fn run_read_benchmark(client: &mut BenchClient, args: &Args) -> Result<()> {
    let auth_header = args.auth_token.as_ref().map(|t| format!("Bearer {}", t));
    
    // Run performance comparison if using both protocols
    if args.protocol == "both" {
        run_protocol_comparison(args).await?;
    }

    // 3) Read-heavy workload and measure latency with spike detection
    println!(
        "[read] running {}s, {} concurrency, dist={}",
        args.read_seconds, args.read_concurrency, args.dist
    );
    
    let deadline = Instant::now() + Duration::from_secs(args.read_seconds);
    let hist = Arc::new(tokio::sync::Mutex::new(Histogram::<u64>::new(3)?));
    let total = Arc::new(AtomicU64::new(0));
    let spike_count = Arc::new(AtomicU64::new(0));
    let spike_threshold = Duration::from_millis(1);  // Flag requests >1ms as spikes
    let mut join = JoinSet::new();
    
    for i in 0..args.read_concurrency {
        let mut client = client.clone();
        let base = args.base.clone();
        let hist = hist.clone();
        let total = total.clone();
        let spike_count = spike_count.clone();
        let dist = args.dist.clone();
        let load_n = args.load_n;
        let zipf_theta = args.zipf_theta;
        let seed = args.seed ^ (i as u64);
        let auth = auth_header.clone();
        
        join.spawn(async move {
            let mut rng = StdRng::seed_from_u64(seed);
            let zipf = if dist == "zipf" {
                Some(Zipf::new(load_n as u64, zipf_theta).unwrap())
            } else {
                None
            };
            
            while Instant::now() < deadline {
                let k = if let Some(ref z) = zipf {
                    z.sample(&mut rng) as u64 - 1
                } else {
                    rng.gen_range(0..load_n as u64)
                };
                
                let t0 = Instant::now();
                let _ = client.get_fast(k, auth.as_deref(), &base).await;
                let dt = t0.elapsed();
                
                total.fetch_add(1, Ordering::Relaxed);
                let mut h = hist.lock().await;
                let _ = h.record(dt.as_nanos() as u64);
                
                // Spike detection
                if dt > spike_threshold {
                    spike_count.fetch_add(1, Ordering::Relaxed);
                    if spike_count.load(Ordering::Relaxed) < 10 {  // Log first 10 spikes
                        eprintln!("Spike detected: {:.2}ms for key {}", dt.as_secs_f64() * 1000.0, k);
                    }
                }
            }
        });
    }
    
    while let Some(res) = join.join_next().await {
        let _ = res;
    }
    
    let total_reads = total.load(Ordering::Relaxed);
    let elapsed = args.read_seconds as f64;
    let rps = total_reads as f64 / elapsed;
    let total_spikes = spike_count.load(Ordering::Relaxed);

    // 4) Calculate latency percentiles and export results
    let hist = hist.lock().await;
    let p50 = hist.value_at_quantile(0.50) as f64 / 1000.0;
    let p95 = hist.value_at_quantile(0.95) as f64 / 1000.0;
    let p99 = hist.value_at_quantile(0.99) as f64 / 1000.0;
    let p999 = hist.value_at_quantile(0.999) as f64 / 1000.0;

    // Export CSV results
    let mut csv = String::from("protocol,base,dist,regime,reads_total,rps,p50_us,p95_us,p99_us,p999_us,spikes\n");
    csv.push_str(&format!(
        "{},{},{},{},{},{:.2},{:.1},{:.1},{:.1},{:.1},{}\n",
        args.protocol,
        args.base,
        args.dist,
        args.regime,
        total_reads,
        rps,
        p50,
        p95,
        p99,
        p999,
        total_spikes,
    ));
    let _ = std::fs::write(&args.out_csv, csv);

    println!(
        "📊 Results: protocol={}, reads_total={}, rps={:.2}, p50_us={:.1}, p95_us={:.1}, p99_us={:.1}, p999_us={:.1}, spikes={}",
        args.protocol,
        total_reads,
        rps,
        p50,
        p95,
        p99,
        p999,
        total_spikes,
    );

    Ok(())
}

// Protocol comparison benchmark
async fn run_protocol_comparison(args: &Args) -> Result<()> {
    println!("🔬 Running protocol comparison benchmark...");
    
    let test_iterations = 1000;
    
    // Test gRPC performance
    #[cfg(feature = "grpc")]
    {
        let mut grpc_client = BenchClient::new(
            "grpc",
            &args.base,
            &args.grpc_addr,
            args.auth_token.clone()
        ).await?;
        
        let grpc_start = Instant::now();
        for i in 0..test_iterations {
            let key = 300000 + i;
            let value = vec![b'A'; 32];
            let _ = grpc_client.put(key, value, None, &args.base).await;
        }
        let grpc_duration = grpc_start.elapsed();
        
        // Test HTTP performance
        let mut http_client = BenchClient::new(
            "http",
            &args.base,
            &args.grpc_addr,
            args.auth_token.clone()
        ).await?;
        
        let http_start = Instant::now();
        for i in 0..test_iterations {
            let key = 400000 + i;
            let value = vec![b'A'; 32];
            let _ = http_client.put(key, value, None, &args.base).await;
        }
        let http_duration = http_start.elapsed();
        
        println!("📊 Protocol Comparison Results:");
        println!("   ⚡ gRPC: {:.2}ms ({:.0} ops/sec)", 
                 grpc_duration.as_secs_f64() * 1000.0,
                 test_iterations as f64 / grpc_duration.as_secs_f64());
        println!("   🌐 HTTP: {:.2}ms ({:.0} ops/sec)", 
                 http_duration.as_secs_f64() * 1000.0,
                 test_iterations as f64 / http_duration.as_secs_f64());
        println!("   🚀 gRPC is {:.2}x faster than HTTP", 
                 http_duration.as_secs_f64() / grpc_duration.as_secs_f64());
    }
    
    #[cfg(not(feature = "grpc"))]
    println!("⚠️ Protocol comparison requires gRPC feature");

    Ok(())
}

// Wait for server health endpoint
async fn wait_for_health(client: &mut BenchClient, base_url: &str, auth_header: Option<&str>) -> Result<()> {
    for _ in 0..30 {
        if let Ok(_) = client.health_check(auth_header, base_url).await {
            println!("✅ Server is healthy");
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    anyhow::bail!("Server did not become healthy in time")
}

// Run the main benchmark workload using new protocol-agnostic client
async fn run_benchmark_workload(
    args: &Args,
    mut client: BenchClient,
    auth_header: Option<String>
) -> Result<()> {
    println!("🚀 Starting benchmark workload with {} protocol", args.protocol);
    
    // 1) Load phase - populate with key-value pairs
    println!("[load] inserting {} key-value pairs", args.load_n);
    let pb = ProgressBar::new(args.load_n as u64);
    pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({percent}%) {msg}")
        .unwrap());

    let mut join = JoinSet::new();
    for i in 0..args.load_n {
        let mut client = client.clone();
        let base = args.base.clone();
        let auth = auth_header.clone();
        
        join.spawn(async move {
            let key = i as u64;
            let value = vec![b'A'; 32]; // 32-byte value
            client.put(key, value, auth.as_deref(), &base).await
        });
        
        if join.len() >= 100 {
            let _ = join.join_next().await;
        }
        pb.inc(1);
    }
    
    while let Some(res) = join.join_next().await {
        let _ = res;
    }
    pb.finish_with_message("load done");

    // 2) Build RMI and warmup for realistic benchmarking
    if args.regime.eq_ignore_ascii_case("warm") {
        println!("🔥 Warming up server (RMI build + cache warmup)");
        if let Err(e) = client.warmup(auth_header.as_deref(), &args.base).await {
            eprintln!("warn: warmup failed: {} (continuing)", e);
        }
    } else {
        println!("info: regime='{}' — skipping warmup stage", args.regime);
    }

    // 3) Read-heavy workload and measure latency
    println!(
        "[read] running {}s, {} concurrency, dist={}",
        args.read_seconds, args.read_concurrency, args.dist
    );
    
    let deadline = Instant::now() + Duration::from_secs(args.read_seconds);
    let hist = Arc::new(tokio::sync::Mutex::new(Histogram::<u64>::new(3)?));
    let total = Arc::new(AtomicU64::new(0));
    let mut join = JoinSet::new();
    
    for i in 0..args.read_concurrency {
        let mut client = client.clone();
        let base = args.base.clone();
        let hist = hist.clone();
        let total = total.clone();
        let dist = args.dist.clone();
        let load_n = args.load_n;
        let zipf_theta = args.zipf_theta;
        let seed = args.seed ^ (i as u64);
        let auth = auth_header.clone();
        
        join.spawn(async move {
            let mut rng = StdRng::seed_from_u64(seed);
            let zipf = if dist == "zipf" {
                Some(Zipf::new(load_n as u64, zipf_theta).unwrap())
            } else {
                None
            };
            
            while Instant::now() < deadline {
                let k = if let Some(ref z) = zipf {
                    z.sample(&mut rng) as u64 - 1
                } else {
                    rng.gen_range(0..load_n as u64)
                };
                
                let t0 = Instant::now();
                let _ = client.get_fast(k, auth.as_deref(), &base).await;
                let dt = t0.elapsed();
                
                total.fetch_add(1, Ordering::Relaxed);
                let mut h = hist.lock().await;
                let _ = h.record(dt.as_nanos() as u64);
            }
        });
    }
    
    while let Some(res) = join.join_next().await {
        let _ = res;
    }
    
    let total_reads = total.load(Ordering::Relaxed);
    let elapsed = args.read_seconds as f64;
    let rps = total_reads as f64 / elapsed;

    // 4) Calculate latency percentiles and export results
    let hist = hist.lock().await;
    let p50 = hist.value_at_quantile(0.50) as f64 / 1000.0;
    let p95 = hist.value_at_quantile(0.95) as f64 / 1000.0;
    let p99 = hist.value_at_quantile(0.99) as f64 / 1000.0;
    let p999 = hist.value_at_quantile(0.999) as f64 / 1000.0;

    // Export CSV results
    let mut csv = String::from("protocol,base,dist,regime,reads_total,rps,p50_us,p95_us,p99_us,p999_us\n");
    csv.push_str(&format!(
        "{},{},{},{},{},{:.2},{:.1},{:.1},{:.1},{:.1}\n",
        args.protocol,
        args.base,
        args.dist,
        args.regime,
        total_reads,
        rps,
        p50,
        p95,
        p99,
        p999,
    ));
    let _ = std::fs::write(&args.out_csv, csv);

    println!(
        "📊 Results: protocol={}, reads_total={}, rps={:.2}, p50_us={:.1}, p95_us={:.1}, p99_us={:.1}, p999_us={:.1}",
        args.protocol,
        total_reads,
        rps,
        p50,
        p95,
        p99,
        p999,
    );

    Ok(())
}
