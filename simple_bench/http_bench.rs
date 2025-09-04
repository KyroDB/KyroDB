use anyhow::Result;
use clap::Parser;
use hdrhistogram::Histogram;
use indicatif::{ProgressBar, ProgressStyle};
use rand::distributions::Uniform;
use rand::prelude::*;
use rand_distr::Zipf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinSet;

#[derive(Parser)]
#[command(name = "http_bench")]
#[command(about = "KyroDB HTTP benchmark client")]
struct Args {
    /// HTTP Base URL (e.g., http://localhost:9000)
    #[arg(long, default_value = "http://localhost:9000")]
    base: String,

    /// Number of key-value pairs to load initially
    #[arg(long, default_value_t = 10000)]
    load_n: usize,

    /// Number of seconds to run read benchmark
    #[arg(long, default_value_t = 30)]
    read_seconds: u64,

    /// Read concurrency level
    #[arg(long, default_value_t = 10)]
    read_concurrency: usize,

    /// Distribution for read keys: uniform or zipf
    #[arg(long, default_value = "uniform")]
    dist: String,

    /// Zipf theta parameter (for zipf distribution)
    #[arg(long, default_value_t = 1.0)]
    zipf_theta: f64,

    /// Random seed
    #[arg(long, default_value_t = 42)]
    seed: u64,

    /// Auth token
    #[arg(long)]
    auth_token: Option<String>,

    /// Output CSV file
    #[arg(long, default_value = "bench_results.csv")]
    out_csv: String,

    /// Regime: warm or cold (whether to prebuild RMI index)
    #[arg(long, default_value = "warm")]
    regime: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    
    println!("🚀 Starting KyroDB HTTP benchmark...");
    println!("   Base URL: {}", args.base);
    println!("   Load phase: {} keys", args.load_n);
    println!("   Read phase: {}s with {} concurrency", args.read_seconds, args.read_concurrency);
    println!("   Distribution: {}", args.dist);
    
    let client = reqwest::Client::new();
    let auth_header = args.auth_token.as_ref().map(|t| format!("Bearer {}", t));

    // Wait for server health
    wait_for_health(&client, &args.base, auth_header.as_deref()).await?;

    // 1) Load phase - populate with key-value pairs
    println!("\n📦 Loading {} key-value pairs...", args.load_n);
    let pb = ProgressBar::new(args.load_n as u64);
    pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({percent}%) {msg}")
        .unwrap());

    let load_start = Instant::now();
    let mut join = JoinSet::new();
    
    for i in 0..args.load_n {
        let client = client.clone();
        let base = args.base.clone();
        let auth = auth_header.clone();
        
        join.spawn(async move {
            let key = i as u64;
            let value = vec![b'A'; 32]; // 32-byte value
            
            let url = format!("{}/v1/put/{}", base, key);
            let mut req = client.post(&url).json(&serde_json::json!({
                "value": base64::encode(&value)
            }));
            
            if let Some(a) = auth.as_deref() {
                req = req.header("Authorization", a);
            }
            
            req.send().await.map(|_| ())
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
    
    let load_duration = load_start.elapsed();
    println!("✅ Load phase complete in {:.2}s", load_duration.as_secs_f64());

    // 2) Build RMI and warmup for realistic benchmarking  
    if args.regime.eq_ignore_ascii_case("warm") {
        println!("🔥 Warming up server (RMI build + cache warmup)");
        if let Err(e) = prebuild_and_warm(&client, &args.base, auth_header.as_deref()).await {
            eprintln!("warn: warmup failed: {} (continuing)", e);
        }
    }

    // 3) Read-heavy workload and measure latency
    println!("\n🔍 Running read benchmark: {}s, {} threads, {} distribution", 
             args.read_seconds, args.read_concurrency, args.dist);
    
    let deadline = Instant::now() + Duration::from_secs(args.read_seconds);
    let hist = Arc::new(tokio::sync::Mutex::new(Histogram::<u64>::new(3)?));
    let total = Arc::new(AtomicU64::new(0));
    let mut join = JoinSet::new();
    
    for i in 0..args.read_concurrency {
        let client = client.clone();
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
                
                // Use get_fast endpoint for best performance
                let url = format!("{}/v1/get_fast/{}", base, k);
                let mut req = client.get(&url);
                if let Some(a) = auth.as_deref() {
                    req = req.header("Authorization", a);
                }
                
                let t0 = Instant::now();
                let _ = req.send().await;
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
    let p50 = hist.value_at_quantile(0.50) as f64 / 1000.0; // Convert to microseconds
    let p95 = hist.value_at_quantile(0.95) as f64 / 1000.0;
    let p99 = hist.value_at_quantile(0.99) as f64 / 1000.0;
    let p999 = hist.value_at_quantile(0.999) as f64 / 1000.0;

    // Export CSV results
    let mut csv = String::from("base,dist,regime,reads_total,rps,p50_us,p95_us,p99_us,p999_us\n");
    csv.push_str(&format!(
        "{},{},{},{},{:.2},{:.1},{:.1},{:.1},{:.1}\n",
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

    println!("\n📊 Benchmark Results:");
    println!("   🔥 Total reads: {}", total_reads);
    println!("   ⚡ Throughput: {:.2} ops/sec", rps);
    println!("   📈 Latency percentiles (μs):");
    println!("      p50: {:.1}", p50);
    println!("      p95: {:.1}", p95);
    println!("      p99: {:.1}", p99);
    println!("      p999: {:.1}", p999);
    println!("\n✅ Results saved to: {}", args.out_csv);

    Ok(())
}

// Wait for server health endpoint
async fn wait_for_health(client: &reqwest::Client, base: &str, auth: Option<&str>) -> Result<()> {
    let url = format!("{}/health", base);
    for _ in 0..30 {
        let mut req = client.get(&url);
        if let Some(a) = auth {
            req = req.header("Authorization", a);
        }
        if let Ok(resp) = req.send().await {
            if resp.status().is_success() {
                println!("✅ Server is healthy");
                return Ok(());
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    anyhow::bail!("Server did not become healthy in time")
}

// Prebuild RMI and warm up server for realistic benchmarking
async fn prebuild_and_warm(client: &reqwest::Client, base: &str, auth: Option<&str>) -> Result<()> {
    // Take a snapshot so snapshot.data exists and mmap index is built (fast get by offset)
    {
        let url = format!("{}/v1/snapshot", base);
        let mut req = client.post(&url);
        if let Some(a) = auth {
            req = req.header("Authorization", a);
        }
        let _ = req.send().await?;
    }
    // Build RMI index (optional if already built)
    {
        let url = format!("{}/v1/rmi/build", base);
        let mut req = client.post(&url);
        if let Some(a) = auth {
            req = req.header("Authorization", a);
        }
        let _ = req.send().await?;
    }
    // Warmup
    {
        let url = format!("{}/v1/warmup", base);
        let mut req = client.post(&url);
        if let Some(a) = auth {
            req = req.header("Authorization", a);
        }
        let _ = req.send().await?;
    }
    println!("✅ Warmup complete");
    Ok(())
}
