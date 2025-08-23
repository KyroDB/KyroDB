use anyhow::Result;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use clap::Parser;
use hdrhistogram::Histogram;
use indicatif::{ProgressBar, ProgressStyle};
use rand::{rngs::StdRng, Rng, SeedableRng};
use rand_distr::{Distribution, Zipf};
use serde::Deserialize;
use std::{sync::Arc, time::{Duration, Instant}};
use tokio::{sync::Semaphore, task::JoinSet};

#[derive(Parser, Debug, Clone)]
struct Args {
    /// Engine base URL
    #[arg(long, default_value = "http://127.0.0.1:3030")]
    base: String,
    /// Optional bearer token for protected endpoints
    #[arg(long)]
    auth_token: Option<String>,
    /// Total keys to load
    #[arg(long, default_value_t = 1_000_0)]
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
}

#[derive(Deserialize)]
struct OffsetResp { offset: u64 }

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;

    // Helper to attach auth header if provided
    let auth_header = args.auth_token.as_ref().map(|t| format!("Bearer {}", t));

    // Wait for server health
    wait_for_health(&client, &args.base, auth_header.as_deref()).await?;

    // 1) Bulk load N keys
    println!("[load] inserting {} keys", args.load_n);
    let pb = ProgressBar::new(args.load_n as u64);
    pb.set_style(ProgressStyle::with_template("{spinner} {msg} {bar:40.cyan/blue} {pos}/{len}")?.progress_chars("##-"));
    let sem = Arc::new(Semaphore::new(args.load_concurrency));
    let mut join = JoinSet::new();
    for i in 0..args.load_n {
        let permit = sem.clone().acquire_owned().await?;
        let client = client.clone();
        let base = args.base.clone();
        let val_bytes = args.val_bytes;
        let auth = auth_header.clone();
        join.spawn(async move {
            let k = i as u64;
            let url = format!("{}/v1/put", base);
            let value = "A".repeat(val_bytes);
            let body = serde_json::json!({ "key": k, "value": value });
            let mut req = client.post(url).json(&body);
            if let Some(a) = auth.as_deref() { req = req.header("Authorization", a); }
            let _ = req.send().await;
            drop(permit);
        });
        pb.inc(1);
    }
    while let Some(res) = join.join_next().await { let _ = res; }
    pb.finish_with_message("load done");

    // 0) Build RMI then warmup to avoid cold-start artifacts before measuring
    if let Err(e) = prebuild_and_warm(&client, &args.base, auth_header.as_deref()).await {
        eprintln!("warn: prebuild/warm step failed: {} (continuing)", e);
    }

    // 2) Read-heavy workload and measure latency (get_fast)
    println!("[read] running {}s, {} concurrency, dist={}", args.read_seconds, args.read_concurrency, args.dist);
    let deadline = Instant::now() + Duration::from_secs(args.read_seconds);
    let hist = Arc::new(tokio::sync::Mutex::new(Histogram::<u64>::new(3)?));
    let sem_r = Arc::new(Semaphore::new(args.read_concurrency));
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
        join.spawn(async move {
            let mut rng = StdRng::seed_from_u64(seed);
            let zipf = if dist == "zipf" {
                Some(Zipf::new(load_n as u64, zipf_theta).unwrap())
            } else { None };
            while Instant::now() < deadline {
                let k = if let Some(ref z) = zipf {
                    z.sample(&mut rng) as u64 - 1
                } else {
                    rng.gen_range(0..load_n as u64)
                };
                // Use fast value path to include value fetch cost realistically
                let url = format!("{}/v1/get_fast/{}", base, k);
                let mut req = client.get(url);
                if let Some(a) = auth_header.as_deref() { req = req.header("Authorization", a); }
                let t0 = Instant::now();
                let _ = req.send().await;
                let dt = t0.elapsed();
                total.fetch_add(1, Ordering::Relaxed);
                let mut h = hist.lock().await;
                let _ = h.record(dt.as_nanos() as u64);
            }
        });
    }
    while let Some(res) = join.join_next().await { let _ = res; }
    let total_reads = total.load(Ordering::Relaxed);
    let elapsed = args.read_seconds as f64;
    let rps = total_reads as f64 / elapsed;

    // Export CSV with p50/p95/p99 (microseconds) and optional RMI metrics
    let hist = hist.lock().await;
    let p50 = hist.value_at_quantile(0.50) as f64 / 1000.0;
    let p95 = hist.value_at_quantile(0.95) as f64 / 1000.0;
    let p99 = hist.value_at_quantile(0.99) as f64 / 1000.0;

    // Scrape /metrics to compute avg RMI lookup latency and avg probe len if present
    let mut rmi_lookup_avg_us: Option<f64> = None;
    let mut rmi_probe_len_avg: Option<f64> = None;
    if let Ok(resp) = client.get(format!("{}/metrics", args.base)).send().await {
        if let Ok(text) = resp.text().await {
            let mut sums: HashMap<&str, f64> = HashMap::new();
            let mut counts: HashMap<&str, f64> = HashMap::new();
            for line in text.lines() {
                if line.starts_with("kyrodb_rmi_lookup_latency_seconds_sum") {
                    if let Some(v) = line.split_whitespace().nth(1) { if let Ok(x) = v.parse::<f64>() { sums.insert("lookup", x); } }
                } else if line.starts_with("kyrodb_rmi_lookup_latency_seconds_count") {
                    if let Some(v) = line.split_whitespace().nth(1) { if let Ok(x) = v.parse::<f64>() { counts.insert("lookup", x); } }
                } else if line.starts_with("kyrodb_rmi_probe_len_sum") {
                    if let Some(v) = line.split_whitespace().nth(1) { if let Ok(x) = v.parse::<f64>() { sums.insert("probe", x); } }
                } else if line.starts_with("kyrodb_rmi_probe_len_count") {
                    if let Some(v) = line.split_whitespace().nth(1) { if let Ok(x) = v.parse::<f64>() { counts.insert("probe", x); } }
                }
            }
            if let (Some(s), Some(c)) = (sums.get("lookup"), counts.get("lookup")) {
                if *c > 0.0 { rmi_lookup_avg_us = Some((*s / *c) * 1_000_000.0); }
            }
            if let (Some(s), Some(c)) = (sums.get("probe"), counts.get("probe")) {
                if *c > 0.0 { rmi_probe_len_avg = Some(*s / *c); }
            }
        }
    }

    let mut csv = String::from("base,dist,reads_total,rps,p50_us,p95_us,p99_us,rmi_lookup_avg_us,rmi_probe_len_avg\n");
    csv.push_str(&format!(
        "{},{},{},{:.2},{:.1},{:.1},{:.1},{},{}\n",
        args.base,
        args.dist,
        total_reads,
        rps,
        p50,
        p95,
        p99,
        rmi_lookup_avg_us.map(|v| format!("{:.1}", v)).unwrap_or_else(|| "".into()),
        rmi_probe_len_avg.map(|v| format!("{:.2}", v)).unwrap_or_else(|| "".into()),
    ));
    let _ = std::fs::write(&args.out_csv, csv);

    println!(
        "reads_total={}, rps={:.2}, p50_us={:.1}, p95_us={:.1}, p99_us={:.1}{}{}",
        total_reads,
        rps,
        p50,
        p95,
        p99,
        rmi_lookup_avg_us.map(|v| format!(", rmi_lookup_avg_us={:.1}", v)).unwrap_or_default(),
        rmi_probe_len_avg.map(|v| format!(", rmi_probe_len_avg={:.2}", v)).unwrap_or_default(),
    );

    Ok(())
}

// Wait for /health endpoint to return 200
async fn wait_for_health(client: &reqwest::Client, base: &str, auth: Option<&str>) -> Result<()> {
    let url = format!("{}/health", base);
    for _ in 0..30 {
        let mut req = client.get(&url);
        if let Some(a) = auth { req = req.header("Authorization", a); }
        if let Ok(resp) = req.send().await {
            if resp.status().is_success() { return Ok(()); }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    anyhow::bail!("server not healthy at {}", url)
}

// Prebuild RMI and warm up server for realistic benchmarking
async fn prebuild_and_warm(client: &reqwest::Client, base: &str, auth: Option<&str>) -> Result<()> {
    // Try to build RMI index
    {
        let url = format!("{}/v1/rmi/build", base);
        let mut req = client.post(&url);
        if let Some(a) = auth { req = req.header("Authorization", a); }
        let _ = req.send().await?; // ignore body; server swaps if ok
    }
    // Warmup
    {
        let url = format!("{}/v1/warmup", base);
        let mut req = client.post(&url);
        if let Some(a) = auth { req = req.header("Authorization", a); }
        let _ = req.send().await?;
    }
    Ok(())
}
