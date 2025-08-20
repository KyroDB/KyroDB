use anyhow::Result;
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
        let endpoint = args.endpoint.clone();
        let dist = args.dist.clone();
        let load_n = args.load_n as usize;
        let zipf_s = args.zipf_s;
        warm_tasks.push(tokio::spawn(async move {
            let mut rng = StdRng::seed_from_u64(0xC0FFEE ^ (i as u64));
            let zipf = if dist == "zipf" {
                Zipf::new(load_n.max(1), zipf_s).ok()
            } else { None };
            while Instant::now() < warmup_deadline {
                let k = if let Some(ref z) = zipf { z.sample(&mut rng) as u64 - 1 } else { rng.gen_range(0..load_n as u64) };
                let url = make_url(&base, &endpoint, k);
                let _ = client.get(url).send().await;
            }
        }));
    }
    let _ = futures::future::join_all(warm_tasks).await;

    // 3) Read-heavy workload and measure latency (lookup_raw)
    println!("[read] running {}s, {} concurrency, dist={}", args.read_seconds, args.read_concurrency, args.dist);
    let deadline = Instant::now() + Duration::from_secs(args.read_seconds);
    let hist = Arc::new(tokio::sync::Mutex::new(Histogram::<u64>::new(3)?));
    let sem_r = Arc::new(Semaphore::new(args.read_concurrency));
    let mut set = JoinSet::new();

    // key generators
    let mut rng = StdRng::seed_from_u64(args.seed);
    let zipf = if args.dist == "zipf" { Some(Zipf::new(args.load_n as u64, args.zipf_theta).unwrap()) } else { None };

    while Instant::now() < deadline {
        let permit = sem_r.clone().acquire_owned().await?;
        let client = client.clone();
        let base = args.base.clone();
        let endpoint = args.endpoint.clone();
        let deadline = deadline.clone();
        let hist = hist.clone();
        let total = total.clone();
        let dist = args.dist.clone();
        let load_n = args.load_n as usize;
        let zipf_s = args.zipf_s;
        tasks.push(tokio::spawn(async move {
            let mut rng = StdRng::seed_from_u64(0xDEADBEEF ^ (i as u64));
            let zipf = if dist == "zipf" {
                Zipf::new(load_n.max(1), zipf_s).ok()
            } else { None };
            while Instant::now() < deadline {
                let k = if let Some(ref z) = zipf { z.sample(&mut rng) as u64 - 1 } else { rng.gen_range(0..load_n as u64) };
                let url = make_url(&base, &endpoint, k);
                let t0 = Instant::now();
                let _ = client.get(url).send().await;
                let dt = t0.elapsed();
                total.fetch_add(1, Ordering::Relaxed);
                let mut h = hist.lock().await;
                let _ = h.record(dt.as_nanos() as u64);
            }
        }));
    }

    let _ = futures::future::join_all(tasks).await;
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
    if let Ok(text) = client.get(format!("{}/metrics", args.base)).send().await.and_then(|r| r.text()).await {
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

    let mut csv = String::from("endpoint,dist,reads_total,rps,p50_us,p95_us,p99_us,rmi_lookup_avg_us,rmi_probe_len_avg\n");
    csv.push_str(&format!(
        "{},{},{},{:.2},{:.1},{:.1},{:.1},{},{}\n",
        args.endpoint,
        args.dist,
        total_reads,
        rps,
        p50,
        p95,
        p99,
        rmi_lookup_avg_us.map(|v| format!("{:.1}", v)).unwrap_or_else(|| "".into()),
        rmi_probe_len_avg.map(|v| format!("{:.2}", v)).unwrap_or_else(|| "".into()),
    ));
    let _ = std::fs::write(&args.csv_out, csv);

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
}
