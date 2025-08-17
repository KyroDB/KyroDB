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
        let val = vec![b'x'; args.val_bytes];
        let auth = auth_header.clone();
        let pb2 = pb.clone();
        join.spawn(async move {
            let _g = permit;
            let body = serde_json::json!({"key": i as u64, "value": String::from_utf8_lossy(&val)});
            let mut req = client.post(format!("{}/put", base)).json(&body);
            if let Some(h) = auth.as_ref() { req = req.header("Authorization", h); }
            let _ = req.send().await;
            pb2.inc(1);
        });
    }
    while let Some(_res) = join.join_next().await {}
    pb.finish_with_message("load done");

    // 2) Trigger RMI build and wait until loaded
    println!("[build] POST /rmi/build");
    {
        let mut req = client.post(format!("{}/rmi/build", args.base));
        if let Some(h) = auth_header.as_ref() { req = req.header("Authorization", h); }
        if let Err(e) = req.send().await { eprintln!("warn: rmi/build failed: {}", e); }
    }
    wait_for_rmi_loaded(&client, &args.base, auth_header.as_deref()).await?;

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
        let h = hist.clone();
        let key = match &zipf {
            Some(z) => {
                let s = z.sample(&mut rng);
                if s < 1.0 { 0 } else { (s as u64).saturating_sub(1) }
            },
            None => rng.gen_range(0..args.load_n as u64),
        };
        let auth = auth_header.clone();
        set.spawn(async move {
            let _g = permit;
            let t0 = Instant::now();
            let url = format!("{}/lookup_raw?key={}", base, key);
            let mut req = client.get(url);
            if let Some(h) = auth.as_ref() { req = req.header("Authorization", h); }
            let _ = req.send().await;
            let us = t0.elapsed().as_micros() as u64;
            let mut lock = h.lock().await;
            let _ = lock.record(us);
        });
    }
    while let Some(_r) = set.join_next().await {}

    // 4) Export histogram percentiles + simple throughput
    let h = hist.lock().await;
    let p50 = h.value_at_quantile(0.50);
    let p95 = h.value_at_quantile(0.95);
    let p99 = h.value_at_quantile(0.99);
    println!("p50={}us p95={}us p99={}us", p50, p95, p99);

    let mut wtr = csv::Writer::from_path(&args.out_csv)?;
    wtr.write_record(["quantile", "latency_us"])?;
    for q in [0.50, 0.90, 0.95, 0.99, 0.999] {
        wtr.write_record(&[format!("{q}"), h.value_at_quantile(q).to_string()])?;
    }
    wtr.flush()?;

    // 5) Scrape a few metrics and append
    let mut req = client.get(format!("{}/metrics", args.base));
    if let Some(h) = auth_header.as_ref() { req = req.header("Authorization", h); }
    let text = req.send().await?.text().await?;
    let wal_bytes = grep_metric(&text, "kyrodb_wal_size_bytes");
    let snap_bytes = grep_metric(&text, "kyrodb_snapshot_size_bytes");
    let hit_rate = grep_metric(&text, "kyrodb_rmi_hit_rate");
    let probe_sum = grep_hist_sum_or_zero(&text, "kyrodb_rmi_probe_len");
    let probe_cnt = grep_hist_count_or_zero(&text, "kyrodb_rmi_probe_len");
    let probe_avg = if probe_cnt > 0.0 { probe_sum / probe_cnt } else { 0.0 };
    println!("wal_bytes={} snapshot_bytes={} rmi_hit_rate={} avg_probe_len={}", wal_bytes, snap_bytes, hit_rate, probe_avg);

    Ok(())
}

async fn wait_for_health(client: &reqwest::Client, base: &str, auth: Option<&str>) -> Result<()> {
    let mut tries = 0;
    loop {
        let mut req = client.get(format!("{}/health", base));
        if let Some(h) = auth { req = req.header("Authorization", h); }
        if let Ok(resp) = req.send().await { if resp.status().is_success() { return Ok(()); } }
        tries += 1;
        if tries > 120 { anyhow::bail!("server not healthy after timeout"); }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn wait_for_rmi_loaded(client: &reqwest::Client, base: &str, auth: Option<&str>) -> Result<()> {
    let mut tries = 0;
    loop {
        let mut req = client.get(format!("{}/metrics", base));
        if let Some(h) = auth { req = req.header("Authorization", h); }
        if let Ok(resp) = req.send().await { if let Ok(text) = resp.text().await {
            let leaves = grep_metric(&text, "kyrodb_rmi_index_leaves");
            if leaves > 0.0 { return Ok(()); }
        }}
        tries += 1;
        if tries > 240 { anyhow::bail!("rmi not loaded after timeout"); }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

fn grep_metric(text: &str, name: &str) -> f64 {
    for line in text.lines() {
        if line.starts_with(name) {
            if let Some(val) = line.split_whitespace().nth(1) {
                if let Ok(f) = val.parse::<f64>() { return f; }
            }
        }
    }
    0.0
}

fn grep_hist_sum_or_zero(text: &str, base: &str) -> f64 {
    let target = format!("{}_sum", base);
    for line in text.lines() {
        if line.starts_with(&target) {
            if let Some(val) = line.split_whitespace().nth(1) {
                if let Ok(f) = val.parse::<f64>() { return f; }
            }
        }
    }
    0.0
}

fn grep_hist_count_or_zero(text: &str, base: &str) -> f64 {
    let target = format!("{}_count", base);
    for line in text.lines() {
        if line.starts_with(&target) {
            if let Some(val) = line.split_whitespace().nth(1) {
                if let Ok(f) = val.parse::<f64>() { return f; }
            }
        }
    }
    0.0
}
