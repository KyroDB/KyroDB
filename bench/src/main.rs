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
        join.spawn(async move {
            let _g = permit;
            let body = serde_json::json!({"key": i as u64, "value": String::from_utf8_lossy(&val)});
            let _ = client
                .post(format!("{}/put", base))
                .json(&body)
                .send()
                .await;
            pb.inc(1);
        });
    }
    while let Some(_res) = join.join_next().await {}
    pb.finish_with_message("load done");

    // 2) Trigger RMI build
    println!("[build] POST /rmi/build");
    let _ = client.post(format!("{}/rmi/build", args.base)).send().await?;

    // 3) Read-heavy workload and measure latency
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
            Some(z) => (z.sample(&mut rng) - 1) as u64, // Zipf 1..N
            None => rng.gen_range(0..args.load_n as u64),
        };
        set.spawn(async move {
            let _g = permit;
            let t0 = Instant::now();
            let url = format!("{}/lookup?key={}", base, key);
            let _ = client.get(url).send().await;
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
    let text = client.get(format!("{}/metrics", args.base)).send().await?.text().await?;
    let wal_bytes = grep_metric(&text, "kyrodb_wal_size_bytes");
    let snap_bytes = grep_metric(&text, "kyrodb_snapshot_size_bytes");
    let hit_rate = grep_metric(&text, "kyrodb_rmi_hit_rate");
    println!("wal_bytes={} snapshot_bytes={} rmi_hit_rate={}", wal_bytes, snap_bytes, hit_rate);

    Ok(())
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
