use base64::Engine;
use clap::Parser; // bring Engine trait into scope

#[derive(Parser, Debug)]
struct Args {
    /// Base URL, e.g. http://127.0.0.1:3030
    #[clap(long, default_value = "http://127.0.0.1:3030")]
    base: String,
    /// endpoint: lookup|lookup_raw|lookup_fast
    #[clap(long, default_value = "lookup_fast")]
    endpoint: String,
    /// number of keys to load
    #[clap(long, default_value_t = 100_000)]
    load_n: u64,
    /// payload size (bytes)
    #[clap(long, default_value_t = 64)]
    val_bytes: usize,
    /// read workers
    #[clap(long, default_value_t = 64)]
    read_concurrency: usize,
    /// total read seconds (measurement period)
    #[clap(long, default_value_t = 30)]
    read_seconds: u64,
    /// warmup seconds (excluded from metrics)
    #[clap(long, default_value_t = 5)]
    warmup_seconds: u64,
    /// key distribution: uniform|zipf
    #[clap(long, default_value = "uniform")]
    dist: String,
    /// Zipf exponent (s), used when dist=zipf
    #[clap(long, default_value_t = 1.1)]
    zipf_s: f64,
    /// Output CSV path for latency percentiles
    #[clap(long, default_value = "bench_latency.csv")]
    csv_out: String,
    /// Build RMI before reads (POST /rmi/build)
    #[clap(long, default_value_t = true)]
    build_rmi: bool,
}

#[tokio::main]
async fn main() {
    use hdrhistogram::Histogram;
    use rand::{rngs::StdRng, Rng, SeedableRng};
    use rand_distr::{Distribution, Zipf};
    use std::collections::HashMap;
    use std::sync::{Arc, atomic::{AtomicU64, Ordering}};
    use tokio::time::{Duration, Instant};

    let args = Args::parse();
    let client = reqwest::Client::new();

    // Load keys
    let engine = base64::engine::general_purpose::STANDARD;
    for k in 0..args.load_n {
        let v = vec![0u8; args.val_bytes];
        let body = serde_json::json!({"key": k, "value": engine.encode(v)});
        let _ = client
            .post(format!("{}/put", args.base))
            .json(&body)
            .send()
            .await;
    }

    // Optionally trigger RMI rebuild before reads
    if args.build_rmi {
        let url = format!("{}/rmi/build", args.base);
        match client.post(&url).send().await {
            Ok(resp) => {
                if resp.status().as_u16() == 501 {
                    eprintln!("RMI build endpoint not available (server not built with learned-index feature) - aborting.");
                    return;
                }
                if !resp.status().is_success() {
                    eprintln!("RMI build request failed: {}", resp.status());
                } else if let Ok(txt) = resp.text().await {
                    println!("rmi_build_response={}", txt);
                }
            }
            Err(e) => {
                eprintln!("Failed to call /rmi/build: {}", e);
            }
        }
    }

    // Build URLs based on selected endpoint
    let make_url = |base: &str, endpoint: &str, key: u64| -> String {
        match endpoint {
            "lookup_fast" => format!("{}/lookup_fast/{}", base, key),
            "lookup_raw" => format!("{}/lookup_raw?key={}", base, key),
            _ => format!("{}/lookup?key={}", base, key),
        }
    };

    // Warmup
    let warmup_deadline = Instant::now() + Duration::from_secs(args.warmup_seconds);
    let mut warm_tasks = Vec::new();
    for i in 0..args.read_concurrency {
        let client = client.clone();
        let base = args.base.clone();
        let endpoint = args.endpoint.clone();
        let dist = args.dist.clone();
        let load_n = args.load_n as u64;
        let zipf_s = args.zipf_s;
        warm_tasks.push(tokio::spawn(async move {
            let mut rng = StdRng::seed_from_u64(0xC0FFEE ^ (i as u64));
            let zipf = if dist == "zipf" {
                Zipf::new(load_n.max(1), zipf_s).ok()
            } else { None };
            while Instant::now() < warmup_deadline {
                let k = if let Some(ref z) = zipf { z.sample(&mut rng) as u64 - 1 } else { rng.gen_range(0..load_n) };
                let url = make_url(&base, &endpoint, k);
                let _ = client.get(url).send().await;
            }
        }));
    }
    let _ = futures::future::join_all(warm_tasks).await;

    // Measurement
    let deadline = Instant::now() + Duration::from_secs(args.read_seconds);
    let hist = Arc::new(tokio::sync::Mutex::new(Histogram::<u64>::new_with_max(5_000_000_000, 3).unwrap()));
    let total = Arc::new(AtomicU64::new(0));

    let mut tasks = Vec::new();
    for i in 0..args.read_concurrency {
        let client = client.clone();
        let base = args.base.clone();
        let endpoint = args.endpoint.clone();
        let deadline = deadline.clone();
        let hist = hist.clone();
        let total = total.clone();
        let dist = args.dist.clone();
        let load_n = args.load_n as u64;
        let zipf_s = args.zipf_s;
        tasks.push(tokio::spawn(async move {
            let mut rng = StdRng::seed_from_u64(0xDEADBEEF ^ (i as u64));
            let zipf = if dist == "zipf" {
                Zipf::new(load_n.max(1), zipf_s).ok()
            } else { None };
            while Instant::now() < deadline {
                let k = if let Some(ref z) = zipf { z.sample(&mut rng) as u64 - 1 } else { rng.gen_range(0..load_n) };
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
