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
    /// total read seconds
    #[clap(long, default_value_t = 30)]
    read_seconds: u64,
}

#[tokio::main]
async fn main() {
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

    // Build URLs based on selected endpoint
    let make_url = |base: &str, endpoint: &str, key: u64| -> String {
        match endpoint {
            "lookup_fast" => format!("{}/lookup_fast/{}", base, key),
            "lookup_raw" => format!("{}/lookup_raw?key={}", base, key),
            _ => format!("{}/lookup?key={}", base, key),
        }
    };

    // Spawn read workers
    let mut tasks = Vec::new();
    for _ in 0..args.read_concurrency {
        let client = client.clone();
        let base = args.base.clone();
        let endpoint = args.endpoint.clone();
        let load_n = args.load_n;
        let secs = args.read_seconds;
        tasks.push(tokio::spawn(async move {
            use rand::{rngs::StdRng, Rng, SeedableRng};
            let start = std::time::Instant::now();
            let mut count = 0u64;
            // Each task owns its own RNG (Send)
            let mut rng = StdRng::seed_from_u64(
                0xDEADBEEF ^ (std::time::Instant::now().elapsed().as_nanos() as u64),
            );
            while start.elapsed().as_secs() < secs {
                let k = rng.gen_range(0..load_n);
                let url = make_url(&base, &endpoint, k);
                let _ = client.get(url).send().await;
                count += 1;
            }
            count
        }));
    }

    let totals = futures::future::join_all(tasks).await;
    let total: u64 = totals.into_iter().filter_map(|r| r.ok()).sum();
    println!(
        "reads_total={}, rps={}",
        total,
        total as f64 / args.read_seconds as f64
    );
}
