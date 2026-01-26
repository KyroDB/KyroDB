use anyhow::{anyhow, Context, Result};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;
use tokio::time::{sleep, Instant as TokioInstant, MissedTickBehavior};

pub mod kyrodb {
    tonic::include_proto!("kyrodb.v1");
}

use kyrodb::kyro_db_service_client::KyroDbServiceClient;
use kyrodb::{InsertRequest, SearchRequest};

#[derive(Debug, Clone)]
struct Config {
    server: String,
    target_qps: f64,
    duration: Duration,
    concurrency: usize,
    dataset_size: usize,
    dimension: usize,
}

#[derive(Default)]
struct WorkerStats {
    successes: u64,
    failures: u64,
    latencies_ns: Vec<u64>,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let config = parse_args()?;

    println!("Starting KyroDB load test");
    println!("  Target QPS: {:.1}", config.target_qps);
    println!("  Duration: {:?}", config.duration);
    println!("  Concurrency: {}", config.concurrency);
    println!("  Dataset size: {}", config.dataset_size);

    let dataset = Arc::new(build_dataset(config.dataset_size, config.dimension));

    seed_dataset(&config, dataset.clone()).await?;
    warm_up_queries(&config, dataset.clone()).await?;

    let run_until = TokioInstant::now() + config.duration;
    let qps_per_worker = (config.target_qps / config.concurrency as f64).max(1.0);

    let mut handles: Vec<JoinHandle<Result<WorkerStats>>> = Vec::with_capacity(config.concurrency);
    for worker_id in 0..config.concurrency {
        let dataset_clone = dataset.clone();
        let endpoint = config.server.clone();
        let worker_run_until = run_until;
        handles.push(tokio::spawn(async move {
            run_worker(
                worker_id,
                endpoint,
                dataset_clone,
                qps_per_worker,
                worker_run_until,
            )
            .await
        }));
    }

    let mut aggregate = WorkerStats::default();
    for handle in handles {
        let worker_stats = handle.await.context("worker task panicked")??;
        aggregate.successes += worker_stats.successes;
        aggregate.failures += worker_stats.failures;
        aggregate.latencies_ns.extend(worker_stats.latencies_ns);
    }

    if aggregate.latencies_ns.is_empty() {
        return Err(anyhow!("no successful requests recorded"));
    }

    aggregate.latencies_ns.sort_unstable();

    let total_duration = config.duration.as_secs_f64();
    let achieved_qps = aggregate.successes as f64 / total_duration;

    println!("\nLoad test complete");
    println!("  Successful requests: {}", aggregate.successes);
    println!("  Failed requests: {}", aggregate.failures);
    println!("  Achieved QPS: {:.1}", achieved_qps);
    println!(
        "  p50 latency: {:.3} ms",
        percentile(&aggregate.latencies_ns, 50.0)
    );
    println!(
        "  p95 latency: {:.3} ms",
        percentile(&aggregate.latencies_ns, 95.0)
    );
    println!(
        "  p99 latency: {:.3} ms",
        percentile(&aggregate.latencies_ns, 99.0)
    );

    Ok(())
}

fn parse_args() -> Result<Config> {
    let mut server = "http://127.0.0.1:50051".to_string();
    let mut target_qps = 1000.0;
    let mut duration_secs = 30;
    let mut concurrency = 32usize;
    let mut dataset_size = 2048usize;
    let mut dimension = 384usize;

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--server" => {
                server = args.next().context("missing value for --server")?;
            }
            "--qps" => {
                let qps_str = args.next().context("missing value for --qps")?;
                target_qps = qps_str.parse().context("invalid --qps value")?;
            }
            "--duration" => {
                let val = args.next().context("missing value for --duration")?;
                duration_secs = val.parse().context("invalid --duration value")?;
            }
            "--concurrency" => {
                let val = args.next().context("missing value for --concurrency")?;
                concurrency = val.parse().context("invalid --concurrency value")?;
            }
            "--dataset" => {
                let val = args.next().context("missing value for --dataset")?;
                dataset_size = val.parse().context("invalid --dataset value")?;
            }
            "--dimension" => {
                let val = args.next().context("missing value for --dimension")?;
                dimension = val.parse().context("invalid --dimension value")?;
            }
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            other => {
                return Err(anyhow!("unknown argument: {other}"));
            }
        }
    }

    if target_qps <= 0.0 {
        return Err(anyhow!("--qps must be positive"));
    }
    if duration_secs == 0 {
        return Err(anyhow!("--duration must be at least 1 second"));
    }
    if concurrency == 0 {
        return Err(anyhow!("--concurrency must be at least 1"));
    }
    if dataset_size == 0 {
        return Err(anyhow!("--dataset must be at least 1"));
    }
    if dimension == 0 {
        return Err(anyhow!("--dimension must be at least 1"));
    }

    Ok(Config {
        server,
        target_qps,
        duration: Duration::from_secs(duration_secs as u64),
        concurrency,
        dataset_size,
        dimension,
    })
}

fn print_usage() {
    println!("KyroDB load tester");
    println!("USAGE: kyrodb_load_tester [options]\n");
    println!("Options:");
    println!("  --server <addr>       gRPC endpoint (default http://127.0.0.1:50051)");
    println!("  --qps <value>         target queries per second (default 1000)");
    println!("  --duration <secs>     test duration in seconds (default 30)");
    println!("  --concurrency <n>     concurrent workers (default 32)");
    println!("  --dataset <n>         number of vectors to seed (default 2048)");
    println!("  --dimension <n>       embedding dimension (default 384)");
}

fn build_dataset(count: usize, dimension: usize) -> Vec<Vec<f32>> {
    let mut rng = StdRng::seed_from_u64(42);
    (0..count)
        .map(|_| random_embedding(&mut rng, dimension))
        .collect()
}

fn random_embedding(rng: &mut StdRng, dimension: usize) -> Vec<f32> {
    (0..dimension)
        .map(|_| rng.gen_range(-1.0f32..1.0f32))
        .collect()
}

async fn seed_dataset(config: &Config, dataset: Arc<Vec<Vec<f32>>>) -> Result<()> {
    let mut client = KyroDbServiceClient::connect(config.server.clone())
        .await
        .context("failed to connect to server for dataset seeding")?;

    for (idx, embedding) in dataset.iter().enumerate() {
        let request = InsertRequest {
            doc_id: (idx + 1) as u64,
            embedding: embedding.clone(),
            metadata: HashMap::new(),
            namespace: String::new(),
        };

        let response = client
            .insert(request)
            .await
            .context("insert request failed")?;

        if !response.get_ref().success {
            return Err(anyhow!(
                "insert failed for doc {}: {}",
                idx + 1,
                response.get_ref().error
            ));
        }
    }

    Ok(())
}

async fn warm_up_queries(config: &Config, dataset: Arc<Vec<Vec<f32>>>) -> Result<()> {
    let mut client = KyroDbServiceClient::connect(config.server.clone())
        .await
        .context("failed to connect for warm-up queries")?;

    for embedding in dataset.iter().take(config.concurrency.max(1)) {
        let request = SearchRequest {
            query_embedding: embedding.clone(),
            k: 10,
            ef_search: 0,
            min_score: -1.0,
            namespace: String::new(),
            include_embeddings: false,
            metadata_filters: HashMap::new(),
            filter: None,
        };

        let response = client
            .search(request)
            .await
            .context("warm-up search failed")?;

        if response.get_ref().results.is_empty() {
            return Err(anyhow!("warm-up search returned no results"));
        }
    }

    Ok(())
}

async fn run_worker(
    worker_id: usize,
    endpoint: String,
    dataset: Arc<Vec<Vec<f32>>>,
    qps: f64,
    run_until: TokioInstant,
) -> Result<WorkerStats> {
    let mut client = KyroDbServiceClient::connect(endpoint)
        .await
        .context("failed to connect to server")?;
    let mut interval = tokio::time::interval(Duration::from_secs_f64(1.0 / qps));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    let mut rng = StdRng::seed_from_u64(worker_id as u64 + 1);
    let mut stats = WorkerStats::default();

    loop {
        tokio::select! {
            _ = interval.tick() => {
                if TokioInstant::now() >= run_until {
                    break;
                }

                let idx = rng.gen_range(0..dataset.len());
                let query_embedding = dataset[idx].clone();
                let request = SearchRequest {
                    query_embedding,
                    k: 10,
                    ef_search: 0,
                    min_score: -1.0,
                    namespace: String::new(),
                    include_embeddings: false,
                    metadata_filters: HashMap::new(),
                    filter: None,
                };

                let start = Instant::now();
                match client.search(request).await {
                    Ok(response) => {
                        if response.get_ref().results.is_empty() {
                            stats.failures += 1;
                        } else {
                            stats.successes += 1;
                            stats.latencies_ns.push(start.elapsed().as_nanos() as u64);
                        }
                    }
                    Err(err) => {
                        stats.failures += 1;
                        if stats.failures % 100 == 0 {
                            eprintln!("worker {worker_id}: search error: {err}");
                        }
                        sleep(Duration::from_millis(10)).await;
                    }
                }
            }
            _ = sleep_until(run_until) => {
                break;
            }
        }
    }

    Ok(stats)
}

async fn sleep_until(deadline: TokioInstant) {
    let now = TokioInstant::now();
    if deadline > now {
        tokio::time::sleep_until(deadline).await;
    }
}

fn percentile(latencies: &[u64], pct: f64) -> f64 {
    if latencies.is_empty() {
        return 0.0;
    }
    let rank = ((pct / 100.0) * (latencies.len() - 1) as f64).round() as usize;
    (latencies[rank] as f64) / 1_000_000.0
}
