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
    request_timeout: Duration,
    max_failure_ratio: f64,
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
    println!("  Request timeout: {:?}", config.request_timeout);
    println!("  Max failure ratio: {:.3}", config.max_failure_ratio);

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
                config.request_timeout,
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

    let total_requests = aggregate.successes + aggregate.failures;
    let failure_ratio = if total_requests == 0 {
        1.0
    } else {
        aggregate.failures as f64 / total_requests as f64
    };
    println!("  Failure ratio: {:.3}", failure_ratio);
    if failure_ratio > config.max_failure_ratio {
        return Err(anyhow!(
            "failure ratio {:.3} exceeded max {:.3}",
            failure_ratio,
            config.max_failure_ratio
        ));
    }

    Ok(())
}

fn parse_args() -> Result<Config> {
    match parse_args_from(env::args().skip(1))? {
        ParseOutcome::Config(config) => Ok(config),
        ParseOutcome::HelpRequested => {
            print_usage();
            std::process::exit(0);
        }
    }
}

#[derive(Debug)]
enum ParseOutcome {
    Config(Config),
    HelpRequested,
}

fn parse_args_from<I>(args: I) -> Result<ParseOutcome>
where
    I: IntoIterator<Item = String>,
{
    let mut server = "http://127.0.0.1:50051".to_string();
    let mut target_qps = 1000.0;
    let mut duration_secs = 30;
    let mut concurrency = 32usize;
    let mut dataset_size = 2048usize;
    let mut dimension = 384usize;
    let mut request_timeout_ms = 5_000u64;
    let mut max_failure_ratio = 1.0f64;

    let mut args = args.into_iter();
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
            "--request-timeout-ms" => {
                let val = args
                    .next()
                    .context("missing value for --request-timeout-ms")?;
                request_timeout_ms = val.parse().context("invalid --request-timeout-ms value")?;
            }
            "--max-failure-ratio" => {
                let val = args
                    .next()
                    .context("missing value for --max-failure-ratio")?;
                max_failure_ratio = val.parse().context("invalid --max-failure-ratio value")?;
            }
            "--help" | "-h" => {
                return Ok(ParseOutcome::HelpRequested);
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
    if request_timeout_ms == 0 {
        return Err(anyhow!("--request-timeout-ms must be at least 1"));
    }
    if !(0.0..=1.0).contains(&max_failure_ratio) {
        return Err(anyhow!("--max-failure-ratio must be between 0.0 and 1.0"));
    }

    Ok(ParseOutcome::Config(Config {
        server,
        target_qps,
        duration: Duration::from_secs(duration_secs as u64),
        concurrency,
        dataset_size,
        dimension,
        request_timeout: Duration::from_millis(request_timeout_ms),
        max_failure_ratio,
    }))
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
    println!("  --request-timeout-ms <n>  per-request timeout in ms (default 5000)");
    println!(
        "  --max-failure-ratio <value>   fail if failure ratio exceeds this [0.0,1.0] (default 1.0)"
    );
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
    let mut client = connect_client_with_timeout(&config.server, config.request_timeout)
        .await
        .context("failed to connect to server for dataset seeding")?;

    for (idx, embedding) in dataset.iter().enumerate() {
        let request = InsertRequest {
            doc_id: (idx + 1) as u64,
            embedding: embedding.clone(),
            metadata: HashMap::new(),
            namespace: String::new(),
        };

        let response =
            match tokio::time::timeout(config.request_timeout, client.insert(request)).await {
                Ok(result) => result.context("insert request failed")?,
                Err(_) => {
                    return Err(anyhow!(
                        "insert request timed out after {:?}",
                        config.request_timeout
                    ))
                }
            };

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
    let mut client = connect_client_with_timeout(&config.server, config.request_timeout)
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

        let response =
            match tokio::time::timeout(config.request_timeout, client.search(request)).await {
                Ok(result) => result.context("warm-up search failed")?,
                Err(_) => {
                    return Err(anyhow!(
                        "warm-up search timed out after {:?}",
                        config.request_timeout
                    ))
                }
            };

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
    request_timeout: Duration,
) -> Result<WorkerStats> {
    let mut client = connect_client_with_timeout(&endpoint, request_timeout)
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
                match tokio::time::timeout(request_timeout, client.search(request)).await {
                    Ok(Ok(response)) => {
                        if response.get_ref().results.is_empty() {
                            stats.failures += 1;
                        } else {
                            stats.successes += 1;
                            stats.latencies_ns.push(start.elapsed().as_nanos() as u64);
                        }
                    }
                    Ok(Err(err)) => {
                        stats.failures += 1;
                        if stats.failures % 100 == 0 {
                            eprintln!("worker {worker_id}: search error: {err}");
                        }
                        if let Ok(reconnected) = connect_client_with_timeout(&endpoint, request_timeout).await {
                            client = reconnected;
                        }
                        sleep(Duration::from_millis(10)).await;
                    }
                    Err(_) => {
                        stats.failures += 1;
                        if stats.failures % 50 == 0 {
                            eprintln!(
                                "worker {worker_id}: search timeout after {:?}",
                                request_timeout
                            );
                        }
                        if let Ok(reconnected) = connect_client_with_timeout(&endpoint, request_timeout).await {
                            client = reconnected;
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

async fn connect_client_with_timeout(
    endpoint: &str,
    timeout: Duration,
) -> Result<KyroDbServiceClient<tonic::transport::Channel>> {
    match tokio::time::timeout(timeout, KyroDbServiceClient::connect(endpoint.to_string())).await {
        Ok(result) => result.context("transport connect failed"),
        Err(_) => Err(anyhow!("transport connect timed out after {:?}", timeout)),
    }
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

#[cfg(test)]
mod tests {
    use super::{parse_args_from, ParseOutcome};
    use std::time::Duration;

    fn parse_config(args: &[&str]) -> super::Config {
        let parsed = parse_args_from(args.iter().map(|s| s.to_string()))
            .expect("argument parsing should succeed");
        match parsed {
            ParseOutcome::Config(cfg) => cfg,
            ParseOutcome::HelpRequested => panic!("unexpected help request"),
        }
    }

    #[test]
    fn parse_args_uses_expected_defaults() {
        let cfg = parse_config(&[]);
        assert_eq!(cfg.server, "http://127.0.0.1:50051");
        assert_eq!(cfg.target_qps, 1000.0);
        assert_eq!(cfg.duration, Duration::from_secs(30));
        assert_eq!(cfg.concurrency, 32);
        assert_eq!(cfg.dataset_size, 2048);
        assert_eq!(cfg.dimension, 384);
        assert_eq!(cfg.request_timeout, Duration::from_millis(5_000));
        assert_eq!(cfg.max_failure_ratio, 1.0);
    }

    #[test]
    fn parse_args_accepts_request_timeout_override() {
        let cfg = parse_config(&[
            "--server",
            "http://127.0.0.1:56052",
            "--qps",
            "2500",
            "--duration",
            "90",
            "--concurrency",
            "64",
            "--dataset",
            "8192",
            "--dimension",
            "768",
            "--request-timeout-ms",
            "7500",
            "--max-failure-ratio",
            "0.25",
        ]);
        assert_eq!(cfg.server, "http://127.0.0.1:56052");
        assert_eq!(cfg.target_qps, 2500.0);
        assert_eq!(cfg.duration, Duration::from_secs(90));
        assert_eq!(cfg.concurrency, 64);
        assert_eq!(cfg.dataset_size, 8192);
        assert_eq!(cfg.dimension, 768);
        assert_eq!(cfg.request_timeout, Duration::from_millis(7_500));
        assert_eq!(cfg.max_failure_ratio, 0.25);
    }

    #[test]
    fn parse_args_rejects_zero_request_timeout() {
        let err = parse_args_from(
            ["--request-timeout-ms", "0"]
                .into_iter()
                .map(|s| s.to_string()),
        )
        .expect_err("zero timeout must fail");
        assert!(err
            .to_string()
            .contains("--request-timeout-ms must be at least 1"));
    }

    #[test]
    fn parse_args_detects_help_request() {
        let parsed = parse_args_from(["--help"].into_iter().map(|s| s.to_string()))
            .expect("help should parse");
        assert!(matches!(parsed, ParseOutcome::HelpRequested));
    }

    #[test]
    fn parse_args_rejects_invalid_failure_ratio() {
        let err = parse_args_from(
            ["--max-failure-ratio", "1.5"]
                .into_iter()
                .map(|s| s.to_string()),
        )
        .expect_err("invalid ratio must fail");
        assert!(err
            .to_string()
            .contains("--max-failure-ratio must be between 0.0 and 1.0"));
    }
}
