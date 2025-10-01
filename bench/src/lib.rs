//! KyroDB Benchmark Library
//!
//! Modular benchmark infrastructure for testing:
//! - Raw engine performance (WAL, snapshots, indexes)
//! - HTTP layer performance
//! - End-to-end integration
//! - RMI vs BTree comparison
//!
//! # Architecture
//!
//! ```text
//! bench
//! ├── client/       # HTTP and binary protocol clients
//! ├── workload/     # Workload patterns (read-heavy, write-heavy, etc.)
//! ├── metrics/      # Metrics collection and reporting
//! └── runner/       # Benchmark orchestration
//! ```
//!
//! # Usage
//!
//! ```rust,no_run
//! use kyrodb_bench::{BenchmarkConfig, run_full_suite};
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let config = BenchmarkConfig::default();
//!     let results = run_full_suite(config).await?;
//!     Ok(())
//! }
//! ```

use anyhow::Result;

pub mod client;
pub mod metrics;
pub mod runner;
pub mod workload;

pub use client::{BenchClient, ClientConfig, ClientType, create_client};
pub use metrics::{BenchmarkResults, MetricsCollector, ResultsReporter};
pub use runner::{BenchmarkRunner, RunnerConfig};
pub use workload::{Workload, WorkloadConfig, WorkloadType, create_workload};

/// Core benchmark configuration
#[derive(Debug, Clone)]
pub struct BenchmarkConfig {
    pub server_url: String,
    pub client_type: ClientType,
    pub duration_secs: u64,
    pub workers: usize,
    pub warmup_secs: u64,
    pub output_dir: String,
}

impl Default for BenchmarkConfig {
    fn default() -> Self {
        Self {
            server_url: std::env::var("KYRODB_BENCH_URL")
                .unwrap_or_else(|_| "http://127.0.0.1:3030".to_string()),
            client_type: ClientType::Http,
            duration_secs: 30,
            workers: 64,
            warmup_secs: 10,
            output_dir: "bench/results".to_string(),
        }
    }
}

/// Run complete benchmark suite
///
/// Executes all benchmark phases:
/// 1. Raw engine microbenchmarks
/// 2. HTTP layer benchmarks
/// 3. Integration tests
/// 4. RMI vs BTree comparison
pub async fn run_full_suite(config: BenchmarkConfig) -> Result<Vec<BenchmarkResults>> {
    println!("🔥 KyroDB Comprehensive Benchmark Suite");
    println!("========================================");
    println!("Server: {}", config.server_url);
    println!("Workers: {}", config.workers);
    println!("Duration: {}s", config.duration_secs);
    println!();
    
    // Create client
    let client_config = ClientConfig {
        base_url: config.server_url.clone(),
        timeout_secs: 30,
        pool_size: config.workers,
        auth_token: None,
    };
    
    // Create HTTP client (only HTTP supported for now in runner)
    let http_client = client::HttpClient::new(client_config)?;
    
    // Create runner
    let runner_config = RunnerConfig {
        workers: config.workers,
        duration_secs: config.duration_secs,
        warmup_secs: config.warmup_secs,
    };
    
    let mut runner = BenchmarkRunner::new(http_client, runner_config);
    
    // Phase 1: Microbenchmarks
    println!("🔬 Phase 1: Raw Engine Microbenchmarks");
    runner.run_microbenchmarks().await?;
    
    // Phase 2: HTTP Layer
    println!("🌐 Phase 2: HTTP Layer Benchmarks");
    runner.run_http_benchmarks().await?;
    
    // Phase 3: Integration
    println!("🔗 Phase 3: Integration Tests");
    runner.run_integration_benchmarks().await?;
    
    // Phase 4: Comparison
    println!("⚖️  Phase 4: RMI vs BTree Comparison");
    runner.run_comparison_benchmarks().await?;
    
    let results = runner.collect_results();
    
    println!("\n🎉 Benchmark suite complete!");
    println!("Results saved to: {}", config.output_dir);
    
    Ok(results)
}

/// Run quick smoke test (fast validation)
pub async fn run_quick_test(config: BenchmarkConfig) -> Result<BenchmarkResults> {
    let client_config = ClientConfig {
        base_url: config.server_url.clone(),
        timeout_secs: 10,
        pool_size: 8,
        auth_token: None,
    };
    
    // Create HTTP client
    let http_client = client::HttpClient::new(client_config)?;
    
    let workload_config = WorkloadConfig {
        key_count: 10_000,
        value_size: 256,
        read_ratio: 0.8,
        distribution: workload::Distribution::Uniform,
        duration_secs: 10,
    };
    
    let executor = runner::executor::WorkloadExecutor::new_with_http(http_client, 8);
    let workload = create_workload(WorkloadType::Mixed, workload_config)?;
    let results = executor.execute(workload, metrics::BenchmarkPhase::HttpLayer).await?;
    
    ResultsReporter::print_summary(&results);
    
    Ok(results)
}
