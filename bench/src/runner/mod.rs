use anyhow::Result;

pub mod executor;
pub mod warmup;

pub use executor::WorkloadExecutor;
pub use warmup::warmup_server;

use crate::metrics::{BenchmarkPhase, BenchmarkResults};
use crate::workload::{create_workload, WorkloadConfig, WorkloadType};

/// Runner configuration
#[derive(Debug, Clone)]
pub struct RunnerConfig {
    pub workers: usize,
    pub duration_secs: u64,
    pub warmup_secs: u64,
}

impl Default for RunnerConfig {
    fn default() -> Self {
        Self {
            workers: 64,
            duration_secs: 30,
            warmup_secs: 10,
        }
    }
}

use crate::client::{ClientConfig, HttpClient};

/// Benchmark runner orchestrates benchmark execution
pub struct BenchmarkRunner {
    client: HttpClient,
    config: RunnerConfig,
    results: Vec<BenchmarkResults>,
}

impl BenchmarkRunner {
    pub fn new(client: HttpClient, config: RunnerConfig) -> Self {
        Self {
            client,
            config,
            results: Vec::new(),
        }
    }

    pub fn from_config(client_config: ClientConfig, runner_config: RunnerConfig) -> Result<Self> {
        Ok(Self {
            client: HttpClient::new(client_config)?,
            config: runner_config,
            results: Vec::new(),
        })
    }

    /// Run Phase 1: Raw engine microbenchmarks via cargo bench
    pub async fn run_microbenchmarks(&mut self) -> Result<()> {
        println!("🔬 Running raw engine microbenchmarks...");

        let output = std::process::Command::new("cargo")
            .args(&[
                "bench",
                "-p",
                "kyrodb-engine",
                "--features",
                "learned-index",
            ])
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!("Microbenchmarks failed: {}", stderr);
        }

        println!("✅ Microbenchmarks complete");
        Ok(())
    }

    /// Run Phase 2: HTTP layer benchmarks
    pub async fn run_http_benchmarks(&mut self) -> Result<()> {
        println!("🌐 Running HTTP layer benchmarks...");

        // Warmup
        warmup::warmup_server(&self.client, self.config.warmup_secs).await?;

        // Run workloads
        let workloads = vec![
            (WorkloadType::ReadHeavy, 0.95),
            (WorkloadType::WriteHeavy, 0.10),
            (WorkloadType::Mixed, 0.50),
        ];

        for (workload_type, read_ratio) in workloads {
            let config = WorkloadConfig {
                key_count: 1_000_000,
                value_size: 256,
                read_ratio,
                distribution: crate::workload::Distribution::Uniform,
                duration_secs: self.config.duration_secs,
            };

            let result = self
                .run_workload(BenchmarkPhase::HttpLayer, workload_type, config)
                .await?;

            self.results.push(result);
        }

        println!("✅ HTTP benchmarks complete");
        Ok(())
    }

    /// Run Phase 3: Integration benchmarks
    pub async fn run_integration_benchmarks(&mut self) -> Result<()> {
        println!("🔗 Running integration benchmarks...");

        // Test complete flow: PUT → Snapshot → RMI Build → GET
        let config = WorkloadConfig {
            key_count: 1_000_000,
            value_size: 512,
            read_ratio: 0.80,
            distribution: crate::workload::Distribution::Zipf { skew: 1.2 },
            duration_secs: 60,
        };

        let result = self
            .run_workload(BenchmarkPhase::Integration, WorkloadType::Mixed, config)
            .await?;

        self.results.push(result);

        println!("✅ Integration benchmarks complete");
        Ok(())
    }

    /// Run Phase 4: RMI vs BTree comparison
    pub async fn run_comparison_benchmarks(&mut self) -> Result<()> {
        println!("⚖️  Running RMI vs BTree comparison...");
        println!("⚠️  Comparison requires separate builds - use shell script for full comparison");
        Ok(())
    }

    /// Run single workload
    async fn run_workload(
        &self,
        phase: BenchmarkPhase,
        workload_type: WorkloadType,
        config: WorkloadConfig,
    ) -> Result<BenchmarkResults> {
        let workload_name = format!("{:?}", workload_type);
        println!("  🏃 Running {} workload...", workload_name);

        let executor = WorkloadExecutor::new_with_http(self.client.clone(), self.config.workers);

        let workload = create_workload(workload_type, config)?;
        let results = executor.execute(workload, phase).await?;

        println!("    Throughput: {:.0} ops/sec", results.throughput_ops_sec);
        println!("    P99 Latency: {} μs", results.latency.p99_us);

        Ok(results)
    }

    /// Collect all results
    pub fn collect_results(self) -> Vec<BenchmarkResults> {
        self.results
    }
}
