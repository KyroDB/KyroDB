use crate::client::{BenchClient, ClientConfig, HttpClient};
use crate::metrics::{BenchmarkPhase, BenchmarkResults, MetricsCollector};
use crate::workload::Workload;
use anyhow::Result;
use std::time::Instant;
use tokio::task::JoinSet;

/// Executes workloads with multiple concurrent workers
pub struct WorkloadExecutor {
    // Store HttpClient directly since it's Clone
    http_client: HttpClient,
    workers: usize,
}

impl WorkloadExecutor {
    pub fn new_with_http(http_client: HttpClient, workers: usize) -> Self {
        Self {
            http_client,
            workers,
        }
    }

    // Legacy constructor for convenience
    pub fn from_config(config: ClientConfig, workers: usize) -> Result<Self> {
        Ok(Self {
            http_client: HttpClient::new(config)?,
            workers,
        })
    }

    /// Execute workload and collect metrics
    pub async fn execute(
        &self,
        workload: Box<dyn Workload>,
        phase: BenchmarkPhase,
    ) -> Result<BenchmarkResults> {
        let workload_name = workload.name().to_string();
        let duration_secs = workload.config().duration_secs;
        let test_end = Instant::now() + std::time::Duration::from_secs(duration_secs);

        let mut tasks = JoinSet::new();
        let mut global_collector = MetricsCollector::new()?;

        // Spawn worker tasks
        for _worker_id in 0..self.workers {
            let test_end_clone = test_end;
            let key_count = workload.config().key_count;
            let value_size = workload.config().value_size;
            let read_ratio = workload.config().read_ratio;
            let client = self.http_client.clone();

            tasks.spawn(async move {
                let mut local_collector = MetricsCollector::new().unwrap();
                // Use StdRng with thread-local seed for Send-safe RNG
                use rand::rngs::StdRng;
                use rand::{Rng, SeedableRng};
                let mut rng = StdRng::from_entropy();

                while Instant::now() < test_end_clone {
                    let key = rng.gen_range(0..key_count);
                    let start = Instant::now();

                    let result = if rng.gen::<f64>() < read_ratio {
                        // Real GET operation
                        client.get(key).await.map(|_| ())
                    } else {
                        // Real PUT operation
                        let value = vec![0u8; value_size];
                        client.put(key, value).await
                    };

                    let latency = start.elapsed();

                    match result {
                        Ok(_) => local_collector.record_success(latency),
                        Err(_) => local_collector.record_failure(),
                    }
                }

                local_collector
            });
        }

        // Collect results from all workers
        while let Some(result) = tasks.join_next().await {
            let worker_collector = result?;
            global_collector.merge(&worker_collector)?;
        }

        Ok(global_collector.finalize(phase, workload_name))
    }
}
