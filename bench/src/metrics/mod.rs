use anyhow::Result;
use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};

pub mod reporter;
pub use reporter::ResultsReporter;

/// Comprehensive benchmark results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkResults {
    pub phase: BenchmarkPhase,
    pub workload: String,
    pub duration: Duration,
    pub total_ops: u64,
    pub successful_ops: u64,
    pub failed_ops: u64,
    pub throughput_ops_sec: f64,
    pub latency: LatencyMetrics,
    pub system: Option<SystemMetrics>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BenchmarkPhase {
    Microbenchmark,
    HttpLayer,
    Integration,
    Comparison,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencyMetrics {
    pub min_us: u64,
    pub mean_us: f64,
    pub p50_us: u64,
    pub p95_us: u64,
    pub p99_us: u64,
    pub p999_us: u64,
    pub max_us: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemMetrics {
    pub memory_mb: u64,
    pub cpu_percent: f64,
    pub disk_reads_mb: u64,
    pub disk_writes_mb: u64,
}

/// Metrics collector for benchmark runs
pub struct MetricsCollector {
    histogram: Histogram<u64>,
    total_ops: u64,
    successful_ops: u64,
    failed_ops: u64,
    start_time: Instant,
}

impl MetricsCollector {
    pub fn new() -> Result<Self> {
        Ok(Self {
            histogram: Histogram::new(3)?,
            total_ops: 0,
            successful_ops: 0,
            failed_ops: 0,
            start_time: Instant::now(),
        })
    }

    pub fn record_success(&mut self, latency: Duration) {
        let _ = self.histogram.record(latency.as_micros() as u64);
        self.total_ops += 1;
        self.successful_ops += 1;
    }

    pub fn record_failure(&mut self) {
        self.total_ops += 1;
        self.failed_ops += 1;
    }

    pub fn merge(&mut self, other: &MetricsCollector) -> Result<()> {
        self.histogram.add(&other.histogram)?;
        self.total_ops += other.total_ops;
        self.successful_ops += other.successful_ops;
        self.failed_ops += other.failed_ops;
        Ok(())
    }

    pub fn finalize(&self, phase: BenchmarkPhase, workload: String) -> BenchmarkResults {
        let duration = self.start_time.elapsed();
        let throughput = if duration.as_secs_f64() > 0.0 {
            self.successful_ops as f64 / duration.as_secs_f64()
        } else {
            0.0
        };

        BenchmarkResults {
            phase,
            workload,
            duration,
            total_ops: self.total_ops,
            successful_ops: self.successful_ops,
            failed_ops: self.failed_ops,
            throughput_ops_sec: throughput,
            latency: LatencyMetrics {
                min_us: self.histogram.min(),
                mean_us: self.histogram.mean(),
                p50_us: self.histogram.value_at_quantile(0.50),
                p95_us: self.histogram.value_at_quantile(0.95),
                p99_us: self.histogram.value_at_quantile(0.99),
                p999_us: self.histogram.value_at_quantile(0.999),
                max_us: self.histogram.max(),
            },
            system: None, // System metrics can be added later
        }
    }
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new().expect("Failed to create metrics collector")
    }
}
