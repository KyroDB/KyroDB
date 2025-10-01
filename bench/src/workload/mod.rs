use anyhow::Result;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rand_distr::{Distribution as RandDist, Zipf};

pub mod generator;
pub mod patterns;

pub use generator::{KeyGenerator, ValueGenerator};
pub use patterns::{MixedWorkload, ReadHeavyWorkload, ScanHeavyWorkload, WriteHeavyWorkload};

/// Workload configuration
#[derive(Debug, Clone)]
pub struct WorkloadConfig {
    pub key_count: u64,
    pub value_size: usize,
    pub read_ratio: f64,
    pub distribution: Distribution,
    pub duration_secs: u64,
}

impl Default for WorkloadConfig {
    fn default() -> Self {
        Self {
            key_count: 100_000,
            value_size: 256,
            read_ratio: 0.5,
            distribution: Distribution::Uniform,
            duration_secs: 30,
        }
    }
}

/// Key distribution pattern
#[derive(Debug, Clone)]
pub enum Distribution {
    Uniform,
    Zipf {
        skew: f64,
    },
    Sequential,
    Hotspot {
        hot_key_ratio: f64,
        hot_access_ratio: f64,
    },
}

impl Distribution {
    pub fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "uniform" => Ok(Distribution::Uniform),
            "zipf" => Ok(Distribution::Zipf { skew: 1.2 }),
            "sequential" => Ok(Distribution::Sequential),
            "hotspot" => Ok(Distribution::Hotspot {
                hot_key_ratio: 0.1,
                hot_access_ratio: 0.8,
            }),
            _ => anyhow::bail!(
                "Unknown distribution: {}. Use 'uniform', 'zipf', 'sequential', or 'hotspot'",
                s
            ),
        }
    }
}

/// Workload operation
#[derive(Debug, Clone)]
pub enum Operation {
    Put { key: u64, value: Vec<u8> },
    Get { key: u64 },
    Scan { start: u64, limit: usize },
}

/// Workload trait
pub trait Workload: Send + Sync {
    fn name(&self) -> &str;
    fn config(&self) -> &WorkloadConfig;

    /// Generate next operation
    fn next_operation(&mut self) -> Operation;

    /// Check if workload is complete
    fn is_complete(&self) -> bool;
}

/// Workload type
#[derive(Debug, Clone, Copy)]
pub enum WorkloadType {
    ReadHeavy,
    WriteHeavy,
    Mixed,
    ScanHeavy,
}

impl WorkloadType {
    pub fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "read_heavy" | "read" => Ok(WorkloadType::ReadHeavy),
            "write_heavy" | "write" => Ok(WorkloadType::WriteHeavy),
            "mixed" => Ok(WorkloadType::Mixed),
            "scan_heavy" | "scan" => Ok(WorkloadType::ScanHeavy),
            _ => anyhow::bail!("Unknown workload type: {}. Use 'read_heavy', 'write_heavy', 'mixed', or 'scan_heavy'", s),
        }
    }
}

/// Workload factory
pub fn create_workload(
    workload_type: WorkloadType,
    config: WorkloadConfig,
) -> Result<Box<dyn Workload>> {
    match workload_type {
        WorkloadType::ReadHeavy => Ok(Box::new(ReadHeavyWorkload::new(config)?)),
        WorkloadType::WriteHeavy => Ok(Box::new(WriteHeavyWorkload::new(config)?)),
        WorkloadType::Mixed => Ok(Box::new(MixedWorkload::new(config)?)),
        WorkloadType::ScanHeavy => Ok(Box::new(ScanHeavyWorkload::new(config)?)),
    }
}

/// Distribution generator for key selection
pub struct DistributionGenerator {
    rng: StdRng,
    distribution: Distribution,
    key_count: u64,
    sequential_counter: u64,
    zipf_dist: Option<Zipf<f64>>,
}

impl DistributionGenerator {
    pub fn new(distribution: Distribution, key_count: u64, seed: u64) -> Result<Self> {
        let rng = StdRng::seed_from_u64(seed);
        let zipf_dist = match &distribution {
            Distribution::Zipf { skew } => Some(Zipf::new(key_count, *skew)?),
            _ => None,
        };

        Ok(Self {
            rng,
            distribution,
            key_count,
            sequential_counter: 0,
            zipf_dist,
        })
    }

    pub fn next_key(&mut self) -> u64 {
        match &self.distribution {
            Distribution::Uniform => self.rng.gen_range(0..self.key_count),
            Distribution::Zipf { .. } => {
                if let Some(ref dist) = self.zipf_dist {
                    let sample = dist.sample(&mut self.rng);
                    (sample as u64).min(self.key_count - 1)
                } else {
                    self.rng.gen_range(0..self.key_count)
                }
            }
            Distribution::Sequential => {
                let key = self.sequential_counter % self.key_count;
                self.sequential_counter += 1;
                key
            }
            Distribution::Hotspot {
                hot_key_ratio,
                hot_access_ratio,
            } => {
                let hot_keys = (self.key_count as f64 * hot_key_ratio) as u64;

                if self.rng.gen::<f64>() < *hot_access_ratio {
                    // Access hot keys
                    self.rng.gen_range(0..hot_keys.max(1))
                } else {
                    // Access cold keys
                    self.rng.gen_range(hot_keys..self.key_count)
                }
            }
        }
    }
}
