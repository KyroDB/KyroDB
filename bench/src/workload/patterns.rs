use super::{Workload, WorkloadConfig, Operation, DistributionGenerator};
use anyhow::Result;
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
use std::time::Instant;

/// Read-heavy workload (95% reads, 5% writes)
pub struct ReadHeavyWorkload {
    config: WorkloadConfig,
    key_gen: DistributionGenerator,
    value_rng: StdRng,
    op_rng: StdRng,
    start_time: Instant,
}

impl ReadHeavyWorkload {
    pub fn new(mut config: WorkloadConfig) -> Result<Self> {
        config.read_ratio = 0.95;
        
        Ok(Self {
            key_gen: DistributionGenerator::new(
                config.distribution.clone(),
                config.key_count,
                42,
            )?,
            value_rng: StdRng::seed_from_u64(43),
            op_rng: StdRng::seed_from_u64(44),
            start_time: Instant::now(),
            config,
        })
    }
}

impl Workload for ReadHeavyWorkload {
    fn name(&self) -> &str {
        "read_heavy"
    }
    
    fn config(&self) -> &WorkloadConfig {
        &self.config
    }
    
    fn next_operation(&mut self) -> Operation {
        let key = self.key_gen.next_key();
        
        if self.op_rng.gen::<f64>() < self.config.read_ratio {
            Operation::Get { key }
        } else {
            let mut value = vec![0u8; self.config.value_size];
            self.value_rng.fill(&mut value[..]);
            Operation::Put { key, value }
        }
    }
    
    fn is_complete(&self) -> bool {
        self.start_time.elapsed().as_secs() >= self.config.duration_secs
    }
}

/// Write-heavy workload (10% reads, 90% writes)
pub struct WriteHeavyWorkload {
    config: WorkloadConfig,
    key_gen: DistributionGenerator,
    value_rng: StdRng,
    op_rng: StdRng,
    start_time: Instant,
}

impl WriteHeavyWorkload {
    pub fn new(mut config: WorkloadConfig) -> Result<Self> {
        config.read_ratio = 0.10;
        
        Ok(Self {
            key_gen: DistributionGenerator::new(
                config.distribution.clone(),
                config.key_count,
                42,
            )?,
            value_rng: StdRng::seed_from_u64(43),
            op_rng: StdRng::seed_from_u64(44),
            start_time: Instant::now(),
            config,
        })
    }
}

impl Workload for WriteHeavyWorkload {
    fn name(&self) -> &str {
        "write_heavy"
    }
    
    fn config(&self) -> &WorkloadConfig {
        &self.config
    }
    
    fn next_operation(&mut self) -> Operation {
        let key = self.key_gen.next_key();
        
        if self.op_rng.gen::<f64>() < self.config.read_ratio {
            Operation::Get { key }
        } else {
            let mut value = vec![0u8; self.config.value_size];
            self.value_rng.fill(&mut value[..]);
            Operation::Put { key, value }
        }
    }
    
    fn is_complete(&self) -> bool {
        self.start_time.elapsed().as_secs() >= self.config.duration_secs
    }
}

/// Mixed workload (50% reads, 50% writes)
pub struct MixedWorkload {
    config: WorkloadConfig,
    key_gen: DistributionGenerator,
    value_rng: StdRng,
    op_rng: StdRng,
    start_time: Instant,
}

impl MixedWorkload {
    pub fn new(mut config: WorkloadConfig) -> Result<Self> {
        config.read_ratio = 0.50;
        
        Ok(Self {
            key_gen: DistributionGenerator::new(
                config.distribution.clone(),
                config.key_count,
                42,
            )?,
            value_rng: StdRng::seed_from_u64(43),
            op_rng: StdRng::seed_from_u64(44),
            start_time: Instant::now(),
            config,
        })
    }
}

impl Workload for MixedWorkload {
    fn name(&self) -> &str {
        "mixed"
    }
    
    fn config(&self) -> &WorkloadConfig {
        &self.config
    }
    
    fn next_operation(&mut self) -> Operation {
        let key = self.key_gen.next_key();
        
        if self.op_rng.gen::<f64>() < self.config.read_ratio {
            Operation::Get { key }
        } else {
            let mut value = vec![0u8; self.config.value_size];
            self.value_rng.fill(&mut value[..]);
            Operation::Put { key, value }
        }
    }
    
    fn is_complete(&self) -> bool {
        self.start_time.elapsed().as_secs() >= self.config.duration_secs
    }
}

/// Scan-heavy workload (99% reads with some scans)
pub struct ScanHeavyWorkload {
    config: WorkloadConfig,
    key_gen: DistributionGenerator,
    value_rng: StdRng,
    op_rng: StdRng,
    scan_rng: StdRng,
    start_time: Instant,
}

impl ScanHeavyWorkload {
    pub fn new(mut config: WorkloadConfig) -> Result<Self> {
        config.read_ratio = 0.99;
        
        Ok(Self {
            key_gen: DistributionGenerator::new(
                config.distribution.clone(),
                config.key_count,
                42,
            )?,
            value_rng: StdRng::seed_from_u64(43),
            op_rng: StdRng::seed_from_u64(44),
            scan_rng: StdRng::seed_from_u64(45),
            start_time: Instant::now(),
            config,
        })
    }
}

impl Workload for ScanHeavyWorkload {
    fn name(&self) -> &str {
        "scan_heavy"
    }
    
    fn config(&self) -> &WorkloadConfig {
        &self.config
    }
    
    fn next_operation(&mut self) -> Operation {
        if self.op_rng.gen::<f64>() < self.config.read_ratio {
            // 20% of reads are scans
            if self.scan_rng.gen::<f64>() < 0.2 {
                let start = self.key_gen.next_key();
                let limit = self.scan_rng.gen_range(10..100);
                Operation::Scan { start, limit }
            } else {
                let key = self.key_gen.next_key();
                Operation::Get { key }
            }
        } else {
            let key = self.key_gen.next_key();
            let mut value = vec![0u8; self.config.value_size];
            self.value_rng.fill(&mut value[..]);
            Operation::Put { key, value }
        }
    }
    
    fn is_complete(&self) -> bool {
        self.start_time.elapsed().as_secs() >= self.config.duration_secs
    }
}
