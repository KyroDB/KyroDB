//! Test Data Fixtures
//!
//! Generate realistic test data for various scenarios

use rand::Rng;

/// Test value sizes
pub const SMALL_VALUE: usize = 64;
pub const MEDIUM_VALUE: usize = 1024;
pub const LARGE_VALUE: usize = 65536;

/// Generate value of specific size
pub fn value_of_size(size: usize) -> Vec<u8> {
    vec![b'x'; size]
}

/// Generate random value
pub fn random_value(min_size: usize, max_size: usize) -> Vec<u8> {
    let mut rng = rand::thread_rng();
    let size = rng.gen_range(min_size..=max_size);
    (0..size).map(|_| rng.gen::<u8>()).collect()
}

/// Generate compressible value (lots of zeros)
pub fn compressible_value(size: usize) -> Vec<u8> {
    let mut value = vec![0u8; size];
    // Add some non-zero bytes to make it realistic
    let mut rng = rand::thread_rng();
    for i in (0..size).step_by(64) {
        value[i] = rng.gen();
    }
    value
}

/// Generate incompressible value (random data)
pub fn incompressible_value(size: usize) -> Vec<u8> {
    random_value(size, size)
}

/// Test dataset configurations
#[derive(Clone, Debug)]
pub struct DatasetConfig {
    pub key_count: usize,
    pub value_size: usize,
    pub sequential: bool,
    pub duplicates: bool,
}

impl Default for DatasetConfig {
    fn default() -> Self {
        Self {
            key_count: 1000,
            value_size: MEDIUM_VALUE,
            sequential: true,
            duplicates: false,
        }
    }
}

impl DatasetConfig {
    /// Small dataset for quick tests
    pub fn small() -> Self {
        Self {
            key_count: 100,
            value_size: SMALL_VALUE,
            ..Default::default()
        }
    }

    /// Medium dataset for standard tests
    pub fn medium() -> Self {
        Self {
            key_count: 10_000,
            value_size: MEDIUM_VALUE,
            ..Default::default()
        }
    }

    /// Large dataset for stress tests
    pub fn large() -> Self {
        Self {
            key_count: 1_000_000,
            value_size: MEDIUM_VALUE,
            ..Default::default()
        }
    }

    /// Generate dataset based on configuration
    pub fn generate(&self) -> Vec<(u64, Vec<u8>)> {
        let mut data = Vec::with_capacity(self.key_count);
        let mut rng = rand::thread_rng();

        for i in 0..self.key_count {
            let key = if self.sequential {
                i as u64
            } else {
                rng.gen::<u64>()
            };

            let value = value_of_size(self.value_size);
            data.push((key, value));

            // Add duplicates if configured
            if self.duplicates && rng.gen_bool(0.1) {
                data.push((key, value_of_size(self.value_size)));
            }
        }

        data
    }
}

/// Workload pattern generator
pub enum WorkloadPattern {
    Sequential,
    Random,
    Zipfian,
    HotCold { hot_ratio: f64, hot_keys: Vec<u64> },
}

impl WorkloadPattern {
    /// Generate key sequence based on pattern
    pub fn generate_keys(&self, count: usize, key_range: u64) -> Vec<u64> {
        match self {
            WorkloadPattern::Sequential => (0..count as u64).collect(),
            WorkloadPattern::Random => {
                let mut rng = rand::thread_rng();
                (0..count).map(|_| rng.gen_range(0..key_range)).collect()
            }
            WorkloadPattern::Zipfian => {
                let mut rng = rand::thread_rng();
                (0..count)
                    .map(|_| {
                        if rng.gen_bool(0.8) {
                            rng.gen_range(0..key_range / 5)
                        } else {
                            rng.gen_range(0..key_range)
                        }
                    })
                    .collect()
            }
            WorkloadPattern::HotCold {
                hot_ratio,
                hot_keys,
            } => {
                let mut rng = rand::thread_rng();
                (0..count)
                    .map(|_| {
                        if rng.gen_bool(*hot_ratio) {
                            hot_keys[rng.gen_range(0..hot_keys.len())]
                        } else {
                            rng.gen_range(0..key_range)
                        }
                    })
                    .collect()
            }
        }
    }
}
