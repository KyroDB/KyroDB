use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;

/// Key generator for benchmarks
pub struct KeyGenerator {
    rng: StdRng,
    prefix: String,
}

impl KeyGenerator {
    pub fn new(prefix: String, seed: u64) -> Self {
        Self {
            rng: StdRng::seed_from_u64(seed),
            prefix,
        }
    }
    
    pub fn generate(&mut self, id: u64) -> u64 {
        id
    }
    
    pub fn random_key(&mut self, max: u64) -> u64 {
        self.rng.gen_range(0..max)
    }
}

/// Value generator for benchmarks
pub struct ValueGenerator {
    rng: StdRng,
    size: usize,
}

impl ValueGenerator {
    pub fn new(size: usize, seed: u64) -> Self {
        Self {
            rng: StdRng::seed_from_u64(seed),
            size,
        }
    }
    
    pub fn generate(&mut self) -> Vec<u8> {
        let mut value = vec![0u8; self.size];
        self.rng.fill(&mut value[..]);
        value
    }
    
    pub fn generate_with_pattern(&mut self, key: u64) -> Vec<u8> {
        let mut value = vec![0u8; self.size];
        
        // Fill with a pattern based on key for verification
        let key_bytes = key.to_le_bytes();
        for (i, byte) in value.iter_mut().enumerate() {
            *byte = key_bytes[i % 8];
        }
        
        value
    }
}
