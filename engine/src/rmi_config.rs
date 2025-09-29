//! RMI Optimization Configuration
//!
//! Configuration options for tuning RMI performance parameters

use serde::{Deserialize, Serialize};
use std::env;

/// RMI Optimization Configuration
///
/// Comprehensive configuration for tuning RMI performance
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RmiOptimizationConfig {
    /// SIMD batch size for vectorized operations
    pub simd_batch_size: usize,
    /// Cache-aligned buffer size
    pub cache_buffer_size: usize,
    /// Prefetch distance for predictive prefetching
    pub prefetch_distance: usize,
    /// Memory pool size for zero-allocation operations
    pub memory_pool_size: usize,
    /// Adaptive optimization threshold (operations per second)
    pub adaptive_threshold: u64,
    /// Cache hit rate threshold for optimization
    pub cache_hit_threshold: f64,
    /// Performance monitoring interval (seconds)
    pub monitoring_interval: u64,
    /// Enable aggressive prefetching
    pub enable_aggressive_prefetching: bool,
    /// Enable memory layout optimization
    pub enable_memory_optimization: bool,
    /// Enable adaptive batch sizing
    pub enable_adaptive_batching: bool,
    /// Maximum epsilon bound for segments
    pub max_epsilon_bound: usize,
    /// Minimum epsilon bound for segments
    pub min_epsilon_bound: usize,
    /// SIMD width for vectorized operations
    pub simd_width: usize,
    /// Enable cache line prefetching
    pub enable_prefetching: bool,
    /// Prefetch pattern detection window
    pub prefetch_window_size: usize,
    /// Memory alignment for cache optimization
    pub cache_line_alignment: usize,
}

impl Default for RmiOptimizationConfig {
    fn default() -> Self {
        Self {
            // Batch size: total keys processed per function call (AVX2 uses 4 registers to process 16 keys)
            simd_batch_size: 16,
            cache_buffer_size: 64,
            prefetch_distance: 2,
            memory_pool_size: 16,
            adaptive_threshold: 1_000_000,
            cache_hit_threshold: 90.0,
            monitoring_interval: 30,
            enable_aggressive_prefetching: true,
            enable_memory_optimization: true,
            enable_adaptive_batching: true,
            max_epsilon_bound: 256,
            min_epsilon_bound: 32,
            // SIMD register width: AVX2 = 4 u64 per register (256 bits / 64 bits), NEON = 2 u64 per register (128 bits / 64 bits)
            simd_width: 4,
            enable_prefetching: true,
            prefetch_window_size: 1000,
            cache_line_alignment: 64,
        }
    }
}

impl RmiOptimizationConfig {
    /// Create configuration from environment variables with sensible defaults
    pub fn from_env() -> Self {
        let mut config = Self::default();

        // Load SIMD batch size with enhanced validation
        if let Ok(value) = env::var("KYRODB_SIMD_BATCH_SIZE") {
            if let Ok(size) = value.parse::<usize>() {
                // Enhanced validation: ensure power of 2 and within safe bounds
                if size > 0 && size <= 64 && size.is_power_of_two() {
                    config.simd_batch_size = size;
                } else {
                    eprintln!("Warning: Invalid SIMD batch size {}, must be power of 2 between 1-64. Using default: {}", 
                             size, config.simd_batch_size);
                }
            } else {
                eprintln!(
                    "Warning: Could not parse SIMD batch size from '{}'. Using default: {}",
                    value, config.simd_batch_size
                );
            }
        }

        // Load cache buffer size
        if let Ok(value) = env::var("KYRODB_CACHE_BUFFER_SIZE") {
            if let Ok(size) = value.parse::<usize>() {
                config.cache_buffer_size = size.clamp(32, 256);
            }
        }

        // Load prefetch distance
        if let Ok(value) = env::var("KYRODB_PREFETCH_DISTANCE") {
            if let Ok(distance) = value.parse::<usize>() {
                config.prefetch_distance = distance.clamp(1, 8);
            }
        }

        // Load memory pool size
        if let Ok(value) = env::var("KYRODB_MEMORY_POOL_SIZE") {
            if let Ok(size) = value.parse::<usize>() {
                config.memory_pool_size = size.clamp(8, 128);
            }
        }

        // Load adaptive threshold
        if let Ok(value) = env::var("KYRODB_ADAPTIVE_THRESHOLD") {
            if let Ok(threshold) = value.parse::<u64>() {
                config.adaptive_threshold = threshold.clamp(100_000, 10_000_000);
            }
        }

        // Load cache hit threshold
        if let Ok(value) = env::var("KYRODB_CACHE_HIT_THRESHOLD") {
            if let Ok(threshold) = value.parse::<f64>() {
                config.cache_hit_threshold = threshold.clamp(50.0, 99.9);
            }
        }

        // Load monitoring interval
        if let Ok(value) = env::var("KYRODB_MONITORING_INTERVAL") {
            if let Ok(interval) = value.parse::<u64>() {
                config.monitoring_interval = interval.clamp(5, 300);
            }
        }

        // Load feature flags
        config.enable_aggressive_prefetching = env::var("KYRODB_AGGRESSIVE_PREFETCHING")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(true);

        config.enable_memory_optimization = env::var("KYRODB_MEMORY_OPTIMIZATION")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(true);

        config.enable_adaptive_batching = env::var("KYRODB_ADAPTIVE_BATCHING")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(true);

        config.enable_prefetching = env::var("KYRODB_PREFETCHING")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(true);

        // Load epsilon bounds
        if let Ok(value) = env::var("KYRODB_MAX_EPSILON_BOUND") {
            if let Ok(bound) = value.parse::<usize>() {
                config.max_epsilon_bound = bound.clamp(64, 1024);
            }
        }

        if let Ok(value) = env::var("KYRODB_MIN_EPSILON_BOUND") {
            if let Ok(bound) = value.parse::<usize>() {
                config.min_epsilon_bound = bound.clamp(16, 128);
            }
        }

        // Load SIMD width with enhanced validation
        if let Ok(value) = env::var("KYRODB_SIMD_WIDTH") {
            if let Ok(width) = value.parse::<usize>() {
                // Validate SIMD width matches architectural capabilities
                // Register width = values per SIMD register: scalar=1, NEON=2, AVX2=4
                let valid_widths = [1, 2, 4];
                if valid_widths.contains(&width) {
                    config.simd_width = width;
                } else {
                    eprintln!(
                        "Warning: Invalid SIMD width {}, must be 1 (scalar), 2 (NEON), or 4 (AVX2). Using default: {}",
                        width, config.simd_width
                    );
                }
            } else {
                eprintln!(
                    "Warning: Could not parse SIMD width from '{}'. Using default: {}",
                    value, config.simd_width
                );
            }
        }

        // Load prefetch window size
        if let Ok(value) = env::var("KYRODB_PREFETCH_WINDOW_SIZE") {
            if let Ok(size) = value.parse::<usize>() {
                config.prefetch_window_size = size.clamp(100, 10000);
            }
        }

        config
    }

    /// Validate configuration parameters are within valid ranges
    pub fn validate(&self) -> Result<(), String> {
        // Enhanced SIMD batch size validation
        if self.simd_batch_size == 0 {
            return Err("SIMD batch size cannot be zero".to_string());
        }
        if self.simd_batch_size > 64 {
            return Err("SIMD batch size cannot exceed 64 keys".to_string());
        }
        if !self.simd_batch_size.is_power_of_two() {
            return Err("SIMD batch size must be a power of 2".to_string());
        }

        if self.cache_buffer_size < 32 || self.cache_buffer_size > 256 {
            return Err("Cache buffer size must be between 32 and 256".to_string());
        }

        if self.prefetch_distance < 1 || self.prefetch_distance > 8 {
            return Err("Prefetch distance must be between 1 and 8".to_string());
        }

        if self.memory_pool_size < 8 || self.memory_pool_size > 128 {
            return Err("Memory pool size must be between 8 and 128".to_string());
        }

        if self.adaptive_threshold < 100_000 || self.adaptive_threshold > 10_000_000 {
            return Err("Adaptive threshold must be between 100,000 and 10,000,000".to_string());
        }

        if self.cache_hit_threshold < 50.0 || self.cache_hit_threshold > 99.9 {
            return Err("Cache hit threshold must be between 50.0 and 99.9".to_string());
        }

        if self.monitoring_interval < 5 || self.monitoring_interval > 300 {
            return Err("Monitoring interval must be between 5 and 300 seconds".to_string());
        }

        if self.max_epsilon_bound <= self.min_epsilon_bound {
            return Err("Max epsilon bound must be greater than min epsilon bound".to_string());
        }

        // Enhanced SIMD width validation
        // SIMD register width = u64 values per register: 1=scalar, 2=NEON (128-bit), 4=AVX2 (256-bit)
        let valid_simd_widths = [1, 2, 4];
        if !valid_simd_widths.contains(&self.simd_width) {
            return Err("SIMD width must be 1 (scalar), 2 (NEON), or 4 (AVX2)".to_string());
        }

        if self.prefetch_window_size < 100 || self.prefetch_window_size > 10000 {
            return Err("Prefetch window size must be between 100 and 10000".to_string());
        }

        // Cross-validation: ensure SIMD batch size is compatible with SIMD width
        if self.simd_batch_size < self.simd_width {
            return Err(format!(
                "SIMD batch size ({}) must be at least as large as SIMD width ({})",
                self.simd_batch_size, self.simd_width
            ));
        }

        Ok(())
    }

    /// Get optimized batch size based on current configuration
    pub fn get_optimized_batch_size(&self, current_throughput: u64) -> usize {
        if !self.enable_adaptive_batching {
            return self.simd_batch_size;
        }

        // Adaptive batch sizing based on throughput
        if current_throughput > self.adaptive_threshold * 10 {
            // High throughput - use larger batches
            (self.simd_batch_size * 2).min(64)
        } else if current_throughput > self.adaptive_threshold {
            // Medium throughput - use configured batch size
            self.simd_batch_size
        } else {
            // Low throughput - use smaller batches for better latency
            (self.simd_batch_size / 2).max(8)
        }
    }

    /// Get optimized prefetch distance based on access patterns
    pub fn get_optimized_prefetch_distance(&self, cache_hit_rate: f64) -> usize {
        if !self.enable_prefetching {
            return 0;
        }

        if cache_hit_rate < self.cache_hit_threshold {
            // Low cache hit rate - increase prefetch distance
            (self.prefetch_distance * 2).min(8)
        } else {
            // High cache hit rate - use configured distance
            self.prefetch_distance
        }
    }

    /// Get optimized epsilon bound based on segment characteristics
    pub fn get_optimized_epsilon_bound(&self, segment_size: usize) -> usize {
        if segment_size < 100 {
            // Small segments - use smaller epsilon
            self.min_epsilon_bound
        } else if segment_size > 1000 {
            // Large segments - use larger epsilon
            self.max_epsilon_bound
        } else {
            // Medium segments - use average epsilon
            (self.min_epsilon_bound + self.max_epsilon_bound) / 2
        }
    }

    /// Get performance targets for optimization
    pub fn get_performance_targets(&self) -> PerformanceTargets {
        PerformanceTargets {
            target_latency_us: 100, // 100 microseconds
            target_throughput_ops_per_sec: self.adaptive_threshold,
            target_cache_hit_rate: self.cache_hit_threshold,
            target_memory_usage_mb: 1024, // 1GB
        }
    }
}

/// Performance targets for optimization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceTargets {
    pub target_latency_us: u64,
    pub target_throughput_ops_per_sec: u64,
    pub target_cache_hit_rate: f64,
    pub target_memory_usage_mb: usize,
}

/// Configuration validator for validating configuration parameters
pub struct ConfigValidator;

impl ConfigValidator {
    /// Validate all configuration parameters
    pub fn validate_all(config: &RmiOptimizationConfig) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();

        if let Err(e) = config.validate() {
            errors.push(e);
        }

        // Additional validation checks
        if config.simd_batch_size % 8 != 0
            && config.simd_batch_size != 1
            && config.simd_batch_size != 2
            && config.simd_batch_size != 4
        {
            errors.push("SIMD batch size must be 1, 2, 4, or a multiple of 8".to_string());
        }

        // Ensure cache buffer size is properly aligned
        if config.cache_buffer_size % config.cache_line_alignment != 0 {
            errors.push("Cache buffer size must be aligned to cache line size".to_string());
        }

        if config.prefetch_distance > config.prefetch_window_size / 10 {
            errors.push("Prefetch distance too large for window size".to_string());
        }

        // Validate SIMD configuration consistency
        if config.simd_width > config.simd_batch_size {
            errors.push("SIMD width cannot be larger than batch size".to_string());
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

/// Configuration helper functions

/// Load configuration from a JSON file
pub fn load_config_from_file(
    path: &str,
) -> Result<RmiOptimizationConfig, Box<dyn std::error::Error>> {
    let content = std::fs::read_to_string(path)?;
    let config: RmiOptimizationConfig = serde_json::from_str(&content)?;
    config.validate()?;
    Ok(config)
}

/// Save configuration to a JSON file
pub fn save_config_to_file(
    config: &RmiOptimizationConfig,
    path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let content = serde_json::to_string_pretty(config)?;
    std::fs::write(path, content)?;
    Ok(())
}

/// Get the default configuration file path
pub fn get_default_config_path() -> String {
    "kyrodb_rmi_config.json".to_string()
}

/// Create a default configuration file
pub fn create_default_config_file(path: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    let default_path = get_default_config_path();
    let config_path = path.unwrap_or(&default_path);
    let config = RmiOptimizationConfig::default();
    save_config_to_file(&config, config_path)?;
    println!("Created default RMI configuration file: {}", config_path);
    Ok(())
}
