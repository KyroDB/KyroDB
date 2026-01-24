// engine/src/config.rs
//
// Configuration Management Module
//
// Provides comprehensive configuration management for KyroDB with multiple sources.
// Priority order (highest to lowest):
// 1. Command-line arguments (handled by server binary via clap, then passed to load())
// 2. Environment variables (KYRODB__* prefix) - highest priority in this module
// 3. Config file (YAML/TOML)
// 4. Built-in defaults (lowest priority)
//
// Design principles:
// - Sensible defaults (works out of the box)
// - Type-safe enums instead of strings
// - Clear validation errors
// - Optional config file (for ease of use)

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;

// ============================================================================
// Main Configuration Structure
// ============================================================================

/// Complete KyroDB configuration with all tunable parameters
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct KyroDbConfig {
    /// Server configuration (gRPC and HTTP endpoints)
    pub server: ServerConfig,

    /// Vector cache configuration (Hybrid Semantic Cache layer)
    pub cache: CacheConfig,

    /// HNSW index configuration (vector search backend)
    pub hnsw: HnswConfig,

    /// Persistence configuration (WAL and snapshots)
    pub persistence: PersistenceConfig,

    /// SLO thresholds for alerting
    pub slo: SloConfig,

    /// Rate limiting configuration
    pub rate_limit: RateLimitConfig,

    /// Logging configuration
    pub logging: LoggingConfig,

    /// Authentication and authorization configuration
    pub auth: AuthConfig,

    /// Timeout configuration for tiered engine operations
    pub timeouts: TimeoutConfig,
}

// ============================================================================
// Server Configuration
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    /// gRPC server host (IPv4 or IPv6)
    pub host: String,

    /// gRPC server port
    pub port: u16,

    /// HTTP observability server port (defaults to gRPC port + 1000)
    pub http_port: Option<u16>,

    /// Maximum number of concurrent connections
    pub max_connections: usize,

    /// Connection idle timeout (seconds)
    pub connection_timeout_secs: u64,

    /// Graceful shutdown timeout (seconds)
    pub shutdown_timeout_secs: u64,

    /// TLS configuration for secure connections
    pub tls: TlsConfig,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 50051,
            http_port: None,
            max_connections: 10_000,
            connection_timeout_secs: 300,
            shutdown_timeout_secs: 30,
            tls: TlsConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct TlsConfig {
    /// Enable TLS for gRPC server
    pub enabled: bool,

    /// Path to PEM-encoded server certificate
    pub cert_path: Option<PathBuf>,

    /// Path to PEM-encoded server private key
    pub key_path: Option<PathBuf>,

    /// Path to CA certificate for client verification (mTLS)
    pub ca_cert_path: Option<PathBuf>,

    /// Require client certificate (mTLS)
    pub require_client_cert: bool,
}

// ============================================================================
// Cache Configuration
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CacheConfig {
    /// Maximum number of vectors to cache
    pub capacity: usize,

    /// Cache eviction strategy
    pub strategy: CacheStrategy,

    /// Training interval for Hybrid Semantic Cache (seconds)
    pub training_interval_secs: u64,

    /// Training window duration (seconds) - how far back to consider accesses
    pub training_window_secs: u64,

    /// Recency decay half-life (seconds) - exponential decay for old accesses
    pub recency_halflife_secs: u64,

    /// Minimum access count before training
    pub min_training_samples: usize,

    /// Enable A/B testing between strategies
    pub enable_ab_testing: bool,

    /// A/B test traffic split (0.0-1.0, fraction for treatment group)
    pub ab_test_split: f64,

    /// Admission threshold for cache (0.0-1.0)
    pub admission_threshold: f32,

    /// Auto-tune admission threshold based on utilization
    pub auto_tune_threshold: bool,

    /// Target cache utilization for auto-tuning (0.0-1.0)
    pub target_utilization: f32,

    /// Enable query clustering for semantic cache optimization
    pub enable_query_clustering: bool,

    /// Cosine similarity threshold for query clustering (0.0-1.0)
    pub clustering_similarity_threshold: f32,

    /// Enable predictive prefetching
    pub enable_prefetching: bool,

    /// Prefetch threshold - minimum hotness score to prefetch (0.0-1.0)
    pub prefetch_threshold: f32,

    /// Maximum documents to prefetch per access
    pub max_prefetch_per_doc: usize,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            capacity: 10_000,
            strategy: CacheStrategy::Learned,
            training_interval_secs: 600,
            training_window_secs: 3600,
            recency_halflife_secs: 1800,
            min_training_samples: 100,
            enable_ab_testing: false,
            ab_test_split: 0.5,
            admission_threshold: 0.15,
            auto_tune_threshold: true,
            target_utilization: 0.85,
            enable_query_clustering: true,
            clustering_similarity_threshold: 0.85,
            enable_prefetching: true,
            prefetch_threshold: 0.10,
            max_prefetch_per_doc: 5,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CacheStrategy {
    /// LRU eviction (baseline)
    Lru,
    /// Hybrid Semantic Cache with RMI predictor
    Learned,
    /// A/B test between LRU and Learned
    AbTest,
}

// ============================================================================
// HNSW Configuration
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct HnswConfig {
    /// Maximum number of vectors in index
    pub max_elements: usize,

    /// Number of bidirectional links per node (M parameter)
    pub m: usize,

    /// Size of dynamic candidate list during construction (ef_construction)
    pub ef_construction: usize,

    /// Size of dynamic candidate list during search (ef_search)
    pub ef_search: usize,

    /// Vector dimension (must match your embeddings)
    pub dimension: usize,

    /// Distance metric
    pub distance: DistanceMetric,

    /// Disable L2-normalization checks for inner-product vectors (performance opt-out)
    pub disable_normalization_check: bool,
}

impl Default for HnswConfig {
    fn default() -> Self {
        Self {
            max_elements: 100_000,
            m: 16,
            ef_construction: 200,
            ef_search: 50,
            dimension: 768, // Common for sentence transformers
            distance: DistanceMetric::Cosine,
            disable_normalization_check: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DistanceMetric {
    /// Cosine similarity (1 - cosine distance)
    Cosine,
    /// Euclidean distance (L2)
    Euclidean,
    /// Inner product (dot product)
    InnerProduct,
}

impl Default for DistanceMetric {
    fn default() -> Self {
        DistanceMetric::Cosine
    }
}

// ============================================================================
// Persistence Configuration
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PersistenceConfig {
    /// Data directory for WAL and snapshots
    pub data_dir: PathBuf,

    /// Enable write-ahead log
    pub enable_wal: bool,

    /// WAL flush interval (milliseconds, 0 = immediate)
    pub wal_flush_interval_ms: u64,

    /// fsync policy
    pub fsync_policy: FsyncPolicy,

    /// Snapshot interval (seconds, 0 = disabled)
    pub snapshot_interval_secs: u64,

    /// Maximum WAL size before rotation (bytes)
    pub max_wal_size_bytes: u64,

    /// Enable automatic crash recovery on startup
    pub enable_recovery: bool,

    /// If true, allow the server to start with a fresh empty database when recovery fails.
    ///
    /// This is dangerous in production because it can silently discard access to existing data
    /// if the on-disk state is corrupted or misconfigured.
    pub allow_fresh_start_on_recovery_failure: bool,
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data"),
            enable_wal: true,
            wal_flush_interval_ms: 100,
            fsync_policy: FsyncPolicy::DataOnly,
            snapshot_interval_secs: 3600,          // 1 hour
            max_wal_size_bytes: 100 * 1024 * 1024, // 100 MB
            enable_recovery: true,
            allow_fresh_start_on_recovery_failure: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FsyncPolicy {
    /// No fsync (fastest, data loss on crash)
    None,
    /// fsync data only (good balance)
    DataOnly,
    /// fsync data and metadata (safest, slowest)
    Full,
}

// ============================================================================
// SLO Configuration
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SloConfig {
    /// P99 latency threshold (milliseconds)
    pub p99_latency_ms: f64,

    /// Cache hit rate threshold (0.0-1.0)
    pub cache_hit_rate: f64,

    /// Error rate threshold (0.0-1.0)
    pub error_rate: f64,

    /// Availability threshold (0.0-1.0)
    pub availability: f64,

    /// Minimum sample size before alerting
    pub min_samples: usize,
}

impl Default for SloConfig {
    fn default() -> Self {
        Self {
            p99_latency_ms: 1.0,
            cache_hit_rate: 0.70,
            error_rate: 0.001,
            availability: 0.999,
            min_samples: 100,
        }
    }
}

// ============================================================================
// Rate Limiting Configuration
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RateLimitConfig {
    /// Enable rate limiting
    pub enabled: bool,

    /// Maximum queries per second per connection
    pub max_qps_per_connection: usize,

    /// Maximum queries per second globally
    pub max_qps_global: usize,

    /// Burst capacity (tokens in bucket)
    pub burst_capacity: usize,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_qps_per_connection: 1_000,
            max_qps_global: 100_000,
            burst_capacity: 10_000,
        }
    }
}

// ============================================================================
// Logging Configuration
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct LoggingConfig {
    /// Log level (trace, debug, info, warn, error)
    pub level: LogLevel,

    /// Log format (json or text)
    pub format: LogFormat,

    /// Log to file (path, or None for stdout only)
    pub file: Option<PathBuf>,

    /// Enable log rotation
    pub rotation: bool,

    /// Maximum log file size before rotation (bytes)
    pub max_file_size_bytes: u64,

    /// Maximum number of rotated log files to keep
    pub max_files: usize,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: LogLevel::Info,
            format: LogFormat::Text,
            file: None, // stdout only
            rotation: true,
            max_file_size_bytes: 100 * 1024 * 1024, // 100 MB
            max_files: 10,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl LogLevel {
    pub fn as_str(&self) -> &'static str {
        match self {
            LogLevel::Trace => "trace",
            LogLevel::Debug => "debug",
            LogLevel::Info => "info",
            LogLevel::Warn => "warn",
            LogLevel::Error => "error",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    /// Human-readable text format
    Text,
    /// JSON format (for log aggregation)
    Json,
}

// ============================================================================
// Authentication Configuration
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AuthConfig {
    /// Enable authentication (default: false for backward compatibility)
    pub enabled: bool,

    /// Path to API keys file (YAML format)
    pub api_keys_file: Option<PathBuf>,

    /// Path to usage stats export (CSV)
    pub usage_stats_file: PathBuf,

    /// How often to export usage stats (seconds)
    pub usage_export_interval_secs: u64,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            enabled: false, // Disabled by default (backward compatibility)
            api_keys_file: None,
            usage_stats_file: PathBuf::from("data/usage_stats.csv"),
            usage_export_interval_secs: 300, // Every 5 minutes
        }
    }
}

// ============================================================================
// Timeout Configuration
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct TimeoutConfig {
    /// Cache tier timeout in milliseconds (default: 10ms)
    pub cache_ms: u64,

    /// Hot tier timeout in milliseconds (default: 50ms)
    pub hot_tier_ms: u64,

    /// Cold tier timeout in milliseconds (default: 1000ms)
    pub cold_tier_ms: u64,
}

impl Default for TimeoutConfig {
    fn default() -> Self {
        Self {
            cache_ms: 10,
            hot_tier_ms: 50,
            cold_tier_ms: 1000,
        }
    }
}

// ============================================================================
// Configuration Loading
// ============================================================================

impl KyroDbConfig {
    /// Load configuration from file (YAML or TOML)
    pub fn from_file(path: &str) -> Result<Self> {
        let settings = config::Config::builder()
            .add_source(config::File::with_name(path))
            .add_source(
                config::Environment::with_prefix("KYRODB")
                    .separator("__")
                    .try_parsing(true),
            )
            .build()
            .context("Failed to build config")?;

        settings
            .try_deserialize()
            .context("Failed to deserialize config")
    }

    /// Load configuration with priority chain:
    /// 1. Environment variables (KYRODB_*)
    /// 2. Config file (if provided)
    /// 3. Built-in defaults
    pub fn load(config_file: Option<&str>) -> Result<Self> {
        let mut builder = config::Config::builder();

        // Start with defaults (converted to config source)
        let defaults = Self::default();
        let defaults_json =
            serde_json::to_string(&defaults).context("Failed to serialize defaults")?;
        builder = builder.add_source(config::File::from_str(
            &defaults_json,
            config::FileFormat::Json,
        ));

        // Add config file if provided
        if let Some(path) = config_file {
            builder = builder.add_source(
                config::File::with_name(path).required(false), // Don't fail if file doesn't exist
            );
        }

        // Add environment variables (highest priority)
        builder = builder.add_source(
            config::Environment::with_prefix("KYRODB")
                .separator("__")
                .try_parsing(true),
        );

        let settings = builder.build().context("Failed to build config")?;

        let config: Self = settings
            .try_deserialize()
            .context("Failed to deserialize config")?;

        config.validate()?;
        Ok(config)
    }

    /// Validate configuration parameters
    pub fn validate(&self) -> Result<()> {
        // Server validation
        anyhow::ensure!(
            self.server.port > 0,
            "Server port must be > 0, got {}",
            self.server.port
        );

        // Validate HTTP port (auto-calculated or explicit)
        // Note: Since port is u16, it's automatically <= 65535
        // But we check for overflow when auto-calculating (port + 1000)
        let http_port = self.http_port();
        anyhow::ensure!(
            (self.server.port as u32 + 1000) <= 65535 || self.server.http_port.is_some(),
            "HTTP port would exceed 65535 (gRPC port {} + 1000 = {}). Set http_port explicitly.",
            self.server.port,
            self.server.port as u32 + 1000
        );
        anyhow::ensure!(
            http_port != self.server.port,
            "HTTP port ({}) must be different from gRPC port ({})",
            http_port,
            self.server.port
        );

        anyhow::ensure!(
            self.server.max_connections > 0,
            "max_connections must be > 0, got {}",
            self.server.max_connections
        );

        // Cache validation
        anyhow::ensure!(
            self.cache.capacity > 0,
            "Cache capacity must be > 0, got {}",
            self.cache.capacity
        );
        anyhow::ensure!(
            self.cache.ab_test_split >= 0.0 && self.cache.ab_test_split <= 1.0,
            "A/B test split must be in [0.0, 1.0], got {}",
            self.cache.ab_test_split
        );
        anyhow::ensure!(
            self.cache.min_training_samples > 0
                && self.cache.min_training_samples <= self.cache.capacity,
            "min_training_samples must be in range [1, {}], got {}",
            self.cache.capacity,
            self.cache.min_training_samples
        );
        anyhow::ensure!(
            self.cache.training_interval_secs > 0,
            "training_interval_secs must be > 0, got {}",
            self.cache.training_interval_secs
        );

        // HNSW validation - basic bounds
        anyhow::ensure!(
            self.hnsw.max_elements > 0,
            "HNSW max_elements must be > 0, got {}",
            self.hnsw.max_elements
        );
        anyhow::ensure!(
            self.hnsw.m >= 5 && self.hnsw.m <= 48,
            "HNSW M parameter should be in range [5, 48] for optimal performance, got {}",
            self.hnsw.m
        );
        anyhow::ensure!(
            self.hnsw.ef_construction > 0,
            "HNSW ef_construction must be > 0, got {}",
            self.hnsw.ef_construction
        );
        anyhow::ensure!(
            self.hnsw.ef_search > 0,
            "HNSW ef_search must be > 0, got {}",
            self.hnsw.ef_search
        );
        anyhow::ensure!(
            self.hnsw.dimension > 0,
            "HNSW dimension must be > 0, got {}",
            self.hnsw.dimension
        );

        // HNSW parameter relationships
        anyhow::ensure!(
            self.hnsw.ef_construction >= self.hnsw.m,
            "HNSW ef_construction ({}) should be >= M ({})",
            self.hnsw.ef_construction,
            self.hnsw.m
        );

        // Warn if ef_search > ef_construction (not an error, but likely suboptimal)
        if self.hnsw.ef_search > self.hnsw.ef_construction {
            eprintln!(
                "Warning: ef_search ({}) > ef_construction ({}) may not improve recall",
                self.hnsw.ef_search, self.hnsw.ef_construction
            );
        }

        // Persistence validation
        anyhow::ensure!(
            !self.persistence.data_dir.as_os_str().is_empty(),
            "Persistence data_dir cannot be empty"
        );
        anyhow::ensure!(
            self.persistence.max_wal_size_bytes > 0,
            "max_wal_size_bytes must be > 0, got {}",
            self.persistence.max_wal_size_bytes
        );

        // SLO validation
        anyhow::ensure!(
            self.slo.p99_latency_ms > 0.0,
            "SLO P99 latency must be > 0, got {}",
            self.slo.p99_latency_ms
        );
        anyhow::ensure!(
            self.slo.cache_hit_rate >= 0.0 && self.slo.cache_hit_rate <= 1.0,
            "SLO cache hit rate must be in [0.0, 1.0], got {}",
            self.slo.cache_hit_rate
        );
        anyhow::ensure!(
            self.slo.error_rate >= 0.0 && self.slo.error_rate <= 1.0,
            "SLO error rate must be in [0.0, 1.0], got {}",
            self.slo.error_rate
        );
        anyhow::ensure!(
            self.slo.availability >= 0.0 && self.slo.availability <= 1.0,
            "SLO availability must be in [0.0, 1.0], got {}",
            self.slo.availability
        );

        // Rate limit validation
        if self.rate_limit.enabled {
            anyhow::ensure!(
                self.rate_limit.max_qps_per_connection > 0,
                "max_qps_per_connection must be > 0, got {}",
                self.rate_limit.max_qps_per_connection
            );
            anyhow::ensure!(
                self.rate_limit.max_qps_global > 0,
                "max_qps_global must be > 0, got {}",
                self.rate_limit.max_qps_global
            );
        }

        // Authentication validation
        if self.auth.enabled {
            anyhow::ensure!(
                self.auth.api_keys_file.is_some(),
                "api_keys_file must be provided when authentication is enabled"
            );
        }
        anyhow::ensure!(
            self.auth.usage_export_interval_secs > 0,
            "usage_export_interval_secs must be > 0, got {}",
            self.auth.usage_export_interval_secs
        );

        // TLS validation
        if self.server.tls.enabled {
            anyhow::ensure!(
                self.server.tls.cert_path.is_some(),
                "TLS enabled but cert_path not provided"
            );
            anyhow::ensure!(
                self.server.tls.key_path.is_some(),
                "TLS enabled but key_path not provided"
            );
            if self.server.tls.require_client_cert {
                anyhow::ensure!(
                    self.server.tls.ca_cert_path.is_some(),
                    "mTLS enabled but ca_cert_path not provided"
                );
            }
        }

        Ok(())
    }

    /// Get the HTTP observability port (auto-calculated if not set)
    ///
    /// If http_port is not explicitly set, returns gRPC port + 1000.
    /// Note: Validation ensures this doesn't overflow (checked in validate()).
    pub fn http_port(&self) -> u16 {
        self.server
            .http_port
            .unwrap_or_else(|| self.server.port.saturating_add(1000))
    }

    /// Get connection timeout as Duration
    pub fn connection_timeout(&self) -> Duration {
        Duration::from_secs(self.server.connection_timeout_secs)
    }

    /// Get shutdown timeout as Duration
    pub fn shutdown_timeout(&self) -> Duration {
        Duration::from_secs(self.server.shutdown_timeout_secs)
    }

    /// Get WAL flush interval as Duration
    pub fn wal_flush_interval(&self) -> Duration {
        Duration::from_millis(self.persistence.wal_flush_interval_ms)
    }

    /// Get snapshot interval as Duration
    pub fn snapshot_interval(&self) -> Duration {
        Duration::from_secs(self.persistence.snapshot_interval_secs)
    }

    /// Get cache training interval as Duration
    pub fn cache_training_interval(&self) -> Duration {
        Duration::from_secs(self.cache.training_interval_secs)
    }

    /// Get usage export interval as Duration
    pub fn usage_export_interval(&self) -> Duration {
        Duration::from_secs(self.auth.usage_export_interval_secs)
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Generate YAML config with default values (without explanatory comments).
///
/// This outputs a valid YAML config file with default values only.
/// For a fully-documented example with comments explaining each option,
/// see `config.example.yaml` in the repository root.
///
/// # Example
/// ```bash
/// kyrodb_server --generate-config yaml > my_config.yaml
/// ```
pub fn generate_example_yaml() -> String {
    let config = KyroDbConfig::default();
    serde_yaml::to_string(&config).expect("Failed to serialize default config")
}

/// Generate TOML config with default values (without explanatory comments).
///
/// This outputs a valid TOML config file with default values only.
/// For a fully-documented example with comments explaining each option,
/// see `config.example.toml` in the repository root.
///
/// # Example
/// ```bash
/// kyrodb_server --generate-config toml > my_config.toml
/// ```
pub fn generate_example_toml() -> String {
    let config = KyroDbConfig::default();
    toml::to_string_pretty(&config).expect("Failed to serialize default config")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config_validates() {
        let config = KyroDbConfig::default();
        config.validate().expect("Default config should be valid");
    }

    #[test]
    fn test_invalid_cache_capacity() {
        let mut config = KyroDbConfig::default();
        config.cache.capacity = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_invalid_ab_test_split() {
        let mut config = KyroDbConfig::default();
        config.cache.ab_test_split = 1.5;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_http_port_auto_calculation() {
        let config = KyroDbConfig::default();
        assert_eq!(config.http_port(), 51051); // 50051 + 1000
    }

    #[test]
    fn test_http_port_explicit() {
        let mut config = KyroDbConfig::default();
        config.server.http_port = Some(8080);
        assert_eq!(config.http_port(), 8080);
    }

    #[test]
    fn test_duration_conversions() {
        let config = KyroDbConfig::default();
        assert_eq!(config.connection_timeout(), Duration::from_secs(300));
        assert_eq!(config.shutdown_timeout(), Duration::from_secs(30));
        assert_eq!(config.wal_flush_interval(), Duration::from_millis(100));
        assert_eq!(config.snapshot_interval(), Duration::from_secs(3600));
        assert_eq!(config.cache_training_interval(), Duration::from_secs(600));
    }

    #[test]
    fn test_log_level_conversion() {
        assert_eq!(LogLevel::Info.as_str(), "info");
        assert_eq!(LogLevel::Debug.as_str(), "debug");
        assert_eq!(LogLevel::Error.as_str(), "error");
    }

    // ===== New Validation Tests =====

    #[test]
    fn test_port_collision_validation() {
        let mut config = KyroDbConfig::default();
        config.server.http_port = Some(50051); // Same as gRPC port
        assert!(config.validate().is_err(), "Should reject colliding ports");
    }

    #[test]
    fn test_http_port_overflow() {
        let mut config = KyroDbConfig::default();
        config.server.port = 65000; // Would overflow (65000 + 1000 > 65535)
        assert!(
            config.validate().is_err(),
            "Should reject port that would overflow"
        );
    }

    #[test]
    fn test_cache_min_training_samples_validation() {
        let mut config = KyroDbConfig::default();

        // Test: min_training_samples = 0 should fail
        config.cache.min_training_samples = 0;
        assert!(config.validate().is_err());

        // Test: min_training_samples > capacity should fail
        config.cache.min_training_samples = config.cache.capacity + 1;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_cache_training_interval_validation() {
        let mut config = KyroDbConfig::default();
        config.cache.training_interval_secs = 0;
        assert!(
            config.validate().is_err(),
            "training_interval_secs must be > 0"
        );
    }

    #[test]
    fn test_hnsw_m_parameter_range() {
        let mut config = KyroDbConfig::default();

        // M too small
        config.hnsw.m = 4;
        assert!(config.validate().is_err(), "M < 5 should fail");

        // M too large
        config.hnsw.m = 50;
        assert!(config.validate().is_err(), "M > 48 should fail");

        // M in valid range
        config.hnsw.m = 16;
        assert!(config.validate().is_ok(), "M = 16 should pass");
    }

    #[test]
    fn test_hnsw_ef_construction_vs_m() {
        let mut config = KyroDbConfig::default();
        config.hnsw.m = 20;
        config.hnsw.ef_construction = 15; // Less than M
        assert!(
            config.validate().is_err(),
            "ef_construction < M should fail"
        );
    }

    #[test]
    fn test_valid_config_passes_all_checks() {
        let config = KyroDbConfig::default();
        assert!(
            config.validate().is_ok(),
            "Default config should pass all validations"
        );
    }
}
