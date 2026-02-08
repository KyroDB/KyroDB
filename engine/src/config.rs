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
#[serde(default, deny_unknown_fields)]
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

    /// Environment declaration (benchmark vs production)
    pub environment: EnvironmentConfig,
}

// ============================================================================
// Server Configuration
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ServerConfig {
    /// gRPC server host (IPv4 or IPv6)
    pub host: String,

    /// gRPC server port
    pub port: u16,

    /// HTTP observability server port (defaults to gRPC port + 1000)
    pub http_port: Option<u16>,

    /// HTTP observability server host (defaults to `server.host` if omitted)
    ///
    /// This allows binding the observability endpoints to a different interface
    /// (e.g., loopback-only) without changing the gRPC bind address.
    pub http_host: Option<String>,

    /// Authentication policy for HTTP observability endpoints.
    pub observability_auth: ObservabilityAuthMode,

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
            http_host: None,
            observability_auth: ObservabilityAuthMode::Disabled,
            max_connections: 10_000,
            connection_timeout_secs: 300,
            shutdown_timeout_secs: 30,
            tls: TlsConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ObservabilityAuthMode {
    /// No authentication on HTTP observability endpoints.
    #[default]
    Disabled,

    /// Require auth for `/metrics` and `/slo` only.
    MetricsAndSlo,

    /// Require auth for `/metrics`, `/health`, `/ready`, and `/slo`.
    All,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
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
#[serde(default, deny_unknown_fields)]
pub struct CacheConfig {
    /// Maximum number of vectors to cache
    pub capacity: usize,

    /// Cache eviction strategy
    pub strategy: CacheStrategy,

    /// Training interval for Hybrid Semantic Cache (seconds)
    pub training_interval_secs: u64,

    /// Whether to run the background training task in `kyrodb_server`.
    ///
    /// When enabled, the server logs accesses into an in-memory ring buffer and
    /// periodically retrains the learned cache predictor.
    pub enable_training_task: bool,

    /// Access logger ring buffer capacity (number of access events).
    ///
    /// Memory usage is roughly `capacity × 24 bytes`.
    pub logger_window_size: usize,

    /// Predictor capacity multiplier relative to L1a cache capacity.
    ///
    /// The predictor can track more candidates than the cache can hold;
    /// retraining then selects the hottest subset.
    pub predictor_capacity_multiplier: usize,

    /// Query cache (L1b) capacity (number of cached queries).
    pub query_cache_capacity: usize,

    /// Query cache (L1b) cosine similarity threshold for semantic matches.
    pub query_cache_similarity_threshold: f32,

    /// Optional max age (seconds) for Hot Tier flush decisions.
    ///
    /// If not set, the server falls back to `training_interval_secs` for
    /// backward compatibility.
    pub hot_tier_max_age_secs: Option<u64>,

    /// Training window duration (seconds) - how far back to consider accesses
    pub training_window_secs: u64,

    /// Recency decay half-life (seconds) - exponential decay for old accesses
    pub recency_halflife_secs: u64,

    /// Minimum access count before training
    pub min_training_samples: usize,

    /// Admission threshold for cache (0.0-1.0)
    pub admission_threshold: f32,

    /// Auto-tune admission threshold based on utilization
    pub auto_tune_threshold: bool,

    /// Target cache utilization for auto-tuning (0.0-1.0)
    pub target_utilization: f32,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            capacity: 10_000,
            strategy: CacheStrategy::Learned,
            training_interval_secs: 600,
            enable_training_task: true,
            logger_window_size: 1_000_000,
            predictor_capacity_multiplier: 4,
            query_cache_capacity: 100,
            query_cache_similarity_threshold: 0.52,
            hot_tier_max_age_secs: None,
            training_window_secs: 3600,
            recency_halflife_secs: 1800,
            min_training_samples: 100,
            admission_threshold: 0.15,
            auto_tune_threshold: true,
            target_utilization: 0.85,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CacheStrategy {
    /// LRU eviction (baseline)
    Lru,
    /// Hybrid Semantic Cache with Learned predictor
    Learned,
    /// A/B test between LRU and Learned
    AbTest,
}

// ============================================================================
// HNSW Configuration
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum DistanceMetric {
    /// Cosine similarity (1 - cosine distance)
    #[default]
    Cosine,
    /// Euclidean distance (L2)
    Euclidean,
    /// Inner product (dot product)
    InnerProduct,
}

// ============================================================================
// Persistence Configuration
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PersistenceConfig {
    /// Data directory for WAL and snapshots
    pub data_dir: PathBuf,

    /// WAL flush interval (milliseconds, 0 = immediate)
    pub wal_flush_interval_ms: u64,

    /// fsync policy
    pub fsync_policy: FsyncPolicy,

    /// Snapshot interval (number of WAL mutations including inserts and deletes, 0 = disabled).
    ///
    /// Previously named `snapshot_interval_inserts`; the old key is accepted as a
    /// backward-compatible alias during deserialization.
    #[serde(alias = "snapshot_interval_inserts")]
    pub snapshot_interval_mutations: u64,

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
            wal_flush_interval_ms: 100,
            fsync_policy: FsyncPolicy::DataOnly,
            snapshot_interval_mutations: 10_000,
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
#[serde(default, deny_unknown_fields)]
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
            // Default to a realistic end-to-end query SLO.
            //
            // Note: `kyrodb_query_latency_ns` includes cold-tier HNSW misses, so a 1ms
            // default would mark the server as degraded/unhealthy for typical workloads.
            p99_latency_ms: 10.0,
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
#[serde(default, deny_unknown_fields)]
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
#[serde(default, deny_unknown_fields)]
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

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct AuthConfig {
    /// Enable authentication (default: false for backward compatibility)
    pub enabled: bool,

    /// Path to API keys file (YAML format)
    pub api_keys_file: Option<PathBuf>,
}

// ============================================================================
// Timeout Configuration
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
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
// Environment Configuration
// ============================================================================

/// Declares the intended deployment context for this configuration.
///
/// - `production` (default): standard safety checks.
/// - `pilot`: stricter launch checks (auth/rate-limit/observability guardrails).
/// - `benchmark`: permits unsafe persistence settings for performance experiments.
///
/// When `type` is not `"benchmark"`, the server validates that persistence
/// settings are safe for production (fsync_policy != none, snapshots enabled).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct EnvironmentConfig {
    /// Environment type: `"production"` (default), `"pilot"`, or `"benchmark"`.
    /// Benchmark mode permits unsafe persistence settings; non-benchmark modes
    /// trigger persistence safety validation on startup.
    #[serde(rename = "type")]
    pub environment_type: String,
}

impl Default for EnvironmentConfig {
    fn default() -> Self {
        Self {
            environment_type: "production".to_string(),
        }
    }
}

fn is_loopback_host(host: &str) -> bool {
    matches!(host.trim(), "127.0.0.1" | "::1" | "localhost")
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
                    .prefix_separator("__")
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
    /// 1. Environment variables (KYRODB__*)
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
                .prefix_separator("__")
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
        let environment_type = self
            .environment
            .environment_type
            .trim()
            .to_ascii_lowercase();
        anyhow::ensure!(
            !environment_type.is_empty(),
            "environment.type cannot be empty"
        );
        anyhow::ensure!(
            matches!(
                environment_type.as_str(),
                "production" | "pilot" | "benchmark"
            ),
            "invalid environment.type '{}': expected one of production, pilot, benchmark",
            self.environment.environment_type
        );

        // Server validation
        anyhow::ensure!(
            self.server.port > 0,
            "Server port must be > 0, got {}",
            self.server.port
        );

        if let Some(http_host) = &self.server.http_host {
            anyhow::ensure!(
                !http_host.trim().is_empty(),
                "server.http_host cannot be empty when set"
            );
        }

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
            self.cache.query_cache_capacity > 0,
            "query_cache_capacity must be > 0, got {}",
            self.cache.query_cache_capacity
        );
        anyhow::ensure!(
            (0.0..=1.0).contains(&self.cache.query_cache_similarity_threshold),
            "query_cache_similarity_threshold must be in [0.0, 1.0], got {}",
            self.cache.query_cache_similarity_threshold
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

        anyhow::ensure!(
            self.cache.logger_window_size > 0,
            "logger_window_size must be > 0, got {}",
            self.cache.logger_window_size
        );
        anyhow::ensure!(
            self.cache.predictor_capacity_multiplier > 0,
            "predictor_capacity_multiplier must be > 0, got {}",
            self.cache.predictor_capacity_multiplier
        );
        if let Some(age) = self.cache.hot_tier_max_age_secs {
            anyhow::ensure!(
                age > 0,
                "hot_tier_max_age_secs must be > 0 when set, got {:?}",
                age
            );
        }

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
            self.slo.p99_latency_ms.is_finite() && self.slo.p99_latency_ms > 0.0,
            "SLO P99 latency must be a finite value > 0, got {}",
            self.slo.p99_latency_ms
        );
        anyhow::ensure!(
            self.slo.cache_hit_rate.is_finite() && (0.0..=1.0).contains(&self.slo.cache_hit_rate),
            "SLO cache hit rate must be a finite value in [0.0, 1.0], got {}",
            self.slo.cache_hit_rate
        );
        anyhow::ensure!(
            self.slo.error_rate.is_finite() && (0.0..=1.0).contains(&self.slo.error_rate),
            "SLO error rate must be a finite value in [0.0, 1.0], got {}",
            self.slo.error_rate
        );
        anyhow::ensure!(
            self.slo.availability.is_finite() && (0.0..=1.0).contains(&self.slo.availability),
            "SLO availability must be a finite value in [0.0, 1.0], got {}",
            self.slo.availability
        );
        anyhow::ensure!(
            self.slo.min_samples > 0,
            "SLO min_samples must be > 0, got {}",
            self.slo.min_samples
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

        // Environment-aware persistence safety check.
        // If the environment type is not "benchmark", reject configurations
        // that disable durability guarantees (these are only safe for benchmarks).
        if environment_type != "benchmark" {
            if matches!(self.persistence.fsync_policy, FsyncPolicy::None) {
                anyhow::bail!(
                    "Unsafe persistence configuration: fsync_policy=\"none\" is only permitted \
                     when environment.type=\"benchmark\". Current environment type: \"{}\". \
                     Set fsync_policy to \"data_only\" or \"full\" for production safety, \
                     or set [environment] type = \"benchmark\" to acknowledge the risk.",
                    self.environment.environment_type
                );
            }
            if self.persistence.snapshot_interval_mutations == 0 {
                anyhow::bail!(
                    "Unsafe persistence configuration: snapshot_interval_mutations=0 (snapshots disabled) \
                     is only permitted when environment.type=\"benchmark\". Current environment type: \"{}\". \
                     Set a positive snapshot_interval_mutations for production safety, \
                     or set [environment] type = \"benchmark\" to acknowledge the risk.",
                    self.environment.environment_type
                );
            }
        }

        if environment_type == "pilot" {
            anyhow::ensure!(self.auth.enabled, "Pilot mode requires auth.enabled=true");
            anyhow::ensure!(
                self.rate_limit.enabled,
                "Pilot mode requires rate_limit.enabled=true"
            );
            anyhow::ensure!(
                self.server.observability_auth != ObservabilityAuthMode::Disabled,
                "Pilot mode requires server.observability_auth to protect HTTP observability endpoints"
            );
            anyhow::ensure!(
                !self.persistence.allow_fresh_start_on_recovery_failure,
                "Pilot mode requires allow_fresh_start_on_recovery_failure=false"
            );
            anyhow::ensure!(
                self.server.tls.enabled || is_loopback_host(&self.server.host),
                "Pilot mode requires TLS for non-loopback gRPC bind addresses"
            );
        }

        if environment_type == "production" {
            let grpc_loopback = is_loopback_host(&self.server.host);
            let http_host = self
                .server
                .http_host
                .as_deref()
                .unwrap_or(&self.server.host);
            let http_loopback = is_loopback_host(http_host);

            if !grpc_loopback {
                anyhow::ensure!(
                    self.auth.enabled,
                    "Production mode with non-loopback gRPC bind requires auth.enabled=true"
                );
            }

            if !http_loopback {
                anyhow::ensure!(
                    self.server.observability_auth != ObservabilityAuthMode::Disabled,
                    "Production mode with non-loopback HTTP observability bind requires server.observability_auth to protect endpoints"
                );
            }
        }

        // Authentication validation
        if self.auth.enabled {
            anyhow::ensure!(
                self.auth.api_keys_file.is_some(),
                "api_keys_file must be provided when authentication is enabled"
            );
        }

        if self.server.observability_auth != ObservabilityAuthMode::Disabled {
            anyhow::ensure!(
                self.auth.enabled,
                "server.observability_auth requires auth.enabled=true"
            );
        }

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

    /// Get the HTTP observability host (defaults to the gRPC host)
    pub fn http_host(&self) -> &str {
        self.server
            .http_host
            .as_deref()
            .unwrap_or(self.server.host.as_str())
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

    /// Get snapshot interval as an operation count.
    ///
    /// `0` disables automatic snapshots.
    pub fn snapshot_interval_mutations(&self) -> usize {
        usize::try_from(self.persistence.snapshot_interval_mutations).unwrap_or(usize::MAX)
    }

    /// Get cache training interval as Duration
    pub fn cache_training_interval(&self) -> Duration {
        Duration::from_secs(self.cache.training_interval_secs)
    }

    // Usage export is intentionally not part of the Phase 0 server config.
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
    match serde_yaml::to_string(&config) {
        Ok(serialized) => serialized,
        Err(error) => format!("# failed to serialize default config to YAML: {error}\n"),
    }
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
    match toml::to_string_pretty(&config) {
        Ok(serialized) => serialized,
        Err(error) => format!("# failed to serialize default config to TOML: {error}\n"),
    }
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
        assert_eq!(config.snapshot_interval_mutations(), 10_000);
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
    fn test_pilot_mode_requires_auth() {
        let mut config = KyroDbConfig::default();
        config.environment.environment_type = "pilot".to_string();
        config.rate_limit.enabled = true;
        config.server.observability_auth = ObservabilityAuthMode::All;
        config.server.tls.enabled = true;
        config.server.tls.cert_path = Some(PathBuf::from("cert.pem"));
        config.server.tls.key_path = Some(PathBuf::from("key.pem"));
        assert!(config.validate().is_err(), "pilot mode must require auth");
    }

    #[test]
    fn test_invalid_environment_type_rejected() {
        let mut config = KyroDbConfig::default();
        config.environment.environment_type = "prod".to_string();
        assert!(
            config.validate().is_err(),
            "unknown environment types must be rejected"
        );
    }

    #[test]
    fn test_pilot_mode_requires_rate_limit_and_observability_auth() {
        let mut config = KyroDbConfig::default();
        config.environment.environment_type = "pilot".to_string();
        config.auth.enabled = true;
        config.auth.api_keys_file = Some(PathBuf::from("api_keys.yaml"));
        config.server.tls.enabled = true;
        config.server.tls.cert_path = Some(PathBuf::from("cert.pem"));
        config.server.tls.key_path = Some(PathBuf::from("key.pem"));
        assert!(
            config.validate().is_err(),
            "pilot mode must require rate limiting and observability auth"
        );
    }

    #[test]
    fn test_pilot_mode_requires_tls_for_non_loopback() {
        let mut config = KyroDbConfig::default();
        config.environment.environment_type = "pilot".to_string();
        config.auth.enabled = true;
        config.auth.api_keys_file = Some(PathBuf::from("api_keys.yaml"));
        config.rate_limit.enabled = true;
        config.server.observability_auth = ObservabilityAuthMode::All;
        config.server.host = "0.0.0.0".to_string();
        assert!(
            config.validate().is_err(),
            "pilot mode requires TLS on non-loopback host"
        );
    }

    #[test]
    fn test_pilot_mode_valid_profile_passes() {
        let mut config = KyroDbConfig::default();
        config.environment.environment_type = "pilot".to_string();
        config.auth.enabled = true;
        config.auth.api_keys_file = Some(PathBuf::from("api_keys.yaml"));
        config.rate_limit.enabled = true;
        config.server.observability_auth = ObservabilityAuthMode::All;
        config.server.tls.enabled = true;
        config.server.tls.cert_path = Some(PathBuf::from("cert.pem"));
        config.server.tls.key_path = Some(PathBuf::from("key.pem"));
        config.persistence.allow_fresh_start_on_recovery_failure = false;
        assert!(
            config.validate().is_ok(),
            "pilot mode config should pass with required guardrails"
        );
    }

    #[test]
    fn test_production_non_loopback_requires_auth() {
        let mut config = KyroDbConfig::default();
        config.server.host = "0.0.0.0".to_string();
        assert!(
            config.validate().is_err(),
            "production mode must require auth on non-loopback gRPC bind"
        );
    }

    #[test]
    fn test_production_non_loopback_http_requires_observability_auth() {
        let mut config = KyroDbConfig::default();
        config.server.host = "0.0.0.0".to_string();
        config.auth.enabled = true;
        config.auth.api_keys_file = Some(PathBuf::from("api_keys.yaml"));
        assert!(
            config.validate().is_err(),
            "production mode must require observability auth when HTTP is non-loopback"
        );
    }

    #[test]
    fn test_production_non_loopback_secure_profile_passes() {
        let mut config = KyroDbConfig::default();
        config.server.host = "0.0.0.0".to_string();
        config.auth.enabled = true;
        config.auth.api_keys_file = Some(PathBuf::from("api_keys.yaml"));
        config.server.observability_auth = ObservabilityAuthMode::MetricsAndSlo;
        assert!(
            config.validate().is_ok(),
            "production secure profile should pass"
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
