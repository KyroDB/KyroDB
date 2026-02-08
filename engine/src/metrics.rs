//! Production-Grade Metrics and Observability System
//!
//! Provides Prometheus-compatible metrics, health checks, and SLO monitoring
//! for KyroDB production deployments.
//!
//! # Features
//! - Prometheus-compatible /metrics endpoint with comprehensive vector DB metrics
//! - Health checks: /health (liveness) and /ready (readiness)
//! - SLO breach detection with configurable thresholds
//! - Low-overhead metrics collection (atomic counters, minimal lock contention)
//!
//! # Metrics Categories
//! - **Query Performance**: P50/P95/P99 latency, throughput (QPS)
//! - **Cache Performance**: Hit rates, prediction accuracy, eviction rates
//! - **Resource Usage**: Memory, CPU, disk I/O, connection counts
//! - **Error Rates**: Failure rates by operation type
//! - **HNSW Performance**: k-NN search latency, recall metrics
//!
//! # Usage
//! ```rust
//! use kyrodb_engine::metrics::{MetricsCollector, HealthStatus};
//!
//! let metrics = MetricsCollector::new();
//! metrics.record_query_latency(123456); // nanoseconds
//! metrics.record_cache_hit(true);
//!
//! // Export for Prometheus
//! let prometheus_text = metrics.export_prometheus();
//!
//! // Health checks
//! let status = metrics.health_status();
//! ```

use parking_lot::RwLock;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

// ============================================================================
// METRIC CONSTANTS
// ============================================================================

/// SLO: P99 latency threshold in nanoseconds (1ms = 1,000,000ns)
const SLO_P99_LATENCY_NS: u64 = 1_000_000;

/// SLO: Minimum cache hit rate (70%)
const SLO_MIN_CACHE_HIT_RATE: f64 = 0.70;

/// SLO: Maximum error rate (0.1%)
const SLO_MAX_ERROR_RATE: f64 = 0.001;

/// SLO: Minimum availability (99.9%)
const SLO_MIN_AVAILABILITY: f64 = 0.999;

/// SLO: Minimum sample size before alerting
const SLO_MIN_SAMPLES: u64 = 100;

/// Number of latency buckets for histogram (P50, P95, P99)
const LATENCY_BUCKETS: usize = 1000;

/// WAL Circuit Breaker States
pub const WAL_CIRCUIT_BREAKER_CLOSED: u64 = 0;
pub const WAL_CIRCUIT_BREAKER_OPEN: u64 = 1;
pub const WAL_CIRCUIT_BREAKER_HALF_OPEN: u64 = 2;

// ============================================================================
// CORE METRICS STRUCTURE
// ============================================================================

/// Global metrics collector - designed for zero-overhead production use
#[derive(Clone)]
pub struct MetricsCollector {
    inner: Arc<MetricsInner>,
}

struct MetricsInner {
    // Query Performance Metrics
    total_queries: AtomicU64,
    failed_queries: AtomicU64,
    query_latencies: RwLock<LatencyHistogram>,

    // Cache Performance Metrics
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    cache_evictions: AtomicU64,
    cache_size: AtomicUsize,

    // Learned Cache Specific Metrics
    learned_predictions: AtomicU64,
    learned_correct: AtomicU64,
    learned_false_positives: AtomicU64,
    learned_false_negatives: AtomicU64,

    // HNSW Performance Metrics
    hnsw_searches: AtomicU64,
    hnsw_latencies: RwLock<LatencyHistogram>,
    hot_tier_hits: AtomicU64,
    cold_tier_hits: AtomicU64,

    // Resource Metrics
    memory_used_bytes: AtomicU64,
    disk_used_bytes: AtomicU64,
    active_connections: AtomicUsize,

    // Write Path Metrics
    inserts_total: AtomicU64,
    inserts_failed: AtomicU64,
    hot_tier_flushes: AtomicU64,

    // WAL Metrics
    wal_writes_total: AtomicU64,
    wal_writes_failed: AtomicU64,
    wal_retries_total: AtomicU64,
    wal_circuit_breaker_state: AtomicU64, // 0=closed, 1=open, 2=half-open
    wal_disk_full_errors: AtomicU64,

    // HNSW Corruption Metrics
    hnsw_corruption_detected_total: AtomicU64,
    hnsw_fallback_recovery_success: AtomicU64,
    hnsw_fallback_recovery_failed: AtomicU64,

    // Training Task Metrics
    training_crashes_total: AtomicU64,
    training_restarts_total: AtomicU64,
    training_cycles_completed: AtomicU64,

    // Error Tracking
    error_counts: RwLock<ErrorCounters>,

    // Server State
    start_time: Instant,
    ready: AtomicU64, // 0 = not ready, 1 = ready

    slo_thresholds: SloThresholds,
}

/// Latency histogram for computing percentiles
/// Uses circular buffer for O(1) sample insertion
struct LatencyHistogram {
    samples: Vec<u64>,
    head: usize,
    filled: bool,
    max_samples: usize,
    cached_sorted: Option<Vec<u64>>,
}

/// Error counters by category
#[derive(Default)]
struct ErrorCounters {
    validation_errors: u64,
    timeout_errors: u64,
    internal_errors: u64,
    resource_exhausted: u64,
}

// ============================================================================
// HEALTH CHECK STRUCTURES
// ============================================================================

/// Health status for liveness and readiness probes
#[derive(Debug, Clone, PartialEq)]
pub enum HealthStatus {
    /// Service is healthy and ready to serve traffic
    Healthy,
    /// Service is starting up (not ready yet)
    Starting,
    /// Service is degraded but still functional
    Degraded { reason: String },
    /// Service is unhealthy and should not receive traffic
    Unhealthy { reason: String },
}

/// SLO breach detection result
#[derive(Debug, Clone)]
pub struct SloStatus {
    pub p99_latency_breached: bool,
    pub cache_hit_rate_breached: bool,
    pub error_rate_breached: bool,
    pub availability_breached: bool,
    pub current_p99_ns: u64,
    pub current_cache_hit_rate: f64,
    pub current_error_rate: f64,
    pub current_availability: f64,
    pub insufficient_data: bool, // True when no queries have been processed yet
}

/// SLO thresholds used by the metrics/health system.
#[derive(Debug, Clone, Copy)]
pub struct SloThresholds {
    pub p99_latency_ns: u64,
    pub min_cache_hit_rate: f64,
    pub max_error_rate: f64,
    pub min_availability: f64,
    pub min_samples: u64,
}

impl Default for SloThresholds {
    fn default() -> Self {
        Self {
            p99_latency_ns: SLO_P99_LATENCY_NS,
            min_cache_hit_rate: SLO_MIN_CACHE_HIT_RATE,
            max_error_rate: SLO_MAX_ERROR_RATE,
            min_availability: SLO_MIN_AVAILABILITY,
            min_samples: SLO_MIN_SAMPLES,
        }
    }
}

impl SloThresholds {
    pub fn from_config(cfg: &crate::config::SloConfig) -> Self {
        let p99_latency_ns = (cfg.p99_latency_ms * 1_000_000.0).ceil().max(1.0) as u64;

        Self {
            p99_latency_ns,
            min_cache_hit_rate: cfg.cache_hit_rate,
            max_error_rate: cfg.error_rate,
            min_availability: cfg.availability,
            min_samples: cfg.min_samples as u64,
        }
    }
}

// ============================================================================
// IMPLEMENTATION
// ============================================================================

impl MetricsCollector {
    /// Create new metrics collector
    pub fn new() -> Self {
        Self::new_with_slo_thresholds(SloThresholds::default())
    }

    pub fn new_with_slo_thresholds(slo_thresholds: SloThresholds) -> Self {
        Self {
            inner: Arc::new(MetricsInner {
                total_queries: AtomicU64::new(0),
                failed_queries: AtomicU64::new(0),
                query_latencies: RwLock::new(LatencyHistogram::new(LATENCY_BUCKETS)),

                cache_hits: AtomicU64::new(0),
                cache_misses: AtomicU64::new(0),
                cache_evictions: AtomicU64::new(0),
                cache_size: AtomicUsize::new(0),

                learned_predictions: AtomicU64::new(0),
                learned_correct: AtomicU64::new(0),
                learned_false_positives: AtomicU64::new(0),
                learned_false_negatives: AtomicU64::new(0),

                hnsw_searches: AtomicU64::new(0),
                hnsw_latencies: RwLock::new(LatencyHistogram::new(LATENCY_BUCKETS)),
                hot_tier_hits: AtomicU64::new(0),
                cold_tier_hits: AtomicU64::new(0),

                memory_used_bytes: AtomicU64::new(0),
                disk_used_bytes: AtomicU64::new(0),
                active_connections: AtomicUsize::new(0),

                inserts_total: AtomicU64::new(0),
                inserts_failed: AtomicU64::new(0),
                hot_tier_flushes: AtomicU64::new(0),

                wal_writes_total: AtomicU64::new(0),
                wal_writes_failed: AtomicU64::new(0),
                wal_retries_total: AtomicU64::new(0),
                wal_circuit_breaker_state: AtomicU64::new(0), // 0=closed
                wal_disk_full_errors: AtomicU64::new(0),

                hnsw_corruption_detected_total: AtomicU64::new(0),
                hnsw_fallback_recovery_success: AtomicU64::new(0),
                hnsw_fallback_recovery_failed: AtomicU64::new(0),

                training_crashes_total: AtomicU64::new(0),
                training_restarts_total: AtomicU64::new(0),
                training_cycles_completed: AtomicU64::new(0),

                error_counts: RwLock::new(ErrorCounters::default()),

                start_time: Instant::now(),
                ready: AtomicU64::new(0),
                slo_thresholds,
            }),
        }
    }

    pub fn slo_thresholds(&self) -> SloThresholds {
        self.inner.slo_thresholds
    }

    // ========================================================================
    // QUERY METRICS
    // ========================================================================

    /// Record query latency (in nanoseconds)
    #[inline]
    pub fn record_query_latency(&self, latency_ns: u64) {
        self.inner.total_queries.fetch_add(1, Ordering::Relaxed);
        self.inner.query_latencies.write().add_sample(latency_ns);
    }

    /// Record query failure
    #[inline]
    pub fn record_query_failure(&self) {
        self.inner.failed_queries.fetch_add(1, Ordering::Relaxed);
    }

    // ========================================================================
    // CACHE METRICS
    // ========================================================================

    /// Record cache hit or miss
    #[inline]
    pub fn record_cache_hit(&self, hit: bool) {
        if hit {
            self.inner.cache_hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.inner.cache_misses.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record cache eviction
    #[inline]
    pub fn record_cache_eviction(&self) {
        self.inner.cache_evictions.fetch_add(1, Ordering::Relaxed);
    }

    /// Update cache size
    #[inline]
    pub fn set_cache_size(&self, size: usize) {
        self.inner.cache_size.store(size, Ordering::Relaxed);
    }

    // ========================================================================
    // LEARNED CACHE METRICS
    // ========================================================================

    /// Record learned cache prediction accuracy
    #[inline]
    pub fn record_learned_prediction(&self, predicted_hot: bool, actually_hot: bool) {
        self.inner
            .learned_predictions
            .fetch_add(1, Ordering::Relaxed);

        if predicted_hot == actually_hot {
            self.inner.learned_correct.fetch_add(1, Ordering::Relaxed);
        } else if predicted_hot && !actually_hot {
            self.inner
                .learned_false_positives
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.inner
                .learned_false_negatives
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    // ========================================================================
    // HNSW METRICS
    // ========================================================================

    /// Record HNSW k-NN search latency
    #[inline]
    pub fn record_hnsw_search(&self, latency_ns: u64) {
        self.inner.hnsw_searches.fetch_add(1, Ordering::Relaxed);
        self.inner.hnsw_latencies.write().add_sample(latency_ns);
    }

    /// Record tier hit (hot or cold)
    #[inline]
    pub fn record_tier_hit(&self, hot_tier: bool) {
        if hot_tier {
            self.inner.hot_tier_hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.inner.cold_tier_hits.fetch_add(1, Ordering::Relaxed);
        }
    }

    // ========================================================================
    // RESOURCE METRICS
    // ========================================================================

    /// Update memory usage
    #[inline]
    pub fn set_memory_used(&self, bytes: u64) {
        self.inner.memory_used_bytes.store(bytes, Ordering::Relaxed);
    }

    /// Update disk usage
    #[inline]
    pub fn set_disk_used(&self, bytes: u64) {
        self.inner.disk_used_bytes.store(bytes, Ordering::Relaxed);
    }

    /// Track active connection count
    #[inline]
    pub fn increment_connections(&self) {
        self.inner
            .active_connections
            .fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn decrement_connections(&self) {
        self.inner
            .active_connections
            .fetch_sub(1, Ordering::Relaxed);
    }

    // ========================================================================
    // WRITE PATH METRICS
    // ========================================================================

    /// Record insert operation
    #[inline]
    pub fn record_insert(&self, success: bool) {
        self.inner.inserts_total.fetch_add(1, Ordering::Relaxed);
        if !success {
            self.inner.inserts_failed.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record hot tier flush
    #[inline]
    pub fn record_flush(&self) {
        self.inner.hot_tier_flushes.fetch_add(1, Ordering::Relaxed);
    }

    // ========================================================================
    // WAL METRICS
    // ========================================================================

    /// Record WAL write operation
    #[inline]
    pub fn record_wal_write(&self, success: bool) {
        self.inner.wal_writes_total.fetch_add(1, Ordering::Relaxed);
        if !success {
            self.inner.wal_writes_failed.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record WAL retry
    #[inline]
    pub fn record_wal_retry(&self) {
        self.inner.wal_retries_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record disk full error
    #[inline]
    pub fn record_wal_disk_full(&self) {
        self.inner
            .wal_disk_full_errors
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Update WAL circuit breaker state (0=closed, 1=open, 2=half-open)
    #[inline]
    pub fn update_wal_circuit_breaker_state(&self, state: u64) {
        debug_assert!(state <= 2, "Invalid circuit breaker state: {}", state);
        self.inner
            .wal_circuit_breaker_state
            .store(state, Ordering::Relaxed);
    }

    // ========================================================================
    // HNSW CORRUPTION METRICS
    // ========================================================================

    /// Record HNSW corruption detection
    #[inline]
    pub fn record_hnsw_corruption(&self) {
        self.inner
            .hnsw_corruption_detected_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record successful fallback recovery
    #[inline]
    pub fn record_hnsw_fallback_success(&self) {
        self.inner
            .hnsw_fallback_recovery_success
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record failed fallback recovery
    #[inline]
    pub fn record_hnsw_fallback_failed(&self) {
        self.inner
            .hnsw_fallback_recovery_failed
            .fetch_add(1, Ordering::Relaxed);
    }

    // ========================================================================
    // TRAINING TASK METRICS
    // ========================================================================

    /// Record training task crash
    #[inline]
    pub fn record_training_crash(&self) {
        self.inner
            .training_crashes_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record training task restart
    #[inline]
    pub fn record_training_restart(&self) {
        self.inner
            .training_restarts_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record completed training cycle
    #[inline]
    pub fn record_training_cycle(&self) {
        self.inner
            .training_cycles_completed
            .fetch_add(1, Ordering::Relaxed);
    }

    // ========================================================================
    // ERROR TRACKING
    // ========================================================================

    /// Record error by category
    pub fn record_error(&self, category: ErrorCategory) {
        let mut errors = self.inner.error_counts.write();
        match category {
            ErrorCategory::Validation => errors.validation_errors += 1,
            ErrorCategory::Timeout => errors.timeout_errors += 1,
            ErrorCategory::Internal => errors.internal_errors += 1,
            ErrorCategory::ResourceExhausted => errors.resource_exhausted += 1,
        }
    }

    // ========================================================================
    // SERVER STATE
    // ========================================================================

    /// Mark server as ready to receive traffic
    pub fn mark_ready(&self) {
        self.inner.ready.store(1, Ordering::Release);
    }

    /// Mark server as not ready
    pub fn mark_not_ready(&self) {
        self.inner.ready.store(0, Ordering::Release);
    }

    /// Get server uptime
    pub fn uptime(&self) -> Duration {
        self.inner.start_time.elapsed()
    }

    // ========================================================================
    // HEALTH CHECKS
    // ========================================================================

    /// Compute health status for liveness/readiness probes
    pub fn health_status(&self) -> HealthStatus {
        // Check if server is ready
        if self.inner.ready.load(Ordering::Acquire) == 0 {
            return HealthStatus::Starting;
        }

        // Check SLO breaches
        let slo = self.slo_status();

        // Don't assess health if insufficient data
        if slo.insufficient_data {
            return HealthStatus::Healthy; // Assume healthy during warmup
        }

        // Determine health based on SLO violations
        let mut violations = Vec::new();

        if slo.p99_latency_breached {
            violations.push(format!("P99 latency {}ns exceeds SLO", slo.current_p99_ns));
        }

        if slo.cache_hit_rate_breached {
            violations.push(format!(
                "Cache hit rate {:.2}% below SLO",
                slo.current_cache_hit_rate * 100.0
            ));
        }

        if slo.error_rate_breached {
            violations.push(format!(
                "Error rate {:.3}% exceeds SLO",
                slo.current_error_rate * 100.0
            ));
        }

        if slo.availability_breached {
            violations.push(format!(
                "Availability {:.3}% below SLO",
                slo.current_availability * 100.0
            ));
        }

        if violations.is_empty() {
            HealthStatus::Healthy
        } else if violations.len() <= 1 {
            HealthStatus::Degraded {
                reason: violations.join("; "),
            }
        } else {
            HealthStatus::Unhealthy {
                reason: violations.join("; "),
            }
        }
    }

    /// Compute SLO breach status
    pub fn slo_status(&self) -> SloStatus {
        let total = self.inner.total_queries.load(Ordering::Relaxed);
        let failed = self.inner.failed_queries.load(Ordering::Relaxed);

        let thresholds = self.inner.slo_thresholds;

        // Check if we have sufficient data to make SLO assessments
        let insufficient_data = total < thresholds.min_samples;

        let p99 = if total > 0 {
            self.inner.query_latencies.write().percentile(99.0)
        } else {
            0
        };

        let cache_hits = self.inner.cache_hits.load(Ordering::Relaxed);
        let cache_misses = self.inner.cache_misses.load(Ordering::Relaxed);
        let cache_total = cache_hits + cache_misses;

        // No data = 0% hit rate (will trigger breach alerting, which is correct)
        let cache_hit_rate = if cache_total > 0 {
            cache_hits as f64 / cache_total as f64
        } else {
            0.0
        };

        let error_rate = if total > 0 {
            failed as f64 / total as f64
        } else {
            0.0
        };

        // No failures = available (1.0 is correct for no data)
        let availability = if total > 0 {
            (total - failed) as f64 / total as f64
        } else {
            1.0
        };

        SloStatus {
            // Don't flag breaches if we don't have enough data yet
            p99_latency_breached: !insufficient_data && p99 > thresholds.p99_latency_ns,
            cache_hit_rate_breached: !insufficient_data
                && cache_hit_rate < thresholds.min_cache_hit_rate,
            error_rate_breached: !insufficient_data && error_rate > thresholds.max_error_rate,
            availability_breached: !insufficient_data && availability < thresholds.min_availability,
            current_p99_ns: p99,
            current_cache_hit_rate: cache_hit_rate,
            current_error_rate: error_rate,
            current_availability: availability,
            insufficient_data,
        }
    }

    // ========================================================================
    // METRIC GETTERS (for testing)
    // ========================================================================

    /// Get HNSW corruption detection count
    pub fn get_hnsw_corruption_count(&self) -> u64 {
        self.inner
            .hnsw_corruption_detected_total
            .load(Ordering::Relaxed)
    }

    /// Get successful fallback recovery count
    pub fn get_hnsw_fallback_success_count(&self) -> u64 {
        self.inner
            .hnsw_fallback_recovery_success
            .load(Ordering::Relaxed)
    }

    /// Get failed fallback recovery count
    pub fn get_hnsw_fallback_failed_count(&self) -> u64 {
        self.inner
            .hnsw_fallback_recovery_failed
            .load(Ordering::Relaxed)
    }

    /// Get training crashes count
    pub fn get_training_crashes_count(&self) -> u64 {
        self.inner.training_crashes_total.load(Ordering::Relaxed)
    }

    /// Get training restarts count
    pub fn get_training_restarts_count(&self) -> u64 {
        self.inner.training_restarts_total.load(Ordering::Relaxed)
    }

    /// Get training cycles completed count
    pub fn get_training_cycles_completed(&self) -> u64 {
        self.inner.training_cycles_completed.load(Ordering::Relaxed)
    }

    /// Get WAL writes total count
    pub fn get_wal_writes_total(&self) -> u64 {
        self.inner.wal_writes_total.load(Ordering::Relaxed)
    }

    /// Get WAL writes failed count
    pub fn get_wal_writes_failed(&self) -> u64 {
        self.inner.wal_writes_failed.load(Ordering::Relaxed)
    }

    /// Raise WAL failure counter to at least `observed`.
    ///
    /// Used when subsystem-local WAL error accounting is merged into global metrics.
    pub fn set_wal_writes_failed_floor(&self, observed: u64) {
        let mut current = self.inner.wal_writes_failed.load(Ordering::Relaxed);
        while current < observed {
            match self.inner.wal_writes_failed.compare_exchange_weak(
                current,
                observed,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }

    /// Get WAL retries total count
    pub fn get_wal_retries_total(&self) -> u64 {
        self.inner.wal_retries_total.load(Ordering::Relaxed)
    }

    /// Get WAL circuit breaker state (0=closed, 1=open, 2=half-open)
    pub fn get_wal_circuit_breaker_state(&self) -> u64 {
        self.inner.wal_circuit_breaker_state.load(Ordering::Relaxed)
    }

    /// Get WAL disk full errors count
    pub fn get_wal_disk_full_errors(&self) -> u64 {
        self.inner.wal_disk_full_errors.load(Ordering::Relaxed)
    }

    /// Get query latency percentiles in nanoseconds (p50, p95, p99).
    pub fn latency_percentiles_ns(&self) -> (u64, u64, u64) {
        let mut latencies = self.inner.query_latencies.write();
        (
            latencies.percentile(50.0),
            latencies.percentile(95.0),
            latencies.percentile(99.0),
        )
    }

    /// Get total query count tracked by metrics collector.
    pub fn get_total_queries(&self) -> u64 {
        self.inner.total_queries.load(Ordering::Relaxed)
    }

    /// Get total insert operations.
    pub fn get_inserts_total(&self) -> u64 {
        self.inner.inserts_total.load(Ordering::Relaxed)
    }

    /// Get failed insert operations.
    pub fn get_inserts_failed(&self) -> u64 {
        self.inner.inserts_failed.load(Ordering::Relaxed)
    }

    /// Get cache size gauge value.
    pub fn get_cache_size(&self) -> usize {
        self.inner.cache_size.load(Ordering::Relaxed)
    }

    /// Get memory usage gauge value.
    pub fn get_memory_used_bytes(&self) -> u64 {
        self.inner.memory_used_bytes.load(Ordering::Relaxed)
    }

    /// Get disk usage gauge value.
    pub fn get_disk_used_bytes(&self) -> u64 {
        self.inner.disk_used_bytes.load(Ordering::Relaxed)
    }

    /// Get active connection count.
    pub fn get_active_connections(&self) -> usize {
        self.inner.active_connections.load(Ordering::Relaxed)
    }

    // ========================================================================
    // PROMETHEUS EXPORT
    // ========================================================================

    /// Export metrics in Prometheus text format
    pub fn export_prometheus(&self) -> String {
        let mut output = String::with_capacity(8192);

        // Query Performance Metrics
        let total_queries = self.inner.total_queries.load(Ordering::Relaxed);
        let failed_queries = self.inner.failed_queries.load(Ordering::Relaxed);

        output.push_str("# HELP kyrodb_queries_total Total number of queries\n");
        output.push_str("# TYPE kyrodb_queries_total counter\n");
        output.push_str(&format!("kyrodb_queries_total {}\n", total_queries));

        output.push_str("# HELP kyrodb_queries_failed Total number of failed queries\n");
        output.push_str("# TYPE kyrodb_queries_failed counter\n");
        output.push_str(&format!("kyrodb_queries_failed {}\n", failed_queries));

        // Query Latency Percentiles (need write lock for cached sorting)
        let mut latencies = self.inner.query_latencies.write();
        let p50 = latencies.percentile(50.0);
        let p95 = latencies.percentile(95.0);
        let p99 = latencies.percentile(99.0);
        drop(latencies); // Release lock early

        output
            .push_str("# HELP kyrodb_query_latency_ns Query latency percentiles in nanoseconds\n");
        output.push_str("# TYPE kyrodb_query_latency_ns gauge\n");
        output.push_str(&format!(
            "kyrodb_query_latency_ns{{percentile=\"50\"}} {}\n",
            p50
        ));
        output.push_str(&format!(
            "kyrodb_query_latency_ns{{percentile=\"95\"}} {}\n",
            p95
        ));
        output.push_str(&format!(
            "kyrodb_query_latency_ns{{percentile=\"99\"}} {}\n",
            p99
        ));

        // Cache Metrics
        let cache_hits = self.inner.cache_hits.load(Ordering::Relaxed);
        let cache_misses = self.inner.cache_misses.load(Ordering::Relaxed);
        let cache_evictions = self.inner.cache_evictions.load(Ordering::Relaxed);
        let cache_size = self.inner.cache_size.load(Ordering::Relaxed);

        output.push_str("# HELP kyrodb_cache_hits_total Total cache hits\n");
        output.push_str("# TYPE kyrodb_cache_hits_total counter\n");
        output.push_str(&format!("kyrodb_cache_hits_total {}\n", cache_hits));

        output.push_str("# HELP kyrodb_cache_misses_total Total cache misses\n");
        output.push_str("# TYPE kyrodb_cache_misses_total counter\n");
        output.push_str(&format!("kyrodb_cache_misses_total {}\n", cache_misses));

        output.push_str("# HELP kyrodb_cache_hit_rate Cache hit rate (0.0-1.0)\n");
        output.push_str("# TYPE kyrodb_cache_hit_rate gauge\n");
        let cache_total = cache_hits + cache_misses;
        let hit_rate = if cache_total > 0 {
            cache_hits as f64 / cache_total as f64
        } else {
            0.0
        };
        output.push_str(&format!("kyrodb_cache_hit_rate {:.6}\n", hit_rate));

        output.push_str("# HELP kyrodb_cache_evictions_total Total cache evictions\n");
        output.push_str("# TYPE kyrodb_cache_evictions_total counter\n");
        output.push_str(&format!(
            "kyrodb_cache_evictions_total {}\n",
            cache_evictions
        ));

        output.push_str("# HELP kyrodb_cache_size Current cache size (entries)\n");
        output.push_str("# TYPE kyrodb_cache_size gauge\n");
        output.push_str(&format!("kyrodb_cache_size {}\n", cache_size));

        // Learned Cache Metrics
        let learned_predictions = self.inner.learned_predictions.load(Ordering::Relaxed);
        let learned_correct = self.inner.learned_correct.load(Ordering::Relaxed);
        let learned_fp = self.inner.learned_false_positives.load(Ordering::Relaxed);
        let learned_fn = self.inner.learned_false_negatives.load(Ordering::Relaxed);

        output.push_str(
            "# HELP kyrodb_learned_cache_predictions_total Total learned cache predictions\n",
        );
        output.push_str("# TYPE kyrodb_learned_cache_predictions_total counter\n");
        output.push_str(&format!(
            "kyrodb_learned_cache_predictions_total {}\n",
            learned_predictions
        ));

        output.push_str(
            "# HELP kyrodb_learned_cache_accuracy Learned cache prediction accuracy (0.0-1.0)\n",
        );
        output.push_str("# TYPE kyrodb_learned_cache_accuracy gauge\n");
        let accuracy = if learned_predictions > 0 {
            learned_correct as f64 / learned_predictions as f64
        } else {
            0.0
        };
        output.push_str(&format!("kyrodb_learned_cache_accuracy {:.6}\n", accuracy));

        output.push_str(
            "# HELP kyrodb_learned_cache_false_positives_total False positive predictions\n",
        );
        output.push_str("# TYPE kyrodb_learned_cache_false_positives_total counter\n");
        output.push_str(&format!(
            "kyrodb_learned_cache_false_positives_total {}\n",
            learned_fp
        ));

        output.push_str(
            "# HELP kyrodb_learned_cache_false_negatives_total False negative predictions\n",
        );
        output.push_str("# TYPE kyrodb_learned_cache_false_negatives_total counter\n");
        output.push_str(&format!(
            "kyrodb_learned_cache_false_negatives_total {}\n",
            learned_fn
        ));

        // HNSW Metrics
        let hnsw_searches = self.inner.hnsw_searches.load(Ordering::Relaxed);
        let mut hnsw_latencies = self.inner.hnsw_latencies.write();
        let hnsw_p99 = hnsw_latencies.percentile(99.0);
        drop(hnsw_latencies); // Release lock early

        output.push_str("# HELP kyrodb_hnsw_searches_total Total HNSW k-NN searches\n");
        output.push_str("# TYPE kyrodb_hnsw_searches_total counter\n");
        output.push_str(&format!("kyrodb_hnsw_searches_total {}\n", hnsw_searches));

        output.push_str("# HELP kyrodb_hnsw_latency_ns HNSW search latency P99 in nanoseconds\n");
        output.push_str("# TYPE kyrodb_hnsw_latency_ns gauge\n");
        output.push_str(&format!("kyrodb_hnsw_latency_ns {}\n", hnsw_p99));

        // Tier Metrics
        let hot_tier_hits = self.inner.hot_tier_hits.load(Ordering::Relaxed);
        let cold_tier_hits = self.inner.cold_tier_hits.load(Ordering::Relaxed);

        output.push_str("# HELP kyrodb_tier_hits_total Hits by tier\n");
        output.push_str("# TYPE kyrodb_tier_hits_total counter\n");
        output.push_str(&format!(
            "kyrodb_tier_hits_total{{tier=\"hot\"}} {}\n",
            hot_tier_hits
        ));
        output.push_str(&format!(
            "kyrodb_tier_hits_total{{tier=\"cold\"}} {}\n",
            cold_tier_hits
        ));

        // Resource Metrics
        let memory_used = self.inner.memory_used_bytes.load(Ordering::Relaxed);
        let disk_used = self.inner.disk_used_bytes.load(Ordering::Relaxed);
        let connections = self.inner.active_connections.load(Ordering::Relaxed);

        output.push_str("# HELP kyrodb_memory_used_bytes Memory usage in bytes\n");
        output.push_str("# TYPE kyrodb_memory_used_bytes gauge\n");
        output.push_str(&format!("kyrodb_memory_used_bytes {}\n", memory_used));

        output.push_str("# HELP kyrodb_disk_used_bytes Disk usage in bytes\n");
        output.push_str("# TYPE kyrodb_disk_used_bytes gauge\n");
        output.push_str(&format!("kyrodb_disk_used_bytes {}\n", disk_used));

        output.push_str("# HELP kyrodb_active_connections Current active connections\n");
        output.push_str("# TYPE kyrodb_active_connections gauge\n");
        output.push_str(&format!("kyrodb_active_connections {}\n", connections));

        // Write Path Metrics
        let inserts_total = self.inner.inserts_total.load(Ordering::Relaxed);
        let inserts_failed = self.inner.inserts_failed.load(Ordering::Relaxed);
        let flushes = self.inner.hot_tier_flushes.load(Ordering::Relaxed);

        output.push_str("# HELP kyrodb_inserts_total Total insert operations\n");
        output.push_str("# TYPE kyrodb_inserts_total counter\n");
        output.push_str(&format!("kyrodb_inserts_total {}\n", inserts_total));

        output.push_str("# HELP kyrodb_inserts_failed Total failed inserts\n");
        output.push_str("# TYPE kyrodb_inserts_failed counter\n");
        output.push_str(&format!("kyrodb_inserts_failed {}\n", inserts_failed));

        output.push_str("# HELP kyrodb_hot_tier_flushes_total Total hot tier flushes\n");
        output.push_str("# TYPE kyrodb_hot_tier_flushes_total counter\n");
        output.push_str(&format!("kyrodb_hot_tier_flushes_total {}\n", flushes));

        // WAL Metrics
        let wal_writes = self.inner.wal_writes_total.load(Ordering::Relaxed);
        let wal_failed = self.inner.wal_writes_failed.load(Ordering::Relaxed);
        let wal_retries = self.inner.wal_retries_total.load(Ordering::Relaxed);
        let wal_circuit_state = self.inner.wal_circuit_breaker_state.load(Ordering::Relaxed);
        let wal_disk_full = self.inner.wal_disk_full_errors.load(Ordering::Relaxed);

        output.push_str("# HELP kyrodb_wal_writes_total Total WAL write operations\n");
        output.push_str("# TYPE kyrodb_wal_writes_total counter\n");
        output.push_str(&format!("kyrodb_wal_writes_total {}\n", wal_writes));

        output.push_str("# HELP kyrodb_wal_writes_failed WAL write failures\n");
        output.push_str("# TYPE kyrodb_wal_writes_failed counter\n");
        output.push_str(&format!("kyrodb_wal_writes_failed {}\n", wal_failed));

        output.push_str("# HELP kyrodb_wal_retries_total WAL write retries\n");
        output.push_str("# TYPE kyrodb_wal_retries_total counter\n");
        output.push_str(&format!("kyrodb_wal_retries_total {}\n", wal_retries));

        output.push_str("# HELP kyrodb_wal_circuit_breaker_state WAL circuit breaker state (0=closed, 1=open, 2=half-open)\n");
        output.push_str("# TYPE kyrodb_wal_circuit_breaker_state gauge\n");
        output.push_str(&format!(
            "kyrodb_wal_circuit_breaker_state {}\n",
            wal_circuit_state
        ));

        output.push_str("# HELP kyrodb_wal_disk_full_errors Disk full errors during WAL writes\n");
        output.push_str("# TYPE kyrodb_wal_disk_full_errors counter\n");
        output.push_str(&format!("kyrodb_wal_disk_full_errors {}\n", wal_disk_full));

        // HNSW Corruption Metrics
        let corruption_detected = self
            .inner
            .hnsw_corruption_detected_total
            .load(Ordering::Relaxed);
        let fallback_success = self
            .inner
            .hnsw_fallback_recovery_success
            .load(Ordering::Relaxed);
        let fallback_failed = self
            .inner
            .hnsw_fallback_recovery_failed
            .load(Ordering::Relaxed);

        output.push_str(
            "# HELP kyrodb_hnsw_corruption_detected_total HNSW snapshot corruption detections\n",
        );
        output.push_str("# TYPE kyrodb_hnsw_corruption_detected_total counter\n");
        output.push_str(&format!(
            "kyrodb_hnsw_corruption_detected_total {}\n",
            corruption_detected
        ));

        output.push_str(
            "# HELP kyrodb_hnsw_fallback_recovery_success Successful fallback recoveries\n",
        );
        output.push_str("# TYPE kyrodb_hnsw_fallback_recovery_success counter\n");
        output.push_str(&format!(
            "kyrodb_hnsw_fallback_recovery_success {}\n",
            fallback_success
        ));

        output.push_str("# HELP kyrodb_hnsw_fallback_recovery_failed Failed fallback recoveries\n");
        output.push_str("# TYPE kyrodb_hnsw_fallback_recovery_failed counter\n");
        output.push_str(&format!(
            "kyrodb_hnsw_fallback_recovery_failed {}\n",
            fallback_failed
        ));

        // Training Task Metrics
        let training_crashes = self.inner.training_crashes_total.load(Ordering::Relaxed);
        let training_restarts = self.inner.training_restarts_total.load(Ordering::Relaxed);
        let training_cycles = self.inner.training_cycles_completed.load(Ordering::Relaxed);

        output.push_str("# HELP kyrodb_training_crashes_total Training task crashes\n");
        output.push_str("# TYPE kyrodb_training_crashes_total counter\n");
        output.push_str(&format!(
            "kyrodb_training_crashes_total {}\n",
            training_crashes
        ));

        output.push_str("# HELP kyrodb_training_restarts_total Training task restarts\n");
        output.push_str("# TYPE kyrodb_training_restarts_total counter\n");
        output.push_str(&format!(
            "kyrodb_training_restarts_total {}\n",
            training_restarts
        ));

        output
            .push_str("# HELP kyrodb_training_cycles_completed_total Completed training cycles\n");
        output.push_str("# TYPE kyrodb_training_cycles_completed_total counter\n");
        output.push_str(&format!(
            "kyrodb_training_cycles_completed_total {}\n",
            training_cycles
        ));

        // Error Metrics
        let errors = self.inner.error_counts.read();
        output.push_str("# HELP kyrodb_errors_total Errors by category\n");
        output.push_str("# TYPE kyrodb_errors_total counter\n");
        output.push_str(&format!(
            "kyrodb_errors_total{{category=\"validation\"}} {}\n",
            errors.validation_errors
        ));
        output.push_str(&format!(
            "kyrodb_errors_total{{category=\"timeout\"}} {}\n",
            errors.timeout_errors
        ));
        output.push_str(&format!(
            "kyrodb_errors_total{{category=\"internal\"}} {}\n",
            errors.internal_errors
        ));
        output.push_str(&format!(
            "kyrodb_errors_total{{category=\"resource_exhausted\"}} {}\n",
            errors.resource_exhausted
        ));

        // Server Info
        let uptime_secs = self.uptime().as_secs();
        output.push_str("# HELP kyrodb_uptime_seconds Server uptime in seconds\n");
        output.push_str("# TYPE kyrodb_uptime_seconds counter\n");
        output.push_str(&format!("kyrodb_uptime_seconds {}\n", uptime_secs));

        output.push_str("# HELP kyrodb_ready Server readiness status (0=not ready, 1=ready)\n");
        output.push_str("# TYPE kyrodb_ready gauge\n");
        let ready = self.inner.ready.load(Ordering::Relaxed);
        output.push_str(&format!("kyrodb_ready {}\n", ready));

        output
    }
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// LATENCY HISTOGRAM
// ============================================================================

impl LatencyHistogram {
    fn new(max_samples: usize) -> Self {
        Self {
            samples: vec![0; max_samples],
            head: 0,
            filled: false,
            max_samples,
            cached_sorted: None,
        }
    }

    /// Add sample with O(1) circular buffer insertion
    fn add_sample(&mut self, value: u64) {
        self.samples[self.head] = value;
        self.head = (self.head + 1) % self.max_samples;
        if self.head == 0 {
            self.filled = true;
        }
        // Invalidate cached sorted array
        self.cached_sorted = None;
    }

    /// Calculate percentile with cached sorting for better performance
    fn percentile(&mut self, p: f64) -> u64 {
        let len = if self.filled {
            self.max_samples
        } else {
            self.head
        };

        if len == 0 {
            return 0;
        }

        // Use cached sorted array if available
        if self.cached_sorted.is_none() {
            let mut sorted = if self.filled {
                self.samples.clone()
            } else {
                self.samples[..len].to_vec()
            };
            sorted.sort_unstable();
            self.cached_sorted = Some(sorted);
        }

        let Some(sorted) = self.cached_sorted.as_ref() else {
            return 0;
        };
        let idx = ((p / 100.0) * len as f64) as usize;
        let idx = idx.min(len - 1);
        sorted[idx]
    }
}

// ============================================================================
// ERROR CATEGORY
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCategory {
    Validation,
    Timeout,
    Internal,
    ResourceExhausted,
}

// ============================================================================
// TESTS
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_collection() {
        let metrics = MetricsCollector::new_with_slo_thresholds(SloThresholds {
            min_samples: 10,
            ..SloThresholds::default()
        });

        // Record enough queries to pass insufficient_data threshold (need >=10)
        for i in 0..8 {
            if i < 7 {
                metrics.record_query_latency(100_000); // 100us
            } else {
                metrics.record_query_latency(200_000); // 200us
            }
        }
        metrics.record_query_latency(1_500_000); // 1.5ms (SLO breach)
        metrics.record_query_latency(1_500_000); // Another high latency
        metrics.record_query_latency(1_500_000); // One more = 11 total queries

        // Record cache hits/misses (need at least 7 hits to reach 70% threshold with 10 total)
        metrics.record_cache_hit(true);
        metrics.record_cache_hit(true);
        metrics.record_cache_hit(true);
        metrics.record_cache_hit(true);
        metrics.record_cache_hit(true);
        metrics.record_cache_hit(true);
        metrics.record_cache_hit(true); // 7 hits
        metrics.record_cache_hit(false); // 1 miss
        metrics.record_cache_hit(false); // 2 miss
        metrics.record_cache_hit(false); // 3 miss = 70% hit rate

        // Check computed metrics
        let slo = metrics.slo_status();
        assert!(
            !slo.insufficient_data,
            "Should have sufficient data with 11 queries"
        );
        assert!(slo.p99_latency_breached); // P99 = 1.5ms > 1ms SLO
        assert!(!slo.cache_hit_rate_breached); // 70% = 70% SLO (not breached)
    }

    #[test]
    fn test_health_status() {
        let metrics = MetricsCollector::new();

        // Initially not ready
        assert_eq!(metrics.health_status(), HealthStatus::Starting);

        // Mark ready
        metrics.mark_ready();

        // Record enough successful queries to be healthy
        for _ in 0..100 {
            metrics.record_query_latency(100_000);
        }

        // Record enough cache hits to meet SLO
        for _ in 0..75 {
            metrics.record_cache_hit(true);
        }
        for _ in 0..25 {
            metrics.record_cache_hit(false);
        }

        let health = metrics.health_status();
        assert!(matches!(
            health,
            HealthStatus::Healthy | HealthStatus::Degraded { .. }
        ));
    }

    #[test]
    fn test_prometheus_export() {
        let metrics = MetricsCollector::new();
        metrics.mark_ready();
        metrics.record_query_latency(100_000);

        let output = metrics.export_prometheus();
        assert!(output.contains("kyrodb_queries_total"));
        assert!(output.contains("kyrodb_cache_hit_rate"));
        assert!(output.contains("kyrodb_uptime_seconds"));
    }

    #[test]
    fn test_latency_histogram() {
        let mut hist = LatencyHistogram::new(100);

        for i in 1..=100 {
            hist.add_sample(i * 1000);
        }

        // Percentile calculation: P50 = value at index 50 (50th percentile of 100 samples)
        // With 100 samples, P50 is at index (0.5 * 100) = 50, which contains value 51_000
        let p50 = hist.percentile(50.0);
        assert!((50_000..=51_000).contains(&p50), "P50 was {}", p50);

        let p99 = hist.percentile(99.0);
        assert!((99_000..=100_000).contains(&p99), "P99 was {}", p99);
    }

    #[test]
    fn test_insufficient_data_handling() {
        let metrics = MetricsCollector::new();
        metrics.mark_ready();

        // Record only 5 queries (below min_samples threshold)
        for _ in 0..5 {
            metrics.record_query_latency(2_000_000); // 2ms - would breach SLO
        }

        let slo = metrics.slo_status();

        // Should flag insufficient data
        assert!(slo.insufficient_data);

        // Should NOT flag breaches when insufficient data
        assert!(
            !slo.p99_latency_breached,
            "Should not breach with insufficient data"
        );

        // Health should be healthy during warmup
        let health = metrics.health_status();
        assert_eq!(
            health,
            HealthStatus::Healthy,
            "Should be healthy during warmup"
        );
    }

    #[test]
    fn test_circular_buffer_wraparound() {
        let mut hist = LatencyHistogram::new(10);

        // Add 15 samples to ensure wraparound
        for i in 1..=15 {
            hist.add_sample(i * 1000);
        }

        // Should only contain last 10 samples (6-15)
        assert!(hist.filled, "Buffer should be marked as filled");

        // Median should be around 10-11 (samples 6-15)
        let p50 = hist.percentile(50.0);
        assert!(
            (10_000..=12_000).contains(&p50),
            "P50 was {}, expected ~10500",
            p50
        );
    }
}
