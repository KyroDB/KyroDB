use once_cell::sync::Lazy;
#[cfg(not(feature = "bench-no-metrics"))]
use prometheus::{Counter, Gauge, Histogram, HistogramOpts};

#[cfg(feature = "bench-no-metrics")]
mod shim {
    use super::*;
    pub struct NoopCounter;
    impl NoopCounter {
        pub fn inc(&self) {}
    }
    pub struct NoopGauge;
    impl NoopGauge {
        pub fn set(&self, _v: f64) {}
        pub fn get(&self) -> f64 {
            0.0
        }
    }
    pub struct NoopHistogram;
    impl NoopHistogram {
        pub fn observe(&self, _v: f64) {}
        pub fn start_timer(&self) -> NoopTimer {
            NoopTimer
        }
    }
    pub struct NoopTimer;
    impl NoopTimer {
        pub fn observe_duration(&self) {}
    }
    pub static APPENDS_TOTAL: Lazy<NoopCounter> = Lazy::new(|| NoopCounter);
    pub static APPEND_LATENCY_SECONDS: Lazy<NoopHistogram> = Lazy::new(|| NoopHistogram);
    pub static SNAPSHOTS_TOTAL: Lazy<NoopCounter> = Lazy::new(|| NoopCounter);
    pub static SNAPSHOT_LATENCY_SECONDS: Lazy<NoopHistogram> = Lazy::new(|| NoopHistogram);
    pub static SSE_LAGGED_TOTAL: Lazy<NoopCounter> = Lazy::new(|| NoopCounter);
    pub static WAL_CRC_ERRORS_TOTAL: Lazy<NoopCounter> = Lazy::new(|| NoopCounter);
    pub static WAL_BLOCK_CACHE_HITS_TOTAL: Lazy<NoopCounter> = Lazy::new(|| NoopCounter);
    pub static WAL_BLOCK_CACHE_MISSES_TOTAL: Lazy<NoopCounter> = Lazy::new(|| NoopCounter);
    pub static COMPACTIONS_TOTAL: Lazy<NoopCounter> = Lazy::new(|| NoopCounter);
    pub static COMPACTION_DURATION_SECONDS: Lazy<NoopHistogram> = Lazy::new(|| NoopHistogram);
    pub static RMI_HITS_TOTAL: Lazy<NoopCounter> = Lazy::new(|| NoopCounter);
    pub static RMI_MISSES_TOTAL: Lazy<NoopCounter> = Lazy::new(|| NoopCounter);
    pub static RMI_LOOKUP_LATENCY_SECONDS: Lazy<NoopHistogram> = Lazy::new(|| NoopHistogram);
    pub static RMI_LOOKUP_LATENCY_DURING_REBUILD_SECONDS: Lazy<NoopHistogram> =
        Lazy::new(|| NoopHistogram);
    pub static RMI_EPSILON_HISTOGRAM: Lazy<NoopHistogram> = Lazy::new(|| NoopHistogram);
    pub static RMI_INDEX_LEAVES: Lazy<NoopGauge> = Lazy::new(|| NoopGauge);
    pub static RMI_INDEX_SIZE_BYTES: Lazy<NoopGauge> = Lazy::new(|| NoopGauge);
    pub static RMI_EPSILON_MAX: Lazy<NoopGauge> = Lazy::new(|| NoopGauge);
    pub static RMI_REBUILDS_TOTAL: Lazy<NoopCounter> = Lazy::new(|| NoopCounter);
    pub static RMI_SWAPS_TOTAL: Lazy<NoopCounter> = Lazy::new(|| NoopCounter);
    pub static RMI_REBUILD_DURATION_SECONDS: Lazy<NoopHistogram> = Lazy::new(|| NoopHistogram);
    pub static RMI_PROBE_LEN: Lazy<NoopHistogram> = Lazy::new(|| NoopHistogram);
    pub static RMI_MISPREDICTS_TOTAL: Lazy<NoopCounter> = Lazy::new(|| NoopCounter);
    pub static RMI_READS_TOTAL: Lazy<NoopCounter> = Lazy::new(|| NoopCounter);
    pub static BTREE_READS_TOTAL: Lazy<NoopCounter> = Lazy::new(|| NoopCounter);
    pub static LOOKUP_FALLBACK_SCAN_TOTAL: Lazy<NoopCounter> = Lazy::new(|| NoopCounter);
    pub static INDEX_FALLBACK_SCANS_TOTAL: Lazy<NoopCounter> = Lazy::new(|| NoopCounter);
    pub static RMI_REBUILD_IN_PROGRESS: Lazy<NoopGauge> = Lazy::new(|| NoopGauge);
    #[allow(dead_code)]
    pub static RMI_REBUILD_STALLS_TOTAL: Lazy<NoopCounter> = Lazy::new(|| NoopCounter);
    pub static GROUP_COMMIT_BATCH_SIZE: Lazy<NoopHistogram> = Lazy::new(|| NoopHistogram);
    pub static GROUP_COMMIT_LATENCY_SECONDS: Lazy<NoopHistogram> = Lazy::new(|| NoopHistogram);
    pub static GROUP_COMMIT_BATCHES_TOTAL: Lazy<NoopCounter> = Lazy::new(|| NoopCounter);
    pub fn inc_sse_lagged() {}
    pub fn render() -> String {
        String::new()
    }
    pub fn rmi_rebuild_in_progress() -> bool {
        RMI_REBUILD_IN_PROGRESS.get() > 0.5
    }
}

#[cfg(feature = "bench-no-metrics")]
pub use shim::{
    inc_sse_lagged, render, rmi_rebuild_in_progress, APPENDS_TOTAL, APPEND_LATENCY_SECONDS,
    BTREE_READS_TOTAL, COMPACTIONS_TOTAL, COMPACTION_DURATION_SECONDS, GROUP_COMMIT_BATCHES_TOTAL,
    GROUP_COMMIT_BATCH_SIZE, GROUP_COMMIT_LATENCY_SECONDS, INDEX_FALLBACK_SCANS_TOTAL,
    LOOKUP_FALLBACK_SCAN_TOTAL, RMI_EPSILON_HISTOGRAM, RMI_EPSILON_MAX, RMI_HITS_TOTAL,
    RMI_INDEX_LEAVES, RMI_INDEX_SIZE_BYTES, RMI_LOOKUP_LATENCY_DURING_REBUILD_SECONDS,
    RMI_LOOKUP_LATENCY_SECONDS, RMI_MISPREDICTS_TOTAL, RMI_MISSES_TOTAL, RMI_PROBE_LEN,
    RMI_READS_TOTAL, RMI_REBUILDS_TOTAL, RMI_SWAPS_TOTAL, RMI_REBUILD_DURATION_SECONDS, RMI_REBUILD_IN_PROGRESS,
    SNAPSHOTS_TOTAL, SNAPSHOT_LATENCY_SECONDS, SSE_LAGGED_TOTAL, WAL_BLOCK_CACHE_HITS_TOTAL,
    WAL_BLOCK_CACHE_MISSES_TOTAL, WAL_CRC_ERRORS_TOTAL,
};

#[cfg(not(feature = "bench-no-metrics"))]
pub static APPENDS_TOTAL: Lazy<Counter> = Lazy::new(|| {
    prometheus::register_counter!("kyrodb_appends_total", "Total number of appends")
        .expect("register kyrodb_appends_total")
});

#[cfg(not(feature = "bench-no-metrics"))]
pub static APPEND_LATENCY_SECONDS: Lazy<Histogram> = Lazy::new(|| {
    let opts = HistogramOpts::new("kyrodb_append_latency_seconds", "Append latency in seconds")
        .buckets(vec![0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0]);
    prometheus::register_histogram!(opts).expect("register kyrodb_append_latency_seconds")
});

#[cfg(not(feature = "bench-no-metrics"))]
pub static SNAPSHOTS_TOTAL: Lazy<Counter> = Lazy::new(|| {
    prometheus::register_counter!("kyrodb_snapshots_total", "Total snapshots taken")
        .expect("register kyrodb_snapshots_total")
});

#[cfg(not(feature = "bench-no-metrics"))]
pub static SNAPSHOT_LATENCY_SECONDS: Lazy<Histogram> = Lazy::new(|| {
    let opts = HistogramOpts::new(
        "kyrodb_snapshot_latency_seconds",
        "Snapshot latency in seconds",
    )
    .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 2.0]);
    prometheus::register_histogram!(opts).expect("register kyrodb_snapshot_latency_seconds")
});

#[cfg(not(feature = "bench-no-metrics"))]
pub static SSE_LAGGED_TOTAL: Lazy<Counter> = Lazy::new(|| {
    prometheus::register_counter!(
        "kyrodb_sse_lagged_total",
        "Total number of lagged/dropped SSE events"
    )
    .expect("register kyrodb_sse_lagged_total")
});

// WAL CRC errors
#[cfg(not(feature = "bench-no-metrics"))]
pub static WAL_CRC_ERRORS_TOTAL: Lazy<Counter> = Lazy::new(|| {
    prometheus::register_counter!(
        "kyrodb_wal_crc_errors_total",
        "Total number of WAL frames dropped due to CRC error"
    )
    .expect("register kyrodb_wal_crc_errors_total")
});

// Compactions
#[cfg(not(feature = "bench-no-metrics"))]
pub static COMPACTIONS_TOTAL: Lazy<Counter> = Lazy::new(|| {
    prometheus::register_counter!(
        "kyrodb_compactions_total",
        "Total number of compactions performed"
    )
    .expect("register kyrodb_compactions_total")
});

#[cfg(not(feature = "bench-no-metrics"))]
pub static COMPACTION_DURATION_SECONDS: Lazy<Histogram> = Lazy::new(|| {
    let opts = HistogramOpts::new(
        "kyrodb_compaction_duration_seconds",
        "Compaction duration in seconds",
    )
    .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0]);
    prometheus::register_histogram!(opts).expect("register kyrodb_compaction_duration_seconds")
});

// RMI hits/misses (only incremented when learned-index feature is active)
#[cfg(not(feature = "bench-no-metrics"))]
pub static RMI_HITS_TOTAL: Lazy<Counter> = Lazy::new(|| {
    prometheus::register_counter!(
        "kyrodb_rmi_hits_total",
        "Total number of RMI index hits (delta or main)"
    )
    .expect("register kyrodb_rmi_hits_total")
});

#[cfg(not(feature = "bench-no-metrics"))]
pub static RMI_MISSES_TOTAL: Lazy<Counter> = Lazy::new(|| {
    prometheus::register_counter!(
        "kyrodb_rmi_misses_total",
        "Total number of RMI index misses"
    )
    .expect("register kyrodb_rmi_misses_total")
});

// RMI lookup latency histogram
#[cfg(not(feature = "bench-no-metrics"))]
pub static RMI_LOOKUP_LATENCY_SECONDS: Lazy<Histogram> = Lazy::new(|| {
    let opts = HistogramOpts::new(
        "kyrodb_rmi_lookup_latency_seconds",
        "RMI lookup latency in seconds",
    )
    .buckets(vec![
        0.000_05, 0.000_1, 0.000_2, 0.000_5, 0.001, 0.005, 0.01,
    ]);
    prometheus::register_histogram!(opts).expect("register kyrodb_rmi_lookup_latency_seconds")
});

// RMI lookup latency during rebuild (segmented)
#[cfg(not(feature = "bench-no-metrics"))]
pub static RMI_LOOKUP_LATENCY_DURING_REBUILD_SECONDS: Lazy<Histogram> = Lazy::new(|| {
    let opts = HistogramOpts::new(
        "kyrodb_rmi_lookup_latency_during_rebuild_seconds",
        "RMI lookup latency in seconds while rebuild is in progress",
    )
    .buckets(vec![
        0.000_05, 0.000_1, 0.000_2, 0.000_5, 0.001, 0.005, 0.01,
    ]);
    prometheus::register_histogram!(opts)
        .expect("register kyrodb_rmi_lookup_latency_during_rebuild_seconds")
});

// Epsilon distribution (recorded as histogram in units of keys)
#[cfg(not(feature = "bench-no-metrics"))]
pub static RMI_EPSILON_HISTOGRAM: Lazy<Histogram> = Lazy::new(|| {
    let opts = HistogramOpts::new(
        "kyrodb_rmi_epsilon",
        "Observed epsilon distribution for leaves",
    )
    .buckets(vec![1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0]);
    prometheus::register_histogram!(opts).expect("register kyrodb_rmi_epsilon")
});

// RMI gauges
#[cfg(not(feature = "bench-no-metrics"))]
pub static RMI_INDEX_LEAVES: Lazy<Gauge> = Lazy::new(|| {
    prometheus::register_gauge!(
        "kyrodb_rmi_index_leaves",
        "Number of leaves in the loaded RMI"
    )
    .expect("register kyrodb_rmi_index_leaves")
});

#[cfg(not(feature = "bench-no-metrics"))]
pub static RMI_INDEX_SIZE_BYTES: Lazy<Gauge> = Lazy::new(|| {
    prometheus::register_gauge!(
        "kyrodb_rmi_index_size_bytes",
        "Size of the RMI file on disk"
    )
    .expect("register kyrodb_rmi_index_size_bytes")
});

#[cfg(not(feature = "bench-no-metrics"))]
pub static RMI_EPSILON_MAX: Lazy<Gauge> = Lazy::new(|| {
    prometheus::register_gauge!("kyrodb_rmi_epsilon_max", "Maximum epsilon across leaves")
        .expect("register kyrodb_rmi_epsilon_max")
});

// RMI rebuild metrics
#[cfg(not(feature = "bench-no-metrics"))]
pub static RMI_REBUILDS_TOTAL: Lazy<Counter> = Lazy::new(|| {
    prometheus::register_counter!(
        "kyrodb_rmi_rebuilds_total",
        "Total number of successful RMI rebuilds"
    )
    .expect("register kyrodb_rmi_rebuilds_total")
});

#[cfg(not(feature = "bench-no-metrics"))]
pub static RMI_SWAPS_TOTAL: Lazy<Counter> = Lazy::new(|| {
    prometheus::register_counter!(
        "kyrodb_rmi_swaps_total",
        "Total number of successful atomic index swaps"
    )
    .expect("register kyrodb_rmi_swaps_total")
});

#[cfg(not(feature = "bench-no-metrics"))]
pub static RMI_REBUILD_DURATION_SECONDS: Lazy<Histogram> = Lazy::new(|| {
    let opts = HistogramOpts::new(
        "kyrodb_rmi_rebuild_duration_seconds",
        "Duration of RMI rebuilds in seconds",
    )
    .buckets(vec![0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0]);
    prometheus::register_histogram!(opts).expect("register kyrodb_rmi_rebuild_duration_seconds")
});

// RMI probe length histogram and mispredict counter
#[cfg(not(feature = "bench-no-metrics"))]
pub static RMI_PROBE_LEN: Lazy<Histogram> = Lazy::new(|| {
    let opts = HistogramOpts::new(
        "kyrodb_rmi_probe_len",
        "Number of steps in bounded binary search per RMI lookup",
    )
    .buckets(vec![1.0, 2.0, 3.0, 4.0, 6.0, 8.0, 12.0, 16.0, 24.0, 32.0]);
    prometheus::register_histogram!(opts).expect("register kyrodb_rmi_probe_len")
});

#[cfg(not(feature = "bench-no-metrics"))]
pub static RMI_MISPREDICTS_TOTAL: Lazy<Counter> = Lazy::new(|| {
    prometheus::register_counter!(
        "kyrodb_rmi_mispredicts_total",
        "Total number of bounded-search misses in RMI"
    )
    .expect("register kyrodb_rmi_mispredicts_total")
});

#[cfg(not(feature = "bench-no-metrics"))]
pub static WAL_BLOCK_CACHE_HITS_TOTAL: Lazy<Counter> = Lazy::new(|| {
    prometheus::register_counter!(
        "kyrodb_wal_block_cache_hits_total",
        "Total number of payload cache hits"
    )
    .expect("register kyrodb_wal_block_cache_hits_total")
});

#[cfg(not(feature = "bench-no-metrics"))]
pub static WAL_BLOCK_CACHE_MISSES_TOTAL: Lazy<Counter> = Lazy::new(|| {
    prometheus::register_counter!(
        "kyrodb_wal_block_cache_misses_total",
        "Total number of payload cache misses"
    )
    .expect("register kyrodb_wal_block_cache_misses_total")
});

// Read counters by index type
#[cfg(not(feature = "bench-no-metrics"))]
pub static RMI_READS_TOTAL: Lazy<Counter> = Lazy::new(|| {
    prometheus::register_counter!(
        "kyrodb_rmi_reads_total",
        "Total successful reads served by the RMI index"
    )
    .expect("register kyrodb_rmi_reads_total")
});

#[cfg(not(feature = "bench-no-metrics"))]
pub static BTREE_READS_TOTAL: Lazy<Counter> = Lazy::new(|| {
    prometheus::register_counter!(
        "kyrodb_btree_reads_total",
        "Total successful reads served by the B-Tree index"
    )
    .expect("register kyrodb_btree_reads_total")
});

#[cfg(not(feature = "bench-no-metrics"))]
pub static LOOKUP_FALLBACK_SCAN_TOTAL: Lazy<Counter> = Lazy::new(|| {
    prometheus::register_counter!(
        "kyrodb_lookup_fallback_scan_total",
        "Total number of times we fell back to linear scan after RMI miss"
    )
    .expect("register kyrodb_lookup_fallback_scan_total")
});

#[cfg(not(feature = "bench-no-metrics"))]
pub static INDEX_FALLBACK_SCANS_TOTAL: Lazy<Counter> = Lazy::new(|| {
    prometheus::register_counter!(
        "kyrodb_index_fallback_scans_total",
        "Total number of emergency WAL scans when index corruption is suspected"
    )
    .expect("register kyrodb_index_fallback_scans_total")
});

#[cfg(not(feature = "bench-no-metrics"))]
pub static RMI_REBUILD_IN_PROGRESS: Lazy<Gauge> = Lazy::new(|| {
    prometheus::register_gauge!(
        "kyrodb_rmi_rebuild_in_progress",
        "Gauge set to 1.0 while a background RMI rebuild is running"
    )
    .expect("register kyrodb_rmi_rebuild_in_progress")
});

// RMI rebuild stalls
#[cfg(not(feature = "bench-no-metrics"))]
pub static RMI_REBUILD_STALLS_TOTAL: Lazy<Counter> = Lazy::new(|| {
    prometheus::register_counter!(
        "kyrodb_rmi_rebuild_stalls_total",
        "Total number of stalls during RMI rebuild"
    )
    .expect("register kyrodb_rmi_rebuild_stalls_total")
});

#[cfg(not(feature = "bench-no-metrics"))]
pub fn inc_sse_lagged() {
    SSE_LAGGED_TOTAL.inc();
}

#[cfg(not(feature = "bench-no-metrics"))]
pub fn render() -> String {
    use prometheus::{Encoder, TextEncoder};
    let mut buf = Vec::new();
    let encoder = TextEncoder::new();
    let mf = prometheus::gather();
    encoder.encode(&mf, &mut buf).unwrap();
    String::from_utf8_lossy(&buf).into_owned()
}

#[cfg(not(feature = "bench-no-metrics"))]
pub fn rmi_rebuild_in_progress() -> bool {
    RMI_REBUILD_IN_PROGRESS.get() > 0.5
}

// Enterprise Group Commit Metrics
#[cfg(not(feature = "bench-no-metrics"))]
pub static GROUP_COMMIT_BATCH_SIZE: Lazy<Histogram> = Lazy::new(|| {
    let opts = prometheus::HistogramOpts::new(
        "kyrodb_group_commit_batch_size",
        "Number of writes per group commit batch",
    )
    .buckets(vec![1.0, 5.0, 10.0, 50.0, 100.0, 500.0, 1000.0, 5000.0]);
    prometheus::register_histogram!(opts).expect("register kyrodb_group_commit_batch_size")
});

#[cfg(not(feature = "bench-no-metrics"))]
pub static GROUP_COMMIT_LATENCY_SECONDS: Lazy<Histogram> = Lazy::new(|| {
    let opts = prometheus::HistogramOpts::new(
        "kyrodb_group_commit_latency_seconds",
        "Group commit batch processing latency",
    )
    .buckets(prometheus::exponential_buckets(0.00001, 2.0, 20).unwrap());
    prometheus::register_histogram!(opts).expect("register kyrodb_group_commit_latency_seconds")
});

#[cfg(not(feature = "bench-no-metrics"))]
pub static GROUP_COMMIT_BATCHES_TOTAL: Lazy<Counter> = Lazy::new(|| {
    prometheus::register_counter!(
        "kyrodb_group_commit_batches_total",
        "Total number of group commit batches processed"
    )
    .expect("register kyrodb_group_commit_batches_total")
});

// Binary Protocol Performance Metrics

// Binary Protocol Connection Metrics
#[cfg(not(feature = "bench-no-metrics"))]
pub static BINARY_CONNECTIONS_TOTAL: Lazy<Counter> = Lazy::new(|| {
    prometheus::register_counter!(
        "kyrodb_binary_connections_total",
        "Total binary protocol connections established"
    )
    .expect("register kyrodb_binary_connections_total")
});

#[cfg(not(feature = "bench-no-metrics"))]
pub static BINARY_ACTIVE_CONNECTIONS: Lazy<prometheus::Gauge> = Lazy::new(|| {
    prometheus::register_gauge!(
        "kyrodb_binary_active_connections",
        "Current number of active binary protocol connections"
    )
    .expect("register kyrodb_binary_active_connections")
});

// Binary Protocol Command Metrics
#[cfg(not(feature = "bench-no-metrics"))]
pub static BINARY_COMMANDS_TOTAL: Lazy<prometheus::CounterVec> = Lazy::new(|| {
    prometheus::register_counter_vec!(
        "kyrodb_binary_commands_total",
        "Binary protocol commands processed by type",
        &["command"]
    )
    .expect("register kyrodb_binary_commands_total")
});

#[cfg(not(feature = "bench-no-metrics"))]
pub static BINARY_COMMAND_LATENCY_SECONDS: Lazy<prometheus::HistogramVec> = Lazy::new(|| {
    let opts = prometheus::HistogramOpts::new(
        "kyrodb_binary_command_latency_seconds",
        "Binary protocol command processing latency by type",
    )
    .buckets(prometheus::exponential_buckets(0.000001, 2.0, 20).unwrap()); // Microsecond precision

    prometheus::register_histogram_vec!(opts, &["command"])
        .expect("register kyrodb_binary_command_latency_seconds")
});

// Binary Protocol Batch Processing Metrics
#[cfg(not(feature = "bench-no-metrics"))]
pub static BINARY_BATCH_SIZE: Lazy<Histogram> = Lazy::new(|| {
    let opts = prometheus::HistogramOpts::new(
        "kyrodb_binary_batch_size",
        "Binary protocol batch operation sizes",
    )
    .buckets(vec![
        1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0,
    ]);

    prometheus::register_histogram!(opts).expect("register kyrodb_binary_batch_size")
});

#[cfg(not(feature = "bench-no-metrics"))]
pub static BINARY_BATCH_LOOKUP_LATENCY_SECONDS: Lazy<Histogram> = Lazy::new(|| {
    let opts = prometheus::HistogramOpts::new(
        "kyrodb_binary_batch_lookup_latency_seconds",
        "Binary protocol batch lookup processing latency",
    )
    .buckets(prometheus::exponential_buckets(0.000001, 2.0, 20).unwrap()); // Microsecond precision

    prometheus::register_histogram!(opts)
        .expect("register kyrodb_binary_batch_lookup_latency_seconds")
});

// Binary Protocol Frame Processing Metrics
#[cfg(not(feature = "bench-no-metrics"))]
pub static BINARY_FRAMES_PROCESSED_TOTAL: Lazy<Counter> = Lazy::new(|| {
    prometheus::register_counter!(
        "kyrodb_binary_frames_processed_total",
        "Total binary protocol frames processed successfully"
    )
    .expect("register kyrodb_binary_frames_processed_total")
});

#[cfg(not(feature = "bench-no-metrics"))]
pub static BINARY_FRAMES_INVALID_TOTAL: Lazy<Counter> = Lazy::new(|| {
    prometheus::register_counter!(
        "kyrodb_binary_frames_invalid_total",
        "Total invalid binary protocol frames received"
    )
    .expect("register kyrodb_binary_frames_invalid_total")
});

#[cfg(not(feature = "bench-no-metrics"))]
pub static BINARY_FRAME_SIZE_BYTES: Lazy<Histogram> = Lazy::new(|| {
    let opts = prometheus::HistogramOpts::new(
        "kyrodb_binary_frame_size_bytes",
        "Binary protocol frame sizes in bytes",
    )
    .buckets(prometheus::exponential_buckets(64.0, 2.0, 20).unwrap()); // 64B to 32MB

    prometheus::register_histogram!(opts).expect("register kyrodb_binary_frame_size_bytes")
});

// Binary Protocol Error Metrics
#[cfg(not(feature = "bench-no-metrics"))]
pub static BINARY_PROTOCOL_ERRORS_TOTAL: Lazy<prometheus::CounterVec> = Lazy::new(|| {
    prometheus::register_counter_vec!(
        "kyrodb_binary_protocol_errors_total",
        "Binary protocol errors by type",
        &["error_type"]
    )
    .expect("register kyrodb_binary_protocol_errors_total")
});

#[cfg(not(feature = "bench-no-metrics"))]
pub static BINARY_CRC_ERRORS_TOTAL: Lazy<Counter> = Lazy::new(|| {
    prometheus::register_counter!(
        "kyrodb_binary_crc_errors_total",
        "Total binary protocol CRC validation errors"
    )
    .expect("register kyrodb_binary_crc_errors_total")
});

// Binary Protocol Throughput Metrics
#[cfg(not(feature = "bench-no-metrics"))]
pub static BINARY_BYTES_RECEIVED_TOTAL: Lazy<Counter> = Lazy::new(|| {
    prometheus::register_counter!(
        "kyrodb_binary_bytes_received_total",
        "Total bytes received via binary protocol"
    )
    .expect("register kyrodb_binary_bytes_received_total")
});

#[cfg(not(feature = "bench-no-metrics"))]
pub static BINARY_BYTES_SENT_TOTAL: Lazy<Counter> = Lazy::new(|| {
    prometheus::register_counter!(
        "kyrodb_binary_bytes_sent_total",
        "Total bytes sent via binary protocol"
    )
    .expect("register kyrodb_binary_bytes_sent_total")
});

// Binary Protocol SIMD Optimization Metrics (Phase 4)
#[cfg(not(feature = "bench-no-metrics"))]
pub static BINARY_SIMD_BATCHES_TOTAL: Lazy<Counter> = Lazy::new(|| {
    prometheus::register_counter!(
        "kyrodb_binary_simd_batches_total",
        "Total SIMD-optimized batch operations processed"
    )
    .expect("register kyrodb_binary_simd_batches_total")
});

#[cfg(not(feature = "bench-no-metrics"))]
pub static BINARY_SIMD_SPEEDUP_RATIO: Lazy<Histogram> = Lazy::new(|| {
    let opts = prometheus::HistogramOpts::new(
        "kyrodb_binary_simd_speedup_ratio",
        "SIMD vs scalar performance speedup ratio",
    )
    .buckets(vec![1.0, 1.5, 2.0, 3.0, 4.0, 6.0, 8.0, 12.0, 16.0]);

    prometheus::register_histogram!(opts).expect("register kyrodb_binary_simd_speedup_ratio")
});

// Enhanced RMI performance metrics
#[cfg(not(feature = "bench-no-metrics"))]
pub static RMI_BATCH_LOOKUP_LATENCY_SECONDS: Lazy<Histogram> = Lazy::new(|| {
    let opts = HistogramOpts::new(
        "kyrodb_rmi_batch_lookup_latency_seconds",
        "RMI batch lookup latency in seconds",
    )
    .buckets(vec![
        0.000_001, 0.000_005, 0.000_01, 0.000_05, 0.000_1, 0.000_5, 0.001, 0.005, 0.01,
    ]);
    prometheus::register_histogram!(opts).expect("register kyrodb_rmi_batch_lookup_latency_seconds")
});

#[cfg(not(feature = "bench-no-metrics"))]
pub static RMI_SINGLE_LOOKUP_LATENCY_SECONDS: Lazy<Histogram> = Lazy::new(|| {
    let opts = HistogramOpts::new(
        "kyrodb_rmi_single_lookup_latency_seconds",
        "RMI single lookup latency in seconds",
    )
    .buckets(vec![
        0.000_000_1,
        0.000_000_5,
        0.000_001,
        0.000_005,
        0.000_01,
        0.000_05,
        0.000_1,
        0.000_5,
        0.001,
    ]);
    prometheus::register_histogram!(opts)
        .expect("register kyrodb_rmi_single_lookup_latency_seconds")
});

#[cfg(not(feature = "bench-no-metrics"))]
pub static RMI_SIMD_BATCH_SIZE: Lazy<Gauge> = Lazy::new(|| {
    prometheus::register_gauge!(
        "kyrodb_rmi_simd_batch_size",
        "Current SIMD batch size for RMI operations"
    )
    .expect("register kyrodb_rmi_simd_batch_size")
});

#[cfg(not(feature = "bench-no-metrics"))]
pub static RMI_CACHE_HIT_RATE: Lazy<Gauge> = Lazy::new(|| {
    prometheus::register_gauge!("kyrodb_rmi_cache_hit_rate", "RMI cache hit rate percentage")
        .expect("register kyrodb_rmi_cache_hit_rate")
});

#[cfg(not(feature = "bench-no-metrics"))]
pub static RMI_PREFETCH_EFFECTIVENESS: Lazy<Gauge> = Lazy::new(|| {
    prometheus::register_gauge!(
        "kyrodb_rmi_prefetch_effectiveness",
        "RMI prefetch effectiveness percentage"
    )
    .expect("register kyrodb_rmi_prefetch_effectiveness")
});

#[cfg(not(feature = "bench-no-metrics"))]
pub static RMI_MEMORY_POOL_USAGE: Lazy<Gauge> = Lazy::new(|| {
    prometheus::register_gauge!(
        "kyrodb_rmi_memory_pool_usage",
        "RMI memory pool usage percentage"
    )
    .expect("register kyrodb_rmi_memory_pool_usage")
});

#[cfg(not(feature = "bench-no-metrics"))]
pub static RMI_ADAPTIVE_OPTIMIZATIONS_TOTAL: Lazy<Counter> = Lazy::new(|| {
    prometheus::register_counter!(
        "kyrodb_rmi_adaptive_optimizations_total",
        "Total number of adaptive optimizations applied"
    )
    .expect("register kyrodb_rmi_adaptive_optimizations_total")
});

// Binary Protocol Metric Helper Functions

/// Register binary protocol metrics with the global Prometheus registry
#[cfg(not(feature = "bench-no-metrics"))]
pub fn register_binary_metrics() {
    // Metrics are registered lazily via Lazy::new(), so just force evaluation
    Lazy::force(&BINARY_CONNECTIONS_TOTAL);
    Lazy::force(&BINARY_ACTIVE_CONNECTIONS);
    Lazy::force(&BINARY_COMMANDS_TOTAL);
    Lazy::force(&BINARY_COMMAND_LATENCY_SECONDS);
    Lazy::force(&BINARY_BATCH_SIZE);
    Lazy::force(&BINARY_BATCH_LOOKUP_LATENCY_SECONDS);
    Lazy::force(&BINARY_FRAMES_PROCESSED_TOTAL);
    Lazy::force(&BINARY_FRAMES_INVALID_TOTAL);
    Lazy::force(&BINARY_FRAME_SIZE_BYTES);
    Lazy::force(&BINARY_PROTOCOL_ERRORS_TOTAL);
    Lazy::force(&BINARY_CRC_ERRORS_TOTAL);
    Lazy::force(&BINARY_BYTES_RECEIVED_TOTAL);
    Lazy::force(&BINARY_BYTES_SENT_TOTAL);
    Lazy::force(&BINARY_SIMD_BATCHES_TOTAL);
    Lazy::force(&BINARY_SIMD_SPEEDUP_RATIO);
}

/// Increment binary protocol command counter for specific command type
#[cfg(not(feature = "bench-no-metrics"))]
pub fn inc_binary_command(command: &str) {
    BINARY_COMMANDS_TOTAL.with_label_values(&[command]).inc();
}

/// Record binary protocol command latency
#[cfg(not(feature = "bench-no-metrics"))]
pub fn observe_binary_command_latency(command: &str, duration_seconds: f64) {
    BINARY_COMMAND_LATENCY_SECONDS
        .with_label_values(&[command])
        .observe(duration_seconds);
}

/// Record binary protocol error by type
#[cfg(not(feature = "bench-no-metrics"))]
pub fn inc_binary_protocol_error(error_type: &str) {
    BINARY_PROTOCOL_ERRORS_TOTAL
        .with_label_values(&[error_type])
        .inc();
}

// Stub implementations for bench-no-metrics feature

#[cfg(feature = "bench-no-metrics")]
pub fn register_binary_metrics() {
    // No-op for benchmarking
}

#[cfg(feature = "bench-no-metrics")]
pub fn inc_binary_command(_command: &str) {
    // No-op for benchmarking
}

#[cfg(feature = "bench-no-metrics")]
pub fn observe_binary_command_latency(_command: &str, _duration_seconds: f64) {
    // No-op for benchmarking
}

#[cfg(feature = "bench-no-metrics")]
pub fn inc_binary_protocol_error(_error_type: &str) {
    // No-op for benchmarking
}

// Export binary protocol metrics to shim module for bench-no-metrics
#[cfg(feature = "bench-no-metrics")]
mod binary_protocol_shim {
    use super::shim::*;
    use once_cell::sync::Lazy;

    pub static BINARY_CONNECTIONS_TOTAL: Lazy<NoopCounter> = Lazy::new(|| NoopCounter);
    pub static BINARY_ACTIVE_CONNECTIONS: Lazy<NoopGauge> = Lazy::new(|| NoopGauge);
    pub static BINARY_FRAMES_PROCESSED_TOTAL: Lazy<NoopCounter> = Lazy::new(|| NoopCounter);
    pub static BINARY_FRAMES_INVALID_TOTAL: Lazy<NoopCounter> = Lazy::new(|| NoopCounter);
    pub static BINARY_CRC_ERRORS_TOTAL: Lazy<NoopCounter> = Lazy::new(|| NoopCounter);
    pub static BINARY_BYTES_RECEIVED_TOTAL: Lazy<NoopCounter> = Lazy::new(|| NoopCounter);
    pub static BINARY_BYTES_SENT_TOTAL: Lazy<NoopCounter> = Lazy::new(|| NoopCounter);
    pub static BINARY_SIMD_BATCHES_TOTAL: Lazy<NoopCounter> = Lazy::new(|| NoopCounter);
    pub static BINARY_BATCH_SIZE: Lazy<NoopHistogram> = Lazy::new(|| NoopHistogram);
    pub static BINARY_BATCH_LOOKUP_LATENCY_SECONDS: Lazy<NoopHistogram> =
        Lazy::new(|| NoopHistogram);
    pub static BINARY_FRAME_SIZE_BYTES: Lazy<NoopHistogram> = Lazy::new(|| NoopHistogram);
    pub static BINARY_SIMD_SPEEDUP_RATIO: Lazy<NoopHistogram> = Lazy::new(|| NoopHistogram);

    // Enhanced RMI performance metrics
    pub static RMI_BATCH_LOOKUP_LATENCY_SECONDS: Lazy<NoopHistogram> = Lazy::new(|| NoopHistogram);
    pub static RMI_SINGLE_LOOKUP_LATENCY_SECONDS: Lazy<NoopHistogram> = Lazy::new(|| NoopHistogram);
    pub static RMI_SIMD_BATCH_SIZE: Lazy<NoopGauge> = Lazy::new(|| NoopGauge);
    pub static RMI_CACHE_HIT_RATE: Lazy<NoopGauge> = Lazy::new(|| NoopGauge);
    pub static RMI_PREFETCH_EFFECTIVENESS: Lazy<NoopGauge> = Lazy::new(|| NoopGauge);
    pub static RMI_MEMORY_POOL_USAGE: Lazy<NoopGauge> = Lazy::new(|| NoopGauge);
    pub static RMI_ADAPTIVE_OPTIMIZATIONS_TOTAL: Lazy<NoopCounter> = Lazy::new(|| NoopCounter);
}

#[cfg(feature = "bench-no-metrics")]
pub use binary_protocol_shim::*;

/// Thread-safe metrics recording utilities
///
/// Provides consistent and thread-safe metrics recording to prevent
/// race conditions and inconsistent values
pub mod metrics_utils {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    /// Thread-safe hit rate calculator
    pub struct HitRateCalculator {
        hits: Arc<AtomicU64>,
        total: Arc<AtomicU64>,
    }

    impl HitRateCalculator {
        pub fn new() -> Self {
            Self {
                hits: Arc::new(AtomicU64::new(0)),
                total: Arc::new(AtomicU64::new(0)),
            }
        }

        /// Record a hit
        pub fn record_hit(&self) {
            self.hits.fetch_add(1, Ordering::Relaxed);
            self.total.fetch_add(1, Ordering::Relaxed);
        }

        /// Record a miss
        pub fn record_miss(&self) {
            self.total.fetch_add(1, Ordering::Relaxed);
        }

        /// Record batch results
        pub fn record_batch(&self, hits: u64, total: u64) {
            self.hits.fetch_add(hits, Ordering::Relaxed);
            self.total.fetch_add(total, Ordering::Relaxed);
        }

        /// Get current hit rate (0.0 to 1.0)
        pub fn hit_rate(&self) -> f64 {
            let total = self.total.load(Ordering::Relaxed);
            if total == 0 {
                return 1.0; // Perfect hit rate for no operations
            }
            let hits = self.hits.load(Ordering::Relaxed);
            hits as f64 / total as f64
        }

        /// Get current statistics
        pub fn stats(&self) -> (u64, u64) {
            let hits = self.hits.load(Ordering::Relaxed);
            let total = self.total.load(Ordering::Relaxed);
            (hits, total)
        }

        /// Reset counters
        pub fn reset(&self) {
            self.hits.store(0, Ordering::Relaxed);
            self.total.store(0, Ordering::Relaxed);
        }
    }

    impl Default for HitRateCalculator {
        fn default() -> Self {
            Self::new()
        }
    }

    /// Safely record metrics with proper error handling
    #[cfg(not(feature = "bench-no-metrics"))]
    pub fn record_rmi_lookup_result(result: &Option<u64>, latency_seconds: f64) {
        // Record latency
        crate::metrics::RMI_SINGLE_LOOKUP_LATENCY_SECONDS.observe(latency_seconds);

        // Record hit/miss
        if result.is_some() {
            crate::metrics::RMI_HITS_TOTAL.inc();
        } else {
            crate::metrics::RMI_MISSES_TOTAL.inc();
        }
    }

    /// Safely record batch metrics
    #[cfg(not(feature = "bench-no-metrics"))]
    pub fn record_rmi_batch_results(results: &[Option<u64>], latency_seconds: f64) {
        // Record batch latency
        crate::metrics::RMI_BATCH_LOOKUP_LATENCY_SECONDS.observe(latency_seconds);

        // Count hits and misses
        let hits = results.iter().filter(|r| r.is_some()).count();
        let misses = results.len() - hits;

        // Record metrics atomically
        crate::metrics::RMI_HITS_TOTAL.inc_by(hits as f64);
        crate::metrics::RMI_MISSES_TOTAL.inc_by(misses as f64);

        // Record batch size
        crate::metrics::BINARY_BATCH_SIZE.observe(results.len() as f64);
    }

    /// Update performance gauges
    #[cfg(not(feature = "bench-no-metrics"))]
    pub fn update_performance_gauges(
        cache_hit_rate: f64,
        prefetch_effectiveness: f64,
        memory_pool_usage: f64,
        current_batch_size: usize,
    ) {
        crate::metrics::RMI_CACHE_HIT_RATE.set(cache_hit_rate);
        crate::metrics::RMI_PREFETCH_EFFECTIVENESS.set(prefetch_effectiveness);
        crate::metrics::RMI_MEMORY_POOL_USAGE.set(memory_pool_usage);
        crate::metrics::RMI_SIMD_BATCH_SIZE.set(current_batch_size as f64);
    }

    // No-op implementations for bench-no-metrics
    #[cfg(feature = "bench-no-metrics")]
    pub fn record_rmi_lookup_result(_result: &Option<u64>, _latency_seconds: f64) {}

    #[cfg(feature = "bench-no-metrics")]
    pub fn record_rmi_batch_results(_results: &[Option<u64>], _latency_seconds: f64) {}

    #[cfg(feature = "bench-no-metrics")]
    pub fn update_performance_gauges(
        _cache_hit_rate: f64,
        _prefetch_effectiveness: f64,
        _memory_pool_usage: f64,
        _current_batch_size: usize,
    ) {
    }
}
