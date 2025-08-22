use once_cell::sync::Lazy;
use prometheus::{Counter, Gauge, Histogram, HistogramOpts};

#[cfg(feature = "bench-no-metrics")]
mod shim {
    use super::*;
    pub struct NoopCounter;
    impl NoopCounter { pub fn inc(&self) {} }
    pub struct NoopGauge;
    impl NoopGauge { pub fn set(&self, _v: f64) {} }
    pub struct NoopHistogram;
    impl NoopHistogram { pub fn observe(&self, _v: f64) {} pub fn start_timer(&self) -> NoopTimer { NoopTimer } }
    pub struct NoopTimer; impl NoopTimer { pub fn observe_duration(&self) {} }
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
    pub static RMI_EPSILON_HISTOGRAM: Lazy<NoopHistogram> = Lazy::new(|| NoopHistogram);
    pub static RMI_INDEX_LEAVES: Lazy<NoopGauge> = Lazy::new(|| NoopGauge);
    pub static RMI_INDEX_SIZE_BYTES: Lazy<NoopGauge> = Lazy::new(|| NoopGauge);
    pub static RMI_EPSILON_MAX: Lazy<NoopGauge> = Lazy::new(|| NoopGauge);
    pub static RMI_REBUILDS_TOTAL: Lazy<NoopCounter> = Lazy::new(|| NoopCounter);
    pub static RMI_REBUILD_DURATION_SECONDS: Lazy<NoopHistogram> = Lazy::new(|| NoopHistogram);
    pub static RMI_PROBE_LEN: Lazy<NoopHistogram> = Lazy::new(|| NoopHistogram);
    pub static RMI_MISPREDICTS_TOTAL: Lazy<NoopCounter> = Lazy::new(|| NoopCounter);
    pub static RMI_READS_TOTAL: Lazy<NoopCounter> = Lazy::new(|| NoopCounter);
    pub static BTREE_READS_TOTAL: Lazy<NoopCounter> = Lazy::new(|| NoopCounter);
    pub fn inc_sse_lagged() {}
    pub fn render() -> String { String::new() }
}

#[cfg(feature = "bench-no-metrics")]
pub use shim::{
    APPEND_LATENCY_SECONDS, APPENDS_TOTAL, BTREE_READS_TOTAL, COMPACTION_DURATION_SECONDS,
    COMPACTIONS_TOTAL, RMI_EPSILON_HISTOGRAM, RMI_EPSILON_MAX, RMI_HITS_TOTAL,
    RMI_INDEX_LEAVES, RMI_INDEX_SIZE_BYTES, RMI_LOOKUP_LATENCY_SECONDS, RMI_MISPREDICTS_TOTAL,
    RMI_MISSES_TOTAL, RMI_PROBE_LEN, RMI_READS_TOTAL, RMI_REBUILD_DURATION_SECONDS,
    RMI_REBUILDS_TOTAL, SNAPSHOT_LATENCY_SECONDS, SNAPSHOTS_TOTAL, SSE_LAGGED_TOTAL,
    WAL_BLOCK_CACHE_HITS_TOTAL, WAL_BLOCK_CACHE_MISSES_TOTAL, WAL_CRC_ERRORS_TOTAL, inc_sse_lagged,
    render,
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

// New: WAL CRC errors
#[cfg(not(feature = "bench-no-metrics"))]
pub static WAL_CRC_ERRORS_TOTAL: Lazy<Counter> = Lazy::new(|| {
    prometheus::register_counter!(
        "kyrodb_wal_crc_errors_total",
        "Total number of WAL frames dropped due to CRC error"
    )
    .expect("register kyrodb_wal_crc_errors_total")
});

// New: Compactions
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

// New: RMI hits/misses (only incremented when learned-index feature is active)
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

// New: RMI lookup latency histogram
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

// New: epsilon distribution (recorded as histogram in units of keys)
#[cfg(not(feature = "bench-no-metrics"))]
pub static RMI_EPSILON_HISTOGRAM: Lazy<Histogram> = Lazy::new(|| {
    let opts = HistogramOpts::new(
        "kyrodb_rmi_epsilon",
        "Observed epsilon distribution for leaves",
    )
    .buckets(vec![1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0]);
    prometheus::register_histogram!(opts).expect("register kyrodb_rmi_epsilon")
});

// New: RMI gauges
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

// New: RMI rebuild metrics
#[cfg(not(feature = "bench-no-metrics"))]
pub static RMI_REBUILDS_TOTAL: Lazy<Counter> = Lazy::new(|| {
    prometheus::register_counter!(
        "kyrodb_rmi_rebuilds_total",
        "Total number of successful RMI rebuilds"
    )
    .expect("register kyrodb_rmi_rebuilds_total")
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

// New: RMI probe length histogram and mispredict counter
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

// New: read counters by index type
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
