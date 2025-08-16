use once_cell::sync::Lazy;
use prometheus::{Counter, Encoder, Gauge, Histogram, HistogramOpts, TextEncoder};

pub static APPENDS_TOTAL: Lazy<Counter> = Lazy::new(|| {
    prometheus::register_counter!(
        "kyrodb_appends_total",
        "Total number of appends"
    )
    .expect("register kyrodb_appends_total")
});

pub static APPEND_LATENCY_SECONDS: Lazy<Histogram> = Lazy::new(|| {
    let opts = HistogramOpts::new("kyrodb_append_latency_seconds", "Append latency in seconds")
        .buckets(vec![0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0]);
    prometheus::register_histogram!(opts).expect("register kyrodb_append_latency_seconds")
});

pub static SNAPSHOTS_TOTAL: Lazy<Counter> = Lazy::new(|| {
    prometheus::register_counter!(
        "kyrodb_snapshots_total",
        "Total snapshots taken"
    )
    .expect("register kyrodb_snapshots_total")
});

pub static SNAPSHOT_LATENCY_SECONDS: Lazy<Histogram> = Lazy::new(|| {
    let opts = HistogramOpts::new(
        "kyrodb_snapshot_latency_seconds",
        "Snapshot latency in seconds",
    )
    .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 2.0]);
    prometheus::register_histogram!(opts).expect("register kyrodb_snapshot_latency_seconds")
});

// New: snapshot size bytes gauge
pub static SNAPSHOT_SIZE_BYTES: Lazy<Gauge> = Lazy::new(|| {
    prometheus::register_gauge!(
        "kyrodb_snapshot_size_bytes",
        "Size of the current snapshot file in bytes"
    ).expect("register kyrodb_snapshot_size_bytes")
});

pub static SSE_LAGGED_TOTAL: Lazy<Counter> = Lazy::new(|| {
    prometheus::register_counter!(
        "kyrodb_sse_lagged_total",
        "Total number of lagged/dropped SSE events"
    )
    .expect("register kyrodb_sse_lagged_total")
});

// New: WAL CRC errors
pub static WAL_CRC_ERRORS_TOTAL: Lazy<Counter> = Lazy::new(|| {
    prometheus::register_counter!(
        "kyrodb_wal_crc_errors_total",
        "Total number of WAL frames dropped due to CRC error"
    )
    .expect("register kyrodb_wal_crc_errors_total")
});

// New: WAL size bytes gauge
pub static WAL_SIZE_BYTES: Lazy<Gauge> = Lazy::new(|| {
    prometheus::register_gauge!(
        "kyrodb_wal_size_bytes",
        "Total size of WAL segments in bytes"
    ).expect("register kyrodb_wal_size_bytes")
});

// New: Compactions
pub static COMPACTIONS_TOTAL: Lazy<Counter> = Lazy::new(|| {
    prometheus::register_counter!(
        "kyrodb_compactions_total",
        "Total number of compactions performed"
    )
    .expect("register kyrodb_compactions_total")
});

pub static COMPACTION_DURATION_SECONDS: Lazy<Histogram> = Lazy::new(|| {
    let opts = HistogramOpts::new(
        "kyrodb_compaction_duration_seconds",
        "Compaction duration in seconds",
    )
    .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0]);
    prometheus::register_histogram!(opts).expect("register kyrodb_compaction_duration_seconds")
});

// New: Compaction bytes processed/saved histograms
pub static COMPACTION_BYTES_PROCESSED: Lazy<Histogram> = Lazy::new(|| {
    let opts = HistogramOpts::new(
        "kyrodb_compaction_bytes_processed",
        "Bytes scanned/processed during compaction"
    ).buckets(vec![1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9]);
    prometheus::register_histogram!(opts).expect("register kyrodb_compaction_bytes_processed")
});

pub static COMPACTION_BYTES_SAVED: Lazy<Histogram> = Lazy::new(|| {
    let opts = HistogramOpts::new(
        "kyrodb_compaction_bytes_saved",
        "Bytes reduced/saved by compaction"
    ).buckets(vec![1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9]);
    prometheus::register_histogram!(opts).expect("register kyrodb_compaction_bytes_saved")
});

// New: RMI hits/misses (only incremented when learned-index feature is active)
pub static RMI_HITS_TOTAL: Lazy<Counter> = Lazy::new(|| {
    prometheus::register_counter!(
        "kyrodb_rmi_hits_total",
        "Total number of RMI index hits (delta or main)"
    )
    .expect("register kyrodb_rmi_hits_total")
});

pub static RMI_MISSES_TOTAL: Lazy<Counter> = Lazy::new(|| {
    prometheus::register_counter!(
        "kyrodb_rmi_misses_total",
        "Total number of RMI index misses"
    )
    .expect("register kyrodb_rmi_misses_total")
});

// New: RMI hit rate gauge
pub static RMI_HIT_RATE: Lazy<Gauge> = Lazy::new(|| {
    prometheus::register_gauge!(
        "kyrodb_rmi_hit_rate",
        "Instantaneous RMI hit rate computed as hits/(hits+misses)"
    ).expect("register kyrodb_rmi_hit_rate")
});

// New: RMI lookup latency histogram
pub static RMI_LOOKUP_LATENCY_SECONDS: Lazy<Histogram> = Lazy::new(|| {
    let opts = HistogramOpts::new(
        "kyrodb_rmi_lookup_latency_seconds",
        "RMI lookup latency in seconds",
    )
    .buckets(vec![0.000_05, 0.000_1, 0.000_2, 0.000_5, 0.001, 0.005, 0.01]);
    prometheus::register_histogram!(opts).expect("register kyrodb_rmi_lookup_latency_seconds")
});

// New: epsilon distribution (recorded as histogram in units of keys)
pub static RMI_EPSILON_HISTOGRAM: Lazy<Histogram> = Lazy::new(|| {
    let opts = HistogramOpts::new(
        "kyrodb_rmi_epsilon",
        "Observed epsilon distribution for leaves",
    )
    .buckets(vec![1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0]);
    prometheus::register_histogram!(opts).expect("register kyrodb_rmi_epsilon")
});

// New: RMI gauges
pub static RMI_INDEX_LEAVES: Lazy<Gauge> = Lazy::new(|| {
    prometheus::register_gauge!(
        "kyrodb_rmi_index_leaves",
        "Number of leaves in the loaded RMI"
    ).expect("register kyrodb_rmi_index_leaves")
});

pub static RMI_INDEX_SIZE_BYTES: Lazy<Gauge> = Lazy::new(|| {
    prometheus::register_gauge!(
        "kyrodb_rmi_index_size_bytes",
        "Size of the RMI file on disk"
    ).expect("register kyrodb_rmi_index_size_bytes")
});

// New: general index size gauge (mirrors RMI size for now)
pub static INDEX_SIZE_BYTES: Lazy<Gauge> = Lazy::new(|| {
    prometheus::register_gauge!(
        "kyrodb_index_size_bytes",
        "Size of the primary index on disk"
    ).expect("register kyrodb_index_size_bytes")
});

pub static RMI_EPSILON_MAX: Lazy<Gauge> = Lazy::new(|| {
    prometheus::register_gauge!(
        "kyrodb_rmi_epsilon_max",
        "Maximum epsilon across leaves"
    ).expect("register kyrodb_rmi_epsilon_max")
});

// New: RMI rebuild metrics
pub static RMI_REBUILDS_TOTAL: Lazy<Counter> = Lazy::new(|| {
    prometheus::register_counter!(
        "kyrodb_rmi_rebuilds_total",
        "Total number of successful RMI rebuilds"
    ).expect("register kyrodb_rmi_rebuilds_total")
});

pub static RMI_REBUILD_DURATION_SECONDS: Lazy<Histogram> = Lazy::new(|| {
    let opts = HistogramOpts::new(
        "kyrodb_rmi_rebuild_duration_seconds",
        "Duration of RMI rebuilds in seconds",
    )
    .buckets(vec![0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0]);
    prometheus::register_histogram!(opts).expect("register kyrodb_rmi_rebuild_duration_seconds")
});

pub fn inc_sse_lagged() { SSE_LAGGED_TOTAL.inc(); }

pub fn render() -> String {
    let mut buf = Vec::new();
    let encoder = TextEncoder::new();
    let mf = prometheus::gather();
    encoder.encode(&mf, &mut buf).unwrap();
    String::from_utf8_lossy(&buf).into_owned()
}