use once_cell::sync::Lazy;
use prometheus::{Counter, Encoder, Histogram, HistogramOpts, TextEncoder};

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

pub fn inc_sse_lagged() { SSE_LAGGED_TOTAL.inc(); }

pub fn render() -> String {
    let mut buf = Vec::new();
    let encoder = TextEncoder::new();
    let mf = prometheus::gather();
    encoder.encode(&mf, &mut buf).unwrap();
    String::from_utf8_lossy(&buf).into_owned()
}