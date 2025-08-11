use once_cell::sync::Lazy;
use prometheus::{self, Encoder, Histogram, HistogramOpts, IntCounter, TextEncoder};

pub static APPENDS_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    prometheus::register_int_counter!(
        "ngdb_appends_total",
        "Total number of append operations"
    )
    .expect("register ngdb_appends_total")
});

pub static APPEND_LATENCY_SECONDS: Lazy<Histogram> = Lazy::new(|| {
    let opts = HistogramOpts::new("ngdb_append_latency_seconds", "Append latency in seconds")
        .buckets(vec![0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0]);
    prometheus::register_histogram!(opts).expect("register ngdb_append_latency_seconds")
});

pub static SNAPSHOTS_TOTAL: Lazy<IntCounter> = Lazy::new(|| {
    prometheus::register_int_counter!(
        "ngdb_snapshots_total",
        "Total number of snapshots written"
    )
    .expect("register ngdb_snapshots_total")
});

pub static SNAPSHOT_LATENCY_SECONDS: Lazy<Histogram> = Lazy::new(|| {
    let opts = HistogramOpts::new(
        "ngdb_snapshot_latency_seconds",
        "Snapshot latency in seconds",
    )
    .buckets(vec![0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0]);
    prometheus::register_histogram!(opts).expect("register ngdb_snapshot_latency_seconds")
});

pub fn render() -> String {
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();
    let encoder = TextEncoder::new();
    encoder
        .encode(&metric_families, &mut buffer)
        .expect("encode metrics");
    String::from_utf8(buffer).unwrap_or_default()
}