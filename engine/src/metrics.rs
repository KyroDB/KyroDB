use once_cell::sync::Lazy;
use prometheus::{register_histogram, register_int_counter, register_int_gauge, Histogram, IntCounter, IntGauge};

pub static APPENDS_TOTAL: Lazy<IntCounter> = Lazy::new(|| register_int_counter!("kyrodb_appends_total", "Total appends").unwrap());
pub static APPEND_LATENCY_SECONDS: Lazy<Histogram> = Lazy::new(|| register_histogram!("kyrodb_append_latency_seconds", "Append latency").unwrap());
pub static SNAPSHOTS_TOTAL: Lazy<IntCounter> = Lazy::new(|| register_int_counter!("kyrodb_snapshots_total", "Total snapshots").unwrap());
pub static SNAPSHOT_LATENCY_SECONDS: Lazy<Histogram> = Lazy::new(|| register_histogram!("kyrodb_snapshot_latency_seconds", "Snapshot latency").unwrap());
pub static COMPACTIONS_TOTAL: Lazy<IntCounter> = Lazy::new(|| register_int_counter!("kyrodb_compactions_total", "Total compactions").unwrap());
pub static COMPACTION_DURATION_SECONDS: Lazy<Histogram> = Lazy::new(|| register_histogram!("kyrodb_compaction_duration_seconds", "Compaction duration").unwrap());
pub static COMPACTION_BYTES_PROCESSED: Lazy<Histogram> = Lazy::new(|| register_histogram!("kyrodb_compaction_bytes_processed", "Compaction bytes processed").unwrap());
pub static COMPACTION_BYTES_SAVED: Lazy<Histogram> = Lazy::new(|| register_histogram!("kyrodb_compaction_bytes_saved", "Compaction bytes saved").unwrap());
pub static WAL_SIZE_BYTES: Lazy<IntGauge> = Lazy::new(|| register_int_gauge!("kyrodb_wal_size_bytes", "Current WAL size").unwrap());
pub static SNAPSHOT_SIZE_BYTES: Lazy<IntGauge> = Lazy::new(|| register_int_gauge!("kyrodb_snapshot_size_bytes", "Current snapshot size").unwrap());
pub static WAL_CRC_ERRORS_TOTAL: Lazy<IntCounter> = Lazy::new(|| register_int_counter!("kyrodb_wal_crc_errors_total", "Total WAL CRC errors").unwrap());
pub static CACHE_HITS_TOTAL: Lazy<IntCounter> = Lazy::new(|| register_int_counter!("kyrodb_cache_hits_total", "Total cache hits for reads").unwrap());
pub static CACHE_MISSES_TOTAL: Lazy<IntCounter> = Lazy::new(|| register_int_counter!("kyrodb_cache_misses_total", "Total cache misses for reads").unwrap());

// --- RMI Metrics ---
#[cfg(feature = "learned-index")]
pub static RMI_HITS_TOTAL: Lazy<IntCounter> = Lazy::new(|| register_int_counter!("kyrodb_rmi_hits_total", "Total RMI hits").unwrap());
#[cfg(feature = "learned-index")]
pub static RMI_MISSES_TOTAL: Lazy<IntCounter> = Lazy::new(|| register_int_counter!("kyrodb_rmi_misses_total", "Total RMI misses").unwrap());
#[cfg(feature = "learned-index")]
pub static RMI_HIT_RATE: Lazy<IntGauge> = Lazy::new(|| register_int_gauge!("kyrodb_rmi_hit_rate", "Instantaneous RMI hit rate").unwrap());
#[cfg(feature = "learned-index")]
pub static RMI_LOOKUP_LATENCY_SECONDS: Lazy<Histogram> = Lazy::new(|| register_histogram!("kyrodb_rmi_lookup_latency_seconds", "RMI lookup latency").unwrap());
#[cfg(feature = "learned-index")]
pub static RMI_EPSILON_HISTOGRAM: Lazy<Histogram> = Lazy::new(|| register_histogram!("kyrodb_rmi_epsilon", "Observed epsilon distribution for leaves").unwrap());
#[cfg(feature = "learned-index")]
pub static RMI_INDEX_LEAVES: Lazy<IntGauge> = Lazy::new(|| register_int_gauge!("kyrodb_rmi_index_leaves", "Number of leaves in the loaded RMI").unwrap());
#[cfg(feature = "learned-index")]
pub static RMI_INDEX_SIZE_BYTES: Lazy<IntGauge> = Lazy::new(|| register_int_gauge!("kyrodb_rmi_index_size_bytes", "Size of the RMI file on disk").unwrap());
pub static INDEX_SIZE_BYTES: Lazy<IntGauge> = Lazy::new(|| register_int_gauge!("kyrodb_index_size_bytes", "Size of the primary index on disk").unwrap());
#[cfg(feature = "learned-index")]
pub static RMI_EPSILON_MAX: Lazy<IntGauge> = Lazy::new(|| register_int_gauge!("kyrodb_rmi_epsilon_max", "Maximum epsilon across leaves").unwrap());
#[cfg(feature = "learned-index")]
pub static RMI_REBUILDS_TOTAL: Lazy<IntCounter> = Lazy::new(|| register_int_counter!("kyrodb_rmi_rebuilds_total", "Total number of successful RMI rebuilds").unwrap());
#[cfg(feature = "learned-index")]
pub static RMI_REBUILD_DURATION_SECONDS: Lazy<Histogram> = Lazy::new(|| register_histogram!("kyrodb_rmi_rebuild_duration_seconds", "Duration of RMI rebuilds").unwrap());
#[cfg(feature = "learned-index")]
pub static RMI_PROBE_LEN: Lazy<Histogram> = Lazy::new(|| register_histogram!("kyrodb_rmi_probe_len", "Number of probe steps taken by RMI bounded search").unwrap());
#[cfg(feature = "learned-index")]
pub static RMI_MISPREDICTS_TOTAL: Lazy<IntCounter> = Lazy::new(|| register_int_counter!("kyrodb_rmi_mispredicts_total", "Total number of RMI mispredicts").unwrap());

pub fn inc_sse_lagged() { SSE_LAGGED_TOTAL.inc(); }

pub fn render() -> String {
    let mut buf = Vec::new();
    let encoder = TextEncoder::new();
    let mf = prometheus::gather();
    encoder.encode(&mf, &mut buf).unwrap();
    String::from_utf8_lossy(&buf).into_owned()
}