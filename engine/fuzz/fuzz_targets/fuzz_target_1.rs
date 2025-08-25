#![no_main]

use libfuzzer_sys::fuzz_target;

// Fuzz the WAL and snapshot data parsers; do not assert correctness, just ensure no panics.
fuzz_target!(|data: &[u8]| {
    // Split input into two parts to simulate WAL bytes and snapshot.data bytes
    let mid = data.len() / 2;
    let (wal, snap) = data.split_at(mid);
    let _ = kyrodb_engine::parse_wal_stream_bytes(wal);
    let _ = kyrodb_engine::parse_snapshot_data_index_bytes(snap);
});
