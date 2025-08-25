Testing and reliability

- Crash/recovery matrix
  - Covered by existing tests in `engine/tests/`:
    - WAL CRC tail corruption, rotation/retention, background compaction
    - Snapshot atomicity and mmap read
    - RMI build/rebuild thresholds and epsilon bounds
    - Subscribe/recovery
  - CI matrix runs tests across OSes and feature sets:
    - Default features (learned-index)
    - No default features
    - http-test feature

- Fuzzing (cargo-fuzz): WAL and snapshot parsers
  - Fuzz target: `engine/fuzz/fuzz_targets/fuzz_target_1.rs`
  - Parsers under test in `engine/src/lib.rs`:
    - `parse_wal_stream_bytes(&[u8])`
    - `parse_snapshot_data_index_bytes(&[u8])`
  - Local run examples:
    - `cd engine && cargo fuzz run fuzz_target_1` (requires `cargo install cargo-fuzz`)
  - CI only builds fuzzers; running long fuzz campaigns is left manual to keep CI fast.

- Concurrency testing (loom)
  - Consider using `loom` for small, critical concurrent sections (e.g., WAL rotate + manifest).
  - Not enabled in CI by default due to runtime cost; track progress in issues.
