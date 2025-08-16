# ADR-0001: Recursive-Model Index (RMI) for Primary-Key Lookup

Status: Accepted
Authors: Kishan

## Context
We need a high-performance primary-key index for point lookups. The engine uses a WAL + snapshot model; a learned RMI reduces memory and yields predictable tail latencies.

## Decision
Adopt a per-leaf linear RMI with bounded last-mile probe and an mmap-friendly on-disk format with versioning and checksum.

## Current Design (v2/v3/v4)
- Magic: `KYRO_RMI` (8 bytes), then 1-byte version, then 3-byte pad.
- v2: `[magic][ver=2][pad3][epsilon u32][count u64][keys u64[]][offs u64[]]` (no checksum).
- v3: `[magic][ver=3][pad3][num_leaves u32][count u64][leaves][keys][offs][checksum u64]`.
- v4: `[magic][ver=4][pad3][num_leaves u32][count u64][leaves][pad to 8B][keys][offs][checksum u64]`; loaded via mmap with zero-copy when 8-byte aligned. Checksum is xxh3_64 over everything before the footer.
- Leaves store: `key_min:u64`, `key_max:u64`, `slope:f32`, `intercept:f32`, `epsilon:u32`, `start:u64`, `len:u64`.

## Lookup path
1) Check delta map (recent writes) first.
2) Find leaf by key range (binary search over leaf metadata).
3) Predict index position: `pos = round(slope * key + intercept)`.
4) Bounded binary search within `[pos-ε, pos+ε]`, clamped to leaf range.
5) If not found, treat as miss. (Today the HTTP handler may optionally scan as a secondary fallback; there is no B-Tree fallback when the active index is RMI.)

## Rebuild & swap
- Triggers: appends threshold and/or delta/total ratio; manual `POST /rmi/build`.
- Flow (current): write to `index-rmi.tmp` → fsync → atomic rename to `index-rmi.bin` → load/validate (magic + checksum for v3/v4) → swap active index in memory → update manifest.
- Planned: epoch-named index files and making the manifest the external commit point for index epoch provenance.

## File naming and manifest (current)
- Files: `data/index-rmi.bin` (active), `data/index-rmi.tmp` (build temp).
- Manifest `manifest.json` includes: `schema`, crate `version`, `next_offset`, `snapshot`, `snapshot_bytes`, `wal_segments`, `wal_total_bytes`, `rmi` (path), `rmi_header_version`, `last_compaction_ts`, `ts`.
- Startup: manifest is strictly parsed (schema==1) to seed `next_offset` and `last_compaction_ts`; index file is validated separately on load. Invalid manifest or index is ignored (fallback to B-Tree on startup only).

## Safety & Recovery
- On startup, validate magic (must be `KYRO_RMI`), version, and checksum (v3/v4). If invalid, ignore the index and keep B-Tree.
- v4 loader pads to 8-byte alignment before keys; uses mmap when aligned, copies otherwise (no undefined behavior on unaligned data).

## Metrics (implemented)
- RMI lookups: `kyrodb_rmi_hits_total`, `kyrodb_rmi_misses_total`, `kyrodb_rmi_hit_rate`, `kyrodb_rmi_lookup_latency_seconds`.
- Model quality: `kyrodb_rmi_epsilon` histogram, `kyrodb_rmi_epsilon_max`, `kyrodb_rmi_index_leaves`.
- Index size: `kyrodb_rmi_index_size_bytes` and `kyrodb_index_size_bytes` (mirror).
- Rebuilds: `kyrodb_rmi_rebuilds_total`, `kyrodb_rmi_rebuild_duration_seconds`.
- Storage: `kyrodb_wal_size_bytes`, `kyrodb_snapshot_size_bytes`, compaction histograms `kyrodb_compaction_bytes_processed` and `kyrodb_compaction_bytes_saved`.
- Planned: probe-length histogram and mispredict counter.

## Alternatives
- PGM/ALEX as future options for ranges or online updates; out of scope for now.

## Consequences
- Smaller memory footprint vs classical trees.
- Tunable ε and leaf sizing trade off write cost and read tail.

## Rollout
- v4 is default for new builds; loader supports v2/v3 for backwards compatibility.