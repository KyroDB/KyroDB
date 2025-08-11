# ADR-0001: Recursive-Model Index (RMI) for Primary-Key Lookup

Status: Proposed
Date: 2025-08-10
Authors: Kishan

## Context
We need a high-performance primary-key index to support point lookups and range scans. The vision targets a learned index (AI4DB) to reduce memory footprint and improve lookup speed. The current engine already maintains a WAL+snapshot log and a naive `BTreeIndex` for key→offset mapping.

## Decision
Adopt a two-stage Recursive-Model Index (RMI) for 1-D monotonic keys (u64). Start with read-optimized, offline-trained models, then add incremental maintenance.

### Model Structure
- Stage 0 (Root): small MLP or linear model mapping key → leaf-id.
- Stage 1 (Leaves): per-leaf linear models mapping key → predicted position (offset). Each leaf stores an error bound ε.
- Search: Use predicted position ± ε to binary search in the log’s in-memory vector or a per-leaf sorted key→offset structure.

### Training Pipeline
- Data: extract `(key, offset)` pairs from recovered events, sorted by key.
- Offline training (initial): Python notebook using scikit-learn or LightGBM; export weights.
- On-device format: serialize as compact arrays (f32 coefficients) with per-leaf ε. Persist to `data/index-rmi.bin`.
- Rebuild triggers: on snapshot, threshold of N appends, or explicit admin command.

### Integration Points
- New `trait LearnedIndex` with `predict(key) -> (leaf_id, pos)` and `bound(key) -> ε`.
- Adapter implements `Index` trait: `get(key)` performs refined search using (pos, ε).
- During recovery, if `index-rmi.bin` exists and schema_version matches, load it; else fall back to `BTreeIndex`.
- Background task: when thresholds are met, (re)train and atomically swap the index.

### Storage Format v1
- Header: magic bytes `KYRO_RMI\x01`, schema_version (u8), num_leaves (u32).
- Root model: coefficients (f32[]), bias.
- Leaves: for each leaf, coefficients (f32[]), bias, epsilon (u32), key-range min/max (u64).
- Footer: checksum (xxhash64).

### API Surface (Rust)
```rust
pub trait LearnedIndex {
    fn predict_leaf(&self, key: u64) -> usize;
    fn predict_pos(&self, leaf_id: usize, key: u64) -> i64;
    fn epsilon(&self, leaf_id: usize) -> u64;
}

pub struct RmiIndex { /* ... */ }
impl LearnedIndex for RmiIndex { /* ... */ }

pub enum PrimaryIndex {
    BTree(BTreeIndex),
    Rmi(RmiIndex),
}
```

### Update Handling (Phase 1 → Phase 2)
- Phase 1: Read-optimized. Appends captured in a small delta-structure (BT map). Lookup checks delta first, else consults RMI.
- Phase 2: Periodic retraining compacts delta. Consider ALEX-style adaptive segments for online updates.

### Failure & Recovery
- Atomic index swap: write to `index-rmi.tmp`, fsync, rename to `index-rmi.bin`.
- On startup: validate header + checksum; if invalid, discard and rebuild from log.
- Maintain compatibility via `SCHEMA_VERSION` gating.

## Alternatives Considered
- Pure B-Tree: simple and robust, but higher memory and slower cache behavior.
- PGM Index: good theoretical guarantees; may be adopted later for range scans.
- ALEX: adaptive learned index with online maintenance; higher implementation complexity initially.

## Consequences
- + Lower memory for large key spaces.
- + Faster point lookups on skewed/read-heavy workloads.
- − Requires training pipeline and careful error bounds.
- − More complex recovery and validation logic.

## Rollout Plan
1. Implement `PrimaryIndex` enum with `BTreeIndex` default.
2. Add loader for optional RMI file (guarded by feature flag `learned-index`).
3. Build offline trainer, produce `index-rmi.bin` from snapshot.
4. Integrate lookup path; add microbenchmarks.
5. Add delta maintenance and retraining thresholds.

## Testing
- Unit tests for serialization/deserialization and bounds.
- Property tests: ensure key is found within predicted ±ε window.
- Chaos tests: crash during swap; ensure fallback to B-Tree works.

## Metrics
- Index memory size, training time, lookup p50/p99 latency, epsilon distribution, hit rate within 1 probe.

## Security & Compliance
- No PII stored in index files beyond keys; respect data governance when keys map to sensitive entities.