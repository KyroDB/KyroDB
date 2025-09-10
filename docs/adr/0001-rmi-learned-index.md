# ADR-0001: Recursive-Model Index (RMI) for Primary-Key Lookup

Status: Accepted
Authors: Kishan

## Context
KyroDB targets sub‑millisecond point lookups and, in Phase 1, sub‑5ms hybrid queries. A learned index (RMI) reduces memory and improves cache locality over B‑Trees. The engine maintains WAL + snapshots and supports a `PrimaryIndex` enum with `BTree` today and `AdaptiveRmi` behind the `learned-index` feature.

## Decision
Adopt a two‑stage Recursive‑Model Index (RMI) for 1‑D monotonic keys (u64), with bounded probe windows and SIMD acceleration. Guard with `#[cfg(feature = "learned-index")]` and retain `BTreeIndex` as fallback.

### Model Structure
- Root (Stage 0): linear model → leaf id
- Leaves (Stage 1): per‑leaf linear model → predicted position; per‑leaf ε bound
- Probe: SIMD probe in [pos−ε, pos+ε]; bounded binary search fallback

### Integration
- `PrimaryIndex::{BTree, AdaptiveRmi}` in `engine/src/index.rs`
- Atomic index swap using `Arc`/`ArcSwap` to avoid read stalls
- Background rebuild triggered by thresholds or admin endpoint; metrics track hits/misses

### Storage & Atomicity
- On‑disk `index-rmi.bin` with header, router, leaf models, key/offset arrays, checksum
- Write to temp + fsync + atomic rename; manifest updated last
- On startup, validate checksum; if invalid, fall back to B‑Tree and schedule rebuild

### API & Feature Gates
- `/v1/*` remains the current API
- `/v2/*` hybrid endpoints will rely on RMI once Phase 1 lands
- Feature flags: `learned-index`, `bench-no-metrics`, SIMD features

### Failure & Recovery
- Crash‑safe swap; recovery loads snapshot, replays WAL, then loads RMI if valid
- If RMI missing/corrupt, operate with B‑Tree and rebuild in background

## Alternatives
- Pure B‑Tree: simpler, higher memory/latency
- PGM Index: strong bounds; consider later for range queries
- ALEX: adaptive online; higher complexity initially

## Consequences
- + Lower memory and faster reads on AI‑like access patterns
- + Clean atomic swap model with low read disruption
- − Requires careful ε management and training
- − More complex testing and recovery surface

## Rollout
1) Keep B‑Tree default; add RMI behind feature gate
2) Build offline/online trainer from snapshots
3) Integrate lookup path with bounded probe + SIMD
4) Add metrics and tests (proptest/chaos/loom)

## Metrics
- RMI hits/misses, probe length histogram, rebuild duration, index size

## Bench Policy
- Disable auth/rate‑limit; enable `bench-no-metrics`
- Warm path: snapshot → RMI build → `/v1/warmup`