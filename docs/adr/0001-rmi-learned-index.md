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
- v4: As v3 plus 8-byte alignment padding before `keys`; loaded via mmap with zero-copy when aligned. Checksum is xxh3_64 over everything before the footer.
- Leaves store: key_min, key_max, slope, intercept, epsilon, start, len.

Lookup path:
1) Check delta map (recent writes) first.
2) Find leaf by key range (binary search over leaf metadata).
3) Predict index position: `pos = round(slope * key + intercept)`.
4) Bounded binary search within `[pos-ε, pos+ε]`, clamped to leaf range.

Rebuild & swap:
- Triggered by appends threshold or delta/total ratio; or manual `POST /rmi/build`.
- Write to `index-rmi.tmp`, fsync, rename to `index-rmi.bin`, then load and atomically swap.

Safety & Recovery:
- On startup, validate magic, version, and checksum (v3+). If invalid, ignore and fall back to B-Tree.
- v4 loader avoids unaligned pointers by padding and copies as fallback if necessary.

Metrics:
- Hits/misses and hit rate, lookup latency, epsilon histogram and max, leaves count, index size, rebuild count and duration.

## Alternatives
- PGM/ALEX as future options for ranges or online updates; out of scope for now.

## Consequences
- Smaller memory footprint vs classical trees.
- Tunable ε and leaf sizing trade off write cost and read tail.

## Rollout
- v4 is default for new builds; loader supports v2/v3 for backwards compatibility.