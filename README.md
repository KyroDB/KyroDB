# KyroDB — Durable KV store with a Production Recursive Model Index (RMI)

**Status:** Alpha (focused scope: KV + RMI)

**One-liner:** KyroDB is a durable, append-only key-value engine with a production-grade learned index (RMI) for ultra-fast point lookups and predictable tail latency.

---

## Why KyroDB (what we’re solving)

Many systems combine a log + key-value store + index and accept complexity, large memory overhead, or brittle tail latencies. KyroDB’s thesis:

> You can get a smaller, simpler, and faster single-binary KV engine by pairing an immutable WAL + snapshot durability model with a learned primary index (RMI) tuned for real workload distributions.

This repository is intentionally **narrow**: the primary goal is producing a **production-grade KV engine** (durability, compaction, recovery) + **RMI** implementation and reproducible benchmarks that demonstrate the advantages of learned indexes in real workloads.

---

## Performance Optimizations

KyroDB includes several optimizations for sub-100µs lookup performance:

**Engine-level optimizations:**
- Fast in-memory offset→Record cache for O(1) record retrieval
- WAL block cache reduces disk I/O for recent segments  
- Synchronous lookup methods bypass async overhead for benchmarks
- Memory-mapped snapshots for zero-copy reads

**HTTP optimizations:**
- `/lookup_fast/{key}` endpoint returns binary offset (8 bytes) 
- `/lookup_raw?key=N` returns 204/404 without JSON overhead
- Optional auth bypass for benchmarking endpoints
- Pre-parsed path parameters avoid query string parsing

**Benchmark targets:**
- Sub-100µs engine-level lookups (Criterion benchmarks)
- Sub-1ms HTTP lookups for local traffic
- Apples-to-apples RMI vs B-Tree comparisons

---

## Scope (what this repo contains now)

**IN SCOPE**

- Durable append-only Write-Ahead Log (WAL) with configurable fsync policy.
- Snapshotter with atomic swap for fast crash recovery.
- In-memory recent-write delta and a single-node read path.
- RMI scaffolding and an admin build endpoint; learned-index read path under active development.
- WAL size management via snapshots and truncation hooks.
- HTTP data plane for Put/Get and HTTP control plane (health, metrics, admin).
- `kyrodbctl` client for basic admin and dev workflows.

**OUT OF SCOPE (for now)**

- Vector search, ANN (HNSW), and vector-related APIs.
- Full SQL engine, complex query planner, joins, or multi-model features.
- Distributed clustering, replication, sharding.
- Production packaging beyond a single binary + Dockerfile (to be added once core is stable).

---

## Quickstart (developer)

Prereqs: Rust toolchain, (optional) Go for `kyrodbctl`.

Run engine (HTTP)

```bash
cargo run -p engine -- serve 127.0.0.1 3030 \
  --wal-segment-bytes 67108864 \
  --wal-max-segments 8 \
  --rmi-rebuild-appends 100000 \
  --rmi-rebuild-ratio 0.25 \
  --auth-token secret123
```

Build CLI

```bash
cd orchestrator && go build -o kyrodbctl .
```

Admin ops

```bash
# health / offset
./kyrodbctl -e http://127.0.0.1:3030 --auth-token secret123 health
./kyrodbctl -e http://127.0.0.1:3030 --auth-token secret123 offset

# trigger snapshot
./kyrodbctl -e http://127.0.0.1:3030 --auth-token secret123 snapshot

# run compaction (keep-latest) and snapshot; prints stats
./kyrodbctl -e http://127.0.0.1:3030 --auth-token secret123 compact

# build RMI (feature learned-index enabled); also auto-rebuilds by thresholds
./kyrodbctl -e http://127.0.0.1:3030 --auth-token secret123 rmi-build

# metrics
curl -H 'Authorization: Bearer secret123' http://127.0.0.1:3030/metrics | grep kyrodb_
```

KV demo

```bash
curl -sX POST http://127.0.0.1:3030/put \
  -H 'content-type: application/json' \
  -H 'Authorization: Bearer secret123' \
  -d '{"key":123,"value":"hello"}'

./kyrodbctl -e http://127.0.0.1:3030 --auth-token secret123 lookup 123
```

---

## Compaction and WAL rotation

- Endpoint: `POST /compact` — performs keep-latest compaction, writes a fresh snapshot, resets WAL segments, and returns stats:
  - before_bytes, after_bytes, segments_removed, segments_active, keys_retained
- Auto triggers:
  - `--snapshot-every-n-appends N` compacts when N new events since last snapshot.
  - `--wal-max-bytes SIZE` compacts when total WAL bytes exceed SIZE.
- WAL rotation & retention:
  - `--wal-segment-bytes SIZE` rotates to `wal.000N` when current segment exceeds SIZE.
  - `--wal-max-segments K` retains at most K segments (oldest removed after rotation).

Metrics:
- `kyrodb_compaction_duration_seconds`, `kyrodb_compactions_total`
- `kyrodb_compaction_bytes_processed`, `kyrodb_compaction_bytes_saved`
- `kyrodb_wal_size_bytes`, `kyrodb_snapshot_size_bytes`

---

## RMI: format, bounded probe, and rebuild

- File: `data/index-rmi.bin` with header magic `KYRO_RMI` and versions:
  - v2: legacy, no checksum.
  - v3: adds xxh3_64 checksum.
  - v4: mmap-friendly, 8-byte alignment padding, xxh3_64 checksum; zero-copy read when aligned.
- Lookup path:
  - Check in-memory delta first (recent writes).
  - Predict leaf and position; bounded binary search within `[pred-ε, pred+ε]` window.
  - On hit/miss, metrics update: `kyrodb_rmi_hits_total`, `kyrodb_rmi_misses_total`, and `kyrodb_rmi_hit_rate`.
- Rebuild options:
  - Background thresholds: `--rmi-rebuild-appends N`, `--rmi-rebuild-ratio R` (delta/total).
  - Manual: `POST /rmi/build` or `./kyrodbctl rmi-build`.
  - Atomic swap: write `index-rmi.tmp` → fsync → rename to `index-rmi.bin`.

Observability:
- `kyrodb_rmi_index_size_bytes`, `kyrodb_index_size_bytes` (mirror), `kyrodb_rmi_index_leaves`
- `kyrodb_rmi_epsilon` histogram and `kyrodb_rmi_epsilon_max`
- `kyrodb_rmi_rebuilds_total`, `kyrodb_rmi_rebuild_duration_seconds`

### Manifest semantics (commit point)

- The manifest (`data/manifest.json`) is the external commit point for the index and data files.
- On startup, the engine consults the manifest first to select and validate the RMI: header version and trailing checksum are checked before loading.
- RMI rebuilds write `index-rmi.tmp`, fsync the file, atomically rename to `index-rmi.bin`, fsync the directory, then update the manifest (with header version and checksum) and fsync the directory again.

---

## Security

- Optional bearer auth: start engine with `--auth-token TOKEN`. The CLI adds the header when `--auth-token` is provided.

---

## ADRs

- `docs/adr/0001-rmi-learned-index.md` updated with current bounded probe and file headers (v2/v3/v4).

---

## Bench (scripted)

Build bench runner

```bash
cargo build -p bench --release
```

Start engine (release, no background rebuilds for fair comparison)

```bash
RUST_LOG=info cargo run -p engine --release --features "learned-index" -- serve 127.0.0.1 3030 \
  --wal-segment-bytes 67108864 --wal-max-segments 8 \
  --rmi-rebuild-appends 0 --rmi-rebuild-ratio 0.0
```

Run workload (uniform or zipf)

```bash
# uniform, 1M keys, 64B values, 64 readers for 60s
./target/release/bench \
  --base http://127.0.0.1:3030 \
  --load-n 1000000 \
  --val-bytes 64 \
  --load-concurrency 64 \
  --read-concurrency 128 \
  --read-seconds 60 \
  --dist uniform \
  --out-csv bench_uniform.csv

# zipf
./target/release/bench --load-n 1000000 --dist zipf --zipf-theta 1.1 --out-csv bench_zipf.csv
```

Outputs
- CSV with latency percentiles (us) for 50/90/95/99/99.9.
- Logs p50/p95/p99, WAL and snapshot sizes, RMI hit rate, and avg RMI probe length scraped from metrics.

Fair-benchmark checklist
- Use raw route: the bench tool hits `/lookup_raw` (204/404 only) to avoid JSON overhead.
- Freeze index: set `--rmi-rebuild-appends 0 --rmi-rebuild-ratio 0.0`, or run `POST /rmi/build` after load, then measure.
- Warm up: load and perform a pass before measuring; keep server logs quiet and CPU steady.
- Compare apples-to-apples: same dataset, concurrency, duration, and machine.
- Monitor probe behavior: scrape `kyrodb_rmi_probe_len_{sum,count}` and keep average small by tuning leaf size (ε) via `--rmi-leaf-target`.

---
## In-Process Microbenchmarks (Criterion)

For raw, HTTP-free performance measurement, use the Criterion benchmarks.

```bash
# Run BTree-only
cargo bench -p bench --bench kv_index

# Run RMI too (enable bench feature forwarding to engine)
cargo bench -p bench --bench kv_index --features learned-index -- --verbose
```

### Benchmark methodology (reproducible)

- Build in release mode, and disable background rebuilds for fair comparisons:
  - Engine: `--rmi-rebuild-appends 0 --rmi-rebuild-ratio 0.0`
- Pin to a consistent environment (same machine, idle, performance governor if applicable).
- Warm up the engine (load and one pass over keys) before measuring.
- Use HTTP-free Criterion benches for raw index lookup latency.
- Record RMI and cache effectiveness via metrics:
  - `kyrodb_rmi_hit_rate`, `kyrodb_rmi_probe_len`, `kyrodb_cache_hits_total`, `kyrodb_cache_misses_total`.
