# KyroDB — Durable KV store with a Production Recursive Model Index (RMI)

**Status:** Alpha (focused scope: KV + RMI)

**One-liner:** KyroDB is a durable, append-only key-value engine with a production-grade learned index (RMI) for ultra-fast point lookups and predictable tail latency.

---

## Reproducible Benchmarks

See `bench/README.bench.md` for the full workflow to reproduce 1M/10M/50M results, including how to:
- Build release binaries with the `learned-index` feature
- Run HTTP bench using raw/fast endpoints and freeze rebuilds for the read phase
- Run in-process microbenches (RMI vs B-Tree)
- Capture artifacts to `bench/results/<commit>/` via `bench/scripts/capture.sh`
- Adjust RMI tuning via env: `KYRODB_RMI_TARGET_LEAF`, `KYRODB_RMI_EPS_MULT`

---

## Why KyroDB (what we’re solving)

Many systems combine a log + key-value store + index and accept complexity, large memory overhead, or brittle tail latencies. KyroDB’s thesis:

> You can get a smaller, simpler, and faster single-binary KV engine by pairing an immutable WAL + snapshot durability model with a learned primary index (RMI) tuned for real workload distributions.

This repository is intentionally **narrow**: the primary goal is producing a **production-grade KV engine** (durability, compaction, recovery) + **RMI** implementation and reproducible benchmarks that demonstrate the advantages of learned indexes in real workloads.

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

Build and run engine (HTTP)

```bash
# from repo root
cargo run -p engine -- serve 127.0.0.1 3030
```

Build CLI (optional)

```bash
cd orchestrator
# build CLI binary in this folder
go build -o kyrodbctl .
```

Basic admin + KV demo (HTTP)

```bash
# health and offset
./kyrodbctl -e http://127.0.0.1:3030 health
./kyrodbctl -e http://127.0.0.1:3030 offset

# trigger a snapshot
./kyrodbctl -e http://127.0.0.1:3030 snapshot

# KV put via HTTP
curl -sX POST http://127.0.0.1:3030/put \
  -H 'content-type: application/json' \
  -d '{"key":123,"value":"hello"}'

# KV get via CLI (lookup by key)
./kyrodbctl -e http://127.0.0.1:3030 lookup 123
```

Notes:
- If you start the server with `--auth-token <TOKEN>`, pass `Authorization: Bearer <TOKEN>` headers in your HTTP requests; the CLI will add a flag for this soon.
- Release build: `cargo build -p engine --release` (binary at `target/release/engine`).

---

## Protocol decision (current)

- Data plane: HTTP/JSON for Put/Get (temporary while iterating).
- Control plane: HTTP/JSON endpoints for `/health`, `/metrics` (Prometheus), `/snapshot`, `/offset`, and `/rmi/build`.

gRPC for the data plane is planned (see Roadmap) but not implemented yet.

---

## Architecture (high level)

- **PersistentEventLog (WAL)** — append-only records with configurable fsync.
- **Snapshotter** — write new snapshot → atomic rename/swap → truncate WAL.
- **In-memory delta** — fast recent writes map checked before probing index.
- **RMI (learned index)** — builder and on-disk format under active development; read path will predict approximate position and do a bounded last-mile probe.
- **Compactor** — will build new snapshots with latest values and reclaim WAL space.

Simplified flow (current):

```
Client -> HTTP -> WAL.append -> mem-delta -> background snapshot
                 \-> Get -> mem-delta -> (future: RMI predict) -> last-mile probe -> disk
```

---

## What’s implemented (current status)

- WAL append + recovery ✅
- Snapshot + atomic swap ✅
- Cold start recovery (snapshot load + WAL replay) ✅
- In-memory delta and baseline read path ✅
- RMI scaffolding (admin build endpoint; on-disk loader WIP) ⚠️ in progress
- Compaction (keep-latest) ⚠️ planned
- Basic HTTP API + `kyrodbctl` for admin ✅

---

## Benchmarks (how we’ll present claims)

Bench scripts and results will live in `/bench`. The README will only present **reproducible benchmarks** with exact hardware and commit hashes. Primary comparisons: RMI vs a baseline B-Tree/rocks-like indexing on point-lookup workloads across uniform and Zipfian keys.

Example claim format (to be populated when results are published):

> On N keys (V-byte values) on machine X with SSD: RMI p99 read latency = X ms vs B-Tree p99 = Y ms (exact bench scripts and CSV in /bench/results).

Until the CSVs and run scripts are in the repo, treat any numbers as provisional.

---

## Roadmap (focus: KV + RMI)

**Short-term (now)**

- Harden WAL + snapshot atomicity and crash tests (fuzz/CI).
- Finalize RMI file layout (mmap-friendly) + versioning.
- Implement compaction + WAL truncation service.
- Publish reproducible benches for 10M and 50M keys.

**Mid-term**

- gRPC data-plane for Put/Get/Subscribe.
- Perf polish (prefetch, packed layouts), reduce probe tail latencies, autoswitch rebuild heuristics.
- Deliver Docker image + reproducible bench workflow.

**Long-term (after core is stable)**

- Consider range queries, optional supplemental B-Tree fallback, replication model, and selective vector features (as an experimental plugin).

---

## How to help / contribute

- Open issues with proposed changes and include tests (unit, property) when possible.
- When touching WAL/snapshot/RMI code paths, include regression/bench evidence.
- Share bench CSVs in `/bench/results/<your-name>` with machine specs for comparison.

---

## License

Apache-2.0

---

## Contact / Community

Open an issue for design discussions; label PRs that are experimental with `experimental`. For fast feedback ping @vatskishan03 on GitHub and Twitter(kishanvats03).
