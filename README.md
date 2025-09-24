# KyroDB — High-Performance Key-Value Database with Learned Indexes

[![License: BSL 1.1](https://img.shields.io/badge/License-BSL%201.1-blue.svg)](https://mariadb.com/bsl11-faq/)
[![Rust](https://img.shields.io/badge/Rust-1.70%2B-orange)](https://www.rust-lang.org/)
[![CI](https://github.com/vatskishan03/KyroDB/actions/workflows/ci.yml/badge.svg)](https://github.com/vatskishan03/KyroDB/actions)

**Status: Phase 0 (Foundation)** — Production‑hardening of the single‑node engine with learned indexing.

KyroDB is a high-performance, durable key-value database engine featuring Recursive Model Index (RMI) for predictable sub-millisecond lookups. It is the foundation of an AI‑native database platform.

## 🚀 Core Features

- ⚡ **Learned Indexing (RMI)**: 2‑stage models with SIMD‑accelerated probing for O(1) expected lookup time (feature‑gated via `learned-index`)
- 🛡️ **ACID Durability**: WAL + snapshots with atomic operations and fast crash recovery  
- 🔒 **Concurrency Discipline**: Defensive locking; moving to lock‑free/atomic swaps on hot paths
- 📊 **Observability**: Prometheus metrics, health checks, structured logging (feature‑gated for benches)
- 🌐 **HTTP API**: Stable `/v1/*` endpoints; planned `/v2/*` hybrid queries (Phase 1)

## 📈 Phase 0 Targets

- P99 < 1ms for point lookups on 10M keys (warm path)
- Zero O(n) fallbacks; bounded epsilon search
- No deadlocks under mixed load (loom + chaos tests)
- Recovery ≤ 2s for 1GB WAL; bounded memory usage

---

## Quick Start

### Prerequisites
- Rust 1.70+ 
- 8GB+ RAM (for large datasets)

### Installation & Run
```bash
# Clone and build
git clone https://github.com/vatskishan03/KyroDB.git
cd KyroDB
cargo build -p kyrodb-engine --release

# Start server
./target/release/kyrodb-engine serve 127.0.0.1 3030
```

### Basic Operations (v1)
```bash
# Put key-value
curl -X POST http://localhost:3030/v1/put -H "Content-Type: application/json" \
  -d '{"key": 123, "value": "hello world"}'

# Get value by key  
curl "http://localhost:3030/v1/lookup?key=123"

# High-performance binary API
curl -X POST http://localhost:3030/v1/put_fast/456 --data-binary "binary data"
curl http://localhost:3030/v1/get_fast/456
```

---

## Architecture Overview

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   HTTP Server   │    │  RMI Learned     │    │   WAL +         │
│   (Warp/Tokio)  │◄──►│  Index (SIMD)    │◄──►│   Snapshots     │
│                 │    │                  │    │                 │
│ • REST API      │    │ • 2-stage models │    │ • Durable       │
│ • Metrics       │    │ • Bounded probe  │    │ • Atomic ops    │
│ • Rate limiting │    │ • AVX2/AVX512    │    │ • Fast recovery │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

Key technologies: RMI (Recursive Model Index), memory‑mapped I/O, SIMD acceleration, atomic index swaps.

---

## API Reference

### Core Operations (v1)
```http
POST /v1/put {"key": u64, "value": string}
GET  /v1/lookup?key=123
POST /v1/put_fast/{key}
GET  /v1/get_fast/{key}

POST /v1/snapshot
POST /v1/compact
POST /v1/rmi/build
POST /v1/warmup

GET  /health
GET  /metrics
GET  /build_info
```

Planned (v2, Phase 1): `/v2/search/hybrid`, `/v2/documents/stream`.

---

## Operations

### Production Deployment
```bash
./kyrodb-engine serve 0.0.0.0 3030 \
  --auto-snapshot-secs 3600 \
  --wal-max-bytes 1073741824 \
  --rmi-rebuild-appends 100000 \
  --enable-rate-limiting

# Env tuning
RUST_LOG=info \
KYRODB_WARM_ON_START=1 \
KYRODB_RL_RPS=50000 \
./kyrodb-engine serve 0.0.0.0 3030
```

### Monitoring & Metrics
```bash
curl http://localhost:3030/health
curl http://localhost:3030/metrics | grep kyrodb_
# kyrodb_appends_total, kyrodb_rmi_reads_total, kyrodb_rmi_probe_length_histogram, ...
```

---

## Benchmarks & Feature Gates

- Disable auth/rate limits for perf runs; enable `bench-no-metrics` to avoid metrics overhead
- Warm path before measuring: snapshot → RMI build → `/v1/warmup`
- Record p50/p99, probe length, rebuild time, cache hit rate

Features: `learned-index`, `bench-no-metrics`, `failpoints`, SIMD feature flags (`simd-avx2`, `simd-avx512`).

---

## Positioning & Migration

KyroDB targets AI retrieval workloads and can replace multi‑DB stacks there. It complements OLTP/analytics systems like PostgreSQL.

Adoption path: start with an AI collection in KyroDB, dual‑write/batch import, validate, then cut over hot paths. Bridges for Kafka/Parquet planned.

---

## Development

```bash
cargo test -p kyrodb-engine
cd bench && cargo run --release
cargo +nightly fuzz run rmi_probe
```

---

## License

This project is licensed under the **Business Source License 1.1 (BSL 1.1)**.

- **Permitted Use**: You can use KyroDB for development, testing, and for production use in deployments with up to 3 nodes and 1TB of data.
- **Prohibited Use**: Offering KyroDB as a commercial managed database service is not allowed.
- **Change License**: On **September 24, 2029**, the license will automatically convert to the **GNU Affero General Public License v3.0 (AGPLv3)**.

See the [LICENSE](LICENSE) file for the full terms.

**KyroDB: Foundation-first AI‑native database** 🚀
