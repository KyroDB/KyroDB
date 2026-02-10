# KyroDB

**Read-Optimized Vector Database for RAG and Agent Retrieval**

[![License: BSL 1.1](https://img.shields.io/badge/License-BSL%201.1-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/Rust-1.70%2B-orange)](https://www.rust-lang.org/)
[![Status](https://img.shields.io/badge/Status-v0.1-green)]()

KyroDB is a gRPC-first vector database focused on the hardest part of production retrieval systems: consistent low-latency reads under real query skew, with strict correctness and operational safety.

Mission:
Build the highest read-speed vector database on the planet for AI workloads

## Why This Architecture

Most retrieval workloads are not uniformly random:

- Query distributions are skewed (hot subsets dominate)
- Read-to-write ratios are often extreme
- Tail latency (P99) drives user experience and system cost

KyroDB is designed around this reality:

- Two-level Layer 1 cache (frequency/recency + semantic query-result reuse)
- Hot-tier + cold-tier separation
- HNSW ANN search backend
- WAL + snapshot durability
- Strict multi-tenant guards and observability

## Current Scope (Phase 0)

What is in scope now:

- Single-node deployment
- gRPC data plane (insert/query/search/bulk operations)
- Hybrid Semantic Cache + HNSW read path
- WAL/snapshot crash recovery
- Auth, tenancy, namespaces, metadata filtering
- Operational endpoints and runbooks

What is intentionally out of scope until Phase 1 gates are green:

- Distributed/cluster mode
- New `/v2/*` product APIs
- Feature expansion that is not benchmark-backed

For the source-of-truth status and gates, see [docs/ENGINEERING_STATUS.md](docs/ENGINEERING_STATUS.md).

## System Overview

```text
Client
  |
  | gRPC (data plane)
  v
KyroDB Server
  |- L1a: learned document cache
  |- L1b: semantic search-result cache
  |- L2 : hot tier (recent writes)
  |- L3 : HNSW cold tier
  |- WAL + snapshots (durability)
  |- Auth + tenant/namespace/filter enforcement
  |- Metrics/health/SLO/usage endpoints
```

Data plane protocol:

- Primary: gRPC (`KyroDBService`)
- HTTP is observability/admin-facing only (`/metrics`, `/health`, `/ready`, `/slo`, `/usage`)

## SOTA Engineering Posture

KyroDB follows explicit rules to keep performance and reliability defensible:

- Correctness before speed; invalid semantics void performance claims
- No unbounded work in steady-state read paths
- Evidence-first optimization (before/after benchmarks required)
- Hot-path discipline (allocation/lock minimization, bounded contention)
- Operational safety (durability, recovery drills, monitoring, runbooks)

This posture is enforced through benchmarks, tests, docs, and phase gates.

## Performance and Evidence

Current public numbers are workload-dependent and must be interpreted with caveats.

Cache validation harness (representative workload):

- Combined L1 hit rate around 73.5% (L1a + L1b)
- Bimodal latency profile: microseconds on cache hits, milliseconds on cold-tier misses


## Security, Tenancy, and Billing Readiness

KyroDB includes:

- API-key auth and tenant scoping
- Per-tenant QPS and vector quotas
- Reserved metadata protection for tenant/namespace isolation
- Pilot-mode startup guardrails
- Per-tenant usage accounting (queries/inserts/deletes/storage)
- `/usage` endpoint for billing/ops snapshots

See:

- [docs/AUTHENTICATION.md](docs/AUTHENTICATION.md)
- [docs/CONFIGURATION_MANAGEMENT.md](docs/CONFIGURATION_MANAGEMENT.md)
- [docs/API_REFERENCE.md](docs/API_REFERENCE.md)

## Quick Start

```bash
# Clone
git clone https://github.com/vatskishan03/KyroDB.git
cd KyroDB

# Build
cargo build --release -p kyrodb-engine

# Start server
./target/release/kyrodb_server

# Health + metrics
curl http://127.0.0.1:51051/health
curl http://127.0.0.1:51051/metrics
```

For full quickstart and API examples:

- [docs/QUICKSTART.md](docs/QUICKSTART.md)
- [docs/API_REFERENCE.md](docs/API_REFERENCE.md)

## Pilot Deployment Baseline

Use the secure baseline templates for external pilots:

- [config.pilot.toml](config.pilot.toml)
- [config.pilot.yaml](config.pilot.yaml)

Then execute with:

```bash
./target/release/kyrodb_server --config config.pilot.toml
```

Pilot go/no-go checklist:

- [docs/PILOT_LAUNCH_CHECKLIST.md](docs/PILOT_LAUNCH_CHECKLIST.md)

## Documentation Map

Core docs:

- [docs/ENGINEERING_STATUS.md](docs/ENGINEERING_STATUS.md) - implementation truth, gates, risks
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) - component and data flow
- [docs/TWO_LEVEL_CACHE_ARCHITECTURE.md](docs/TWO_LEVEL_CACHE_ARCHITECTURE.md) - Layer 1 cache design
- [docs/OPERATIONS.md](docs/OPERATIONS.md) - runbook and failure handling
- [docs/OBSERVABILITY.md](docs/OBSERVABILITY.md) - metrics, probes, alerts
- [docs/BACKUP_AND_RECOVERY.md](docs/BACKUP_AND_RECOVERY.md) - backup/restore policy
- [docs/CONFIGURATION_MANAGEMENT.md](docs/CONFIGURATION_MANAGEMENT.md) - full config contract
- [docs/PILOT_LAUNCH_CHECKLIST.md](docs/PILOT_LAUNCH_CHECKLIST.md) - pilot go/no-go gates
- [docs/BENCHMARK_EVIDENCE_PACKAGE.md](docs/BENCHMARK_EVIDENCE_PACKAGE.md) - benchmark claim discipline
- [docs/README.md](docs/README.md) - complete index

## Development

Prerequisites:

- Rust 1.70+
- Linux or macOS
- Sufficient RAM for your target dataset

Common commands:

```bash
# Build
cargo build -p kyrodb-engine
cargo build --release -p kyrodb-engine --features cli-tools

# Test
cargo test --all

# Format + lint
cargo fmt --all
cargo clippy --all-targets -- -D warnings
```

## License

Business Source License 1.1 (BSL 1.1).

See [LICENSE](LICENSE) for complete terms.
