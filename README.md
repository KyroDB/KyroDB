# KyroDB

**The High-Performance Vector Database for RAG Workloads**

[![License: BSL 1.1](https://img.shields.io/badge/License-BSL%201.1-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/Rust-1.70%2B-orange)](https://www.rust-lang.org/)
[![Status](https://img.shields.io/badge/Status-v0.1-green)]()

> **Mission**: Build the fastest read-optimized vector database on the planet, purpose-built for RAG workloads and AI agents through learned access pattern prediction. In the long term, converting this as the database which can support data infrastructure needs for AGI.

---

## Why KyroDB?

Traditional vector databases treat all documents equally. **RAG workloads don't.**

In production RAG systems:
- **80% of queries** access **20% of documents** (Zipfian distribution)
- **Read-to-write ratio**: 1000:1
- **Latency requirements**: Sub-10ms for user-facing applications
- **Query patterns**: Semantically clustered and highly repetitive

**The Problem**: Standard vector databases achieve only 20-30% cache hit rates with simple LRU caching, forcing expensive HNSW searches for 70-80% of queries.

**KyroDB's Solution**: A **Hybrid Semantic Cache** that learns your access patterns and understands query semantics, achieving **73.5% cache hit rate** on realistic workloads.

---

## Key Features

### Hybrid Semantic Cache (73.5% Hit Rate)

KyroDB's two-level Layer 1 cache combines complementary caching strategies:

| Layer | Strategy | Hit Rate | Purpose |
|-------|----------|----------|---------|
| **L1a** | Learned hotness prediction (freq+recency) | ~63% | Predicts hot documents from access patterns |
| **L1b** | Semantic search‑result cache | ~10% | Caches top‑k results for similar queries |
| **Combined** | Hybrid approach | **73.5%** | 2-3× better than LRU baseline (25-35%) |

**Validation harness** (workload-dependent; not a production guarantee):
- ~73.5% combined L1 hit rate (L1a ~63%, L1b ~10%)

See [Two-Level Cache Architecture](docs/TWO_LEVEL_CACHE_ARCHITECTURE.md) for methodology, numbers, and limitations.

### ⚡ Three-Tier Architecture

```
┌────────────────────────────────────────────────┐
│  Layer 1: Hybrid Semantic Cache (HSC)         │
│  • L1a: Learned hotness prediction             │
│  • L1b: Semantic search‑result cache           │
│  • ~73.5% combined hit rate (validated harness)│
└────────────────┬───────────────────────────────┘
                 │ miss (26.5%)
                 ▼
┌────────────────────────────────────────────────┐
│  Layer 2: Hot Tier (Recent Writes)            │
│  • In-memory tier for recent writes            │
└────────────────┬───────────────────────────────┘
                 │ miss
                 ▼
┌────────────────────────────────────────────────┐
│  Layer 3: HNSW Index (Cold Storage)           │
│  • Approximate k-NN search                      │
│  • WAL + snapshot persistence                  │
└────────────────────────────────────────────────┘
```

### 🎯 Production-Ready Features

- **Crash Recovery**: WAL (Write-Ahead Log) + snapshot persistence
- **A/B Testing**: Built-in framework for cache strategy experimentation
- **Observability**: Prometheus metrics, structured logging, health checks
- **Flexible Config**: TOML/YAML files + environment variables + CLI args
- **Quality Metrics**: NDCG@10 for cache admission validation
- **Auto-Learning**: Background predictor retraining (optional)

---

## Quick Start

### Installation

```bash
# Clone repository
git clone https://github.com/vatskishan03/KyroDB.git
cd KyroDB

# Build release binaries
cargo build --release -p kyrodb-engine

# Optional: build validation binaries and enable table/progress output in CLI tools
cargo build --release -p kyrodb-engine --features cli-tools
```

### Run Validation Demo

See the Hybrid Semantic Cache in action with the MS MARCO dataset:

```bash
# Run end-to-end validation
./target/release/validation_enterprise

# Or customize the workload
./target/release/validation_enterprise \
    --cache-capacity 80 \
    --working-set-multiplier 2.0 \
    --query-reuse-probability 0.75
```

**Demonstrates**:
- A/B testing (LRU baseline vs. Hybrid Semantic Cache)
- Three-tier query flow with automatic cache admission
- Learned hotness prediction and adaptive thresholding
- Memory stability under sustained load
- Automatic retraining cycles
- NDCG@10 quality metrics

### Start gRPC Server

```bash
# Generate config file
./target/release/kyrodb_server --generate-config yaml > config.yaml

# Edit config.yaml to customize settings

# Start server
./target/release/kyrodb_server --config config.yaml

# Server starts on:
# - gRPC: port 50051
# - HTTP observability: port 51051
```

---

## Configuration

KyroDB supports flexible configuration with priority: **CLI args** > **env vars** > **config file** > **defaults**

### Example Configuration

```yaml
# config.yaml
server:
  port: 50051
  max_connections: 10000

cache:
  capacity: 10000
  strategy: "learned"  # or "lru" or "abtest"
  training_interval_secs: 600

hnsw:
  max_elements: 100000
  dimension: 768
  m: 16
  ef_construction: 200
  ef_search: 50

persistence:
  data_dir: "/var/lib/kyrodb/data"
  fsync_policy: "data_only"
  wal_flush_interval_ms: 100
  snapshot_interval_mutations: 10000
  max_wal_size_bytes: 104857600

slo:
  p99_latency_ms: 10.0
  cache_hit_rate: 0.70
```

### Environment Variable Overrides

```bash
# Override cache capacity
KYRODB__CACHE__CAPACITY=50000 ./kyrodb_server --config config.yaml

# Override server port
KYRODB__SERVER__PORT=50051 ./kyrodb_server
```

**See**: [Configuration Management Guide](docs/CONFIGURATION_MANAGEMENT.md) for complete options.

---

## Performance

### Validation Harness Results (workload-dependent)

Cache validation (see [Two-Level Cache Architecture](docs/TWO_LEVEL_CACHE_ARCHITECTURE.md)):
- Combined L1 hit rate: **~73.5%** (L1a ~63.5%, L1b ~10.1%)
- Bimodal latency: cache hits in microseconds; cold-tier misses in milliseconds

ANN benchmarking:
- See `benchmarks/README.md` and `benchmarks/results/` for dataset-specific recall/QPS/latency.

### Latency Distribution (12-Hour Test)

| Percentile | Cache Hits (73.5%) | Cache Misses (26.5%) | Overall |
|------------|-------------------|---------------------|---------|
| **P50** | 4 μs | 4.3 ms | 12 μs |
| **P90** | 1 ms | 8.8 ms | 6.8 ms |
| **P99** | 9 ms | 9.9 ms | 9.7 ms |

**Why P50 is 12μs but P99 is 9.7ms?** The distribution is **bimodal**:
- 73.5% of queries hit cache (microsecond latency)
- 26.5% require HNSW search (millisecond latency)

---

## Use Cases

KyroDB is optimized for:

- **Customer Support RAG**: FAQ retrieval with predictable access patterns
- **Code Search**: Semantic code similarity with frequent queries
- **Enterprise Knowledge Bases**: Document retrieval with learned hotness
- **Real-time Recommendations**: Personalization with high read-to-write ratios

**Not Optimized For**:
- Write-heavy workloads (KyroDB is read-optimized)
- Uniform access patterns (no benefit from learned prediction)
- Cold traffic only (cache benefits from repeated access)

---

## Architecture

### Core Components

| Component | Purpose | Location |
|-----------|---------|----------|
| **TieredEngine** | Three-tier orchestrator | [`engine/src/tiered_engine.rs`](engine/src/tiered_engine.rs) |
| **HnswBackend** | HNSW + persistence | [`engine/src/hnsw_backend.rs`](engine/src/hnsw_backend.rs) |
| **LearnedCache** | Learned hotness prediction | [`engine/src/learned_cache.rs`](engine/src/learned_cache.rs) |
| **QueryHashCache** | Semantic search‑result cache (L1b) | [`engine/src/query_hash_cache.rs`](engine/src/query_hash_cache.rs) |
| **HotTier** | Recent writes buffer | [`engine/src/hot_tier.rs`](engine/src/hot_tier.rs) |
| **AccessLogger** | Pattern tracking | [`engine/src/access_logger.rs`](engine/src/access_logger.rs) |
| **TrainingTask** | Background retraining | [`engine/src/training_task.rs`](engine/src/training_task.rs) |

### Technology Stack

- **Language**: Rust 1.70+ (performance, safety, zero-cost abstractions)
- **Vector Search**: `hnsw_rs` (HNSW k-NN implementation)
- **Async Runtime**: `tokio` (background tasks, gRPC server)
- **Serialization**: `serde` (WAL and snapshots)
- **Memory Profiling**: `jemallocator` (leak detection)

---

## Documentation

### Getting Started
- [Quick Start Guide](docs/QUICKSTART.md) - Get running in 5 minutes
- [Documentation Index](docs/README.md) - Complete guide navigation

### Architecture & Design
- [System Architecture](docs/ARCHITECTURE.md) - Query flow and components
- [Two-Level Cache Architecture](docs/TWO_LEVEL_CACHE_ARCHITECTURE.md) - HSC deep dive

### Operations
- [Operations Guide](docs/OPERATIONS.md) - Troubleshooting and procedures
- [Observability](docs/OBSERVABILITY.md) - Metrics, monitoring, alerts
- [Configuration Management](docs/CONFIGURATION_MANAGEMENT.md) - All config options
- [Backup & Recovery](docs/BACKUP_AND_RECOVERY.md) - Disaster recovery procedures

### API & Development
- [API Reference](docs/API_REFERENCE.md) - Complete API documentation
- [Concurrency](docs/CONCURRENCY.md) - Thread safety and lock ordering

---

## Development

### Prerequisites
- Rust 1.70+
- 4GB+ RAM
- Linux or macOS (Windows untested)

### Build & Test

```bash
# Build all binaries
cargo build --release --features cli-tools

# Run all tests
cargo test --all

# Run lib tests only
cargo test --lib

# Format code
cargo fmt --all

# Lint (strict warnings)
cargo clippy --all-targets -- -D warnings
```

### Available Binaries

```bash
# Main server
./target/release/kyrodb_server

# Validation harness (MS MARCO dataset)
./target/release/validation_enterprise

# Backup tool
./target/release/kyrodb_backup

# 24-hour stress test
./target/release/validation_24h
```

---

## Roadmap

### Completed (v0.1)
- HNSW vector search with >95% recall
- Two-level Hybrid Semantic Cache (73.5% hit rate)
- Three-tier architecture (Cache → Hot Tier → HNSW)
- WAL + snapshot persistence with crash recovery
- A/B testing framework
- Access pattern logging and auto-retraining
- NDCG@10 quality metrics
- Memory profiling and leak detection
- Production config management

### Current Focus (v0.2)
- Extended validation under diverse RAG workloads
- Performance optimization (SIMD, hot path profiling)
- Concurrent load testing and stress scenarios
- Chaos engineering and fault injection

### Future (v0.3+)
- Distributed deployment (multi-node clustering)
- Hybrid queries (vector + metadata filters)
- Real-time replication
- Cloud-native deployment guides
- Beta customer deployments

---

## Contributing

We welcome contributions!

**Areas of Interest**:
- Performance optimization
- Cache strategy improvements
- Documentation enhancements
- Testing and validation

---

## License

**Business Source License 1.1 (BSL 1.1)**

**Permitted Use**:
-  Development, testing, and non-production use
-  Production use with up to 1 node and 1TB of data
-  Creating derivative works and modifications

**Prohibited Use**:
- Offering KyroDB as a commercial managed database service

**Change License**: On **September 24, 2029**, the license automatically converts to **GNU Affero General Public License v3.0 (AGPLv3)**.

See [LICENSE](LICENSE) for full terms.

---

**Built with ❤️ in Rust**
