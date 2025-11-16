# KyroDB — The Fastest Vector Database for RAG

[![License: BSL 1.1](https://img.shields.io/badge/License-BSL%201.1-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/Rust-1.70%2B-orange)](https://www.rust-lang.org/)

**Status**: Phase 0 Weeks 17-20 in progress. Hybrid Semantic Cache architecture under active optimization for long-term 70%+ hit rate targets.

KyroDB is a vector database optimized for RAG workloads, featuring a **Hybrid Semantic Cache** that combines learned frequency prediction (RMI) with semantic similarity scoring. The system is in active development with a focus on long-term architecture and performance.

> **Development Status**: Research and development project. Core architecture implemented, performance optimization in progress.

## Mission

Build the highest read-speed vector database on the planet, optimized for RAG workloads with learned access pattern prediction.

## What Makes KyroDB Different

### Hybrid Semantic Cache (HSC)

KyroDB implements a learned cache admission policy combining two signals:
- **RMI frequency prediction**: Learned index predicts document-level hotness from access patterns
- **Semantic similarity scoring**: Optional query-level embedding cosine similarity (currently disabled in testing)
- **Admission policy**: Documents with predicted hotness above learned threshold are cached

**Current Focus**:
- Maintain a clear improvement over naive LRU baselines while we iterate on cache design
- Use Hybrid Semantic Cache as a long-term mechanism to target 70%+ hit rates on realistic RAG workloads
- Keep the cache layer architecture flexible enough for future tuning and algorithmic upgrades

**Technical Direction**:
- RMI-based predictors for document-level hotness
- Optional semantic adapters for query-level similarity
- Periodic retraining and adaptive thresholding based on observed access patterns

### Three-Tier Architecture

**Implementation Status**: Core architecture complete, undergoing iterative performance tuning.

- **Layer 1 (Cache)**: Learned predictor identifies hot documents with a long-term goal of 70%+ hit rates on RAG workloads
- **Layer 2 (Hot Tier)**: Recent writes buffer (HashMap), handles 1000-doc working set
- **Layer 3 (Cold Tier)**: HNSW vector index with WAL and snapshot persistence, <1ms P99 search

**Key Challenge**: Achieving stable 60%+ cache hit rate requires precise threshold calibration to avoid:
- **Score clustering**: All tracked documents scoring similarly → threshold too permissive
- **Over-tracking**: Tracking 9,000+ documents when only top 100 are truly hot
- **Threshold drift**: Training cycles resetting tuned parameters

## Current Performance

KyroDB’s performance work is ongoing and focused on:
- Keeping HNSW recall and latency within strict SLOs
- Using the cache layer to significantly outperform simple LRU baselines
- Iteratively tuning and evolving the Hybrid Semantic Cache to approach 70%+ hit rates on production-like RAG workloads

## Development Roadmap

### Phase 0 Weeks 1-16 (Complete)
- HNSW vector search wrapper with >95% recall validation
- **Hybrid Semantic Cache** combining RMI frequency prediction with semantic similarity
- Access pattern logging with ring buffer (32 bytes/event, 17.6ns overhead)
- A/B testing framework validating 2.18x hit rate improvement
- Three-tier architecture (Cache → Hot Tier → HNSW) fully integrated
- WAL and snapshot persistence with crash recovery
- Production validation harness with MS MARCO dataset (71,878 queries, 10K corpus)
- NDCG@10 quality metrics for cache admission validation
- Memory profiling with jemalloc integration

**Key Direction**: The Hybrid Semantic Cache is the primary lever for long-term read performance, with a clear goal of achieving 70%+ hit rates on realistic RAG workloads through tuning, algorithmic improvements, and architectural refinements.

### Phase 0 Weeks 17-20 (Current Focus)
- Performance tuning of the cache layer and three-tier architecture
- Validation and load testing under realistic RAG scenarios
- Instrumentation and observability for long-term cache behavior
- Preparing the system for future phases targeting sustained 70%+ cache hit rates

## Configuration

KyroDB supports **flexible configuration** through multiple sources (priority: CLI args > env vars > config file > defaults):

```bash
# 1. Generate example config
./kyrodb_server --generate-config yaml > config.yaml

# 2. Edit config.yaml to customize:
#    - Cache capacity (1K-100K+ documents)
#    - HNSW parameters (M, ef_construction, ef_search)
#    - SLO thresholds (P99 latency, cache hit rate)
#    - Persistence settings (fsync policy, WAL flush interval)

# 3. Start server with config
./kyrodb_server --config config.yaml

# 4. Override specific settings via environment variables
KYRODB__CACHE__CAPACITY=50000 \
KYRODB__SERVER__PORT=50051 \
./kyrodb_server --config config.yaml

# 5. CLI overrides (highest priority)
./kyrodb_server --config config.yaml --port 50051 --data-dir /mnt/ssd/data
```

**Key Configuration Sections**:
- **Server**: Host, port, connections, timeouts
- **Cache**: Capacity, strategy (LRU/Learned/A-B test), training interval
- **HNSW**: Vector dimensions, M parameter, ef_construction, distance metric
- **Persistence**: Data directory, WAL settings, fsync policy, snapshots
- **SLO**: P99 latency, cache hit rate, error rate, availability thresholds
- **Rate Limiting**: QPS limits per connection and globally
- **Logging**: Level, format (text/JSON), file rotation

See [Configuration Management Guide](docs/CONFIGURATION_MANAGEMENT.md) for complete documentation.

## Quick Start

Validation binary demonstrates end-to-end three-tier architecture:

```bash
# Build the validation binary
cargo build --release --bin validation_enterprise

# Run validation with three-tier query flow
./target/release/validation_enterprise

# Demonstrates:
# - A/B testing between LRU baseline and Hybrid Semantic Cache
# - Three-tier architecture with automatic cache admission
# - RMI-based frequency prediction and adaptive thresholding
# - Memory stability under sustained load
# - Automatic retraining cycles
# - Cache quality metrics (NDCG@10)
```

**Implemented Features**:
- Three-tier query flow (Cache → Hot Tier → HNSW)
- Hybrid Semantic Cache with RMI frequency prediction and semantic similarity
- A/B testing framework (LRU baseline vs Hybrid strategy)
- WAL and snapshot persistence with crash recovery
- Access pattern logging (32 bytes/event, 17.6ns overhead)
- Automatic RMI retraining every 10 minutes
- NDCG@10 quality metrics for cache admission validation
- MS MARCO dataset integration (71,878 queries, 10K corpus)

**Prerequisites**:
- Rust 1.70+
- 4GB+ RAM
- Linux/macOS (Windows untested)

## Architecture

### Three-Tier Query Flow

```
┌─────────────────────────────────────────────────┐
│        Query (doc_id + optional embedding)      │
└────────────────────┬────────────────────────────┘
                     │
                     ▼
        ┌────────────────────────────┐
        │  A/B Test Splitter (50/50) │
        └────────┬─────────────┬─────┘
                 │             │
        ┌────────▼───────┐   ┌▼──────────────────────┐
        │ LRU Baseline   │   │ Hybrid Semantic Cache │
        │ Strategy       │   │ Strategy              │
        └────────┬───────┘   └┬──────────────────────┘
                 │             │
                 │  ┌──────────▼──────────┐
                 │  │ RMI Frequency Model │
                 │  │ (doc-level hotness) │
                 │  └──────────┬──────────┘
                 │             │
                 │  ┌──────────▼──────────┐
                 │  │ Semantic Adapter    │
                 │  │ (query similarity)  │
                 │  └──────────┬──────────┘
                 │             │
                 └─────────────▼─────────────────┐
                           │                      │
                      ┌────▼─────────┐   ┌───────▼──────┐
                      │ Layer 1 Hit  │   │ Layer 1 Miss │
                      │ Return fast  │   └───────┬──────┘
                      └──────────────┘           │
                                                 ▼
                                        ┌─────────────────┐
                                        │ Layer 2: Hot    │
                                        │ Tier (HashMap)  │
                                        └────────┬────────┘
                                                 │
                                            ┌────▼────┐
                                            │ Hit/Miss│
                                            └────┬────┘
                                                 │
                                                 ▼
                                        ┌─────────────────┐
                                        │ Layer 3: HNSW   │
                                        │ Index + WAL     │
                                        └────────┬────────┘
                                                 │
                                        ┌────────▼────────┐
                                        │ Access Logger   │
                                        │ (training data) │
                                        └─────────────────┘
```

**Core Components**:
- `engine/src/tiered_engine.rs` - Three-tier orchestrator
- `engine/src/hnsw_backend.rs` - HNSW with WAL and snapshot persistence
- `engine/src/hot_tier.rs` - Recent writes buffer (Layer 2)
- `engine/src/cache_strategy.rs` - A/B testing framework
- `engine/src/learned_cache.rs` - RMI frequency prediction (Layer 1)
- `engine/src/semantic_adapter.rs` - Query-level semantic similarity
- `engine/src/access_logger.rs` - Access pattern tracking (ring buffer)
- `engine/src/training_task.rs` - Background RMI retraining
- `engine/src/ndcg.rs` - Cache admission quality metrics

## Why KyroDB?

### The RAG Performance Problem

Most vector databases treat all documents equally. RAG workloads exhibit distinct access patterns:

**Characteristics**:
- Zipfian distribution: 80% of queries access 20% of documents
- Read-heavy: 1000:1 read-to-write ratio in production systems
- Latency-sensitive: User-facing applications require consistent sub-10ms response times
- Semantic clustering: Related queries access semantically similar documents

**Existing Solutions**:
- Standard vector databases use LRU caching (20-30% hit rate, no semantic awareness)
- Semantic caches (GPTCache) operate at query level with high false positive rates (15-30%)
- No solutions combine document-level hotness prediction with semantic similarity

### KyroDB's Approach

**Hybrid Semantic Cache** combines two signals:
1. **Document-level frequency**: RMI predicts which documents will be hot (based on historical access patterns)
2. **Query-level semantics**: Semantic adapter scores documents by embedding similarity to recent queries

**Approach**: The cache layer demonstrates significant improvements over naive LRU baselines by learning access patterns and adapting to workload characteristics. Ongoing tuning targets 70%+ hit rates on production RAG workloads.

### Performance Characteristics

The Hybrid Semantic Cache is designed to:
- **Outperform LRU**: Learn from access patterns rather than simple recency
- **Cache diversity**: Track semantically diverse documents rather than just frequent repeats
- **Quality preservation**: Maintain ranking quality (NDCG@10) while improving hit rates
- **Memory efficiency**: Stable memory usage under sustained load
- **Adaptive learning**: Automatic retraining to track workload shifts

**Target Use Cases**:
- Customer support RAG systems (FAQ retrieval)
- Code completion and search (semantic code similarity)
- Enterprise knowledge bases (document retrieval with access patterns)
- Real-time recommendation engines (personalization)

## Technology Stack

**Core Language**: Rust (performance, memory safety, zero-cost abstractions)

**Key Dependencies**:
- `hnswlib-rs` - HNSW k-NN search implementation
- `tokio` - Async runtime for background training tasks
- `serde` - Serialization for WAL and snapshots
- `jemallocator` - Memory profiling and leak detection

**Testing Tools**:
- Criterion - Performance benchmarking
- Proptest - Property-based testing (planned)
- Loom - Concurrency testing (planned)
- cargo-fuzz - Fuzzing (planned)

## Documentation

### Getting Started
- [`docs/QUICKSTART.md`](docs/QUICKSTART.md) - Quick start guide for running KyroDB
- [`docs/README.md`](docs/README.md) - Documentation overview and navigation

### Architecture & Design
- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) - System architecture and data flow
- [`docs/THREE_TIER_IMPLEMENTATION.md`](docs/THREE_TIER_IMPLEMENTATION.md) - Three-tier architecture details (Cache → Hot Tier → HNSW)

### Operations & Management
- [`docs/OPERATIONS.md`](docs/OPERATIONS.md) - Operational procedures and troubleshooting
- [`docs/OBSERVABILITY.md`](docs/OBSERVABILITY.md) - Monitoring, metrics, and alerting
- [`docs/CONFIGURATION_MANAGEMENT.md`](docs/CONFIGURATION_MANAGEMENT.md) - Configuration options and deployment settings
- [`docs/AUTHENTICATION.md`](docs/AUTHENTICATION.md) - API key authentication and multi-tenancy

### API & Development
- [`docs/API_REFERENCE.md`](docs/API_REFERENCE.md) - Complete API documentation and examples

### Backup & Recovery
- [`docs/BACKUP_AND_RECOVERY.md`](docs/BACKUP_AND_RECOVERY.md) - Backup and restore procedures
- [`docs/CLI_BACKUP_REFERENCE.md`](docs/CLI_BACKUP_REFERENCE.md) - Command-line backup tool reference

### Quality & Metrics
- [`docs/NDCG_IMPLEMENTATION.md`](docs/NDCG_IMPLEMENTATION.md) - Cache quality metrics and NDCG@10 guide

### Advanced Topics
- [`docs/CONCURRENCY.md`](docs/CONCURRENCY.md) - Lock ordering, atomicity guarantees, and thread safety


## Development
```bash
## Development

```bash
# Clone repository
git clone https://github.com/vatskishan03/KyroDB.git
cd KyroDB

# Build release binary
cargo build --release --features cli-tools

# Run tests
cargo test

# Run validation (6-minute test with MS MARCO dataset)
cargo run --release --bin validation_enterprise

# Format code
cargo fmt

# Lint
cargo clippy
```

## Project Status

**Phase**: Phase 0 Weeks 17-20 (Production hardening and optimization)

**Completed** (Phase 0 Weeks 1-16):
- HNSW vector search with high recall validation
- Hybrid Semantic Cache (RMI frequency + semantic similarity)
- Three-tier architecture (Cache → Hot Tier → HNSW)
- WAL and snapshot persistence
- A/B testing framework demonstrating improvements over LRU baseline
- Access pattern logging and automatic RMI retraining
- NDCG@10 quality metrics
- Memory profiling with jemalloc

**Current Focus**:
- Extended validation under realistic RAG workloads
- Cache parameter tuning for long-term 70%+ hit rate targets
- Performance optimization (hot path profiling, SIMD)
- Concurrent load testing and stress scenarios

**Next Milestones**:
- Chaos engineering and crash recovery testing
- Beta customer deployments
- Operational runbooks and monitoring
- Public MVP launch (Phase 1)

## License

This project is licensed under the **Business Source License 1.1 (BSL 1.1)**.

**Permitted Use**:
- Development, testing, and non-production use
- Production use with up to 1 node and 1TB of data
- Creating derivative works and modifications

**Prohibited Use**:
- Offering KyroDB as a commercial managed database service

**Change License**: On September 24, 2029, the license automatically converts to the **GNU Affero General Public License v3.0 (AGPLv3)**.

See the [LICENSE](LICENSE) file for full terms.
