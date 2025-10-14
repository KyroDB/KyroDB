# KyroDB — The Fastest Vector Database for RAG

[![License: BSL 1.1](https://img.shields.io/badge/License-BSL%201.1-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/Rust-1.70%2B-orange)](https://www.rust-lang.org/)

**Status**: Phase 0 in progress (Weeks 1-16 complete). Three-tier architecture implemented with Hybrid Semantic Cache showing 2.18x hit rate improvement over LRU baseline.

KyroDB is a vector database optimized for RAG workloads, featuring a **Hybrid Semantic Cache** that combines RMI frequency prediction with semantic similarity scoring. The three-layer architecture (Cache → Hot Tier → HNSW) is complete and validated end-to-end.

> **Development Status**: Research and development project. Core architecture validated.

## Mission

Build the highest read-speed vector database on the planet, optimized for RAG workloads with learned access pattern prediction.

## What Makes KyroDB Different

### Hybrid Semantic Cache (HSC)

KyroDB implements a novel cache admission policy combining two signals:
- **RMI frequency prediction**: Document-level hotness prediction using Recursive Model Index
- **Semantic similarity scoring**: Query-level embedding cosine similarity
- **Hybrid admission**: Cache documents predicted hot AND semantically similar to recent queries

**Validated Results** (October 2025):
- LRU baseline: 20.7% hit rate on MS MARCO dataset
- Hybrid Semantic Cache: 45.1% hit rate
- Improvement: 2.18x over baseline
- Cache diversity: 5.6x more unique documents cached vs LRU
- Memory stability: 2% growth over 6-minute sustained load

### Three-Tier Architecture

Complete implementation of layered query routing:
- **Layer 1 (Cache)**: RMI-predicted hot documents with semantic filtering, 70-90% target hit rate
- **Layer 2 (Hot Tier)**: Recent writes buffer (HashMap), handles 1000-doc working set
- **Layer 3 (Cold Tier)**: HNSW vector index with WAL and snapshot persistence, <1ms P99 search

### Design Philosophy

- **Performance-first**: Every line of code on the query path is optimized
- **Zero allocations**: Arena allocators, pre-allocated buffers, SIMD everywhere
- **Lock-free reads**: Atomic index swaps, zero contention
- **Bounded operations**: No O(n) scans, no unpredictable latency
- **Deep implementation**: Production-grade from day one, not MVP quality

## Current Performance

| Component | Status | Result |
|-----------|--------|--------|
| **Hybrid Cache Hit Rate** | Validated | 45.1% (2.18x over 20.7% LRU baseline) |
| **Cache Diversity** | Validated | 79 unique docs (5.6x over 14 for LRU) |
| **HNSW Recall@10** | Validated | >95% on MS MARCO dataset |
| **Memory Efficiency** | Validated | 2% growth over 6-minute sustained load |
| **Training Stability** | Validated | 6 training cycles, no crashes |
| **Three-Tier Architecture** | Complete | All layers implemented and tested |
| **Persistence Layer** | Complete | WAL and snapshot recovery working |
| **NDCG@10 Quality** | Validated | 1.0 perfect ranking by access frequency |

**Next Milestones**: Scale validation to 1 hour, tune cache parameters for 60%+ hit rate, beta customer deployments

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

**Key Achievement**: Hybrid Semantic Cache achieves 45.1% hit rate (2.18x over 20.7% LRU baseline) while caching 5.6x more diverse documents.

### Phase 0 Weeks 17-20 (Current Focus)
- Scale validation from 6 minutes to 1 hour sustained load
- Tune cache parameters (threshold, capacity, training frequency) for 75%+ hit rate target
- Performance optimization: hot path profiling, SIMD vectorization
- Integration testing under concurrent load


## Quick Start

Validation binary demonstrates end-to-end three-tier architecture:

```bash
# Build the validation binary
cargo build --release --bin validation_enterprise

# Run 6-minute validation with three-tier query flow
./target/release/validation_enterprise

# Expected output:
# - LRU baseline: 20.7% hit rate, 14 unique docs cached
# - Hybrid Semantic Cache: 45.1% hit rate, 79 unique docs cached (5.6x diversity)
# - Improvement: 2.18x hit rate gain
# - Memory stability: 2% growth over sustained load
# - Training cycles: 6 successful, no crashes
# - NDCG@10: 1.0 (perfect ranking quality)
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

**Result**: 2.18x hit rate improvement over LRU baseline while caching 5.6x more diverse documents.

### Performance Characteristics

Based on validation with MS MARCO dataset (71,878 queries, 10K corpus):

| Metric | LRU Baseline | Hybrid Semantic Cache | Improvement |
|--------|--------------|----------------------|-------------|
| Hit rate | 20.7% | 45.1% | 2.18x |
| Unique docs cached | 14 | 79 | 5.6x |
| Cache diversity | Low (concentrated) | High (distributed) | Semantic clustering |
| NDCG@10 quality | 1.0 | 1.0 | Both rank correctly |
| Memory stability | Stable | Stable (2% growth) | Efficient |

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

- [`docs/THREE_TIER_IMPLEMENTATION.md`](docs/THREE_TIER_IMPLEMENTATION.md) - Architecture details
- [`docs/NDCG_IMPLEMENTATION.md`](docs/NDCG_IMPLEMENTATION.md) - Quality metrics

## Development
```bash
## Development

```bash
# Clone repository
git clone https://github.com/vatskishan03/KyroDB.git
cd KyroDB

# Build release binary
cargo build --release

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
- HNSW vector search with >95% recall validation
- Hybrid Semantic Cache (RMI frequency + semantic similarity)
- Three-tier architecture (Cache → Hot Tier → HNSW)
- WAL and snapshot persistence
- A/B testing framework showing 2.18x improvement
- Access pattern logging and automatic RMI retraining
- NDCG@10 quality metrics
- Memory profiling with jemalloc

**Current Focus**:
- Scale validation from 6 minutes to 1 hour
- Tune cache parameters for 60%+ hit rate target
- Performance optimization (hot path profiling, SIMD)
- Concurrent load testing

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
