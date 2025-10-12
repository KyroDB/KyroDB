# KyroDB — The Fastest Vector Database for RAG

[![License: BSL 1.1](https://img.shields.io/badge/License-BSL%201.1-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/Rust-1.70%2B-orange)](https://www.rust-lang.org/)
[![CI](https://github.com/vatskishan03/KyroDB/actions/workflows/ci.yml/badge.svg)](https://github.com/vatskishan03/KyroDB/actions)

**Status: Phase 0 Complete (Weeks 1-12)** • Hybrid semantic cache validated with 2.1x improvement over baseline.

KyroDB is a vector database optimized for RAG workloads, featuring a hybrid semantic cache that combines learned frequency prediction with semantic similarity. Current focus: integrating the cache layer with HNSW vector search and adding persistence.

> **⚠️ Development Status**: Core caching and validation infrastructure complete. Integration work in progress. Not yet production-ready.

## 🎯 Mission

**Build the highest read-speed vector database on the planet.**

No compromises. No competition. Just pure speed optimized for RAG workloads.

## 🚀 What Makes KyroDB Different

### Hybrid Semantic Cache (Complete)

**What's Built**:
- Learned frequency prediction via RMI (Recursive Model Index)
- Semantic similarity scoring via embedding comparisons
- Hybrid cache admission combining both signals
- A/B testing framework proving 2.1x hit rate improvement (44% vs 21% baseline)
- Memory-efficient access logging with automatic training cycles

**What's Next**:
- Integration with HNSW vector index for full query path
- Persistence layer (WAL + snapshots)
- Three-tier architecture (cache → hot tier → cold tier)

### Design Philosophy

- **Performance-first**: Every line of code on the query path is optimized
- **Zero allocations**: Arena allocators, pre-allocated buffers, SIMD everywhere
- **Lock-free reads**: Atomic index swaps, zero contention
- **Bounded operations**: No O(n) scans, no unpredictable latency
- **Deep implementation**: Production-grade from day one, not MVP quality

## 📊 Current Performance

| Component | Status | Result |
|-----------|--------|--------|
| **Hybrid Cache Hit Rate** | ✅ Validated | 44% (2.1x over 21% LRU baseline) |
| **HNSW Recall@10** | ✅ Validated | >95% on MS MARCO dataset |
| **Memory Efficiency** | ✅ Validated | 2% growth over 6-minute sustained load |
| **Training Stability** | ✅ Validated | 6 training cycles with no crashes |
| **Cache Integration** | ✅ Complete | HNSW backend created, validation updated |
| **Persistence Layer** | 📋 Planned | WAL + snapshots for durability |

**Next Target**: Scale validation to 1 hour, achieve 60%+ hit rate with tuning

---

## 🏗️ Development Roadmap

### ✅ Phase 0 Weeks 1-12: Foundation (Complete)
- HNSW vector search wrapper with >95% recall validation
- RMI-based learned cache with frequency prediction
- Semantic similarity adapter for hybrid cache decisions
- Access pattern logging with fixed-capacity ring buffer
- A/B testing framework (LRU vs Learned strategies)
- Production-grade validation harness with MS MARCO dataset
- Memory profiling and leak detection (jemalloc integration)

**Achievement**: 2.1x cache hit improvement validated (44% vs 21% baseline)

### ⏳ Current: Integration & Persistence
- Connect cache layer to HNSW index for full query path
- Add WAL and snapshot persistence
- Scale validation tests from 6 minutes to 1 hour
- Tune cache parameters to reach 60%+ hit rate target

### 📋 Phase 1: Production Deployment
- Beta customer deployments with monitoring
- Case studies documenting performance improvements
- Hybrid query API (vector + metadata filters)
- Public MVP launch

See [`Implementation.md`](Implementation.md) for the detailed week-by-week execution plan.

---

## 🚀 Quick Start

Current validation binaries demonstrate the cache layer:

```bash
# Build the validation binary
cargo build --release --bin validation_enterprise

# Run 6-minute validation with hybrid semantic cache
./target/release/validation_enterprise

# Output shows:
# - LRU baseline: ~21% hit rate
# - Learned cache: ~44% hit rate (2.1x improvement)
# - Memory stability: <2% growth
# - Training cycles: 6 successful runs
```

**Current Capabilities**:
- Hybrid semantic cache with frequency + similarity scoring
- A/B testing framework for strategy comparison
- Memory-efficient access logging (32 bytes/event)
- Automatic model retraining every 60 seconds
- Production-grade validation with real embeddings (MS MARCO)

**Prerequisites**:
- Rust 1.70+
- 4GB+ RAM
- Linux/macOS (Windows untested)

---

## 🏛️ Architecture

### Current Implementation

```
┌─────────────────────────────────────────────────┐
│           Query (embedding + doc_id)            │
└────────────────────┬────────────────────────────┘
                     │
                     ▼
        ┌────────────────────────────┐
        │  A/B Test Splitter (50/50) │
        └────────┬─────────────┬─────┘
                 │             │
        ┌────────▼───────┐   ┌▼────────────────────┐
        │ LRU Baseline   │   │ Hybrid Semantic     │
        │ (21% hit rate) │   │ Cache (44% hit)     │
        └────────┬───────┘   └┬────────────────────┘
                 │             │
                 │  ┌──────────▼──────────┐
                 │  │ RMI Frequency Model │
                 │  │ (learned hotness)   │
                 │  └──────────┬──────────┘
                 │             │
                 │  ┌──────────▼──────────┐
                 │  │ Semantic Adapter    │
                 │  │ (embedding cosine)  │
                 │  └──────────┬──────────┘
                 │             │
                 └─────────────▼─────────────────┐
                           │                      │
                      ┌────▼─────────┐   ┌───────▼──────┐
                      │ Cache Hit    │   │ Cache Miss   │
                      │ Return fast  │   │ Access logger│
                      └──────────────┘   └──────────────┘
```

**What's Built**:
- `engine/src/hnsw_index.rs` - HNSW wrapper with recall validation
- `engine/src/rmi_core.rs` - Recursive model index for frequency prediction
- `engine/src/learned_cache.rs` - Cache predictor with hotness scoring
- `engine/src/semantic_adapter.rs` - Embedding similarity computation
- `engine/src/access_logger.rs` - Ring buffer for access pattern tracking
- `engine/src/cache_strategy.rs` - A/B testing framework
- `engine/src/vector_cache.rs` - Unified cache storage
- `engine/src/training_task.rs` - Background model retraining

**What's Missing**:
- Integration: Cache layer currently isolated from HNSW index
- Persistence: No WAL or snapshots yet
- Tier separation: Hot/cold tier architecture not implemented

---

## 🎓 Why KyroDB?

### The RAG Performance Problem

Most vector databases were built for general-purpose similarity search. RAG workloads have unique characteristics:

1. **Highly skewed access patterns**: 80% of queries hit 20% of documents (Zipfian distribution)
2. **Read-heavy**: 1000:1 read-to-write ratio in production RAG systems
3. **Latency-sensitive**: Every millisecond counts in user-facing applications
4. **Metadata filtering**: Hybrid queries combining vector similarity + structured filters

Existing solutions force you to choose:
- **Pinecone/Weaviate**: Good features, but slow (5-50ms P99)
- **Qdrant**: Fast for small datasets, but doesn't scale with learned optimizations
- **FAISS/Annoy**: Library-only, no durability or production features

KyroDB is purpose-built for RAG: learned cache + HNSW + tiered storage = unbeatable read speed.

### Performance Advantage

Our learned cache predicts hot documents **before** you query them:

| Operation | KyroDB (target) | Pinecone | Qdrant |
|-----------|-----------------|----------|--------|
| Cache hit | **<100µs** | N/A | N/A |
| Cache miss | **<1ms** | 5-20ms | 2-10ms |
| Bulk insert | **>100k/sec** | 10-50k/sec | 20-80k/sec |

*Benchmarks are targets for Phase 0 completion. Actual performance TBD.*

### Use Cases

- **Real-time RAG**: Customer support bots, coding assistants, search engines
- **High-frequency retrieval**: Recommendation systems, personalization engines
- **Large-scale knowledge bases**: Documentation search, enterprise knowledge graphs
- **Multi-modal search**: Text + image embeddings with metadata filters

---

## 🛠️ Technology Stack

**Core Language**: Rust (performance, memory safety, zero-cost abstractions)

**Key Dependencies** (planned):
- `hnswlib-rs` - Battle-tested HNSW implementation
- `tokio` - Async runtime for background tasks
- `memmap2` - Memory-mapped file I/O
- `serde` - Serialization for persistence

**Development Tools**:
- Property testing with QuickCheck
- Fuzzing with cargo-fuzz
- Benchmarking with Criterion
- Continuous integration with GitHub Actions

---

## 📖 Documentation

- [`STATUS.md`](STATUS.md) - **Start here** - Current state summary for stakeholders
- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) - Current vs target architecture
- [`docs/visiondocument.md`](docs/visiondocument.md) - Long-term vision

---

**Development workflow**:
```bash
# Clone repository
git clone https://github.com/vatskishan03/KyroDB.git
cd KyroDB

# Build and test
cargo build
cargo test

# Run with development logging
RUST_LOG=debug cargo run

# Format and lint
cargo fmt
cargo clippy
```

---

## 🗓️ Project Status

**Current Focus**: Integration and persistence

**Completed** (Phase 0 Weeks 1-12):
- ✅ HNSW vector search with >95% recall validation
- ✅ Hybrid semantic cache (frequency + similarity)
- ✅ A/B testing showing 2.1x improvement
- ✅ Memory-efficient access logging
- ✅ Production validation harness

**In Progress**:
- ⏳ Cache + HNSW integration for full query path
- ⏳ WAL and snapshot persistence
- ⏳ Scaling validation to 1-hour tests

**Next Up**:
- � Tune cache parameters to 60%+ hit rate
- � Three-tier architecture (cache → hot → cold)
- 📋 Beta customer deployments

Watch this repo or [follow @vatskishan03](https://github.com/vatskishan03) for updates.

---

## 📄 License

This project is licensed under the **Business Source License 1.1 (BSL 1.1)**.

**Permitted Use**:
- Development, testing, and non-production use
- Production use with up to 1 nodes and 1TB of data
- Creating derivative works and modifications

**Prohibited Use**:
- Offering KyroDB as a commercial managed database service

**Change License**: On **September 24, 2029**, the license will automatically convert to the **GNU Affero General Public License v3.0 (AGPLv3)**.

See the [LICENSE](LICENSE) file for full terms.

---

## 🌟 Vision

**We're building the database that RAG applications deserve.**

Not another vector database with incremental improvements. Not a general-purpose system with RAG as an afterthought. A purpose-built, obsessively optimized engine where every architectural decision serves one goal:

**The highest read speed on the planet for retrieval-augmented generation.**

Star this repo to follow our journey. We're just getting started.

---

**KyroDB** — Learned cache. HNSW search. Unbeatable speed.
