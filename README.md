# KyroDB — The Fastest Vector Database for RAG

[![License: BSL 1.1](https://img.shields.io/badge/License-BSL%201.1-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/Rust-1.70%2B-orange)](https://www.rust-lang.org/)
[![CI](https://github.com/vatskishan03/KyroDB/actions/workflows/ci.yml/badge.svg)](https://github.com/vatskishan03/KyroDB/actions)

**Status: Phase 0 — Early Development** • Building the highest read-speed vector database on the planet.

KyroDB is an ultra-high-performance vector database optimized for Retrieval-Augmented Generation (RAG) workloads. We're building a three-layer architecture with HNSW for k-NN search and a learned cache that predicts hot documents before you query them.

> **⚠️ Development Status**: KyroDB is in active early-stage development (Phase 0). The system is not yet production-ready. Watch this repo or star it to follow our progress as we build towards our ambitious performance targets.

## 🎯 Mission

**Build the highest read-speed vector database on the planet.**

No compromises. No competition. Just pure speed optimized for RAG workloads.

## 🚀 What Makes KyroDB Different

### Three-Layer Architecture (In Development)

**Layer 1: Learned Cache** (70-90% hit rate)
- RMI (Recursive Model Index) predicts which documents are hot *before* you query
- Sub-100µs P99 latency for cache hits
- Trained on real access patterns, retrains automatically

**Layer 2: Hot Tier** (Recent writes)
- BTree for fresh documents not yet in cold storage
- Millisecond-latency hybrid search (vector + metadata)

**Layer 3: Cold Tier** (Historical data)
- HNSW for k-NN search with >95% recall@10
- Sub-millisecond P99 latency on 10M+ vectors
- Memory-mapped for zero-copy reads

### Design Philosophy

- **Performance-first**: Every line of code on the query path is optimized
- **Zero allocations**: Arena allocators, pre-allocated buffers, SIMD everywhere
- **Lock-free reads**: Atomic index swaps, zero contention
- **Bounded operations**: No O(n) scans, no unpredictable latency
- **Deep implementation**: Production-grade from day one, not MVP quality

## 📊 Target Performance (Phase 0 Goals)

| Metric | Target | Workload |
|--------|--------|----------|
| **P99 Query Latency** | < 1ms | 10M vectors, 128-dim, L2 distance |
| **Cache Hit Rate** | 70-90% | Zipfian distribution (80/20 hot/cold) |
| **Cache Hit Latency** | < 100µs | Learned cache prediction |
| **Recall@10** | > 95% | HNSW k-NN search |
| **Write Throughput** | > 100k/sec | Bulk inserts with persistence |
| **Crash Recovery** | < 10s | 10M vectors from WAL replay |

**Status**: Phase 0 Week 1-2 in progress (HNSW prototype)

---

## 🏗️ Development Roadmap

We're building KyroDB in disciplined phases, with strict go/no-go gates based on performance SLOs.

### Phase 0: Single-Node Foundation (12 weeks)

**Week 1-2: HNSW Vector Search** *(In Progress)*
- Integrate battle-tested hnswlib-rs library
- Property tests proving >95% recall@10
- Benchmark: P99 < 1ms @ 10M vectors

**Week 3-8: Learned Cache with RMI**
- Train RMI on access patterns (Zipfian distribution)
- Three-layer query execution (cache → hot → cold)
- Target: 70-90% cache hit rate, P99 < 100µs

**Week 9-12: Basic Persistence**
- WAL + snapshots for vectors and metadata
- Crash recovery with WAL replay
- Write throughput: >100k inserts/sec

### Phase 1: Production Hardening (6 months)
- Distributed consensus (Raft)
- Replication and high availability
- Hybrid queries (vector + metadata filters)
- Compression and tiered storage

### Phase 2: Advanced Features (6 months)
- Multi-tenancy and resource isolation
- Streaming updates and incremental indexing
- GPU acceleration for batch queries
- Cross-datacenter replication

### Phase 3-4: Scale & Autonomy (12 months)
- Autonomous tuning and adaptive algorithms
- Federated search across clusters
- Advanced RAG optimizations (late interaction, ColBERT)

See [`Implementation.md`](Implementation.md) for the detailed week-by-week execution plan.

---

## 🚀 Quick Start (Coming Soon)

KyroDB is in early development. The following API is the target design:

```rust
use kyrodb::{VectorDB, Config, Metric};

// Initialize database
let db = VectorDB::open(Config {
    data_dir: "./data",
    dimension: 128,
    metric: Metric::L2,
})?;

// Insert vectors
db.insert(doc_id, embedding, metadata)?;

// Search with learned cache
let results = db.search(query_embedding, k=10)?;
// Returns: Vec<(doc_id, distance)>

// Hybrid search (vector + metadata filter)
let results = db.search_filtered(
    query_embedding,
    k=10,
    filter="category == 'science'"
)?;
```

**Prerequisites** (when available):
- Rust 1.70+
- 8GB+ RAM recommended
- Linux/macOS (Windows support planned)

---

## 🏛️ Architecture Deep Dive

### Query Execution Flow (Target Design)

```
┌─────────────────────────────────────────────────────────────┐
│                      Client Query                           │
│                  (embedding + k + filters)                  │
└────────────────────────────┬────────────────────────────────┘
                             │
                             ▼
            ┌────────────────────────────────┐
            │   LAYER 1: Learned Cache       │
            │   RMI predicts hot documents   │
            │   70-90% hit rate, <100µs P99  │
            └────────┬───────────────────┬───┘
                     │ HIT               │ MISS
                     ▼                   ▼
            ┌────────────┐      ┌────────────────────┐
            │   Return   │      │  LAYER 2: Hot Tier │
            │   Results  │      │  Recent writes     │
            └────────────┘      │  BTree search      │
                                └─────────┬──────────┘
                                          │ Not found
                                          ▼
                                ┌──────────────────────┐
                                │ LAYER 3: Cold Tier   │
                                │ HNSW k-NN search     │
                                │ >95% recall, <1ms    │
                                └──────────┬───────────┘
                                           │
                                           ▼
                                  ┌─────────────────┐
                                  │ Update learned  │
                                  │ cache (async)   │
                                  └─────────────────┘
```

### Core Components

**HNSW Index** (Phase 0 Week 1-2)
- Hierarchical Navigable Small World graphs for k-NN search
- Battle-tested hnswlib-rs library (wraps C++ implementation)
- Tunable parameters: M (graph connectivity), ef (search quality)

**Learned Cache** (Phase 0 Week 3-8)
- RMI (Recursive Model Index) predicts document hotness
- Trained on access logs (Zipfian distribution: 80/20 hot/cold)
- Retrains automatically every 10 minutes
- O(log n) prediction time with bounded search

**Tiered Storage** (Phase 0 Week 9-12)
- Hot tier: BTree for recent writes (millisecond latency)
- Cold tier: HNSW for historical data (sub-millisecond latency)
- Async flush from hot → cold with atomic index swaps

**Persistence Layer** (Phase 0 Week 9-12)
- Write-Ahead Log (WAL) for durability
- Periodic snapshots for fast recovery
- Memory-mapped reads for zero-copy access

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

- [`docs/visiondocument.md`](docs/visiondocument.md) - Long-term vision and architecture

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

**Current Phase**: Phase 0 Week 1-2 (HNSW Prototype)


**Next Milestones**:
- 📅 Week 2: HNSW property tests (recall@10 > 95%)
- 📅 Week 2: Performance benchmark (P99 < 1ms @ 10M vectors)
- 📅 Week 8: Learned cache integration (70-90% hit rate)
- 📅 Week 12: Basic persistence (WAL + snapshots)

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
