# Changelog

All notable changes to KyroDB will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2024-11-29

### Added

**Core Architecture**
- Three-tier query engine (Cache → Hot Tier → HNSW)
- Two-level Layer 1 Hybrid Semantic Cache (HSC)
  - L1a: RMI-based frequency prediction (50% hit rate)
  - L1b: Query-level semantic similarity (21% hit rate)
  - Combined: 73.5% hit rate on MS MARCO dataset
- HNSW vector index with >95% recall @ k=10
- Hot tier buffer for recent writes

**Persistence & Recovery**
- Write-Ahead Log (WAL) with configurable fsync policies
- Snapshot-based persistence
- Automatic crash recovery on startup
- Backup and restore CLI tool

**Caching & Learning**
- Access pattern logger (17.6ns overhead, ring buffer)
- RMI predictor with automatic retraining (10-minute intervals)
- A/B testing framework (LRU baseline vs. Learned strategy)
- NDCG@10 quality metrics for cache admission validation

**Configuration & Operations**
- Flexible configuration: TOML/YAML files + environment variables + CLI args
- Prometheus metrics export
- Structured logging with JSON and text formats
- Health check endpoints
- Multi-tenant API key authentication
- Rate limiting (per-connection and global)

**Testing & Validation**
- MS MARCO dataset integration (71,878 queries, 10K corpus)
- 12-hour sustained load validation
- 246 unit tests (100% passing)
- Memory profiling with jemalloc
- Chaos recovery testing

**Documentation**
- Complete architecture documentation
- API reference guide
- Operations and troubleshooting guide
- Backup and recovery procedures
- Configuration management guide
- Quick start guide

### Performance

**Validated Metrics (MS MARCO Dataset)**
- L1 cache hit rate: 73.5% (vs. 25-35% LRU baseline)
- HNSW search latency: <1ms P99 @ 10M vectors
- HNSW recall: >95% @ k=10
- Overall query latency: P50: 12μs, P99: 9.7ms (bimodal distribution)
- Memory stability: 0% growth under sustained load
- Training cycles: 40/40 successful (15-second intervals)

### Technical Details

**Language & Dependencies**
- Rust 1.70+ (memory safety, zero-cost abstractions)
- hnswlib-rs (HNSW k-NN search)
- tokio (async runtime)
- serde (serialization)
- jemallocator (memory profiling)

**Code Quality**
- Zero clippy warnings in production code
- All 246 unit tests passing
- Consistent formatting via cargo fmt
- Professional documentation (15 markdown files)

### Known Limitations

- Single-node deployment only
- gRPC-only interface (HTTP observability separate)
- Read-optimized (not suitable for write-heavy workloads)
- Uniform access patterns see limited cache benefit

---

## Release Notes

KyroDB v0.1 is the first public release, focusing on validating the core Hybrid Semantic Cache architecture.

**Key Achievement**: 73.5% cache hit rate on realistic RAG workloads, representing a 2-3× improvement over traditional LRU caching.

**Recommended For**:
- RAG systems with predictable access patterns
- Customer support FAQ retrieval
- Code search and completion
- Enterprise knowledge bases

**Not Recommended For**:
- Write-heavy workloads
- Uniform access patterns
- Production deployments requiring multi-node clustering

### Upgrade Path

This is the initial release. Future versions will maintain backward compatibility where possible.

---

## Future Roadmap

**v0.2** (Q1 2025)
- Extended validation on diverse RAG workloads
- Performance optimization (SIMD, hot path profiling)
- Concurrent load testing
- Chaos engineering

**v0.3+** (2025)
- Multi-node distributed deployment
- Hybrid queries (vector + metadata filters)
- Real-time replication
- Cloud-native deployment guides

---

[0.1.0]: https://github.com/vatskishan03/KyroDB/releases/tag/v0.1.0
