# Changelog

All notable changes to KyroDB will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2025-09-25

### Added
- Initial public release of KyroDB v0.1.0
- Core KV storage engine with durable WAL and atomic snapshots
- Recursive Model Index (RMI) with SIMD-accelerated probing via AdaptiveRMI
- Comprehensive HTTP API with /v1/* endpoints for basic operations
- Advanced memory management with bounded allocation and leak prevention
- Background maintenance with CPU throttling protection
- Comprehensive concurrency safety with deadlock prevention
- Prometheus metrics integration with detailed performance monitoring
- Production-ready benchmarking suite with performance validation
- Complete testing infrastructure (unit, integration, property, chaos)
- Professional installation script with system service integration
- Enterprise-grade observability and health monitoring

### Fixed
- **Critical**: SIMD compilation issues on non-AVX2 systems
- **Architecture**: Proper conditional compilation for x86_64 AVX2 and ARM64 NEON
- **Installation**: Universal installation compatibility across all architectures

### Technical Foundation
- Rust-based implementation with memory safety guarantees
- Adaptive RMI with intelligent segment management
- WAL-based durability with fast crash recovery
- Lock-free data structures on hot paths
- Feature-gated components (learned-index, bench-no-metrics, failpoints)
- Multi-architecture support (x86_64, ARM64/Apple Silicon)
- SIMD-optimized operations with AVX2/NEON support

### Development & Operations
- Comprehensive CI/CD pipeline with cross-platform testing
- Professional development tooling and debugging capabilities
- Production deployment guides and performance tuning
- Complete API documentation with usage examples
- Enterprise licensing model (BSL 1.1)

## Release Notes

This is the inaugural release of KyroDB, representing the completion of **Phase 0: Foundation Rescue**. 
The single-node engine is now production-ready with comprehensive validation of core functionality.

### What's Ready for Production
- ✅ Single-node KV operations with learned indexing
- ✅ Durable persistence with atomic operations
- ✅ Memory management and concurrency control
- ✅ Background maintenance with bounded execution
- ✅ Professional monitoring and observability
- ✅ Complete installation and deployment automation

### Coming in Phase 1 (v0.2.x)
- Multi-modal queries combining vector similarity and metadata filtering
- Real-time streaming ingestion for AI workloads
- /v2/* API endpoints for advanced AI-specific operations
- Enhanced learned index optimizations for AI access patterns

---

For more information about KyroDB's roadmap and upcoming features, see [docs/visiondocument.md](docs/visiondocument.md).
