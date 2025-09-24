# KyroDB v0.1.0 Release Notes

**Release Date**: 2025-09-25  
**Commit**: f293090  
**Test Environment**: Darwin arm64, Rust rustc 1.89.0 (29483883e 2025-08-04)

## 🎯 **Phase 0 Foundation Complete**

This inaugural release represents the completion of **Phase 0: Foundation Rescue**, delivering a production-ready single-node database engine with learned indexing capabilities.

### ⚡ **Core Features**
- **Adaptive RMI (Recursive Model Index)**: Learned indexing with intelligent segment management
- **Durable Storage**: WAL-based persistence with atomic snapshots
- **Memory Safety**: Comprehensive memory management with leak prevention
- **Concurrency Control**: Professional-grade locking with deadlock prevention
- **SIMD Optimization**: AVX2/NEON accelerated operations for performance
- **HTTP API**: Complete /v1/* REST endpoints for database operations

### 🛡️ **Production Readiness**
- Comprehensive testing infrastructure (unit, integration, property tests)
- Professional observability with Prometheus metrics
- Enterprise-grade error handling and recovery
- Complete operational tooling and deployment automation
- Multi-architecture support (x86_64, ARM64/Apple Silicon)

### 🔧 **Installation**

```bash
# Standard installation (works on all architectures)
cargo install kyrodb-engine --features learned-index

# With SIMD optimization (x86_64 + AVX2)
RUSTFLAGS="-C target-feature=+avx2" cargo install kyrodb-engine --features learned-index

# With SIMD optimization (ARM64 + NEON)  
RUSTFLAGS="-C target-feature=+neon" cargo install kyrodb-engine --features learned-index

# Build from source with native optimizations
git clone https://github.com/vatskishan03/KyroDB.git
cd KyroDB
RUSTFLAGS="-C target-cpu=native" cargo build --release --features learned-index

# Start server
./target/release/kyrodb-engine serve 127.0.0.1 3030
```

### ⚠️ **Architecture Notes**
- **x86_64**: AVX2 support provides 4-wide SIMD operations for maximum performance
- **ARM64**: NEON support provides 2-wide SIMD operations  
- **Universal**: Scalar fallback operations work on all architectures without SIMD features

### 📚 **Documentation**
- **API Reference**: Complete HTTP endpoint documentation
- **Installation Guide**: Automated system setup
- **Performance Guide**: Benchmarking and optimization
- **Developer Guide**: Testing and contribution guidelines

### 🔮 **What's Next**
Phase 1 (v0.2.x) will introduce AI-native capabilities including multi-modal queries, real-time streaming, and enhanced learned index optimizations.

### 📄 **License**
KyroDB is licensed under the Business Source License 1.1 (BSL 1.1), enabling free use for development, testing, and small production deployments.

---

**Ready to experience next-generation database performance with learned indexing?**
Download KyroDB v0.1.0 today! 🚀
