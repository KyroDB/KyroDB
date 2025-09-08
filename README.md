# KyroDB — High-Performance Key-Value Database with Learned Indexes

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Rust](https://img.shields.io/badge/Rust-1.70%2B-orange)](https://www.rust-lang.org/)
[![CI](https://github.com/vatskishan03/KyroDB/actions/workflows/ci.yml/badge.svg)](https://github.com/vatskishan03/KyroDB/actions)

**Status: Phase 0 Development** - Foundation-first approach to building a production-ready database with learned indexing.

KyroDB is a high-performance, durable key-value database engine featuring Recursive Model Index (RMI) for predictable sub-millisecond lookups.

## 🚀 Core Features

- ⚡ **Learned Indexing (RMI)**: 2-stage models with SIMD-accelerated probing for O(1) expected lookup time
- 🛡️ **ACID Durability**: WAL + snapshots with atomic operations and fast crash recovery  
- 🔒 **Lock-Free Concurrency**: Eliminates deadlocks while maintaining high throughput
- 📊 **Enterprise Monitoring**: Prometheus metrics, health checks, structured logging
- 🌐 **HTTP API**: RESTful endpoints optimized for maximum performance

## 📈 Performance Targets (Phase 0)

- **P99 Latency**: < 1ms for point lookups
- **Throughput**: > 100K QPS sustained
- **Memory Efficiency**: Bounded usage with predictable performance
- **Zero Deadlocks**: Lock-free architecture eliminates concurrency issues

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

### Basic Operations
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

## Architecture

### Core Components
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

### Key Technologies
- **RMI (Recursive Model Index)**: Learned data structures that predict key locations
- **Lock-Free Concurrency**: ArcSwap and atomic operations eliminate deadlocks
- **Memory-Mapped I/O**: Zero-copy reads for maximum throughput
- **SIMD Acceleration**: AVX2/AVX512 vectorized search operations

---

## API Reference

### Core Operations
```http
# Key-value operations
POST /v1/put {"key": u64, "value": string}
GET  /v1/lookup?key=123
POST /v1/put_fast/{key}  # Binary body for max performance
GET  /v1/get_fast/{key}  # Binary response

# Administrative
POST /v1/snapshot       # Trigger manual snapshot
POST /v1/compact        # Trigger compaction
POST /v1/rmi/build      # Rebuild RMI index
POST /v1/warmup         # Warm up caches

# Monitoring  
GET  /health            # Health check
GET  /metrics           # Prometheus metrics
GET  /build_info        # Version and build info
```

---

## Operations

### Production Deployment
```bash
# Optimized for production
./kyrodb-engine serve 0.0.0.0 3030 \
  --auto-snapshot-secs 3600 \
  --wal-max-bytes 1073741824 \
  --rmi-rebuild-appends 100000 \
  --enable-rate-limiting

# With environment tuning
RUST_LOG=info \
KYRODB_WARM_ON_START=1 \
KYRODB_RL_RPS=50000 \
./kyrodb-engine serve 0.0.0.0 3030
```

### Monitoring & Metrics
```bash
# Health check
curl http://localhost:3030/health

# Key metrics to monitor
curl http://localhost:3030/metrics | grep kyrodb_
# - kyrodb_rmi_reads_total
# - kyrodb_rmi_probe_length_histogram  
# - kyrodb_appends_total
# - kyrodb_snapshot_duration_seconds
```

---

## Development

### Build & Test
```bash
# Full test suite
cargo test -p kyrodb-engine

# Benchmarks
cd bench && cargo run --release

# Fuzzing (requires nightly)
cargo +nightly fuzz run rmi_probe
```

### Performance Validation
```bash
# End-to-end benchmark
cd bench
cargo run --release -- --load-n 1000000 --read-seconds 30

# Check for O(n) fallbacks (should be zero)
curl http://localhost:3030/metrics | grep kyrodb_btree_reads_total
```

---

## Contributing

We welcome contributions focused on Phase 0 foundation work:

- 🚀 **Performance**: RMI optimizations, SIMD improvements
- 🔒 **Concurrency**: Lock-free data structures, deadlock elimination  
- 🧪 **Testing**: Property-based tests, chaos testing, fuzzing
- 📊 **Benchmarking**: Performance validation, regression detection

```bash
# Development setup
git clone https://github.com/vatskishan03/KyroDB.git
cd KyroDB
cargo test -p kyrodb-engine
```

---

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.

---

**KyroDB: Foundation-first approach to the world's fastest learned-index database** 🚀
