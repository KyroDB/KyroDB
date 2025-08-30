# KyroDB — Durable KV with a Production Recursive Model Index (RMI)

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Rust](https://img.shields.io/badge/Rust-1.70%2B-orange)](https://www.rust-lang.org/)
[![Go](https://img.shields.io/badge/Go-1.21%2B-00ADD8)](https://golang.org/)
[![Docker](https://img.shields.io/badge/Docker-Ready-2496ED)](https://www.docker.com/)
[![CI](https://github.com/vatskishan03/KyroDB/actions/workflows/test-matrix.yml/badge.svg)](https://github.com/vatskishan03/KyroDB/actions)
[![codecov](https://codecov.io/gh/vatskishan03/KyroDB/branch/main/graph/badge.svg)](https://codecov.io/gh/vatskishan03/KyroDB)

**Status: Alpha** (focused scope: KV + RMI) | **Latest: v0.1.0**

KyroDB is a durable, append-only key-value engine with a production-grade learned index (RMI) for ultra-fast point lookups and predictable tail latency.

- ⚡ **Default read path**: RMI (learned-index) with SIMD-accelerated probing
- 🛡️ **Durability**: WAL + snapshot durability, fast recovery, compaction controls
- 🌐 **Simple HTTP API**: RESTful endpoints under `/v1`, Prometheus metrics at `/metrics`
- 📊 **Observability**: Built-in metrics, health checks, and performance monitoring
- 🐳 **Production Ready**: Docker support, rate limiting, authentication

---

## Table of Contents
- [Quickstart](#quickstart)
- [Benchmarks](#benchmarks)
- [API Reference](#api-reference)
- [Operations](#operations)
- [Architecture](#architecture)
- [Contributing](#contributing)
- [License](#license)

---

## Quickstart

### Prerequisites
- Rust toolchain (1.70+)
- Go (1.21+) - optional, for CLI
- Python (3.8+) - optional, for plotting

### Installation & Run
```bash
# Clone repository
git clone https://github.com/vatskishan03/KyroDB.git
cd KyroDB

# Build and run
cargo build -p kyrodb-engine --release
./target/release/kyrodb-engine serve 127.0.0.1 3030
```

### Basic Operations
```bash
# Health check
curl -s http://127.0.0.1:3030/health

# Put data
curl -X POST http://127.0.0.1:3030/v1/put \
  -H 'Content-Type: application/json' \
  -d '{"key": 123, "value": "hello"}'

# Get data (ultra-fast RMI lookup)
curl -s http://127.0.0.1:3030/v1/get_fast/123

# View metrics
curl -s http://127.0.0.1:3030/metrics | head -20
```

### Docker
```bash
# Quick start with Docker
docker run -p 3030:3030 ghcr.io/vatskishan03/kyrodb:latest

# Or use docker-compose
docker-compose up -d
```

---

## Benchmarks

### Microbenchmarks (Engine)
Compare raw key-lookup latency for RMI vs B-Tree:

```bash
# Run comparison benchmark
cargo bench -p bench --bench kv_index

# Scale test (1M, 10M, 50M keys)
KYRO_BENCH_N=10000000 cargo bench -p bench --bench kv_index
```

### HTTP Workload (End-to-End)
```bash
# Load 1M keys, run 64-concurrent uniform reads for 30s
cargo run -p bench --release -- \
  --base http://127.0.0.1:3030 \
  --load-n 1000000 \
  --val-bytes 64 \
  --read-concurrency 64 \
  --read-seconds 30 \
  --dist uniform
```

### Headline Results

**RMI vs B-Tree Lookup Latency** (engine microbenchmark):
![RMI vs B-Tree](bench/rmi_vs_btree.png)

**HTTP Read Performance** (1M keys, 64B values, uniform distribution):
- **Throughput**: 150K+ ops/sec sustained
- **P99 Latency**: <2.2ms consistently
- **Stability**: Predictable tail latency across scales

### Large-Scale Testing
```bash
# Run comprehensive benchmark suite (10M/50M keys)
./bench/scripts/run_large_benchmarks.sh

# Generate performance plots
python3 bench/scripts/generate_plots.py
```

**Key Findings**:
- ✅ RMI provides 2-5x faster lookups than B-Tree at scale
- ✅ Sub-millisecond p99 latency maintained across 50M+ keys
- ✅ SIMD optimizations deliver significant performance gains
- ✅ Predictable performance regardless of dataset size

---

## API Reference

### Data Operations
```http
# Fast RMI-based lookup (recommended)
GET /v1/get_fast/{key} → value bytes or 404

# Standard lookup with metadata
GET /v1/lookup?key=123 → {"value": "...", "offset": 456}

# Insert/Update
POST /v1/put → {"offset": 789}
Content-Type: application/json
{"key": 123, "value": "data"}
```

### Administrative
```http
# Build/optimize RMI index
POST /v1/rmi/build → {"ok": true, "count": 1000000}

# Create snapshot
POST /v1/snapshot → {"status": "ok"}

# Warm up system (preload indexes)
POST /v1/warmup → {"status": "ok"}

# Get current offset
GET /v1/offset → {"offset": 1234567}
```

### Monitoring
```http
# Prometheus metrics
GET /metrics

# Health check
GET /health → {"status": "ok"}

# Build information
GET /build_info → {"commit": "abc123", "features": ["learned-index"]}
```

### Authentication
```bash
# Enable with environment variable
export KYRODB_AUTH_TOKEN="your-secret-token"

# Use in requests
curl -H "Authorization: Bearer your-secret-token" \
  http://127.0.0.1:3030/v1/put -d '{"key":1,"value":"secret"}'
```

---

## Operations

### Configuration
```bash
# Core settings
KYRODB_PORT=3030
KYRODB_DATA_DIR=./data

# Performance tuning
KYRODB_WARM_ON_START=1          # Preload indexes on startup
KYRODB_RMI_ROUTER_BITS=10       # RMI router configuration
KYRODB_FSYNC_POLICY=data        # WAL fsync policy

# Rate limiting (per IP)
KYRODB_RL_DATA_RPS=5000         # Data operations per second
KYRODB_RL_DATA_BURST=10000      # Burst capacity
KYRODB_RL_ADMIN_RPS=2           # Admin operations per second

# Observability
KYRODB_DISABLE_HTTP_LOG=1       # Reduce request logging
```

### Production Deployment
```bash
# Using systemd
sudo cp docs/systemd/kyrodb-engine.service /etc/systemd/system/
sudo systemctl enable kyrodb-engine
sudo systemctl start kyrodb-engine

# Using Docker Compose (recommended)
docker-compose -f docker-compose.yaml up -d

# Behind reverse proxy (Caddy example)
caddy reverse_proxy localhost:3030
```

### Monitoring & Alerting
- **Metrics Endpoint**: `/metrics` (Prometheus format)
- **Health Checks**: `/health` (Kubernetes compatible)
- **Logging**: Structured JSON logs to stdout
- **Performance**: Built-in latency histograms and throughput counters

### Backup & Recovery
```bash
# Manual snapshot
curl -X POST http://127.0.0.1:3030/v1/snapshot

# Files to backup
data/snapshot.bin      # Latest consistent snapshot
data/wal.*            # Write-ahead logs
data/index-rmi.bin    # Learned index (rebuildable)

# Recovery: restart service (automatic)
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

### Data Flow
1. **Write Path**: HTTP → Validation → WAL append → In-memory delta → Response
2. **Read Path**: HTTP → RMI predict → SIMD probe → Value lookup → Response
3. **Background**: Compaction, index rebuilding, metrics collection

### Key Technologies
- **Rust**: Memory safety, zero-cost abstractions, high performance
- **SIMD**: AVX2/AVX-512/NEON for probe acceleration
- **Learned Index**: RMI with ε-bounded predictions
- **Durability**: WAL + atomic snapshots with fsync
- **Observability**: Prometheus metrics, structured logging

### Performance Characteristics
- **Latency**: Sub-millisecond p99 for point lookups
- **Throughput**: 100K+ ops/sec sustained
- **Scalability**: Linear performance with dataset size
- **Reliability**: Crash-safe with fast recovery (<1s)

---

## Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

### Quick Development Setup
```bash
# Clone and setup
git clone https://github.com/vatskishan03/KyroDB.git
cd KyroDB

# Run tests
cargo test -p kyrodb-engine

# Run fuzzing (nightly required)
cargo +nightly fuzz run rmi_probe

# Build docs
cargo doc --open
```

### Areas for Contribution
- 🚀 **Performance**: SIMD optimizations, algorithmic improvements
- 🧪 **Testing**: Additional fuzz targets, chaos testing
- 📚 **Documentation**: Tutorials, examples, API docs
- 🔧 **Tooling**: CLI improvements, monitoring integrations
- 🎯 **Features**: Vector search, clustering, advanced queries

---

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.

---

## Community & Support

- 🐛 **Issues**: [GitHub Issues](https://github.com/vatskishan03/KyroDB/issues)
- 💬 **Discussions**: [GitHub Discussions](https://github.com/vatskishan03/KyroDB/discussions)
- 📧 **Email**: kishanvats2003@gmail.com

---

## Roadmap

See [visiondocument.md](visiondocument.md) for our ambitious roadmap including:
- 🚀 **Multi-node clustering** and replication
- 🔍 **Advanced vector search** with HNSW optimizations
- ⚡ **Query processing** with SQL extensions
- 📊 **Advanced analytics** and time-series support
- ☁️ **Cloud-native** deployment and auto-scaling

---

*KyroDB: Where machine learning meets database performance* 🚀
