# Configuration Guide

Configure KyroDB via YAML/TOML files, environment variables, or command-line arguments.

## Configuration Sources (Priority Order)

1. **Command-line arguments** (highest priority)
2. **Environment variables** (`KYRODB_*` prefix)
3. **Config file** (YAML or TOML)
4. **Built-in defaults** (lowest priority)

---

## Quick Start

**Generate example config:**
```bash
# YAML format
./kyrodb_server --generate-config yaml > config.yaml

# TOML format
./kyrodb_server --generate-config toml > config.toml
```

**Run with config file:**
```bash
./kyrodb_server --config config.yaml
```

---

## Configuration Sections

### Server

```yaml
server:
  host: "127.0.0.1"
  port: 50052                    # gRPC port
  http_port: 51052               # HTTP port (gRPC + 1000)
  max_connections: 10000
  connection_timeout_secs: 300
  shutdown_timeout_secs: 30
```

### Cache

```yaml
cache:
  capacity: 10000                # Number of vectors to cache
  strategy: learned              # lru | learned | abtest
  training_interval_secs: 600    # Train RMI every 10 minutes
  min_training_samples: 100
  enable_ab_testing: false
  ab_test_split: 0.5             # 50/50 traffic split
```

### HNSW

```yaml
hnsw:
  max_elements: 100000           # Maximum vectors in index
  m: 16                          # Links per node (higher = better recall)
  ef_construction: 200           # Build quality (higher = slower build)
  ef_search: 50                  # Query quality (higher = slower queries)
  dimension: 768                 # MUST match your embeddings
  distance: cosine               # cosine | euclidean | innerproduct
```

**Embedding dimensions for common models:**
- Sentence transformers: 768
- OpenAI ada-002: 1536
- MiniLM: 384
- BGE-large: 1024

### Persistence

```yaml
persistence:
  data_dir: "./data"
  enable_wal: true
  wal_flush_interval_ms: 100
  fsync_policy: data_only        # none | data_only | full
  snapshot_interval_secs: 3600   # Create snapshot every hour
  max_wal_size_bytes: 104857600  # 100 MB
  enable_recovery: true
```

**Fsync policies:**
- `none`: Fast, risky (data loss on crash)
- `data_only`: Balanced (recommended for production)
- `full`: Safest, slowest (financial/critical data)

### SLO

```yaml
slo:
  p99_latency_ms: 1.0            # P99 query latency target
  cache_hit_rate: 0.70           # Minimum 70% cache hits
  error_rate: 0.001              # Maximum 0.1% errors
  availability: 0.999            # 99.9% uptime
  min_samples: 100
```

### Rate Limiting

```yaml
rate_limit:
  enabled: false
  max_qps_per_connection: 1000
  max_qps_global: 100000
  burst_capacity: 10000
```

### Logging

```yaml
logging:
  level: info                    # trace | debug | info | warn | error
  format: text                   # text | json
  file: "./logs/kyrodb.log"      # Omit for stdout only
  rotation: true
  max_file_size_bytes: 104857600  # 100 MB
  max_files: 10
```

### Authentication

```yaml
auth:
  enabled: false
  api_keys_file: "data/api_keys.yaml"
  usage_stats_file: "data/usage_stats.csv"
  usage_export_interval_secs: 300
```

---

## Environment Variables

Override any config value using `KYRODB__` prefix with double underscores:

```bash
# Server port
export KYRODB__SERVER__PORT=50052

# Cache capacity
export KYRODB__CACHE__CAPACITY=50000

# HNSW dimension
export KYRODB__HNSW__DIMENSION=1536

# Logging level
export KYRODB__LOGGING__LEVEL=debug

# Run server
./kyrodb_server
```

**Nested structures use double underscore:**
```
KYRODB__<section>__<key>=<value>
```

---

## Command-Line Arguments

Override specific settings (highest priority):

```bash
# Override port and data directory
./kyrodb_server --config config.yaml \
  --port 50052 \
  --data-dir /var/lib/kyrodb

# Available arguments:
#   --config <FILE>       Config file path
#   --port <PORT>         gRPC port
#   --data-dir <PATH>     Data directory
#   --generate-config     Generate example config (yaml|toml)
```

---

## Usage Examples

### 1. Default Configuration
```bash
# Uses built-in defaults
./kyrodb_server
```

### 2. With YAML Config File
```bash
# Generate example config
./kyrodb_server --generate-config yaml > config.yaml

# Edit config.yaml, then run
./kyrodb_server --config config.yaml
```

### 3. With TOML Config File
```bash
./kyrodb_server --generate-config toml > config.toml
./kyrodb_server --config config.toml
```

### 4. Environment Variables
```bash
KYRODB__SERVER__PORT=50052 \
KYRODB__CACHE__CAPACITY=50000 \
./kyrodb_server
```

### 5. Combined (CLI + Env + File)
```bash
# Priority: CLI > Env > File > Defaults
KYRODB__CACHE__CAPACITY=25000 \
./kyrodb_server --config config.yaml --port 50052
```

---

## Common Configurations

### Development Setup

```yaml
cache:
  capacity: 1000
hnsw:
  max_elements: 10000
  dimension: 384
persistence:
  fsync_policy: none  # Fast, skip fsync
logging:
  level: debug
```

### Production Setup

```yaml
cache:
  capacity: 100000
hnsw:
  max_elements: 10000000  # 10M vectors
  dimension: 1536
  m: 32  # Higher recall
persistence:
  fsync_policy: data_only  # Balanced durability
  data_dir: /var/lib/kyrodb
slo:
  p99_latency_ms: 1.0
logging:
  level: info
  format: json  # For log aggregation
```

### High-Throughput Setup

```yaml
cache:
  capacity: 200000
  strategy: learned
hnsw:
  max_elements: 100000000  # 100M vectors
  ef_search: 100
persistence:
  wal_flush_interval_ms: 500  # Larger batch
server:
  max_connections: 50000
rate_limit:
  enabled: true
  max_qps_global: 1000000
```

---

## Validation

Configuration validated on startup:

```bash
# Invalid config
cache:
  capacity: 0  # Error: must be > 0
  
# Output:
Error: Configuration validation failed
  - cache.capacity must be greater than 0
```

**Common validation errors:**
- Cache capacity must be > 0
- HNSW dimension must match data
- SLO values must be in valid ranges (0.0-1.0 for rates)
- Paths must be readable/writable

---

## Tuning Guidelines

### Cache Size
- Start with 10% of total vectors
- Increase if hit rate < 70%
- Monitor memory usage

### HNSW Parameters
- `m`: Higher = better recall, more memory (default: 16)
- `ef_construction`: Higher = better index, slower build (default: 200)
- `ef_search`: Higher = better search, slower queries (default: 50)

### Fsync Policy
- Development: `none` (fast)
- Production: `data_only` (balanced)
- Critical: `full` (safest)

---

## Troubleshooting

### Server won't start

```bash
# Check config validation
./kyrodb_server --config config.yaml

# Common issues:
# - Invalid YAML/TOML syntax
# - Data directory doesn't exist
# - Port already in use
# - Insufficient permissions
```

### Config not taking effect

```bash
# Check priority order:
# 1. CLI arguments override everything
# 2. Env vars override config file
# 3. Config file overrides defaults
```

### Performance issues

```bash
# Increase cache capacity
KYRODB__CACHE__CAPACITY=200000 ./kyrodb_server

# Tune HNSW for speed
KYRODB__HNSW__EF_SEARCH=30 ./kyrodb_server

# Disable fsync for benchmarking
KYRODB__PERSISTENCE__FSYNC_POLICY=none ./kyrodb_server
```
