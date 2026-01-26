# Configuration Guide

Configure KyroDB via YAML/TOML files, environment variables, or command-line arguments.

## Configuration sources (priority order)

1. Command-line arguments
2. Environment variables (`KYRODB__` prefix)
3. Config file (YAML or TOML)
4. Built-in defaults

## Quick start

Generate an example config:

```bash
./kyrodb_server --generate-config yaml > config.yaml
./kyrodb_server --generate-config toml > config.toml
```

Run with a config file:

```bash
./kyrodb_server --config config.yaml
```

## Configuration reference

### Server (used)

```yaml
server:
  host: "127.0.0.1"
  port: 50051
  http_port: 51051   # default is port + 1000 when unset
  max_connections: 10000
  connection_timeout_secs: 300
  shutdown_timeout_secs: 30
```

### Cache (partially used)

```yaml
cache:
  capacity: 10000
  training_interval_secs: 600
  training_window_secs: 3600
  recency_halflife_secs: 1800
  min_training_samples: 100
  enable_ab_testing: false
  ab_test_split: 0.5
  admission_threshold: 0.15
  auto_tune_threshold: true
  target_utilization: 0.85
  enable_query_clustering: true
  clustering_similarity_threshold: 0.85
  enable_prefetching: true
  prefetch_threshold: 0.1
  max_prefetch_per_doc: 5
  strategy: learned
```

Notes:

- `cache.capacity` is used to size the cache and hot tier.
- `cache.training_interval_secs` is currently used as the hot tier max age.
- `cache.strategy`, `enable_ab_testing`, `ab_test_split`, and the clustering/prefetching fields are currently not used by `kyrodb_server`.

### HNSW (used)

```yaml
hnsw:
  max_elements: 100000
  m: 16
  ef_construction: 200
  ef_search: 50
  dimension: 768
  distance: cosine
  disable_normalization_check: false
```

### Persistence (partially used)

```yaml
persistence:
  data_dir: "./data"
  enable_wal: true
  wal_flush_interval_ms: 100
  fsync_policy: data_only
  snapshot_interval_secs: 3600
  max_wal_size_bytes: 104857600
  enable_recovery: true
  allow_fresh_start_on_recovery_failure: false
```

Notes:

- `data_dir`, `wal_flush_interval_ms`, `fsync_policy`, `snapshot_interval_secs`, `max_wal_size_bytes`, `enable_recovery`, and `allow_fresh_start_on_recovery_failure` are used.
- `enable_wal` is defined but not currently used by `kyrodb_server`.

### SLO (defined, not wired)

```yaml
slo:
  p99_latency_ms: 1.0
  cache_hit_rate: 0.70
  error_rate: 0.001
  availability: 0.999
  min_samples: 100
```

`kyrodb_server` currently uses fixed SLO thresholds in the metrics module; these values are validated but not applied.

### Rate limiting (defined, not wired)

```yaml
rate_limit:
  enabled: false
  max_qps_per_connection: 1000
  max_qps_global: 100000
  burst_capacity: 10000
```

Per-tenant rate limiting uses `max_qps` from the API keys file when authentication is enabled.

### Logging (used)

```yaml
logging:
  level: info
  format: text
  file: "./logs/kyrodb.log"
  rotation: true
  max_file_size_bytes: 104857600
  max_files: 10
```

### Authentication (partially used)

```yaml
auth:
  enabled: false
  api_keys_file: "data/api_keys.yaml"
  usage_stats_file: "data/usage_stats.csv"
  usage_export_interval_secs: 300
```

Notes:

- `enabled` and `api_keys_file` are used by `kyrodb_server`.
- Usage export settings are defined but not currently used.

### Timeouts (used)

```yaml
timeouts:
  cache_ms: 10
  hot_tier_ms: 50
  cold_tier_ms: 1000
```

## Environment variables

Override any config value with `KYRODB__` and double-underscore separators:

```bash
export KYRODB__SERVER__PORT=50051
export KYRODB__CACHE__CAPACITY=50000
export KYRODB__HNSW__DIMENSION=1536
export KYRODB__LOGGING__LEVEL=debug
./kyrodb_server
```

Format:

```
KYRODB__<section>__<key>=<value>
```

## Command-line arguments

```bash
./kyrodb_server --config config.yaml \
  --port 50051 \
  --data-dir /var/lib/kyrodb
```

Available arguments:

- `--config <FILE>`
- `--port <PORT>`
- `--data-dir <PATH>`
- `--generate-config <yaml|toml>`

## Validation

Configuration is validated on startup. Common errors include:

- Cache capacity must be > 0
- HNSW dimension must be > 0
- SLO rates must be in [0.0, 1.0]
- Paths must be readable/writable

## Troubleshooting

### Server won't start

```bash
./kyrodb_server --config config.yaml
```

### Config not taking effect

Priority order is: CLI > env vars > config file > defaults.

### Performance tuning

```bash
KYRODB__CACHE__CAPACITY=200000 ./kyrodb_server
KYRODB__HNSW__EF_SEARCH=30 ./kyrodb_server
KYRODB__PERSISTENCE__FSYNC_POLICY=none ./kyrodb_server
```
