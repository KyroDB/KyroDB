# Configuration Guide

Configure KyroDB via YAML/TOML files, environment variables, or command-line arguments.

## Breaking changes / migration guide

KyroDB config parsing is intentionally strict: unknown keys are rejected at startup. If you are upgrading from an older config, pay attention to these changes:

- **Environment variables**: nested configuration now uses the `KYRODB__` (double underscore) prefix + separator.
  - Example: `KYRODB_CACHE_CAPACITY=50000` → `KYRODB__CACHE__CAPACITY=50000`
  - Example: `KYRODB_HNSW_DIMENSION=1536` → `KYRODB__HNSW__DIMENSION=1536`
  - The server still supports CLI shortcut env vars for convenience: `KYRODB_CONFIG`, `KYRODB_PORT`, `KYRODB_DATA_DIR`.
- **Persistence snapshot interval**: `persistence.snapshot_interval_secs` was renamed to `persistence.snapshot_interval_mutations` (operation-count based: counts all WAL mutations including inserts and deletes; `0` disables automatic snapshots). The legacy key `snapshot_interval_inserts` is still accepted as a backward-compatible alias during deserialization. **Migration**: replace `snapshot_interval_secs: N` with `snapshot_interval_mutations: N` in your config file; the semantics change from wall-clock seconds to mutation count, so choose a value appropriate for your write throughput (e.g., `1000` triggers a snapshot every 1 000 mutations).
- **Removed config keys (now rejected)**:
  - `persistence.enable_wal`
  - `auth.usage_stats_file`, `auth.usage_export_interval_secs`
  - Cache flags that were previously documented but ignored: `cache.enable_ab_testing`, `cache.ab_test_split`, `cache.enable_query_clustering`, `cache.clustering_similarity_threshold`, `cache.enable_prefetching`, `cache.prefetch_threshold`, `cache.max_prefetch_per_doc`

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
  http_host: "127.0.0.1"  # optional; defaults to server.host
  observability_auth: disabled  # disabled | metrics_and_slo | all
  max_connections: 10000
  connection_timeout_secs: 300
  shutdown_timeout_secs: 30
  tls:
    enabled: false
    cert_path: null
    key_path: null
    ca_cert_path: null
    require_client_cert: false
```

Notes:

- `server.http_host` sets the HTTP observability bind host (`/metrics`, `/health`, `/ready`, `/slo`, `/usage`). When omitted, it defaults to `server.host`.
- `server.observability_auth` controls auth on `/metrics`, `/health`, `/ready`, and `/slo`:
  - `disabled`: no auth on observability endpoints.
  - `metrics_and_slo`: require auth on `/metrics` and `/slo`.
  - `all`: require auth on `/metrics`, `/health`, `/ready`, and `/slo`.
- `GET /usage` is controlled independently by `auth.enabled`:
  - `auth.enabled=true`: `/usage` always requires API key auth (regardless of `server.observability_auth`).
  - `auth.enabled=false`: usage tracking is disabled and `/usage` returns `404`.
- `server.observability_auth != disabled` requires `auth.enabled=true`.
- `server.tls` configures gRPC TLS:
  - `enabled=true` requires both `cert_path` and `key_path`.
  - `require_client_cert=true` (mTLS) additionally requires `ca_cert_path`.
- TLS fields under `server.tls` apply to the gRPC server. HTTP observability endpoints do not have an in-process TLS listener setting; for remote HTTP exposure, terminate TLS at an external proxy/load balancer and bind `server.http_host` conservatively.

TLS example for non-loopback gRPC host:

```yaml
server:
  host: "0.0.0.0"
  port: 50051
  tls:
    enabled: true
    cert_path: "/etc/kyrodb/tls/server.crt"
    key_path: "/etc/kyrodb/tls/server.key"
    ca_cert_path: "/etc/kyrodb/tls/ca.crt"
    require_client_cert: false
```

### Cache (used + partially wired)

> **Partially wired** means the config keys are parsed and stored, but not all of
> them drive runtime behavior yet. Keys marked *used* below are fully functional;
> the rest are reserved for upcoming features and will be wired in future releases.

```yaml
cache:
  capacity: 10000
  strategy: learned
  enable_training_task: true
  training_interval_secs: 600
  training_window_secs: 3600
  recency_halflife_secs: 1800
  min_training_samples: 100
  admission_threshold: 0.15
  auto_tune_threshold: true
  target_utilization: 0.85
  predictor_capacity_multiplier: 4
  logger_window_size: 1000000
  query_cache_capacity: 100
  query_cache_similarity_threshold: 0.52
  hot_tier_max_age_secs: 600
```

Notes:

- `cache.capacity` is used to size the cache and hot tier.
- `cache.hot_tier_max_age_secs` controls hot-tier max age; if omitted, the server falls back to `cache.training_interval_secs` for backward compatibility.
- `cache.training_interval_secs` controls the training cadence for the learned predictor.
- `cache.strategy` is used by `kyrodb_server` (lru/learned/abtest).
- `cache.query_cache_similarity_threshold=0.52` is an intentionally recall-oriented default for semantic query cache reuse; if you observe false semantic matches, increase toward `0.70-0.80`.

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
  ann_search_mode: fp32_strict
  quantized_rerank_multiplier: 8
```

Notes:

- `ann_search_mode` controls cold-tier ANN traversal mode:
  - `fp32_strict` for strict full-precision scoring
  - `sq8_rerank` for SQ8 approximate traversal with fp32 rerank
- `quantized_rerank_multiplier` applies in quantized modes only, where rerank candidate count is `k * quantized_rerank_multiplier`.
- Validation enforces `hnsw.m` in `[5, 128]` and `hnsw.quantized_rerank_multiplier` in `[1, 64]`.

### Persistence (used)

```yaml
persistence:
  data_dir: "./data"
  wal_flush_interval_ms: 100
  fsync_policy: data_only
  snapshot_interval_mutations: 10000
  max_wal_size_bytes: 104857600
  enable_recovery: true
  allow_fresh_start_on_recovery_failure: false
```

Notes:

- `data_dir`, `wal_flush_interval_ms`, `fsync_policy`, `snapshot_interval_mutations`, `max_wal_size_bytes`, `enable_recovery`, and `allow_fresh_start_on_recovery_failure` are used.
- `snapshot_interval_mutations` is **operation-count based** (WAL mutations, including inserts and deletes); set to `0` to disable automatic snapshots. The legacy key `snapshot_interval_inserts` is accepted as a backward-compatible alias.

### SLO (used)

```yaml
slo:
  p99_latency_ms: 10.0
  cache_hit_rate: 0.70
  error_rate: 0.001
  availability: 0.999
  min_samples: 100
```

SLO thresholds are validated on startup and used by the metrics/health system:

- `/slo` reports breach status using these thresholds.
- `/health` and `/ready` incorporate SLO breach status after `slo.min_samples` queries (warmup protection).

### Rate limiting (partially wired)

```yaml
rate_limit:
  enabled: false
  max_qps_per_connection: 1000
  max_qps_global: 100000
  burst_capacity: 10000
```

Per-tenant rate limiting uses `max_qps` from the API keys file when authentication is enabled.

`max_qps_per_connection` and `max_qps_global` are **global defaults** that apply when a tenant does not provide a per-tenant override (i.e., `max_qps` omitted or set to `0`). Per-tenant `max_qps` in `api_keys.yaml` overrides the global defaults for that tenant. When `rate_limit.enabled=true`, the server also enforces the global QPS cap (`max_qps_global`) across all tenants; when disabled, tenants without `max_qps` are effectively unlimited.

Example:

```yaml
# data/api_keys.yaml
api_keys:
  - key: kyro_acme_corp_a3f9d8e2c1b4567890abcdef12345678
    tenant_id: acme_corp
    max_qps: 500
```

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

### Authentication (used)

```yaml
auth:
  enabled: false
  api_keys_file: "data/api_keys.yaml"
```

Notes:

- `enabled` and `api_keys_file` are used by `kyrodb_server`.

### Timeouts (used)

```yaml
timeouts:
  cache_ms: 10
  hot_tier_ms: 50
  cold_tier_ms: 1000
```

Notes:

- Timeout values are per-layer fail-safe ceilings, not SLO targets.
- `slo.p99_latency_ms` is measured end-to-end query latency; keep your hit-rate profile high enough that cold-tier worst-case budgets are rare in steady state.

### Environment mode (used)

```yaml
environment:
  type: production # production | pilot | benchmark
```

Modes:

- `production` (default): standard runtime validation and production-safe persistence checks.
- `pilot`: stricter launch guardrails for external pilots.
- `benchmark`: allows unsafe persistence settings for benchmark throughput experiments.

`environment.type=pilot` enables additional startup validation to prevent insecure pilot configs.

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

Common arguments:

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
- `environment.type=pilot` requires:
  - `auth.enabled=true`
  - `rate_limit.enabled=true`
  - `server.observability_auth != disabled`
  - `allow_fresh_start_on_recovery_failure=false`
  - TLS enabled for non-loopback gRPC hosts
- `environment.type=production` with non-loopback bind requires:
  - `auth.enabled=true` when `server.host` is non-loopback
  - `server.observability_auth != disabled` when observability HTTP bind is non-loopback (`server.http_host` or `server.host`)

These guardrails are coupled: `environment.type=pilot`/`production` checks evaluate `auth.enabled`, `rate_limit.enabled`, `server.observability_auth`, `server.http_host`, and `server.tls.*` together to prevent insecure remote exposure.

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
KYRODB__PERSISTENCE__FSYNC_POLICY=data_only ./kyrodb_server
```

`fsync_policy=none` is benchmark-only and is rejected when `environment.type` is `production` or `pilot`.
