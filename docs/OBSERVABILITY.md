# Observability Guide

Monitor KyroDB with Prometheus metrics and health checks.

## Quick setup

1. Start KyroDB (metrics enabled by default):

```bash
./target/release/kyrodb_server
```

2. Check metrics endpoint (HTTP port defaults to gRPC port + 1000):

```bash
curl http://localhost:51051/metrics
```

3. Add to Prometheus:

```yaml
# prometheus.yml
scrape_configs:
  - job_name: "kyrodb"
    static_configs:
      - targets: ["localhost:51051"]
    metrics_path: "/metrics"
    scrape_interval: 15s
```

## Key metrics

### Query performance

```promql
rate(kyrodb_queries_total[1m])
kyrodb_query_latency_ns{percentile="99"} / 1000000
```

Error rate (safe against divide-by-zero when `kyrodb_queries_total` has no samples):

```promql
# Safe pattern: clamp_min prevents division by zero
rate(kyrodb_queries_failed[5m]) / clamp_min(rate(kyrodb_queries_total[5m]), 1e-10) * 100
```

> **Note**: The raw `rate(kyrodb_queries_failed[5m]) / rate(kyrodb_queries_total[5m]) * 100` will return `NaN` when there are no query samples. The `clamp_min` wrapper ensures the denominator is never zero.

### Cache performance

```promql
kyrodb_cache_hit_rate * 100
rate(kyrodb_cache_evictions_total[1m])
kyrodb_cache_size
```

### HNSW and tiers

```promql
rate(kyrodb_hnsw_searches_total[1m])
kyrodb_hnsw_latency_ns
kyrodb_tier_hits_total{tier="hot"}
kyrodb_tier_hits_total{tier="cold"}
```

### Errors and WAL

```promql
kyrodb_errors_total{category="validation"}
kyrodb_errors_total{category="timeout"}
kyrodb_errors_total{category="internal"}
kyrodb_errors_total{category="resource_exhausted"}
kyrodb_wal_circuit_breaker_state
kyrodb_wal_writes_failed
```

## Health checks

### Liveness

```bash
curl http://localhost:51051/health
```

Healthy response:

```json
{"status":"healthy","uptime_seconds":3600}
```

Unhealthy response (example):

```json
{"status":"unhealthy","reason":"P99 latency 1500000ns exceeds SLO","uptime_seconds":3600}
```

K8s configuration:

```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 51051
  initialDelaySeconds: 10
  periodSeconds: 10
  failureThreshold: 3
```

### Readiness

```bash
curl http://localhost:51051/ready
```

```json
{"ready":true,"status":"ready"}
```

> Both `ready` (boolean) and `status` (string) are intentionally present: `ready` is the canonical programmatic field for automated health checks (K8s probes, load balancers), while `status` provides a human-readable label useful in dashboards and logs. Consumers should key on the `ready` boolean for readiness decisions.

K8s configuration:

```yaml
readinessProbe:
  httpGet:
    path: /ready
    port: 51051
  initialDelaySeconds: 5
  periodSeconds: 5
  failureThreshold: 2
```

### SLO status

```bash
curl http://localhost:51051/slo
```

Example response:

```json
{
  "current_metrics": {
    "availability": 1.0,
    "cache_hit_rate": 0.85,
    "error_rate": 0.0,
    "p99_latency_ns": 250000
  },
  "slo_breaches": {
    "availability": false,
    "cache_hit_rate": false,
    "error_rate": false,
    "p99_latency": false
  },
  "slo_thresholds": {
    "max_error_rate": 0.001,
    "min_availability": 0.999,
    "min_cache_hit_rate": 0.7,
    "p99_latency_ns": 10000000
  }
}
```

SLO targets:

- P99 latency: < 10ms
- Cache hit rate: > 70%
- Error rate: < 0.1%
- Availability: > 99.9%

### Usage snapshots (billing/ops)

Per-tenant usage is available at `GET /usage` when `auth.enabled=true`.
Authentication setup (API keys file, key format, and headers) is documented in [Authentication and multi-tenancy](AUTHENTICATION.md).
`/usage` accepts either `x-api-key: <key>` or `authorization: Bearer <key>`.
When `auth.enabled=false`, usage tracking is disabled and `/usage` returns `404`.

```bash
curl -H "x-api-key: <API_KEY>" http://localhost:51051/usage
```

## Prometheus alerts

```yaml
# alerts.yml
groups:
- name: kyrodb_slo
  rules:
  - alert: KyroDBHighLatency
    expr: kyrodb_query_latency_ns{percentile="99"} > 10000000
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "P99 latency {{ $value | humanizeDuration }} exceeds 10ms SLO"

  - alert: KyroDBLowCacheHit
    expr: kyrodb_cache_hit_rate < 0.70
    for: 10m
    labels:
      severity: warning
    annotations:
      summary: "Cache hit rate {{ $value | humanizePercentage }} below 70%"

  - alert: KyroDBHighErrors
    expr: rate(kyrodb_queries_failed[5m]) / rate(kyrodb_queries_total[5m]) > 0.001
    for: 5m
    labels:
      severity: critical
    annotations:
      summary: "Error rate {{ $value | humanizePercentage }} exceeds 0.1%"

  - alert: KyroDBNotReady
    expr: kyrodb_ready == 0
    for: 2m
    labels:
      severity: critical
    annotations:
      summary: "Server not ready for 2+ minutes"
```

## Grafana panels (examples)

### Query performance

```json
{
  "title": "Query Performance",
  "targets": [
    {"expr": "rate(kyrodb_queries_total[1m])", "legendFormat": "QPS"},
    {"expr": "kyrodb_query_latency_ns{percentile=\"99\"} / 1000000", "legendFormat": "P99 (ms)"}
  ]
}
```

### Cache performance

```json
{
  "title": "Cache Performance",
  "targets": [
    {"expr": "kyrodb_cache_hit_rate * 100", "legendFormat": "Hit Rate %"},
    {"expr": "rate(kyrodb_cache_evictions_total[1m])", "legendFormat": "Evictions/min"}
  ]
}
```

## Logging

Logging defaults to text format. Configure JSON output in the config file.

KyroDB does not auto-discover config files. Point the server at the file explicitly with `--config <path>` or set `KYRODB_CONFIG`. Example:

```bash
./target/release/kyrodb_server --config ./config.yaml
# or: KYRODB_CONFIG=./config.yaml ./target/release/kyrodb_server
```

Edit the `logging:` block in that file to switch formats (e.g., `format: json`).

```yaml
logging:
  level: info
  format: json
  file: null
  rotation: true
  max_file_size_bytes: 104857600
  max_files: 10
```

You can override module levels via `RUST_LOG`:

```bash
RUST_LOG=kyrodb_engine=debug,kyrodb_server=info ./target/release/kyrodb_server
```

## Troubleshooting

### No metrics showing

1. Confirm the server is running: `curl http://localhost:51051/health`
2. Ensure the build does not use `bench-no-metrics`
3. Send queries to generate metrics
4. Increase log level with `RUST_LOG`

### High P99 latency

1. Check `kyrodb_query_latency_ns{percentile="99"}`
2. Check cache hit rate and tier hits
3. Inspect `kyrodb_hnsw_latency_ns` for cold-tier pressure

### Low cache hit rate

1. Confirm cache capacity is adequate for your working set
2. Ensure queries repeatedly hit the same document IDs

### Health check failing

1. Check `/slo` for the breached metric
2. Investigate the corresponding metric in `/metrics`

## Production checklist

- Prometheus scrapes `/metrics` every 15s
- Alerts configured for SLO breaches
- Liveness/readiness probes configured
- Log aggregation enabled
- Baseline metrics captured during load tests
- Runbook includes the operations guide

## Disable metrics for benchmarks

```bash
cargo build --release --features bench-no-metrics
```
