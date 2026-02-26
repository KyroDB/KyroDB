# Observability

## Purpose

Define observability endpoints, auth behavior, and metric names.

## Scope

- `/health`, `/ready`, `/slo`, `/metrics`, `/usage`
- endpoint auth behavior
- Prometheus scrape pattern
- primary operational metrics

## Commands

```bash
./target/release/kyrodb_server --generate-config toml > config.toml
./target/release/kyrodb_server --config config.toml

curl http://127.0.0.1:51051/health
curl http://127.0.0.1:51051/ready
curl http://127.0.0.1:51051/slo
curl http://127.0.0.1:51051/metrics

curl -H "x-api-key: <API_KEY>" http://127.0.0.1:51051/usage
curl -H "authorization: Bearer <API_KEY>" http://127.0.0.1:51051/usage
curl -H "x-api-key: <ADMIN_API_KEY>" "http://127.0.0.1:51051/usage?scope=all"
```

## Key Contracts

### Endpoint Auth

- `server.observability_auth` controls auth requirements for `/metrics`, `/health`, `/ready`, `/slo`
- accepted values:
  - `disabled`: all four endpoints are publicly accessible (no auth required)
  - `metrics_and_slo`: auth is required for `/metrics` and `/slo`; `/health` and `/ready` remain open
  - `all`: auth is required for `/metrics`, `/health`, `/ready`, and `/slo`
- in this config, "enabled/protected" means authentication is mandatory for the affected endpoint(s), not optional
- `/usage` is controlled by `auth.enabled`
  - `auth.enabled=false` => `404`
  - `auth.enabled=true` => API key required
  - `scope=all` => admin key required

### Response Shapes

- `/health`: `{"status":"healthy|unhealthy", ...}`
- `/ready`: `{"ready":true|false, ...}`
- `/slo`: `current_metrics`, `slo_breaches`, `slo_thresholds`
- `/metrics`: Prometheus text format

### Metric Names

- `kyrodb_queries_total`
- `kyrodb_queries_failed`
- `kyrodb_query_latency_ns{percentile="50|95|99"}`
- `kyrodb_hnsw_latency_ns`
- `kyrodb_cache_hits_total`
- `kyrodb_cache_misses_total`
- `kyrodb_cache_hit_rate`
- `kyrodb_cache_size`
- `kyrodb_tier_hits_total{tier="hot|cold"}`
- `kyrodb_wal_circuit_breaker_state`
- `kyrodb_wal_writes_failed`
- `kyrodb_errors_total{category="validation|timeout|internal|resource_exhausted"}`
- `kyrodb_memory_used_bytes`
- `kyrodb_disk_used_bytes`
- `kyrodb_ready`

## Prometheus Example

```yaml
scrape_configs:
  - job_name: kyrodb
    static_configs:
      - targets: ["127.0.0.1:51051"]
    metrics_path: /metrics
    scrape_interval: 15s
```

## Related Docs

- [API_REFERENCE.md](API_REFERENCE.md)
- [AUTHENTICATION.md](AUTHENTICATION.md)
- [OPERATIONS.md](OPERATIONS.md)
