# Observability Guide

Monitor KyroDB in production with Prometheus metrics and health checks.

## Quick Setup

**1. Start KyroDB (metrics enabled by default):**
```bash
./target/release/kyrodb_server
```

**2. Check metrics endpoint:**
```bash
curl http://localhost:51052/metrics
```

**3. Add to Prometheus:**
```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'kyrodb'
    static_configs:
      - targets: ['localhost:51052']
    metrics_path: '/metrics'
    scrape_interval: 15s
```

**4. View in Grafana** (see dashboard queries below).

## Key Metrics

### Query Performance
```promql
# Queries per second
rate(kyrodb_queries_total[1m])

# P99 latency (milliseconds)
kyrodb_query_latency_p99

# Error rate (percent)
rate(kyrodb_queries_failed[5m]) / rate(kyrodb_queries_total[5m]) * 100
```

### Cache Performance
```promql
# Cache hit rate (percent)
kyrodb_cache_hit_rate * 100

# Cache evictions per minute
rate(kyrodb_cache_evictions_total[1m])

# Hybrid Semantic Cache prediction accuracy
kyrodb_learned_cache_accuracy * 100
```

### Resource Usage
```promql
# Memory usage (MB)
kyrodb_memory_used_bytes / 1024 / 1024

# Disk usage (GB)
kyrodb_disk_used_bytes / 1024 / 1024 / 1024

# Active connections
kyrodb_active_connections
```

---

## Health Checks

### Liveness Probe (K8s/Docker)

Check if server is alive:
```bash
curl http://localhost:51052/health
```

**Response (healthy):**
```json
{"status": "healthy", "uptime_seconds": 3600}
```

**Response (unhealthy):**
```json
{
  "status": "unhealthy",
  "reason": "P99 latency 15ms exceeds 10ms SLO",
  "uptime_seconds": 3600
}
```

**K8s config:**
```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 51052
  initialDelaySeconds: 10
  periodSeconds: 10
  failureThreshold: 3
```

---

### Readiness Probe (Load Balancer)

Check if server should receive traffic:
```bash
curl http://localhost:51052/ready
```

**Response:**
```json
{"ready": true, "status": "ready"}
```

**K8s config:**
```yaml
readinessProbe:
  httpGet:
    path: /ready
    port: 51052
  initialDelaySeconds: 5
  periodSeconds: 5
  failureThreshold: 2
```

---

### SLO Status

Check SLO breach status:
```bash
curl http://localhost:51052/slo
```

**Response:**
```json
{
  "status": "ok",
  "metrics": {
    "p99_latency_ms": 2.5,
    "cache_hit_rate": 0.85,
    "error_rate_5m": 0.0001
  },
  "thresholds": {
    "p99_latency_ms": 10.0,
    "min_cache_hit_rate": 0.70,
    "max_error_rate_5m": 0.001
  },
  "breaches": []
}
```

**SLO Targets:**
- P99 latency: < 10ms
- Cache hit rate: > 70%
- Error rate: < 0.1%
- Availability: > 99.9%

---

## Prometheus Alerts

Create alerts for SLO breaches:

```yaml
# alerts.yml
groups:
- name: kyrodb_slo
  rules:
  # High latency
  - alert: KyroDBHighLatency
    expr: kyrodb_query_latency_p99 > 10
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "P99 latency {{ $value }}ms exceeds 10ms SLO"

  # Low cache hit rate
  - alert: KyroDBLowCacheHit
    expr: kyrodb_cache_hit_rate < 0.70
    for: 10m
    labels:
      severity: warning
    annotations:
      summary: "Cache hit rate {{ $value | humanizePercentage }} below 70%"

  # High error rate
  - alert: KyroDBHighErrors
    expr: rate(kyrodb_queries_failed[5m]) / rate(kyrodb_queries_total[5m]) > 0.001
    for: 5m
    labels:
      severity: critical
    annotations:
      summary: "Error rate {{ $value | humanizePercentage }} exceeds 0.1%"

  # Server not ready
  - alert: KyroDBNotReady
    expr: kyrodb_ready == 0
    for: 2m
    labels:
      severity: critical
    annotations:
      summary: "Server not ready for 2+ minutes"
```

**Connect to PagerDuty:**
```yaml
# alertmanager.yml
receivers:
- name: 'pagerduty'
  pagerduty_configs:
  - service_key: '<YOUR_KEY>'
    severity: '{{ .CommonLabels.severity }}'
```

---

## Grafana Dashboard

**Import these panels:**

### Query Performance Panel
```json
{
  "title": "Query Performance",
  "targets": [
    {
      "expr": "rate(kyrodb_queries_total[1m])",
      "legendFormat": "QPS"
    },
    {
      "expr": "kyrodb_query_latency_p99",
      "legendFormat": "P99 Latency (ms)"
    }
  ]
}
```

### Cache Performance Panel
```json
{
  "title": "Cache Performance",
  "targets": [
    {
      "expr": "kyrodb_cache_hit_rate * 100",
      "legendFormat": "Hit Rate %"
    },
    {
      "expr": "kyrodb_learned_cache_accuracy * 100",
      "legendFormat": "Prediction Accuracy %"
    }
  ]
}
```

### Resource Usage Panel
```json
{
  "title": "Resource Usage",
  "targets": [
    {
      "expr": "kyrodb_memory_used_bytes / 1024 / 1024",
      "legendFormat": "Memory (MB)"
    },
    {
      "expr": "kyrodb_disk_used_bytes / 1024 / 1024 / 1024",
      "legendFormat": "Disk (GB)"
    }
  ]
}
```

---

## Logging

KyroDB logs in JSON format for easy parsing.

**Enable logging:**
```bash
export RUST_LOG=kyrodb_engine=info
./target/release/kyrodb_server
```

**Log levels:**
- `error`: Critical failures
- `warn`: Performance warnings
- `info`: Normal operations (default)
- `debug`: Detailed debugging
- `trace`: Ultra-verbose (not for production)

**Example logs:**
```json
{"timestamp":"2025-10-15T10:23:45Z","level":"INFO","message":"Query executed","latency_ms":2.5,"doc_id":"doc_123"}
{"timestamp":"2025-10-15T10:23:46Z","level":"WARN","message":"Cache miss","doc_id":"doc_456"}
{"timestamp":"2025-10-15T10:24:00Z","level":"INFO","message":"Flush completed","docs_flushed":1000}
```

**Forward to Datadog/Splunk:**
```bash
# Pipe JSON logs to log shipper
./target/release/kyrodb_server 2>&1 | datadog-agent
```

---

## Troubleshooting

### No metrics showing

**Symptom:** `/metrics` endpoint empty

**Fix:**
1. Check server is running: `curl http://localhost:51052/health`
2. Verify metrics not disabled: ensure `bench-no-metrics` feature NOT enabled
3. Send some queries to generate metrics
4. Check logs: `RUST_LOG=kyrodb_engine=debug`

### High P99 latency

**Symptom:** `kyrodb_query_latency_p99 > 10ms`

**Fix:**
1. Check cache hit rate: `curl http://localhost:51052/metrics | grep cache_hit_rate`
   - If < 70%: cache misses forcing slow HNSW searches
2. Check HNSW latency: `grep hnsw_latency_p99`
   - If high: HNSW index may need rebuild or disk is slow
3. Check memory: `grep memory_used_bytes`
   - If near limit: increase cache size in config
4. Review query patterns: too many cold/unique queries?

### Low cache hit rate

**Symptom:** `kyrodb_cache_hit_rate < 0.70`

**Fix:**
1. Check prediction accuracy: `grep learned_cache_accuracy`
   - If < 80%: cache predictor needs more training data
2. Wait for training: Predictor trains every 10 minutes
3. Check cache size: `grep cache_size`
   - If 0: cache not initialized, check logs for errors
4. Review workload: High % of unique queries will lower hit rate

### Health check failing

**Symptom:** `/health` returns 503

**Fix:**
1. Check SLO status: `curl http://localhost:51052/slo`
2. Identify breached metric (latency/cache/errors)
3. Follow fix for that metric above
4. Restart if "unhealthy" state persists after fix

---

## Production Checklist

Before going live:

- [ ] Prometheus scraping `/metrics` every 15s
- [ ] Grafana dashboard created and visible to ops team
- [ ] PagerDuty alerts configured for SLO breaches
- [ ] K8s liveness/readiness probes configured
- [ ] Log forwarding to Datadog/Splunk enabled
- [ ] Baseline metrics captured during load test
- [ ] Runbook includes metric interpretation (see [Operations Guide](OPERATIONS.md))

---

## Performance Overhead

Metrics collection is ultra-low overhead:

- **Query latency**: +17.6ns per query (0.0018% at 1ms baseline)
- **Insert latency**: +10.2ns per insert
- **Memory**: ~8KB + 8 bytes per latency sample (max 1000)

**Disable for benchmarking:**
```bash
cargo build --release --features bench-no-metrics
```
