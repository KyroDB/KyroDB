# Operations Guide

Practical guidance for running the KyroDB gRPC server and backup tooling.

## Ports and endpoints

- gRPC server: `server.port` (default: 50051)
- HTTP observability: `server.http_port` (default: gRPC port + 1000)

HTTP endpoints:

- `GET /health` (liveness)
- `GET /ready` (readiness)
- `GET /metrics` (Prometheus)
- `GET /slo` (SLO breach status and current values)

## Quick checks

```bash
curl -s http://localhost:51051/health
curl -s http://localhost:51051/ready
curl -s http://localhost:51051/slo
```

Key metrics (all available via `/metrics`):

- `kyrodb_queries_total`
- `kyrodb_queries_failed`
- `kyrodb_query_latency_ns{percentile="50|95|99"}`
- `kyrodb_cache_hit_rate`
- `kyrodb_cache_hits_total`
- `kyrodb_cache_misses_total`
- `kyrodb_hnsw_searches_total`
- `kyrodb_hnsw_latency_ns`
- `kyrodb_tier_hits_total{tier="hot|cold"}`
- `kyrodb_wal_circuit_breaker_state`
- `kyrodb_wal_writes_failed`
- `kyrodb_errors_total{category="validation|timeout|internal|resource_exhausted"}`
- `kyrodb_inserts_total`
- `kyrodb_inserts_failed`
- `kyrodb_ready`

## Health check fails (503)

### Diagnosis

```bash
ps aux | grep kyrodb_server
```

Check logs (stdout by default; file path if `logging.file` is set).

```bash
curl -s http://localhost:51051/slo
curl -s http://localhost:51051/metrics | grep -E "wal_circuit_breaker_state|queries_failed|errors_total"
```

### Fix

#### Disk full or I/O errors

```bash
df -h /var/lib/kyrodb
dmesg | grep -i "i/o error" | tail -20
```

Free space or move the data directory to faster storage, then restart the server.

#### WAL circuit breaker open

```bash
curl -s http://localhost:51051/metrics | grep kyrodb_wal_circuit_breaker_state
```

The circuit breaker transitions to half-open after 60 seconds when writes succeed. If it stays open, fix the underlying disk or permission issue and restart the server.

## P99 latency high

### Diagnosis

```bash
curl -s http://localhost:51051/metrics | grep kyrodb_query_latency_ns
curl -s http://localhost:51051/metrics | grep kyrodb_hnsw_latency_ns
curl -s http://localhost:51051/metrics | grep kyrodb_cache_hit_rate
curl -s http://localhost:51051/metrics | grep kyrodb_tier_hits_total
```

### Fix

- If cache hit rate is low, increase `cache.capacity` and ensure your workload reuses document IDs.
- If `kyrodb_hnsw_latency_ns` is high, reduce `hnsw.ef_search` to lower latency (recall may drop), or reduce `hnsw.max_elements` and `hnsw.m` to lower memory pressure.
- If disk I/O is saturated, move `persistence.data_dir` to faster storage.

## Out of memory (OOM)

### Diagnosis

```bash
dmesg | tail -20
```

Common causes are oversized cache or HNSW settings.

### Fix

Reduce memory usage in `config.yaml` or `config.toml`:

```yaml
cache:
  capacity: 50000
hnsw:
  max_elements: 100000
  m: 16
```

Restart the server after changes.

## Data corruption or failed recovery

Use the backup tool to restore a known-good state. See the backup guide for full recovery workflows.

```bash
kyrodb_backup list --backup-dir ./backups --format json
kyrodb_backup restore --backup-id <BACKUP_ID> --data-dir ./data --backup-dir ./backups
```

If you remove WAL files to force snapshot-only recovery, recent writes will be lost. Use this only as a last resort.

## Backup failed (no space left on device)

```bash
du -sh ./backups
kyrodb_backup prune --keep-daily 7 --keep-weekly 4 --data-dir ./data --backup-dir ./backups
```

If you do not build with the `cli-tools` feature, use `--format json` for `kyrodb_backup list`.

## Monitoring checklist

Daily:

```bash
curl -s http://localhost:51051/health
curl -s http://localhost:51051/ready
curl -s http://localhost:51051/slo
curl -s http://localhost:51051/metrics | grep kyrodb_query_latency_ns
curl -s http://localhost:51051/metrics | grep kyrodb_cache_hit_rate
curl -s http://localhost:51051/metrics | grep kyrodb_wal_circuit_breaker_state
```

Weekly:

```bash
kyrodb_backup verify --backup-id <BACKUP_ID> --data-dir ./data --backup-dir ./backups
```

## Escalation

| Severity | Condition | Response time | Action |
| --- | --- | --- | --- |
| P0 | Server down > 5 minutes | Immediate | Restart, restore from backup if needed |
| P0 | WAL circuit breaker open > 10 minutes | Immediate | Fix disk/I/O issue, restart |
| P1 | SLO breaches for > 5 minutes | 15 minutes | Investigate latency or error rate |
| P1 | OOM crash | 15 minutes | Reduce memory footprint, restart |
| P2 | Backup failures | 1 hour | Free space, re-run backup |

## Getting help

1. Check logs (stdout or `logging.file` if configured)
2. Check `/metrics` and `/slo`
3. Restore from backup if the system does not recover cleanly
