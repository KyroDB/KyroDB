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
- `GET /usage` (per-tenant usage snapshots; auth required when enabled)

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

## Restarting the Server

Common restart commands (choose the one that matches your deployment):

```bash
# systemd (replace service name)
sudo systemctl restart kyrodb.service

# Docker (single container)
docker restart <container>

# Docker Compose
docker compose restart <service>
# or: docker-compose restart <service>

# Process manager
pm2 restart <name>
# or: kill -HUP <pid>  # if your manager supports reload on HUP
```

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

#### Moving the data directory safely

1. Stop the server process (systemd/Docker/process manager).
2. Create a backup (recommended) or copy the directory as a fallback.
3. Move `persistence.data_dir` to the new location (fast SSD / local NVMe).
4. Update your config (`persistence.data_dir`) or pass `--data-dir <PATH>` / `KYRODB_DATA_DIR`.
5. Verify the move completed:
   - Compare file counts: `find <OLD_DIR> -type f | wc -l` vs `find <NEW_DIR> -type f | wc -l`
   - Check permissions: `ls -la <NEW_DIR>/` — the server process must have read/write access.
   - Verify the MANIFEST file exists: `test -f <NEW_DIR>/MANIFEST && echo OK`
6. Restart the server and verify `/health`, `/ready`, and logs. If validation fails, revert using the backup.

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

Use the backup tool to restore a known-good state. See the [backup guide](BACKUP_AND_RECOVERY.md) for full recovery workflows.

> **DESTRUCTIVE OPERATION WARNING**: Setting `BACKUP_ALLOW_CLEAR=true` enables the restore command to **permanently delete all existing data** in the target `--data-dir`. This action is irreversible. Before running restore with this flag:
> 1. Ensure you have a verified, current backup of the data directory.
> 2. Confirm that losing all data in the target directory is acceptable.
> 3. Understand that the directory contents will be fully replaced by the backup being restored.
>
> Only use `BACKUP_ALLOW_CLEAR=true` when data loss in the target directory is intentional.

```bash
# List available backups. Use --format json to get machine-parseable output.
kyrodb_backup list --backup-dir ./backups --format json

# Restore a specific backup. Extract the backup ID from the list output above
# (the "id" field in JSON output, e.g. "20250601T120000Z").
export BACKUP_ALLOW_CLEAR=true
kyrodb_backup restore --backup-id <BACKUP_ID> --data-dir ./data --backup-dir ./backups
```

If you remove WAL files to force snapshot-only recovery, recent writes will be lost. Use this only as a last resort.

## Backup failed (no space left on device)

```bash
du -sh ./backups
kyrodb_backup prune --keep-daily 7 --keep-weekly 4 --data-dir ./data --backup-dir ./backups
```

When built without the `cli-tools` feature, `kyrodb_backup list` only supports JSON output—use `--format json`. Enabling `cli-tools` adds the human-readable formats.

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
