# Operations

## Purpose

Provide the day-2 runbook for KyroDB service and backup operations.

## Scope

- baseline health checks
- backup/restore operations
- common symptom diagnostics
- daily/weekly operational checks

## Commands

```bash
# Baseline probes
curl -s http://127.0.0.1:51051/health
curl -s http://127.0.0.1:51051/ready
curl -s http://127.0.0.1:51051/slo

# Usage endpoint checks (status-code oriented)
# auth disabled -> expected 404
curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:51051/usage
# auth enabled, no key -> expected 401
curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:51051/usage
# auth enabled, non-admin key requesting all tenants -> expected 403
curl -s -o /dev/null -w "%{http_code}\n" \
  -H "x-api-key: <API_KEY>" "http://127.0.0.1:51051/usage?scope=all"
# auth enabled, tenant key -> expected 200
curl -s -o /dev/null -w "%{http_code}\n" \
  -H "x-api-key: <API_KEY>" http://127.0.0.1:51051/usage

# Core metrics checks
curl -s http://127.0.0.1:51051/metrics | grep kyrodb_query_latency_ns
curl -s http://127.0.0.1:51051/metrics | grep kyrodb_cache_hit_rate
curl -s http://127.0.0.1:51051/metrics | grep kyrodb_wal_circuit_breaker_state

# Backup listing
./target/release/kyrodb_backup --data-dir ./data --backup-dir ./backups list --format json

# Backup verification
./target/release/kyrodb_backup --data-dir ./data --backup-dir ./backups verify <BACKUP_ID>

# Restore (destructive)
export BACKUP_ALLOW_CLEAR=true
./target/release/kyrodb_backup --data-dir ./data --backup-dir ./backups restore --backup-id <BACKUP_ID>

# Retention pruning
./target/release/kyrodb_backup --data-dir ./data --backup-dir ./backups prune --keep-daily 7 --keep-weekly 4 --keep-monthly 12 --min-age-days 1
```

## Key Contracts

- `/usage` status codes:
  - `404` when auth disabled
  - `401` when auth enabled but no/invalid key
  - `403` on `scope=all` for non-admin keys
- backup restore requires explicit `BACKUP_ALLOW_CLEAR=true`
- incremental backup requires new WAL since parent

## Symptom Guide

### High latency

Check:

- `kyrodb_query_latency_ns`
- `kyrodb_hnsw_latency_ns`
- `kyrodb_cache_hit_rate`

### Sudden INTERNAL gRPC errors with panic logs

Check:

- server logs for `gRPC handler panicked; returning INTERNAL and continuing`
- request payload quality (non-finite embeddings are rejected)
- recent deploy changes for panic-triggering handler paths

### WAL breaker open

Check:

- `kyrodb_wal_circuit_breaker_state`
- `kyrodb_wal_writes_failed`

### Backup failures

Check:

- data directory recoverable state (manifest/snapshot/WAL)
- backup storage free space
- backup ID validity and chain continuity

## Related Docs

- [OBSERVABILITY.md](OBSERVABILITY.md)
- [BACKUP_AND_RECOVERY.md](BACKUP_AND_RECOVERY.md)
- [AUTHENTICATION.md](AUTHENTICATION.md)
