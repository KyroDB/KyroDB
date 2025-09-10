# KyroDB Ops Guide

## Install
- Binary: GitHub Releases (`kyrodb-engine`) → place at /usr/local/bin
- Docker: `docker run -p 3030:3030 kyrodb/kyrodb-engine:latest`
- Systemd: see `docs/systemd/kyrodb-engine.service`

## Run
- Data dir: `/var/lib/kyrodb` (set via global `--data-dir` before `serve`)
- Auth (optional): `--auth-token` (rw), `--admin-token` (admin). Omit for benchmarks.
- TLS (optional): `--tls-cert`, `--tls-key`
- Rotation: `--wal-segment-bytes`, `--wal-max-segments`
- Compaction: `--compact-interval-secs`, `--compact-when-wal-bytes`, or `--wal-max-bytes`

## Security Configuration

### TLS/HTTPS Setup
```bash
openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem -days 365 -nodes -subj "/CN=localhost"
./kyrodb-engine --tls-cert cert.pem --tls-key key.pem serve 0.0.0.0 3030
```

### Authentication & Authorization (Optional)
- Read‑only: no token required (when not configured)
- Read/Write: `--auth-token` for data operations
- Admin: `--admin-token` for privileged ops (snapshot, compaction, etc.)

```bash
./kyrodb-engine serve 0.0.0.0 3030 \
  --auth-token "rw-token-123" \
  --admin-token "admin-token-456"
```

## Backup
- Snapshot artifacts: `snapshot.bin`, `snapshot.data`, `manifest.json`, `index-rmi.bin`
- Steps:
  1) `POST :3030/v1/snapshot`
  2) Optionally pause writes for strict RPO=0
  3) Copy all files atomically in the data dir
  4) Verify RMI checksum (listed in manifest)

## Restore
- Place snapshot artifacts into the data dir
- Start server; WAL tail replays; manifest is the commit point

## Metrics & Build Info
- Prometheus: scrape `/metrics`
  - `kyrodb_appends_total`
  - `kyrodb_rmi_reads_total`, `kyrodb_rmi_probe_length_histogram`
  - `kyrodb_compactions_total`, `kyrodb_snapshot_duration_seconds`
- Build info: `/build_info` (commit, features, rustc, target)

## Tuning & Bench Policy
- RMI: `KYRODB_RMI_TARGET_LEAF`, `KYRODB_RMI_EPS_MULT`
- Cache: payload cache size (LRU) and memory budget
- CPU: pinning/governor for stable tails
- Benchmarks: disable auth/rate limit; enable `bench-no-metrics`; warm via snapshot → RMI → `/v1/warmup`

## API Versioning
- Stable today: `/v1/*`
- Planned Phase 1: `/v2/*` (hybrid queries, streaming ingest)
