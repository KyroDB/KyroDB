# KyroDB Ops Guide

## Install
- Binary: download from GitHub Releases (kyrodb-engine). Place at /usr/local/bin.
- Docker: docker run -p 3030:3030 kyrodb/kyrodb-engine:latest
- Systemd: see docs/systemd/kyrodb-engine.service

## Run
- Data dir: /var/lib/kyrodb (set --data-dir)
- Auth: --auth-token TOKEN (read/write access), --admin-token TOKEN (admin access)
- TLS: --tls-cert /path/to/cert.pem --tls-key /path/to/key.pem (enables HTTPS)
- Rotation: --wal-segment-bytes, --wal-max-segments
- Compaction: --compact-interval-secs, --compact-when-wal-bytes, or --wal-max-bytes

## Security Configuration

### TLS/HTTPS Setup
```bash
# Generate self-signed certificate for testing
openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem -days 365 -nodes -subj "/CN=localhost"

# Run with TLS
./kyrodb-engine serve 0.0.0.0 3030 --tls-cert cert.pem --tls-key key.pem
```

### Authentication & Authorization
- **Read-Only Access**: No token required (when no auth configured)
- **Read/Write Access**: Use `--auth-token` for read/write operations
- **Admin Access**: Use `--admin-token` for privileged operations (snapshot, compaction, etc.)

```bash
# Example with role-based access
./kyrodb-engine serve 0.0.0.0 3030 \
  --auth-token "rw-token-123" \
  --admin-token "admin-token-456"
```

### API Permissions by Role
- **Admin**: All operations (snapshot, compact, RMI rebuild, etc.)
- **Read/Write**: Data operations (put, append, vector insert, SQL)
- **Read-Only**: Read operations (lookup, get, offset, subscribe)

## Backup
- Snapshot files: snapshot.bin, snapshot.data, manifest.json, index-rmi.bin
- Steps:
  1) curl -X POST :3030/v1/snapshot
  2) Stop writes or accept a short RPO
  3) Copy files atomically (same directory)
  4) Verify RMI checksum (manifest lists files)

## Restore
- Place files into data dir
- Start server; WAL is reset post-snapshot; manifest is commit point

## Metrics
- Scrape /metrics (Prometheus exposition). Key series:
  - kyrodb_appends_total
  - kyrodb_rmi_reads_total, kyrodb_btree_reads_total
  - kyrodb_compactions_total, kyrodb_snapshot_latency_seconds

## Build Info & Provenance
- Endpoint: GET /build_info
- Includes: commit hash, branch, build time, Rust version, target triple, enabled features

## Tuning
- RMI tuning via KYRODB_RMI_TARGET_LEAF, KYRODB_RMI_EPS_MULT
- Payload cache: default LRU 4096 entries
- CPU pinning and governor for stable tail latencies
