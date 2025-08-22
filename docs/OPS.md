# KyroDB Ops Guide

## Install
- Binary: download from GitHub Releases (kyrodb-engine). Place at /usr/local/bin.
- Docker: docker run -p 3030:3030 kyrodb/kyrodb-engine:latest
- Systemd: see docs/systemd/kyrodb-engine.service

## Run
- Data dir: /var/lib/kyrodb (set --data-dir)
- Auth: --auth-token TOKEN
- Rotation: --wal-segment-bytes, --wal-max-segments
- Compaction: --compact-interval-secs, --compact-when-wal-bytes, or --wal-max-bytes

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

## Tuning
- RMI tuning via KYRODB_RMI_TARGET_LEAF, KYRODB_RMI_EPS_MULT
- Payload cache: default LRU 4096 entries
- CPU pinning and governor for stable tail latencies
