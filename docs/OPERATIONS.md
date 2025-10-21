# Operations Guide

Common failure scenarios and how to fix them.

## Health Check Failed

### Symptom
```bash
curl http://localhost:51052/health
# Returns: 503 Service Unavailable
```

### Diagnosis

**Step 1**: Check if process is running
```bash
ps aux | grep kyrodb_server
```

**Step 2**: Check logs
```bash
tail -f /var/log/kyrodb/server.log

# Look for:
# - "SLO breach" → Performance degraded
# - "circuit breaker open" → Write failures
# - "training crash" → RMI training failed
# - "panic" → Code bug
```

**Step 3**: Check metrics
```bash
curl -s http://localhost:51052/metrics | grep -E "error|breach|circuit"

# Key indicators:
# - kyrodb_wal_circuit_breaker_state = 1 (open) → Disk I/O issue
# - kyrodb_error_rate_5m > 0.01 → Too many errors
# - kyrodb_training_crashes_total > 0 → Training unstable
```

### Fix

#### Cause: Disk Full

```bash
# Check disk space
df -h /var/lib/kyrodb

# Free space by deleting old backups
rm -rf /var/lib/kyrodb/backups/old_*

# Or compact WAL
systemctl stop kyrodb
rm /var/lib/kyrodb/data/wal_*.wal  # Loses uncommitted writes!
systemctl start kyrodb
```

#### Cause: Circuit Breaker Open

```bash
# Check why WAL writes failed
dmesg | grep -i "i/o error" | tail -20

# Circuit breaker auto-resets after 60 seconds
# Wait and monitor:
watch -n 5 'curl -s http://localhost:51052/metrics | grep circuit_breaker'

# If doesn't reset: restart server
systemctl restart kyrodb
```

#### Cause: Training Crash

```bash
# Check crash count
curl -s http://localhost:51052/metrics | grep training_crashes

# Restart training task (not whole server)
kill -HUP $(pgrep kyrodb_server)

# Monitor logs for "training task restarted"
journalctl -f -u kyrodb | grep training
```

## P99 Latency High

### Symptom
```bash
curl -s http://localhost:51052/metrics | grep p99
# kyrodb_query_latency_p99 = 15.2  (>10ms threshold)
```

### Diagnosis

**Which layer is slow?**
```bash
curl -s http://localhost:51052/metrics | grep latency

# Compare:
# - kyrodb_cache_latency_p99 (Layer 1)
# - kyrodb_hot_tier_latency_p99 (Layer 2)  
# - kyrodb_hnsw_latency_p99 (Layer 3)
```

### Fix

#### Layer 1 (Cache) Slow

```bash
# Check cache hit rate
curl -s http://localhost:51052/metrics | grep cache_hit_rate

# If <40%: Cache not trained yet or too small
# Solution: Increase cache capacity

# Edit config.yaml:
# Workaround: Disable Hybrid Semantic Cache temporarily
cache:
  capacity: 10000
  strategy: LRU  # Disable Hybrid Semantic Cache until bug fixed

# Restart server
systemctl restart kyrodb
```

#### Layer 3 (HNSW) Slow

```bash
# Check vector count
curl -s http://localhost:51052/metrics | grep hnsw_vector_count

# If >10M vectors: Index too large for RAM
# Solution: Horizontal scaling (Phase 1 feature)
# Workaround: Increase HNSW ef_search parameter

# Edit config.yaml:
hnsw:
  ef_search: 100  # Was 50 (slower but more accurate)
```

#### Disk I/O Bottleneck

```bash
# Check I/O wait
iostat -x 1 5

# If %iowait >50%: Disk is bottleneck
# Solution: Move data to faster storage (SSD)

# Stop server
systemctl stop kyrodb

# Move data
rsync -av /var/lib/kyrodb/data /mnt/ssd/kyrodb/

# Update config
vim /etc/kyrodb/config.yaml  # data_dir: /mnt/ssd/kyrodb/data

# Restart
systemctl start kyrodb
```

## Out of Memory (OOM)

### Symptom
```bash
# Server crashes with exit code 137
dmesg | tail -20
# Out of memory: Killed process kyrodb_server
```

### Diagnosis

```bash
# Check memory usage before crash
grep kyrodb /var/log/syslog | grep -i "out of memory"

# Common causes:
# - cache_capacity too large
# - training_window_size too large  
# - HNSW index >available RAM
```

### Fix

**Immediate**: Reduce memory usage

```bash
# Edit config.yaml
cache:
  capacity: 50000  # Reduce from 100000

training:
  window_duration_secs: 300  # Reduce from 86400 (1 day → 5 min)

access_logger:
  window_size: 50000  # Reduce from 100000
```

**Calculate safe limits**:
```bash
# Available RAM
FREE_RAM=$(free -g | awk 'NR==2 {print $7}')

# Safe cache capacity (20% of RAM)
# Assuming 1KB per cached document
SAFE_CACHE=$(( FREE_RAM * 1024 * 1024 * 20 / 100 / 1024 ))

echo "Set cache.capacity = $SAFE_CACHE"
```

**Long-term**: Increase RAM or scale horizontally.

## Data Corruption

### Symptom
```bash
# Server fails to start
tail -f /var/log/kyrodb/server.log
# Error: Snapshot checksum mismatch
# Error: WAL corrupted at entry 12345
```

### Diagnosis

```bash
# Check filesystem
sudo fsck -n /dev/sdX  # Non-destructive check

# Check disk hardware
sudo smartctl -H /dev/sdX

# If errors: Hardware failure
```

### Recovery

**Option 1**: Restore from backup
```bash
# Stop server
systemctl stop kyrodb

# Restore latest backup
kyrodb_backup restore \
  --backup-id $(kyrodb_backup list --json | jq -r '.[0].id') \
  --data-dir /var/lib/kyrodb/data \
  --allow-clear

# Start server
systemctl start kyrodb
```

**Option 2**: Truncate corrupted WAL (loses recent writes)
```bash
systemctl stop kyrodb

# Find last good WAL entry
kyrodb_server --data-dir /var/lib/kyrodb/data --verify-wal

# Truncate WAL at last good entry
truncate -s $(( LAST_GOOD_OFFSET )) /var/lib/kyrodb/data/wal_*.wal

systemctl start kyrodb
```

**Option 3**: Rebuild from snapshot only
```bash
systemctl stop kyrodb

# Delete corrupted WAL
rm /var/lib/kyrodb/data/wal_*.wal

# Server will start from last snapshot
systemctl start kyrodb
```

## Backup Failed

### Symptom
```bash
kyrodb_backup create-full --data-dir ./data --backup-dir ./backups
# Error: No space left on device
```

### Fix

```bash
# Check backup directory size
du -sh /backups

# Delete old backups
kyrodb_backup prune \
  --backup-dir /backups \
  --older-than-days 7

# Or move to cloud storage
aws s3 sync /backups s3://kyrodb-archive --storage-class GLACIER_IR
rm -rf /backups/*
```

## Slow Insert Performance

### Symptom
```bash
# Inserts taking >100ms
curl -s http://localhost:51052/metrics | grep insert_latency
# kyrodb_insert_latency_p99 = 150.5
```

### Diagnosis

```bash
# Check WAL fsync policy
curl -s http://localhost:51052/metrics | grep fsync

# If fsync policy = "DataOnly" or "DataAndManifest":
# Every write waits for disk sync (slow but safe)
```

### Fix

**Option 1**: Change fsync policy (less durable)
```yaml
# config.yaml
persistence:
  fsync_policy: Periodic  # Sync every 100 writes (faster)
```

**Option 2**: Use SSD for WAL
```bash
# Move WAL to faster storage
systemctl stop kyrodb
mv /var/lib/kyrodb/data/wal_*.wal /mnt/ssd/wal/
ln -s /mnt/ssd/wal /var/lib/kyrodb/data/wal
systemctl start kyrodb
```

**Option 3**: Batch inserts
```bash
# Instead of 1000 individual inserts
# Use bulk insert (when implemented in Phase 1)
```

## Circuit Breaker Won't Reset

### Symptom
```bash
curl -s http://localhost:51052/metrics | grep circuit_breaker
# kyrodb_wal_circuit_breaker_state = 1 (open)
# Stays open >5 minutes
```

### Fix

```bash
# Check root cause
dmesg | grep -i error | tail -50

# Common causes:
# 1. Filesystem read-only (remounted after errors)
mount | grep kyrodb  # Should be "rw" not "ro"

# 2. Permissions
ls -la /var/lib/kyrodb/data/
chown -R kyrodb:kyrodb /var/lib/kyrodb/data

# 3. Disk failure
smartctl -H /dev/sdX

# Manual circuit breaker reset (force close)
curl -X POST http://localhost:51052/admin/circuit-breaker/reset
```

## Training Task Crash Loop

### Symptom
```bash
# Logs show:
# "Training task crashed"
# "Training supervisor restarting task (attempt 3/5)"
# Repeats every 30 seconds
```

### Diagnosis

```bash
# Check training metrics
curl -s http://localhost:51052/metrics | grep training

# kyrodb_training_crashes_total increasing
# kyrodb_training_restarts_total >5 in 5 minutes
```

### Fix

**Cause: Out of memory during training**
```yaml
# config.yaml
cache:
  training:
    max_samples: 50000  # Reduce from 100000
    window_duration_secs: 300  # Reduce from 3600
```

**Cause: Corrupted access log**
```bash
# Reset access logger
systemctl stop kyrodb
rm /var/lib/kyrodb/data/access_log.bin  # Loses access patterns
systemctl start kyrodb
```

**Cause: Bug in RMI training**
```bash
# Check logs for panic message
grep -A 10 "panic" /var/log/kyrodb/server.log

# Report bug with logs
# Workaround: Disable learned cache temporarily

# config.yaml
cache:
  strategy: LRU  # Disable learned cache until bug fixed
```

## Decision Tree

```
Server Unhealthy?
├─ Check process
│  ├─ Not running → Start server
│  └─ Running → Check logs
│
├─ Logs show "SLO breach"
│  ├─ P99 latency high → See "P99 Latency High"
│  ├─ Error rate high → Check application errors
│  └─ Cache hit rate low → Increase cache capacity
│
├─ Logs show "circuit breaker open"
│  ├─ Check disk space → Delete old files
│  ├─ Check permissions → Fix ownership
│  ├─ Check filesystem → Run fsck
│  └─ Wait 60s → Auto-resets
│
├─ Logs show "training crash"
│  ├─ Check memory usage → Reduce training window
│  └─ Check restart count → If >5, report bug
│
└─ Logs show "panic"
   ├─ File bug report → Include full logs
   └─ Restore from backup → Last known good state
```

## Monitoring Checklist

Daily checks:
```bash
# 1. Health status
curl http://localhost:51052/health

# 2. Recent errors
journalctl -u kyrodb --since "1 hour ago" | grep -i error

# 3. Backup age
kyrodb_backup list --backup-dir /backups | head -1

# 4. Disk space
df -h /var/lib/kyrodb

# 5. Performance metrics
curl -s http://localhost:51052/metrics | grep -E "p99|hit_rate|error"
```

Weekly checks:
```bash
# 1. Test restore
kyrodb_backup restore --backup-id <latest> --dry-run

# 2. Review SLO trends
# Check if P99 latency trending up

# 3. Check for memory leaks
ps aux | grep kyrodb_server | awk '{print $6}'  # RSS memory
```

## Escalation

| Severity | Condition | Response Time | Action |
|----------|-----------|---------------|--------|
| **P0 - Critical** | Data loss risk (circuit breaker open >1hr) | Immediate | Page on-call engineer |
| **P0** | Server down >5 min | Immediate | Restart, if fails → restore backup |
| **P1 - High** | SLO breach >5 min | 15 minutes | Investigate cause, may need restart |
| **P1** | OOM crash | 15 minutes | Reduce memory usage, restart |
| **P2 - Medium** | Training crash loop | 1 hour | Reduce training window or disable |
| **P2** | Backup failed | 1 hour | Free disk space, verify next backup |
| **P3 - Low** | Cache hit rate <40% | Next day | Tune cache parameters |

## Getting Help

1. **Check logs**: `/var/log/kyrodb/server.log`
2. **Check metrics**: `curl http://localhost:51052/metrics`
3. **Review this guide**: Common issues covered above
4. **GitHub Issues**: Report bugs with full logs
5. **Emergency**: Restore from last known good backup
