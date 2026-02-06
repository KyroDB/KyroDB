# Backup and Recovery Guide

Complete guide to protecting your data.

## Overview

KyroDB provides:
- **Full backups**: Complete snapshot of all data
- **Incremental backups**: Only changes since last backup
- **Point-in-time recovery**: Restore to any moment
- **Backup verification**: Ensure backups are valid

## Quick Reference

```bash
# Build backup CLI (minimal: JSON output; no tables/progress bars)
cargo build --bin kyrodb_backup --release

# Optional: enable table output + progress bars
cargo build --bin kyrodb_backup --release --features cli-tools
```

```bash
# Create full backup (default)
./target/release/kyrodb_backup create \
  --data-dir ./data \
  --backup-dir ./backups \
  --description "Daily backup"

# Create incremental backup
./target/release/kyrodb_backup create \
  --data-dir ./data \
  --backup-dir ./backups \
  --incremental \
  --reference <PARENT_BACKUP_ID> \
  --description "Hourly incremental"

# List all backups
./target/release/kyrodb_backup list \
  --data-dir ./data \
  --backup-dir ./backups

# List backups in JSON format
./target/release/kyrodb_backup list \
  --data-dir ./data \
  --backup-dir ./backups \
  --format json

# Restore from backup (DANGEROUS: may clear/overwrite --data-dir)
export BACKUP_ALLOW_CLEAR=true
./target/release/kyrodb_backup restore \
  --data-dir ./data \
  --backup-dir ./backups \
  --backup-id <BACKUP_ID>

# Restore to specific point in time (Unix timestamp)
./target/release/kyrodb_backup restore \
  --data-dir ./data \
  --backup-dir ./backups \
  --point-in-time <UNIX_TIMESTAMP>

# Verify backup integrity
./target/release/kyrodb_backup verify \
  --data-dir ./data \
  --backup-dir ./backups \
  <BACKUP_ID>

# Prune old backups
./target/release/kyrodb_backup prune \
  --data-dir ./data \
  --backup-dir ./backups \
  --keep-daily 7 \
  --keep-weekly 4 \
  --keep-monthly 6
```

**Note**: All backup commands use default directories:
- Data: `./data` (override with `--data-dir`)
- Backups: `./backups` (override with `--backup-dir`)

## Backup Types

### Full Backup

Captures complete database state at a point in time.

```bash
./target/release/kyrodb_backup create \
  --description "Daily backup $(date +%Y-%m-%d)"
```

**Creates**:
- Snapshot of all vectors
- Backup metadata and checksum

**When to use**: Daily, before major upgrades, before risky operations.

### Incremental Backup

Captures only changes since parent backup.

```bash
# Get ID of last full backup
PARENT_ID=$(./target/release/kyrodb_backup list --format json | jq -r '.[0].id')

# Create incremental
./target/release/kyrodb_backup create \
  --incremental \
  --reference $PARENT_ID \
  --description "Hourly incremental"
```

**When to use**: Hourly, between full backups, for continuous protection.

## Migration Notes

### Legacy Snapshots Without Distance Metric

Legacy snapshot formats (LegacySnapshotV2) do not store the distance metric. During migration, KyroDB assumes the default metric (currently `DistanceMetric::Cosine`) and emits a warning with the snapshot timestamp and document count. If the original index used a different metric, search results may be incorrect after recovery.

Do not resume production traffic until validation passes.

#### Determine the original metric

Use as many of these signals as you have (LegacySnapshotV2 cannot tell you by itself):

1. Application configuration used at snapshot time
  - Inspect the config file deployed at the time (TOML/YAML) and the effective runtime config.
  - If you keep config in Git, use the commit/tag that produced the snapshot.
2. Snapshot/backup metadata outside the snapshot file
  - Some deployments store the metric in backup manifests or operational runbooks.
3. Server logs
  - KyroDB logs a warning when it loads a legacy snapshot and falls back to the default metric.
  - Example:

```bash
grep -n "LegacySnapshotV2\|legacy snapshot\|DistanceMetric" /var/log/kyrodb/kyrodb.log | tail -n 50
```

#### Validation procedure after restore

Run validation in a staging environment first.

1. Stop KyroDB

```bash
systemctl stop kyrodb
```

2. Restore the backup

```bash
export BACKUP_ALLOW_CLEAR=true
./target/release/kyrodb_backup restore --backup-id <BACKUP_ID> --data-dir ./data --backup-dir ./backups
```

3. Start KyroDB

```bash
systemctl start kyrodb
```

4. Validate correctness with known vectors/queries
  - Run a fixed set of “golden” query vectors (and expected top-k IDs) against the recovered index.
  - Also validate similarity/distance values by computing them offline using the intended metric.

At minimum, confirm:
- Top-1 / top-k IDs match expected results.
- Score ordering is consistent with the metric (Cosine vs Euclidean vs InnerProduct).

#### Remediation if the metric is wrong

1. Stop KyroDB

```bash
systemctl stop kyrodb
```

2. Update the index configuration to the correct metric
  - Edit the KyroDB config file used by the service (TOML/YAML).
  - In KyroDB, the index configuration is under the `hnsw` section. Set `distance` to one of: `cosine`, `euclidean`, `innerproduct`.

```toml
[hnsw]
distance = "euclidean"
```

3. Rebuild/reindex the dataset using the correct metric
  - KyroDB does not provide a standalone `kyrodb_reindex` CLI.
  - Rebuild by loading all vectors into a fresh data directory using the gRPC bulk insert API:
    1) Stop KyroDB.
    2) Move or clear the existing data directory (e.g., `/var/lib/kyrodb/data`).
    3) Start KyroDB with the updated config.
    4) Stream all vectors via `BulkInsert`, then call `FlushHotTier` and optionally `CreateSnapshot`.
  - API details are in [docs/API_REFERENCE.md](docs/API_REFERENCE.md).

4. Restart and re-run the validation procedure

```bash
systemctl start kyrodb
```

Do not resume production traffic until the validation procedure passes for LegacySnapshotV2 restores.

## Backup Schedule (Recommended)

```bash
# Daily full backup at 2 AM
0 2 * * * /usr/local/bin/kyrodb_backup create \
  --description "Daily-$(date +\%Y-\%m-\%d)"

# Hourly incremental (during business hours)
0 9-17 * * 1-5 /usr/local/bin/kyrodb_backup create \
  --incremental \
  --reference $(kyrodb_backup list --format json | jq -r '.[0].id') \
  --description "Hourly-$(date +\%Y-\%m-\%d-\%H)"
```

## Recovery Scenarios

### Scenario 1: Restore from Specific Backup

```bash
# 1. Stop server
systemctl stop kyrodb

# 2. Allow data directory clear for restore
export BACKUP_ALLOW_CLEAR=true

# 3. List available backups
./target/release/kyrodb_backup list

# 4. Restore from backup ID
./target/release/kyrodb_backup restore --backup-id <BACKUP_ID>

# 5. Start server
systemctl start kyrodb

# 6. Verify data restored
curl http://localhost:51051/health
```

### Scenario 2: Point-in-Time Recovery

Restore database to exact moment (e.g., before bad data was written).

```bash
# 1. Get Unix timestamp for target time
# Example: October 20, 2025 14:30 UTC
TARGET_TIME=$(date -d "2025-10-20 14:30:00 UTC" +%s)

# 2. Stop server
systemctl stop kyrodb

# 3. Restore to that time
BACKUP_ALLOW_CLEAR=true ./target/release/kyrodb_backup restore --point-in-time $TARGET_TIME

# 4. Start server
systemctl start kyrodb
```

**How it works**:
- Finds most recent backup before target time
- Applies incremental changes up to timestamp
- Precision: 1 second

### Scenario 3: Disaster Recovery

Complete data center failure. Restore from offsite backup.

```bash
# 1. Download backups from S3/cloud storage
aws s3 sync s3://kyrodb-backups ./backups

# 2. Restore on new server
BACKUP_ALLOW_CLEAR=true ./target/release/kyrodb_backup \
  --backup-dir ./backups \
  restore --backup-id <ID>

# 3. Verify restoration
curl http://localhost:51051/health
```

## Backup Retention

Keep backups organized and storage costs low.

### Automatic Pruning

```bash
# Custom retention policy
./target/release/kyrodb_backup prune \
  --data-dir ./data \
  --backup-dir ./backups \
  --keep-daily 30 \
  --keep-weekly 12 \
  --keep-monthly 12 \
  --min-age-days 7
```

### Automated Retention Policy

```bash
# Keep:
# - Hourly backups for 24 hours (keep_hourly)
# - Daily backups for 7 days (keep_daily)
# - Weekly backups for 4 weeks (keep_weekly)
# - Monthly backups for 12 months (keep_monthly)
# - Don't delete backups < 7 days old (min_age_days)

./target/release/kyrodb_backup prune \
  --keep-hourly 24 \
  --keep-daily 7 \
  --keep-weekly 4 \
  --keep-monthly 12 \
  --min-age-days 7
```

**Automated retention script** (run daily via cron):

```bash
#!/bin/bash
# /usr/local/bin/kyrodb_backup_retention.sh

set -e

BACKUP_DIR=/backups
DATA_DIR=/var/lib/kyrodb/data

# Apply retention policy
/usr/local/bin/kyrodb_backup prune \
  --data-dir $DATA_DIR \
  --backup-dir $BACKUP_DIR \
  --keep-daily 30 \
  --keep-weekly 12 \
  --keep-monthly 12 \
  --min-age-days 1

# Log result
echo "$(date): Backup retention applied successfully" >> /var/log/kyrodb/retention.log
```

Add to crontab:
```bash
# Daily backup retention at 3 AM
0 3 * * * /usr/local/bin/kyrodb_backup_retention.sh
```

## Backup Verification

**Always verify backups** before you need them.

### Quick Verification

```bash
# Verify backup integrity and checksum
./target/release/kyrodb_backup verify \
  --data-dir ./data \
  --backup-dir ./backups \
  <BACKUP_ID>
```

### Full Verification (Test Restore)

```bash
# 1. Create temporary directory
TMP_DIR=$(mktemp -d)

# 2. Restore to temporary location
BACKUP_ALLOW_CLEAR=true ./target/release/kyrodb_backup \
  --backup-dir ./backups \
  --data-dir $TMP_DIR \
  restore --backup-id <BACKUP_ID>

# 3. Start server on temporary data (different port)
./target/release/kyrodb_server \
  --data-dir $TMP_DIR \
  --port 50099 &
SERVER_PID=$!

# 4. Run smoke tests
sleep 2
curl http://localhost:51099/health

# 5. Clean up
kill $SERVER_PID
rm -rf $TMP_DIR
```

**Schedule monthly**: Full restore test to ensure backups are valid.

## Safeguards

KyroDB protects against accidental data loss.

### Important: Stop Server Before Restore

**Always stop the KyroDB server before performing a restore operation.**

Restores clear the target data directory. Confirm this by setting `BACKUP_ALLOW_CLEAR=true` in the environment.

```bash
# 1. Stop server
systemctl stop kyrodb

# 2. Allow data directory clear for restore
export BACKUP_ALLOW_CLEAR=true

# 3. Perform restore
./target/release/kyrodb_backup restore \
  --data-dir ./data \
  --backup-dir ./backups \
  --backup-id <BACKUP_ID>

# 4. Start the server
systemctl start kyrodb
```

Restoring while the server is running may cause data corruption or inconsistent state.

## Cloud Backup (S3)

The CLI does not expose S3 operations directly. Use standard tools like `aws s3 sync` to copy backups to cloud storage:

```bash
# Sync local backups to S3
aws s3 sync ./backups s3://your-bucket/kyrodb-backups/

# Download backups from S3
aws s3 sync s3://your-bucket/kyrodb-backups/ ./backups

# Then restore locally
./target/release/kyrodb_backup restore \
  --data-dir ./data \
  --backup-dir ./backups \
  --backup-id <BACKUP_ID>
```

## Monitoring Backups

### Backup Health Check

```bash
#!/bin/bash
# /usr/local/bin/check_backup_age.sh

BACKUP_DIR=/backups
MAX_AGE_HOURS=26  # Alert if no backup in 26 hours

# Get latest backup timestamp
LATEST=$(kyrodb_backup list --backup-dir $BACKUP_DIR --format json | \
  jq -r '.[0].timestamp')

NOW=$(date +%s)
AGE_HOURS=$(( (NOW - LATEST) / 3600 ))

if [ $AGE_HOURS -gt $MAX_AGE_HOURS ]; then
  echo "ALERT: No backup in $AGE_HOURS hours (threshold: $MAX_AGE_HOURS)"
  exit 1
fi

echo "OK: Latest backup is $AGE_HOURS hours old"
```

## Troubleshooting

### Backup Failed: Disk Full

```bash
# Check disk space
df -h ./backups

# Prune old backups using retention policy
./target/release/kyrodb_backup prune \
  --data-dir ./data \
  --backup-dir ./backups \
  --min-age-days 1

# Or move to cheaper storage
aws s3 sync ./backups s3://kyrodb-archive --storage-class GLACIER
rm -rf ./backups/*
```

### Restore Failed: Checksum Mismatch

```bash
# Backup may be corrupted - verify it
./target/release/kyrodb_backup verify \
  --data-dir ./data \
  --backup-dir ./backups \
  <BACKUP_ID>

# If corrupted, use previous backup
./target/release/kyrodb_backup list \
  --data-dir ./data \
  --backup-dir ./backups

./target/release/kyrodb_backup restore \
  --data-dir ./data \
  --backup-dir ./backups \
  --backup-id <PREVIOUS_BACKUP_ID>
```

### Incremental Chain Broken

```bash
# Error: Parent backup not found

# Solution: Create new full backup
./target/release/kyrodb_backup create \
  --data-dir ./data \
  --backup-dir ./backups \
  --description "New full backup (chain reset)"
```

## Best Practices

1. **Test restores monthly** - Ensure backups actually work
2. **Store backups offsite** - Protect against data center failure
3. **Automate backups** - Never rely on manual backups
4. **Monitor backup age** - Alert if backups stop working
5. **Keep retention policy** - Balance cost vs recovery options
6. **Verify after creation** - Check checksums immediately
7. **Document recovery procedures** - Train your team

## Security

### Access Control

```bash
# Restrict backup directory permissions
chmod 700 ./backups
chown kyrodb:kyrodb ./backups

# Restrict S3 bucket access (IAM policy)
{
  "Effect": "Allow",
  "Action": ["s3:GetObject", "s3:PutObject"],
  "Resource": "arn:aws:s3:::kyrodb-backups/*"
}
```

## Recovery Time Objectives

Typical recovery times:

| Backup Size | Restore Time | Network Required |
|-------------|--------------|------------------|
| 1 GB | 10 seconds | No |
| 10 GB | 60 seconds | No |
| 100 GB | 10 minutes | No |
| 1 TB | 2 hours | Yes (if remote) |

**To minimize downtime**:
- Keep backups on fast storage (SSD)
- Use incremental backups (faster to create, slower to restore)
- Test restore speed in advance
