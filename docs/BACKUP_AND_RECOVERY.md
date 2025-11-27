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

# Restore from backup
./target/release/kyrodb_backup restore \
  --data-dir ./data \
  --backup-dir ./backups \
  --backup-id <BACKUP_ID>

# Restore to specific point in time
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

# 2. List available backups
./target/release/kyrodb_backup list

# 3. Restore from backup ID
./target/release/kyrodb_backup restore --backup-id <BACKUP_ID>

# 4. Start server
systemctl start kyrodb

# 5. Verify data restored
curl http://localhost:51051/metrics | grep kyrodb_hnsw_vector_count
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
./target/release/kyrodb_backup restore --point-in-time $TARGET_TIME

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
./target/release/kyrodb_backup \
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
./target/release/kyrodb_backup \
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
curl http://localhost:51099/metrics | grep kyrodb_hnsw_vector_count

# 5. Clean up
kill $SERVER_PID
rm -rf $TMP_DIR
```

**Schedule monthly**: Full restore test to ensure backups are valid.

## Safeguards

KyroDB protects against accidental data loss.

### Important: Stop Server Before Restore

**Always stop the KyroDB server before performing a restore operation.**

```bash
# 1. Stop server
systemctl stop kyrodb

# 2. Perform restore
./target/release/kyrodb_backup restore \
  --data-dir ./data \
  --backup-dir ./backups \
  --backup-id <BACKUP_ID>

# 3. Start the server
systemctl start kyrodb
```

Restoring while the server is running may cause data corruption or inconsistent state.

## Cloud Backup (S3)

**Note**: S3 backup integration is planned for a future release. For now, use standard tools like `aws s3 sync` to copy backups to cloud storage:

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

### Backup Metrics

```bash
# View backup statistics
curl http://localhost:51051/metrics | grep backup

# Key metrics:
# - kyrodb_backup_total: Total backups created
# - kyrodb_backup_size_bytes: Last backup size
# - kyrodb_backup_duration_seconds: Last backup duration
# - kyrodb_restore_total: Total restores performed
```

### Backup Health Check

```bash
#!/bin/bash
# /usr/local/bin/check_backup_age.sh

BACKUP_DIR=/backups
MAX_AGE_HOURS=26  # Alert if no backup in 26 hours

# Get latest backup timestamp
LATEST=$(kyrodb_backup list --backup-dir $BACKUP_DIR --json | \
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
