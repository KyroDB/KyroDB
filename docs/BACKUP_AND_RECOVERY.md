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
# Create full backup
kyrodb_backup create-full --data-dir ./data --backup-dir ./backups

# Create incremental backup
kyrodb_backup create-incremental --parent-id <backup_id> --data-dir ./data

# List all backups
kyrodb_backup list --backup-dir ./backups

# Restore from backup
kyrodb_backup restore --backup-id <id> --data-dir ./data --allow-clear

# Verify backup integrity
kyrodb_backup verify --backup-id <id> --backup-dir ./backups
```

## Backup Types

### Full Backup

Captures complete database state at a point in time.

```bash
./target/release/kyrodb_backup create-full \
  --data-dir /var/lib/kyrodb/data \
  --backup-dir /backups \
  --description "Daily backup $(date +%Y-%m-%d)"
```

**Creates**:
- `backup_<uuid>.tar` - All data files (vectors, snapshots, WAL)
- `backup_<uuid>.json` - Metadata (timestamp, size, checksum)

**When to use**: Daily, before major upgrades, before risky operations.

### Incremental Backup

Captures only changes since parent backup.

```bash
# Get ID of last full backup
PARENT_ID=$(kyrodb_backup list --backup-dir /backups --json | jq -r '.[0].id')

# Create incremental
./target/release/kyrodb_backup create-incremental \
  --parent-id $PARENT_ID \
  --data-dir /var/lib/kyrodb/data \
  --backup-dir /backups \
  --description "Hourly incremental"
```

**Creates**:
- Only new WAL files since parent backup
- Much faster and smaller than full backup

**When to use**: Hourly, between full backups, for continuous protection.

## Backup Schedule (Recommended)

```bash
# Daily full backup at 2 AM
0 2 * * * /usr/local/bin/kyrodb_backup create-full \
  --data-dir /var/lib/kyrodb/data \
  --backup-dir /backups/daily \
  --description "Daily-$(date +\%Y-\%m-\%d)"

# Hourly incremental (during business hours)
0 9-17 * * 1-5 /usr/local/bin/kyrodb_backup create-incremental \
  --parent-id $(kyrodb_backup list --backup-dir /backups/daily --json | jq -r '.[0].id') \
  --data-dir /var/lib/kyrodb/data \
  --backup-dir /backups/hourly
```

## Recovery Scenarios

### Scenario 1: Restore Latest Backup

```bash
# 1. Stop server
systemctl stop kyrodb

# 2. Find latest backup
kyrodb_backup list --backup-dir /backups | head -n 5

# 3. Restore (with confirmation)
kyrodb_backup restore \
  --backup-id <latest_backup_id> \
  --data-dir /var/lib/kyrodb/data \
  --allow-clear

# 4. Start server
systemctl start kyrodb
```

**Dry run first** (recommended):
```bash
kyrodb_backup restore \
  --backup-id <id> \
  --data-dir /var/lib/kyrodb/data \
  --dry-run
```

### Scenario 2: Point-in-Time Recovery

Restore database to exact moment (e.g., before bad data was written).

```bash
# 1. Get Unix timestamp for target time
# Example: October 20, 2025 14:30 UTC
TARGET_TIME=$(date -d "2025-10-20 14:30:00 UTC" +%s)

# 2. Restore to that time
kyrodb_backup restore-pitr \
  --timestamp $TARGET_TIME \
  --data-dir /var/lib/kyrodb/data \
  --backup-dir /backups \
  --allow-clear

# 3. Verify data
kyrodb_backup list --backup-dir /backups | grep $TARGET_TIME
```

**How it works**:
1. Finds most recent full backup before target time
2. Applies incremental backups up to target time
3. Stops at exact timestamp (precision: 1 second)

### Scenario 3: Disaster Recovery

Complete data center failure. Restore from offsite backup.

```bash
# 1. Download backups from S3/cloud storage
aws s3 sync s3://kyrodb-backups /tmp/backups

# 2. Restore on new server
kyrodb_backup restore \
  --backup-id <id> \
  --data-dir /var/lib/kyrodb/data \
  --allow-clear

# 3. Verify restoration
curl http://localhost:51052/health
curl http://localhost:51052/metrics | grep kyrodb_hnsw_vector_count
```

## Backup Retention

Keep backups organized and storage costs low.

### Manual Pruning

```bash
# Delete backups older than 30 days
kyrodb_backup prune \
  --backup-dir /backups \
  --older-than-days 30
```

### Automated Retention Policy

```bash
# Keep:
# - All hourly backups for 24 hours
# - Daily backups for 7 days
# - Weekly backups for 4 weeks
# - Monthly backups for 12 months

kyrodb_backup prune \
  --backup-dir /backups \
  --hourly-hours 24 \
  --daily-days 7 \
  --weekly-weeks 4 \
  --monthly-months 12
```

**Retention script** (cron daily):
```bash
#!/bin/bash
# /usr/local/bin/kyrodb_backup_retention.sh

BACKUP_DIR=/backups

# Apply retention policy
/usr/local/bin/kyrodb_backup prune \
  --backup-dir $BACKUP_DIR \
  --hourly-hours 24 \
  --daily-days 7 \
  --weekly-weeks 4 \
  --monthly-months 12 \
  --min-age-days 1  # Never delete backups < 1 day old

# Log result
echo "$(date): Backup retention applied" >> /var/log/kyrodb/retention.log
```

## Backup Verification

**Always verify backups** before you need them.

### Quick Verification

```bash
# Verify metadata and checksum
kyrodb_backup verify \
  --backup-id <id> \
  --backup-dir /backups
```

### Full Verification (Test Restore)

```bash
# 1. Restore to temporary directory
TMP_DIR=$(mktemp -d)

kyrodb_backup restore \
  --backup-id <id> \
  --data-dir $TMP_DIR \
  --allow-clear

# 2. Start server on temporary data
kyrodb_server --data-dir $TMP_DIR --port 50099 &
SERVER_PID=$!

# 3. Run smoke tests
sleep 5
curl http://localhost:51099/health
curl http://localhost:51099/metrics | grep kyrodb_hnsw_vector_count

# 4. Clean up
kill $SERVER_PID
rm -rf $TMP_DIR
```

**Schedule monthly**: Full restore test to ensure backups are valid.

## Safeguards

KyroDB protects against accidental data loss.

### Restore Confirmation Required

```bash
# This FAILS (refuses to clear data)
kyrodb_backup restore --backup-id <id> --data-dir ./data

# Error: Data directory clear requires explicit confirmation
```

**To allow restore** (destructive operation):
```bash
# Option 1: Use --allow-clear flag
kyrodb_backup restore --backup-id <id> --data-dir ./data --allow-clear

# Option 2: Set environment variable
export BACKUP_ALLOW_CLEAR=true
kyrodb_backup restore --backup-id <id> --data-dir ./data
```

### Dry Run Mode

```bash
# Preview what will be deleted/restored
kyrodb_backup restore \
  --backup-id <id> \
  --data-dir ./data \
  --dry-run \
  --allow-clear

# Output:
# DRY-RUN: Would delete 15 file(s) from /var/lib/kyrodb/data:
#   - MANIFEST
#   - snapshot_100
#   - wal_1000.wal
#   ...
# DRY-RUN: Would restore 12 file(s) from backup
```

## Cloud Backup (S3)

Requires `s3-backup` feature (enterprise).

### Upload to S3

```bash
# Configure AWS credentials
export AWS_ACCESS_KEY_ID=<your_key>
export AWS_SECRET_ACCESS_KEY=<your_secret>
export AWS_REGION=us-west-2

# Upload backup
kyrodb_backup upload-s3 \
  --backup-id <id> \
  --bucket kyrodb-backups \
  --prefix production/
```

### Download from S3

```bash
# Download specific backup
kyrodb_backup download-s3 \
  --backup-id <id> \
  --bucket kyrodb-backups \
  --prefix production/ \
  --output-dir /tmp/restore
```

### Automated S3 Sync

```bash
# Sync all local backups to S3
kyrodb_backup sync-s3 \
  --backup-dir /backups \
  --bucket kyrodb-backups \
  --prefix production/ \
  --delete-after-upload
```

## Monitoring Backups

### Backup Metrics

```bash
# View backup statistics
curl http://localhost:51052/metrics | grep backup

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
df -h /backups

# Delete old backups
kyrodb_backup prune --backup-dir /backups --older-than-days 7

# Or move to cheaper storage
aws s3 sync /backups s3://kyrodb-archive --storage-class GLACIER
rm -rf /backups/*
```

### Restore Failed: Checksum Mismatch

```bash
# Backup may be corrupted
kyrodb_backup verify --backup-id <id> --full-scan

# If corrupted, use previous backup
kyrodb_backup list --backup-dir /backups
kyrodb_backup restore --backup-id <previous_id> --allow-clear
```

### Incremental Chain Broken

```bash
# Error: Parent backup not found

# Solution: Create new full backup
kyrodb_backup create-full \
  --data-dir ./data \
  --backup-dir /backups \
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

### Encrypt Backups

```bash
# Encrypt backup with GPG
kyrodb_backup create-full --data-dir ./data --backup-dir /tmp/backup
gpg --encrypt --recipient <your-key> /tmp/backup/backup_*.tar

# Decrypt for restore
gpg --decrypt backup_*.tar.gpg > backup.tar
```

### Access Control

```bash
# Restrict backup directory permissions
chmod 700 /backups
chown kyrodb:kyrodb /backups

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
