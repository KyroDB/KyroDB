# KyroDB Backup CLI - Quick Reference

Command-line tool for KyroDB backup and restore operations.

## Installation

```bash
# Build from source (minimal: JSON output; no tables/progress bars)
cargo build --bin kyrodb_backup --release

# Optional: enable table output + progress bars
cargo build --bin kyrodb_backup --release --features cli-tools

# Binary location
./target/release/kyrodb_backup
```

## Usage

```
kyrodb_backup [OPTIONS] <COMMAND>

Global Options:
  --data-dir <PATH>     Database data directory [default: ./data]
  --backup-dir <PATH>   Backup storage directory [default: ./backups]
  -h, --help           Print help
```

## Commands

### 1. Create Backup

Create a new full or incremental backup.

```bash
# Full backup
./target/release/kyrodb_backup \
  --data-dir ./data \
  --backup-dir ./backups \
  create \
  --description "Daily backup"

# Incremental backup (requires reference backup ID)
./target/release/kyrodb_backup \
  --data-dir ./data \
  --backup-dir ./backups \
  create \
  --incremental \
  --reference 550e8400-e29b-41d4-a716-446655440000 \
  --description "Hourly incremental"

# With custom directories
./target/release/kyrodb_backup \
  --data-dir /var/lib/kyrodb \
  --backup-dir /backup/kyrodb \
  create --description "Production backup"
```

**Options:**
- `--incremental`: Create incremental backup instead of full
- `--description <TEXT>`: Human-readable description
- `--reference <UUID>`: Parent backup ID (required for incremental)

**Output:**
```
Creating full backup...
Full backup created successfully

Backup ID: 550e8400-e29b-41d4-a716-446655440000
Type: Full
Size: 1048576 bytes
Vectors: 10000
```

---

### 2. List Backups

List all available backups in table or JSON format. The default output format depends on whether the binary was built with `cli-tools`.

```bash
# Table format (available with `cli-tools`; default only when built with `cli-tools`)
./target/release/kyrodb_backup \
  --data-dir ./data \
  --backup-dir ./backups \
  list

# JSON format for scripting
./target/release/kyrodb_backup \
  --data-dir ./data \
  --backup-dir ./backups \
  list \
  --format json
```

**Options:**
- `--format <table|json>`: Output format. Default is `table` when built with `cli-tools`; otherwise `json`. For stable automation, pass `--format json`.

**Table Output:**
```
╭──────────────────────────────────────┬─────────────┬─────────────────────┬──────────┬─────────┬──────────────╮
│ Backup ID                            │ Type        │ Created             │ Size     │ Vectors │ Description  │
├──────────────────────────────────────┼─────────────┼─────────────────────┼──────────┼─────────┼──────────────┤
│ 550e8400-e29b-41d4-a716-446655440000 │ Full        │ 2025-10-16 14:30:00 │ 1.00 MB  │ 10000   │ Daily backup │
│ 6ba7b810-9dad-11d1-80b4-00c04fd430c8 │ Incremental │ 2025-10-16 18:00:00 │ 256.00 KB│ 2500    │ Hourly sync  │
╰──────────────────────────────────────┴─────────────┴─────────────────────┴──────────┴─────────┴──────────────╯
```

**JSON Output:**
```json
[
  {
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "timestamp": 1729090200,
    "backup_type": "Full",
    "size_bytes": 1048576,
    "vector_count": 10000,
    "checksum": 2882400000,
    "parent_id": null,
    "description": "Daily backup"
  }
]
```

---

### 3. Restore Database

Restore database from a backup or to a specific point in time.

```bash
# Restores may clear/overwrite the existing --data-dir.
# Safety mechanism: restore will refuse to clear unless you explicitly confirm.
export BACKUP_ALLOW_CLEAR=true

# Restore from specific backup
./target/release/kyrodb_backup \
  --data-dir ./data \
  --backup-dir ./backups \
  restore \
  --backup-id 550e8400-e29b-41d4-a716-446655440000

# Point-in-time restore (Unix timestamp)
./target/release/kyrodb_backup \
  --data-dir ./data \
  --backup-dir ./backups \
  restore \
  --point-in-time 1729090200

# With custom directories
./target/release/kyrodb_backup \
  --data-dir /var/lib/kyrodb \
  --backup-dir /backup/kyrodb \
  restore --backup-id 550e8400-e29b-41d4-a716-446655440000
```

**Options:**
- `--backup-id <UUID>`: Specific backup to restore from
- `--point-in-time <TIMESTAMP>`: Unix timestamp for PITR

**Output:**
```
Restoring from backup 550e8400...
Restore completed successfully

Database restored from backup 550e8400-e29b-41d4-a716-446655440000
```

**Warning:** Restore operation will overwrite existing data directory.

If `BACKUP_ALLOW_CLEAR` is not set to `true`, the restore will abort/refuse to clear the data directory and print a warning/error.

---

### 4. Prune Old Backups

Remove old backups based on retention policy.

```bash
# Custom retention policy
./target/release/kyrodb_backup \
  --data-dir ./data \
  --backup-dir ./backups \
  prune \
  --keep-daily 14 \
  --keep-weekly 8 \
  --keep-monthly 12
```

**Options:**
- `--keep-hourly <N>`: Keep last N hourly backups
- `--keep-daily <N>`: Keep last N daily backups
- `--keep-weekly <N>`: Keep last N weekly backups
- `--keep-monthly <N>`: Keep last N monthly backups
- `--min-age-days <N>`: Minimum age in days before pruning

**Output:**
```
Evaluating backups for pruning...
Done

Pruned 3 backup(s):
  - 6ba7b810-9dad-11d1-80b4-00c04fd430c8
  - 7ca8c920-8ebe-22e2-91c5-11d15fe541d9
  - 8db9d030-7fcf-33f3-a2d6-22e26gf652e0
```

---

### 5. Verify Backup Integrity

Verify backup file integrity and checksum.

```bash
# Verify specific backup
./target/release/kyrodb_backup \
  --data-dir ./data \
  --backup-dir ./backups \
  verify \
  550e8400-e29b-41d4-a716-446655440000
```

**Arguments:**
- `<backup-id>`: UUID of backup to verify

**Output:**
```
Verifying backup integrity...
Backup verification successful

Backup 550e8400-e29b-41d4-a716-446655440000 is valid
Size: 1048576 bytes
Checksum: 0xABCD1234
```

---

## Common Workflows

### Daily Backup Routine

```bash
#!/bin/bash
# Create full backup daily at midnight
./target/release/kyrodb_backup \
  --data-dir /var/lib/kyrodb \
  --backup-dir /backup/kyrodb \
  create --description "Daily backup $(date +%Y-%m-%d)"

# Prune old backups
./target/release/kyrodb_backup \
  --data-dir /var/lib/kyrodb \
  --backup-dir /backup/kyrodb \
  prune --keep-daily 7 --keep-weekly 4 --keep-monthly 6
```

### Hourly Incremental Backups

```bash
#!/bin/bash
# Get latest backup ID
LATEST=$(./target/release/kyrodb_backup \
  --data-dir ./data \
  --backup-dir ./backups \
  list \
  --format json | jq -r '.[0].id')

# Create incremental backup
./target/release/kyrodb_backup \
  --data-dir ./data \
  --backup-dir ./backups \
  create \
  --incremental \
  --reference "$LATEST" \
  --description "Hourly backup $(date +%H:%M)"
```

### Disaster Recovery

```bash
#!/bin/bash
# List available backups
./target/release/kyrodb_backup \
  --data-dir ./data \
  --backup-dir ./backups \
  list

# Restore from latest backup
LATEST=$(./target/release/kyrodb_backup \
  --data-dir ./data \
  --backup-dir ./backups \
  list \
  --format json | jq -r '.[0].id')

export BACKUP_ALLOW_CLEAR=true
./target/release/kyrodb_backup \
  --data-dir ./data \
  --backup-dir ./backups \
  restore \
  --backup-id "$LATEST"

# Verify restore success
./target/release/kyrodb_backup \
  --data-dir ./data \
  --backup-dir ./backups \
  verify \
  "$LATEST"
```

### Backup Before Upgrade

```bash
#!/bin/bash
# Create pre-upgrade backup
./target/release/kyrodb_backup \
  --data-dir ./data \
  --backup-dir ./backups \
  create \
  --description "Pre-upgrade backup v2.0"

# Perform upgrade
./upgrade.sh

# If upgrade fails, restore
if [ $? -ne 0 ]; then
  BACKUP_ID=$(./target/release/kyrodb_backup \
  --data-dir ./data \
  --backup-dir ./backups \
  list \
    --format json | \
    jq -r '.[] | select(.description | contains("Pre-upgrade")) | .id' | \
    head -1)
  export BACKUP_ALLOW_CLEAR=true
  ./target/release/kyrodb_backup \
  --data-dir ./data \
  --backup-dir ./backups \
  restore \
    --backup-id "$BACKUP_ID"
fi
```

---

## Error Handling

The CLI provides clear error messages for common issues:

```bash
# Missing backup ID
$ kyrodb_backup verify
error: the following required arguments were not provided:
  <backup-id>
```

### Restore Refuses to Clear Data Directory

When running a restore without `BACKUP_ALLOW_CLEAR` set, the CLI emits:

```
Error: Cannot clear data directory. Set BACKUP_ALLOW_CLEAR=true environment variable to allow clearing the data directory during restore.
```

**Solution**: set the environment variable to acknowledge the destructive operation:

```bash
export BACKUP_ALLOW_CLEAR=true
./target/release/kyrodb_backup restore --backup-id <backup-id>
```

```bash
# Invalid backup ID format
$ kyrodb_backup verify invalid-uuid
Error: invalid UUID format

# Backup not found
$ kyrodb_backup verify 00000000-0000-0000-0000-000000000000
Error: Backup 00000000-0000-0000-0000-000000000000 not found

# Conflicting options
$ kyrodb_backup restore --backup-id UUID --point-in-time 1729090200
error: the argument '--backup-id <BACKUP_ID>' cannot be used with '--point-in-time <POINT_IN_TIME>'
```

---

## Logging

Enable detailed logging for debugging:

```bash
# Set log level
export RUST_LOG=info
kyrodb_backup list

# Debug logging
export RUST_LOG=debug
kyrodb_backup create --description "Debug backup"

# Trace logging for deep debugging
export RUST_LOG=trace
kyrodb_backup restore --backup-id UUID
```

---

## Performance Tips

1. **Incremental Backups**: Use incremental backups for frequent snapshots (lower overhead)

---

## Troubleshooting

### Backup Creation Fails

```bash
# Check disk space
df -h /backup/kyrodb

# Verify data directory exists and is readable
ls -la /var/lib/kyrodb

# Check permissions
chmod 755 /backup/kyrodb
```

### Restore Fails

```bash
# Verify backup integrity first
kyrodb_backup verify <backup-id>

# Ensure target directory is writable
chmod 755 /var/lib/kyrodb

# Check for conflicting processes
lsof /var/lib/kyrodb
```

### Prune Removes Too Many Backups

```bash
# List backups before pruning to review
kyrodb_backup list

# Adjust retention policy to keep more backups
kyrodb_backup prune \
  --keep-daily 30 \
  --keep-weekly 12 \
  --keep-monthly 24
```

---

## Integration with Monitoring

The backup CLI does not emit Prometheus metrics. Use exit codes and periodic verification to monitor backup health.

### Health Checks

```bash
# Verify backups exist
BACKUP_COUNT=$(./target/release/kyrodb_backup \
  --data-dir ./data \
  --backup-dir ./backups \
  list \
  --format json | jq 'length')
if [ "$BACKUP_COUNT" -eq 0 ]; then
  echo "CRITICAL: No backups found"
  exit 2
fi
```

---

## Best Practices

1. **Regular Backups**: Schedule daily full backups + hourly incrementals
2. **Off-site Storage**: Use cloud storage (S3, GCS) for geographic redundancy
3. **Retention Policy**: Keep daily, weekly, monthly backups as needed for compliance
4. **Verification**: Verify backups after creation
5. **Test Restores**: Perform quarterly restore drills
6. **Monitoring**: Alert on backup failures
7. **Documentation**: Document backup IDs for critical snapshots

---

## See Also

- [BACKUP_AND_RECOVERY.md](./BACKUP_AND_RECOVERY.md) - Comprehensive backup guide

---

**Version**: 0.1.0
