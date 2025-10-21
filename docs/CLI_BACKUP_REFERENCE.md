# KyroDB Backup CLI - Quick Reference

Professional command-line tool for KyroDB backup and restore operations.

## Installation

```bash
# Build from source
cargo build --bin kyrodb_backup --release

# Binary location
./target/release/kyrodb_backup
```

## Usage

```
kyrodb_backup [OPTIONS] <COMMAND>

Global Options:
  --data-dir <PATH>      Database data directory [default: ./data]
  --backup-dir <PATH>    Backup storage directory [default: ./backups]
  -h, --help            Print help
```

## Commands

### 1. Create Backup

Create a new full or incremental backup.

```bash
# Full backup
kyrodb_backup create --description "Daily backup"

# Incremental backup (requires reference backup ID)
kyrodb_backup create --incremental \
  --reference 550e8400-e29b-41d4-a716-446655440000 \
  --description "Hourly incremental"

# With custom directories
kyrodb_backup --data-dir /var/lib/kyrodb \
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

List all available backups in table or JSON format.

```bash
# Table format (default)
kyrodb_backup list

# JSON format for scripting
kyrodb_backup list --format json
```

**Options:**
- `--format <table|json>`: Output format (default: table)

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
# Restore from specific backup
kyrodb_backup restore --backup-id 550e8400-e29b-41d4-a716-446655440000

# Point-in-time restore (Unix timestamp)
kyrodb_backup restore --point-in-time 1729090200

# With custom directories
kyrodb_backup --data-dir /var/lib/kyrodb \
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

---

### 4. Prune Old Backups

Remove old backups based on retention policy.

```bash
# Default policy (7 daily, 4 weekly, 6 monthly)
kyrodb_backup prune

# Custom retention policy
kyrodb_backup prune \
  --keep-daily 14 \
  --keep-weekly 8 \
  --keep-monthly 12
```

**Options:**
- `--keep-daily <N>`: Keep last N daily backups (default: 7)
- `--keep-weekly <N>`: Keep last N weekly backups (default: 4)
- `--keep-monthly <N>`: Keep last N monthly backups (default: 6)
- `--min-age-days <N>`: Minimum age in days before pruning (default: 0)

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
kyrodb_backup verify 550e8400-e29b-41d4-a716-446655440000
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
kyrodb_backup --data-dir /var/lib/kyrodb \
  --backup-dir /backup/kyrodb \
  create --description "Daily backup $(date +%Y-%m-%d)"

# Prune old backups
kyrodb_backup --backup-dir /backup/kyrodb prune
```

### Hourly Incremental Backups

```bash
#!/bin/bash
# Get latest backup ID
LATEST=$(kyrodb_backup list --format json | jq -r '.[0].id')

# Create incremental backup
kyrodb_backup create \
  --incremental \
  --reference "$LATEST" \
  --description "Hourly backup $(date +%H:%M)"
```

### Disaster Recovery

```bash
#!/bin/bash
# List available backups
kyrodb_backup list

# Restore from latest backup
LATEST=$(kyrodb_backup list --format json | jq -r '.[0].id')
kyrodb_backup restore --backup-id "$LATEST"

# Verify restore success
kyrodb_backup verify "$LATEST"
```

### Backup Before Upgrade

```bash
#!/bin/bash
# Create pre-upgrade backup
kyrodb_backup create --description "Pre-upgrade backup v2.0"

# Perform upgrade
./upgrade.sh

# If upgrade fails, restore
if [ $? -ne 0 ]; then
  BACKUP_ID=$(kyrodb_backup list --format json | \
    jq -r '.[] | select(.description | contains("Pre-upgrade")) | .id' | \
    head -1)
  kyrodb_backup restore --backup-id "$BACKUP_ID"
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
2. **Parallel Operations**: CLI automatically uses multiple threads for large backups
3. **Compression**: Future versions will support compression (planned for Phase 1)
4. **S3 Remote Storage**: Configure S3 for automatic off-site backup replication

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
# List backups before pruning (dry run coming in v2.0)
kyrodb_backup list

# Adjust retention policy
kyrodb_backup prune \
  --keep-daily 30 \
  --keep-weekly 12 \
  --keep-monthly 24
```

---

## Integration with Monitoring

### Prometheus Metrics

The backup operations expose metrics:
- `kyrodb_backup_count_total`: Total number of backups
- `kyrodb_backup_size_bytes`: Size of all backups
- `kyrodb_backup_duration_seconds`: Backup creation duration
- `kyrodb_restore_duration_seconds`: Restore operation duration

### Health Checks

```bash
# Verify backups exist
BACKUP_COUNT=$(kyrodb_backup list --format json | jq 'length')
if [ "$BACKUP_COUNT" -eq 0 ]; then
  echo "CRITICAL: No backups found"
  exit 2
fi
```

---

## Best Practices

1. **Regular Backups**: Schedule daily full backups + hourly incrementals
2. **Off-site Storage**: Use S3 for geographic redundancy
3. **Retention Policy**: Keep 7 daily, 4 weekly, 6 monthly backups (default), or customize as needed (e.g., 12 monthly for compliance)
4. **Verification**: Verify backups after creation
5. **Test Restores**: Perform quarterly restore drills
6. **Monitoring**: Alert on backup failures
7. **Documentation**: Document backup IDs for critical snapshots

---

## See Also

- [ERROR_RECOVERY_AND_BACKUP.md](./ERROR_RECOVERY_AND_BACKUP.md) - Comprehensive design document

---

**Version**: 1.0.0  
**Status**: Production Ready
