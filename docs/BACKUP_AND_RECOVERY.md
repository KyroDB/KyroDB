# Backup and Recovery

## Purpose

Define the operational backup and restore workflow for KyroDB data directories.

## Scope

- full and incremental backup creation
- verification and restore
- retention pruning
- recovery constraints

## Commands

```bash
# Build CLI
# Common case: build the backup binary
cargo build --release --bin kyrodb_backup
# Optional: same backup binary with additional CLI-tooling UX
cargo build --release --bin kyrodb_backup --features cli-tools

# Full backup
./target/release/kyrodb_backup --data-dir ./data --backup-dir ./backups create --description "daily-full"

# List backups
./target/release/kyrodb_backup --data-dir ./data --backup-dir ./backups list --format json

# Verify backup
./target/release/kyrodb_backup --data-dir ./data --backup-dir ./backups verify <BACKUP_ID>

# Incremental backup (after new WAL activity)
command -v jq >/dev/null || { echo "jq is required for parent backup selection"; exit 1; }
PARENT_ID=$(
  ./target/release/kyrodb_backup --data-dir ./data --backup-dir ./backups list --format json \
    | jq -r 'sort_by(.timestamp) | last | .id // empty'
)
if [ -z "$PARENT_ID" ]; then
  echo "No parent backup found. Run a full backup first or select a parent manually from list output."
  exit 1
fi
./target/release/kyrodb_backup --data-dir ./data --backup-dir ./backups create --incremental --reference "$PARENT_ID" --description "hourly-inc"

# Restore (destructive to target data-dir)
export BACKUP_ALLOW_CLEAR=true
./target/release/kyrodb_backup --data-dir ./data --backup-dir ./backups restore --backup-id <BACKUP_ID>
./target/release/kyrodb_backup --data-dir ./data --backup-dir ./backups restore --point-in-time <UNIX_TIMESTAMP>

# Prune
./target/release/kyrodb_backup --data-dir ./data --backup-dir ./backups prune --keep-hourly 24 --keep-daily 7 --keep-weekly 4 --keep-monthly 12 --min-age-days 1
```

## Key Contracts

- backup creation requires a recoverable KyroDB data directory (manifest/snapshot/WAL state)
- incremental backup requires post-parent WAL segments
- if no new WAL exists after parent, incremental create fails (`No new WAL files since parent backup`)
- `verify` takes a positional ID (`verify <BACKUP_ID>`)
- restore refuses directory clear unless `BACKUP_ALLOW_CLEAR=true`

## Recovery Drill Pattern

1. create full backup
2. verify backup
3. restore into temporary data directory
4. run server against restored directory on alternate port
5. probe `/health` and run sanity traffic

## Related Docs

- [CLI_BACKUP_REFERENCE.md](CLI_BACKUP_REFERENCE.md)
- [OPERATIONS.md](OPERATIONS.md)
- [CONFIGURATION_MANAGEMENT.md](CONFIGURATION_MANAGEMENT.md)
