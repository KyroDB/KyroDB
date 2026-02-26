# Backup CLI Reference

## Purpose

Provides a command-level reference for `kyrodb_backup`.

## Scope

- command syntax
- required/optional arguments
- common failure modes

## Commands

```bash
# Build
cargo build --release --bin kyrodb_backup
cargo build --release --bin kyrodb_backup --features cli-tools

# create
./target/release/kyrodb_backup --data-dir ./data --backup-dir ./backups create --description "full"
./target/release/kyrodb_backup --data-dir ./data --backup-dir ./backups create --incremental --reference <PARENT_ID> --description "inc"

# list
./target/release/kyrodb_backup --data-dir ./data --backup-dir ./backups list --format json
./target/release/kyrodb_backup --data-dir ./data --backup-dir ./backups list --format table

# verify
./target/release/kyrodb_backup --data-dir ./data --backup-dir ./backups verify <BACKUP_ID>

# restore
export BACKUP_ALLOW_CLEAR=true
./target/release/kyrodb_backup --data-dir ./data --backup-dir ./backups restore --backup-id <BACKUP_ID>
./target/release/kyrodb_backup --data-dir ./data --backup-dir ./backups restore --point-in-time <UNIX_TIMESTAMP>

# prune
# Example policy with explicit non-default overrides:
# --keep-monthly 12 (default 6), --min-age-days 1 (default 0)
./target/release/kyrodb_backup --data-dir ./data --backup-dir ./backups prune --keep-hourly 24 --keep-daily 7 --keep-weekly 4 --keep-monthly 12 --min-age-days 1
```

## Key Contracts

Top-level usage:

```text
kyrodb_backup [OPTIONS] <COMMAND>
```

Options:

- `--data-dir <PATH>`
- `--backup-dir <PATH>`

Commands:

- `create`
- `list`
- `restore`
- `prune`
- `verify`

### Restore Safety Gate (`BACKUP_ALLOW_CLEAR`)

`BACKUP_ALLOW_CLEAR` explicitly enables destructive clear/overwrite behavior for the restore command (`kyrodb_backup ... restore ...`). When this variable is unset/false (default), restore remains in safe mode and refuses directory-clear operations.

Set `BACKUP_ALLOW_CLEAR=true` only when you intentionally want restore to replace existing state. Before running a destructive restore, back up current data and validate the restore flow in a test environment first.

Usage example:

```bash
export BACKUP_ALLOW_CLEAR=true
./target/release/kyrodb_backup --data-dir ./data --backup-dir ./backups restore --backup-id <BACKUP_ID>
```

CAUTION: `restore` with `BACKUP_ALLOW_CLEAR=true` can permanently delete or overwrite existing data in `--data-dir`.

### Prune Retention Flags

- `--keep-hourly N` - retain `N` most recent hourly backups
- `--keep-daily N` - retain `N` most recent daily backups
- `--keep-weekly N` - retain `N` most recent weekly backups
- `--keep-monthly N` - retain `N` most recent monthly backups
- `--min-age-days N` - only consider backups older than `N` days for pruning

Retention overlap rule: backups matching any retention bucket are kept.

CLI defaults (when flags are omitted):
- `--keep-hourly 24`
- `--keep-daily 7`
- `--keep-weekly 4`
- `--keep-monthly 6`
- `--min-age-days 0`

The prune command in the Commands block is an explicit policy example; it intentionally overrides the defaults for `--keep-monthly` and `--min-age-days`.

Prune permanently deletes backup files from `--backup-dir`; verify policy values before running.

### Important Error Cases

```bash
# Missing positional backup ID
./target/release/kyrodb_backup verify

# Invalid backup ID
./target/release/kyrodb_backup verify invalid-uuid

# Conflicting restore options
./target/release/kyrodb_backup restore --backup-id UUID --point-in-time 1729090200
```

## Related Docs

- [BACKUP_AND_RECOVERY.md](BACKUP_AND_RECOVERY.md)
- [OPERATIONS.md](OPERATIONS.md)
