# Quick Start

## Purpose

Bring up a local KyroDB instance with validated commands.

## Scope

- build server and backup binaries
- start server
- validate endpoints
- run basic backup flow

## Commands

```bash
# Build
git clone https://github.com/vatskishan03/KyroDB.git
cd KyroDB
cargo build --release -p kyrodb-engine --bin kyrodb_server --bin kyrodb_backup

# Optional CLI utilities + validation binaries
cargo build --release -p kyrodb-engine --features cli-tools

# Start server
./target/release/kyrodb_server --generate-config toml > config.toml
./target/release/kyrodb_server --config config.toml

# `kyrodb_server --config ...` runs in the foreground (blocking).
# Use a second terminal for probes/backups, or run it in background:
./target/release/kyrodb_server --config config.toml >/tmp/kyrodb.log 2>&1 &
# For long-running environments, prefer a process manager (systemd/screen/tmux).

# Probe endpoints
curl http://127.0.0.1:51051/health
curl http://127.0.0.1:51051/ready
curl http://127.0.0.1:51051/slo
curl http://127.0.0.1:51051/metrics

# Backup smoke flow
./target/release/kyrodb_backup --data-dir ./data --backup-dir ./backups create --description "quickstart-full"
./target/release/kyrodb_backup --data-dir ./data --backup-dir ./backups list --format json
./target/release/kyrodb_backup --data-dir ./data --backup-dir ./backups verify <BACKUP_ID>
```

Restore requires explicit destructive confirmation:

```bash
export BACKUP_ALLOW_CLEAR=true
./target/release/kyrodb_backup --data-dir ./data --backup-dir ./backups restore --backup-id <BACKUP_ID>
```

## Key Contracts

- default gRPC endpoint: `127.0.0.1:50051`
- default HTTP observability endpoint: `127.0.0.1:51051`
- `/usage` returns `404` when `auth.enabled=false`

## Related Docs

- [API_REFERENCE.md](API_REFERENCE.md)
- [AUTHENTICATION.md](AUTHENTICATION.md)
- [CONFIGURATION_MANAGEMENT.md](CONFIGURATION_MANAGEMENT.md)
- [OPERATIONS.md](OPERATIONS.md)
