# KyroDB

## Purpose

KyroDB is a single-node, gRPC-first vector database focused on low-latency retrieval with strict correctness and operational safety.

## Scope

Current repository scope:

- single-node engine and server (`kyrodb-engine`)
- gRPC data plane (insert/query/search/bulk operations)
- panic-contained gRPC request execution (handler panic => `INTERNAL`, process stays up)
- layered retrieval path (L1 cache, hot tier, HNSW cold tier)
- WAL/snapshot persistence and recovery
- auth, tenancy, rate limiting, usage accounting
- HTTP observability endpoints (`/health`, `/ready`, `/slo`, `/metrics`, `/usage`)

## Commands

```bash
# Build
cargo build -p kyrodb-engine
cargo build --release -p kyrodb-engine --bin kyrodb_server --bin kyrodb_backup
cargo build --release -p kyrodb-engine --features cli-tools

# Run server
./target/release/kyrodb_server --generate-config toml > config.toml
./target/release/kyrodb_server --config config.toml

# Observability probes
curl http://127.0.0.1:51051/health
curl http://127.0.0.1:51051/ready
curl http://127.0.0.1:51051/slo
curl http://127.0.0.1:51051/metrics

# Backup CLI
./target/release/kyrodb_backup --data-dir ./data --backup-dir ./backups create --description "full"
./target/release/kyrodb_backup --data-dir ./data --backup-dir ./backups list --format json
./target/release/kyrodb_backup --data-dir ./data --backup-dir ./backups verify <BACKUP_ID>
export BACKUP_ALLOW_CLEAR=true
./target/release/kyrodb_backup --data-dir ./data --backup-dir ./backups restore --backup-id <BACKUP_ID>

# Validation matrix
cargo fmt --all
cargo clippy --all-targets -- -D warnings
cargo test --all
cargo test -p kyrodb-engine --features cli-tools --test cli_integration_tests
cargo test -p kyrodb-engine --features loom --test loom_concurrency_tests --release
scripts/qa/run_soak_chaos_recovery.sh
scripts/qa/run_fuzz_smoke.sh
```

## Key Contracts

- default gRPC endpoint: `127.0.0.1:50051`
- default HTTP observability endpoint: `127.0.0.1:51051`
- `/usage` behavior:
  - `auth.enabled=false` => `404`
  - `auth.enabled=true` => API key required
  - `scope=all` => admin key required
- input validation rejects non-finite embeddings (`NaN`/`Inf`) on insert/search paths
- gRPC handler panics are converted to `INTERNAL` responses; server process continues serving
- persistence recovery defaults to `strict`; `best_effort` is only allowed in `benchmark` environment mode
- backup restore is destructive to target data directory and requires `BACKUP_ALLOW_CLEAR=true`
- incremental backups require new WAL entries since the parent backup (writes must occur after the parent); incremental creation is only possible after additional WAL activity
- backup create is fail-closed if snapshot/WAL source files change during archive creation; quiesce writes for deterministic backup windows

Pilot baseline note:

- `config.pilot.toml` / `config.pilot.yaml` require a real API key file at `auth.api_keys_file`

## Related Docs

- [docs/README.md](docs/README.md)
- [docs/QUICKSTART.md](docs/QUICKSTART.md)
- [docs/API_REFERENCE.md](docs/API_REFERENCE.md)
- [docs/CONFIGURATION_MANAGEMENT.md](docs/CONFIGURATION_MANAGEMENT.md)
- [docs/AUTHENTICATION.md](docs/AUTHENTICATION.md)
- [docs/OBSERVABILITY.md](docs/OBSERVABILITY.md)
- [docs/BACKUP_AND_RECOVERY.md](docs/BACKUP_AND_RECOVERY.md)
- [docs/OPERATIONS.md](docs/OPERATIONS.md)
- [docs/ENGINEERING_STATUS.md](docs/ENGINEERING_STATUS.md)

## License

Business Source License 1.1 (BSL 1.1). See [LICENSE](LICENSE).
