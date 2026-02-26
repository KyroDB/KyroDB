# Concurrency and Locking

## Purpose

Define concurrency constraints for engine and server changes.

## Scope

- lock-safety rules
- debug deadlock detection
- concurrency validation commands
- incident response checklist

## Commands

```bash
# Build debug server (deadlock detection active in debug)
cargo build -p kyrodb-engine --bin kyrodb_server

# Loom model checks
cargo test -p kyrodb-engine --features loom --test loom_concurrency_tests --release

# Full regression suite
cargo test --all

# Optional HTTP pressure check (server on gRPC 3030, HTTP 4030)
ab -n 10000 -c 50 http://127.0.0.1:4030/health
```

## Key Contracts

- never hold locks across blocking I/O or `.await`
- keep lock scopes minimal
- avoid unreviewed nested lock chains
- treat starvation risk as correctness risk

Debug behavior:

- deadlock detector runs in debug builds
- deadlock detection is disabled in release builds

## Incident Flow

1. capture logs/backtraces
2. reproduce in debug mode
3. add deterministic regression test
4. re-run full test matrix before merge

## Related Docs

- [ARCHITECTURE.md](ARCHITECTURE.md)
- [OPERATIONS.md](OPERATIONS.md)
