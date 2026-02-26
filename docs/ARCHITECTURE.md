# Architecture

## Purpose

Describe the runtime structure of the single-node KyroDB server.

## Scope

- gRPC data plane request flow
- cache/hot-tier/cold-tier layering
- persistence and recovery boundaries
- tenancy and metadata isolation points

## Commands

```bash
# Start server with explicit config
./target/release/kyrodb_server --config config.toml

# Verify observability plane
curl http://127.0.0.1:51051/health
curl http://127.0.0.1:51051/ready
curl http://127.0.0.1:51051/metrics
```

## Key Contracts

### Runtime Layers

1. L1 caches
- L1a: document cache
- L1b: semantic query-result cache

2. L2 hot tier
- recent writes and fast replayable state

3. L3 cold tier
- HNSW-backed long-lived vector index

### Search Path

- request validation
- tenant/namespace/filter scoping
- cache lookup
- hot-tier lookup
- HNSW lookup
- response shaping and score conversion

### Write Path

- request validation and quota checks
- WAL mutation recording
- hot-tier insertion/update
- background flush into cold tier

### Persistence Boundary

- `persistence.data_dir` contains manifest/snapshots/WAL files
- startup recovery depends on manifest + snapshot + WAL continuity
- strict recovery policy is expected outside benchmark mode

### Multi-Tenant Boundary

- auth-enabled mode maps tenant-local IDs into global IDs
- reserved metadata keys (`__tenant_id__`, `__tenant_idx__`, `__namespace__`) are server-managed
- cross-tenant visibility is blocked at query and usage layers

## Related Docs

- [CONFIGURATION_MANAGEMENT.md](CONFIGURATION_MANAGEMENT.md)
- [AUTHENTICATION.md](AUTHENTICATION.md)
- [OBSERVABILITY.md](OBSERVABILITY.md)
- [BACKUP_AND_RECOVERY.md](BACKUP_AND_RECOVERY.md)
