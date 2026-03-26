# Configuration Management

## Purpose

Define how KyroDB configuration is loaded and validated.

## Scope

- config source precedence
- file generation and runtime overrides
- security/startup guardrails
- key config sections

## Commands

```bash
# Generate templates
./target/release/kyrodb_server --generate-config toml > config.toml
./target/release/kyrodb_server --generate-config yaml > config.yaml

# Run with config
./target/release/kyrodb_server --config config.toml

# CLI overrides
./target/release/kyrodb_server --config config.toml --port 50051 --data-dir ./data

# Environment overrides
export KYRODB__SERVER__PORT=50051
export KYRODB__CACHE__CAPACITY=50000
export KYRODB__HNSW__DIMENSION=768
./target/release/kyrodb_server
```

## Key Contracts

### Source Precedence

1. CLI flags
2. `KYRODB__...` environment variables
3. file config (`toml`/`yaml`)
4. defaults

### Critical Sections

- `server` (gRPC/HTTP bind + observability auth + TLS)
- `auth`
- `rate_limit`
- `cache`
- `hnsw`
- `persistence` (WAL/snapshot + `recovery_mode`)
- `timeouts`
- `environment`

### Cache/HSC Contract

- Outside `benchmark` mode, `cache.strategy` must be `learned` (HSC fail-closed).
- Base predictor threshold lives at `cache.admission_threshold`.
- Strategy-layer adaptive admission tuning lives under `cache.adaptive_admission`:
  - `enabled`
  - `target_utilization`
  - `control_interval_secs`
  - `max_bias`
- L1a semantic admission tuning lives under `cache.semantic`:
  - `high_confidence_threshold`
  - `low_confidence_threshold`
  - `semantic_similarity_threshold`
  - `max_cached_embeddings`
  - `similarity_scan_limit`
- Search-result training ingestion is bounded per request by `cache.search_access_log_top_n`.

### Startup Guardrails

`production`/`pilot` modes enforce fail-closed checks for insecure bind/auth combinations.

Examples:

- non-loopback production gRPC bind with `auth.enabled=false` is rejected
- non-loopback observability bind with `server.observability_auth=disabled` is rejected
- pilot mode requires stronger auth/rate-limit protections
- `persistence.recovery_mode=best_effort` is rejected unless `[environment] type = "benchmark"`

Recovery mode contract:

- `strict` (default): startup fails closed if manifest/snapshot/WAL evidence is incomplete/corrupted
- `best_effort`: degraded startup mode for benchmark workflows only

### `/usage` Independence

`/usage` behavior is tied to `auth.enabled`, not to `server.observability_auth`.

## Related Docs

- [AUTHENTICATION.md](AUTHENTICATION.md)
- [OBSERVABILITY.md](OBSERVABILITY.md)
- [OPERATIONS.md](OPERATIONS.md)
