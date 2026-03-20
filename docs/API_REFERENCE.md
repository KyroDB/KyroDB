# API Reference

## Purpose

Define the KyroDB runtime API contract.

## Scope

- gRPC service surface (`KyroDBService`)
- HTTP observability endpoints
- request validation constraints
- auth header behavior

## Commands

```bash
# Observability endpoints
curl http://127.0.0.1:51051/health
curl http://127.0.0.1:51051/ready
curl http://127.0.0.1:51051/slo
curl http://127.0.0.1:51051/metrics

# Usage endpoint (auth-enabled mode)
curl -H "x-api-key: <API_KEY>" https://127.0.0.1:51051/usage
curl -H "authorization: Bearer <API_KEY>" https://127.0.0.1:51051/usage
curl -H "x-api-key: <ADMIN_API_KEY>" "https://127.0.0.1:51051/usage?scope=all"
```

Security Note: In production, always use HTTPS/TLS when transmitting API keys to prevent credential interception.
Local default configs run without TLS; for local-only testing use `http://127.0.0.1:51051/...`.

## Key Contracts

### gRPC Surface

Proto source:

- `engine/proto/kyrodb.proto`

RPC groups:

- writes: `Insert`, `BulkInsert`, `BulkLoadHnsw`, `Delete`, `BatchDelete`, `UpdateMetadata`
- reads: `Query`, `Search`, `BulkSearch`, `BulkQuery`
- admin: `Health`, `Metrics`, `FlushHotTier`, `CreateSnapshot`, `GetConfig`

Operation semantics:

- `Insert` is durable-first: successful writes are committed to the cold tier before the response returns.
- `UpdateMetadata` is durable-first: the cold tier is canonical, and the hot tier is only mirrored if the document is resident there.
- `UpdateMetadata.namespace` is a selector/boundary check, not a namespace-rewrite field; reserved tenant/namespace keys are preserved on update.
- Query/search responses strip reserved internal metadata keys such as `__tenant_id__`, `__tenant_idx__`, and `__namespace__`.
- Read APIs only surface documents backed by a canonical cold-tier record; L1a/hot-tier embeddings carry canonical coherence tokens (version + 128-bit integrity digest), are revalidated before they are served, hot-tier-only drift is ignored, and stale mirrors are scrubbed when detected.
- `FlushHotTier` drains/reconciles the hot-tier mirror; it does not make buffered writes durable because inserts are already durable before the RPC returns, and it will not let mirror state overwrite an existing canonical cold-tier record.

### Input Validation

- `InsertRequest.doc_id >= 1`
- `InsertRequest.embedding` non-empty and `<= 4096` dimensions
- `InsertRequest.embedding` values must be finite (`NaN`/`Inf` rejected)
- `SearchRequest.query_embedding` non-empty and `<= 4096` dimensions
- `SearchRequest.query_embedding` values must be finite (`NaN`/`Inf` rejected)
- `SearchRequest.k` in `1..=1000`
- `SearchRequest.ef_search <= 10000` (`0` means server default)

Tenant-mode doc ID mapping:

- tenant-local doc IDs must fit `u32`
- overflow returns `INVALID_ARGUMENT` with `DOC_ID_OUT_OF_RANGE`

### Failure Containment

- unexpected panic during gRPC handler execution is contained at the transport layer
- affected RPC returns `INTERNAL` (`internal server panic`)
- server process remains running for subsequent requests

### HTTP Endpoint Behavior

- `/health`: liveness
- `/ready`: readiness
- `/slo`: threshold + breach summary
- `/metrics`: Prometheus exposition
- `/usage`:
  - `auth.enabled=false` => `404`
  - `auth.enabled=true` => API key required
  - `scope=all` => admin key required

### Auth Headers

Accepted on auth-protected paths:

- `x-api-key: <key>`
- `authorization: Bearer <key>`

## Related Docs

- [AUTHENTICATION.md](AUTHENTICATION.md)
- [CONFIGURATION_MANAGEMENT.md](CONFIGURATION_MANAGEMENT.md)
- [OBSERVABILITY.md](OBSERVABILITY.md)
