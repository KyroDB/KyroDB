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
