# Authentication and Tenancy

## Purpose

Define API-key auth, tenant isolation, and quota behavior.

## Scope

- API key file format and validation
- request headers accepted by server
- observability endpoint auth behavior
- tenant quota and ID constraints

## Commands

```bash
# Generate key material (32 hex bytes after tenant prefix)
TENANT_ID="newclient"
echo "kyro_${TENANT_ID}_$(openssl rand -hex 16)"

# Protect key file permissions
chmod 600 data/api_keys.yaml

# Usage endpoint with API key
curl -H "x-api-key: <API_KEY>" http://127.0.0.1:51051/usage

# Alternate bearer header
curl -H "authorization: Bearer <API_KEY>" http://127.0.0.1:51051/usage
```

## Key Contracts

### Config

```yaml
auth:
  enabled: true
  api_keys_file: "data/api_keys.yaml"
```

### API Keys File

```yaml
api_keys:
  - key: kyro_tenant_a_0123456789abcdef0123456789abcdef
    tenant_id: tenant_a
    tenant_name: Tenant A
    max_qps: 1000
    max_vectors: 100000
    enabled: true
```

### Request Auth Rules

- `auth.enabled=false`
  - gRPC does not require API key
  - `/usage` returns `404`
- `auth.enabled=true`
  - gRPC requires API key
  - `/usage` requires API key

Accepted auth headers:

- `x-api-key: <key>`
- `authorization: Bearer <key>`

### Observability Auth Rules

`server.observability_auth` controls `/metrics`, `/health`, `/ready`, `/slo`:

- `disabled`
- `metrics_and_slo`
- `all`

`/usage` is controlled independently by `auth.enabled`.

### Tenant Isolation and Quotas

- `max_vectors` is enforced on write path (`RESOURCE_EXHAUSTED` on overflow)
- tenant-local `doc_id` must fit `u32`
- out-of-range tenant-local IDs return `INVALID_ARGUMENT` (`DOC_ID_OUT_OF_RANGE`)
- reserved metadata keys are server-owned and overwritten if client-provided

## Related Docs

- [API_REFERENCE.md](API_REFERENCE.md)
- [CONFIGURATION_MANAGEMENT.md](CONFIGURATION_MANAGEMENT.md)
- [OBSERVABILITY.md](OBSERVABILITY.md)
- [OPERATIONS.md](OPERATIONS.md)
