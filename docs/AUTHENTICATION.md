# Authentication and multi-tenancy

API key authentication is integrated with the gRPC server. When `auth.enabled` is set, all gRPC requests must include an API key.

HTTP observability endpoints (`/metrics`, `/health`, `/ready`, `/slo`) are unauthenticated by default for operational simplicity, but can (and typically should) be protected in production via either:

- Binding the HTTP server to a restricted interface using `server.http_host` (e.g., loopback only)
- Requiring API key auth using `server.observability_auth` (same headers as gRPC)

## Configuration

Enable authentication and point to the API keys file:

```yaml
# config.yaml
auth:
  enabled: true
  api_keys_file: "data/api_keys.yaml"
```

## API keys file

```yaml
# data/api_keys.yaml
api_keys:
  - key: kyro_acme_corp_a3f9d8e2c1b4567890abcdef12345678
    tenant_id: acme_corp
    tenant_name: Acme Corporation
    max_qps: 1000
    max_vectors: 10000000
    created_at: "2025-10-01T00:00:00Z"
    enabled: true

  - key: kyro_startup_x_b4e8f1a9d2c3567890fedcba98765432
    tenant_id: startup_x
    tenant_name: Startup X
    max_qps: 100
    max_vectors: 100000
    enabled: true
```

Notes:

- `created_at` is optional but must be ISO 8601 if present.
- `max_vectors` is validated when loading `api_keys.yaml` (must be > 0), but it is not enforced as a hard quota on inserts yet.
  In other words, the server currently will not reject inserts after a tenant exceeds `max_vectors`.

## Observability endpoint protection

KyroDB exposes HTTP endpoints intended for monitoring and orchestration:

- `GET /metrics`
- `GET /health`
- `GET /ready`
- `GET /slo`

These endpoints are often deployed on internal networks. If you expose them outside a trusted boundary, protect them.

Example: bind observability to loopback and require auth for `/metrics` and `/slo`:

```yaml
server:
  http_host: "127.0.0.1"
  observability_auth: metrics_and_slo

auth:
  enabled: true
  api_keys_file: "data/api_keys.yaml"
```

## API key format

```
kyro_<tenant_id>_<random_32_chars>
```

Examples:

- `kyro_acme_corp_a3f9d8e2c1b4567890abcdef12345678`
- `kyro_startup_x_b4e8f1a9d2c3567890fedcba98765432`

Generate a key:

```bash
TENANT_ID="newclient"
echo "kyro_${TENANT_ID}_$(openssl rand -hex 16)"
```

## Request headers

gRPC requests accept either header:

- `x-api-key: <key>`
- `authorization: Bearer <key>`

Example in Python:

```python
stub.Search(request, metadata=[("x-api-key", "kyro_acme_corp_a3f9d8e2c1b4567890abcdef12345678")])
```

## Tenant scoping

When authentication is enabled:

- `doc_id` values are tenant-scoped; the server maps them to a global ID internally.
- `doc_id` must fit within a 32-bit unsigned integer for each tenant.
- Reserved metadata keys `__tenant_id__`, `__tenant_idx__`, and `__namespace__` are managed by the server and will be overwritten if provided by clients.
- `max_qps` is enforced per tenant and returns `RESOURCE_EXHAUSTED` on overflow.

## Security practices

- Store `api_keys.yaml` with restricted permissions (0600)
- Never commit API keys to version control
- Rotate keys regularly

```bash
chmod 600 data/api_keys.yaml
```
