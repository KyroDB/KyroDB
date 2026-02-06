# Authentication and multi-tenancy

## Known Limitations

**`max_vectors` is not enforced on inserts.** The `max_vectors` field in `api_keys.yaml` is validated at load time (must be > 0), but the server **will not reject inserts** after a tenant exceeds this value. Operators must not rely on `max_vectors` for quota enforcement.

**Recommendations until enforcement is implemented:**
- Use Prometheus metrics (e.g., per-tenant vector count) with alerting to detect when tenants approach or exceed their configured `max_vectors`.
- Apply external enforcement at the proxy or infrastructure level (e.g., disk quotas, rate-limiting gateways).
- When insert-time enforcement is added in a future release, it will **not be retroactive**: tenants that already exceed their `max_vectors` limit will not have existing vectors removed, but new inserts will be rejected once enforcement is active.

---

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
- `max_qps` is optional; if omitted or set to `0`, the server falls back to the global defaults in `rate_limit` (see Configuration Guide). If `rate_limit.enabled=false`, tenants without `max_qps` are effectively unlimited.
- `max_vectors` is validated when loading `api_keys.yaml` (must be > 0), but it is **not** enforced as a hard quota on inserts yet. See the **Known Limitations** section at the top of this document for details and recommended mitigations.
  Enforcement is planned after Phase 0 hardening.

## Observability endpoint protection

KyroDB exposes HTTP endpoints intended for monitoring and orchestration:

- `GET /metrics`
- `GET /health`
- `GET /ready`
- `GET /slo`

These endpoints are often deployed on internal networks. If you expose them outside a trusted boundary, protect them.

`server.observability_auth` is a string enum (snake_case) with these values:

- `disabled` (default): no auth on any observability endpoint.
- `metrics_and_slo`: require auth for `/metrics` and `/slo` only.
- `all`: require auth for `/metrics`, `/health`, `/ready`, and `/slo`.

Any non-`disabled` value requires `auth.enabled=true` and a valid `auth.api_keys_file`.

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
  - `__tenant_id__`: the authenticated tenant's string identifier (e.g., `"acme_corp"`).
  - `__tenant_idx__`: the server-assigned numeric tenant index used for internal doc_id mapping.
  - `__namespace__`: scopes resources within a tenant to a logical partition (e.g., environment, project, or dataset). When provided in the gRPC request, the server overwrites this key with the canonical value derived from the request's `namespace` field. For details on namespace behavior, see the [API Reference](API_REFERENCE.md).
- `max_qps` is enforced per tenant and returns `RESOURCE_EXHAUSTED` on overflow.

## `doc_id` range limits

Tenant-local `doc_id` values must fit within a 32-bit unsigned integer (`<= 4,294,967,295`). Any request that supplies a larger value is rejected.

- gRPC: `INVALID_ARGUMENT` with message `DOC_ID_OUT_OF_RANGE: doc_id exceeds tenant-local max (u32)`.
- If you front gRPC with an HTTP gateway, this typically maps to HTTP 400 Bad Request.

Example (gateway-style JSON error payload):

```json
{
  "error": {
    "code": "DOC_ID_OUT_OF_RANGE",
    "message": "doc_id exceeds tenant-local max (u32)",
    "status": 400
  }
}
```

Remediation: use smaller tenant-local IDs or map external IDs/UUIDs to a 32-bit space.

## Security practices

- Store `api_keys.yaml` with restricted permissions (0600)
- Never commit API keys to version control
- Rotate keys regularly

```bash
chmod 600 data/api_keys.yaml
```
