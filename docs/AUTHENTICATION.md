# Authentication & Multi-Tenancy

API key authentication with per-tenant configuration.

**Status**: The authentication module (`auth.rs`) is implemented but not yet integrated with the HTTP and gRPC servers. The configuration and API key format described below are ready for use once integration is complete.

## Configuration

**1. Enable authentication in config:**
```yaml
# config.yaml
auth:
  enabled: true
  api_keys_file: "data/api_keys.yaml"
```

**2. Create API keys file:**
```yaml
# data/api_keys.yaml
api_keys:
  - key: kyro_acme_corp_a3f9d8e2c1b4567890abcdef12345678
    tenant_id: acme_corp
    tenant_name: Acme Corporation
    max_qps: 1000
    max_vectors: 10_000_000
    enabled: true
    
  - key: kyro_startup_x_b4e8f1a9d2c3567890fedcba98765432
    tenant_id: startup_x
    tenant_name: Startup X
    max_qps: 100
    max_vectors: 100_000
    enabled: true
```

---

## API Key Format

```
kyro_<tenant_id>_<random_32_chars>

Examples:
- kyro_acme_corp_a3f9d8e2c1b4567890abcdef12345678
- kyro_startup_x_b4e8f1a9d2c3567890fedcba98765432
```

**Generate new key:**
```bash
TENANT_ID="newclient"
echo "kyro_${TENANT_ID}_$(openssl rand -hex 32)"
```

---

## Planned Features

Once authentication is integrated with the server:

- **Multi-Tenant Isolation**: Each tenant gets isolated namespace
- **Per-Tenant Rate Limiting**: Token bucket algorithm
- **Usage Tracking**: Query count, insert count, vector count per tenant

---

## Security Best Practices

**API Key Storage:**
- Store `api_keys.yaml` with restricted permissions (0600)
- Never commit API keys to version control
- Rotate keys regularly

```bash
chmod 600 data/api_keys.yaml
```
