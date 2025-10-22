# Authentication & Multi-Tenancy

Secure API key authentication with per-tenant rate limiting and usage tracking.

## Setup

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

**3. Use API key in requests:**
```bash
curl -H "Authorization: Bearer kyro_acme_corp_..." \
  http://localhost:51051/v1/insert
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
echo "kyro_<tenant_id>_$(openssl rand -hex 32)"
```

---

## Features

### Multi-Tenant Isolation
- Each tenant gets isolated namespace
- Document IDs automatically prefixed: `<tenant_id>:<doc_id>`
- Search results filtered to tenant's documents only

### Per-Tenant Rate Limiting
- Token bucket algorithm (refills at configured rate)
- Prevents one tenant from DoS-ing others
- Returns `429 Too Many Requests` when limit exceeded

### Usage Tracking
Tracks per tenant:
- Query count
- Insert count
- Vector count
- Storage bytes

Export usage stats:
```bash
# Stats exported to CSV every 5 minutes (configurable)
cat data/usage_stats.csv
```

---

## Configuration

```yaml
# config.yaml
auth:
  enabled: true
  api_keys_file: "data/api_keys.yaml"
  usage_stats_file: "data/usage_stats.csv"
  usage_export_interval_secs: 300  # Export every 5 minutes
```

**File permissions:**
```bash
chmod 600 data/api_keys.yaml  # Protect API keys
```

---

## Security

**API Key Storage:**
- Store `api_keys.yaml` with restricted permissions (0600)
- Never commit API keys to version control
- Rotate keys regularly (monthly recommended)

**Rate Limiting:**
- Per-tenant limits prevent DoS attacks
- Global limits protect server capacity
- Burst capacity allows traffic spikes

**Tenant Isolation:**
- Document IDs automatically namespaced
- Search results filtered to tenant only
- No cross-tenant data leakage

---

## Operations

### Add New Tenant

```bash
# 1. Generate API key
KEY="kyro_newclient_$(openssl rand -hex 32)"

# 2. Add to api_keys.yaml
echo "  - key: $KEY" >> data/api_keys.yaml
echo "    tenant_id: newclient" >> data/api_keys.yaml
echo "    tenant_name: New Client" >> data/api_keys.yaml
echo "    max_qps: 100" >> data/api_keys.yaml
echo "    max_vectors: 100000" >> data/api_keys.yaml
echo "    enabled: true" >> data/api_keys.yaml

# 3. Reload server or restart
kill -HUP $(pidof kyrodb_server)
```

### Monitor Usage

```bash
# View usage stats
cat data/usage_stats.csv

# Watch for rate limiting
tail -f /var/log/kyrodb/server.log | grep "Rate limit"
```

### Rotate API Key

```bash
# 1. Generate new key
NEW_KEY="kyro_tenant_$(openssl rand -hex 32)"

# 2. Add new key to api_keys.yaml (keep old key)
# 3. Reload server
kill -HUP $(pidof kyrodb_server)

# 4. Update client to use new key
# 5. Remove old key from api_keys.yaml after grace period
# 6. Reload server again
```

---

## Performance

Authentication overhead per request:
- API key validation: ~50ns
- Rate limit check: ~100ns
- Usage tracking: ~20ns
- **Total: ~170ns** (negligible vs 1ms query)

No performance impact on query path.
