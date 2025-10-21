# API Reference

Complete HTTP API documentation.

## Base URL

```
http://localhost:51052
```

Default HTTP port is `gRPC port + 1000` (e.g., gRPC on 50052 → HTTP on 51052).

## Authentication

Currently disabled by default. Enable in `config.yaml`:

```yaml
auth:
  enabled: true
  api_keys_file: /etc/kyrodb/api_keys.yaml
```

When enabled, include API key in requests:
```bash
curl -H "X-API-Key: kyrodb_your_api_key_here" http://localhost:51052/v1/insert
```

## Endpoints

### Health & Monitoring

#### GET /health

Check if server is healthy.

**Request:**
```bash
curl http://localhost:51052/health
```

**Response (healthy):**
```json
{"status": "healthy"}
```

**Response (unhealthy):**
```json
{
  "status": "unhealthy",
  "reason": "SLO breach: P99 latency 15.2ms > 10ms threshold"
}
```

**Status Codes:**
- `200 OK`: Server healthy
- `503 Service Unavailable`: Server unhealthy

---

#### GET /ready

Check if server is ready to accept traffic (used by load balancers).

**Request:**
```bash
curl http://localhost:51052/ready
```

**Response:**
```json
{"status": "ready"}
```

**Status Codes:**
- `200 OK`: Ready
- `503 Service Unavailable`: Not ready (starting up or shutting down)

---

#### GET /metrics

Prometheus-compatible metrics.

**Request:**
```bash
curl http://localhost:51052/metrics
```

**Response (text/plain):**
```
# HELP kyrodb_query_latency_p99 Query latency P99 in milliseconds
# TYPE kyrodb_query_latency_p99 gauge
kyrodb_query_latency_p99 2.5

# HELP kyrodb_cache_hit_rate Cache hit rate (0-1)
# TYPE kyrodb_cache_hit_rate gauge
kyrodb_cache_hit_rate 0.451

# HELP kyrodb_hnsw_vector_count Total vectors in HNSW index
# TYPE kyrodb_hnsw_vector_count gauge
kyrodb_hnsw_vector_count 10000

...
```

See [Observability Guide](OBSERVABILITY.md) for all metrics.

---

#### GET /slo

Current SLO status and breach information.

**Request:**
```bash
curl http://localhost:51052/slo
```

**Response:**
```json
{
  "status": "ok",
  "metrics": {
    "p99_latency_ms": 2.5,
    "error_rate_5m": 0.001,
    "cache_hit_rate": 0.451,
    "availability_5m": 0.9998
  },
  "thresholds": {
    "p99_latency_ms": 10.0,
    "error_rate_5m": 0.01,
    "cache_hit_rate": 0.40,
    "availability_5m": 0.995
  },
  "breaches": []
}
```

**Response (with breaches):**
```json
{
  "status": "breach",
  "breaches": [
    {
      "metric": "p99_latency_ms",
      "current": 15.2,
      "threshold": 10.0,
      "since": "2025-10-20T14:30:00Z"
    }
  ]
}
```

---

### Vector Operations

#### POST /v1/insert

Insert a vector.

**Request:**
```bash
curl -X POST http://localhost:51052/v1/insert \
  -H "Content-Type: application/json" \
  -d '{
    "doc_id": "doc_123",
    "embedding": [0.1, 0.2, 0.3, ...]
  }'
```

**Request Body:**
```json
{
  "doc_id": "string (required)",
  "embedding": "array of f32 (required, must match configured dimension)"
}
```

**Response (success):**
```json
{
  "status": "ok",
  "doc_id": "doc_123"
}
```

**Response (error):**
```json
{
  "error": "Embedding dimension mismatch: expected 768, got 384"
}
```

**Status Codes:**
- `200 OK`: Inserted successfully
- `400 Bad Request`: Invalid input (wrong dimension, malformed JSON)
- `500 Internal Server Error`: Server error (disk full, WAL failure)

---

#### POST /v1/query

Query a vector by ID.

**Request:**
```bash
curl -X POST http://localhost:51052/v1/query \
  -H "Content-Type: application/json" \
  -d '{
    "doc_id": "doc_123"
  }'
```

**Response (found):**
```json
{
  "doc_id": "doc_123",
  "embedding": [0.1, 0.2, 0.3, ...],
  "found": true
}
```

**Response (not found):**
```json
{
  "doc_id": "doc_123",
  "found": false
}
```

---

#### POST /v1/search

k-NN vector search.

**Request:**
```bash
curl -X POST http://localhost:51052/v1/search \
  -H "Content-Type: application/json" \
  -d '{
    "query_embedding": [0.1, 0.2, 0.3, ...],
    "k": 10
  }'
```

**Request Body:**
```json
{
  "query_embedding": "array of f32 (required)",
  "k": "int (required, 1-1000)"
}
```

**Response:**
```json
{
  "results": [
    {
      "doc_id": "doc_123",
      "score": 0.95,
      "distance": 0.05
    },
    {
      "doc_id": "doc_456",
      "score": 0.87,
      "distance": 0.13
    }
  ],
  "query_time_ms": 2.5
}
```

**Notes:**
- Results ordered by similarity (highest score first)
- `score`: Cosine similarity (1.0 = identical, 0 = orthogonal)
- `distance`: Cosine distance (0 = identical, 2 = opposite)

---

### Administrative

#### POST /v1/flush

Flush hot tier to cold tier (HNSW).

**Request:**
```bash
curl -X POST http://localhost:51052/v1/flush \
  -H "Content-Type: application/json" \
  -d '{"force": true}'
```

**Request Body:**
```json
{
  "force": "bool (optional, default: false)"
}
```

**Response:**
```json
{
  "status": "ok",
  "docs_flushed": 1523,
  "duration_ms": 150.5
}
```

**When to use:**
- Before backup (ensures all data persisted)
- Before shutdown (prevents data loss)
- When hot tier size exceeds threshold

---

#### POST /admin/circuit-breaker/reset

Manually reset circuit breaker (dangerous).

**Request:**
```bash
curl -X POST http://localhost:51052/admin/circuit-breaker/reset
```

**Response:**
```json
{
  "status": "ok",
  "message": "Circuit breaker reset to CLOSED state"
}
```

**WARNING**: Only use if you've fixed the root cause. Resetting without fixing will trigger immediate re-open.

---

## Error Responses

All errors follow this format:

```json
{
  "error": "Human-readable error message",
  "error_code": "DIMENSION_MISMATCH",
  "details": {
    "expected": 768,
    "actual": 384
  }
}
```

**Common Error Codes:**

| Code | Meaning | Fix |
|------|---------|-----|
| `DIMENSION_MISMATCH` | Embedding dimension wrong | Use correct dimension (check config) |
| `INVALID_JSON` | Malformed JSON | Fix JSON syntax |
| `DOC_NOT_FOUND` | Document doesn't exist | Check doc_id spelling |
| `INTERNAL_ERROR` | Server error | Check logs, may need restart |
| `CIRCUIT_BREAKER_OPEN` | WAL writes disabled | Wait 60s for auto-reset or fix disk issue |
| `RATE_LIMIT_EXCEEDED` | Too many requests | Slow down request rate |

---

## Rate Limiting

Configured in `config.yaml`:

```yaml
rate_limiting:
  enabled: true
  global_qps: 10000  # Max queries per second globally
  per_connection_qps: 1000  # Max per client
```

**When rate limited:**
```json
{
  "error": "Rate limit exceeded",
  "error_code": "RATE_LIMIT_EXCEEDED",
  "retry_after_ms": 1000
}
```

**Status Code:** `429 Too Many Requests`

---

## Pagination

Search results are not paginated. Specify desired `k` value:

```bash
# Get top 100 results
curl -X POST http://localhost:51052/v1/search \
  -d '{"query_embedding": [...], "k": 100}'
```

**Maximum k**: 1000 (configurable in `config.yaml`)

---

## Batch Operations

Currently not supported. Insert vectors one at a time:

```bash
for id in {1..1000}; do
  curl -X POST http://localhost:51052/v1/insert \
    -d "{\"doc_id\": \"doc_$id\", \"embedding\": [...]}"
done
```

**Future**: Batch insert API planned for Phase 1.

---

## Client Examples

### Python

```python
import requests
import json

# Insert vector
response = requests.post(
    "http://localhost:51052/v1/insert",
    json={
        "doc_id": "doc_1",
        "embedding": [0.1] * 768
    }
)
print(response.json())

# Search
response = requests.post(
    "http://localhost:51052/v1/search",
    json={
        "query_embedding": [0.1] * 768,
        "k": 10
    }
)
results = response.json()["results"]
for result in results:
    print(f"{result['doc_id']}: {result['score']}")
```

### JavaScript

```javascript
// Insert vector
const insert = await fetch("http://localhost:51052/v1/insert", {
  method: "POST",
  headers: {"Content-Type": "application/json"},
  body: JSON.stringify({
    doc_id: "doc_1",
    embedding: Array(768).fill(0.1)
  })
});

// Search
const search = await fetch("http://localhost:51052/v1/search", {
  method: "POST",
  headers: {"Content-Type": "application/json"},
  body: JSON.stringify({
    query_embedding: Array(768).fill(0.1),
    k: 10
  })
});

const results = await search.json();
console.log(results.results);
```

### cURL

```bash
# Health check
curl http://localhost:51052/health

# Insert
curl -X POST http://localhost:51052/v1/insert \
  -H "Content-Type: application/json" \
  -d '{"doc_id": "doc_1", "embedding": [0.1, 0.2, 0.3]}'

# Search
curl -X POST http://localhost:51052/v1/search \
  -H "Content-Type: application/json" \
  -d '{"query_embedding": [0.1, 0.2, 0.3], "k": 5}'
```

---

## Timeouts

Default timeouts (configurable):

| Operation | Timeout |
|-----------|---------|
| Insert | 10 seconds |
| Query | 5 seconds |
| Search | 10 seconds |
| Flush | 60 seconds |

Configure in `config.yaml`:
```yaml
server:
  read_timeout_secs: 30
  write_timeout_secs: 30
```

---

## Versioning

Current API version: **v1**

Future versions will be prefixed: `/v2/insert`, `/v2/search`, etc.

All v1 endpoints remain backwards-compatible.
