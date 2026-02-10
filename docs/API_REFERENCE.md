# API Reference

KyroDB provides two interfaces:

1. **gRPC API** (primary) - Vector operations, high performance
2. **HTTP observability endpoints** - Monitoring and health checks

## gRPC API (Vector Operations)

All vector operations use gRPC for performance and efficiency. 

### Protocol Details

- **Server**: `127.0.0.1:50051` by default (configurable via `--port` or config file)
- **TLS**: configurable in the server config; see [Configuration Management](CONFIGURATION_MANAGEMENT.md)

### Service Definition

Complete service definitions are in `engine/proto/kyrodb.proto`.

**KyroDBService** includes:

**Write Operations**:
- `Insert(InsertRequest) → InsertResponse` - Insert single vector
- `BulkInsert(stream InsertRequest) → InsertResponse` - Bulk insert (streaming)
- `BulkLoadHnsw(stream InsertRequest) → BulkLoadResponse` - Bulk load directly into HNSW (benchmark/migration)
- `Delete(DeleteRequest) → DeleteResponse` - Delete by ID
- `BatchDelete(BatchDeleteRequest) → BatchDeleteResponse` - Delete by IDs or filter
- `UpdateMetadata(UpdateMetadataRequest) → UpdateMetadataResponse` - Update metadata only

**Read Operations**:
- `Query(QueryRequest) → QueryResponse` - Point lookup by ID
- `Search(SearchRequest) → SearchResponse` - k-NN search
- `BulkSearch(stream SearchRequest) → stream SearchResponse` - Batch search
- `BulkQuery(BulkQueryRequest) → BulkQueryResponse` - Bulk point lookup

**Admin**:
- `Health(HealthRequest) → HealthResponse` - Health check
- `Metrics(MetricsRequest) → MetricsResponse` - Metrics
- `FlushHotTier(FlushRequest) → FlushResponse` - Force flush hot tier
- `CreateSnapshot(SnapshotRequest) → SnapshotResponse` - Create backup snapshot
- `GetConfig(ConfigRequest) → ConfigResponse` - Get server config

### Message Types

## Insert

**InsertRequest**:
```protobuf
message InsertRequest {
  uint64 doc_id = 1;                    // Document ID (non-zero)
  repeated float embedding = 2;         // Vector (dimension from config)
  map<string, string> metadata = 3;     // Optional metadata
  string namespace = 4;                 // Optional namespace (multi-tenancy)
}
```

## Search

**SearchRequest**:
```protobuf
message SearchRequest {
  repeated float query_embedding = 1;   // Query vector
  uint32 k = 2;                         // Top-k results (1-1000)
  float min_score = 3;                  // Optional score threshold
  string namespace = 4;                 // Optional namespace filter
  bool include_embeddings = 5;          // Include embeddings in results
  MetadataFilter filter = 6;            // Structured metadata filter
  uint32 ef_search = 7;                 // Optional ef_search override (0 = default)
}
```

### Example: Python Client

```python
import grpc
from kyrodb_pb2 import InsertRequest, SearchRequest
from kyrodb_pb2_grpc import KyroDBServiceStub

# Connect to server
# WARNING: grpc.insecure_channel is for local development and testing ONLY.
# It transmits data in plaintext with no authentication or encryption.
# For production, use grpc.secure_channel with TLS credentials:
#   credentials = grpc.ssl_channel_credentials(root_cert, private_key, cert_chain)
#   channel = grpc.secure_channel('host:port', credentials)
channel = grpc.insecure_channel('127.0.0.1:50051')
stub = KyroDBServiceStub(channel)

# Insert a vector
response = stub.Insert(InsertRequest(
    doc_id=1,
    embedding=[0.1, 0.2, 0.3, 0.4]
))
if not response.success:
  raise RuntimeError(response.error)
print(f"Insert OK (tier={response.tier}, inserted_at={response.inserted_at})")

# Search for similar vectors
results = stub.Search(SearchRequest(
  query_embedding=[0.1, 0.2, 0.3, 0.4],
  k=10,
  min_score=-1.0,
  include_embeddings=False
))

for result in results.results:
    print(f"doc_id: {result.doc_id}, score: {result.score}")

channel.close()
```

### Example: Go Client

```go
package main

import (
    pb "github.com/kyrodb/kyrodb/engine/proto"
    "google.golang.org/grpc"
)

func main() {
    conn, _ := grpc.Dial("127.0.0.1:50051", grpc.WithInsecure())
    defer conn.Close()
    client := pb.NewKyroDBServiceClient(conn)

    // Insert
    resp, _ := client.Insert(context.Background(), &pb.InsertRequest{
        DocId:     1,
        Embedding: []float32{0.1, 0.2, 0.3, 0.4},
    })
    println("Inserted:", resp.DocId)
}
```

## HTTP Observability Endpoints

These endpoints are read-only and used for monitoring:

- `GET /health` - Liveness check
- `GET /ready` - Readiness check
- `GET /slo` - SLO breach status
- `GET /metrics` - Prometheus metrics
- `GET /usage` - Per-tenant usage snapshots (billing/ops)

### Base URL

```
http://localhost:51051
```

HTTP port is `gRPC port + 1000` (e.g., if gRPC is 50051, HTTP is 51051).

**Important**: HTTP API is for observability/monitoring only. **All vector operations must use gRPC**.

`/usage` is sensitive and is intended for authenticated internal operators.

### Endpoints

#### GET /health

Check if server is healthy.

**Request:**
```bash
curl http://localhost:51051/health
```

**Response (healthy):**
```json
{"status": "healthy", "uptime_seconds": 12345}
```

**Status Codes:**
- `200 OK`: Server healthy
- `503 Service Unavailable`: Server unhealthy

---

#### GET /ready

Check if server is ready to accept traffic (used by load balancers).

**Request:**
```bash
curl http://localhost:51051/ready
```

**Response:**
```json
{"ready": true, "status": "ready"}
```

Use the `ready` boolean field for programmatic checks.

**Status Codes:**
- `200 OK`: Ready
- `503 Service Unavailable`: Not ready (starting up or shutting down)

---

#### GET /metrics

Prometheus-compatible metrics.

**Request:**
```bash
curl http://localhost:51051/metrics
```

**Response (text/plain):**
```
# HELP kyrodb_queries_total Total number of queries
# TYPE kyrodb_queries_total counter
kyrodb_queries_total 1234

# HELP kyrodb_query_latency_ns Query latency percentiles in nanoseconds
# TYPE kyrodb_query_latency_ns gauge
kyrodb_query_latency_ns{percentile="50"} 4792
kyrodb_query_latency_ns{percentile="95"} 68042
kyrodb_query_latency_ns{percentile="99"} 68042

# HELP kyrodb_cache_hit_rate Cache hit rate (0.0-1.0)
# TYPE kyrodb_cache_hit_rate gauge
kyrodb_cache_hit_rate 0.735

# HELP kyrodb_cache_size Current cache size (entries)
# TYPE kyrodb_cache_size gauge
kyrodb_cache_size 10000
```

See [Observability Guide](OBSERVABILITY.md) for complete metric reference.

---

#### GET /slo

Current SLO status and breach information.

**Request:**
```bash
curl http://localhost:51051/slo
```

**Response:**
```json
{
  "current_metrics": {
    "availability": 1.0,
    "cache_hit_rate": 0.735,
    "error_rate": 0.0,
    "p99_latency_ns": 68042
  },
  "slo_breaches": {
    "availability": false,
    "cache_hit_rate": false,
    "error_rate": false,
    "p99_latency": false
  },
  "slo_thresholds": {
    "max_error_rate": 0.001,
    "min_availability": 0.999,
    "min_cache_hit_rate": 0.7,
    "p99_latency_ns": 10000000
  }
}
```

---

#### GET /usage

Per-tenant usage snapshot for billing and operations.

When `auth.enabled=true`, this endpoint requires an API key (same headers as gRPC).
When `auth.enabled=false`, usage tracking is disabled and this endpoint returns `404`.

**Request:**
```bash
curl -H "x-api-key: <API_KEY>" http://localhost:51051/usage
```

**Response:**
```json
{
  "generated_at": 1739250000,
  "totals": {
    "query_count": 12345,
    "vector_count": 100000,
    "storage_bytes": 307200000,
    "billable_events": 12567
  },
  "tenants": [
    {
      "tenant_id": "acme_corp",
      "query_count": 12000,
      "insert_count": 550,
      "delete_count": 50,
      "vector_count": 50000,
      "storage_bytes": 153600000,
      "storage_mb": 146.48,
      "storage_gb": 0.14,
      "billable_events": 12600
    }
  ]
}
```

---

## Authentication

API key authentication is optional (disabled by default). Enable it in `config.yaml`:

Note: for `environment.type=production`, non-loopback `server.host` requires `auth.enabled=true` at startup.

```yaml
auth:
  enabled: true
  api_keys_file: "data/api_keys.yaml"
```

Example `data/api_keys.yaml`:

```yaml
api_keys:
  - key: kyro_acme_corp_a3f9d8e2c1b4567890abcdef12345678
    tenant_id: acme_corp
    tenant_name: Acme Corporation
    max_qps: 1000          # optional (0 or omitted uses server defaults)
    max_vectors: 10000000
    created_at: "2025-10-01T00:00:00Z"  # optional (ISO 8601)
    enabled: true
```

> **`max_vectors` enforcement**: `max_vectors` is validated at API-key load time (must be > 0) and enforced on write paths. Inserts that would exceed the tenant quota are rejected with `RESOURCE_EXHAUSTED`.

For details (including observability endpoint auth), see [Authentication](AUTHENTICATION.md).

### Using with gRPC

Include API key in metadata:

```python
metadata = [("authorization", "Bearer kyro_acme_corp_a3f9d8e2c1b4567890abcdef12345678")]
channel = grpc.aio.secure_channel('127.0.0.1:50051', credentials=...)
stub = KyroDBServiceStub(channel)

# Calls automatically include metadata
response = await stub.Insert(request, metadata=metadata)
```

---

## Rate Limiting

Configured in `config.yaml`:

```yaml
rate_limit:
  enabled: true
  max_qps_global: 10000
  max_qps_per_connection: 1000
```

Rate limit errors return gRPC status code `RESOURCE_EXHAUSTED`.

---

## Configuration

See [Configuration Management Guide](CONFIGURATION_MANAGEMENT.md) for:
- Server port configuration
- TLS certificate setup
- Performance tuning parameters
- Timeout configuration

---

## Troubleshooting

### Connection refused
- Check server is running: `./target/release/kyrodb_server`
- Verify port (default 50051): `netstat -an | grep 50051`
- Check firewall rules

### Dimension mismatch error
- Vector embedding dimension must match server config
- Check config: `curl http://localhost:51051/slo`
- Look for `embedding_dimension` in response

### Timeout errors
- Increase timeout in gRPC client
- Check server logs for performance issues
- Verify network latency to server

---

## See Also

- [gRPC Proto Definition](../engine/proto/kyrodb.proto)
- [Configuration Guide](CONFIGURATION_MANAGEMENT.md)
- [Observability Guide](OBSERVABILITY.md)
- [Operations Guide](OPERATIONS.md)
