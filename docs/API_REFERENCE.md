# API Reference

KyroDB provides two interfaces:

1. **gRPC API** (primary) - Vector operations, high performance
2. **HTTP API** (observability only) - Monitoring and health checks

## gRPC API (Vector Operations)

All vector operations use gRPC for performance and efficiency. 

### Protocol Details

- **Server**: `127.0.0.1:50051` (default, configurable via `--port`)
- **Credentials**: TLS optional (disable in dev with environment: `KYRODB_GRPC_TLS=false`)
- **Timeout**: 30 seconds (configurable per request)

### Service Definition

Complete service definitions are in `engine/proto/kyrodb.proto`.

**KyroDBService** includes:

**Write Operations**:
- `Insert(InsertRequest) → InsertResponse` - Insert single vector
- `BulkInsert(stream InsertRequest) → InsertResponse` - Bulk insert (streaming)
- `Delete(DeleteRequest) → DeleteResponse` - Delete by ID

**Read Operations**:
- `Query(QueryRequest) → QueryResponse` - Point lookup by ID
- `Search(SearchRequest) → SearchResponse` - k-NN search
- `BulkSearch(stream SearchRequest) → stream SearchResponse` - Batch search

**Admin**:
- `Health(HealthRequest) → HealthResponse` - Health check
- `Metrics(MetricsRequest) → MetricsResponse` - Metrics
- `FlushHotTier(FlushRequest) → FlushResponse` - Force flush hot tier
- `CreateSnapshot(SnapshotRequest) → SnapshotResponse` - Create backup snapshot
- `GetConfig(ConfigRequest) → ConfigResponse` - Get server config

### Message Types

**InsertRequest**:
```protobuf
message InsertRequest {
  uint64 doc_id = 1;                    // Document ID (non-zero)
  repeated float embedding = 2;         // Vector (dimension from config)
  map<string, string> metadata = 3;     // Optional metadata
  string namespace = 4;                 // Optional namespace (multi-tenancy)
}
```

**SearchRequest**:
```protobuf
message SearchRequest {
  repeated float query_embedding = 1;   // Query vector
  uint32 k = 2;                         // Top-k results (1-1000)
  string namespace = 3;                 // Optional namespace filter
}
```

### Example: Python Client

```python
import grpc
from kyrodb_pb2 import InsertRequest, SearchRequest
from kyrodb_pb2_grpc import KyroDBServiceStub

# Connect to server
channel = grpc.aio.secure_channel(
    '127.0.0.1:50051',
    grpc.aio.ssl_channel_credentials()
)
stub = KyroDBServiceStub(channel)

# Insert a vector
response = await stub.Insert(InsertRequest(
    doc_id=1,
    embedding=[0.1, 0.2, 0.3, 0.4]
))
print(f"Inserted: {response.doc_id}")

# Search for similar vectors
results = await stub.Search(SearchRequest(
    query_embedding=[0.1, 0.2, 0.3, 0.4],
    k=10
))

for result in results.results:
    print(f"doc_id: {result.doc_id}, score: {result.score}")

await channel.close()
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

## HTTP API (Observability Only)

### Base URL

```
http://localhost:51051
```

HTTP port is `gRPC port + 1000` (e.g., if gRPC is 50051, HTTP is 51051).

**Important**: HTTP API is for observability/monitoring only. **All vector operations must use gRPC**.

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
    "p99_latency_ns": 1000000
  }
}
```

---

## Authentication

API key authentication is available (disabled by default).

### Enable in config.yaml

```yaml
auth:
  enabled: true
  api_keys_file: /etc/kyrodb/api_keys.yaml
```

### API Key Format

File: `/etc/kyrodb/api_keys.yaml`

```yaml
api_keys:
  - key: "kyrodb_prod_xyz123"
    namespace: "production"
    permissions: ["read", "write", "admin"]
  - key: "kyrodb_read_only_abc456"
    namespace: "staging"
    permissions: ["read"]
```

### Using with gRPC

Include API key in metadata:

```python
metadata = [('authorization', f'Bearer kyrodb_prod_xyz123')]
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
  max_qps_per_client: 1000
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
