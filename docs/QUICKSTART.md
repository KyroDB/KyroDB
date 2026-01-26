# Quick Start Guide

Get KyroDB running in 5 minutes.

## Prerequisites

- **Rust (stable)**: [Install Rust](https://rustup.rs/)
- **4GB RAM minimum**
- **Linux or macOS** (Windows untested)

## 1. Install KyroDB

```bash
# Clone repository
git clone https://github.com/vatskishan03/KyroDB.git
cd KyroDB

# Build release binaries with all CLI tools enabled
cargo build --release --features cli-tools

# Binaries created in ./target/release/
ls target/release/kyrodb_*
```

**Note**: The `--features cli-tools` flag is required to build all binaries (server, load tester, backup tool). Without it, you'll get a compile error on the backup binary.

## 2. Start the Server

```bash
# Start with default settings (port 50051)
./target/release/kyrodb_server

# Or specify custom port and data directory
./target/release/kyrodb_server \
  --port 50051 \
  --data-dir ./data
```

**Server starts in seconds**. Logs indicate:
```
gRPC server listening on 127.0.0.1:50051
HTTP observability on http://127.0.0.1:51051 (default = gRPC port + 1000)
```

## 3. Insert Your First Vector

KyroDB uses **gRPC** for all vector operations. Here is a minimal Python example that inserts one vector using the `Insert(InsertRequest) → InsertResponse` RPC.

```bash
# Install Python gRPC tooling
python -m pip install --upgrade grpcio grpcio-tools

# Generate Python stubs from the repo proto
python -m grpc_tools.protoc \
  -Iengine/proto \
  --python_out=. \
  --grpc_python_out=. \
  engine/proto/kyrodb.proto
```

```python
import grpc

import kyrodb_pb2
import kyrodb_pb2_grpc


def main() -> None:
  channel = grpc.insecure_channel("127.0.0.1:50051")
  stub = kyrodb_pb2_grpc.KyroDBServiceStub(channel)

  # The server's embedding dimension must match the request embedding length.
  # Default config uses 768, so we build a simple 768-dim vector.
  embedding = [0.0] * 768
  embedding[0] = 0.1
  embedding[1] = 0.2

  resp = stub.Insert(
    kyrodb_pb2.InsertRequest(
      doc_id=1,
      embedding=embedding,
      metadata={"source": "quickstart"},
      namespace="default",
    )
  )
  print(resp)

  channel.close()


if __name__ == "__main__":
  main()
```

For advanced usage and all RPCs, see [engine/proto/kyrodb.proto](../engine/proto/kyrodb.proto) and [API Reference](API_REFERENCE.md).

## 4. Search for Similar Vectors

Vector search operations use gRPC. See the [API Reference](API_REFERENCE.md#search) for required fields and examples.

## 5. Check System Health

```bash
# Health check
curl http://127.0.0.1:51051/health

# Metrics (Prometheus format)
curl http://127.0.0.1:51051/metrics
```

## What's Next?

### Use Configuration File

```bash
# Generate example config
./target/release/kyrodb_server --generate-config yaml > config.yaml

# Edit config.yaml to customize:
# - Cache capacity
# - Vector dimensions
# - Performance thresholds

# Start with config
./target/release/kyrodb_server --config config.yaml
```

See [Configuration Guide](CONFIGURATION_MANAGEMENT.md) for all options.

### Set Up Backups

```bash
# Create full backup (default behavior without --incremental flag)
./target/release/kyrodb_backup \
  --data-dir ./data \
  --backup-dir ./backups \
  create \
  --description "My first backup"

# List all backups
./target/release/kyrodb_backup \
  --data-dir ./data \
  --backup-dir ./backups \
  list

# Restore from a backup (requires backup ID from list command)
export BACKUP_ALLOW_CLEAR=true
./target/release/kyrodb_backup \
  --data-dir ./data \
  --backup-dir ./backups \
  restore \
  --backup-id <BACKUP_ID>
```

See [Backup Guide](BACKUP_AND_RECOVERY.md) for backup strategies.

### Monitor Performance

```bash
# View real-time metrics
watch -n 2 'curl -s http://127.0.0.1:51051/metrics | grep kyrodb_'

# Key metrics to watch:
# - kyrodb_cache_hit_rate: Cache hit rate (0.0-1.0)
# - kyrodb_query_latency_ns{percentile="99"}: P99 query latency in nanoseconds
# - kyrodb_hnsw_searches_total: HNSW k-NN searches performed
```

See [Observability Guide](OBSERVABILITY.md) for monitoring setup.

### Production Deployment

Before going to production:

1. **Read [Operations Guide](OPERATIONS.md)** - Common failure scenarios
2. **Set up monitoring** - Prometheus + Grafana dashboards
3. **Configure backups** - Automated daily backups
4. **Test recovery** - Practice restoring from backup

## Common Issues

### Port already in use
```bash
# Use different port
./target/release/kyrodb_server --port 50053
```

### Permission denied on data directory
```bash
# Create data directory first
mkdir -p ./data
chmod 755 ./data
```

### Server won't start
```bash
# Check logs
./target/release/kyrodb_server 2>&1 | tee server.log

# Common causes:
# - Port already in use
# - Data directory doesn't exist
# - Insufficient memory (need 4GB+)
```

## Getting Help

- **Documentation**: All guides in `/docs` folder
- **Issues**: [GitHub Issues](https://github.com/vatskishan03/KyroDB/issues)
- **Logs**: Check server output for errors

## Next Steps

Choose your path:

**Developers**: Read [API Reference](API_REFERENCE.md)  
**Operators**: Read [Operations Guide](OPERATIONS.md)  
**Performance Tuning**: Read [Configuration Guide](CONFIGURATION_MANAGEMENT.md)
