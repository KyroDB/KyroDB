# Quick Start Guide

Get KyroDB running in 5 minutes.

## Prerequisites

- **Rust (stable)**: [Install Rust](https://rustup.rs/)
- **Python 3.10+** (for official SDK usage)
- **4GB RAM minimum**
- **Linux or macOS** (Windows untested)

## 1. Install KyroDB

```bash
# Clone repository
git clone https://github.com/vatskishan03/KyroDB.git
cd KyroDB

# Build release binary for the kyrodb-engine package
cargo build --release -p kyrodb-engine

# Optional: build validation binaries and enable table/progress output in CLI tools
cargo build --release -p kyrodb-engine --features cli-tools

# Binaries created in ./target/release/
ls target/release/kyrodb_*
```

**Note**: `cli-tools` is optional. When built without `cli-tools`, `kyrodb_backup` defaults to JSON output and table/progress output is disabled. The validation binaries (`validation_24h`, `validation_enterprise`) require `cli-tools`.

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

## 3. Insert and Search with the Official Python SDK

KyroDB’s recommended client path is the official SDK (`kyrodb`).

Install:

```bash
python -m pip install --upgrade kyrodb
```

Save as `quickstart_sdk.py`:

```python
from kyrodb import KyroDBClient


def main() -> None:
    # For local development this target can run without TLS.
    # For non-loopback targets, configure TLS and API key auth.
    with KyroDBClient(target="127.0.0.1:50051", api_key="dev_local_key") as client:
        client.wait_for_ready(timeout_s=5.0)

        embedding = [0.0] * 768
        embedding[0] = 0.1
        embedding[1] = 0.2

        ack = client.insert(
            doc_id=1,
            embedding=embedding,
            metadata={"source": "quickstart"},
            namespace="default",
        )
        print("insert:", ack.success, ack.tier)

        result = client.search(
            query_embedding=embedding,
            k=10,
            namespace="default",
        )
        print("search total_found:", result.total_found)
        for hit in result.results:
            print("doc_id:", hit.doc_id, "score:", hit.score)


if __name__ == "__main__":
    main()
```

Run:

```bash
python quickstart_sdk.py
```

For full SDK coverage, see `kyrodb-python/README.md` and `kyrodb-python/docs/api-reference.md`.

## 4. Low-Level gRPC (Optional)

Use raw protobuf/gRPC only for custom non-SDK clients or protocol-level testing.
Service and message definitions live in `engine/proto/kyrodb.proto`.

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
