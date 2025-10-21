# Quick Start Guide

Get KyroDB running in 5 minutes.

## Prerequisites

- **Rust 1.70+**: [Install Rust](https://rustup.rs/)
- **4GB RAM minimum**
- **Linux or macOS** (Windows untested)

## 1. Install KyroDB

```bash
# Clone repository
git clone https://github.com/vatskishan03/KyroDB.git
cd KyroDB

# Build release binaries
cargo build --release

# Binaries created in ./target/release/
ls target/release/kyrodb_*
```

## 2. Start the Server

```bash
# Start with default settings (port 50052)
./target/release/kyrodb_server

# Or specify custom port and data directory
./target/release/kyrodb_server \
  --port 50052 \
  --data-dir ./data
```

**Server starts in seconds**. You should see:
```
KyroDB server listening on 127.0.0.1:50052
HTTP observability on http://127.0.0.1:51052
```

## 3. Insert Your First Vector

```bash
# Using curl (HTTP API)
curl -X POST http://127.0.0.1:51052/v1/insert \
  -H "Content-Type: application/json" \
  -d '{
    "doc_id": "doc_1",
    "embedding": [0.1, 0.2, 0.3, 0.4]
  }'
```

**Response**: `{"status": "ok"}`

## 4. Search for Similar Vectors

```bash
# Find 5 nearest neighbors
curl -X POST http://127.0.0.1:51052/v1/search \
  -H "Content-Type: application/json" \
  -d '{
    "query_embedding": [0.1, 0.2, 0.3, 0.4],
    "k": 5
  }'
```

**Response**:
```json
{
  "results": [
    {"doc_id": "doc_1", "score": 1.0}
  ]
}
```

## 5. Check System Health

```bash
# Health check
curl http://127.0.0.1:51052/health

# Metrics (Prometheus format)
curl http://127.0.0.1:51052/metrics
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
# Create full backup
./target/release/kyrodb_backup create-full \
  --data-dir ./data \
  --backup-dir ./backups \
  --description "My first backup"

# List backups
./target/release/kyrodb_backup list \
  --backup-dir ./backups
```

See [Backup Guide](BACKUP_AND_RECOVERY.md) for backup strategies.

### Monitor Performance

```bash
# View real-time metrics
watch -n 2 'curl -s http://127.0.0.1:51052/metrics | grep kyrodb_'

# Key metrics to watch:
# - kyrodb_cache_hit_rate: Should be >40%
# - kyrodb_query_latency_p99: Should be <10ms
# - kyrodb_hnsw_vector_count: Total vectors stored
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
