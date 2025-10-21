# KyroDB Documentation Index

Complete guide to KyroDB - the fastest vector database for AI workloads.

## For New Users

**Start here:**
1. [**Quick Start**](QUICKSTART.md) - Get running in 5 minutes
2. [**API Reference**](API_REFERENCE.md) - HTTP endpoints and examples
3. [**Architecture**](ARCHITECTURE.md) - How KyroDB works

## For Operators

**Production deployment:**
1. [**Configuration Management**](CONFIGURATION_MANAGEMENT.md) - All config options explained
2. [**Backup & Recovery**](BACKUP_AND_RECOVERY.md) - Full and incremental backups, disaster recovery
3. [**Observability**](OBSERVABILITY.md) - Prometheus metrics, Grafana dashboards, alerting
4. [**Operations Guide**](OPERATIONS.md) - Troubleshooting common issues

## For Developers

**Advanced topics:**
1. [**Authentication**](AUTHENTICATION.md) - Multi-tenant API keys, rate limiting
2. [**Three-Tier Implementation**](THREE_TIER_IMPLEMENTATION.md) - Cache → Hot → Cold architecture
3. [**NDCG Implementation**](NDCG_IMPLEMENTATION.md) - Search quality metrics
4. [**Concurrency**](CONCURRENCY.md) - Lock-free reads, atomic swaps

## By Use Case

### "I want to get started quickly"
→ [Quick Start](QUICKSTART.md)

### "I need to deploy to production"
→ [Configuration](CONFIGURATION_MANAGEMENT.md) → [Backup](BACKUP_AND_RECOVERY.md) → [Observability](OBSERVABILITY.md)

### "Something is broken"
→ [Operations Guide](OPERATIONS.md)

### "I want to understand how it works"
→ [Architecture](ARCHITECTURE.md) → [Three-Tier Implementation](THREE_TIER_IMPLEMENTATION.md)

### "I need API documentation"
→ [API Reference](API_REFERENCE.md)

### "I need to restore from backup"
→ [Backup & Recovery Guide](BACKUP_AND_RECOVERY.md)

### "I need to set up monitoring"
→ [Observability Guide](OBSERVABILITY.md)

---

## Quick Links

**Common Tasks:**
- [Insert a vector](API_REFERENCE.md#post-v1insert)
- [Search for similar vectors](API_REFERENCE.md#post-v1search)
- [Create a backup](BACKUP_AND_RECOVERY.md#full-backup)
- [Restore from backup](BACKUP_AND_RECOVERY.md#restore-from-backup)
- [Check server health](API_REFERENCE.md#get-health)
- [View metrics](OBSERVABILITY.md#key-metrics)
- [Fix high latency](OPERATIONS.md#high-p99-latency)
- [Fix low cache hit rate](OPERATIONS.md#low-cache-hit-rate)

**Configuration Examples:**
- [Enable authentication](AUTHENTICATION.md#setup)
- [Configure backups](BACKUP_AND_RECOVERY.md#retention-policies)
- [Set up Prometheus](OBSERVABILITY.md#quick-setup)
- [Tune HNSW parameters](CONFIGURATION_MANAGEMENT.md#hnsw-configuration)

---

## External Resources

- [GitHub Repository](https://github.com/kyrodb/kyrodb)
- [Issue Tracker](https://github.com/kyrodb/kyrodb/issues)

---

## Need Help?

1. **Check the docs** - Use the index above to find relevant guides
2. **Search issues** - Someone may have had the same problem
4. **File an issue** - For bugs or feature requests
