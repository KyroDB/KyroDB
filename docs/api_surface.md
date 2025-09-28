# KyroDB HTTP Surface Audit

_Last updated: 2025-09-28_

This document captures the current HTTP routing surface exported by `engine/src/main.rs` and maps each endpoint to its handler and core engine dependencies. It also records how the benchmark harness exercises the surface.

## 1. Route → Handler → Dependency Matrix

| Endpoint | HTTP | Handler (function) | Primary Engine Calls | Subsystem Dependencies |
| --- | --- | --- | --- | --- |
| `/v1/put_fast/{key}` | `POST` | `create_ultra_fast_write_routes` | `PersistentEventLog::append_kv` | WAL append, group commit state |
| `/v1/get_fast/{key}` | `GET` | `create_ultra_fast_lookup_routes` | `PersistentEventLog::lookup_key_ultra_fast`, `PersistentEventLog::get` | Adaptive RMI (lookup), snapshot mmap, WAL cache |
| `/v1/lookup_ultra/{key}` | `GET` | `create_ultra_fast_lookup_routes` | `PersistentEventLog::lookup_key_ultra_fast`, `get_ultra_fast_pool` | Adaptive RMI, shared JSON buffer pool |
| `/v1/lookup_batch` | `POST` | `create_ultra_fast_lookup_routes` | `PersistentEventLog::lookup_keys_ultra_batch` | Adaptive RMI SIMD path, JSON encoding |
| `/v1/warmup` | `POST` | `create_admin_endpoints` | `PersistentEventLog::warmup` | Snapshot mmap prefetch, WAL cache |
| `/v1/snapshot` | `POST` | `create_admin_endpoints` | `PersistentEventLog::snapshot` | Snapshot writer |
| `/v1/rmi/build` | `POST` | `create_admin_endpoints` | `PersistentEventLog::build_rmi` | Adaptive RMI rebuild |
| `/v1/compact` | `POST` | `create_admin_endpoints` | `PersistentEventLog::compact_keep_latest_and_snapshot_stats` | Compactor, WAL manager |
| `/v1/health` | `GET` | `create_health_endpoints` | Static JSON | N/A |
| `/metrics` | `GET` | `create_health_endpoints` | `prometheus::gather` | Metrics registry |


> All legacy JSON PUT/lookup routes have been removed from the routing tree. Only the endpoints listed above remain reachable by default.

## 2. Benchmark Harness → Endpoint Mapping

The benchmark CLI (`bench/src/main.rs`) relies on the following HTTP calls:

| Benchmark Component | Code Location | Endpoint(s) Used |
| --- | --- | --- |
| Warmup PUT loop | `bench/src/main.rs::warmup_server` via `BenchClient::put` | `POST /v1/put_fast/{key}` |
| Warmup read sampling | `bench/src/main.rs::warmup_server` via `BenchClient::get` | `GET /v1/get_fast/{key}` |
| Steady-state reads | `bench/src/main.rs::run_workload` via `BenchClient::get` | `GET /v1/get_fast/{key}` |
| Steady-state writes | `bench/src/main.rs::run_workload` via `BenchClient::put` | `POST /v1/put_fast/{key}` |
| Batch workloads | `BenchClient::batch_lookup` | `POST /v1/lookup_batch` |
| RMI build hook | `warmup_server` | `POST /v1/rmi/build` |
| Warmup hook | `warmup_server` | `POST /v1/warmup` |

Shell wrappers invoke the same executable, so their endpoint footprint matches the table:

| Script | Path | Notes |
| --- | --- | --- |
| `bench/run_quick_benchmark.sh` | Quick suite | Runs `bench` binary with defaults (`put_fast`, `get_fast`, `lookup_batch`) |
| `bench/scripts/comprehensive_benchmark.sh` | Comprehensive suite | Same endpoints plus optional streaming mode (still uses point PUT/GET per `BenchClient`) |

## 3. Conclusions

- After trimming, the production surface exports **nine** HTTP endpoints, all aligned with the ultra-fast path.
- Benchmarks now exclusively use `/v1/put_fast`, `/v1/get_fast`, `/v1/lookup_ultra`, `/v1/lookup_batch`, and the admin controls needed for warm RMI state.
- `/v1/warmup` is implemented on the server to mirror the benchmark warmup contract.
