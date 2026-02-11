# Pilot Launch Checklist

Go/no-go checklist for external startup pilots.

## 1. Security Baseline (Must Pass)

- [ ] `environment.type=pilot` is set.
- [ ] `auth.enabled=true` and `auth.api_keys_file` exists.
- [ ] `rate_limit.enabled=true`.
- [ ] `server.observability_auth` is set to `metrics_and_slo` or `all` (not `disabled`).
- [ ] `persistence.allow_fresh_start_on_recovery_failure=false`.
- [ ] If `server.host` is non-loopback, TLS is enabled with valid cert/key paths.
- [ ] API keys are unique per startup tenant and stored outside git.
- [ ] Audit logging enabled for authentication, authorization, and data operations.
- [ ] Audit logs include tenant ID, operation type, timestamp, and result.

Reference profile: `config.pilot.toml` / `config.pilot.yaml`.

## 2. Tenant Setup (Must Pass)

- [ ] One API key per startup tenant.
- [ ] Tenant quotas configured (`max_qps`, `max_vectors`).
- [ ] Namespace plan agreed (`prod`, `staging`, etc.).
- [ ] Tenant isolation validated using cross-tenant negative tests.

## 3. Reliability and Recovery (Must Pass)

- [ ] WAL and snapshot paths writable on target host.
- [ ] Backup create/list/restore verified in staging.
- [ ] Recovery startup test completed from snapshot + WAL tail.
- [ ] `/health`, `/ready`, `/slo`, `/metrics` endpoints verified.
- [ ] Rollback procedure documented and tested (config revert, traffic cutover, data sync).
- [ ] Rollback decision authority and communication plan defined.

## 4. Performance Readiness (Must Pass)

- [ ] Workload-specific baseline captured before pilot cutover.
- [ ] Target `k`, `ef_search`, and embedding dimension are fixed.
- [ ] Capacity plan defined (expected vectors + QPS + concurrency).
- [ ] Rollback threshold defined (latency/error/cache-hit regressions).

## 5. Usage Billing Readiness (Must Pass for Paid Pilot)

- [ ] Per-tenant usage tracking enabled (auth mode).
- [ ] `/usage` endpoint returns tenant snapshots.
- [ ] Query/storage metering mapping agreed in commercial terms.
- [ ] Billing export cadence defined (daily/weekly).

## 6. Onboarding Runbook (Execution)

1. Day 0: Success metrics and rollback criteria signed off.
2. Day 1: Secure config + tenant key provisioning.
3. Day 2: Sample corpus ingest and validation.
4. Day 3: Shadow traffic replay.
5. Day 4: Canary rollout.
6. Day 5+: Ramp, tune, and monitor.

## 7. Go/No-Go Decision

Go only if all critical checklist sections pass:

- Security baseline
- Tenant isolation
- Recovery drills
- SLO behavior under pilot traffic
- Metering visibility for paid conversion
