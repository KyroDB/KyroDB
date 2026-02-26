# KyroDB Engine Fuzz Targets

This directory contains `cargo-fuzz` harnesses for security and correctness-critical input surfaces.

## Targets

- `auth_surface`: API key and tenant metadata validation in `AuthManager`.
- `tenant_surface`: tenant namespace parsing and result isolation in `TenantManager`.
- `api_validation_surface`: protobuf `SearchRequest`/`InsertRequest` decode + request validation contract.

## Run Locally

```bash
# One-time setup
cargo install cargo-fuzz --locked

# Smoke run all targets (short)
scripts/qa/run_fuzz_smoke.sh

# Long run (example)
FUZZ_TIME_PER_TARGET_SECS=300 scripts/qa/run_fuzz_smoke.sh

# Single target (example)
FUZZ_TARGETS="api_validation_surface" scripts/qa/run_fuzz_smoke.sh
```

## Notes

- Fuzzing uses nightly Rust and libFuzzer.
- Any crash or assertion failure is a correctness/security bug and should block promotion.
