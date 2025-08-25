# KyroDB — Fast durable KV with a production RMI

Status: Alpha

Quickstart

- Build: `cargo build -p engine --release` (binary at `target/release/kyrodb-engine`)
- Run: `target/release/kyrodb-engine serve 127.0.0.1 8080`
- Health: `curl -s http://127.0.0.1:8080/health`
- Offset: `curl -s http://127.0.0.1:8080/v1/offset`
- Put: `curl -sX POST http://127.0.0.1:8080/v1/put -H 'content-type: application/json' -d '{"key":1,"value":"v"}'`
- Get(fast): `curl -i http://127.0.0.1:8080/v1/get_fast/1`

Warm start

- Auto warm on boot: set env `KYRODB_WARM_ON_START=1`
- Manual: `POST /v1/rmi/build` (if needed) then `POST /v1/warmup`

Security/ops

- Auth: start with `--auth-token <TOKEN>`; send `Authorization: Bearer <TOKEN>`
- Rate limiting (per-IP):
  - Admin: KYRODB_RL_ADMIN_RPS (default 2), KYRODB_RL_ADMIN_BURST (5)
  - Data: KYRODB_RL_DATA_RPS (5000), KYRODB_RL_DATA_BURST (10000)
- Metrics: `/metrics` (Prometheus)
- TLS: run behind a reverse proxy (see README.md for Caddy/Nginx examples)

Benchmarks

- Use `/v1/lookup_fast/{k}` for index-only or `/v1/get_fast/{k}` for full Get
- Bench client: see `bench/README.bench.md`

Build info

- Endpoint: `/build_info` returns commit and enabled features

More

- Full docs in `README.md` and `docs/`
