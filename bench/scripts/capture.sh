#!/usr/bin/env bash
set -euo pipefail

# Quick single-run capture script using the bench binary (warm regime)
# Usage: capture.sh [N_KEYS] [VAL_BYTES] [DIST]
#   N_KEYS    default: 1000000
#   VAL_BYTES default: 64
#   DIST      default: uniform (or zipf)

ROOT_DIR=$(git rev-parse --show-toplevel)
cd "$ROOT_DIR"

N_KEYS=${1:-1000000}
VAL_BYTES=${2:-64}
DIST=${3:-uniform}
BASE=${BASE:-http://127.0.0.1:3030}
AUTH_TOKEN=${AUTH_TOKEN:-}
COMMIT=$(git rev-parse --short HEAD)
OUT="bench/results/${COMMIT}/quick"
READ_CONCURRENCY=${READ_CONCURRENCY:-64}
READ_SECONDS=${READ_SECONDS:-30}

mkdir -p "$OUT"

# Capture config
{
  echo "commit=$COMMIT"
  echo "n_keys=$N_KEYS"
  echo "val_bytes=$VAL_BYTES"
  echo "dist=$DIST"
  echo "concurrency=$READ_CONCURRENCY"
  echo "duration=$READ_SECONDS"
  echo "base=$BASE"
  [[ -n "$AUTH_TOKEN" ]] && echo "auth_token=***" || true
  env | grep '^KYRODB_' || true
} > "$OUT/config.txt"

# Scrape metrics before
curl -s "$BASE/metrics" > "$OUT/metrics.before.prom" || true

# Build bench
cargo build -p bench --release >/dev/null

# Build args
ARGS=(
  --base "$BASE"
  --load-n "$N_KEYS"
  --val-bytes "$VAL_BYTES"
  --read-concurrency "$READ_CONCURRENCY"
  --read-seconds "$READ_SECONDS"
  --dist "$DIST"
  --out-csv "$OUT/bench_results.csv"
  --regime warm
)
if [[ -n "$AUTH_TOKEN" ]]; then
  ARGS+=(--auth-token "$AUTH_TOKEN")
fi

# Run bench (warm regime triggers snapshot + RMI build + warmup)
set -x
 "$ROOT_DIR/target/release/bench" "${ARGS[@]}" | tee "$OUT/bench.out"
set +x

# Scrape metrics after
curl -s "$BASE/metrics" > "$OUT/metrics.after.prom" || true

echo "Saved artifacts to $OUT"
