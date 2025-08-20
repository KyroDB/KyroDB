#!/usr/bin/env bash
set -euo pipefail

N_KEYS=${1:-1000000}
VAL_BYTES=${2:-64}
DIST=${3:-uniform}
BASE=${BASE:-http://127.0.0.1:3030}
COMMIT=$(git rev-parse --short HEAD)
OUT=bench/results/${COMMIT}
mkdir -p "$OUT"

# capture config
{
  echo "commit=$COMMIT"
  echo "n_keys=$N_KEYS"
  echo "val_bytes=$VAL_BYTES"
  echo "endpoint=lookup_fast"
  echo "dist=$DIST"
  echo "concurrency=${READ_CONCURRENCY:-64}"
  echo "warmup_seconds=${WARMUP_SECONDS:-5}"
  echo "duration=${READ_SECONDS:-30}"
  env | grep '^KYRODB_' || true
} > "$OUT/config.txt"

# Scrape metrics before
curl -s "$BASE/metrics" > "$OUT/metrics.prom" || true

# Build benches
cargo build -p bench --release >/dev/null

# Load+bench with RMI build step
READ_CONCURRENCY=${READ_CONCURRENCY:-64} \
READ_SECONDS=${READ_SECONDS:-30} \
WARMUP_SECONDS=${WARMUP_SECONDS:-5} \
  target/release/bench --base "$BASE" --endpoint lookup_fast \
  --load-n "$N_KEYS" --val-bytes "$VAL_BYTES" \
  --read-concurrency "$READ_CONCURRENCY" --warmup-seconds "$WARMUP_SECONDS" --read-seconds "$READ_SECONDS" \
  --dist "$DIST" --csv-out "$OUT/latency.csv" | tee "$OUT/bench.out"

# re-scrape metrics after run
curl -s "$BASE/metrics" > "$OUT/metrics.after.prom" || true

echo "Saved artifacts to $OUT"
