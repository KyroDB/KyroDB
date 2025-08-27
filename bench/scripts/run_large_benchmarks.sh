#!/usr/bin/env bash
set -euo pipefail

# Orchestrate multi-scale benchmarks and collect a summary
# Requires the server running at $BASE

BASE=${BASE:-http://127.0.0.1:3030}
COMMIT=$(git rev-parse --short HEAD)
ROOT_DIR=$(git rev-parse --show-toplevel)
OUT_DIR="$ROOT_DIR/bench/results/${COMMIT}"

SCALES=${SCALES:-"100000 1000000 10000000"}
VAL_BYTES=${VAL_BYTES:-64}
READ_CONCURRENCY=${READ_CONCURRENCY:-64}
READ_SECONDS=${READ_SECONDS:-30}
DISTRIBUTIONS=${DISTRIBUTIONS:-"uniform"} # add zipf to include zipf; set ZIPF_THETA for zipf
ZIPF_THETA=${ZIPF_THETA:-1.2}

mkdir -p "$OUT_DIR/logs"

# Build bench once
cargo build -p bench --release

SUMMARY="$OUT_DIR/summary.csv"
echo "scale,distribution,rps,p50_us,p95_us,p99_us,zipf_theta" > "$SUMMARY"

echo "Starting runs (commit=$COMMIT) -> $OUT_DIR"

for dist in $DISTRIBUTIONS; do
  for n in $SCALES; do
    RUN_DIR="$OUT_DIR/${dist}_${n}"
    mkdir -p "$RUN_DIR"

    echo "Running dist=$dist scale=$n"
    extra_args=()
    if [[ "$dist" == "zipf" ]]; then
      extra_args+=("--zipf-theta" "$ZIPF_THETA")
    fi

    CSV="$RUN_DIR/results.csv"
    set -x
     "$ROOT_DIR/target/release/bench" \
      --base "$BASE" \
      --load-n "$n" \
      --val-bytes "$VAL_BYTES" \
      --read-concurrency "$READ_CONCURRENCY" \
      --read-seconds "$READ_SECONDS" \
      --dist "$dist" \
      "${extra_args[@]}" \
      --out-csv "$CSV" \
      --regime warm | tee "$RUN_DIR/run.out"
    set +x

    # Extract metrics row from CSV (bench header: base,dist,regime,reads_total,rps,p50_us,p95_us,p99_us,...)
    if [[ -f "$CSV" ]]; then
      rps=$(tail -n +2 "$CSV" | awk -F, '{print $5}' | tail -n1)
      p50=$(tail -n +2 "$CSV" | awk -F, '{print $6}' | tail -n1)
      p95=$(tail -n +2 "$CSV" | awk -F, '{print $7}' | tail -n1)
      p99=$(tail -n +2 "$CSV" | awk -F, '{print $8}' | tail -n1)
      echo "$n,$dist,$rps,$p50,$p95,$p99,$([[ "$dist" == "zipf" ]] && echo "$ZIPF_THETA" || echo "")" >> "$SUMMARY"
    else
      echo "WARN: missing $CSV"
    fi
  done
done

echo "Wrote summary to $SUMMARY"

# Optional: generate plots
if [[ -f "$ROOT_DIR/bench/scripts/generate_plots.py" ]]; then
  python3 "$ROOT_DIR/bench/scripts/generate_plots.py" || true
elif [[ -f "$ROOT_DIR/bench/scripts/generate_plot.py" ]]; then
  python3 "$ROOT_DIR/bench/scripts/generate_plot.py" || true
fi

echo "All done. Artifacts in $OUT_DIR"