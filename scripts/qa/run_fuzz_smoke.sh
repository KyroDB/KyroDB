#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
FUZZ_DIR="$ROOT_DIR/engine/fuzz"

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "error: required command not found: $1" >&2
    exit 1
  fi
}

require_cmd cargo
require_cmd bash

if ! cargo fuzz --help >/dev/null 2>&1; then
  echo "error: cargo-fuzz is not installed (expected 'cargo fuzz')." >&2
  echo "hint: cargo install cargo-fuzz --locked" >&2
  exit 1
fi

if [[ ! -d "$FUZZ_DIR" ]]; then
  echo "error: fuzz directory not found: $FUZZ_DIR" >&2
  exit 1
fi

TIME_PER_TARGET_SECS="${FUZZ_TIME_PER_TARGET_SECS:-20}"
MAX_INPUT_LEN="${FUZZ_MAX_INPUT_LEN:-4096}"
RSS_LIMIT_MB="${FUZZ_RSS_LIMIT_MB:-2048}"
SANITIZER="${FUZZ_SANITIZER:-address}"

TARGETS=(
  auth_surface
  tenant_surface
  api_validation_surface
)

if [[ -n "${FUZZ_TARGETS:-}" ]]; then
  # shellcheck disable=SC2206
  TARGETS=(${FUZZ_TARGETS})
fi

run_target() {
  local target="$1"
  echo "[fuzz] target=$target sanitizer=$SANITIZER time=${TIME_PER_TARGET_SECS}s max_len=$MAX_INPUT_LEN"

  (
    cd "$FUZZ_DIR"
    cargo +nightly fuzz run "$target" --sanitizer "$SANITIZER" -- \
      -max_total_time="$TIME_PER_TARGET_SECS" \
      -max_len="$MAX_INPUT_LEN" \
      -rss_limit_mb="$RSS_LIMIT_MB"
  )
}

for target in "${TARGETS[@]}"; do
  run_target "$target"
done

echo "[fuzz] smoke run complete"
