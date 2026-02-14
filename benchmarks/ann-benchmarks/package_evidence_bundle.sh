#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

OUT_ROOT="${OUT_ROOT:-$ROOT_DIR/target/ann_inproc_parallel}"
OUTPUT_DIR="${OUTPUT_DIR:-$OUT_ROOT}"
RUN_IDS_CSV=""
EXTRA_LOGS_CSV=""

usage() {
  cat <<'USAGE'
Package completed ANN benchmark runs into a reproducible evidence bundle.

Usage:
  benchmarks/ann-benchmarks/package_evidence_bundle.sh --run-ids CSV [options]

Options:
  --run-ids CSV        Required. Run directory ids under --out-root.
  --out-root PATH      Root containing run directories (default: target/ann_inproc_parallel)
  --output-dir PATH    Directory for resulting tarball (default: --out-root)
  --extra-logs CSV     Optional additional files to include in bundle.
  -h, --help
USAGE
}

trim() {
  local s="$1"
  s="${s#"${s%%[![:space:]]*}"}"
  s="${s%"${s##*[![:space:]]}"}"
  printf '%s' "${s}"
}

require_arg() {
  local flag="$1"
  local value="${2-}"
  if [[ -z "${value}" ]]; then
    echo "missing value for ${flag}" >&2
    exit 1
  fi
}

pick_summary_json() {
  local run_dir="$1"
  if [[ -f "${run_dir}/aggregate/summary/inproc_summary.json" ]]; then
    echo "${run_dir}/aggregate/summary/inproc_summary.json"
    return 0
  fi
  if [[ -f "${run_dir}/summary/inproc_summary.json" ]]; then
    echo "${run_dir}/summary/inproc_summary.json"
    return 0
  fi
  return 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --run-ids) require_arg "$1" "${2-}"; RUN_IDS_CSV="$2"; shift 2 ;;
    --out-root) require_arg "$1" "${2-}"; OUT_ROOT="$2"; shift 2 ;;
    --output-dir) require_arg "$1" "${2-}"; OUTPUT_DIR="$2"; shift 2 ;;
    --extra-logs) require_arg "$1" "${2-}"; EXTRA_LOGS_CSV="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage; exit 1 ;;
  esac
done

if [[ -z "$RUN_IDS_CSV" ]]; then
  echo "--run-ids is required" >&2
  usage
  exit 1
fi

mkdir -p "$OUTPUT_DIR"

RUN_IDS=()
IFS=',' read -r -a RUN_IDS_RAW <<<"$RUN_IDS_CSV"
for raw in "${RUN_IDS_RAW[@]}"; do
  run_id="$(trim "$raw")"
  if [[ -n "${run_id}" ]]; then
    RUN_IDS+=("${run_id}")
  fi
done
if [[ "${#RUN_IDS[@]}" -eq 0 ]]; then
  echo "no valid run ids parsed from --run-ids" >&2
  exit 1
fi

RUN_IDS_JOINED="$(IFS=,; echo "${RUN_IDS[*]}")"
for run_id in "${RUN_IDS[@]}"; do
  run_dir="${OUT_ROOT}/${run_id}"
  if [[ ! -d "$run_dir" ]]; then
    echo "missing run directory: $run_dir" >&2
    exit 1
  fi
  if ! summary_json="$(pick_summary_json "$run_dir")"; then
    echo "run not complete (missing summary json): $run_dir" >&2
    exit 1
  fi
done

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
SAFE_IDS="$(echo "$RUN_IDS_JOINED" | tr ', ' '__' | tr -cs '[:alnum:]_.-' '_')"
BUNDLE_BASE="ann_evidence_${SAFE_IDS}_${STAMP}"
BUNDLE_PATH="${OUTPUT_DIR}/${BUNDLE_BASE}.tar.gz"
MANIFEST_PATH="${OUTPUT_DIR}/${BUNDLE_BASE}.manifest.txt"
CHECKSUM_PATH="${BUNDLE_PATH}.sha256"

{
  echo "created_utc=${STAMP}"
  echo "git_commit=$(git rev-parse HEAD)"
  echo "git_branch=$(git rev-parse --abbrev-ref HEAD)"
  echo "out_root=${OUT_ROOT}"
  echo "run_ids=${RUN_IDS_JOINED}"
} >"$MANIFEST_PATH"

TAR_INPUTS=()
for run_id in "${RUN_IDS[@]}"; do
  TAR_INPUTS+=("${OUT_ROOT}/${run_id}")
done
TAR_INPUTS+=("$MANIFEST_PATH")

if [[ -n "$EXTRA_LOGS_CSV" ]]; then
  IFS=',' read -r -a EXTRA_LOGS_RAW <<<"$EXTRA_LOGS_CSV"
  for raw in "${EXTRA_LOGS_RAW[@]}"; do
    item="$(trim "$raw")"
    if [[ -n "$item" ]] && [[ -f "$item" ]]; then
      TAR_INPUTS+=("$item")
    fi
  done
fi

TAR_INPUTS_REL=()
for item in "${TAR_INPUTS[@]}"; do
  abs_item="$(cd -- "$(dirname -- "$item")" && pwd)/$(basename -- "$item")"
  TAR_INPUTS_REL+=("${abs_item#/}")
done

tar -czf "$BUNDLE_PATH" -C / "${TAR_INPUTS_REL[@]}"
if command -v sha256sum >/dev/null 2>&1; then
  sha256sum "$BUNDLE_PATH" >"$CHECKSUM_PATH"
elif command -v shasum >/dev/null 2>&1; then
  shasum -a 256 "$BUNDLE_PATH" >"$CHECKSUM_PATH"
else
  echo "missing checksum tool: install sha256sum or shasum" >&2
  exit 1
fi

echo "bundle=$BUNDLE_PATH"
echo "checksum=$CHECKSUM_PATH"
echo "manifest=$MANIFEST_PATH"
