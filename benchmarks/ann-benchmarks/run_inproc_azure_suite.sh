#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_ROOT}"

DEFAULT_DATASETS="sift-128-euclidean,glove-100-angular,gist-960-euclidean,mnist-784-euclidean"
DEFAULT_M_VALUES="24,32,40,48,56,64,72,80"
DEFAULT_EF_CONSTRUCTION_VALUES="200,400,600,800,1000,1200,1600"
DEFAULT_EF_SEARCH_VALUES="64,128,192,256,384,512,768,1024,1536,2048,2560,3072"
DEFAULT_RECALL_TARGETS="0.90,0.95,0.99"

DATASETS="${DEFAULT_DATASETS}"
DATA_DIR="${REPO_ROOT}/benchmarks/data"
OUT_ROOT="${REPO_ROOT}/target/ann_inproc"
RUN_LABEL=""

M_VALUES="${DEFAULT_M_VALUES}"
EF_CONSTRUCTION_VALUES="${DEFAULT_EF_CONSTRUCTION_VALUES}"
EF_SEARCH_VALUES="${DEFAULT_EF_SEARCH_VALUES}"
RECALL_TARGETS="${DEFAULT_RECALL_TARGETS}"

K=10
REPETITIONS=3
WARMUP_QUERIES=200
MAX_TRAIN=0
MAX_QUERIES=0
ANN_SEARCH_MODE="fp32-strict"
QUANTIZED_RERANK_MULTIPLIER=8

PYTHON_BIN="${PYTHON_BIN:-python3}"
THREADS=""

SKIP_BUILD=0
SKIP_DOWNLOAD=0
FORCE_RECONVERT=0
NO_SUMMARY=0
CHECK_GATES=0

usage() {
  cat <<'USAGE'
Run KyroDB true in-process ANN suite (Azure-friendly, reproducible).

Usage:
  benchmarks/ann-benchmarks/run_inproc_azure_suite.sh [options]

Options:
  --datasets CSV              Dataset list (default: sift-128-euclidean,glove-100-angular,gist-960-euclidean,mnist-784-euclidean)
  --data-dir PATH             Dataset directory (default: benchmarks/data)
  --out-root PATH             Output root (default: target/ann_inproc)
  --run-label STRING          Label prefix for run directory name
  --m-values CSV              HNSW M sweep (default: 24,32,40,48,56,64,72,80)
  --ef-construction-values CSV
                              HNSW ef_construction sweep (default: 200,400,600,800,1000,1200,1600)
  --ef-search-values CSV      ef_search sweep passed to ann_inproc_bench (default: 64,128,192,256,384,512,768,1024,1536,2048,2560,3072)
  --recall-targets CSV        Summary targets (default: 0.90,0.95,0.99)
  --k INT                     Top-k (default: 10)
  --repetitions INT           Repetitions per ef_search (default: 3)
  --warmup-queries INT        Warmup queries per sweep (default: 200)
  --max-train INT             Optional train truncation at benchmark-time (default: 0 = full)
  --max-queries INT           Optional query truncation at benchmark-time (default: 0 = full)
  --ann-search-mode MODE      ANN search mode: fp32-strict|sq8-rerank (default: fp32-strict)
  --quantized-rerank-multiplier INT
                              Candidate expansion factor before fp32 rerank in quantized modes (default: 8)
  --threads INT               Export RAYON_NUM_THREADS for run reproducibility
  --python BIN                Python executable (default: python3 or PYTHON_BIN env)
  --skip-build                Skip cargo build
  --skip-download             Do not download missing HDF5 datasets
  --force-reconvert           Re-run HDF5 -> ANNBIN conversion even if ANNBIN exists
  --no-summary                Skip summary generation
  --check-gates              Run acceptance-gate validation on summary outputs
  -h, --help                  Show help
USAGE
}

trim() {
  local s="$1"
  s="${s#"${s%%[![:space:]]*}"}"
  s="${s%"${s##*[![:space:]]}"}"
  printf '%s' "${s}"
}

split_csv() {
  local csv="$1"
  local part
  SPLIT_CSV_RESULT=()
  IFS=',' read -r -a __parts <<<"${csv}"
  for part in "${__parts[@]}"; do
    part="$(trim "${part}")"
    if [[ -n "${part}" ]]; then
      SPLIT_CSV_RESULT+=("${part}")
    fi
  done
  local out_count="${#SPLIT_CSV_RESULT[@]}"
  if [[ "${out_count}" -eq 0 ]]; then
    echo "empty CSV list: '${csv}'" >&2
    exit 1
  fi
}

dataset_url() {
  case "$1" in
    sift-128-euclidean) echo "https://ann-benchmarks.com/sift-128-euclidean.hdf5" ;;
    glove-100-angular) echo "https://ann-benchmarks.com/glove-100-angular.hdf5" ;;
    gist-960-euclidean) echo "https://ann-benchmarks.com/gist-960-euclidean.hdf5" ;;
    mnist-784-euclidean) echo "https://ann-benchmarks.com/mnist-784-euclidean.hdf5" ;;
    *)
      echo "unsupported dataset '${1}'" >&2
      echo "supported: sift-128-euclidean, glove-100-angular, gist-960-euclidean, mnist-784-euclidean" >&2
      exit 1
      ;;
  esac
}

dataset_distance() {
  case "$1" in
    *-angular|*cosine*) echo "cosine" ;;
    *) echo "euclidean" ;;
  esac
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "required command not found: $1" >&2
    exit 1
  fi
}

require_arg() {
  local flag="$1"
  local value="${2-}"
  if [[ -z "${value}" ]] || [[ "${value}" == --* ]]; then
    echo "missing value for ${flag}" >&2
    usage
    exit 1
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --datasets) require_arg "$1" "${2-}"; DATASETS="$2"; shift 2 ;;
    --data-dir) require_arg "$1" "${2-}"; DATA_DIR="$2"; shift 2 ;;
    --out-root) require_arg "$1" "${2-}"; OUT_ROOT="$2"; shift 2 ;;
    --run-label) require_arg "$1" "${2-}"; RUN_LABEL="$2"; shift 2 ;;
    --m-values) require_arg "$1" "${2-}"; M_VALUES="$2"; shift 2 ;;
    --ef-construction-values) require_arg "$1" "${2-}"; EF_CONSTRUCTION_VALUES="$2"; shift 2 ;;
    --ef-search-values) require_arg "$1" "${2-}"; EF_SEARCH_VALUES="$2"; shift 2 ;;
    --recall-targets) require_arg "$1" "${2-}"; RECALL_TARGETS="$2"; shift 2 ;;
    --k) require_arg "$1" "${2-}"; K="$2"; shift 2 ;;
    --repetitions) require_arg "$1" "${2-}"; REPETITIONS="$2"; shift 2 ;;
    --warmup-queries) require_arg "$1" "${2-}"; WARMUP_QUERIES="$2"; shift 2 ;;
    --max-train) require_arg "$1" "${2-}"; MAX_TRAIN="$2"; shift 2 ;;
    --max-queries) require_arg "$1" "${2-}"; MAX_QUERIES="$2"; shift 2 ;;
    --ann-search-mode) require_arg "$1" "${2-}"; ANN_SEARCH_MODE="$2"; shift 2 ;;
    --quantized-rerank-multiplier) require_arg "$1" "${2-}"; QUANTIZED_RERANK_MULTIPLIER="$2"; shift 2 ;;
    --threads) require_arg "$1" "${2-}"; THREADS="$2"; shift 2 ;;
    --python) require_arg "$1" "${2-}"; PYTHON_BIN="$2"; shift 2 ;;
    --skip-build) SKIP_BUILD=1; shift ;;
    --skip-download) SKIP_DOWNLOAD=1; shift ;;
    --force-reconvert) FORCE_RECONVERT=1; shift ;;
    --no-summary) NO_SUMMARY=1; shift ;;
    --check-gates) CHECK_GATES=1; shift ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

require_cmd cargo
require_cmd curl
require_cmd "${PYTHON_BIN}"

BENCH_BIN="${REPO_ROOT}/target/release/ann_inproc_bench"
CONVERTER="${REPO_ROOT}/benchmarks/ann-benchmarks/export_ann_hdf5_to_annbin.py"
SUMMARIZER="${REPO_ROOT}/benchmarks/ann-benchmarks/summarize_inproc_results.py"
GATE_CHECKER="${REPO_ROOT}/benchmarks/ann-benchmarks/check_frontier_gates.py"

if [[ ! -f "${CONVERTER}" ]]; then
  echo "missing converter script: ${CONVERTER}" >&2
  exit 1
fi

if [[ "${SKIP_BUILD}" -eq 0 ]]; then
  echo "[build] cargo build --release -p kyrodb-engine --bin ann_inproc_bench"
  cargo build --release -p kyrodb-engine --bin ann_inproc_bench
fi

if [[ ! -x "${BENCH_BIN}" ]]; then
  echo "benchmark binary not found: ${BENCH_BIN}" >&2
  exit 1
fi

BENCH_HELP="$("${BENCH_BIN}" --help 2>&1 || true)"
BENCH_SUPPORTS_ANN_MODE=0
if printf '%s\n' "${BENCH_HELP}" | grep -q -- "--ann-search-mode"; then
  BENCH_SUPPORTS_ANN_MODE=1
fi

if [[ "${BENCH_SUPPORTS_ANN_MODE}" -eq 0 ]]; then
  if [[ "${ANN_SEARCH_MODE}" != "fp32-strict" || "${QUANTIZED_RERANK_MULTIPLIER}" != "8" ]]; then
    echo "selected ann-search-mode/quantized-rerank-multiplier require a newer ann_inproc_bench binary" >&2
    echo "rebuild without --skip-build, or use defaults: --ann-search-mode fp32-strict --quantized-rerank-multiplier 8" >&2
    exit 1
  fi
fi

if [[ -n "${THREADS}" ]]; then
  export RAYON_NUM_THREADS="${THREADS}"
fi

split_csv "${DATASETS}"
DATASET_LIST=("${SPLIT_CSV_RESULT[@]}")
split_csv "${M_VALUES}"
M_LIST=("${SPLIT_CSV_RESULT[@]}")
split_csv "${EF_CONSTRUCTION_VALUES}"
EFC_LIST=("${SPLIT_CSV_RESULT[@]}")

for value in "${M_LIST[@]}" "${EFC_LIST[@]}"; do
  if [[ ! "${value}" =~ ^[0-9]+$ ]]; then
    echo "expected integer in M/ef_construction sweep, got '${value}'" >&2
    exit 1
  fi
done

if [[ ! "${K}" =~ ^[0-9]+$ ]] || [[ ! "${REPETITIONS}" =~ ^[0-9]+$ ]] || [[ ! "${WARMUP_QUERIES}" =~ ^[0-9]+$ ]] || [[ ! "${MAX_TRAIN}" =~ ^[0-9]+$ ]] || [[ ! "${MAX_QUERIES}" =~ ^[0-9]+$ ]]; then
  echo "k/repetitions/warmup-queries/max-train/max-queries must be integers >= 0" >&2
  exit 1
fi
if [[ ! "${QUANTIZED_RERANK_MULTIPLIER}" =~ ^[0-9]+$ ]]; then
  echo "quantized-rerank-multiplier must be an integer >= 0" >&2
  exit 1
fi
if [[ "${ANN_SEARCH_MODE}" != "fp32-strict" && "${ANN_SEARCH_MODE}" != "sq8-rerank" ]]; then
  echo "ann-search-mode must be one of: fp32-strict, sq8-rerank" >&2
  exit 1
fi

RUN_TS="$(date -u +%Y%m%dT%H%M%SZ)"
if [[ -n "${RUN_LABEL}" ]]; then
  SAFE_LABEL="$(echo "${RUN_LABEL}" | tr -cs '[:alnum:]_.-' '_')"
  RUN_ID="${SAFE_LABEL}_${RUN_TS}"
else
  RUN_ID="${RUN_TS}"
fi

RUN_DIR="${OUT_ROOT}/${RUN_ID}"
RAW_DIR="${RUN_DIR}/raw"
LOG_DIR="${RUN_DIR}/logs"
SUMMARY_DIR="${RUN_DIR}/summary"

mkdir -p "${DATA_DIR}" "${RAW_DIR}" "${LOG_DIR}" "${SUMMARY_DIR}"

MANIFEST="${RUN_DIR}/manifest.txt"
{
  echo "run_id=${RUN_ID}"
  echo "started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "repo_root=${REPO_ROOT}"
  echo "datasets=${DATASETS}"
  echo "m_values=${M_VALUES}"
  echo "ef_construction_values=${EF_CONSTRUCTION_VALUES}"
  echo "ef_search_values=${EF_SEARCH_VALUES}"
  echo "recall_targets=${RECALL_TARGETS}"
  echo "k=${K}"
  echo "repetitions=${REPETITIONS}"
  echo "warmup_queries=${WARMUP_QUERIES}"
  echo "max_train=${MAX_TRAIN}"
  echo "max_queries=${MAX_QUERIES}"
  echo "ann_search_mode=${ANN_SEARCH_MODE}"
  echo "quantized_rerank_multiplier=${QUANTIZED_RERANK_MULTIPLIER}"
  echo "bench_supports_ann_mode_flags=${BENCH_SUPPORTS_ANN_MODE}"
  echo "metrics_schema=v2"
  echo "qps_primary_metric=search_only"
  echo "qps_end_to_end_metric=end_to_end_qps_mean"
  echo "ann_backend_expected=kyro_single_graph"
  echo "python_bin=${PYTHON_BIN}"
  echo "threads=${THREADS:-default}"
  echo "host=$(hostname)"
  echo "kernel=$(uname -a)"
  echo "rustc=$(rustc -V 2>/dev/null || true)"
  echo "cargo=$(cargo -V 2>/dev/null || true)"
  echo "python=$(${PYTHON_BIN} --version 2>&1 || true)"
  echo "git_commit=$(git rev-parse HEAD 2>/dev/null || true)"
  echo "git_short=$(git rev-parse --short HEAD 2>/dev/null || true)"
  echo "git_status_short="
  git status --short 2>/dev/null || true
  if command -v lscpu >/dev/null 2>&1; then
    echo "lscpu="
    lscpu
  fi
} >"${MANIFEST}"

echo "[run] output root: ${RUN_DIR}"
echo "[run] manifest: ${MANIFEST}"

TOTAL_RUNS=$(( ${#DATASET_LIST[@]} * ${#M_LIST[@]} * ${#EFC_LIST[@]} ))
RUN_IDX=0

for dataset in "${DATASET_LIST[@]}"; do
  url="$(dataset_url "${dataset}")"
  distance="$(dataset_distance "${dataset}")"

  hdf5_path="${DATA_DIR}/${dataset}.hdf5"
  annbin_path="${DATA_DIR}/${dataset}.annbin"

  if [[ "${SKIP_DOWNLOAD}" -eq 1 ]]; then
    if [[ ! -f "${hdf5_path}" ]]; then
      echo "missing dataset (and --skip-download set): ${hdf5_path}" >&2
      exit 1
    fi
  elif [[ ! -f "${hdf5_path}" ]]; then
    echo "[data] downloading ${dataset} -> ${hdf5_path}"
    curl -fL --retry 5 --retry-delay 2 --retry-all-errors "${url}" -o "${hdf5_path}"
  else
    echo "[data] reusing ${hdf5_path}"
  fi

  if [[ "${FORCE_RECONVERT}" -eq 1 || ! -f "${annbin_path}" ]]; then
    echo "[data] converting ${hdf5_path} -> ${annbin_path}"
    "${PYTHON_BIN}" "${CONVERTER}" --input "${hdf5_path}" --output "${annbin_path}"
  else
    echo "[data] reusing ${annbin_path}"
  fi

  for m in "${M_LIST[@]}"; do
    for efc in "${EFC_LIST[@]}"; do
      RUN_IDX=$((RUN_IDX + 1))
      mode_tag="${ANN_SEARCH_MODE//-/_}"
      run_key="${dataset}.mode${mode_tag}.m${m}.efc${efc}"
      result_json="${RAW_DIR}/${run_key}.json"
      stdout_log="${LOG_DIR}/${run_key}.stdout.log"
      stderr_log="${LOG_DIR}/${run_key}.stderr.log"

      echo "[bench ${RUN_IDX}/${TOTAL_RUNS}] ${run_key}"
      start_s="$(date +%s)"
      bench_cmd=(
        "${BENCH_BIN}"
        --dataset-annbin "${annbin_path}" \
        --dataset-name "${dataset}" \
        --distance "${distance}" \
        --k "${K}" \
        --m "${m}" \
        --ef-construction "${efc}" \
        --ef-search "${EF_SEARCH_VALUES}" \
        --repetitions "${REPETITIONS}" \
        --warmup-queries "${WARMUP_QUERIES}" \
        --max-train "${MAX_TRAIN}" \
        --max-queries "${MAX_QUERIES}" \
        --output-json "${result_json}"
      )
      if [[ "${BENCH_SUPPORTS_ANN_MODE}" -eq 1 ]]; then
        bench_cmd+=(--ann-search-mode "${ANN_SEARCH_MODE}")
        bench_cmd+=(--quantized-rerank-multiplier "${QUANTIZED_RERANK_MULTIPLIER}")
      fi
      "${bench_cmd[@]}" >"${stdout_log}" 2>"${stderr_log}"
      end_s="$(date +%s)"
      elapsed_s=$((end_s - start_s))
      echo "[ok] ${run_key} (${elapsed_s}s)"
    done
  done
done

if [[ "${NO_SUMMARY}" -eq 0 ]]; then
  if [[ ! -f "${SUMMARIZER}" ]]; then
    echo "missing summary script: ${SUMMARIZER}" >&2
    exit 1
  fi
  summary_json="${SUMMARY_DIR}/inproc_summary.json"
  summary_md="${SUMMARY_DIR}/inproc_summary.md"
  summary_csv="${SUMMARY_DIR}/inproc_candidates.csv"
  echo "[summary] generating ${summary_json} and ${summary_md}"
  "${PYTHON_BIN}" "${SUMMARIZER}" \
    --input-glob "${RAW_DIR}/*.json" \
    --recall-targets "${RECALL_TARGETS}" \
    --output-json "${summary_json}" \
    --output-md "${summary_md}" \
    --output-csv "${summary_csv}"

  if [[ "${CHECK_GATES}" -eq 1 ]]; then
    if [[ ! -f "${GATE_CHECKER}" ]]; then
      echo "missing gate checker script: ${GATE_CHECKER}" >&2
      exit 1
    fi
    echo "[gates] validating frontier acceptance gates"
    "${PYTHON_BIN}" "${GATE_CHECKER}" \
      --summary-json "${summary_json}" \
      --candidates-csv "${summary_csv}" \
      --log-glob "${LOG_DIR}/*.stderr.log" \
      --log-glob "${LOG_DIR}/*.stdout.log"
  fi
elif [[ "${CHECK_GATES}" -eq 1 ]]; then
  echo "--check-gates requires summary output; remove --no-summary" >&2
  exit 1
fi

{
  echo "finished_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "run_dir=${RUN_DIR}"
} >>"${MANIFEST}"

echo "[done] run directory: ${RUN_DIR}"
