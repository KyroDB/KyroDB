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
RUN_ID_OVERRIDE=""

M_VALUES="${DEFAULT_M_VALUES}"
EF_CONSTRUCTION_VALUES="${DEFAULT_EF_CONSTRUCTION_VALUES}"
EF_SEARCH_VALUES="${DEFAULT_EF_SEARCH_VALUES}"
RECALL_TARGETS="${DEFAULT_RECALL_TARGETS}"

K=10
REPETITIONS=3
WARMUP_QUERIES=200
MAX_TRAIN=0
MAX_QUERIES=0

PYTHON_BIN="${PYTHON_BIN:-python3}"
THREADS=""

SKIP_BUILD=0
SKIP_DOWNLOAD=0
FORCE_RECONVERT=0
NO_SUMMARY=0
NO_PLOTS=0
CHECK_GATES=0
ALLOW_MISSING_GATE_DATASETS=0
RESUME=0

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
  --run-id STRING             Fixed run directory id (no timestamp suffix)
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
  --threads INT               Export RAYON_NUM_THREADS for run reproducibility
  --python BIN                Python executable (default: python3 or PYTHON_BIN env)
  --skip-build                Skip cargo build
  --skip-download             Do not download missing HDF5 datasets
  --force-reconvert           Re-run HDF5 -> ANNBIN conversion even if ANNBIN exists
  --resume                    Reuse existing run dir and skip completed shard JSON outputs
  --no-summary                Skip summary generation
  --no-plots                  Skip plot generation from summary/candidate outputs
  --check-gates              Run acceptance-gate validation on summary outputs
  --allow-missing-gate-datasets
                              Allow gate checks to pass when some datasets/targets are absent
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

is_valid_json_file() {
  local path="$1"
  "${PYTHON_BIN}" -c 'import json,sys; json.load(open(sys.argv[1], "r", encoding="utf-8"))' \
    "$path" >/dev/null 2>&1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --datasets) require_arg "$1" "${2-}"; DATASETS="$2"; shift 2 ;;
    --data-dir) require_arg "$1" "${2-}"; DATA_DIR="$2"; shift 2 ;;
    --out-root) require_arg "$1" "${2-}"; OUT_ROOT="$2"; shift 2 ;;
    --run-label) require_arg "$1" "${2-}"; RUN_LABEL="$2"; shift 2 ;;
    --run-id) require_arg "$1" "${2-}"; RUN_ID_OVERRIDE="$2"; shift 2 ;;
    --m-values) require_arg "$1" "${2-}"; M_VALUES="$2"; shift 2 ;;
    --ef-construction-values) require_arg "$1" "${2-}"; EF_CONSTRUCTION_VALUES="$2"; shift 2 ;;
    --ef-search-values) require_arg "$1" "${2-}"; EF_SEARCH_VALUES="$2"; shift 2 ;;
    --recall-targets) require_arg "$1" "${2-}"; RECALL_TARGETS="$2"; shift 2 ;;
    --k) require_arg "$1" "${2-}"; K="$2"; shift 2 ;;
    --repetitions) require_arg "$1" "${2-}"; REPETITIONS="$2"; shift 2 ;;
    --warmup-queries) require_arg "$1" "${2-}"; WARMUP_QUERIES="$2"; shift 2 ;;
    --max-train) require_arg "$1" "${2-}"; MAX_TRAIN="$2"; shift 2 ;;
    --max-queries) require_arg "$1" "${2-}"; MAX_QUERIES="$2"; shift 2 ;;
    --threads) require_arg "$1" "${2-}"; THREADS="$2"; shift 2 ;;
    --python) require_arg "$1" "${2-}"; PYTHON_BIN="$2"; shift 2 ;;
    --skip-build) SKIP_BUILD=1; shift ;;
    --skip-download) SKIP_DOWNLOAD=1; shift ;;
    --force-reconvert) FORCE_RECONVERT=1; shift ;;
    --resume) RESUME=1; shift ;;
    --no-summary) NO_SUMMARY=1; shift ;;
    --no-plots) NO_PLOTS=1; shift ;;
    --check-gates) CHECK_GATES=1; shift ;;
    --allow-missing-gate-datasets) ALLOW_MISSING_GATE_DATASETS=1; shift ;;
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
PLOTTER="${REPO_ROOT}/benchmarks/ann-benchmarks/render_inproc_plots.py"

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
  echo "k/repetitions/warmup-queries/max-train/max-queries must be integers" >&2
  exit 1
fi
if [[ -n "${THREADS}" ]] && [[ ! "${THREADS}" =~ ^[0-9]+$ ]]; then
  echo "threads must be an integer >= 1" >&2
  exit 1
fi
if [[ "${K}" -lt 1 ]] || [[ "${REPETITIONS}" -lt 1 ]]; then
  echo "k and repetitions must be >= 1" >&2
  exit 1
fi
if [[ "${WARMUP_QUERIES}" -lt 0 ]] || [[ "${MAX_TRAIN}" -lt 0 ]] || [[ "${MAX_QUERIES}" -lt 0 ]]; then
  echo "warmup-queries/max-train/max-queries must be >= 0" >&2
  exit 1
fi
if [[ -n "${THREADS}" ]] && [[ "${THREADS}" -lt 1 ]]; then
  echo "threads must be >= 1" >&2
  exit 1
fi
if [[ "${RESUME}" -eq 1 ]] && [[ -z "${RUN_ID_OVERRIDE}" ]]; then
  echo "--resume requires --run-id so the script can target an existing run directory" >&2
  exit 1
fi
RUN_TS="$(date -u +%Y%m%dT%H%M%SZ)"
if [[ -n "${RUN_ID_OVERRIDE}" ]]; then
  RUN_ID="$(echo "${RUN_ID_OVERRIDE}" | tr -cs '[:alnum:]_.-' '_')"
elif [[ -n "${RUN_LABEL}" ]]; then
  SAFE_LABEL="$(echo "${RUN_LABEL}" | tr -cs '[:alnum:]_.-' '_')"
  RUN_ID="${SAFE_LABEL}_${RUN_TS}"
else
  RUN_ID="${RUN_TS}"
fi

RUN_DIR="${OUT_ROOT}/${RUN_ID}"
RAW_DIR="${RUN_DIR}/raw"
LOG_DIR="${RUN_DIR}/logs"
SUMMARY_DIR="${RUN_DIR}/summary"

if [[ -d "${RUN_DIR}" ]] && [[ "${RESUME}" -eq 0 ]]; then
  echo "run directory already exists (use --resume to continue): ${RUN_DIR}" >&2
  exit 1
fi

mkdir -p "${DATA_DIR}" "${RAW_DIR}" "${LOG_DIR}" "${SUMMARY_DIR}"

MANIFEST="${RUN_DIR}/manifest.txt"
if [[ "${RESUME}" -eq 1 ]] && [[ -f "${MANIFEST}" ]]; then
  {
    echo "resumed_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "resumed_by=run_inproc_azure_suite.sh"
  } >>"${MANIFEST}"
else
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
    echo "metrics_schema=v2"
    echo "qps_primary_metric=search_only"
    echo "qps_end_to_end_metric=end_to_end_qps_mean"
    echo "ann_backend_expected=kyro_single_graph"
    echo "python_bin=${PYTHON_BIN}"
    echo "threads=${THREADS:-default}"
    echo "run_id_override=${RUN_ID_OVERRIDE:-none}"
    echo "resume=${RESUME}"
    echo "no_plots=${NO_PLOTS}"
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
fi

echo "[run] output root: ${RUN_DIR}"
echo "[run] manifest: ${MANIFEST}"

TOTAL_RUNS=$(( ${#DATASET_LIST[@]} * ${#M_LIST[@]} * ${#EFC_LIST[@]} ))
RUN_IDX=0
SKIPPED_RUNS=0

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
    hdf5_tmp="${hdf5_path}.tmp"
    curl -fL --retry 5 --retry-delay 2 --retry-all-errors "${url}" -o "${hdf5_tmp}"
    mv "${hdf5_tmp}" "${hdf5_path}"
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
      run_key="${dataset}.m${m}.efc${efc}"
      result_json="${RAW_DIR}/${run_key}.json"
      stdout_log="${LOG_DIR}/${run_key}.stdout.log"
      stderr_log="${LOG_DIR}/${run_key}.stderr.log"

      if [[ "${RESUME}" -eq 1 ]] && [[ -s "${result_json}" ]]; then
        if is_valid_json_file "${result_json}"; then
          SKIPPED_RUNS=$((SKIPPED_RUNS + 1))
          echo "[skip ${RUN_IDX}/${TOTAL_RUNS}] ${run_key} (existing ${result_json})"
          continue
        fi
        echo "[resume] removing invalid JSON output and rerunning: ${result_json}" >&2
        rm -f "${result_json}"
      fi

      echo "[bench ${RUN_IDX}/${TOTAL_RUNS}] ${run_key}"
      start_s="$(date +%s)"
      result_json_tmp="${result_json}.tmp"
      rm -f "${result_json_tmp}"
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
        --output-json "${result_json_tmp}"
      )
      if ! "${bench_cmd[@]}" >"${stdout_log}" 2>"${stderr_log}"; then
        rm -f "${result_json_tmp}"
        echo "[fail] ${run_key}" >&2
        exit 1
      fi
      mv "${result_json_tmp}" "${result_json}"
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
    gate_args=(
      --summary-json "${summary_json}"
      --candidates-csv "${summary_csv}"
      --log-glob "${LOG_DIR}/*.stderr.log"
      --log-glob "${LOG_DIR}/*.stdout.log"
    )
    if [[ "${ALLOW_MISSING_GATE_DATASETS}" -eq 1 ]]; then
      gate_args+=(--allow-missing-datasets)
    fi
    "${PYTHON_BIN}" "${GATE_CHECKER}" "${gate_args[@]}"
  fi

  if [[ "${NO_PLOTS}" -eq 0 ]]; then
    if [[ -f "${PLOTTER}" ]]; then
      plot_dir="${RUN_DIR}/plots"
      if ! "${PYTHON_BIN}" "${PLOTTER}" \
        --summary-json "${summary_json}" \
        --candidates-csv "${summary_csv}" \
        --output-dir "${plot_dir}" \
        --recall-targets "${RECALL_TARGETS}"; then
        echo "warning: plot generation failed for ${RUN_DIR}" >&2
      fi
    else
      echo "warning: plot script missing (${PLOTTER}); skipping plot generation" >&2
    fi
  fi
elif [[ "${CHECK_GATES}" -eq 1 ]]; then
  echo "--check-gates requires summary output; remove --no-summary" >&2
  exit 1
fi

{
  echo "finished_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "skipped_runs=${SKIPPED_RUNS}"
  if [[ -n "${plot_dir:-}" ]]; then
    echo "plots_dir=${plot_dir}"
  fi
  echo "run_dir=${RUN_DIR}"
} >>"${MANIFEST}"

echo "[done] run directory: ${RUN_DIR}"
