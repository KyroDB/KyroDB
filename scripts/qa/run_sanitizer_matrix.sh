#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "error: required command not found: $1" >&2
    exit 1
  fi
}

require_cmd rustup
require_cmd cargo
require_cmd rustc
require_cmd awk
require_cmd date
require_cmd tee

SANITIZERS_CSV="${SANITIZERS_CSV:-address,thread}"
TARGET_TRIPLE="${TARGET_TRIPLE:-$(rustc -vV | awk '/^host:/ {print $2}')}"
CONTINUE_ON_ERROR="${CONTINUE_ON_ERROR:-1}"
PANIC_STRATEGY="${PANIC_STRATEGY:-unwind}"
BUILD_STD="${BUILD_STD:-0}"
BUILD_STD_FEATURES="${BUILD_STD_FEATURES:-}"
NIGHTLY_TOOLCHAIN="${NIGHTLY_TOOLCHAIN:-}"
HAS_STD_WORKAROUND="${HAS_STD_WORKAROUND:-1}"

RUN_STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
ARTIFACT_DIR="${ARTIFACT_DIR:-$ROOT_DIR/target/qa/sanitizers_${RUN_STAMP}}"
mkdir -p "$ARTIFACT_DIR"

SUMMARY_FILE="$ARTIFACT_DIR/summary.txt"
: >"$SUMMARY_FILE"

if [[ -z "$NIGHTLY_TOOLCHAIN" ]]; then
  NIGHTLY_TOOLCHAIN="$(rustup toolchain list | awk '/^nightly/{print $1; exit}')"
fi

if [[ -z "$NIGHTLY_TOOLCHAIN" ]]; then
  echo "error: nightly toolchain is required for Rust sanitizers." >&2
  echo "run: rustup toolchain install nightly" >&2
  exit 1
fi

if ! rustup toolchain list | awk '{print $1}' | grep -qx "$NIGHTLY_TOOLCHAIN"; then
  echo "error: requested nightly toolchain is not installed: $NIGHTLY_TOOLCHAIN" >&2
  echo "run: rustup toolchain install $NIGHTLY_TOOLCHAIN" >&2
  exit 1
fi

if [[ "$PANIC_STRATEGY" != "unwind" && "$PANIC_STRATEGY" != "abort" ]]; then
  echo "error: PANIC_STRATEGY must be 'unwind' or 'abort' (got '$PANIC_STRATEGY')" >&2
  exit 1
fi

if [[ "$BUILD_STD" != "0" && "$BUILD_STD" != "1" ]]; then
  echo "error: BUILD_STD must be 0 or 1 (got '$BUILD_STD')" >&2
  exit 1
fi

if [[ "$HAS_STD_WORKAROUND" != "0" && "$HAS_STD_WORKAROUND" != "1" ]]; then
  echo "error: HAS_STD_WORKAROUND must be 0 or 1 (got '$HAS_STD_WORKAROUND')" >&2
  exit 1
fi

if ([[ "$BUILD_STD" == "1" ]] || [[ "$SANITIZERS_CSV" == *thread* ]]) && ! rustup component list --toolchain "$NIGHTLY_TOOLCHAIN" | grep -q '^rust-src.*installed'; then
  echo "error: rust-src component is required for sanitizer builds with -Zbuild-std." >&2
  echo "run: rustup component add rust-src --toolchain $NIGHTLY_TOOLCHAIN" >&2
  exit 1
fi

run_one() {
  local sanitizer="$1"
  local log_file="$ARTIFACT_DIR/${sanitizer}.log"
  local target_dir="$ARTIFACT_DIR/target/${sanitizer}"
  local build_std_for_run="$BUILD_STD"
  local status="ok"
  local rustflags="${RUSTFLAGS:-}"
  local rustdocflags="${RUSTDOCFLAGS:-}"
  local dyld_insert_libs="${DYLD_INSERT_LIBRARIES:-}"
  local host_os
  local runtime_name=""
  local runtime_path=""
  local -a cargo_args=(
    +"$NIGHTLY_TOOLCHAIN"
    test
  )

  case "$sanitizer" in
    address|thread|leak|memory)
      ;;
    *)
      echo "error: unsupported sanitizer '$sanitizer'" >&2
      return 1
      ;;
  esac

  rustflags="${rustflags}${rustflags:+ }-Zsanitizer=${sanitizer}"
  rustdocflags="${rustdocflags}${rustdocflags:+ }-Zsanitizer=${sanitizer}"

  # Thread sanitizer requires std/libtest to be built with matching sanitizer ABI.
  if [[ "$sanitizer" == "thread" && "$build_std_for_run" == "0" ]]; then
    build_std_for_run="1"
  fi

  # Work around indexmap/tower cfg(has_std) detection breakage when using -Zbuild-std.
  if [[ "$build_std_for_run" == "1" && "$HAS_STD_WORKAROUND" == "1" ]]; then
    rustflags="${rustflags} --cfg has_std"
    rustdocflags="${rustdocflags} --cfg has_std"
  fi

  if [[ "$PANIC_STRATEGY" == "abort" ]]; then
    rustflags="${rustflags} -Cpanic=abort"
    rustdocflags="${rustdocflags} -Cpanic=abort"
    cargo_args+=("-Zpanic_abort_tests")
  fi

  if [[ "$build_std_for_run" == "1" ]]; then
    cargo_args+=("-Zbuild-std")
    if [[ -n "$BUILD_STD_FEATURES" ]]; then
      cargo_args+=("-Zbuild-std-features=$BUILD_STD_FEATURES")
    fi
  fi

  cargo_args+=(
    --target "$TARGET_TRIPLE"
    -p kyrodb-engine
    --lib
    --tests
    --bins
    --no-fail-fast
  )

  host_os="$(uname -s)"
  if [[ "$host_os" == "Darwin" ]]; then
    case "$sanitizer" in
      address|leak)
        runtime_name="librustc-nightly_rt.asan.dylib"
        ;;
      thread)
        runtime_name="librustc-nightly_rt.tsan.dylib"
        ;;
      *)
        runtime_name=""
        ;;
    esac
    if [[ -n "$runtime_name" ]]; then
      runtime_path="$(rustc +"$NIGHTLY_TOOLCHAIN" --print sysroot)/lib/rustlib/${TARGET_TRIPLE}/lib/${runtime_name}"
      if [[ -f "$runtime_path" ]]; then
        dyld_insert_libs="${runtime_path}${dyld_insert_libs:+:${dyld_insert_libs}}"
      fi
    fi
  fi

  mkdir -p "$target_dir"
  echo "[sanitizer:$sanitizer] toolchain=$NIGHTLY_TOOLCHAIN target=$TARGET_TRIPLE build_std=$build_std_for_run panic=$PANIC_STRATEGY log=$log_file"
  set +e
  CARGO_TARGET_DIR="$target_dir" \
    KYRODB_SANITIZER="$sanitizer" \
    RUSTFLAGS="$rustflags" \
    RUSTDOCFLAGS="$rustdocflags" \
    ASAN_OPTIONS="detect_leaks=1:halt_on_error=1" \
    TSAN_OPTIONS="halt_on_error=1" \
    UBSAN_OPTIONS="halt_on_error=1:print_stacktrace=1" \
    DYLD_INSERT_LIBRARIES="$dyld_insert_libs" \
    cargo "${cargo_args[@]}" 2>&1 | tee "$log_file"
  local rc="${PIPESTATUS[0]}"
  set -e

  if [[ "$rc" -ne 0 ]]; then
    status="failed"
  fi
  echo "${sanitizer},${status},${log_file}" >>"$SUMMARY_FILE"

  if [[ "$status" != "ok" && "$CONTINUE_ON_ERROR" != "1" ]]; then
    return 1
  fi
  return 0
}

IFS=',' read -r -a sanitizers <<<"$SANITIZERS_CSV"
for sanitizer in "${sanitizers[@]}"; do
  run_one "$sanitizer"
done

echo "[done] sanitizer summary: $SUMMARY_FILE"
cat "$SUMMARY_FILE"
