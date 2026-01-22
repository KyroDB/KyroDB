#!/usr/bin/env bash
# KyroDB Azure VM Benchmark entrypoint.
#
# This intentionally delegates to the canonical, VM-ready suite:
#   benchmarks/scripts/vm_ann_suite.sh
#
# Keep Azure usage minimal and reproducible; avoid duplicating logic.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

exec bash benchmarks/scripts/vm_ann_suite.sh
