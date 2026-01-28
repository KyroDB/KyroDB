#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import itertools
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


@dataclass(frozen=True)
class RunSpec:
    distance: str
    dim: int
    n: int
    queries: int
    k: int
    m: int
    ef_construction: int
    ef_search: int
    exact_recall: bool


def run_one(bin_path: Path, spec: RunSpec) -> dict[str, str]:
    cmd = [
        str(bin_path),
        "--distance",
        spec.distance,
        "--dim",
        str(spec.dim),
        "--n",
        str(spec.n),
        "--queries",
        str(spec.queries),
        "--k",
        str(spec.k),
        "--m",
        str(spec.m),
        "--ef-construction",
        str(spec.ef_construction),
        "--ef-search",
        str(spec.ef_search),
    ]
    if spec.exact_recall:
        cmd.append("--exact-recall")

    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=False)
    if proc.returncode != 0:
        raise RuntimeError(
            "command failed\n"
            + "cmd: "
            + " ".join(cmd)
            + "\n\nstdout:\n"
            + proc.stdout
            + "\n\nstderr:\n"
            + proc.stderr
        )

    lines = [ln.strip() for ln in proc.stdout.splitlines() if ln.strip()]
    # Last non-empty line is the single CSV result row.
    row_line = lines[-1]

    header = lines[-2]
    if not header.startswith("distance,"):
        raise RuntimeError(f"unexpected header line: {header}")

    cols = header.split(",")
    vals = row_line.split(",")
    if len(cols) != len(vals):
        raise RuntimeError(f"csv column mismatch: {len(cols)} vs {len(vals)}\n{header}\n{row_line}")

    return dict(zip(cols, vals))


def pareto_best(rows: list[dict[str, str]], *, min_recall: float) -> dict[str, str] | None:
    best = None
    best_qps = -1.0
    for r in rows:
        recall = float(r["recall"]) if r["recall"] != "NaN" else float("nan")
        qps = float(r["qps"])
        if recall != recall:  # NaN
            continue
        if recall < min_recall:
            continue
        if qps > best_qps:
            best_qps = qps
            best = r
    return best


def main() -> int:
    parser = argparse.ArgumentParser(description="Sweep local hnsw_experiment and save CSV")
    parser.add_argument("--bin", default="target/release/hnsw_experiment", help="Path to built hnsw_experiment")
    parser.add_argument("--out", default="target/hnsw_sweep.csv", help="Output CSV path")

    parser.add_argument("--distance", default="cosine", choices=["cosine", "euclidean", "inner-product"])
    parser.add_argument("--dim", type=int, default=100)
    parser.add_argument("--n", type=int, default=20000)
    parser.add_argument("--queries", type=int, default=200)
    parser.add_argument("--k", type=int, default=10)

    parser.add_argument("--m", type=str, default="16,24,32")
    parser.add_argument("--ef-construction", type=str, default="200,300,400")
    parser.add_argument("--ef-search", type=str, default="50,100,200")

    args = parser.parse_args()

    bin_path = Path(args.bin)
    if not bin_path.exists():
        raise SystemExit(
            f"Missing binary: {bin_path}\n"
            "Build it with: cargo build -p kyrodb-engine --release --features dev-tools --bin hnsw_experiment"
        )

    m_list = [int(x) for x in args.m.split(",") if x]
    efc_list = [int(x) for x in args.ef_construction.split(",") if x]
    efs_list = [int(x) for x in args.ef_search.split(",") if x]

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    all_rows: list[dict[str, str]] = []

    specs: Iterable[RunSpec] = (
        RunSpec(
            distance=args.distance,
            dim=args.dim,
            n=args.n,
            queries=args.queries,
            k=args.k,
            m=m,
            ef_construction=efc,
            ef_search=efs,
            exact_recall=True,
        )
        for (m, efc, efs) in itertools.product(m_list, efc_list, efs_list)
    )

    for spec in specs:
        row = run_one(bin_path, spec)
        all_rows.append(row)
        print(
            f"m={spec.m} efC={spec.ef_construction} ef={spec.ef_search} "
            f"qps={row['qps']} recall={row['recall']} p50_ms={row['p50_ms']} p99_ms={row['p99_ms']}"
        )

    # Write CSV
    fieldnames = list(all_rows[0].keys())
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(all_rows)

    print(f"\nWrote: {out_path}")

    for target in [0.85, 0.9, 0.95]:
        best = pareto_best(all_rows, min_recall=target)
        if best is None:
            print(f"No config meets recall >= {target}")
        else:
            print(
                f"Best QPS with recall>={target}: qps={best['qps']} recall={best['recall']} "
                f"m={best['m']} efC={best['ef_construction']} ef={best['ef_search']} p50_ms={best['p50_ms']} p99_ms={best['p99_ms']}"
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
