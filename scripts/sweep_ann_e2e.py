#!/usr/bin/env python3

from __future__ import annotations

import argparse
import collections
import csv
import os
import re
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


@dataclass(frozen=True)
class BuildSpec:
    m: int
    ef_construction: int


@dataclass(frozen=True)
class QuerySpec:
    ef_search: int


def _parse_int_list(csv_list: str) -> List[int]:
    out: List[int] = []
    for part in csv_list.split(","):
        part = part.strip()
        if not part:
            continue
        out.append(int(part))
    if not out:
        raise ValueError("empty list")
    return out


def _default_distance_for_dataset(dataset: str) -> str:
    if dataset.endswith("-angular"):
        return "cosine"
    return "euclidean"


def _guess_dimension(dataset: str) -> Optional[int]:
    # Common ANN-Benchmarks naming: glove-100-angular, sift-128-euclidean, gist-960-euclidean.
    m = re.search(r"-(\d+)-(?:angular|euclidean)$", dataset)
    if m:
        return int(m.group(1))
    return None


def _try_read_hdf5_shapes(dataset: str) -> Tuple[Optional[int], Optional[int]]:
    """Return (n_train, dim) if local HDF5 exists, else (None, None)."""
    h5_path = Path("benchmarks/data") / f"{dataset}.hdf5"
    if not h5_path.exists():
        return None, None

    try:
        import h5py  # type: ignore

        with h5py.File(h5_path, "r") as f:
            train = f["train"]
            shape = tuple(train.shape)
            if len(shape) != 2:
                return None, None
            return int(shape[0]), int(shape[1])
    except Exception:
        return None, None


def _ensure_binary(bin_path: Path, cargo_args: List[str]) -> None:
    if bin_path.exists():
        return
    proc = subprocess.run(cargo_args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"Failed to build {bin_path.name}:\n{proc.stdout}")
    if not bin_path.exists():
        raise RuntimeError(f"Binary not found after build: {bin_path}")


def _wait_for_log_line(
    proc: subprocess.Popen[str],
    needle: str,
    timeout_s: float,
    *,
    echo: bool,
) -> None:
    start = time.time()
    if proc.stdout is None:
        raise RuntimeError("server stdout is not captured (expected stdout=PIPE)")

    while True:
        if proc.poll() is not None:
            raise RuntimeError(f"server exited early with code={proc.returncode}")

        if time.time() - start > timeout_s:
            raise RuntimeError(f"timed out waiting for server readiness: {needle}")

        line = proc.stdout.readline()
        if not line:
            time.sleep(0.05)
            continue
        if echo:
            # Preserve server logs for diagnosing slow startup/indexing.
            sys.stdout.write(line)
            sys.stdout.flush()
        if needle in line:
            return


def _terminate_process(proc: subprocess.Popen[str], *, timeout_s: float = 10.0) -> None:
    if proc.poll() is not None:
        return
    try:
        proc.terminate()
        proc.wait(timeout=timeout_s)
    except KeyboardInterrupt:
        # Ensure the child doesn't linger if the user interrupts.
        try:
            proc.kill()
        except Exception:
            pass
        raise
    except Exception:
        try:
            proc.kill()
        except Exception:
            pass


def _manifest_path(data_dir: Path) -> Path:
    return data_dir / "MANIFEST"


def _has_manifest(data_dir: Path) -> bool:
    p = _manifest_path(data_dir)
    try:
        return p.exists() and p.stat().st_size > 0
    except Exception:
        return False


def _latest_json(dir_path: Path) -> Path:
    jsons = sorted(dir_path.glob("*.json"), key=lambda p: p.stat().st_mtime)
    if not jsons:
        raise RuntimeError(f"no json results found in {dir_path}")
    return jsons[-1]


def _run_benchmark(
    *,
    dataset: str,
    host: str,
    port: int,
    k: int,
    ef_search: int,
    output_dir: Path,
    max_train: int,
    max_queries: int,
    warmup_queries: int,
    repetitions: int,
    concurrency: int,
    bulk_search: bool,
    skip_index: bool,
    recompute_ground_truth: bool,
) -> Path:
    cmd = [
        sys.executable,
        "-u",
        "benchmarks/run_benchmark.py",
        "--dataset",
        dataset,
        "--host",
        host,
        "--port",
        str(port),
        "--k",
        str(k),
        "--ef-search",
        str(ef_search),
        "--output-dir",
        str(output_dir),
        "--max-train",
        str(max_train),
        "--max-queries",
        str(max_queries),
        "--warmup-queries",
        str(warmup_queries),
        "--repetitions",
        str(repetitions),
        "--concurrency",
        str(concurrency),
    ]
    if bulk_search:
        cmd.append("--bulk-search")
    if skip_index:
        cmd.append("--skip-index")
    if recompute_ground_truth:
        cmd.append("--recompute-ground-truth")

    start = time.time()

    # Stream output so long indexing doesn't look like a hang.
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
    if p.stdout is None:
        raise RuntimeError("benchmark stdout is not captured (expected stdout=PIPE)")
    captured: collections.deque[str] = collections.deque(maxlen=5000)
    try:
        for line in p.stdout:
            sys.stdout.write(line)
            sys.stdout.flush()
            captured.append(line)
        rc = p.wait()
    except KeyboardInterrupt:
        try:
            p.terminate()
        except Exception:
            pass
        raise

    if rc != 0:
        tail = "".join(captured[-200:])
        raise RuntimeError(f"benchmark failed (exit={rc}); last output:\n{tail}")

    elapsed = time.time() - start
    if not skip_index:
        print(f"  Indexed via benchmark runner in {elapsed:.1f}s")
    else:
        print(f"  Queried via benchmark runner in {elapsed:.1f}s")

    return _latest_json(output_dir)


def _read_metrics(json_path: Path) -> Dict[str, Any]:
    import json

    with json_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    results = data.get("results") or {}
    return {
        "json": str(json_path),
        "dataset": data.get("dataset"),
        "k": int(data.get("k", 0) or 0),
        "ef_search": int(data.get("ef_search", 0) or 0),
        "concurrency": int(data.get("concurrency", 0) or 0),
        "bulk_search": bool(data.get("bulk_search", False)),
        "max_train": int(data.get("max_train", 0) or 0),
        "n_train": int(data.get("n_train", 0) or 0),
        "n_queries": int(data.get("n_queries", 0) or 0),
        "dimension": int(data.get("dimension", 0) or 0),
        "recall": float(results.get("recall", 0.0) or 0.0),
        "qps": float(results.get("qps", 0.0) or 0.0),
        "p50_latency_ms": float(results.get("p50_latency_ms", 0.0) or 0.0),
        "p99_latency_ms": float(results.get("p99_latency_ms", 0.0) or 0.0),
    }


def _best_by_recall(rows: List[Dict[str, Any]], *, min_recall: float) -> Optional[Dict[str, Any]]:
    best: Optional[Dict[str, Any]] = None
    best_qps = -1.0
    for r in rows:
        if r["recall"] < min_recall:
            continue
        if r["qps"] > best_qps:
            best_qps = r["qps"]
            best = r
    return best


def main() -> int:
    parser = argparse.ArgumentParser(description="End-to-end KyroDB ANN sweep (server + benchmark runner)")
    parser.add_argument("--dataset", default="sift-128-euclidean")
    parser.add_argument("--k", type=int, default=10)

    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--base-port", type=int, default=50051)

    parser.add_argument("--m", default="16,24,32")
    parser.add_argument("--ef-construction", default="200,400,500")
    parser.add_argument("--ef-search", default="10,50,100,200")

    parser.add_argument("--max-train", type=int, default=0)
    parser.add_argument("--max-queries", type=int, default=300)
    parser.add_argument("--warmup-queries", type=int, default=50)
    parser.add_argument("--repetitions", type=int, default=2)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--bulk-search", action="store_true")

    parser.add_argument("--out", default="target/e2e_sweeps")
    parser.add_argument(
        "--run-dir",
        default=None,
        help="Reuse an existing run directory (e.g., to query-sweep after a long index-only run).",
    )
    parser.add_argument("--keep-data", action="store_true", help="Keep server data dirs (default: delete)")
    parser.add_argument(
        "--skip-index-if-present",
        action="store_true",
        help="If data_dir already contains a MANIFEST, skip the indexing step and only run query sweeps.",
    )
    parser.add_argument(
        "--index-only",
        action="store_true",
        help="Only build/index the dataset for each config and exit (no query sweeps).",
    )

    args = parser.parse_args()

    dataset = str(args.dataset)
    distance = _default_distance_for_dataset(dataset)
    n_train_full, dim_from_hdf5 = _try_read_hdf5_shapes(dataset)
    dim_guess = dim_from_hdf5 if dim_from_hdf5 is not None else _guess_dimension(dataset)
    if dim_guess is None:
        raise SystemExit(
            f"Unable to infer dimension from dataset name and local HDF5 not found: {dataset}"
        )

    max_train = int(args.max_train)
    if max_train < 0:
        raise SystemExit("--max-train must be >= 0")

    if max_train > 0 and n_train_full is not None and max_train < n_train_full:
        print(
            "WARNING: --max-train benchmarks a truncated index; results are not directly comparable to official "
            "ANN-Benchmarks full-dataset runs. This script enables --recompute-ground-truth for truncated runs, "
            "so recall is computed against the truncated set (not under-estimated vs. its own ground truth)."
        )

    m_list = _parse_int_list(args.m)
    efc_list = _parse_int_list(args.ef_construction)
    efs_list = _parse_int_list(args.ef_search)

    out_root = Path(args.out)
    if args.run_dir:
        run_dir = Path(str(args.run_dir))
        run_dir.mkdir(parents=True, exist_ok=True)
    else:
        run_id = time.strftime("%Y%m%d_%H%M%S", time.gmtime())
        run_dir = out_root / f"{dataset}_{run_id}"
        run_dir.mkdir(parents=True, exist_ok=True)

    # Ensure server binary exists.
    server_bin = Path("target/release/kyrodb_server")
    _ensure_binary(
        server_bin,
        ["cargo", "build", "-p", "kyrodb-engine", "--release", "--bin", "kyrodb_server"],
    )

    all_rows: List[Dict[str, Any]] = []

    build_specs: List[BuildSpec] = [BuildSpec(m=m, ef_construction=efc) for m in m_list for efc in efc_list]
    query_specs: List[QuerySpec] = [QuerySpec(ef_search=ef) for ef in efs_list]

    base_port = int(args.base_port)
    if base_port < 0 or base_port > 65535:
        raise SystemExit(f"--base-port must be in [0, 65535], got {base_port}")
    max_port = base_port + len(build_specs) - 1
    if max_port > 65535:
        raise SystemExit(
            f"--base-port {base_port} with {len(build_specs)} builds exceeds max port 65535 (max would be {max_port})"
        )

    for idx, build in enumerate(build_specs):
        port = base_port + idx
        data_dir = run_dir / "data" / f"m{build.m}_efc{build.ef_construction}"
        results_dir = run_dir / "results" / f"m{build.m}_efc{build.ef_construction}"
        results_dir.mkdir(parents=True, exist_ok=True)

        already_indexed = _has_manifest(data_dir) and bool(args.skip_index_if_present)
        if data_dir.exists() and not args.keep_data and not already_indexed:
            shutil.rmtree(data_dir)
        data_dir.mkdir(parents=True, exist_ok=True)

        env = os.environ.copy()

        # Choose max_elements based on the effective train size.
        if max_train > 0:
            max_elements = max_train
        elif n_train_full is not None:
            max_elements = n_train_full
        else:
            # Fallback: large enough for the common ANN datasets.
            max_elements = 2_000_000

        env.update(
            {
                "KYRODB__SERVER__HOST": str(args.host),
                "KYRODB__SERVER__PORT": str(port),
                "KYRODB__PERSISTENCE__DATA_DIR": str(data_dir.resolve()),
                "KYRODB__HNSW__DISTANCE": distance,
                "KYRODB__HNSW__DIMENSION": str(dim_guess),
                "KYRODB__HNSW__MAX_ELEMENTS": str(max_elements),
                "KYRODB__HNSW__M": str(build.m),
                "KYRODB__HNSW__EF_CONSTRUCTION": str(build.ef_construction),
                # Server default; query sweeps override per request.
                "KYRODB__HNSW__EF_SEARCH": "50",
            }
        )

        print(
            f"\n=== Server config: m={build.m} efC={build.ef_construction} port={port} "
            f"distance={distance} dim={dim_guess} max_elements={max_elements} indexed={already_indexed} ==="
        )
        proc = subprocess.Popen(
            [str(server_bin)],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=env,
            bufsize=1,
        )

        try:
            _wait_for_log_line(
                proc,
                "Server ready to accept connections",
                timeout_s=60.0,
                echo=True,
            )

            if not already_indexed:
                # Index once: do a minimal query pass so the runner returns quickly.
                _ = _run_benchmark(
                    dataset=dataset,
                    host=str(args.host),
                    port=port,
                    k=int(args.k),
                    ef_search=int(query_specs[0].ef_search),
                    output_dir=results_dir,
                    max_train=max_train,
                    max_queries=1,
                    warmup_queries=0,
                    repetitions=1,
                    concurrency=1,
                    bulk_search=bool(args.bulk_search),
                    skip_index=False,
                    recompute_ground_truth=bool(max_train > 0),
                )

            if args.index_only:
                continue

            for q in query_specs:
                json_path = _run_benchmark(
                    dataset=dataset,
                    host=str(args.host),
                    port=port,
                    k=int(args.k),
                    ef_search=int(q.ef_search),
                    output_dir=results_dir,
                    max_train=max_train,
                    max_queries=int(args.max_queries),
                    warmup_queries=int(args.warmup_queries),
                    repetitions=int(args.repetitions),
                    concurrency=int(args.concurrency),
                    bulk_search=bool(args.bulk_search),
                    skip_index=True,
                    recompute_ground_truth=bool(max_train > 0),
                )
                row = _read_metrics(json_path)
                row["m"] = int(build.m)
                row["ef_construction"] = int(build.ef_construction)
                all_rows.append(row)
                print(
                    f"m={build.m} efC={build.ef_construction} ef={q.ef_search} "
                    f"recall={row['recall']:.4f} qps={row['qps']:.0f} p50={row['p50_latency_ms']:.3f}ms p99={row['p99_latency_ms']:.3f}ms"
                )

        finally:
            _terminate_process(proc)

            if not args.keep_data:
                try:
                    shutil.rmtree(data_dir)
                except Exception:
                    pass

    # Write aggregate CSV
    csv_path = run_dir / "aggregate.csv"
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "dataset",
        "k",
        "dimension",
        "n_train",
        "n_queries",
        "max_train",
        "concurrency",
        "bulk_search",
        "m",
        "ef_construction",
        "ef_search",
        "recall",
        "qps",
        "p50_latency_ms",
        "p99_latency_ms",
        "json",
    ]

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in all_rows:
            w.writerow({k: r.get(k) for k in fieldnames})

    print(f"\nWrote aggregate: {csv_path}")

    for target in [0.85, 0.90, 0.95]:
        best = _best_by_recall(all_rows, min_recall=target)
        if best is None:
            print(f"No config meets recall >= {target}")
        else:
            print(
                f"Best QPS with recall>={target}: qps={best['qps']:.0f} recall={best['recall']:.4f} "
                f"m={best['m']} efC={best['ef_construction']} ef={best['ef_search']}"
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
