#!/usr/bin/env python3
"""Convert ANN-Benchmarks .hdf5 datasets to KyroDB .annbin format.

annbin layout (little-endian):
- magic: 8 bytes = b"KYROANN1"
- train_rows: u64
- test_rows: u64
- dimension: u64
- neighbors_cols: u64
- train data: train_rows * dimension * f32
- test data: test_rows * dimension * f32
- neighbors data: test_rows * neighbors_cols * u32
"""

from __future__ import annotations

import argparse
import hashlib
import json
import struct
from pathlib import Path

import h5py
import numpy as np

MAGIC = b"KYROANN1"


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description="Export ANN HDF5 dataset to annbin")
    parser.add_argument("--input", required=True, type=Path, help="Input .hdf5 file")
    parser.add_argument("--output", required=True, type=Path, help="Output .annbin file")
    parser.add_argument(
        "--max-train",
        type=int,
        default=0,
        help="Optional train truncation (0 = keep all)",
    )
    parser.add_argument(
        "--max-queries",
        type=int,
        default=0,
        help="Optional query truncation (0 = keep all)",
    )
    args = parser.parse_args()

    if not args.input.exists():
        raise SystemExit(f"Input file does not exist: {args.input}")
    if args.max_train < 0 or args.max_queries < 0:
        raise SystemExit("max-train/max-queries must be >= 0")

    with h5py.File(args.input, "r") as f:
        for key in ("train", "test", "neighbors"):
            if key not in f:
                raise SystemExit(f"Missing dataset key '{key}' in {args.input}")

        train = np.asarray(f["train"], dtype=np.float32)
        test = np.asarray(f["test"], dtype=np.float32)
        neighbors = np.asarray(f["neighbors"])

    if train.ndim != 2 or test.ndim != 2 or neighbors.ndim != 2:
        raise SystemExit(
            f"Expected 2D arrays, got train={train.shape} test={test.shape} neighbors={neighbors.shape}"
        )

    if train.shape[1] != test.shape[1]:
        raise SystemExit(
            f"Dimension mismatch: train dim={train.shape[1]} test dim={test.shape[1]}"
        )

    if args.max_train > 0 and args.max_train < train.shape[0]:
        train = train[: args.max_train]

    if args.max_queries > 0 and args.max_queries < test.shape[0]:
        test = test[: args.max_queries]
        neighbors = neighbors[: args.max_queries]

    # Convert neighbors to u32 and validate range.
    if neighbors.size > 0:
        if np.any(~np.isfinite(neighbors)):
            raise SystemExit("neighbors contains non-finite values, cannot convert to u32")
        if np.any(neighbors < 0):
            raise SystemExit("neighbors contains negative ids, cannot convert to u32")
        if np.issubdtype(neighbors.dtype, np.floating):
            if np.any(np.floor(neighbors) != neighbors):
                raise SystemExit("neighbors contains non-integer values, cannot convert to u32")
        if np.max(neighbors) > np.iinfo(np.uint32).max:
            raise SystemExit("neighbors contains ids > u32::MAX, unsupported")

    train = np.ascontiguousarray(train, dtype=np.float32)
    test = np.ascontiguousarray(test, dtype=np.float32)
    neighbors = np.ascontiguousarray(neighbors, dtype=np.uint32)

    train_rows, dim = train.shape
    test_rows = test.shape[0]
    neighbors_cols = neighbors.shape[1]

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("wb") as out:
        out.write(MAGIC)
        out.write(struct.pack("<QQQQ", train_rows, test_rows, dim, neighbors_cols))
        out.write(train.tobytes(order="C"))
        out.write(test.tobytes(order="C"))
        out.write(neighbors.tobytes(order="C"))

    meta = {
        "input": str(args.input),
        "input_sha256": sha256_file(args.input),
        "output": str(args.output),
        "output_sha256": sha256_file(args.output),
        "train_shape": [int(x) for x in train.shape],
        "test_shape": [int(x) for x in test.shape],
        "neighbors_shape": [int(x) for x in neighbors.shape],
        "dtype": {
            "train": str(train.dtype),
            "test": str(test.dtype),
            "neighbors": str(neighbors.dtype),
        },
    }

    meta_path = args.output.with_suffix(args.output.suffix + ".meta.json")
    meta_path.write_text(json.dumps(meta, indent=2) + "\n", encoding="utf-8")

    print(f"Wrote {args.output}")
    print(f"Wrote {meta_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
