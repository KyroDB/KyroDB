#!/usr/bin/env python3

from __future__ import annotations

import argparse
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate KyroDB gRPC Python stubs for benchmark tooling")
    parser.add_argument(
        "--proto",
        type=str,
        default="engine/proto/kyrodb.proto",
        help="Path to kyrodb.proto (repo-relative)",
    )
    parser.add_argument(
        "--out-dir",
        type=str,
        default="benchmarks/_generated",
        help="Output directory for generated python stubs",
    )
    args = parser.parse_args()

    try:
        from grpc_tools import protoc
    except Exception as e:
        raise SystemExit(
            "Missing dependency grpcio-tools. Install pinned deps with: pip install -r benchmarks/requirements.txt\n"
            f"Original error: {e}"
        )

    repo_root = Path(__file__).resolve().parents[2]
    proto_path = (repo_root / args.proto).resolve()
    if not proto_path.exists():
        raise SystemExit(f"Proto not found: {proto_path}")

    proto_name = proto_path.stem

    out_dir = (repo_root / args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    # Ensure importable package (even though the generated files use absolute imports).
    (out_dir / "__init__.py").touch(exist_ok=True)

    # Run protoc.
    include_dir = str(proto_path.parent)
    cmd = [
        "protoc",
        f"-I{include_dir}",
        f"--python_out={out_dir}",
        f"--grpc_python_out={out_dir}",
        str(proto_path),
    ]

    rc = protoc.main(cmd)
    if rc != 0:
        raise SystemExit(f"protoc failed with exit code {rc}")

    # grpc_tools generates absolute import `import <proto>_pb2 as ...`; keep stubs colocated.
    grpc_file = out_dir / f"{proto_name}_pb2_grpc.py"
    if grpc_file.exists():
        text = grpc_file.read_text()
        text = text.replace(
            f"import {proto_name}_pb2 as ",
            f"from . import {proto_name}_pb2 as ",
        )
        grpc_file.write_text(text)

    print(f"Generated stubs in: {out_dir}")


if __name__ == "__main__":
    main()
