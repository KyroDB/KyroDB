#!/usr/bin/env python3

from __future__ import annotations

import hashlib
import json
import os
import platform
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional


def _run(cmd: list[str], timeout_s: float = 5.0, allow_empty: bool = False) -> Optional[str]:
    try:
        proc = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=timeout_s,
            check=False,
            text=True,
        )
        out = (proc.stdout or "").strip()
        if out:
            return out
        return "" if allow_empty else None
    except Exception:
        return None


def _sha256_file(path: Path, max_bytes: int | None = None) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        if max_bytes is None:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        else:
            remaining = max_bytes
            while remaining > 0:
                chunk = f.read(min(1024 * 1024, remaining))
                if not chunk:
                    break
                h.update(chunk)
                remaining -= len(chunk)
    return h.hexdigest()


def collect_environment(extra: Dict[str, Any] | None = None) -> Dict[str, Any]:
    now = datetime.now(timezone.utc)

    env: Dict[str, Any] = {
        "timestamp_utc": now.isoformat(),
        "platform": {
            "system": platform.system(),
            "release": platform.release(),
            "version": platform.version(),
            "machine": platform.machine(),
            "processor": platform.processor(),
        },
        "python": {
            "version": sys.version,
            "executable": sys.executable,
        },
        "git": {
            "commit": _run(["git", "rev-parse", "HEAD"]),
            "dirty": _run(["git", "status", "--porcelain"], allow_empty=True),
        },
    }

    git_dirty_output = env["git"]["dirty"]
    if git_dirty_output is not None:
        env["git"]["dirty"] = bool(git_dirty_output.strip())
    elif env["git"]["commit"] is not None:
        env["git"]["dirty"] = False

    # Best-effort system probes (Linux VM friendly; safe to ignore if missing).
    env["system"] = {
        "uname_a": _run(["uname", "-a"]),
        "lscpu": _run(["lscpu"], timeout_s=10.0) if shutil.which("lscpu") else None,
        "numactl_h": _run(["numactl", "-H"], timeout_s=10.0) if shutil.which("numactl") else None,
    }

    pip_freeze = _run([sys.executable, "-m", "pip", "freeze"], timeout_s=10.0)
    if pip_freeze is not None:
        env["python"]["pip_freeze"] = pip_freeze.splitlines()

    if extra:
        env.update(extra)

    return env


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Collect benchmark environment metadata")
    parser.add_argument("--out", type=str, required=True, help="Output JSON path")
    parser.add_argument(
        "--dataset-file",
        type=str,
        default=None,
        help="Optional dataset file path to hash (sha256)",
    )
    args = parser.parse_args()

    extra: Dict[str, Any] = {}

    if args.dataset_file:
        p = Path(args.dataset_file)
        if p.exists():
            extra["dataset_file"] = {
                "path": str(p),
                "size_bytes": p.stat().st_size,
                "sha256": _sha256_file(p),
            }
        else:
            print(
                f"Warning: dataset file not found: {p}",
                file=sys.stderr,
            )

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload = collect_environment(extra=extra)
    out_path.write_text(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
