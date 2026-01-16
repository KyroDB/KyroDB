"""Legacy compatibility shim.

Upstream ann-benchmarks expects the algorithm entrypoint to be
`ann_benchmarks.algorithms.kyrodb` with `constructor: KyroDB`.

The canonical implementation lives in `module.py`. This file remains only so
that older local scripts/imports don't silently pick up an outdated wrapper.
"""

from .module import KyroDB  # noqa: F401

__all__ = ["KyroDB"]
