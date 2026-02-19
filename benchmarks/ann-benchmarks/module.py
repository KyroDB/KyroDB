"""KyroDB ANN-Benchmarks adapter.

This file is copied into upstream ann-benchmarks at:
  ann_benchmarks/algorithms/kyrodb/module.py

The adapter starts a local `kyrodb_server` inside the benchmark container and
uses the released `kyrodb` Python SDK for all RPC interactions.
"""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
import tempfile
import time
from collections.abc import Iterator
from typing import Any, Optional

import grpc
import numpy as np
from kyrodb import InsertItem, KyroDBClient, SearchQuery
from kyrodb.errors import KyroDBError

try:
    from ..base.module import BaseANN
except Exception:  # pragma: no cover - allows standalone import tests
    class BaseANN:  # type: ignore
        pass


class KyroDB(BaseANN):
    def __init__(self, metric: str, params: dict[str, Any]):
        self.metric = metric
        self.params = params

        self.M = int(params.get("M", 16))
        self.ef_construction = int(params.get("ef_construction", 200))
        self.ef_search = int(params.get("ef_search", 50))

        self.host = os.environ.get("KYRODB_HOST", "127.0.0.1")
        self.port = int(os.environ.get("KYRODB_PORT", "50051"))
        self.target = f"{self.host}:{self.port}"

        self._client: Optional[KyroDBClient] = None
        self._server_proc: Optional[subprocess.Popen[bytes]] = None
        self._data_dir: Optional[str] = None
        self._server_stdout = None
        self._server_stderr = None
        self._server_stdout_path: Optional[str] = None
        self._server_stderr_path: Optional[str] = None

        self._batch_results: list[list[int]] = []
        self._batch_latencies: list[float] = []
        self.name = "KyroDB"

        self._logger = logging.getLogger(__name__)

    def _needs_unit_norm(self) -> bool:
        return self.metric.lower() in ("angular", "cosine")

    @staticmethod
    def _normalize_rows(arr: np.ndarray) -> np.ndarray:
        out = np.asarray(arr, dtype=np.float32)
        norms = np.linalg.norm(out, axis=1, keepdims=True)
        norms = np.maximum(norms, 1e-12)
        return out / norms

    @staticmethod
    def _normalize_vector(vec: np.ndarray) -> np.ndarray:
        out = np.asarray(vec, dtype=np.float32)
        norm = float(np.linalg.norm(out))
        if norm <= 1e-12:
            return out
        return out / norm

    def _metric_to_distance(self) -> str:
        metric = self.metric.lower()
        if metric in ("angular", "cosine"):
            return "cosine"
        if metric in ("euclidean", "l2"):
            return "euclidean"
        raise ValueError(
            f"Unsupported metric '{self.metric}'. Supported metrics: angular, cosine, euclidean, l2"
        )

    def _start_server_if_needed(self, dimension: int, max_elements: int) -> None:
        if self._server_proc is not None and self._client is not None:
            return

        if self._data_dir is None:
            self._data_dir = tempfile.mkdtemp(prefix="kyrodb_annb_")

        env = os.environ.copy()
        env["KYRODB_PORT"] = str(self.port)
        env["KYRODB_DATA_DIR"] = self._data_dir

        env["KYRODB__HNSW__DIMENSION"] = str(int(dimension))
        env["KYRODB__HNSW__DISTANCE"] = self._metric_to_distance()
        env["KYRODB__HNSW__M"] = str(self.M)
        env["KYRODB__HNSW__EF_CONSTRUCTION"] = str(self.ef_construction)
        env["KYRODB__HNSW__MAX_ELEMENTS"] = str(int(max_elements))

        env.setdefault("KYRODB__ENVIRONMENT__TYPE", "benchmark")
        env.setdefault("KYRODB__PERSISTENCE__SNAPSHOT_INTERVAL_MUTATIONS", "0")
        env.setdefault("KYRODB__PERSISTENCE__FSYNC_POLICY", "none")
        env.setdefault("KYRODB__PERSISTENCE__ALLOW_FRESH_START_ON_RECOVERY_FAILURE", "true")

        env.setdefault("KYRODB__CACHE__STRATEGY", "lru")
        env.setdefault("KYRODB__CACHE__CAPACITY", "1")
        env.setdefault("KYRODB__CACHE__MIN_TRAINING_SAMPLES", "1")
        env.setdefault("KYRODB__CACHE__QUERY_CACHE_CAPACITY", "1")
        env.setdefault("KYRODB__CACHE__ENABLE_TRAINING_TASK", "false")
        env.setdefault("KYRODB__CACHE__AUTO_TUNE_THRESHOLD", "false")

        threads = env.get("KYRODB_BENCH_THREADS", "1")
        env.setdefault("TOKIO_WORKER_THREADS", threads)
        env.setdefault("RAYON_NUM_THREADS", threads)
        env.setdefault("OMP_NUM_THREADS", threads)

        env["KYRODB__SERVER__HTTP_PORT"] = str(self.port + 1000)
        env.setdefault("RUST_LOG", "warn")
        env.setdefault("KYRODB__LOGGING__LEVEL", "warn")

        self._logger.info(
            "Starting kyrodb_server (target=%s dim=%s metric=%s)",
            self.target,
            dimension,
            env["KYRODB__HNSW__DISTANCE"],
        )

        self._server_stdout_path = os.path.join(self._data_dir, "kyrodb_server.stdout.log")
        self._server_stderr_path = os.path.join(self._data_dir, "kyrodb_server.stderr.log")
        self._server_stdout = open(self._server_stdout_path, "wb")
        try:
            self._server_stderr = open(self._server_stderr_path, "wb")
            self._server_proc = subprocess.Popen(
                ["kyrodb_server"],
                env=env,
                stdout=self._server_stdout,
                stderr=self._server_stderr,
            )
            self._client = KyroDBClient(target=self.target, default_timeout_s=30.0)
            self._client.wait_for_ready(timeout_s=30.0)
        except Exception as exc:
            stderr_tail = self._read_stderr_tail()
            self._cleanup_server()
            if stderr_tail:
                raise RuntimeError(
                    f"kyrodb_server failed to become ready: {exc}; stderr tail:\n{stderr_tail}"
                ) from exc
            raise RuntimeError(f"kyrodb_server failed to become ready: {exc}") from exc

    def _read_stderr_tail(self) -> Optional[str]:
        if self._server_stderr_path is None:
            return None
        try:
            if self._server_stderr is not None:
                self._server_stderr.flush()
            with open(self._server_stderr_path, "rb") as handle:
                data = handle.read()
            return data[-8192:].decode("utf-8", errors="replace")
        except Exception:
            return None

    @staticmethod
    def _insert_items(vectors: np.ndarray) -> Iterator[InsertItem]:
        for i, vec in enumerate(vectors):
            yield InsertItem.from_parts(doc_id=i + 1, embedding=vec)

    def fit(self, X: np.ndarray) -> None:
        if len(X.shape) != 2:
            raise ValueError(f"expected 2D array, got shape={X.shape}")

        index_vectors = np.asarray(X, dtype=np.float32)
        if self._needs_unit_norm():
            index_vectors = self._normalize_rows(index_vectors)

        self._start_server_if_needed(int(index_vectors.shape[1]), int(len(index_vectors)))
        assert self._client is not None

        start = time.time()
        try:
            bulk_result = self._client.bulk_load_hnsw(
                self._insert_items(index_vectors),
                timeout_s=None,
            )
            if not bulk_result.success:
                raise RuntimeError(bulk_result.error or "BulkLoadHnsw returned success=false")

            elapsed = time.time() - start
            self._logger.info(
                "BulkLoadHnsw complete: n=%d dim=%d seconds=%.2f rate=%.0f vec/s",
                len(index_vectors),
                index_vectors.shape[1],
                elapsed,
                bulk_result.avg_insert_rate,
            )
            return
        except KyroDBError as exc:
            if exc.code != grpc.StatusCode.UNIMPLEMENTED:
                raise
            self._logger.info("BulkLoadHnsw unavailable; falling back to BulkInsert+Flush")
        except Exception as exc:
            self._logger.info("BulkLoadHnsw failed (%s); falling back to BulkInsert+Flush", exc)

        insert_start = time.time()
        insert_ack = self._client.bulk_insert(
            self._insert_items(index_vectors),
            timeout_s=None,
        )
        if not insert_ack.success:
            raise RuntimeError(f"BulkInsert failed: {insert_ack.error}")

        elapsed = time.time() - insert_start
        self._logger.info(
            "BulkInsert complete: n=%d dim=%d seconds=%.2f",
            len(index_vectors),
            index_vectors.shape[1],
            elapsed,
        )

        flush_result = self._client.flush_hot_tier(force=True, timeout_s=None)
        if not flush_result.success:
            raise RuntimeError(f"FlushHotTier failed: {flush_result.error}")

    def set_query_arguments(self, ef_search: Optional[int] = None, **kwargs: Any) -> None:
        if ef_search is not None:
            self.ef_search = int(ef_search)
        elif kwargs.get("args") and len(kwargs["args"]) > 0:
            self.ef_search = int(kwargs["args"][0])
        self.name = f"KyroDB(M={self.M}, efConstruction={self.ef_construction}, efSearch={self.ef_search})"

    def query(self, q: np.ndarray, k: int) -> list[int]:
        assert self._client is not None

        query_vec = np.asarray(q, dtype=np.float32)
        if self._needs_unit_norm():
            query_vec = self._normalize_vector(query_vec)

        response = self._client.search(
            query_embedding=query_vec,
            k=int(k),
            ef_search=int(self.ef_search),
            timeout_s=None,
        )
        return [int(hit.doc_id) - 1 for hit in response.results]

    def batch_query(self, X: np.ndarray, k: int) -> None:
        assert self._client is not None

        query_vectors = np.asarray(X, dtype=np.float32)
        if self._needs_unit_norm():
            query_vectors = self._normalize_rows(query_vectors)

        def query_stream() -> Iterator[SearchQuery]:
            for q in query_vectors:
                yield SearchQuery.from_parts(
                    query_embedding=q,
                    k=int(k),
                    ef_search=int(self.ef_search),
                )

        start = time.time()
        out: list[list[int]] = []
        for response in self._client.bulk_search(query_stream(), timeout_s=None):
            out.append([int(hit.doc_id) - 1 for hit in response.results])
        total = time.time() - start

        per_query = total / float(len(query_vectors)) if len(query_vectors) > 0 else 0.0
        self._batch_results = out
        self._batch_latencies = [per_query] * len(out)

    def get_batch_results(self) -> list[list[int]]:
        return self._batch_results

    def get_batch_latencies(self) -> list[float]:
        return self._batch_latencies

    def get_memory_usage(self) -> float:
        try:
            assert self._client is not None
            metrics = self._client.metrics(timeout_s=5.0)
            return float(metrics.memory_usage_bytes) / 1024.0
        except Exception:
            return 0.0

    def _cleanup_server(self) -> None:
        try:
            if self._client is not None:
                self._client.close()
        finally:
            self._client = None

        if self._server_proc is not None:
            try:
                self._server_proc.terminate()
                self._server_proc.wait(timeout=5)
            except Exception:
                try:
                    self._server_proc.kill()
                except Exception:
                    pass
            finally:
                self._server_proc = None

        try:
            if self._server_stdout is not None:
                self._server_stdout.close()
        finally:
            self._server_stdout = None

        try:
            if self._server_stderr is not None:
                self._server_stderr.close()
        finally:
            self._server_stderr = None

        if self._data_dir is not None:
            try:
                shutil.rmtree(self._data_dir, ignore_errors=True)
            finally:
                self._data_dir = None

    def done(self) -> None:
        self._cleanup_server()

    def __str__(self) -> str:
        return self.name
