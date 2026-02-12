"""KyroDB ANN-Benchmarks integration module.

This file is meant to be copied into the upstream ann-benchmarks repository at:
  ann_benchmarks/algorithms/kyrodb/module.py

The ann-benchmarks runner executes inside a Docker container and imports this
module via the configured `module` + `constructor` in `config.yml`.

KyroDB is run as an in-container gRPC server process started lazily on first
`fit()`, then queried via gRPC.
"""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
import tempfile
import time
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

try:
    from ..base.module import BaseANN
except Exception:  # pragma: no cover - allows standalone import tests
    class BaseANN:  # type: ignore
        pass

try:
    from . import kyrodb_pb2
    from . import kyrodb_pb2_grpc
except ImportError as e:
    # The generated protobuf stubs MUST be present in this package directory.
    # The Dockerfile generates them and copies them here. If this fails:
    # 1. Rebuild the Docker image completely: docker builder prune -f && python install.py --algorithm kyrodb
    # 2. Ensure the Dockerfile generates stubs from engine/proto/kyrodb.proto
    raise ImportError(
        "kyrodb_pb2 / kyrodb_pb2_grpc not found. "
        "Ensure the Docker image was built correctly with protobuf stub generation. "
        f"Original error: {e}"
    ) from e

import grpc


class KyroDB(BaseANN):
    def __init__(self, metric: str, params: Dict[str, Any]):
        self.metric = metric
        self.params = params

        self.M = int(params.get("M", 16))
        self.ef_construction = int(params.get("ef_construction", 200))
        self.ef_search = int(params.get("ef_search", 50))
        self.ann_search_mode = str(
            params.get(
                "ann_search_mode",
                os.environ.get("KYRODB_BENCH_ANN_SEARCH_MODE", "fp32-strict"),
            )
        ).strip().lower().replace("_", "-")
        self.quantized_rerank_multiplier = int(
            params.get(
                "quantized_rerank_multiplier",
                os.environ.get("KYRODB_BENCH_QUANTIZED_RERANK_MULTIPLIER", "8"),
            )
        )

        if self.ann_search_mode not in {"fp32-strict", "sq8-rerank"}:
            raise ValueError(
                "ann_search_mode must be one of fp32-strict|sq8-rerank, "
                f"got '{self.ann_search_mode}'"
            )
        if not (1 <= self.quantized_rerank_multiplier <= 64):
            raise ValueError(
                "quantized_rerank_multiplier must be in [1, 64], "
                f"got {self.quantized_rerank_multiplier}"
            )

        self.host = os.environ.get("KYRODB_HOST", "127.0.0.1")
        self.port = int(os.environ.get("KYRODB_PORT", "50051"))

        self._channel: Optional[grpc.Channel] = None
        self._stub: Optional[kyrodb_pb2_grpc.KyroDBServiceStub] = None
        self._server_proc: Optional[subprocess.Popen] = None
        self._data_dir: Optional[str] = None
        self._server_stdout = None
        self._server_stderr = None
        self._server_stdout_path: Optional[str] = None
        self._server_stderr_path: Optional[str] = None

        self._logger = logging.getLogger(__name__)

    def _needs_unit_norm(self) -> bool:
        metric = self.metric.lower()
        return metric in ("angular", "cosine")

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

    def _connect(self) -> None:
        if self._channel is None:
            options = [
                ("grpc.max_send_message_length", 100 * 1024 * 1024),
                ("grpc.max_receive_message_length", 100 * 1024 * 1024),
                ("grpc.keepalive_time_ms", 10_000),
                ("grpc.keepalive_timeout_ms", 5_000),
            ]
            self._channel = grpc.insecure_channel(f"{self.host}:{self.port}", options=options)
            self._stub = kyrodb_pb2_grpc.KyroDBServiceStub(self._channel)

    def _metric_to_distance_enum(self) -> str:
        metric = self.metric.lower()
        if metric in ("angular", "cosine"):
            return "cosine"
        if metric in ("euclidean", "l2"):
            return "euclidean"
        raise ValueError(
            f"Unsupported metric '{self.metric}'. Supported metrics: angular, cosine, euclidean, l2"
        )

    def _start_server_if_needed(self, dimension: int, max_elements: int) -> None:
        if self._server_proc is not None:
            return

        if self._data_dir is None:
            self._data_dir = tempfile.mkdtemp(prefix="kyrodb_annb_")

        env = os.environ.copy()
        env["KYRODB_PORT"] = str(self.port)
        env["KYRODB_DATA_DIR"] = self._data_dir

        # KyroDB config is loaded via `KYRODB__...` env vars (config crate).
        env["KYRODB__HNSW__DIMENSION"] = str(int(dimension))
        env["KYRODB__HNSW__DISTANCE"] = self._metric_to_distance_enum()
        env["KYRODB__HNSW__M"] = str(self.M)
        env["KYRODB__HNSW__EF_CONSTRUCTION"] = str(self.ef_construction)
        env["KYRODB__HNSW__MAX_ELEMENTS"] = str(int(max_elements))
        env["KYRODB__HNSW__ANN_SEARCH_MODE"] = self.ann_search_mode.replace("-", "_")
        env["KYRODB__HNSW__QUANTIZED_RERANK_MULTIPLIER"] = str(
            self.quantized_rerank_multiplier
        )

        # Bench runs should measure ANN performance, not durability.
        env.setdefault("KYRODB__ENVIRONMENT__TYPE", "benchmark")
        env.setdefault("KYRODB__PERSISTENCE__SNAPSHOT_INTERVAL_MUTATIONS", "0")
        env.setdefault("KYRODB__PERSISTENCE__FSYNC_POLICY", "none")
        env.setdefault("KYRODB__PERSISTENCE__ALLOW_FRESH_START_ON_RECOVERY_FAILURE", "true")

        # Minimize non-ANN behavior (predictor training/query cache effects).
        env.setdefault("KYRODB__CACHE__STRATEGY", "lru")
        env.setdefault("KYRODB__CACHE__CAPACITY", "1")
        env.setdefault("KYRODB__CACHE__MIN_TRAINING_SAMPLES", "1")
        env.setdefault("KYRODB__CACHE__QUERY_CACHE_CAPACITY", "1")
        env.setdefault("KYRODB__CACHE__ENABLE_TRAINING_TASK", "false")
        env.setdefault("KYRODB__CACHE__AUTO_TUNE_THRESHOLD", "false")

        # Keep thread counts bounded and explicit for reproducibility.
        # Allow overrides via KYRODB_BENCH_THREADS.
        threads = env.get("KYRODB_BENCH_THREADS", "1")
        env.setdefault("TOKIO_WORKER_THREADS", threads)
        env.setdefault("RAYON_NUM_THREADS", threads)
        env.setdefault("OMP_NUM_THREADS", threads)

        # Default HTTP port is grpc+1000. Keep it explicit to make health waits deterministic.
        env["KYRODB__SERVER__HTTP_PORT"] = str(self.port + 1000)

        # Keep logs small but visible in container logs if needed.
        env.setdefault("RUST_LOG", "warn")
        env.setdefault("KYRODB__LOGGING__LEVEL", "warn")

        self._logger.info(
            "Starting kyrodb_server (port=%s http_port=%s dim=%s metric=%s ann_mode=%s rerank_mult=%s)",
            self.port,
            self.port + 1000,
            dimension,
            env["KYRODB__HNSW__DISTANCE"],
            env["KYRODB__HNSW__ANN_SEARCH_MODE"],
            env["KYRODB__HNSW__QUANTIZED_RERANK_MULTIPLIER"],
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
        except Exception:
            self._server_stdout.close()
            if self._server_stderr is not None:
                self._server_stderr.close()
            raise

        # Wait for server readiness (best-effort) by probing gRPC channel connectivity.
        deadline = time.time() + 30.0
        last_err: Optional[BaseException] = None
        while time.time() < deadline:
            try:
                self._connect()
                assert self._channel is not None
                grpc.channel_ready_future(self._channel).result(timeout=1.0)
                return
            except Exception as e:
                last_err = e
                time.sleep(0.2)

        stderr_tail = None
        if self._server_stderr_path is not None:
            try:
                if self._server_stderr is not None:
                    self._server_stderr.flush()
                with open(self._server_stderr_path, "rb") as f:
                    data = f.read()
                stderr_tail = data[-8192:].decode("utf-8", errors="replace")
            except Exception:
                stderr_tail = None

        if stderr_tail:
            self._cleanup_server()
            raise RuntimeError(
                f"kyrodb_server failed to become ready: {last_err}; stderr tail:\n{stderr_tail}"
            )
        self._cleanup_server()
        raise RuntimeError(f"kyrodb_server failed to become ready: {last_err}")

    def _generate_insert_requests(self, X: np.ndarray):
        for i, vec in enumerate(X):
            yield kyrodb_pb2.InsertRequest(doc_id=i + 1, embedding=list(map(float, vec)))

    def fit(self, X: np.ndarray) -> None:
        if len(X.shape) != 2:
            raise ValueError(f"expected 2D array, got shape={X.shape}")

        index_vectors = np.asarray(X, dtype=np.float32)
        if self._needs_unit_norm():
            index_vectors = self._normalize_rows(index_vectors)

        self._start_server_if_needed(int(index_vectors.shape[1]), int(len(index_vectors)))
        self._connect()
        assert self._stub is not None

        # Prefer BulkLoadHnsw for benchmark ingestion (bypasses hot tier, avoids flush).
        start = time.time()
        try:
            resp = self._stub.BulkLoadHnsw(self._generate_insert_requests(index_vectors))
            if not resp.success:
                raise RuntimeError(resp.error)
            elapsed = time.time() - start
            self._logger.info(
                "BulkLoadHnsw complete: n=%d dim=%d seconds=%.2f rate=%.0f vec/s",
                len(index_vectors),
                index_vectors.shape[1],
                elapsed,
                float(getattr(resp, "avg_insert_rate", 0.0) or 0.0),
            )
            return
        except grpc.RpcError as e:
            self._logger.info("BulkLoadHnsw unavailable (%s); falling back to BulkInsert+Flush", e.code())
        except Exception as e:
            self._logger.info("BulkLoadHnsw failed (%s); falling back to BulkInsert+Flush", e)

        start_insert = time.time()
        response = self._stub.BulkInsert(self._generate_insert_requests(index_vectors))
        if not response.success:
            raise RuntimeError(f"BulkInsert failed: {response.error}")

        elapsed = time.time() - start_insert
        self._logger.info(
            "BulkInsert complete: n=%d dim=%d seconds=%.2f",
            len(index_vectors),
            index_vectors.shape[1],
            elapsed,
        )

        flush = self._stub.FlushHotTier(kyrodb_pb2.FlushRequest(force=True))
        if not flush.success:
            raise RuntimeError(f"FlushHotTier failed: {flush.error}")

    def set_query_arguments(self, ef_search: Optional[int] = None, **kwargs) -> None:
        if ef_search is not None:
            self.ef_search = int(ef_search)
            return
        if kwargs.get("args") and len(kwargs["args"]) > 0:
            self.ef_search = int(kwargs["args"][0])

    def _score_to_distance(self, score: float) -> float:
        metric = self.metric.lower()
        if metric in ("angular", "cosine"):
            return 1.0 - score
        if metric in ("euclidean", "l2"):
            if score <= 0.0:
                return float("inf")
            return (1.0 / score) - 1.0
        if metric in ("inner_product", "inner-product", "innerproduct"):
            return 1.0 - score
        return 1.0 - score

    def query(self, q: np.ndarray, k: int) -> List[Tuple[int, float]]:
        self._connect()
        assert self._stub is not None

        query_vec = np.asarray(q, dtype=np.float32)
        if self._needs_unit_norm():
            query_vec = self._normalize_vector(query_vec)

        req = kyrodb_pb2.SearchRequest(
            query_embedding=list(map(float, query_vec)),
            k=int(k),
            ef_search=int(self.ef_search),
        )

        resp = self._stub.Search(req)
        # ann-benchmarks expects 0-indexed dataset row ids; KyroDB uses doc_ids starting at 1.
        return [(int(r.doc_id) - 1, float(self._score_to_distance(r.score))) for r in resp.results]

    def batch_query(self, X: np.ndarray, k: int) -> List[List[Tuple[int, float]]]:
        self._connect()
        assert self._stub is not None

        query_vectors = np.asarray(X, dtype=np.float32)
        if self._needs_unit_norm():
            query_vectors = self._normalize_rows(query_vectors)

        def gen():
            for q in query_vectors:
                yield kyrodb_pb2.SearchRequest(
                    query_embedding=list(map(float, q)),
                    k=int(k),
                    ef_search=int(self.ef_search),
                )

        out: List[List[Tuple[int, float]]] = []
        for resp in self._stub.BulkSearch(gen()):
            out.append(
                [(int(r.doc_id) - 1, float(self._score_to_distance(r.score))) for r in resp.results]
            )
        return out

    def get_memory_usage(self) -> float:
        try:
            self._connect()
            assert self._stub is not None
            resp = self._stub.Metrics(kyrodb_pb2.MetricsRequest())
            return float(resp.memory_usage_bytes) / (1024.0 * 1024.0)
        except Exception:
            return 0.0

    def _cleanup_server(self) -> None:
        try:
            if self._channel is not None:
                self._channel.close()
        finally:
            self._channel = None
            self._stub = None

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
