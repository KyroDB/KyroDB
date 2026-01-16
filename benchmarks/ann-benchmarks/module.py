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
except Exception:  # pragma: no cover - allows standalone import tests
    import kyrodb_pb2  # type: ignore
    import kyrodb_pb2_grpc  # type: ignore

import grpc


class KyroDB(BaseANN):
    def __init__(self, metric: str, params: Dict[str, Any]):
        self.metric = metric
        self.params = params

        self.M = int(params.get("M", 16))
        self.ef_construction = int(params.get("ef_construction", 200))
        self.ef_search = int(params.get("ef_search", 50))

        self.host = os.environ.get("KYRODB_HOST", "127.0.0.1")
        self.port = int(os.environ.get("KYRODB_PORT", "50051"))

        self._channel: Optional[grpc.Channel] = None
        self._stub: Optional[kyrodb_pb2_grpc.KyroDBServiceStub] = None
        self._server_proc: Optional[subprocess.Popen] = None
        self._data_dir: Optional[str] = None

        self._logger = logging.getLogger(__name__)

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
        return "cosine"

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

        # Bench runs should measure ANN performance, not durability.
        env.setdefault("KYRODB__PERSISTENCE__ENABLE_WAL", "false")
        env.setdefault("KYRODB__PERSISTENCE__SNAPSHOT_INTERVAL_SECS", "0")
        env.setdefault("KYRODB__PERSISTENCE__FSYNC_POLICY", "none")

        # ann-benchmarks expects single-CPU saturation; keep thread counts bounded.
        env.setdefault("TOKIO_WORKER_THREADS", "1")
        env.setdefault("RAYON_NUM_THREADS", "1")
        env.setdefault("OMP_NUM_THREADS", "1")

        # Default HTTP port is grpc+1000. Keep it explicit to make health waits deterministic.
        env["KYRODB__SERVER__HTTP_PORT"] = str(self.port + 1000)

        # Keep logs small but visible in container logs if needed.
        env.setdefault("RUST_LOG", "info")

        self._logger.info(
            "Starting kyrodb_server (port=%s http_port=%s dim=%s metric=%s)",
            self.port,
            self.port + 1000,
            dimension,
            env["KYRODB__HNSW__DISTANCE"],
        )

        self._server_proc = subprocess.Popen(
            ["kyrodb_server"],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        # Wait for server readiness (best-effort) by probing gRPC channel connectivity.
        deadline = time.time() + 30.0
        last_err: Optional[BaseException] = None
        while time.time() < deadline:
            try:
                self._connect()
                assert self._channel is not None
                grpc.channel_ready_future(self._channel).result(timeout=1.0)
                return
            except BaseException as e:
                last_err = e
                time.sleep(0.2)

        raise RuntimeError(f"kyrodb_server failed to become ready: {last_err}")

    def _generate_insert_requests(self, X: np.ndarray):
        for i, vec in enumerate(X):
            yield kyrodb_pb2.InsertRequest(doc_id=i + 1, embedding=list(map(float, vec)))

    def fit(self, X: np.ndarray) -> None:
        if len(X.shape) != 2:
            raise ValueError(f"expected 2D array, got shape={X.shape}")

        self._start_server_if_needed(int(X.shape[1]), int(len(X)))
        self._connect()
        assert self._stub is not None

        start = time.time()
        response = self._stub.BulkInsert(self._generate_insert_requests(X))
        if not response.success:
            raise RuntimeError(f"BulkInsert failed: {response.error}")

        elapsed = time.time() - start
        self._logger.info("BulkInsert complete: n=%d dim=%d seconds=%.2f", len(X), X.shape[1], elapsed)

        # Ensure data is searchable via HNSW (cold tier) before benchmark queries.
        flush = self._stub.FlushHotTier(kyrodb_pb2.FlushRequest(force=True))
        if not flush.success:
            raise RuntimeError(f"FlushHotTier failed: {flush.error}")

    def set_query_arguments(self, ef_search: int = None, **kwargs) -> None:
        if ef_search is not None:
            self.ef_search = int(ef_search)
        if kwargs.get("args") and len(kwargs["args"]) > 0:
            self.ef_search = int(kwargs["args"][0])

    def _score_to_distance(self, score: float) -> float:
        metric = self.metric.lower()
        if metric in ("angular", "cosine"):
            return 1.0 - score
        if metric in ("euclidean", "l2"):
            return -score
        return 1.0 - score

    def query(self, q: np.ndarray, k: int) -> List[Tuple[int, float]]:
        self._connect()
        assert self._stub is not None

        req = kyrodb_pb2.SearchRequest(
            query_embedding=list(map(float, q)),
            k=int(k),
            ef_search=int(self.ef_search),
        )

        resp = self._stub.Search(req)
        return [(int(r.doc_id), float(self._score_to_distance(r.score))) for r in resp.results]

    def batch_query(self, X: np.ndarray, k: int) -> List[List[Tuple[int, float]]]:
        self._connect()
        assert self._stub is not None

        def gen():
            for q in X:
                yield kyrodb_pb2.SearchRequest(
                    query_embedding=list(map(float, q)),
                    k=int(k),
                    ef_search=int(self.ef_search),
                )

        out: List[List[Tuple[int, float]]] = []
        for resp in self._stub.BulkSearch(gen()):
            out.append([(int(r.doc_id), float(self._score_to_distance(r.score))) for r in resp.results])
        return out

    def get_memory_usage(self) -> float:
        try:
            self._connect()
            assert self._stub is not None
            resp = self._stub.Metrics(kyrodb_pb2.MetricsRequest())
            return float(resp.memory_usage_bytes) / (1024.0 * 1024.0)
        except Exception:
            return 0.0

    def done(self) -> None:
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
