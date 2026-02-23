from __future__ import annotations

import logging
import os
import shutil
import socket
import subprocess
import tempfile
import time
from collections.abc import Iterable, Iterator
from typing import Any, Optional

import grpc
import numpy as np
from kyrodb import InsertItem, KyroDBClient

try:
    from kyrodb._generated import kyrodb_pb2 as pb2
    from kyrodb._generated import kyrodb_pb2_grpc as pb2_grpc
except Exception:  # pragma: no cover - depends on installed SDK layout
    pb2 = None
    pb2_grpc = None

try:
    from ..base.module import BaseANN
except ImportError:  # pragma: no cover - allows standalone import tests
    class BaseANN:  # type: ignore
        pass


def _available_cores() -> int:
    try:
        return max(1, len(os.sched_getaffinity(0)))
    except (AttributeError, NotImplementedError, OSError):
        return max(1, os.cpu_count() or 1)


class _RawGrpcClient:
    """Thin ANN benchmark client path to minimize Python-side overhead."""

    def __init__(self, target: str) -> None:
        assert pb2_grpc is not None
        self._channel = grpc.insecure_channel(
            target,
            options=[
                ("grpc.keepalive_time_ms", 30000),
                ("grpc.keepalive_timeout_ms", 10000),
                ("grpc.keepalive_permit_without_calls", 1),
            ],
        )
        self._stub = pb2_grpc.KyroDBServiceStub(self._channel)

    def wait_for_ready(self, timeout_s: float) -> None:
        grpc.channel_ready_future(self._channel).result(timeout=timeout_s)

    def close(self) -> None:
        self._channel.close()

    def bulk_load_hnsw(self, vectors: np.ndarray, timeout_s: float | None) -> Any:
        assert pb2 is not None

        def req_stream() -> Iterator[Any]:
            for index, vec in enumerate(vectors, start=1):
                request = pb2.InsertRequest(doc_id=index)
                request.embedding.extend(vec)
                yield request

        return self._stub.BulkLoadHnsw(req_stream(), timeout=timeout_s)

    def search(self, query_vector: np.ndarray, k: int, ef_search: int, timeout_s: float | None) -> Any:
        assert pb2 is not None
        request = pb2.SearchRequest(k=int(k), ef_search=int(ef_search))
        request.query_embedding.extend(query_vector)
        return self._stub.Search(request, timeout=timeout_s)

    def metrics(self, timeout_s: float) -> Any:
        assert pb2 is not None
        return self._stub.Metrics(pb2.MetricsRequest(), timeout=timeout_s)


class KyroDB(BaseANN):
    def __init__(self, metric: str, params: dict[str, Any]):
        self.metric = metric
        self.params = params

        self.M = int(params.get("M", 16))
        self.ef_construction = int(params.get("ef_construction", 200))
        self.ef_search = int(params.get("ef_search", 50))

        self.host = os.environ.get("KYRODB_HOST", "127.0.0.1")
        self.port, self.http_port = self._resolve_ports()
        self.target = f"{self.host}:{self.port}"

        self._client: Optional[KyroDBClient] = None
        self._raw_client: Optional[_RawGrpcClient] = None
        self._server_proc: Optional[subprocess.Popen[bytes]] = None
        self._data_dir: Optional[str] = None
        self._server_stdout = None
        self._server_stderr = None
        self._server_stdout_path: Optional[str] = None
        self._server_stderr_path: Optional[str] = None
        self._last_memory_kb: float = 0.0

        self.name = "KyroDB"

        self._logger = logging.getLogger(__name__)

    def _resolve_ports(self) -> tuple[int, int]:
        # Use benchmark-scoped overrides only. Do not honor generic KYRODB_PORT
        # from ambient environment/image because it breaks parallel ANN workers.
        forced_port = os.environ.get("KYRODB_BENCH_FORCE_PORT")
        forced_http_port = os.environ.get("KYRODB_BENCH_FORCE_HTTP_PORT")
        if forced_port is not None:
            grpc_port = int(forced_port)
            http_port = int(forced_http_port) if forced_http_port is not None else grpc_port + 1000
            return grpc_port, http_port

        # Use an isolated high-port pair to avoid cross-run collisions with stale servers.
        low = 20000
        high = 59000
        span = high - low
        host_fingerprint = sum(ord(ch) for ch in socket.gethostname())
        start = low + ((os.getpid() + host_fingerprint) % span)
        for offset in range(span):
            grpc_port = low + ((start - low + offset) % span)
            http_port = grpc_port + 1000
            if self._port_is_available(grpc_port) and self._port_is_available(http_port):
                return grpc_port, http_port

        raise RuntimeError("failed to allocate free benchmark ports for kyrodb_server")

    def _port_is_available(self, port: int) -> bool:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.bind((self.host, port))
            except OSError:
                return False
        return True

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
        if self._server_proc is not None:
            has_client = self._raw_client is not None or self._client is not None
            if has_client and self._server_proc.poll() is None:
                return
            # Partial initialization or dead process from a previous attempt.
            self._cleanup_server()
        elif self._raw_client is not None or self._client is not None:
            # Defensive cleanup for impossible partial state.
            self._cleanup_server()

        server_bin = shutil.which("kyrodb_server")
        if server_bin is None:
            raise RuntimeError("kyrodb_server binary not found in PATH")

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

        # Respect container CPU affinity to avoid oversubscription in single-core official runs.
        threads = env.get("KYRODB_BENCH_THREADS", str(_available_cores()))
        env.setdefault("TOKIO_WORKER_THREADS", threads)
        env.setdefault("RAYON_NUM_THREADS", threads)
        env.setdefault("OMP_NUM_THREADS", threads)

        env["KYRODB__SERVER__HTTP_PORT"] = str(self.http_port)
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
                [server_bin],
                env=env,
                stdout=self._server_stdout,
                stderr=self._server_stderr,
            )
            use_sdk_path = os.environ.get("KYRODB_BENCH_USE_SDK", "").lower() in {"1", "true", "yes"}
            if not use_sdk_path and pb2 is not None and pb2_grpc is not None:
                self._raw_client = _RawGrpcClient(target=self.target)
                self._raw_client.wait_for_ready(timeout_s=30.0)
                self._logger.info("Using thin gRPC benchmark client path")
            else:
                self._client = KyroDBClient(target=self.target, default_timeout_s=30.0)
                self._client.wait_for_ready(timeout_s=30.0)
                self._logger.info("Using KyroDB SDK benchmark client path")
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

    def _sample_server_rss_kb(self) -> Optional[float]:
        if self._server_proc is None:
            return None
        pid = self._server_proc.pid

        status_path = f"/proc/{pid}/status"
        try:
            with open(status_path, "r", encoding="utf-8") as handle:
                for line in handle:
                    if line.startswith("VmRSS:"):
                        parts = line.split()
                        if len(parts) >= 2:
                            return float(parts[1])
        except Exception:
            pass

        statm_path = f"/proc/{pid}/statm"
        try:
            with open(statm_path, "r", encoding="utf-8") as handle:
                parts = handle.read().split()
            if len(parts) >= 2:
                rss_pages = int(parts[1])
                page_size = os.sysconf("SC_PAGESIZE")
                return float(rss_pages * page_size) / 1024.0
        except Exception:
            return None

    def _remember_memory_sample(self) -> None:
        rss_kb = self._sample_server_rss_kb()
        if rss_kb is not None and rss_kb > 0.0:
            self._last_memory_kb = max(self._last_memory_kb, rss_kb)

    @staticmethod
    def _insert_items(vectors: Iterable[np.ndarray]) -> Iterator[InsertItem]:
        for i, vec in enumerate(vectors):
            yield InsertItem.from_parts(doc_id=i + 1, embedding=vec)

    def fit(self, X: np.ndarray) -> None:
        if len(X.shape) != 2:
            raise ValueError(f"expected 2D array, got shape={X.shape}")

        index_vectors = np.asarray(X, dtype=np.float32)
        if self._needs_unit_norm():
            index_vectors = self._normalize_rows(index_vectors)

        self._start_server_if_needed(int(index_vectors.shape[1]), int(len(index_vectors)))
        start = time.time()
        try:
            if self._raw_client is not None:
                bulk_result = self._raw_client.bulk_load_hnsw(index_vectors, timeout_s=None)
            else:
                if self._client is None:
                    raise RuntimeError("benchmark client is not initialized")
                bulk_result = self._client.bulk_load_hnsw(
                    self._insert_items(index_vectors),
                    timeout_s=None,
                )

            if not bulk_result.success:
                raise RuntimeError(bulk_result.error or "BulkLoadHnsw returned success=false")

            elapsed = time.time() - start
            peak_memory_bytes = getattr(bulk_result, "peak_memory_bytes", 0)
            avg_insert_rate = float(getattr(bulk_result, "avg_insert_rate", 0))
            if peak_memory_bytes:
                self._last_memory_kb = max(self._last_memory_kb, float(peak_memory_bytes) / 1024.0)
            self._logger.info(
                "BulkLoadHnsw complete: n=%d dim=%d seconds=%.2f rate=%.0f vec/s",
                len(index_vectors),
                index_vectors.shape[1],
                elapsed,
                avg_insert_rate,
            )
            self._remember_memory_sample()
            return
        except Exception as exc:
            stderr_tail = self._read_stderr_tail()
            if stderr_tail:
                self._logger.error("KyroDB server stderr tail:\n%s", stderr_tail)
            raise RuntimeError(f"BulkLoadHnsw failed: {exc}") from exc

    def set_query_arguments(self, *args: Any, **kwargs: Any) -> None:
        candidate: Any = None
        if kwargs.get("ef_search") is not None:
            candidate = kwargs.get("ef_search")
        elif args:
            # ann-benchmarks adapters can pass positional query args in multiple shapes:
            # - scalar value (e.g., 200)
            # - single-element list/tuple (e.g., [200])
            # - expanded tuple where first value is ef_search
            candidate = args[0]
        elif kwargs.get("args"):
            candidate = kwargs["args"][0]

        if isinstance(candidate, (list, tuple)):
            if len(candidate) == 0:
                raise ValueError("ef_search query argument cannot be empty")
            candidate = candidate[0]

        if candidate is not None:
            self.ef_search = int(candidate)
        self.name = f"KyroDB(M={self.M}, efConstruction={self.ef_construction}, efSearch={self.ef_search})"

    def query(self, q: np.ndarray, k: int) -> list[int]:
        query_vec = np.asarray(q, dtype=np.float32, order="C")
        if self._needs_unit_norm():
            query_vec = self._normalize_vector(query_vec)

        if self._raw_client is not None:
            response = self._raw_client.search(
                query_vector=query_vec,
                k=int(k),
                ef_search=int(self.ef_search),
                timeout_s=None,
            )
            if response.error:
                raise RuntimeError(f"Search failed: {response.error}")
            return [int(hit.doc_id) - 1 for hit in response.results]

        if self._client is None:
            raise RuntimeError("benchmark client is not initialized")
        response = self._client.search(
            query_embedding=query_vec,
            k=int(k),
            ef_search=int(self.ef_search),
            timeout_s=None,
        )
        if response.error:
            raise RuntimeError(f"Search failed: {response.error}")
        return [int(hit.doc_id) - 1 for hit in response.results]

    def get_memory_usage(self) -> float:
        self._remember_memory_sample()

        try:
            if self._raw_client is not None:
                metrics = self._raw_client.metrics(timeout_s=5.0)
                if getattr(metrics, "memory_usage_bytes", 0):
                    memory_kb = float(metrics.memory_usage_bytes) / 1024.0
                    if memory_kb > 0.0:
                        self._last_memory_kb = max(self._last_memory_kb, memory_kb)
                        return memory_kb
            if self._client is not None:
                metrics = self._client.metrics(timeout_s=5.0)
                if getattr(metrics, "memory_usage_bytes", 0):
                    memory_kb = float(metrics.memory_usage_bytes) / 1024.0
                    if memory_kb > 0.0:
                        self._last_memory_kb = max(self._last_memory_kb, memory_kb)
                        return memory_kb
        except Exception as exc:
            self._logger.warning("Failed to query metrics: %s", exc)

        rss_kb = self._sample_server_rss_kb()
        if rss_kb is not None and rss_kb > 0.0:
            self._last_memory_kb = max(self._last_memory_kb, rss_kb)
            return rss_kb

        if self._last_memory_kb > 0.0:
            return self._last_memory_kb

        self._logger.warning("Unable to sample benchmark memory usage; returning 0 KB")
        return 0.0

    def _cleanup_server(self) -> None:
        self._remember_memory_sample()

        try:
            if self._raw_client is not None:
                self._raw_client.close()
        finally:
            self._raw_client = None

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
