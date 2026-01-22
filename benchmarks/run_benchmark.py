#!/usr/bin/env python3
"""
KyroDB Local Benchmark Runner

Standalone script to run ANN benchmarks locally before submitting to ann-benchmarks.
Supports standard datasets and generates QPS vs Recall plots.

Usage:
    1. Start KyroDB server:
       cargo run --release --bin kyrodb_server
       
    2. Run benchmarks:
       python benchmarks/run_benchmark.py --dataset sift-128-euclidean --k 10
"""

import argparse
import json
import os
import sys
import time
import urllib.request
import math
import hashlib
import platform
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

# Add path for local imports
sys.path.insert(0, str(Path(__file__).parent / "ann-benchmarks"))

try:
    import h5py
except ImportError:
    raise SystemExit(
        "Missing dependency 'h5py'. Install benchmark dependencies with: "
        "pip install -r benchmarks/requirements.txt"
    )

try:
    import grpc
except ImportError:
    raise SystemExit(
        "Missing dependency 'grpcio'. Install benchmark dependencies with: "
        "pip install -r benchmarks/requirements.txt"
    )

# Import generated stubs (from benchmarks/ directory, not ann-benchmarks/)
bench_dir = Path(__file__).parent
generated_dir = bench_dir / "_generated"
if not (generated_dir / "kyrodb_pb2.py").exists() or not (generated_dir / "kyrodb_pb2_grpc.py").exists():
    # Generate stubs deterministically from the repo proto.
    gen_script = bench_dir / "scripts" / "gen_grpc_stubs.py"
    if not gen_script.exists():
        raise SystemExit(f"Missing stub generator: {gen_script}")
    proc = subprocess.run(
        [sys.executable, str(gen_script)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise SystemExit(f"Failed to generate gRPC stubs:\n{proc.stdout}")

sys.path.insert(0, str(bench_dir))

# Import generated stubs as a package so relative imports inside the generated
# files work correctly (kyrodb_pb2_grpc.py imports kyrodb_pb2).
from _generated import kyrodb_pb2  # type: ignore
from _generated import kyrodb_pb2_grpc  # type: ignore


# Dataset URLs (from ann-benchmarks)
DATASETS = {
    # Prefer https; keep http fallback for environments where TLS is blocked.
    "sift-128-euclidean": "https://ann-benchmarks.com/sift-128-euclidean.hdf5",
    "glove-100-angular": "https://ann-benchmarks.com/glove-100-angular.hdf5",
    "gist-960-euclidean": "https://ann-benchmarks.com/gist-960-euclidean.hdf5",
    "mnist-784-euclidean": "https://ann-benchmarks.com/mnist-784-euclidean.hdf5",
    "fashion-mnist-784-euclidean": "https://ann-benchmarks.com/fashion-mnist-784-euclidean.hdf5",
}


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _run_capture(cmd: List[str], timeout_s: float = 5.0) -> Optional[str]:
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
        return out if out else None
    except Exception:
        return None


def _collect_metadata(
    *,
    dataset_name: str,
    dataset_url: str,
    dataset_path: Path,
    args: argparse.Namespace,
) -> Dict[str, Any]:
    return {
        "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "dataset": {
            "name": dataset_name,
            "url": dataset_url,
            "path": str(dataset_path),
            "size_bytes": dataset_path.stat().st_size if dataset_path.exists() else None,
            "sha256": _sha256_file(dataset_path) if dataset_path.exists() else None,
        },
        "client": {
            "python": sys.version,
            "platform": {
                "system": platform.system(),
                "release": platform.release(),
                "machine": platform.machine(),
            },
            "git_commit": _run_capture(["git", "rev-parse", "HEAD"]),
            "grpc_target": f"{args.host}:{args.port}",
            "concurrency": int(args.concurrency),
            "bulk_search": bool(args.bulk_search),
            "warmup_queries": int(args.warmup_queries),
            "repetitions": int(args.repetitions),
            "max_queries": int(args.max_queries),
        },
    }


def download_dataset(name: str, data_dir: Path) -> Path:
    """Download benchmark dataset if not present."""
    data_dir.mkdir(parents=True, exist_ok=True)
    filepath = data_dir / f"{name}.hdf5"
    
    if filepath.exists():
        print(f"Dataset {name} already downloaded")
        return filepath
    
    url = DATASETS.get(name)
    if not url:
        raise ValueError(f"Unknown dataset: {name}. Available: {list(DATASETS.keys())}")
    
    print(f"Downloading {name} from {url}...")
    try:
        urllib.request.urlretrieve(url, filepath)
    except Exception:
        # Fallback to http for older environments.
        http_url = url.replace("https://", "http://", 1)
        print(f"  HTTPS download failed; retrying via HTTP: {http_url}")
        urllib.request.urlretrieve(http_url, filepath)
    print(f"Downloaded to {filepath}")
    return filepath


def load_dataset(filepath: Path) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Load dataset from HDF5 file."""
    with h5py.File(filepath, 'r') as f:
        train = np.array(f['train'])
        test = np.array(f['test'])
        neighbors = np.array(f['neighbors'])
    
    print(f"Loaded dataset: train={train.shape}, test={test.shape}")
    return train, test, neighbors


def compute_recall(results: List[Tuple[int, float]], ground_truth: np.ndarray, k: int) -> float:
    """Compute recall@k.
    
    Note: ground_truth is 0-indexed but our doc_ids are 1-indexed,
    so we subtract 1 from result doc_ids to compare.
    """
    if len(results) == 0:
        return 0.0
    gt_set = set(int(x) for x in ground_truth[:k])
    # Convert 1-indexed doc_ids back to 0-indexed for comparison with ground truth
    result_set = set(int(r[0]) - 1 for r in results[:k])
    return len(gt_set & result_set) / k


class KyroDBBenchmark:
    """KyroDB benchmark runner."""
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 50051,
        metric: str = "euclidean",
        ef_search: int = 0,
    ):
        self.host = host
        self.port = port
        self.metric = metric
        self.ef_search = int(ef_search)
        self._channel = None
        self._stub = None

    def _score_to_distance(self, score: float) -> float:
        metric = self.metric.lower()
        if metric in ("angular", "cosine"):
            return 1.0 - score
        if metric in ("euclidean", "l2"):
            return -score
        return 1.0 - score
        
    def connect(self):
        """Connect to KyroDB server."""
        if self._channel is None:
            options = [
                ('grpc.max_send_message_length', 100 * 1024 * 1024),
                ('grpc.max_receive_message_length', 100 * 1024 * 1024),
            ]
            self._channel = grpc.insecure_channel(
                f'{self.host}:{self.port}', options=options
            )
            self._stub = kyrodb_pb2_grpc.KyroDBServiceStub(self._channel)
            
            # Check connection
            try:
                self._stub.Health(kyrodb_pb2.HealthRequest())
                print(f"Connected to KyroDB at {self.host}:{self.port}")
            except grpc.RpcError as e:
                print(f"Failed to connect to KyroDB at {self.host}:{self.port}")
                print(f"  Error: {e.code()} - {e.details()}")
                print(f"\nMake sure KyroDB server is running:")
                print(f"  cargo run --release --bin kyrodb_server")
                sys.exit(1)

    def _new_stub(self):
        """Create a new gRPC stub.

        For concurrent benchmarks, using a stub per worker thread avoids
        contention and makes latency accounting clearer.
        """
        options = [
            ('grpc.max_send_message_length', 100 * 1024 * 1024),
            ('grpc.max_receive_message_length', 100 * 1024 * 1024),
        ]
        channel = grpc.insecure_channel(f'{self.host}:{self.port}', options=options)
        stub = kyrodb_pb2_grpc.KyroDBServiceStub(channel)
        return channel, stub
    
    def _generate_insert_requests(self, vectors: np.ndarray):
        """Generator for streaming bulk insert."""
        for i, vec in enumerate(vectors):
            yield kyrodb_pb2.InsertRequest(
                doc_id=i + 1,  # doc_id must be >= 1
                embedding=list(map(float, vec))
            )
            if (i + 1) % 10000 == 0:
                print(f"  Streaming {i+1}/{len(vectors)} vectors...")
                
    def index(self, vectors: np.ndarray):
        """Index vectors into KyroDB.
        
        Uses BulkLoadHnsw API for direct HNSW indexing (fastest).
        Falls back to BulkInsert + FlushHotTier if not available.
        """
        self.connect()
        n_vectors = len(vectors)
        
        print(f"Indexing {n_vectors:,} vectors (dim={vectors.shape[1]})...")
        start = time.time()
        
        # Try BulkLoadHnsw first (bypasses hot tier, fastest)
        try:
            print("  Using BulkLoadHnsw (direct HNSW indexing)...")
            response = self._stub.BulkLoadHnsw(self._generate_insert_requests(vectors))
            elapsed = time.time() - start
            
            if response.success:
                rate = response.avg_insert_rate if response.avg_insert_rate > 0 else n_vectors / elapsed
                print(f"  Loaded {response.total_loaded:,} vectors in {response.load_duration_ms/1000:.1f}s ({rate:,.0f} vec/s)")
                print(f"Indexed {n_vectors:,} vectors in {elapsed:.1f}s (total including gRPC)")
                return
            else:
                print(f"  BulkLoadHnsw failed: {response.error}")
                # Fall through to legacy path
        except grpc.RpcError as e:
            print(f"  BulkLoadHnsw not available ({e.code()}), using legacy path...")
        
        # Legacy path: BulkInsert + FlushHotTier
        try:
            response = self._stub.BulkInsert(self._generate_insert_requests(vectors))
            if response.success:
                elapsed_insert = time.time() - start
                print(f"  Bulk inserted {n_vectors:,} vectors in {elapsed_insert:.1f}s")
                # Now flush to HNSW
                self.flush_to_hnsw()
                elapsed = time.time() - start
                print(f"Indexed {n_vectors:,} vectors in {elapsed:.1f}s ({n_vectors/elapsed:,.0f} vec/s)")
                return
        except grpc.RpcError as e:
            print(f"  Bulk insert not available ({e.code()}), using individual inserts...")
        
        # Fallback to individual inserts (slowest)
        for i, vec in enumerate(vectors):
            req = kyrodb_pb2.InsertRequest(
                doc_id=i + 1,  # doc_id must be >= 1
                embedding=list(map(float, vec))
            )
            self._stub.Insert(req)
                    
            if (i + 1) % 50000 == 0:
                elapsed = time.time() - start
                rate = (i + 1) / elapsed
                print(f"  {i+1:,}/{n_vectors:,} ({rate:,.0f} vec/s)")
                
        elapsed = time.time() - start
        print(f"Indexed {n_vectors:,} vectors in {elapsed:.1f}s ({n_vectors/elapsed:,.0f} vec/s)")
        # Flush after individual inserts too
        self.flush_to_hnsw()
        
    def flush_to_hnsw(self):
        """Flush hot tier to HNSW cold tier for fast search.
        
        Data in hot tier is searched linearly (slow). Flushing moves it to
        HNSW index for O(log n) search performance.
        """
        self.connect()
        print("Flushing hot tier to HNSW...")
        start = time.time()
        
        try:
            response = self._stub.FlushHotTier(kyrodb_pb2.FlushRequest(force=True))
            elapsed = time.time() - start
            if response.success:
                print(f"Flushed {response.documents_flushed:,} documents to HNSW in {elapsed:.1f}s")
            else:
                print(f"Flush failed: {response.error}")
        except grpc.RpcError as e:
            elapsed = time.time() - start
            print(f"Flush RPC failed after {elapsed:.1f}s: {e.code()} - {e.details()}")
        
    def query(self, vector: np.ndarray, k: int) -> List[Tuple[int, float]]:
        """Query for k nearest neighbors."""
        request = kyrodb_pb2.SearchRequest(
            query_embedding=list(map(float, vector)),
            k=k,
            ef_search=int(self.ef_search),
        )
        
        response = self._stub.Search(request)
        # Convert similarity score to distance (lower is better)
        return [(r.doc_id, self._score_to_distance(r.score)) for r in response.results]

    def query_preconverted(self, stub, vector_list: List[float], k: int) -> List[Tuple[int, float]]:
        """Query using a pre-converted python list of floats.

        This removes numpy->python conversion cost from the per-request critical path
        when running concurrent benchmarks.
        """
        request = kyrodb_pb2.SearchRequest(
            query_embedding=vector_list,
            k=k,
            ef_search=int(self.ef_search),
        )
        response = stub.Search(request)
        return [(r.doc_id, self._score_to_distance(r.score)) for r in response.results]
        
    def benchmark_queries(
        self,
        queries: np.ndarray,
        ground_truth: np.ndarray,
        k: int,
        concurrency: int = 1,
        bulk_search: bool = False,
        warmup_queries: int = 0,
        repetitions: int = 1,
    ) -> dict:
        """Run query benchmarks.

        Notes on measurement:
        - Uses perf_counter for timing (monotonic, high resolution).
        - Reports wall-clock QPS for all modes for consistency.
        - In BulkSearch mode, per-query latencies are not directly observable
          (responses are streamed); we report response gaps as an approximation.
        """
        self.connect()

        concurrency = int(concurrency)
        if concurrency <= 0:
            raise ValueError("concurrency must be >= 1")
        
        mode = "BulkSearch" if bulk_search else "Search"

        warmup_queries = max(0, int(warmup_queries))
        repetitions = max(1, int(repetitions))

        print(
            f"\nRunning {len(queries)} queries (k={k}, concurrency={concurrency}, mode={mode}, "
            f"warmup={warmup_queries}, repetitions={repetitions})..."
        )

        def run_once() -> Dict[str, Any]:
            recalls: List[float] = []
            latencies_s: List[float] = []
            empty_results = 0

            # Warmup: run a small prefix of queries without recording.
            if warmup_queries > 0:
                warm_n = min(warmup_queries, len(queries))
                for i in range(warm_n):
                    _ = self.query(queries[i], k)

            # BulkSearch mode: single streaming call, responses in request order.
            if bulk_search:
                query_lists: List[List[float]] = [q.astype(np.float32).tolist() for q in queries]

                def request_iter():
                    for vec in query_lists:
                        yield kyrodb_pb2.SearchRequest(
                            query_embedding=vec,
                            k=k,
                            ef_search=int(self.ef_search),
                        )

                wall_start = time.perf_counter()
                last_response_time = wall_start
                responses = self._stub.BulkSearch(request_iter())

                for i, response in enumerate(responses):
                    now = time.perf_counter()
                    response_gap = now - last_response_time
                    last_response_time = now
                    result = [(r.doc_id, self._score_to_distance(r.score)) for r in response.results]

                    if len(result) == 0:
                        empty_results += 1

                    recall = compute_recall(result, ground_truth[i], k)
                    recalls.append(recall)
                    latencies_s.append(response_gap)

                    if (i + 1) % 500 == 0:
                        avg_recall = float(np.mean(recalls))
                        avg_gap_ms = float(np.mean(latencies_s)) * 1000
                        elapsed = time.perf_counter() - wall_start
                        wall_qps = (i + 1) / elapsed if elapsed > 0 else 0.0
                        print(
                            f"  Completed {i+1}/{len(query_lists)}: recall={avg_recall:.3f}, "
                            f"avg_resp_gap={avg_gap_ms:.2f}ms, wall_QPS={wall_qps:,.0f}"
                        )

                wall_elapsed = time.perf_counter() - wall_start
                wall_qps = len(query_lists) / wall_elapsed if wall_elapsed > 0 else 0.0

            # Concurrency=1 (sequential): direct per-query latency.
            elif concurrency == 1:
                wall_start = time.perf_counter()
                for i, (query, gt) in enumerate(zip(queries, ground_truth)):
                    start = time.perf_counter()
                    result = self.query(query, k)
                    latency = time.perf_counter() - start

                    if len(result) == 0:
                        empty_results += 1

                    recall = compute_recall(result, gt, k)
                    recalls.append(recall)
                    latencies_s.append(latency)

                    if (i + 1) % 500 == 0:
                        avg_recall = float(np.mean(recalls))
                        avg_latency = float(np.mean(latencies_s)) * 1000
                        elapsed = time.perf_counter() - wall_start
                        wall_qps = (i + 1) / elapsed if elapsed > 0 else 0.0
                        print(
                            f"  Query {i+1}/{len(queries)}: recall={avg_recall:.3f}, "
                            f"avg_latency={avg_latency:.2f}ms, wall_QPS={wall_qps:,.0f}"
                        )

                wall_elapsed = time.perf_counter() - wall_start
                wall_qps = len(queries) / wall_elapsed if wall_elapsed > 0 else 0.0
            else:
                # Pre-convert queries to python lists once to reduce per-request overhead.
                query_lists = [q.astype(np.float32).tolist() for q in queries]

                wall_start = time.perf_counter()

                # IMPORTANT: reuse one gRPC channel per worker thread.
                def run_batch(worker_indices: List[int]):
                    channel, stub = self._new_stub()
                    try:
                        out: List[Tuple[int, List[Tuple[int, float]], float]] = []
                        for idx in worker_indices:
                            start = time.perf_counter()
                            res = self.query_preconverted(stub, query_lists[idx], k)
                            latency = time.perf_counter() - start
                            out.append((idx, res, latency))
                        return out
                    finally:
                        channel.close()

                n = len(query_lists)
                chunk_size = int(math.ceil(n / concurrency))
                chunks: List[List[int]] = [
                    list(range(i, min(i + chunk_size, n))) for i in range(0, n, chunk_size)
                ]

                completed = 0
                with ThreadPoolExecutor(max_workers=concurrency) as executor:
                    futures = [executor.submit(run_batch, chunk) for chunk in chunks]
                    for fut in as_completed(futures):
                        batch = fut.result()
                        for idx, result, latency in batch:
                            if len(result) == 0:
                                empty_results += 1
                            recall = compute_recall(result, ground_truth[idx], k)
                            recalls.append(recall)
                            latencies_s.append(latency)

                        completed += len(batch)
                        if completed % 500 == 0 or completed == n:
                            avg_recall = float(np.mean(recalls))
                            avg_latency = float(np.mean(latencies_s)) * 1000
                            elapsed = time.perf_counter() - wall_start
                            wall_qps = completed / elapsed if elapsed > 0 else 0.0
                            print(
                                f"  Completed {completed}/{n}: recall={avg_recall:.3f}, "
                                f"avg_latency={avg_latency:.2f}ms, wall_QPS={wall_qps:,.0f}"
                            )

                wall_elapsed = time.perf_counter() - wall_start
                wall_qps = n / wall_elapsed if wall_elapsed > 0 else 0.0

            if empty_results == len(queries):
                raise RuntimeError(
                    "All Search responses were empty. This usually means the server has no indexed data "
                    "(common when using --skip-index against a fresh data dir). Re-run without --skip-index "
                    "or start the server with the data_dir that contains your index/snapshot."
                )

            avg_recall = float(np.mean(recalls)) if recalls else 0.0
            avg_latency = float(np.mean(latencies_s)) * 1000 if latencies_s else 0.0
            p50_latency = float(np.percentile(latencies_s, 50)) * 1000 if latencies_s else 0.0
            p99_latency = float(np.percentile(latencies_s, 99)) * 1000 if latencies_s else 0.0
            out: Dict[str, Any] = {
                "recall": avg_recall,
                "qps": float(wall_qps),
                "avg_latency_ms": avg_latency,
                "p50_latency_ms": p50_latency,
                "p99_latency_ms": p99_latency,
                "total_queries": len(queries),
            }
            if bulk_search:
                out["latency_note"] = "BulkSearch latency is response-gap approximation (streaming)."
            return out

        per_run: List[Dict[str, Any]] = []
        for r in range(repetitions):
            if repetitions > 1:
                print(f"\n--- Repetition {r+1}/{repetitions} ---")
            per_run.append(run_once())

        # Aggregate across runs (mean of per-run metrics).
        agg = {
            "recall": float(np.mean([x["recall"] for x in per_run])),
            "qps": float(np.mean([x["qps"] for x in per_run])),
            "avg_latency_ms": float(np.mean([x["avg_latency_ms"] for x in per_run])),
            "p50_latency_ms": float(np.mean([x["p50_latency_ms"] for x in per_run])),
            "p99_latency_ms": float(np.mean([x["p99_latency_ms"] for x in per_run])),
            "total_queries": len(queries),
            "per_run": per_run,
        }
        return agg


def main():
    parser = argparse.ArgumentParser(description="KyroDB Local Benchmark Runner")
    parser.add_argument("--dataset", type=str, default="sift-128-euclidean",
                        choices=list(DATASETS.keys()), help="Dataset to benchmark")
    parser.add_argument("--k", type=int, default=10, help="Number of neighbors")
    # Use an explicit IPv4 loopback default to avoid localhost->IPv6 issues on some VMs.
    parser.add_argument("--host", type=str, default="127.0.0.1", help="KyroDB host")
    parser.add_argument("--port", type=int, default=50051, help="KyroDB port")
    parser.add_argument(
        "--ef-search",
        type=int,
        default=0,
        help="HNSW ef_search override (0 uses server default)",
    )
    parser.add_argument("--max-queries", type=int, default=1000,
                        help="Maximum number of queries to run")
    parser.add_argument(
        "--warmup-queries",
        type=int,
        default=100,
        help="Warmup queries to run (not measured).",
    )
    parser.add_argument(
        "--repetitions",
        type=int,
        default=3,
        help="Number of repeated query runs to reduce noise.",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=1,
        help="Number of concurrent in-flight queries (threaded client).",
    )
    parser.add_argument(
        "--bulk-search",
        action="store_true",
        help="Use gRPC BulkSearch streaming API to reduce per-query overhead.",
    )
    parser.add_argument("--skip-index", action="store_true",
                        help="Skip indexing (assume data already loaded)")
    parser.add_argument("--output-dir", type=str, default="benchmarks/results",
                        help="Output directory for results")
    
    args = parser.parse_args()
    
    print("="*60)
    print(f"KyroDB Benchmark: {args.dataset}")
    print("="*60)
    
    # Download and load dataset
    data_dir = Path("benchmarks/data")
    filepath = download_dataset(args.dataset, data_dir)
    train, test, neighbors = load_dataset(filepath)
    
    # Limit queries if needed
    if args.max_queries < len(test):
        test = test[:args.max_queries]
        neighbors = neighbors[:args.max_queries]
    
    # Run benchmark
    dataset_metric = "angular" if args.dataset.endswith("-angular") else "euclidean"
    benchmark = KyroDBBenchmark(
        host=args.host,
        port=args.port,
        metric=dataset_metric,
        ef_search=args.ef_search,
    )

    # Best-effort: capture server-side config for traceability.
    server_config: Optional[Dict[str, Any]] = None
    try:
        benchmark.connect()
        resp = benchmark._stub.GetConfig(kyrodb_pb2.ConfigRequest())  # type: ignore[attr-defined]
        server_config = {
            "hot_tier_max_size": int(getattr(resp, "hot_tier_max_size", 0)),
            "hot_tier_max_age_seconds": int(getattr(resp, "hot_tier_max_age_seconds", 0)),
            "hnsw_max_elements": int(getattr(resp, "hnsw_max_elements", 0)),
            "data_dir": getattr(resp, "data_dir", None),
            "fsync_policy": getattr(resp, "fsync_policy", None),
            "snapshot_interval": int(getattr(resp, "snapshot_interval", 0)),
            "flush_interval_seconds": int(getattr(resp, "flush_interval_seconds", 0)),
            "embedding_dimension": int(getattr(resp, "embedding_dimension", 0)),
            "version": getattr(resp, "version", None),
        }
    except Exception:
        server_config = None
    
    if not args.skip_index:
        benchmark.index(train)
        # Note: index() now uses BulkLoadHnsw which bypasses hot tier entirely
        # No flush needed - data goes directly to HNSW
        
    results = benchmark.benchmark_queries(
        test,
        neighbors,
        args.k,
        concurrency=args.concurrency,
        bulk_search=args.bulk_search,
        warmup_queries=args.warmup_queries,
        repetitions=args.repetitions,
    )
    
    # Save results
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = time.strftime("%Y%m%d_%H%M%S", time.gmtime())
    output_file = output_dir / f"{args.dataset}_{timestamp}.json"

    output_data: Dict[str, Any] = {
        "dataset": args.dataset,
        "k": args.k,
        "n_train": int(len(train)),
        "n_queries": int(len(test)),
        "dimension": int(train.shape[1]),
        "timestamp": timestamp,
        "metadata": _collect_metadata(
            dataset_name=args.dataset,
            dataset_url=DATASETS[args.dataset],
            dataset_path=filepath,
            args=args,
        ),
        "server_config": server_config,
        "results": results,
    }
    
    with open(output_file, 'w') as f:
        json.dump(output_data, f, indent=2)
        
    print(f"\nResults saved to {output_file}")
    
    # Print summary
    print("\n" + "="*60)
    print(f"BENCHMARK RESULTS: {args.dataset} (k={args.k})")
    print("="*60)
    print(f"  Recall@{args.k}:     {results['recall']:.4f}")
    print(f"  QPS:            {results['qps']:,.0f}")
    print(f"  Avg Latency:    {results['avg_latency_ms']:.2f} ms")
    print(f"  P50 Latency:    {results['p50_latency_ms']:.2f} ms")
    print(f"  P99 Latency:    {results['p99_latency_ms']:.2f} ms")
    print("="*60)


if __name__ == "__main__":
    main()
