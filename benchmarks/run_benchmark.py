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
from pathlib import Path
from typing import List, Tuple

import numpy as np

# Add path for local imports
sys.path.insert(0, str(Path(__file__).parent / "ann-benchmarks"))

try:
    import h5py
except ImportError:
    print("Installing h5py...")
    os.system("pip install h5py")
    import h5py

try:
    import grpc
except ImportError:
    print("Installing grpcio...")
    os.system("pip install grpcio")
    import grpc

# Import generated stubs
ann_bench_dir = Path(__file__).parent / "ann-benchmarks"
sys.path.insert(0, str(ann_bench_dir))
import kyrodb_pb2
import kyrodb_pb2_grpc


# Dataset URLs (from ann-benchmarks)
DATASETS = {
    "sift-128-euclidean": "http://ann-benchmarks.com/sift-128-euclidean.hdf5",
    "glove-100-angular": "http://ann-benchmarks.com/glove-100-angular.hdf5",
    "gist-960-euclidean": "http://ann-benchmarks.com/gist-960-euclidean.hdf5",
    "mnist-784-euclidean": "http://ann-benchmarks.com/mnist-784-euclidean.hdf5",
    "fashion-mnist-784-euclidean": "http://ann-benchmarks.com/fashion-mnist-784-euclidean.hdf5",
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
    urllib.request.urlretrieve(url, filepath)
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
    """Compute recall@k."""
    if len(results) == 0:
        return 0.0
    gt_set = set(int(x) for x in ground_truth[:k])
    result_set = set(int(r[0]) for r in results[:k])
    return len(gt_set & result_set) / k


class KyroDBBenchmark:
    """KyroDB benchmark runner."""
    
    def __init__(self, host: str = "localhost", port: int = 50051):
        self.host = host
        self.port = port
        self._channel = None
        self._stub = None
        
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
                print(f"✓ Connected to KyroDB at {self.host}:{self.port}")
            except grpc.RpcError as e:
                print(f"✗ Failed to connect to KyroDB at {self.host}:{self.port}")
                print(f"  Error: {e.code()} - {e.details()}")
                print(f"\nMake sure KyroDB server is running:")
                print(f"  cargo run --release --bin kyrodb_server")
                sys.exit(1)
    
    def _generate_insert_requests(self, vectors: np.ndarray):
        """Generator for streaming bulk insert."""
        for i, vec in enumerate(vectors):
            yield kyrodb_pb2.InsertRequest(
                doc_id=i,
                embedding=list(map(float, vec))
            )
            if (i + 1) % 10000 == 0:
                print(f"  Streaming {i+1}/{len(vectors)} vectors...")
                
    def index(self, vectors: np.ndarray):
        """Index vectors into KyroDB."""
        self.connect()
        n_vectors = len(vectors)
        
        print(f"Indexing {n_vectors:,} vectors (dim={vectors.shape[1]})...")
        start = time.time()
        
        # Try streaming bulk insert first
        try:
            response = self._stub.BulkInsert(self._generate_insert_requests(vectors))
            if response.success:
                elapsed = time.time() - start
                print(f"✓ Indexed {n_vectors:,} vectors in {elapsed:.1f}s ({n_vectors/elapsed:,.0f} vec/s)")
                return
        except grpc.RpcError as e:
            print(f"  Bulk insert not available ({e.code()}), using individual inserts...")
        
        # Fallback to individual inserts
        for i, vec in enumerate(vectors):
            req = kyrodb_pb2.InsertRequest(
                doc_id=i,
                embedding=list(map(float, vec))
            )
            self._stub.Insert(req)
                    
            if (i + 1) % 50000 == 0:
                elapsed = time.time() - start
                rate = (i + 1) / elapsed
                print(f"  {i+1:,}/{n_vectors:,} ({rate:,.0f} vec/s)")
                
        elapsed = time.time() - start
        print(f"✓ Indexed {n_vectors:,} vectors in {elapsed:.1f}s ({n_vectors/elapsed:,.0f} vec/s)")
        
    def query(self, vector: np.ndarray, k: int) -> List[Tuple[int, float]]:
        """Query for k nearest neighbors."""
        request = kyrodb_pb2.SearchRequest(
            query_embedding=list(map(float, vector)),
            k=k,
        )
        
        response = self._stub.Search(request)
        # Convert similarity score to distance (lower is better)
        return [(r.doc_id, 1.0 - r.score) for r in response.results]
        
    def benchmark_queries(
        self,
        queries: np.ndarray,
        ground_truth: np.ndarray,
        k: int,
    ) -> dict:
        """Run query benchmarks."""
        self.connect()
        
        print(f"\nRunning {len(queries)} queries (k={k})...")
        
        recalls = []
        latencies = []
        
        for i, (query, gt) in enumerate(zip(queries, ground_truth)):
            start = time.time()
            result = self.query(query, k)
            latency = time.time() - start
            
            recall = compute_recall(result, gt, k)
            recalls.append(recall)
            latencies.append(latency)
            
            if (i + 1) % 500 == 0:
                avg_recall = np.mean(recalls)
                avg_latency = np.mean(latencies) * 1000
                qps = (i + 1) / sum(latencies)
                print(f"  Query {i+1}/{len(queries)}: recall={avg_recall:.3f}, "
                      f"latency={avg_latency:.2f}ms, QPS={qps:,.0f}")
        
        avg_recall = np.mean(recalls)
        avg_latency = np.mean(latencies) * 1000
        p50_latency = np.percentile(latencies, 50) * 1000
        p99_latency = np.percentile(latencies, 99) * 1000
        qps = 1.0 / np.mean(latencies)
        
        return {
            "recall": avg_recall,
            "qps": qps,
            "avg_latency_ms": avg_latency,
            "p50_latency_ms": p50_latency,
            "p99_latency_ms": p99_latency,
            "total_queries": len(queries),
        }


def main():
    parser = argparse.ArgumentParser(description="KyroDB Local Benchmark Runner")
    parser.add_argument("--dataset", type=str, default="sift-128-euclidean",
                        choices=list(DATASETS.keys()), help="Dataset to benchmark")
    parser.add_argument("--k", type=int, default=10, help="Number of neighbors")
    parser.add_argument("--host", type=str, default="localhost", help="KyroDB host")
    parser.add_argument("--port", type=int, default=50051, help="KyroDB port")
    parser.add_argument("--max-queries", type=int, default=1000,
                        help="Maximum number of queries to run")
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
    benchmark = KyroDBBenchmark(host=args.host, port=args.port)
    
    if not args.skip_index:
        benchmark.index(train)
        
    results = benchmark.benchmark_queries(test, neighbors, args.k)
    
    # Save results
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    output_file = output_dir / f"{args.dataset}_{timestamp}.json"
    
    output_data = {
        "dataset": args.dataset,
        "k": args.k,
        "n_train": len(train),
        "n_queries": len(test),
        "dimension": train.shape[1],
        "timestamp": timestamp,
        "results": results,
    }
    
    with open(output_file, 'w') as f:
        json.dump(output_data, f, indent=2)
        
    print(f"\n✓ Results saved to {output_file}")
    
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
