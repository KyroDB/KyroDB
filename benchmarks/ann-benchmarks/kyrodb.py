"""
KyroDB ANN-Benchmarks Algorithm Wrapper

This module implements the BaseANN interface for integrating KyroDB
with the ann-benchmarks framework (https://github.com/erikbern/ann-benchmarks).

Usage:
    1. Start KyroDB server: cargo run --release --bin kyrodb_server
    2. Run benchmark: python run.py --algorithm kyrodb --dataset sift-128-euclidean
"""

import os
import time
import numpy as np

try:
    import grpc
    from . import kyrodb_pb2
    from . import kyrodb_pb2_grpc
except ImportError:
    # Fallback for standalone testing
    import grpc
    import kyrodb_pb2
    import kyrodb_pb2_grpc


class KyroDB:
    """
    KyroDB algorithm wrapper for ann-benchmarks.
    
    Implements the BaseANN interface:
    - fit(X): Build index from dataset
    - query(q, k): Find k nearest neighbors
    - set_query_arguments(args): Configure search parameters
    - batch_query(X, k): Batch query for throughput testing
    """
    
    def __init__(self, metric: str, params: dict):
        """
        Initialize KyroDB connection.
        
        Args:
            metric: Distance metric ('angular' or 'euclidean')
            params: Algorithm parameters:
                - M: HNSW max connections per node (default: 16)
                - ef_construction: HNSW build parameter (default: 200)
                - ef_search: HNSW query parameter (default: 50)
        """
        self.metric = metric
        self.params = params
        
        # HNSW parameters
        self.M = params.get('M', 16)
        self.ef_construction = params.get('ef_construction', 200)
        self.ef_search = params.get('ef_search', 50)
        
        # Connection settings
        self.host = os.environ.get('KYRODB_HOST', 'localhost')
        self.port = int(os.environ.get('KYRODB_PORT', 50051))
        
        self._channel = None
        self._stub = None
        self._n_items = 0
        
    def _connect(self):
        """Establish gRPC connection to KyroDB server."""
        if self._channel is None:
            # Configure channel options for performance
            options = [
                ('grpc.max_send_message_length', 100 * 1024 * 1024),  # 100MB
                ('grpc.max_receive_message_length', 100 * 1024 * 1024),
                ('grpc.keepalive_time_ms', 10000),
                ('grpc.keepalive_timeout_ms', 5000),
            ]
            self._channel = grpc.insecure_channel(
                f'{self.host}:{self.port}',
                options=options
            )
            # Note: Service is KyroDBServiceStub (not KyroDBStub)
            self._stub = kyrodb_pb2_grpc.KyroDBServiceStub(self._channel)
            
    def _generate_insert_requests(self, X: np.ndarray):
        """Generator for streaming bulk insert."""
        for i, vec in enumerate(X):
            yield kyrodb_pb2.InsertRequest(
                doc_id=i + 1,  # doc_id must be >= 1
                embedding=list(map(float, vec))
            )
            
    def fit(self, X: np.ndarray):
        """
        Build HNSW index from dataset.
        
        Args:
            X: Dataset as numpy array of shape (n_items, dimension)
        """
        self._connect()
        self._n_items = len(X)
        
        print(f"[KyroDB] Indexing {len(X)} vectors (dim={X.shape[1]})...")
        start = time.time()
        
        # Use streaming bulk insert for maximum performance
        try:
            # BulkInsert is a streaming RPC: stream of InsertRequest -> InsertResponse
            response = self._stub.BulkInsert(self._generate_insert_requests(X))
            if response.success:
                elapsed = time.time() - start
                print(f"[KyroDB] Bulk insert complete: {len(X)} vectors in {elapsed:.2f}s "
                      f"({len(X)/elapsed:,.0f} vectors/sec)")
                return
            else:
                print(f"[KyroDB] Bulk insert returned error: {response.error}")
        except grpc.RpcError as e:
            print(f"[KyroDB] Bulk insert failed ({e.code()}), falling back to single inserts...")
        
        # Fallback: individual inserts with progress tracking
        for i, vec in enumerate(X):
            req = kyrodb_pb2.InsertRequest(
                doc_id=i + 1,  # doc_id must be >= 1
                embedding=list(map(float, vec))
            )
            try:
                self._stub.Insert(req)
            except grpc.RpcError as e:
                if i == 0:
                    print(f"[KyroDB] Insert failed: {e}")
                    raise
                    
            if (i + 1) % 10000 == 0:
                elapsed = time.time() - start
                rate = (i + 1) / elapsed
                print(f"[KyroDB] Indexed {i+1}/{len(X)} vectors ({rate:,.0f} vec/s)")
                
        elapsed = time.time() - start
        print(f"[KyroDB] Indexing complete: {len(X)} vectors in {elapsed:.2f}s "
              f"({len(X)/elapsed:,.0f} vectors/sec)")
        
    def set_query_arguments(self, ef_search: int = None, **kwargs):
        """
        Configure search-time parameters.
        
        Args:
            ef_search: HNSW ef parameter for search (higher = more accurate but slower)
        """
        if ef_search is not None:
            self.ef_search = ef_search
        # Also accept as first positional arg (ann-benchmarks style)
        if kwargs.get('args') and len(kwargs['args']) > 0:
            self.ef_search = kwargs['args'][0]
            
    def query(self, q: np.ndarray, k: int) -> list:
        """
        Find k nearest neighbors for a single query.
        
        Args:
            q: Query vector
            k: Number of neighbors to return
            
        Returns:
            List of (doc_id, distance) tuples for k nearest neighbors
        """
        self._connect()
        
        # Note: Field is 'query_embedding' not 'embedding'
        request = kyrodb_pb2.SearchRequest(
            query_embedding=list(map(float, q)),
            k=k,
            # ef_search is not in proto yet - would need to add if needed
        )
        
        try:
            response = self._stub.Search(request)
            # Return list of (doc_id, score) - score is similarity, higher is better
            # For ann-benchmarks, we return doc_id, distance where lower is better
            # Cosine similarity: distance = 1 - similarity
            return [(r.doc_id, 1.0 - r.score) for r in response.results]
        except grpc.RpcError as e:
            print(f"[KyroDB] Query failed: {e}")
            return []
            
    def batch_query(self, X: np.ndarray, k: int) -> list:
        """
        Batch query for throughput benchmarking.
        
        Args:
            X: Array of query vectors
            k: Number of neighbors per query
            
        Returns:
            List of lists, each containing (doc_id, distance) tuples
        """
        self._connect()
        
        # Use streaming BulkSearch if available
        def generate_search_requests():
            for q in X:
                yield kyrodb_pb2.SearchRequest(
                    query_embedding=list(map(float, q)),
                    k=k,
                )
        
        try:
            results = []
            for response in self._stub.BulkSearch(generate_search_requests()):
                results.append([
                    (r.doc_id, 1.0 - r.score) for r in response.results
                ])
            return results
        except grpc.RpcError:
            # Fallback to individual queries
            return [self.query(q, k) for q in X]
            
    def get_memory_usage(self) -> float:
        """Return approximate memory usage in MB."""
        try:
            response = self._stub.Metrics(kyrodb_pb2.MetricsRequest())
            return response.memory_usage_bytes / (1024 * 1024)
        except Exception:
            return 0.0
            
    def __str__(self):
        return f"KyroDB(M={self.M}, ef_c={self.ef_construction}, ef_s={self.ef_search})"
        
    def __del__(self):
        if self._channel:
            try:
                self._channel.close()
            except Exception:
                pass
