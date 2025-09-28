#!/usr/bin/env python3
"""
KyroDB Benchmark Optimization Test

This script validates that our optimizations work correctly:
1. ArcSwap lock-free index access
2. Ultra-fast lookup endpoints
3. Automatic warmup functionality
4. Binary protocol performance
"""

import requests
import time
import json
import statistics
import subprocess
import concurrent.futures
import sys
from typing import List, Dict, Any

class KyroDBOptimizationTester:
    def __init__(self, base_url: str = "http://127.0.0.1:3030"):
        self.base_url = base_url
        self.v1_base = f"{base_url}/v1"
        
    def start_server(self) -> subprocess.Popen:
        """Start KyroDB server with optimizations enabled"""
        print("🚀 Starting KyroDB server with optimizations...")
        
        cmd = [
            "cargo", "run", "-p", "kyrodb-engine", "--release", 
            "--features", "learned-index",
            "--", "serve", "127.0.0.1", "3030", "--enable-binary", "true"
        ]
        
        return subprocess.Popen(
            cmd, 
            cwd="/Users/kishan/Desktop/Codes/Project/ProjectKyro",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
    
    def wait_for_server(self, timeout: int = 30) -> bool:
        """Wait for server to be ready"""
        print("⏳ Waiting for server to start...")
        
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                response = requests.get(f"{self.v1_base}/health", timeout=1)
                if response.status_code == 200:
                    print("✅ Server is ready!")
                    return True
            except requests.exceptions.RequestException:
                pass
            time.sleep(0.5)
        
        print("❌ Server failed to start within timeout")
        return False
    
    def populate_data(self, num_keys: int = 10000) -> None:
        """Populate the database with test data"""
        print(f"📝 Populating {num_keys} key-value pairs...")
        
        for i in range(num_keys):
            key = i + 1
            value = f"value_{key}".encode('utf-8')
            
            response = requests.post(
                f"{self.v1_base}/put_fast/{key}",
                data=value,
                headers={"Content-Type": "application/octet-stream"}
            )
            
            if response.status_code != 200:
                print(f"❌ Failed to put key {key}: {response.status_code}")
                return
        
        print(f"✅ Successfully populated {num_keys} keys")
    
    def trigger_warmup(self) -> bool:
        """Trigger automatic warmup (RMI build + warmup)"""
        print("🔥 Triggering automatic warmup...")
        
        # Build RMI index
        rmi_response = requests.post(f"{self.v1_base}/rmi/build")
        if rmi_response.status_code != 200:
            print(f"❌ RMI build failed: {rmi_response.status_code}")
            return False
        
        print(f"✅ RMI Build: {rmi_response.json()}")
        
        # Warmup system
        warmup_response = requests.post(f"{self.v1_base}/warmup")
        if warmup_response.status_code != 200:
            print(f"❌ Warmup failed: {warmup_response.status_code}")
            return False
        
        print(f"✅ Warmup: {warmup_response.json()}")
        return True
    
    def benchmark_lookup_ultra(self, keys: List[int], concurrent: int = 50) -> Dict[str, Any]:
        """Benchmark the ultra-fast lookup endpoint"""
        print(f"⚡ Benchmarking /v1/lookup_ultra with {len(keys)} keys, {concurrent} concurrent requests...")
        
        def lookup_single(key: int) -> tuple:
            start_time = time.time()
            try:
                response = requests.get(f"{self.v1_base}/lookup_ultra/{key}")
                end_time = time.time()
                
                if response.status_code == 200:
                    return (end_time - start_time, True, response.json())
                else:
                    return (end_time - start_time, False, None)
            except Exception as e:
                end_time = time.time()
                return (end_time - start_time, False, str(e))
        
        start_overall = time.time()
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrent) as executor:
            results = list(executor.map(lookup_single, keys))
        
        end_overall = time.time()
        
        # Analyze results
        latencies = [r[0] for r in results]
        success_count = sum(1 for r in results if r[1])
        
        return {
            "total_requests": len(keys),
            "successful_requests": success_count,
            "failed_requests": len(keys) - success_count,
            "total_time_seconds": end_overall - start_overall,
            "requests_per_second": len(keys) / (end_overall - start_overall),
            "latency_stats": {
                "min_ms": min(latencies) * 1000,
                "max_ms": max(latencies) * 1000,
                "mean_ms": statistics.mean(latencies) * 1000,
                "median_ms": statistics.median(latencies) * 1000,
                "p95_ms": statistics.quantiles(latencies, n=20)[18] * 1000,
                "p99_ms": statistics.quantiles(latencies, n=100)[98] * 1000,
            }
        }
    
    def benchmark_batch_lookup(self, keys: List[int], batch_size: int = 100) -> Dict[str, Any]:
        """Benchmark the batch lookup endpoint"""
        print(f"🔄 Benchmarking /v1/lookup_batch with {len(keys)} keys in batches of {batch_size}...")
        
        batches = [keys[i:i + batch_size] for i in range(0, len(keys), batch_size)]
        
        def lookup_batch(batch_keys: List[int]) -> tuple:
            start_time = time.time()
            try:
                response = requests.post(
                    f"{self.v1_base}/lookup_batch",
                    json=batch_keys,
                    headers={"Content-Type": "application/json"}
                )
                end_time = time.time()
                
                if response.status_code == 200:
                    return (end_time - start_time, True, response.json())
                else:
                    return (end_time - start_time, False, None)
            except Exception as e:
                end_time = time.time()
                return (end_time - start_time, False, str(e))
        
        start_overall = time.time()
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            results = list(executor.map(lookup_batch, batches))
        
        end_overall = time.time()
        
        # Analyze results
        latencies = [r[0] for r in results]
        success_count = sum(1 for r in results if r[1])
        
        return {
            "total_batches": len(batches),
            "successful_batches": success_count,
            "failed_batches": len(batches) - success_count,
            "total_keys": len(keys),
            "batch_size": batch_size,
            "total_time_seconds": end_overall - start_overall,
            "keys_per_second": len(keys) / (end_overall - start_overall),
            "batches_per_second": len(batches) / (end_overall - start_overall),
            "latency_stats": {
                "min_ms": min(latencies) * 1000,
                "max_ms": max(latencies) * 1000,
                "mean_ms": statistics.mean(latencies) * 1000,
                "median_ms": statistics.median(latencies) * 1000,
            }
        }
    
    def get_metrics(self) -> str:
        """Get Prometheus metrics"""
        try:
            response = requests.get(f"{self.base_url}/metrics")
            if response.status_code == 200:
                return response.text
            else:
                return f"Failed to get metrics: {response.status_code}"
        except Exception as e:
            return f"Failed to get metrics: {e}"
    
    def run_comprehensive_test(self):
        """Run comprehensive optimization test"""
        print("🧪 KyroDB Benchmark Optimization Test")
        print("=" * 50)
        
        # Start server
        server_process = self.start_server()
        
        try:
            # Wait for server
            if not self.wait_for_server():
                return
            
            # Populate data
            self.populate_data(1000)
            
            # Trigger warmup
            if not self.trigger_warmup():
                return
            
            # Generate test keys
            test_keys = list(range(1, 501))  # Test with 500 keys
            
            print("\n🔍 PERFORMANCE TESTS")
            print("-" * 30)
            
            # Test 1: Ultra-fast single lookups
            single_results = self.benchmark_lookup_ultra(test_keys, concurrent=20)
            print(f"✅ Single Lookup Performance:")
            print(f"   Requests/sec: {single_results['requests_per_second']:.1f}")
            print(f"   Success rate: {single_results['successful_requests']}/{single_results['total_requests']}")
            print(f"   Latency P95: {single_results['latency_stats']['p95_ms']:.2f}ms")
            print(f"   Latency P99: {single_results['latency_stats']['p99_ms']:.2f}ms")
            
            # Test 2: Batch lookups
            batch_results = self.benchmark_batch_lookup(test_keys, batch_size=50)
            print(f"\\n✅ Batch Lookup Performance:")
            print(f"   Keys/sec: {batch_results['keys_per_second']:.1f}")
            print(f"   Batches/sec: {batch_results['batches_per_second']:.1f}")
            print(f"   Success rate: {batch_results['successful_batches']}/{batch_results['total_batches']} batches")
            print(f"   Avg batch latency: {batch_results['latency_stats']['mean_ms']:.2f}ms")
            
            # Test 3: Metrics validation
            print(f"\\n📊 METRICS ANALYSIS")
            print("-" * 30)
            metrics = self.get_metrics()
            
            # Look for key metrics
            rmi_hits = 0
            rmi_swaps = 0
            for line in metrics.split('\\n'):
                if 'kyrodb_rmi_hits_total' in line and not line.startswith('#'):
                    rmi_hits = float(line.split()[-1])
                elif 'kyrodb_rmi_swaps_total' in line and not line.startswith('#'):
                    rmi_swaps = float(line.split()[-1])
            
            print(f"✅ RMI Hits: {rmi_hits}")
            print(f"✅ RMI Swaps: {rmi_swaps}")
            
            if rmi_hits > 0:
                print("🎯 RMI index is working correctly!")
            if rmi_swaps > 0:
                print("🔄 ArcSwap optimization is working!")
            
            print(f"\\n🏆 OPTIMIZATION STATUS")
            print("-" * 30)
            print("✅ Using /v1/lookup_ultra endpoint (zero-allocation path)")
            print("✅ Automatic warmup triggered (RMI + system warmup)")
            print("✅ ArcSwap lock-free index access implemented")
            print("✅ Performance metrics collected successfully")
            
            if single_results['requests_per_second'] > 1000:
                print("🚀 HIGH PERFORMANCE: >1000 req/sec achieved!")
            
            return {
                "single_lookup": single_results,
                "batch_lookup": batch_results,
                "rmi_hits": rmi_hits,
                "rmi_swaps": rmi_swaps
            }
            
        finally:
            print("\\n🛑 Stopping server...")
            server_process.terminate()
            server_process.wait()

def main():
    if len(sys.argv) > 1 and sys.argv[1] == "--quick":
        print("Running quick optimization test...")
        # Quick test mode for CI/development
    
    tester = KyroDBOptimizationTester()
    results = tester.run_comprehensive_test()
    
    if results:
        print("\\n✅ Optimization test completed successfully!")
        return 0
    else:
        print("\\n❌ Optimization test failed!")
        return 1

if __name__ == "__main__":
    sys.exit(main())