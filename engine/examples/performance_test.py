#!/usr/bin/env python3
"""
Performance test to validate that the excessive yielding regression has been fixed.
This test stresses the background maintenance while measuring HTTP response times.
"""

import asyncio
import aiohttp
import time
import statistics
import json
from concurrent.futures import ThreadPoolExecutor

async def measure_http_latency(session, url, data, iterations=100):
    """Measure HTTP latency under load"""
    latencies = []
    
    for i in range(iterations):
        start = time.time()
        try:
            async with session.post(url, json=data) as response:
                await response.read()
                latency = (time.time() - start) * 1000  # Convert to ms
                latencies.append(latency)
        except Exception as e:
            print(f"Request {i} failed: {e}")
            latencies.append(float('inf'))
        
        # Small delay to simulate realistic traffic
        await asyncio.sleep(0.01)
    
    return latencies

async def stress_background_operations(session, base_url, operations=1000):
    """Create load that triggers background maintenance"""
    tasks = []
    
    # Insert many keys to trigger RMI background operations
    for i in range(operations):
        data = {"key": i, "value": f"stress_test_value_{i}"}
        task = session.post(f"{base_url}/v1/put", json=data)
        tasks.append(task)
        
        # Add some variety to trigger different background operations
        if i % 100 == 0:
            lookup_task = session.get(f"{base_url}/v1/get/{i-50}")
            tasks.append(lookup_task)
    
    # Execute all operations concurrently
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Clean up responses
    for result in results:
        if hasattr(result, 'close'):
            result.close()

async def main():
    print("🚀 KyroDB Performance Regression Test")
    print("Testing that excessive yielding fix restored HTTP throughput")
    print("="*60)
    
    base_url = "http://127.0.0.1:3030"
    
    async with aiohttp.ClientSession() as session:
        # Phase 1: Baseline measurement (low load)
        print("📊 Phase 1: Baseline HTTP latency measurement...")
        baseline_data = {"key": 999999, "value": "baseline_test"}
        baseline_latencies = await measure_http_latency(session, f"{base_url}/v1/put", baseline_data, 50)
        
        baseline_mean = statistics.mean([l for l in baseline_latencies if l != float('inf')])
        baseline_p95 = statistics.quantiles([l for l in baseline_latencies if l != float('inf')], n=20)[18]
        
        print(f"   Baseline Mean Latency: {baseline_mean:.2f}ms")
        print(f"   Baseline P95 Latency:  {baseline_p95:.2f}ms")
        
        # Phase 2: Stress test with background maintenance
        print("\n🔥 Phase 2: Stressing background maintenance...")
        stress_task = asyncio.create_task(stress_background_operations(session, base_url, 500))
        
        # Wait a bit for background operations to kick in
        await asyncio.sleep(2)
        
        # Measure HTTP latency during stress
        print("📊 Phase 3: HTTP latency under background maintenance stress...")
        stress_data = {"key": 888888, "value": "stress_measurement"}
        stress_latencies = await measure_http_latency(session, f"{base_url}/v1/put", stress_data, 100)
        
        # Wait for stress operations to complete
        await stress_task
        
        stress_mean = statistics.mean([l for l in stress_latencies if l != float('inf')])
        stress_p95 = statistics.quantiles([l for l in stress_latencies if l != float('inf')], n=20)[18]
        
        print(f"   Stress Mean Latency: {stress_mean:.2f}ms")
        print(f"   Stress P95 Latency:  {stress_p95:.2f}ms")
        
        # Phase 3: Recovery measurement
        print("\n🔄 Phase 4: Recovery latency measurement...")
        await asyncio.sleep(3)  # Let system recover
        
        recovery_data = {"key": 777777, "value": "recovery_test"}
        recovery_latencies = await measure_http_latency(session, f"{base_url}/v1/put", recovery_data, 50)
        
        recovery_mean = statistics.mean([l for l in recovery_latencies if l != float('inf')])
        recovery_p95 = statistics.quantiles([l for l in recovery_latencies if l != float('inf')], n=20)[18]
        
        print(f"   Recovery Mean Latency: {recovery_mean:.2f}ms")
        print(f"   Recovery P95 Latency:  {recovery_p95:.2f}ms")
        
        # Analysis
        print("\n" + "="*60)
        print("📈 PERFORMANCE ANALYSIS")
        print("="*60)
        
        stress_degradation = ((stress_mean - baseline_mean) / baseline_mean) * 100
        recovery_impact = ((recovery_mean - baseline_mean) / baseline_mean) * 100
        
        print(f"Latency increase under stress: {stress_degradation:.1f}%")
        print(f"Recovery latency impact:      {recovery_impact:.1f}%")
        
        # Success criteria (the original issue caused 60% throughput loss)
        if stress_degradation < 100:  # Less than 100% latency increase = good
            print("\n✅ SUCCESS: HTTP performance maintained under background load")
            print("   The excessive yielding regression has been FIXED!")
        else:
            print("\n❌ ISSUE: HTTP performance severely degraded under background load")
            print("   Background maintenance may still be monopolizing CPU resources")
        
        if recovery_impact < 20:  # Recovery should be quick
            print("✅ SUCCESS: System recovers quickly from background stress")
        else:
            print("⚠️  WARNING: System takes time to recover from background stress")

if __name__ == "__main__":
    asyncio.run(main())
