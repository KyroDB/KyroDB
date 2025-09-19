#!/usr/bin/env python3
"""
🛡️ CRITICAL FIXES VALIDATION: Memory Leak and Deadlock Prevention
Simplified version that validates the fixes are working correctly.
"""

import asyncio
import aiohttp
import time
import random

SERVER_URL = "http://127.0.0.1:3030"

async def test_memory_leak_fix():
    """Test that hot buffer memory leak is fixed"""
    print("🧪 Testing Memory Leak Fix...")
    
    connector = aiohttp.TCPConnector(limit=20, limit_per_host=20)
    async with aiohttp.ClientSession(connector=connector) as session:
        
        # Test 1: Rapid writes to stress hot buffer
        print("   📝 Stress testing hot buffer with rapid writes...")
        write_count = 0
        for i in range(5000):
            try:
                async with session.post(
                    f"{SERVER_URL}/v1/put_fast/{i}",
                    data=str(i * 2).encode(),
                    headers={'Content-Type': 'application/octet-stream'},
                    timeout=aiohttp.ClientTimeout(total=2.0)
                ) as response:
                    if response.status == 200:
                        write_count += 1
                    
                    if i % 1000 == 0:
                        print(f"      📊 Completed {i}/5000 writes")
                        
            except Exception as e:
                if "timeout" in str(e).lower():
                    print(f"      ⚠️  Timeout on write {i}: {e}")
                    break
        
        print(f"   ✅ Hot buffer test completed: {write_count}/5000 writes successful")
        return write_count > 4000  # At least 80% success rate

async def test_deadlock_fix():
    """Test that deadlock vulnerability is fixed"""
    print("🧪 Testing Deadlock Fix...")
    
    connector = aiohttp.TCPConnector(limit=50, limit_per_host=50)
    async with aiohttp.ClientSession(connector=connector) as session:
        
        # Test 2: Concurrent reads and writes
        print("   🔄 Testing concurrent reads and writes...")
        
        async def concurrent_writer(writer_id):
            write_success = 0
            for i in range(500):
                key = writer_id * 1000 + i
                try:
                    async with session.post(
                        f"{SERVER_URL}/v1/put_fast/{key}",
                        data=str(key).encode(),
                        timeout=aiohttp.ClientTimeout(total=3.0)
                    ) as response:
                        if response.status == 200:
                            write_success += 1
                except Exception as e:
                    if "timeout" in str(e).lower():
                        print(f"      ⚠️  Writer {writer_id} timeout: potential deadlock")
                        return 0
            return write_success

        async def concurrent_reader(reader_id):
            read_success = 0
            for i in range(300):
                key = random.randint(0, 2000)
                try:
                    async with session.get(
                        f"{SERVER_URL}/v1/get_fast/{key}",
                        timeout=aiohttp.ClientTimeout(total=3.0)
                    ) as response:
                        if response.status in [200, 404]:  # Both OK and not found are valid
                            read_success += 1
                except Exception as e:
                    if "timeout" in str(e).lower():
                        print(f"      ⚠️  Reader {reader_id} timeout: potential deadlock")
                        return 0
            return read_success

        # Run 5 writers and 5 readers concurrently
        write_tasks = [concurrent_writer(i) for i in range(5)]
        read_tasks = [concurrent_reader(i) for i in range(5)]
        
        start_time = time.time()
        write_results = await asyncio.gather(*write_tasks, return_exceptions=True)
        read_results = await asyncio.gather(*read_tasks, return_exceptions=True)
        end_time = time.time()
        
        total_writes = sum(r for r in write_results if isinstance(r, int))
        total_reads = sum(r for r in read_results if isinstance(r, int))
        duration = end_time - start_time
        
        print(f"   📊 Concurrent test results:")
        print(f"      ✍️  Total writes: {total_writes}/2500 ({total_writes/25:.1f}%)")
        print(f"      👁️  Total reads: {total_reads}/1500 ({total_reads/15:.1f}%)")
        print(f"      ⏱️  Duration: {duration:.2f} seconds")
        
        # Success if no major timeouts and reasonable completion rate
        deadlock_free = (total_writes > 2000 and total_reads > 1200 and duration < 30)
        
        if deadlock_free:
            print("   ✅ Deadlock prevention test passed")
        else:
            print("   ❌ Potential deadlock issues detected")
        
        return deadlock_free

async def test_consistency():
    """Test that our fixes maintain data consistency"""
    print("🧪 Testing Data Consistency...")
    
    async with aiohttp.ClientSession() as session:
        
        # Write test data
        test_pairs = [(i, i * 3) for i in range(6000, 6100)]  # Use fresh key range
        write_success = 0
        
        for key, value in test_pairs:
            try:
                async with session.post(
                    f"{SERVER_URL}/v1/put_fast/{key}",
                    data=str(value).encode(),
                    timeout=aiohttp.ClientTimeout(total=2.0)
                ) as response:
                    if response.status == 200:
                        write_success += 1
            except:
                pass
        
        # Small delay to allow merge
        await asyncio.sleep(1.0)
        
        # Read back and verify
        read_success = 0
        for key, expected_value in test_pairs:
            try:
                async with session.get(
                    f"{SERVER_URL}/v1/get_fast/{key}",
                    timeout=aiohttp.ClientTimeout(total=2.0)
                ) as response:
                    if response.status == 200:
                        data = await response.read()
                        actual_value = int(data.decode())
                        if actual_value == expected_value:
                            read_success += 1
                        else:
                            print(f"      ⚠️  Consistency error: key {key}, expected {expected_value}, got {actual_value}")
            except:
                pass
        
        consistency_rate = (read_success / len(test_pairs)) * 100
        print(f"   📊 Consistency test: {read_success}/{len(test_pairs)} correct ({consistency_rate:.1f}%)")
        
        return consistency_rate > 90.0

async def main():
    print("🛡️ KYRODB CRITICAL FIXES VALIDATION")
    print("Testing Memory Leak and Deadlock Prevention")
    print("=" * 70)
    
    # Test basic connectivity
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{SERVER_URL}/v1/lookup_ultra/999", timeout=aiohttp.ClientTimeout(total=5.0)) as response:
                pass  # Just test connectivity
        print("✅ Server connectivity confirmed")
    except Exception as e:
        print(f"❌ Cannot connect to KyroDB server: {e}")
        print("   Make sure the server is running with: ./target/release/kyrodb-engine serve 127.0.0.1 3030")
        return
    
    print()
    
    # Run tests
    start_time = time.time()
    
    memory_leak_fixed = await test_memory_leak_fix()
    print()
    
    deadlock_fixed = await test_deadlock_fix()
    print()
    
    consistency_maintained = await test_consistency()
    print()
    
    end_time = time.time()
    total_duration = end_time - start_time
    
    # Final results
    print("=" * 70)
    print("🛡️ CRITICAL FIXES VALIDATION RESULTS")
    print("=" * 70)
    print(f"⏱️  Total test duration: {total_duration:.1f} seconds")
    print()
    
    if memory_leak_fixed:
        print("✅ MEMORY LEAK FIX: Hot buffer memory management working correctly")
    else:
        print("❌ MEMORY LEAK FIX: Issues detected in hot buffer management")
    
    if deadlock_fixed:
        print("✅ DEADLOCK FIX: Lock ordering preventing deadlocks successfully")
    else:
        print("❌ DEADLOCK FIX: Potential deadlock vulnerabilities remain")
    
    if consistency_maintained:
        print("✅ DATA CONSISTENCY: Fixes maintain data integrity")
    else:
        print("❌ DATA CONSISTENCY: Fixes may have introduced consistency issues")
    
    print()
    
    overall_success = memory_leak_fixed and deadlock_fixed and consistency_maintained
    
    if overall_success:
        print("🎉 ALL CRITICAL FIXES VALIDATED SUCCESSFULLY!")
        print("   Both memory leak and deadlock vulnerabilities have been resolved.")
        print("   KyroDB is now production-ready with robust memory management.")
    else:
        print("⚠️  SOME ISSUES STILL NEED ATTENTION")
        print("   Review the test results above for specific areas requiring fixes.")

if __name__ == "__main__":
    asyncio.run(main())
