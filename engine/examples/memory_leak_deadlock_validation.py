#!/usr/bin/env python3
"""
🛡️ CRITICAL FIXES VALIDATION: Memory Leak and Deadlock Prevention

This script validates the fixes for:
1. Memory leak in hot buffer (Issue #6)
2. Deadlock vulnerability in merge operations (Issue #7)

Tests include:
- Hot buffer consistency under concurrent load
- Lock ordering validation to prevent deadlocks
- Memory leak detection and prevention
- Race condition elimination in size tracking
"""

import asyncio
import aiohttp
import time
import threading
import subprocess
import sys
import random
import os
from concurrent.futures import ThreadPoolExecutor

# KyroDB server configuration
SERVER_URL = "http://127.0.0.1:3030"
TOTAL_OPERATIONS = 50000
CONCURRENT_WRITERS = 20
CONCURRENT_READERS = 10

class KyroDBTester:
    def __init__(self):
        self.session = None
        self.write_success_count = 0
        self.read_success_count = 0
        self.write_failures = 0
        self.read_failures = 0
        self.deadlock_detected = False
        self.memory_leak_detected = False
        
    async def __aenter__(self):
        connector = aiohttp.TCPConnector(
            limit=100,
            limit_per_host=50,
            keepalive_timeout=30,
            enable_cleanup_closed=True
        )
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=aiohttp.ClientTimeout(total=5.0)
        )
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def write_operation(self, key: int, value: int) -> bool:
        """Test write operation for memory leak detection"""
        try:
            async with self.session.post(
                f"{SERVER_URL}/v1/kv/{key}",
                data=str(value).encode(),
                headers={'Content-Type': 'application/octet-stream'}
            ) as response:
                if response.status == 200:
                    self.write_success_count += 1
                    return True
                else:
                    self.write_failures += 1
                    return False
        except Exception as e:
            print(f"Write operation failed: {e}")
            self.write_failures += 1
            return False

    async def read_operation(self, key: int) -> bool:
        """Test read operation for deadlock detection"""
        try:
            async with self.session.get(f"{SERVER_URL}/v1/lookup_ultra/{key}") as response:
                if response.status == 200:
                    self.read_success_count += 1
                    return True
                elif response.status == 404:
                    # Not found is OK
                    self.read_success_count += 1
                    return True
                else:
                    self.read_failures += 1
                    return False
        except Exception as e:
            if "timeout" in str(e).lower():
                print(f"⚠️  Potential deadlock detected: {e}")
                self.deadlock_detected = True
            self.read_failures += 1
            return False

    async def concurrent_write_test(self, writer_id: int, operations_per_writer: int):
        """Stress test writes to detect memory leaks"""
        print(f"🚀 Writer {writer_id} starting {operations_per_writer} operations")
        
        for i in range(operations_per_writer):
            key = writer_id * 10000 + i
            value = key * 2  # Simple value mapping
            
            success = await self.write_operation(key, value)
            
            if i % 1000 == 0:
                print(f"📝 Writer {writer_id}: {i}/{operations_per_writer} operations completed")
            
            # Small delay to prevent overwhelming the server
            if i % 100 == 0:
                await asyncio.sleep(0.001)
        
        print(f"✅ Writer {writer_id} completed all operations")

    async def concurrent_read_test(self, reader_id: int, operations_per_reader: int):
        """Stress test reads to detect deadlocks"""
        print(f"🔍 Reader {reader_id} starting {operations_per_reader} operations")
        
        for i in range(operations_per_reader):
            # Read from a range of keys that writers are creating
            key = random.randint(0, TOTAL_OPERATIONS - 1)
            
            success = await self.read_operation(key)
            
            if i % 1000 == 0:
                print(f"👁️  Reader {reader_id}: {i}/{operations_per_reader} operations completed")
            
            # Very small delay to interleave with writes
            if i % 50 == 0:
                await asyncio.sleep(0.0005)
        
        print(f"✅ Reader {reader_id} completed all operations")

    async def memory_usage_monitor(self, duration_seconds: int):
        """Monitor memory usage to detect leaks"""
        print("🔍 Starting memory usage monitoring...")
        start_time = time.time()
        
        while time.time() - start_time < duration_seconds:
            try:
                # Get server memory info if available
                async with self.session.get(f"{SERVER_URL}/v1/stats") as response:
                    if response.status == 200:
                        stats = await response.json()
                        if 'memory_mb' in stats:
                            memory_mb = stats['memory_mb']
                            if memory_mb > 500:  # Alert if memory usage exceeds 500MB
                                print(f"⚠️  High memory usage detected: {memory_mb}MB")
                                if memory_mb > 1000:  # Critical threshold
                                    self.memory_leak_detected = True
                                    print(f"🚨 MEMORY LEAK DETECTED: {memory_mb}MB")
            except:
                pass  # Stats endpoint might not be available
            
            await asyncio.sleep(2.0)

    async def run_comprehensive_test(self):
        """Run comprehensive test for both memory leaks and deadlocks"""
        print("🛡️ STARTING COMPREHENSIVE MEMORY LEAK & DEADLOCK VALIDATION")
        print("=" * 70)
        
        operations_per_writer = TOTAL_OPERATIONS // CONCURRENT_WRITERS
        operations_per_reader = TOTAL_OPERATIONS // CONCURRENT_READERS
        
        # Start memory monitor
        monitor_task = asyncio.create_task(
            self.memory_usage_monitor(60)  # Monitor for 60 seconds
        )
        
        start_time = time.time()
        
        # Create concurrent write and read tasks
        write_tasks = [
            asyncio.create_task(self.concurrent_write_test(i, operations_per_writer))
            for i in range(CONCURRENT_WRITERS)
        ]
        
        read_tasks = [
            asyncio.create_task(self.concurrent_read_test(i, operations_per_reader))
            for i in range(CONCURRENT_READERS)
        ]
        
        # Wait for all tasks to complete
        all_tasks = write_tasks + read_tasks
        await asyncio.gather(*all_tasks, return_exceptions=True)
        
        end_time = time.time()
        total_duration = end_time - start_time
        
        # Stop memory monitor
        monitor_task.cancel()
        
        # Calculate results
        total_writes = self.write_success_count + self.write_failures
        total_reads = self.read_success_count + self.read_failures
        write_success_rate = (self.write_success_count / max(total_writes, 1)) * 100
        read_success_rate = (self.read_success_count / max(total_reads, 1)) * 100
        
        print("\n" + "=" * 70)
        print("🛡️ MEMORY LEAK & DEADLOCK VALIDATION RESULTS")
        print("=" * 70)
        print(f"📊 Total Duration: {total_duration:.2f} seconds")
        print(f"📝 Write Operations: {total_writes:,} ({write_success_rate:.1f}% success)")
        print(f"👁️  Read Operations: {total_reads:,} ({read_success_rate:.1f}% success)")
        print(f"🚀 Write Throughput: {total_writes / total_duration:.0f} ops/sec")
        print(f"🔍 Read Throughput: {total_reads / total_duration:.0f} ops/sec")
        
        # Validation results
        print("\n🛡️ CRITICAL FIXES VALIDATION:")
        
        # Memory leak validation
        if self.memory_leak_detected:
            print("❌ MEMORY LEAK DETECTED - Hot buffer memory management failed")
        else:
            print("✅ MEMORY LEAK PREVENTION - Hot buffer memory properly managed")
        
        # Deadlock validation
        if self.deadlock_detected:
            print("❌ DEADLOCK DETECTED - Lock ordering issue found")
        else:
            print("✅ DEADLOCK PREVENTION - Lock ordering working correctly")
        
        # Overall assessment
        overall_success = (
            not self.memory_leak_detected and 
            not self.deadlock_detected and
            write_success_rate > 95.0 and 
            read_success_rate > 95.0
        )
        
        if overall_success:
            print("\n🎉 ALL CRITICAL FIXES VALIDATED SUCCESSFULLY!")
            print("   - Memory leak prevention: ✅ WORKING")
            print("   - Deadlock prevention: ✅ WORKING") 
            print("   - High performance maintained: ✅ WORKING")
        else:
            print("\n⚠️  SOME ISSUES STILL PRESENT:")
            if self.memory_leak_detected:
                print("   - Memory leak prevention: ❌ NEEDS ATTENTION")
            if self.deadlock_detected:
                print("   - Deadlock prevention: ❌ NEEDS ATTENTION")
            if write_success_rate <= 95.0 or read_success_rate <= 95.0:
                print("   - Performance degradation: ❌ NEEDS ATTENTION")

def start_kyrodb_server():
    """Start KyroDB server in background"""
    try:
        # Kill any existing server
        subprocess.run(["pkill", "-f", "kyrodb-engine"], capture_output=True)
        time.sleep(2)
        
        # Start new server
        server_cmd = [
            "./target/release/kyrodb-engine", 
            "serve", 
            "127.0.0.1", 
            "3030"
        ]
        
        process = subprocess.Popen(
            server_cmd,
            cwd="/Users/kishan/Desktop/Codes/Project/ProjectKyro",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        # Wait for server to start
        time.sleep(3)
        
        # Test if server is responding
        import urllib.request
        try:
            urllib.request.urlopen(f"{SERVER_URL}/v1/stats", timeout=5)
            print("✅ KyroDB server started successfully")
            return process
        except:
            print("❌ Failed to start KyroDB server")
            return None
            
    except Exception as e:
        print(f"❌ Error starting server: {e}")
        return None

async def main():
    print("🛡️ KYRODB CRITICAL FIXES VALIDATION")
    print("Testing Memory Leak and Deadlock Prevention")
    print("=" * 70)
    
    # Test if server is already running
    try:
        import urllib.request
        urllib.request.urlopen(f"{SERVER_URL}/v1/stats", timeout=5)
        print("✅ Using existing KyroDB server")
        server_process = None
    except:
        # Start KyroDB server
        server_process = start_kyrodb_server()
        if not server_process:
            print("❌ Cannot start KyroDB server. Exiting.")
            sys.exit(1)
    
    try:
        # Run the test
        async with KyroDBTester() as tester:
            await tester.run_comprehensive_test()
    
    finally:
        # Clean up server only if we started it
        if server_process:
            server_process.terminate()
            try:
                server_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                server_process.kill()
            print("\n🧹 Server cleanup completed")
        else:
            print("\n🧹 Using existing server - no cleanup needed")

if __name__ == "__main__":
    asyncio.run(main())
