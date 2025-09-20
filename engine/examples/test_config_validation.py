#!/usr/bin/env python3
"""
Test configuration validation for SIMD settings

This script tests the configuration validation logic to ensure
invalid SIMD parameters are properly rejected.
"""

import os
import subprocess
import tempfile
import json

def test_config_validation():
    """Test that invalid configurations are properly validated"""
    
    # Test cases with expected validation behavior
    test_cases = [
        {
            "name": "Valid 16-key SIMD",
            "env_vars": {
                "KYRODB_SIMD_WIDTH": "16",
                "KYRODB_SIMD_BATCH_SIZE": "16"
            },
            "should_pass": True
        },
        {
            "name": "Invalid non-power-of-2 batch size",
            "env_vars": {
                "KYRODB_SIMD_WIDTH": "16", 
                "KYRODB_SIMD_BATCH_SIZE": "12"  # Not power of 2
            },
            "should_pass": False
        },
        {
            "name": "Mismatched width and batch size",
            "env_vars": {
                "KYRODB_SIMD_WIDTH": "8",
                "KYRODB_SIMD_BATCH_SIZE": "16"  # Batch size > width
            },
            "should_pass": False
        },
        {
            "name": "Zero batch size",
            "env_vars": {
                "KYRODB_SIMD_WIDTH": "16",
                "KYRODB_SIMD_BATCH_SIZE": "0"
            },
            "should_pass": False
        }
    ]
    
    engine_path = "./target/release/kyrodb-engine"
    data_dir = tempfile.mkdtemp()
    
    print("Testing KyroDB Configuration Validation")
    print("=" * 50)
    
    results = []
    
    for test_case in test_cases:
        print(f"\nTest: {test_case['name']}")
        print(f"Environment: {test_case['env_vars']}")
        
        # Set up environment
        test_env = os.environ.copy()
        test_env.update(test_case['env_vars'])
        
        try:
            # Start server briefly to test configuration loading
            cmd = [engine_path, "-d", data_dir, "serve", "127.0.0.1", "0"]
            
            # Run with timeout to prevent hanging
            result = subprocess.run(
                cmd,
                env=test_env,
                capture_output=True,
                text=True,
                timeout=5  # 5 second timeout
            )
            
            print(f"Exit code: {result.returncode}")
            if result.stderr:
                print(f"Stderr: {result.stderr.strip()}")
            if result.stdout:
                print(f"Stdout: {result.stdout.strip()}")
                
            # Check if configuration loaded successfully
            config_loaded = "Optimized RMI configuration" in result.stderr or result.returncode == 0
            
            results.append({
                "test": test_case['name'],
                "expected_pass": test_case['should_pass'],
                "actual_pass": config_loaded,
                "exit_code": result.returncode,
                "stderr": result.stderr
            })
            
            status = "✓ PASS" if config_loaded == test_case['should_pass'] else "✗ FAIL"
            print(f"Result: {status}")
            
        except subprocess.TimeoutExpired:
            # Timeout is actually expected for successful server start
            print("Process started successfully (timeout expected)")
            results.append({
                "test": test_case['name'],
                "expected_pass": test_case['should_pass'],
                "actual_pass": True,  # Server started = config valid
                "exit_code": "timeout",
                "stderr": "Server started successfully"
            })
            
        except Exception as e:
            print(f"Error: {e}")
            results.append({
                "test": test_case['name'],
                "expected_pass": test_case['should_pass'],
                "actual_pass": False,
                "exit_code": "error",
                "stderr": str(e)
            })
    
    # Summary
    print("\n" + "=" * 50)
    print("VALIDATION TEST SUMMARY")
    print("=" * 50)
    
    passed = 0
    total = len(results)
    
    for result in results:
        status = "✓" if result['expected_pass'] == result['actual_pass'] else "✗"
        print(f"{status} {result['test']}")
        if result['expected_pass'] == result['actual_pass']:
            passed += 1
            
    print(f"\nResults: {passed}/{total} tests passed")
    
    # Cleanup
    import shutil
    shutil.rmtree(data_dir, ignore_errors=True)
    
    return passed == total

if __name__ == "__main__":
    success = test_config_validation()
    print(f"\nOverall result: {'SUCCESS' if success else 'FAILURE'}")
    exit(0 if success else 1)
