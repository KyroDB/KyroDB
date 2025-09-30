//! SIMD vectorization tests for AVX2, NEON, and runtime detection.
//!
//! Validates that:
//! 1. Runtime detection works on all architectures
//! 2. SIMD paths produce correct results
//! 3. Fallback to scalar works when SIMD unavailable

use crate::adaptive_rmi::AdaptiveRMI;
use std::sync::Arc;

/// Test runtime SIMD detection on current architecture
#[tokio::test]
async fn test_simd_runtime_detection() {
    // Create test data with known patterns
    let pairs: Vec<(u64, u64)> = (0..1000).map(|i| (i * 10, i * 100)).collect();
    let rmi = AdaptiveRMI::build_from_pairs(&pairs);
    
    // Test batch lookup with various sizes
    let test_cases = vec![
        vec![0, 10, 20, 30],          // Small batch (4 keys)
        (0..100).map(|i| i * 10).collect::<Vec<_>>(),  // Medium batch (100 keys)
        (0..1000).map(|i| i * 10).collect::<Vec<_>>(), // Large batch (1000 keys)
    ];
    
    for keys in test_cases {
        // Should not panic on any architecture (AVX2, NEON, or scalar)
        let results = rmi.lookup_batch_optimized_internal(&keys);
        
        assert_eq!(results.len(), keys.len(), "Result count should match key count");
        
        // Validate correctness: all keys should be found
        for (idx, result) in results.iter().enumerate() {
            assert!(
                result.is_some(),
                "Key {} at index {} should be found",
                keys[idx], idx
            );
        }
    }
}

/// Test SIMD correctness against scalar implementation
#[tokio::test]
async fn test_simd_matches_scalar() {
    let pairs: Vec<(u64, u64)> = (0..500).map(|i| (i * 2, i * 20)).collect();
    let rmi = AdaptiveRMI::build_from_pairs(&pairs);
    
    // Test keys (mix of existing and non-existing)
    let keys: Vec<u64> = vec![
        0, 2, 4, 100, 200, 998,  // Existing keys
        1, 3, 99, 999, 1001,     // Non-existing keys
    ];
    
    // Get SIMD results (will use runtime detection)
    let simd_results = rmi.lookup_batch_optimized_internal(&keys);
    
    // Get scalar results (individual lookups)
    let scalar_results: Vec<Option<u64>> = keys
        .iter()
        .map(|&k| rmi.lookup_key_ultra_fast(k))
        .collect();
    
    // Results must match exactly
    assert_eq!(
        simd_results, scalar_results,
        "SIMD and scalar results must be identical"
    );
}

/// Test large batch processing
#[tokio::test]
async fn test_simd_large_batch() {
    // Large dataset to ensure SIMD paths are taken
    let pairs: Vec<(u64, u64)> = (0..10000).map(|i| (i, i * 10)).collect();
    let rmi = AdaptiveRMI::build_from_pairs(&pairs);
    
    // Large batch of sequential keys
    let keys: Vec<u64> = (0..10000).step_by(10).collect();
    
    let results = rmi.lookup_batch_optimized_internal(&keys);
    
    assert_eq!(results.len(), keys.len());
    
    // All results should be found
    let found_count = results.iter().filter(|r| r.is_some()).count();
    assert_eq!(found_count, keys.len(), "All keys should be found");
}

/// Test edge cases (empty, single key, sparse keys)
#[tokio::test]
async fn test_simd_edge_cases() {
    let pairs: Vec<(u64, u64)> = (0..100).map(|i| (i * 100, i)).collect();
    let rmi = AdaptiveRMI::build_from_pairs(&pairs);
    
    // Edge case 1: Empty batch
    let empty_results = rmi.lookup_batch_optimized_internal(&[]);
    assert_eq!(empty_results.len(), 0);
    
    // Edge case 2: Single key
    let single_results = rmi.lookup_batch_optimized_internal(&[0]);
    assert_eq!(single_results.len(), 1);
    assert!(single_results[0].is_some());
    
    // Edge case 3: Duplicate keys
    let dup_results = rmi.lookup_batch_optimized_internal(&[0, 0, 100, 100]);
    assert_eq!(dup_results.len(), 4);
    assert!(dup_results.iter().all(|r| r.is_some()));
    
    // Edge case 4: Non-existing keys
    let missing_results = rmi.lookup_batch_optimized_internal(&[1, 3, 5, 7, 9]);
    assert_eq!(missing_results.len(), 5);
    assert!(missing_results.iter().all(|r| r.is_none()));
}

/// Test SIMD with concurrent access (thread safety)
#[tokio::test]
async fn test_simd_concurrent_access() {
    let pairs: Vec<(u64, u64)> = (0..1000).map(|i| (i, i * 2)).collect();
    let rmi = Arc::new(AdaptiveRMI::build_from_pairs(&pairs));
    
    let mut handles = vec![];
    
    // Spawn multiple threads doing batch lookups
    for thread_id in 0..4 {
        let rmi_clone = Arc::clone(&rmi);
        let handle = std::thread::spawn(move || {
            let keys: Vec<u64> = (thread_id * 250..(thread_id + 1) * 250).collect();
            let results = rmi_clone.lookup_batch_optimized_internal(&keys);
            
            // Verify all keys found
            assert_eq!(results.len(), keys.len());
            assert!(results.iter().all(|r| r.is_some()));
        });
        handles.push(handle);
    }
    
    // Wait for all threads
    for handle in handles {
        handle.join().expect("Thread panicked");
    }
}

/// Architecture-specific feature detection test
#[tokio::test]
async fn test_architecture_specific_paths() {
    let pairs: Vec<(u64, u64)> = (0..100).map(|i| (i, i * 10)).collect();
    let rmi = AdaptiveRMI::build_from_pairs(&pairs);
    
    let keys: Vec<u64> = (0..100).collect();
    let results = rmi.lookup_batch_optimized_internal(&keys);
    
    // Log which path was taken (for debugging)
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            eprintln!("✓ Using AVX2 SIMD path");
        } else {
            eprintln!("✓ Using scalar fallback on x86_64 (no AVX2)");
        }
    }
    
    #[cfg(target_arch = "aarch64")]
    {
        if std::arch::is_aarch64_feature_detected!("neon") {
            eprintln!("✓ Using NEON SIMD path");
        } else {
            eprintln!("✓ Using scalar fallback on aarch64 (no NEON)");
        }
    }
    
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        eprintln!("✓ Using scalar path on {:?}", std::env::consts::ARCH);
    }
    
    // Verify results regardless of path taken
    assert_eq!(results.len(), keys.len());
    assert!(results.iter().all(|r| r.is_some()));
}
