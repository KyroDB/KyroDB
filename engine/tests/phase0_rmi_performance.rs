// Phase 0 Test: Verify bounded RMI performance fixes
use tempfile::NamedTempFile;

#[cfg(test)]
mod phase0_rmi_tests {
    use super::*;
    use kyrodb_engine::index::RmiIndex;
    use std::time::Instant;

    fn build_rmi_from_pairs(pairs: &[(u64, u64)]) -> std::io::Result<RmiIndex> {
        let temp_file = NamedTempFile::new()?;
        RmiIndex::write_from_pairs(temp_file.path(), pairs)?;
        Ok(RmiIndex::load_from_file(temp_file.path()).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "Failed to load RMI")
        })?)
    }

    #[test]
    fn test_bounded_rmi_guarantees() {
        // Create test data that would trigger O(n) behavior in the old implementation
        let pairs: Vec<(u64, u64)> = (0..10000).map(|i| (i * 1000000, i)).collect(); // Large gaps
        
        let rmi = build_rmi_from_pairs(&pairs)
            .expect("Failed to build RMI");
        
        // Test with various keys to ensure no O(n) behavior
        let test_keys = vec![
            pairs[0].0,           // First key
            pairs[5000].0,        // Middle key
            pairs[9999].0,        // Last key
            1000,                 // Key not in index (low)
            5000000000,          // Key not in index (high)
            pairs[1000].0 + 1,   // Key close to existing
        ];
        
        for &test_key in &test_keys {
            let start = Instant::now();
            let _result = rmi.predict_get(&test_key);
            let duration = start.elapsed();
            
            // With our fixes, no lookup should take more than 100 microseconds
            assert!(
                duration.as_micros() < 100,
                "Lookup for key {} took {:?}, expected < 100μs",
                test_key,
                duration
            );
        }
        
        println!("✅ All lookups completed within performance bounds");
    }
    
    #[test]
    fn test_pathological_key_distribution() {
        // Test with keys that would break epsilon calculations
        let pairs: Vec<(u64, u64)> = vec![
            (0, 0), (1, 1), (2, 2), (3, 3), (4, 4),                           // Dense cluster
            (1000000, 5), (1000001, 6), (1000002, 7),                         // Another dense cluster
            (u64::MAX - 5, 8), (u64::MAX - 4, 9), (u64::MAX - 3, 10), 
            (u64::MAX - 2, 11), (u64::MAX - 1, 12),                           // End cluster
        ];
        
        let rmi = build_rmi_from_pairs(&pairs)
            .expect("Failed to build RMI with pathological distribution");
        
        // Test that all lookups are still bounded
        for &(key, _) in &pairs {
            let start = Instant::now();
            let result = rmi.predict_get(&key);
            let duration = start.elapsed();
            
            assert!(result.is_some(), "Should find key {}", key);
            assert!(
                duration.as_micros() < 50,
                "Pathological lookup for key {} took {:?}",
                key,
                duration
            );
        }
        
        println!("✅ Pathological key distribution handled correctly");
    }
    
    #[test]
    fn test_empty_and_edge_cases() {
        let rmi = RmiIndex::new();
        
        // Test empty index
        assert!(rmi.predict_get(&42).is_none());
        
        // Test single key
        let pairs = vec![(100, 1)];
        let rmi = build_rmi_from_pairs(&pairs)
            .expect("Failed to build single-key RMI");
        
        assert_eq!(rmi.predict_get(&100), Some(1));
        assert!(rmi.predict_get(&99).is_none());
        assert!(rmi.predict_get(&101).is_none());
        
        println!("✅ Edge cases handled correctly");
    }
    
    #[test]
    fn test_bounds_checking() {
        // Create an RMI and verify bounds checking prevents crashes
        let pairs: Vec<(u64, u64)> = (0..1000).map(|i| (i, i)).collect();
        
        let rmi = build_rmi_from_pairs(&pairs)
            .expect("Failed to build RMI");
        
        // Test various edge cases that could cause index out of bounds
        let test_cases = vec![
            0u64,
            999u64,
            1000u64,     // Just past end
            u64::MAX,    // Maximum value
            500u64,      // Middle value
        ];
        
        for &key in &test_cases {
            // Should never panic, even with out-of-bounds keys
            let _result = rmi.predict_get(&key);
        }
        
        println!("✅ Bounds checking prevents crashes");
    }
    
    #[test]
    fn test_large_dataset_performance() {
        // Test with a larger dataset to ensure performance remains bounded
        let size = 100000;
        let pairs: Vec<(u64, u64)> = (0..size).map(|i| (i * 1000, i)).collect();
        
        let rmi = build_rmi_from_pairs(&pairs)
            .expect("Failed to build large RMI");
        
        // Sample lookups across the range
        let sample_keys: Vec<u64> = (0..100)
            .map(|i| pairs[i * (size as usize / 100)].0)
            .collect();
        
        let start = Instant::now();
        for &key in &sample_keys {
            let _result = rmi.predict_get(&key);
        }
        let total_duration = start.elapsed();
        
        let avg_duration = total_duration / sample_keys.len() as u32;
        
        // Average lookup should be very fast
        assert!(
            avg_duration.as_micros() < 10,
            "Average lookup time {:?} exceeds 10μs threshold",
            avg_duration
        );
        
        println!(
            "✅ Large dataset performance: {} lookups in {:?} (avg: {:?})",
            sample_keys.len(),
            total_duration,
            avg_duration
        );
    }
}
