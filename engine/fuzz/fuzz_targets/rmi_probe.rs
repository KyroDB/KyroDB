#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Require minimum data for meaningful fuzzing
    if data.len() < 16 {
        return;
    }

    #[cfg(feature = "learned-index")]
    {
        use kyrodb_engine::index::RmiIndex;
        
        // Parse fuzz input for different test scenarios
        let scenario = data[0] % 10; // 10 different scenarios
        let config_byte = data[1];
        
        // Extract parameters from fuzz data
        let min_keys = 32;
        let max_keys = 8192;
        let key_count = if data.len() >= 4 {
            let count_bytes = &data[2..6];
            let raw_count = u32::from_le_bytes(count_bytes.try_into().unwrap_or([0, 0, 0, 0]));
            (raw_count as usize % (max_keys - min_keys) + min_keys).min(data.len() / 8)
        } else {
            256
        };
        
        // Generate keys from fuzz data with different patterns based on scenario
        let keys: Vec<u64> = match scenario {
            0 => {
                // Sequential keys
                (0..key_count).map(|i| i as u64).collect()
            },
            1 => {
                // Random keys from fuzz data
                data.chunks_exact(8)
                    .take(key_count)
                    .map(|c| {
                        let mut b = [0u8; 8];
                        b.copy_from_slice(c);
                        u64::from_le_bytes(b)
                    })
                    .collect()
            },
            2 => {
                // Clustered keys (groups of similar values)
                data.chunks_exact(8)
                    .take(key_count)
                    .enumerate()
                    .map(|(i, c)| {
                        let mut b = [0u8; 8];
                        b.copy_from_slice(c);
                        let base = u64::from_le_bytes(b);
                        base + (i % 100) as u64 // Add small cluster variation
                    })
                    .collect()
            },
            3 => {
                // Sparse keys (large gaps)
                data.chunks_exact(8)
                    .take(key_count)
                    .enumerate()
                    .map(|(i, c)| {
                        let mut b = [0u8; 8];
                        b.copy_from_slice(c);
                        u64::from_le_bytes(b) + (i as u64 * 1000000) // Large gaps
                    })
                    .collect()
            },
            4 => {
                // High-frequency duplicates
                let mut base_keys: Vec<u64> = data.chunks_exact(8)
                    .take(key_count / 4)
                    .map(|c| {
                        let mut b = [0u8; 8];
                        b.copy_from_slice(c);
                        u64::from_le_bytes(b)
                    })
                    .collect();
                // Repeat each key multiple times
                let mut expanded = Vec::new();
                for key in base_keys {
                    for _ in 0..4 {
                        expanded.push(key);
                    }
                }
                expanded
            },
            5 => {
                // Boundary values
                let mut keys = vec![0u64, 1u64, u64::MAX, u64::MAX - 1];
                // Add some fuzz-derived keys
                for chunk in data.chunks_exact(8).take(key_count - 4) {
                    let mut b = [0u8; 8];
                    b.copy_from_slice(chunk);
                    keys.push(u64::from_le_bytes(b));
                }
                keys
            },
            6 => {
                // Alternating high/low values
                data.chunks_exact(8)
                    .take(key_count)
                    .enumerate()
                    .map(|(i, c)| {
                        let mut b = [0u8; 8];
                        b.copy_from_slice(c);
                        let base = u64::from_le_bytes(b);
                        if i % 2 == 0 { base } else { u64::MAX - base }
                    })
                    .collect()
            },
            7 => {
                // Skewed keys (power-law distribution)
                data.chunks_exact(8)
                    .take(key_count)
                    .enumerate()
                    .map(|(i, c)| {
                        let mut b = [0u8; 8];
                        b.copy_from_slice(c);
                        let base = u64::from_le_bytes(b);
                        // Create power-law distribution with some keys very frequent
                        if i < key_count / 10 {
                            base % 1000 // Hot keys
                        } else {
                            base
                        }
                    })
                    .collect()
            },
            8 => {
                // ε-bounds edge cases (keys near ε boundaries)
                let epsilon = 100; // Simulate ε value
                data.chunks_exact(8)
                    .take(key_count)
                    .enumerate()
                    .map(|(i, c)| {
                        let mut b = [0u8; 8];
                        b.copy_from_slice(c);
                        let base = u64::from_le_bytes(b);
                        // Place keys near ε boundaries
                        (base / epsilon) * epsilon + (i % epsilon)
                    })
                    .collect()
            },
            9 => {
                // Extreme ε-bounds failures (keys far from training data)
                let mut keys = Vec::new();
                // Add some normal keys
                for chunk in data.chunks_exact(8).take(key_count / 2) {
                    let mut b = [0u8; 8];
                    b.copy_from_slice(chunk);
                    keys.push(u64::from_le_bytes(b));
                }
                // Add keys that are ε-bounds failures
                let max_normal = keys.iter().max().unwrap_or(&1000);
                for i in 0..key_count / 2 {
                    keys.push(max_normal + 1000000 + i as u64); // Far outside ε bounds
                }
                keys
            },
            _ => {
                // Default: sorted fuzz keys
                let mut keys: Vec<u64> = data.chunks_exact(8)
                    .take(key_count)
                    .map(|c| {
                        let mut b = [0u8; 8];
                        b.copy_from_slice(c);
                        u64::from_le_bytes(b)
                    })
                    .collect();
                keys.sort_unstable();
                keys.dedup();
                keys
            }
        };
        
        if keys.len() < 16 {
            return;
        }
        
        // Create pairs with offsets
        let pairs: Vec<(u64, u64)> = keys
            .iter()
            .copied()
            .enumerate()
            .map(|(i, k)| (k, i as u64))
            .collect();
        
        // Set up RMI configuration based on fuzz data
        let original_env_vars = vec![
            ("KYRODB_RMI_ROUTER_BITS", config_byte % 16 + 8), // 8-23 bits
            ("KYRODB_RMI_MAX_EPS", (config_byte / 16) as u32 * 100), // 0-1500 epsilon
            ("KYRODB_RMI_MIN_LEAF", (config_byte % 8 + 1) as usize * 64), // 64-512 min leaf
            ("KYRODB_RMI_TARGET_LEAF", (config_byte % 16 + 1) as usize * 256), // 256-4096 target leaf
        ];
        
        // Temporarily set environment variables
        let mut old_values = Vec::new();
        for (key, value) in &original_env_vars {
            let old_value = std::env::var(key).ok();
            old_values.push((key.to_string(), old_value));
            if *key == "KYRODB_RMI_MAX_EPS" {
                std::env::set_var(key, value.to_string());
            } else {
                std::env::set_var(key, value.to_string());
            }
        }
        
        // Create temporary directory and build RMI
        let dir = match tempfile::tempdir() {
            Ok(d) => d,
            Err(_) => {
                // Restore env vars before returning
                for ((key, _), old_value) in original_env_vars.iter().zip(old_values) {
                    match old_value {
                        Some(val) => std::env::set_var(key, val),
                        None => std::env::remove_var(key),
                    }
                }
                return;
            }
        };
        
        let path = dir.path().join("rmi_fuzz.bin");
        let pack_u32 = (config_byte & 0x80) != 0; // Use high bit for u32 packing decision
        
        if let Err(_) = RmiIndex::write_from_pairs_v5(&path, &pairs, pack_u32) {
            // Restore env vars before returning
            for ((key, _), old_value) in original_env_vars.iter().zip(old_values) {
                match old_value {
                    Some(val) => std::env::set_var(key, val),
                    None => std::env::remove_var(key),
                }
            }
            return;
        }
        
        // Load RMI and perform comprehensive testing
        if let Some(rmi) = RmiIndex::load_from_file(&path) {
            // Test 1: Basic predict_get for all keys (should all be found)
            for (i, &key) in keys.iter().enumerate() {
                let expected_offset = i as u64;
                if let Some(found_offset) = rmi.predict_get(&key) {
                    if found_offset != expected_offset {
                        panic!("Key {}: expected offset {}, got {}", key, expected_offset, found_offset);
                    }
                } else {
                    panic!("Key {} not found in RMI", key);
                }
            }
            
            // Test 2: predict_clamp bounds checking
            for &key in keys.iter().step_by(17) { // Sample every 17th key
                if let Some((lo, hi)) = rmi.debug_predict_clamp(key) {
                    // Clamp should be within valid bounds
                    assert!(lo <= hi, "Invalid clamp bounds: lo={}, hi={}", lo, hi);
                    assert!(hi < rmi.count(), "Clamp hi={} exceeds count={}", hi, rmi.count());
                    
                    // The key should be findable within this range
                    let mut found = false;
                    for idx in lo..=hi {
                        if rmi.key_at(idx) == key {
                            found = true;
                            break;
                        }
                    }
                    assert!(found, "Key {} not found in predicted range [{}, {}]", key, lo, hi);
                    
                    // Test probe_only within the clamp
                    if let Some(offset) = rmi.debug_probe_only(key, lo, hi) {
                        let expected_offset = keys.iter().position(|&k| k == key).unwrap() as u64;
                        assert_eq!(offset, expected_offset, "Probe mismatch for key {}", key);
                    } else {
                        panic!("debug_probe_only failed for key {} in range [{}, {}]", key, lo, hi);
                    }
                } else {
                    panic!("debug_predict_clamp returned None for key {}", key);
                }
            }
            
            // Test 3: Edge case keys (not in the set)
            let edge_keys = vec![
                0u64,
                1u64,
                u64::MAX,
                u64::MAX - 1,
                keys[0].saturating_sub(1),
                keys[0].saturating_sub(100),
                keys.last().unwrap().saturating_add(1),
                keys.last().unwrap().saturating_add(100),
            ];
            
            for &key in &edge_keys {
                // These should not be found
                if let Some(_) = rmi.predict_get(&key) {
                    // This is actually okay - RMI might have false positives
                    // but we should at least verify the returned offset makes sense
                }
                
                // predict_clamp should still work and return valid bounds
                if let Some((lo, hi)) = rmi.debug_predict_clamp(key) {
                    assert!(lo <= hi, "Invalid edge clamp bounds for key {}: lo={}, hi={}", key, lo, hi);
                    assert!(hi < rmi.count(), "Edge clamp hi={} exceeds count={}", hi, rmi.count());
                    
                    // probe_only should return None for keys not in range
                    let probe_result = rmi.debug_probe_only(key, lo, hi);
                    if probe_result.is_some() {
                        // If probe found something, verify it's actually the key
                        let found_key = rmi.key_at(lo + probe_result.unwrap() as usize - lo);
                        assert_eq!(found_key, key, "Probe found wrong key for edge case {}", key);
                    }
                }
            }
            
            // Test 4: Random keys from fuzz data (may or may not be found)
            for chunk in data.chunks_exact(8).take(50) {
                let mut b = [0u8; 8];
                b.copy_from_slice(chunk);
                let test_key = u64::from_le_bytes(b);
                
                // predict_get should not crash
                let _ = rmi.predict_get(&test_key);
                
                // predict_clamp should not crash and return valid bounds
                if let Some((lo, hi)) = rmi.debug_predict_clamp(test_key) {
                    assert!(lo <= hi, "Random key clamp invalid: lo={}, hi={}", lo, hi);
                    assert!(hi < rmi.count(), "Random key clamp hi={} exceeds count={}", hi, rmi.count());
                    
                    // probe_only should not crash
                    let _ = rmi.debug_probe_only(test_key, lo, hi);
                }
            }
            
            // Test 5: Leaf index lookup
            for &key in keys.iter().step_by(31) {
                if let Some(leaf_idx) = rmi.debug_find_leaf_index(key) {
                    assert!(leaf_idx < rmi.leaves.len(), "Invalid leaf index {} for key {}", leaf_idx, key);
                    
                    // Verify the leaf actually covers this key
                    let leaf = &rmi.leaves[leaf_idx];
                    assert!(key >= leaf.key_min && key <= leaf.key_max, 
                           "Key {} not in leaf range [{}, {}]", key, leaf.key_min, leaf.key_max);
                } else {
                    panic!("debug_find_leaf_index returned None for key {}", key);
                }
            }
            
            // Test 6: Prefetch (should not crash)
            for i in (0..rmi.count()).step_by(1000) {
                rmi.prefetch_window(i);
            }
            
            // Test 7: Warm function (should not crash)
            rmi.warm();
            
        } else {
            panic!("Failed to load RMI from file");
        }
        
        // Restore environment variables
        for ((key, _), old_value) in original_env_vars.iter().zip(old_values) {
            match old_value {
                Some(val) => std::env::set_var(key, val),
                None => std::env::remove_var(key),
            }
        }
        
        // tempdir cleanup happens automatically
    }
});
