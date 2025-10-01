//! Tests for Test Fixtures

use crate::test::utils::fixtures::*;

#[test]
fn test_value_of_size() {
    let small = value_of_size(SMALL_VALUE);
    assert_eq!(
        small.len(),
        SMALL_VALUE,
        "Small value should be correct size"
    );

    let medium = value_of_size(MEDIUM_VALUE);
    assert_eq!(
        medium.len(),
        MEDIUM_VALUE,
        "Medium value should be correct size"
    );

    let large = value_of_size(LARGE_VALUE);
    assert_eq!(
        large.len(),
        LARGE_VALUE,
        "Large value should be correct size"
    );
}

#[test]
fn test_random_value_size_range() {
    for _ in 0..100 {
        let value = random_value(100, 500);
        assert!(
            value.len() >= 100 && value.len() <= 500,
            "Random value size should be in range [100, 500], got {}",
            value.len()
        );
    }
}

#[test]
fn test_random_value_variance() {
    let value1 = random_value(1000, 1000);
    let value2 = random_value(1000, 1000);

    // Two random values of same size should be different
    assert_ne!(value1, value2, "Random values should differ");
}

#[test]
fn test_compressible_value_structure() {
    let value = compressible_value(1024);

    assert_eq!(value.len(), 1024, "Should be exact size");

    // Count zero bytes
    let zero_count = value.iter().filter(|&&b| b == 0).count();

    // Should be mostly zeros (highly compressible)
    assert!(
        zero_count > 900,
        "Compressible value should be mostly zeros, got {} zeros",
        zero_count
    );

    // But should have some non-zero bytes
    assert!(
        zero_count < 1024,
        "Should have some non-zero bytes for realism"
    );
}

#[test]
fn test_incompressible_value_entropy() {
    let value = incompressible_value(1000);

    assert_eq!(value.len(), 1000, "Should be exact size");

    // Check for high entropy (random data)
    let mut byte_counts = [0usize; 256];
    for &byte in &value {
        byte_counts[byte as usize] += 1;
    }

    // No byte should appear more than ~2% of the time (high entropy check)
    let max_count = byte_counts.iter().max().unwrap();
    assert!(
        *max_count < 50,
        "Incompressible value should have high entropy, max byte count: {}",
        max_count
    );
}

#[test]
fn test_dataset_config_default() {
    let config = DatasetConfig::default();

    assert_eq!(config.key_count, 1000);
    assert_eq!(config.value_size, MEDIUM_VALUE);
    assert!(config.sequential);
    assert!(!config.duplicates);
}

#[test]
fn test_dataset_config_small() {
    let config = DatasetConfig::small();

    assert_eq!(config.key_count, 100);
    assert_eq!(config.value_size, SMALL_VALUE);
}

#[test]
fn test_dataset_config_medium() {
    let config = DatasetConfig::medium();

    assert_eq!(config.key_count, 10_000);
    assert_eq!(config.value_size, MEDIUM_VALUE);
}

#[test]
fn test_dataset_config_large() {
    let config = DatasetConfig::large();

    assert_eq!(config.key_count, 1_000_000);
    assert_eq!(config.value_size, MEDIUM_VALUE);
}

#[test]
fn test_dataset_generation_sequential() {
    let mut config = DatasetConfig::small();
    config.sequential = true;
    config.duplicates = false;

    let data = config.generate();

    assert_eq!(
        data.len(),
        config.key_count,
        "Should generate correct count"
    );

    // Verify sequential keys
    for (i, (key, _)) in data.iter().enumerate() {
        assert_eq!(*key, i as u64, "Keys should be sequential");
    }

    // Verify value sizes
    for (_, value) in &data {
        assert_eq!(
            value.len(),
            config.value_size,
            "All values should be correct size"
        );
    }
}

#[test]
fn test_dataset_generation_random() {
    let mut config = DatasetConfig::small();
    config.sequential = false;
    config.duplicates = false;

    let data = config.generate();

    assert_eq!(
        data.len(),
        config.key_count,
        "Should generate correct count"
    );

    // Keys should have variance (not all sequential)
    let is_sequential = data.windows(2).all(|w| w[1].0 == w[0].0 + 1);
    assert!(!is_sequential, "Random keys should not be sequential");
}

#[test]
fn test_dataset_generation_with_duplicates() {
    let mut config = DatasetConfig::default();
    config.key_count = 1000;
    config.duplicates = true;

    let data = config.generate();

    // Should have more entries than key_count due to duplicates
    assert!(
        data.len() >= config.key_count,
        "Should have at least key_count entries (potentially more with duplicates)"
    );

    // Check for actual duplicates
    let mut keys_seen = std::collections::HashSet::new();
    let mut has_duplicate = false;

    for (key, _) in &data {
        if !keys_seen.insert(*key) {
            has_duplicate = true;
            break;
        }
    }

    // With 1000 keys and 10% duplicate probability, very likely to have at least one
    assert!(has_duplicate, "Should contain duplicate keys");
}

#[test]
fn test_workload_pattern_sequential() {
    let pattern = WorkloadPattern::Sequential;
    let keys = pattern.generate_keys(100, 1000);

    assert_eq!(keys.len(), 100);

    // Verify sequential
    for i in 0..keys.len() {
        assert_eq!(keys[i], i as u64, "Should be sequential");
    }
}

#[test]
fn test_workload_pattern_random() {
    let pattern = WorkloadPattern::Random;
    let keys = pattern.generate_keys(1000, 10000);

    assert_eq!(keys.len(), 1000);

    // All keys should be in range
    for key in &keys {
        assert!(*key < 10000, "Keys should be within range");
    }

    // Should have variance
    let first = keys[0];
    let has_variance = keys.iter().any(|&k| k != first);
    assert!(has_variance, "Random keys should vary");
}

#[test]
fn test_workload_pattern_zipfian() {
    let pattern = WorkloadPattern::Zipfian;
    let keys = pattern.generate_keys(10000, 10000);

    assert_eq!(keys.len(), 10000);

    // Count keys in hot range (first 20%)
    let hot_threshold = 2000;
    let hot_count = keys.iter().filter(|&&k| k < hot_threshold).count();
    let hot_percentage = (hot_count as f64 / keys.len() as f64) * 100.0;

    // Should be roughly 80% in hot range (Zipfian distribution)
    assert!(
        hot_percentage >= 70.0 && hot_percentage <= 90.0,
        "Zipfian should have ~80% keys in hot range, got {:.1}%",
        hot_percentage
    );
}

#[test]
fn test_workload_pattern_hot_cold() {
    let hot_keys = vec![10, 20, 30, 40, 50];
    let pattern = WorkloadPattern::HotCold {
        hot_ratio: 0.9,
        hot_keys: hot_keys.clone(),
    };

    let keys = pattern.generate_keys(1000, 10000);

    assert_eq!(keys.len(), 1000);

    // Count how many keys are from hot set
    let hot_count = keys.iter().filter(|k| hot_keys.contains(k)).count();
    let hot_percentage = (hot_count as f64 / keys.len() as f64) * 100.0;

    // Should be roughly 90% from hot set
    assert!(
        hot_percentage >= 85.0 && hot_percentage <= 95.0,
        "Hot/cold should have ~90% from hot set, got {:.1}%",
        hot_percentage
    );
}

#[test]
fn test_fixture_constants() {
    assert_eq!(SMALL_VALUE, 64);
    assert_eq!(MEDIUM_VALUE, 1024);
    assert_eq!(LARGE_VALUE, 65536);
}

#[test]
fn test_value_of_size_content() {
    let value = value_of_size(100);

    // All bytes should be 'x'
    assert!(value.iter().all(|&b| b == b'x'), "All bytes should be 'x'");
}

#[test]
fn test_dataset_config_builder_pattern() {
    let config = DatasetConfig {
        key_count: 5000,
        value_size: 512,
        sequential: false,
        duplicates: true,
    };

    assert_eq!(config.key_count, 5000);
    assert_eq!(config.value_size, 512);
    assert!(!config.sequential);
    assert!(config.duplicates);
}
