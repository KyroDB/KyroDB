//! RMI Performance Stress Tests
//!
//! Tests RMI accuracy, performance degradation, and rebuild efficiency under stress

use super::super::utils::*;
use std::sync::Arc;
use tokio::time::Instant;

/// Test RMI accuracy with skewed distribution
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[cfg(feature = "learned-index")]
async fn test_rmi_skewed_distribution_stress() {
    let data_dir = test_data_dir();
    let log = Arc::new(open_test_log(data_dir.path()).await.unwrap());

    const NUM_KEYS: usize = 100_000;

    println!(
        "\n📝 Inserting {} keys with Zipfian distribution...",
        NUM_KEYS
    );

    // Zipfian: 80% of operations hit 20% of keys
    for i in 0..NUM_KEYS {
        let key = if (i * 997) % 100 < 80 {
            // 80% access to hot 20%
            (i * 997) as u64 % (NUM_KEYS as u64 / 5)
        } else {
            // 20% hit remaining 80%
            i as u64
        };

        append_kv(&log, key, format!("zipf_{}", i).into_bytes())
            .await
            .unwrap();
    }

    println!("\n🔧 Building RMI for skewed data...");
    log.snapshot().await.unwrap();

    let rmi_start = Instant::now();
    log.build_rmi().await.unwrap();
    let rmi_elapsed = rmi_start.elapsed();

    println!("   RMI build time: {:?}", rmi_elapsed);

    // Test lookup performance on hot keys
    println!("\n🔍 Testing lookups on hot keys (20%)...");
    let hot_start = Instant::now();
    let hot_keys: Vec<u64> = (0..NUM_KEYS as u64 / 5).collect();

    let mut found = 0;
    for &key in &hot_keys {
        if lookup_kv(&log, key).await.unwrap().is_some() {
            found += 1;
        }
    }

    let hot_elapsed = hot_start.elapsed();

    println!("\n📊 Hot Keys Results:");
    println!("   Keys tested: {}", hot_keys.len());
    println!("   Found: {}", found);
    println!("   Duration: {:?}", hot_elapsed);
    println!(
        "   Throughput: {:.2} ops/sec",
        hot_keys.len() as f64 / hot_elapsed.as_secs_f64()
    );
}

/// Test RMI rebuild frequency stress
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[cfg(feature = "learned-index")]
async fn test_rmi_frequent_rebuilds() {
    let data_dir = test_data_dir();
    let log = Arc::new(open_test_log(data_dir.path()).await.unwrap());

    const NUM_REBUILDS: usize = 20;
    const KEYS_PER_REBUILD: usize = 5_000;

    println!(
        "\n🔧 Testing {} RMI rebuilds with {} keys each...",
        NUM_REBUILDS, KEYS_PER_REBUILD
    );

    let mut rebuild_times = Vec::new();

    for rebuild_round in 0..NUM_REBUILDS {
        // Insert new data
        let key_offset = rebuild_round * KEYS_PER_REBUILD;
        for i in 0..KEYS_PER_REBUILD {
            let key = (key_offset + i) as u64;
            append_kv(
                &log,
                key,
                format!("rebuild_{}_{}", rebuild_round, i).into_bytes(),
            )
            .await
            .unwrap();
        }

        // Rebuild RMI
        log.snapshot().await.unwrap();

        let rebuild_start = Instant::now();
        log.build_rmi().await.unwrap();
        let rebuild_time = rebuild_start.elapsed();

        rebuild_times.push(rebuild_time);

        println!(
            "   Rebuild {}/{}: {:?}",
            rebuild_round + 1,
            NUM_REBUILDS,
            rebuild_time
        );
    }

    // Calculate statistics
    let avg_rebuild_time =
        rebuild_times.iter().sum::<std::time::Duration>() / rebuild_times.len() as u32;
    let min_rebuild_time = rebuild_times.iter().min().unwrap();
    let max_rebuild_time = rebuild_times.iter().max().unwrap();

    println!("\n📊 Rebuild Statistics:");
    println!("   Total rebuilds: {}", NUM_REBUILDS);
    println!("   Avg rebuild time: {:?}", avg_rebuild_time);
    println!("   Min rebuild time: {:?}", min_rebuild_time);
    println!("   Max rebuild time: {:?}", max_rebuild_time);

    // Verify final state
    println!("\n🔍 Verifying final RMI accuracy...");
    let total_keys = NUM_REBUILDS * KEYS_PER_REBUILD;
    let mut found = 0;

    for i in (0..total_keys).step_by(100) {
        if lookup_kv(&log, i as u64).await.unwrap().is_some() {
            found += 1;
        }
    }

    println!("   Found {}/{} sampled keys", found, total_keys / 100);
    assert_eq!(found, total_keys / 100, "All keys should be found");
}

/// Test RMI with updates (same keys, different values)
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[cfg(feature = "learned-index")]
async fn test_rmi_with_continuous_updates() {
    let data_dir = test_data_dir();
    let log = Arc::new(open_test_log(data_dir.path()).await.unwrap());

    const NUM_KEYS: usize = 10_000;
    const NUM_UPDATE_ROUNDS: usize = 10;

    println!("\n📝 Initial insertion of {} keys...", NUM_KEYS);
    for i in 0..NUM_KEYS {
        append_kv(&log, i as u64, format!("initial_{}", i).into_bytes())
            .await
            .unwrap();
    }

    log.snapshot().await.unwrap();
    log.build_rmi().await.unwrap();

    println!("\n🔄 Running {} update rounds...", NUM_UPDATE_ROUNDS);

    for round in 0..NUM_UPDATE_ROUNDS {
        // Update all keys
        for i in 0..NUM_KEYS {
            append_kv(
                &log,
                i as u64,
                format!("update_{}_{}", round, i).into_bytes(),
            )
            .await
            .unwrap();
        }

        // Rebuild after updates
        log.snapshot().await.unwrap();
        log.build_rmi().await.unwrap();

        println!(
            "   Update round {}/{} completed",
            round + 1,
            NUM_UPDATE_ROUNDS
        );
    }

    // Verify latest values
    println!("\n🔍 Verifying latest values...");
    for i in (0..NUM_KEYS).step_by(100) {
        let value = lookup_kv(&log, i as u64).await.unwrap().unwrap();
        let expected = format!("update_{}_{}", NUM_UPDATE_ROUNDS - 1, i);
        assert_eq!(
            String::from_utf8(value).unwrap(),
            expected,
            "Should have latest value for key {}",
            i
        );
    }

    println!("   ✅ All values verified as latest");
}

/// Test RMI performance degradation with many segments
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[cfg(feature = "learned-index")]
async fn test_rmi_many_segments_stress() {
    let data_dir = test_data_dir();
    let log = Arc::new(open_test_log(data_dir.path()).await.unwrap());

    // Insert highly random keys to force many segments
    const NUM_KEYS: usize = 50_000;

    println!(
        "\n📝 Inserting {} random keys to stress RMI segmentation...",
        NUM_KEYS
    );

    let mut keys = Vec::new();
    for i in 0..NUM_KEYS {
        let key = (i as u64).wrapping_mul(1_000_000_007);
        keys.push(key);
        append_kv(&log, key, vec![0u8; 64]).await.unwrap();
    }

    println!("\n🔧 Building RMI with many segments...");
    log.snapshot().await.unwrap();

    let rmi_start = Instant::now();
    log.build_rmi().await.unwrap();
    let rmi_elapsed = rmi_start.elapsed();

    println!("   RMI build time: {:?}", rmi_elapsed);

    // Test lookup performance
    println!("\n🔍 Testing lookup performance...");
    let lookup_start = Instant::now();
    let mut found = 0;

    for &key in keys.iter().step_by(10) {
        if lookup_kv(&log, key).await.unwrap().is_some() {
            found += 1;
        }
    }

    let lookup_elapsed = lookup_start.elapsed();

    println!("\n📊 Many Segments Results:");
    println!("   Keys tested: {}", NUM_KEYS / 10);
    println!("   Found: {}", found);
    println!("   Lookup duration: {:?}", lookup_elapsed);
    println!(
        "   Lookup throughput: {:.2} ops/sec",
        found as f64 / lookup_elapsed.as_secs_f64()
    );

    assert_eq!(found, NUM_KEYS / 10, "All keys should be found");
}
