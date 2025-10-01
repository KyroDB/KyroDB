//! Recovery Stress Tests
//!
//! Tests recovery scenarios after crashes, corruptions, and failures

use crate::test::utils::*;
use crate::PersistentEventLog;
use std::sync::Arc;
use tokio::time::Instant;

/// Test recovery after simulated crash during writes
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_recovery_after_write_crash() {
    let data_dir = test_data_dir();

    // Phase 1: Write data and simulate crash
    {
        let log = Arc::new(open_test_log(data_dir.path()).await.unwrap());

        println!("\n📝 Writing data before crash...");
        for i in 0..10_000 {
            append_kv(&log, i, format!("pre_crash_{}", i).into_bytes())
                .await
                .unwrap();
        }

        // Ensure WAL is written
        log.snapshot().await.unwrap();

        println!("   💥 Simulating crash (dropping log without graceful shutdown)");
        drop(log);
    }

    // Phase 2: Recover and verify
    {
        println!("\n🔄 Recovering from crash...");
        let recover_start = Instant::now();

        let log = Arc::new(
            PersistentEventLog::open(data_dir.path().to_path_buf())
                .await
                .unwrap(),
        );

        let recover_elapsed = recover_start.elapsed();
        println!("   Recovery time: {:?}", recover_elapsed);

        #[cfg(feature = "learned-index")]
        {
            log.build_rmi().await.unwrap();
        }

        // Verify data
        println!("\n🔍 Verifying recovered data...");
        let mut found = 0;
        for i in (0..10_000).step_by(100) {
            if lookup_kv(&log, i).await.unwrap().is_some() {
                found += 1;
            }
        }

        println!("   Found {}/100 sampled keys", found);
        assert!(found >= 95, "Should recover at least 95% of data");
    }
}

/// Test recovery with multiple snapshots
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_recovery_with_multiple_snapshots() {
    let data_dir = test_data_dir();

    // Phase 1: Create multiple snapshots
    {
        let log = Arc::new(open_test_log(data_dir.path()).await.unwrap());

        println!("\n📝 Creating multiple snapshots...");

        for snapshot_round in 0..5 {
            // Write data
            let key_offset = snapshot_round * 2_000;
            for i in 0..2_000 {
                let key = (key_offset + i) as u64;
                append_kv(
                    &log,
                    key,
                    format!("snap_{}_{}", snapshot_round, i).into_bytes(),
                )
                .await
                .unwrap();
            }

            // Create snapshot
            log.snapshot().await.unwrap();
            println!("   Snapshot {} created", snapshot_round + 1);
        }

        drop(log);
    }

    // Phase 2: Recover and verify all data
    {
        println!("\n🔄 Recovering from multiple snapshots...");
        let log = Arc::new(
            PersistentEventLog::open(data_dir.path().to_path_buf())
                .await
                .unwrap(),
        );

        #[cfg(feature = "learned-index")]
        {
            log.build_rmi().await.unwrap();
        }

        println!("\n🔍 Verifying all snapshots recovered...");
        let total_keys = 5 * 2_000;
        let mut found = 0;

        for i in (0..total_keys).step_by(100) {
            if lookup_kv(&log, i).await.unwrap().is_some() {
                found += 1;
            }
        }

        println!("   Found {}/{} sampled keys", found, total_keys / 100);
        assert!(
            found >= (total_keys / 100) * 95 / 100,
            "Should recover 95%+ of all snapshots"
        );
    }
}

/// Test recovery after rapid restarts
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_rapid_restart_stress() {
    let data_dir = test_data_dir();

    const NUM_RESTARTS: usize = 10;
    const KEYS_PER_CYCLE: usize = 1_000;

    println!("\n🔄 Testing {} rapid restarts...", NUM_RESTARTS);

    for restart_round in 0..NUM_RESTARTS {
        let log = Arc::new(
            PersistentEventLog::open(data_dir.path().to_path_buf())
                .await
                .unwrap(),
        );

        // Write some data
        let key_offset = restart_round * KEYS_PER_CYCLE;
        for i in 0..KEYS_PER_CYCLE {
            let key = (key_offset + i) as u64;
            append_kv(
                &log,
                key,
                format!("restart_{}_{}", restart_round, i).into_bytes(),
            )
            .await
            .unwrap();
        }

        // Snapshot and close
        log.snapshot().await.unwrap();
        drop(log);

        println!(
            "   Restart {}/{} completed",
            restart_round + 1,
            NUM_RESTARTS
        );
    }

    // Final verification
    println!("\n🔍 Verifying data after all restarts...");
    let log = Arc::new(
        PersistentEventLog::open(data_dir.path().to_path_buf())
            .await
            .unwrap(),
    );

    #[cfg(feature = "learned-index")]
    {
        log.build_rmi().await.unwrap();
    }

    let total_keys = NUM_RESTARTS * KEYS_PER_CYCLE;
    let mut found = 0;

    for i in (0..total_keys).step_by(100) {
        if lookup_kv(&log, i as u64).await.unwrap().is_some() {
            found += 1;
        }
    }

    println!("   Found {}/{} sampled keys", found, total_keys / 100);
    assert_eq!(found, total_keys / 100, "All data should survive restarts");
}

/// Test recovery with WAL replay
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_wal_replay_recovery() {
    let data_dir = test_data_dir();

    // Phase 1: Write data without final snapshot
    {
        let log = Arc::new(open_test_log(data_dir.path()).await.unwrap());

        println!("\n📝 Writing data to WAL...");
        for i in 0..5_000 {
            append_kv(&log, i, format!("wal_entry_{}", i).into_bytes())
                .await
                .unwrap();
        }

        // Don't snapshot - leave data in WAL
        println!("   💾 Data left in WAL (no snapshot)");
        drop(log);
    }

    // Phase 2: Recovery should replay WAL
    {
        println!("\n🔄 Recovering with WAL replay...");
        let recover_start = Instant::now();

        let log = Arc::new(
            PersistentEventLog::open(data_dir.path().to_path_buf())
                .await
                .unwrap(),
        );

        let recover_elapsed = recover_start.elapsed();
        println!("   WAL replay time: {:?}", recover_elapsed);

        #[cfg(feature = "learned-index")]
        {
            log.build_rmi().await.unwrap();
        }

        println!("\n🔍 Verifying WAL replay...");
        let mut found = 0;
        for i in (0..5_000).step_by(50) {
            if lookup_kv(&log, i).await.unwrap().is_some() {
                found += 1;
            }
        }

        println!("   Found {}/100 sampled keys", found);
        assert!(found >= 95, "WAL replay should recover 95%+ of data");
    }
}
