#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Require enough data to form a small key set
    if data.len() < 8 * 8 {
        return;
    }

    #[cfg(feature = "learned-index")]
    {
        use kyrodb_engine::index::RmiIndex;
        // Derive keys from input bytes; cap size to keep fuzz fast
        let mut keys: Vec<u64> = data
            .chunks_exact(8)
            .take(4096)
            .map(|c| {
                let mut b = [0u8; 8];
                b.copy_from_slice(c);
                u64::from_le_bytes(b)
            })
            .collect();
        if keys.is_empty() {
            return;
        }
        keys.sort_unstable();
        keys.dedup();
        if keys.len() < 64 {
            return;
        }
        // Ensure strictly increasing offsets to keep invariants simple
        let pairs: Vec<(u64, u64)> = keys
            .iter()
            .copied()
            .enumerate()
            .map(|(i, k)| (k, i as u64))
            .collect();

        // Build v5 AoS index with u32 packing when possible
        let dir = match tempfile::tempdir() {
            Ok(d) => d,
            Err(_) => return,
        };
        let path = dir.path().join("rmi_v5.bin");
        let _ = RmiIndex::write_from_pairs_v5(&path, &pairs, true);
        if let Some(rmi) = RmiIndex::load_from_file(&path) {
            // Probe a subset of keys and nearby misses; avoid unbounded loops
            for (i, k) in keys.iter().copied().enumerate().step_by(257) {
                let _ = rmi.predict_get(&k);
                let miss1 = k.saturating_add(1);
                let miss2 = k.saturating_sub(1);
                let _ = rmi.predict_get(&miss1);
                let _ = rmi.predict_get(&miss2);
                if i > 4096 {
                    break;
                }
            }
            // Exercise debug helpers (optional window probe)
            if let Some((_lo, _hi)) = rmi.debug_predict_clamp(keys[0]) {
                let _ = rmi.debug_probe_only(keys[0], _lo, _hi);
            }
        }
        // tempdir cleanup on drop
    }
});
