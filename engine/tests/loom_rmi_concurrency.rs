//! Simplified concurrency model tests for KyroDB using Loom.
//! These tests catch basic race conditions in RMI operations.

#![cfg(test)]

use loom::sync::{Arc, Mutex};
use loom::thread;
use std::collections::BTreeMap;

// --- Simplified Model ---

#[derive(Clone, Default)]
struct SimpleIndex {
    map: BTreeMap<u64, u64>,
}

impl SimpleIndex {
    fn insert(&mut self, key: u64, offset: u64) {
        self.map.insert(key, offset);
    }

    fn get(&self, key: &u64) -> Option<u64> {
        self.map.get(key).copied()
    }

    fn swap(&mut self, new_map: BTreeMap<u64, u64>) {
        self.map = new_map;
    }
}

#[test]
fn basic_concurrent_access() {
    loom::model(|| {
        let index = Arc::new(Mutex::new(SimpleIndex::default()));

        let writer = {
            let idx = index.clone();
            thread::spawn(move || {
                let mut i = idx.lock().unwrap();
                i.insert(1, 100);
                i.insert(2, 200);
            })
        };

        let reader = {
            let idx = index.clone();
            thread::spawn(move || {
                let i = idx.lock().unwrap();
                let _ = i.get(&1);
                let _ = i.get(&2);
            })
        };

        writer.join().unwrap();
        reader.join().unwrap();

        // Verify final state
        let final_idx = index.lock().unwrap();
        assert_eq!(final_idx.get(&1), Some(100));
        assert_eq!(final_idx.get(&2), Some(200));
    });
}

#[test]
fn swap_during_reads() {
    loom::model(|| {
        let index = Arc::new(Mutex::new(SimpleIndex::default()));

        // Pre-populate
        {
            let mut i = index.lock().unwrap();
            i.insert(1, 100);
            i.insert(2, 200);
        }

        let reader = {
            let idx = index.clone();
            thread::spawn(move || {
                for _ in 0..3 {
                    let i = idx.lock().unwrap();
                    let _ = i.get(&1);
                    let _ = i.get(&2);
                }
            })
        };

        let swapper = {
            let idx = index.clone();
            thread::spawn(move || {
                loom::thread::yield_now();
                let mut i = idx.lock().unwrap();
                let mut new_map = BTreeMap::new();
                new_map.insert(3, 300);
                new_map.insert(4, 400);
                i.swap(new_map);
            })
        };

        reader.join().unwrap();
        swapper.join().unwrap();

        // Verify swap worked
        let final_idx = index.lock().unwrap();
        assert_eq!(final_idx.get(&3), Some(300));
        assert_eq!(final_idx.get(&4), Some(400));
        // Old keys should be gone
        assert_eq!(final_idx.get(&1), None);
        assert_eq!(final_idx.get(&2), None);
    });
}

#[test]
fn concurrent_writes() {
    loom::model(|| {
        let index = Arc::new(Mutex::new(SimpleIndex::default()));

        let writer1 = {
            let idx = index.clone();
            thread::spawn(move || {
                let mut i = idx.lock().unwrap();
                i.insert(1, 100);
            })
        };

        let writer2 = {
            let idx = index.clone();
            thread::spawn(move || {
                let mut i = idx.lock().unwrap();
                i.insert(2, 200);
            })
        };

        writer1.join().unwrap();
        writer2.join().unwrap();

        let final_idx = index.lock().unwrap();
        assert!(final_idx.get(&1).is_some() || final_idx.get(&2).is_some());
    });
}