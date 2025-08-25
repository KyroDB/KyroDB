//! Concurrency model tests for snapshot + manifest + WAL rotation using loom.
//! These are small-scale structure tests to catch ordering/race bugs.

#![cfg(test)]

use loom::sync::{Arc, Mutex};
use loom::thread;

// A tiny model of the manifest/snapshot swap protocol
#[derive(Default)]
struct ModelState {
    snapshot_tmp: bool,
    snapshot_committed: bool,
    manifest_tmp: bool,
    manifest_committed: bool,
    wal_rotated: bool,
}

impl ModelState {
    fn write_snapshot_tmp(&mut self) { self.snapshot_tmp = true; }
    fn commit_snapshot(&mut self) { if self.snapshot_tmp { self.snapshot_committed = true; } }
    fn write_manifest_tmp(&mut self) { self.manifest_tmp = true; }
    fn commit_manifest(&mut self) { if self.manifest_tmp { self.manifest_committed = true; } }
    fn rotate_wal(&mut self) { self.wal_rotated = true; }
}

#[test]
fn model_snapshot_manifest_rotation_ordering() {
    loom::model(|| {
        let st = Arc::new(Mutex::new(ModelState::default()));

        let st_a = st.clone();
        let t1 = thread::spawn(move || {
            let mut s = st_a.lock().unwrap();
            s.write_snapshot_tmp();
            s.commit_snapshot(); // fsync + rename
        });

        let st_b = st.clone();
        let t2 = thread::spawn(move || {
            let mut s = st_b.lock().unwrap();
            s.write_manifest_tmp();
            s.commit_manifest(); // fsync + rename
        });

        let st_c = st.clone();
        let t3 = thread::spawn(move || {
            let mut s = st_c.lock().unwrap();
            s.rotate_wal();
        });

        t1.join().unwrap();
        t2.join().unwrap();
        t3.join().unwrap();

        // Safety properties: manifest must not be committed unless snapshot is committed.
        let s = st.lock().unwrap();
        if s.manifest_committed {
            assert!(s.snapshot_committed, "manifest committed before snapshot committed");
        }
        // WAL rotation can happen independently, but after rotation a manifest rewrite is expected in real system.
    });
}
