use std::collections::BTreeMap;

/// Basic trait for key → offset lookup.
pub trait Index {
    /// Insert a mapping from key to offset.
    fn insert(&mut self, key: u64, offset: u64);
    /// Lookup a key, returning the log offset if present.
    fn get(&self, key: &u64) -> Option<u64>;
}

/// Naive in-memory BTreeMap based index (single-column primary key).
#[derive(Default)]
pub struct BTreeIndex {
    map: BTreeMap<u64, u64>,
}

impl Index for BTreeIndex {
    fn insert(&mut self, key: u64, offset: u64) {
        self.map.insert(key, offset);
    }

    fn get(&self, key: &u64) -> Option<u64> {
        self.map.get(key).copied()
    }
}

impl BTreeIndex {
    pub fn new() -> Self {
        Self::default()
    }
}

#[cfg(feature = "learned-index")]
#[derive(Debug, Default)]
pub struct RmiIndex {
    // Placeholder: actual model params to be added
    // Delta map: keys appended after last build
    delta: BTreeMap<u64, u64>,
}

#[cfg(feature = "learned-index")]
#[repr(C)]
#[derive(Clone, Copy)]
struct RmiFileHeader {
    magic: [u8; 8],
    version: u8,
    _pad: [u8; 7],
}

#[cfg(feature = "learned-index")]
const RMI_MAGIC: [u8; 8] = *b"KYRO_RMI";

#[cfg(feature = "learned-index")]
impl RmiIndex {
    pub fn new() -> Self { Self { delta: BTreeMap::new() } }

    pub fn load_from_file(path: &std::path::Path) -> Option<Self> {
        use std::io::Read;
        let mut f = std::fs::File::open(path).ok()?;
        let mut buf = [0u8; 16];
        f.read_exact(&mut buf).ok()?;
        if buf[0..8] == RMI_MAGIC && buf[8] == 1u8 { Some(Self::new()) } else { None }
    }

    pub fn write_empty_file(path: &std::path::Path) -> std::io::Result<()> {
        use std::io::Write;
        let mut f = std::fs::File::create(path)?;
        let mut header = [0u8; 16];
        header[0..8].copy_from_slice(&RMI_MAGIC);
        header[8] = 1u8; // version
        f.write_all(&header)?;
        f.flush()
    }

    /// Build a trivial RMI file from key→offset pairs (placeholder for real model)
    pub fn write_from_pairs(path: &std::path::Path, pairs: &[(u64, u64)]) -> std::io::Result<()> {
        use std::io::Write;
        let mut f = std::fs::File::create(path)?;
        let mut header = [0u8; 16];
        header[0..8].copy_from_slice(&RMI_MAGIC);
        header[8] = 1u8; // version
        f.write_all(&header)?;
        // Write count (u64 LE)
        let count: u64 = pairs.len() as u64;
        f.write_all(&count.to_le_bytes())?;
        f.flush()
    }

    pub fn insert_delta(&mut self, key: u64, offset: u64) {
        self.delta.insert(key, offset);
    }

    pub fn delta_get(&self, key: &u64) -> Option<u64> {
        self.delta.get(key).copied()
    }

    pub fn predict_get(&self, _key: &u64) -> Option<u64> {
        // TODO: Implement predict + epsilon search against segments
        None
    }
}

pub enum PrimaryIndex {
    BTree(BTreeIndex),
    #[cfg(feature = "learned-index")]
    Rmi(RmiIndex),
}

impl PrimaryIndex {
    pub fn new_btree() -> Self {
        PrimaryIndex::BTree(BTreeIndex::new())
    }

    #[cfg(feature = "learned-index")]
    pub fn new_rmi() -> Self { PrimaryIndex::Rmi(RmiIndex::new()) }

    pub fn insert(&mut self, key: u64, offset: u64) {
        match self {
            PrimaryIndex::BTree(b) => b.insert(key, offset),
            #[cfg(feature = "learned-index")]
            PrimaryIndex::Rmi(r) => {
                r.insert_delta(key, offset);
            }
        }
    }

    pub fn get(&self, key: &u64) -> Option<u64> {
        match self {
            PrimaryIndex::BTree(b) => b.get(key),
            #[cfg(feature = "learned-index")]
            PrimaryIndex::Rmi(r) => r.delta_get(key).or_else(|| r.predict_get(key)),
        }
    }
}