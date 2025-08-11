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
#[derive(Debug)]
pub struct RmiIndex {
    // Placeholder: actual model params to be added
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
    pub fn load_from_file(path: &std::path::Path) -> Option<Self> {
        use std::io::Read;
        let mut f = std::fs::File::open(path).ok()?;
        let mut buf = [0u8; 16];
        f.read_exact(&mut buf).ok()?;
        if buf[0..8] == RMI_MAGIC && buf[8] == 1u8 { Some(Self {}) } else { None }
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

    pub fn insert(&mut self, key: u64, offset: u64) {
        match self {
            PrimaryIndex::BTree(b) => b.insert(key, offset),
            #[cfg(feature = "learned-index")]
            PrimaryIndex::Rmi(_r) => {
                // RMI uses delta map (to be implemented)
            }
        }
    }

    pub fn get(&self, key: &u64) -> Option<u64> {
        match self {
            PrimaryIndex::BTree(b) => b.get(key),
            #[cfg(feature = "learned-index")]
            PrimaryIndex::Rmi(r) => r.predict_get(key),
        }
    }
}