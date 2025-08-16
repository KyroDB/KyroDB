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
    // Delta map: keys appended after last build
    delta: BTreeMap<u64, u64>,
    // Sorted snapshot view
    sorted_keys: Vec<u64>,
    sorted_offsets: Vec<u64>,
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
    pub fn new() -> Self { Self { delta: BTreeMap::new(), sorted_keys: Vec::new(), sorted_offsets: Vec::new() } }

    pub fn load_from_file(path: &std::path::Path) -> Option<Self> {
        use std::io::Read;
        let mut f = std::fs::File::open(path).ok()?;
        let mut hdr = [0u8; 16];
        f.read_exact(&mut hdr).ok()?;
        if hdr[0..8] != RMI_MAGIC || hdr[8] != 1u8 { return None; }
        // Read count
        let mut cnt_buf = [0u8; 8];
        f.read_exact(&mut cnt_buf).ok()?;
        let count = u64::from_le_bytes(cnt_buf) as usize;
        // Read keys
        let mut keys_bytes = vec![0u8; count * 8];
        if count > 0 { f.read_exact(&mut keys_bytes).ok()?; }
        let mut keys = Vec::with_capacity(count);
        for chunk in keys_bytes.chunks_exact(8) {
            let mut arr = [0u8; 8];
            arr.copy_from_slice(chunk);
            keys.push(u64::from_le_bytes(arr));
        }
        // Read offsets
        let mut offs_bytes = vec![0u8; count * 8];
        if count > 0 { f.read_exact(&mut offs_bytes).ok()?; }
        let mut offs = Vec::with_capacity(count);
        for chunk in offs_bytes.chunks_exact(8) {
            let mut arr = [0u8; 8];
            arr.copy_from_slice(chunk);
            offs.push(u64::from_le_bytes(arr));
        }
        Some(Self { delta: BTreeMap::new(), sorted_keys: keys, sorted_offsets: offs })
    }

    pub fn write_empty_file(path: &std::path::Path) -> std::io::Result<()> {
        use std::io::Write;
        let mut f = std::fs::File::create(path)?;
        let mut header = [0u8; 16];
        header[0..8].copy_from_slice(&RMI_MAGIC);
        header[8] = 1u8; // version
        f.write_all(&header)?;
        // zero count
        f.write_all(&0u64.to_le_bytes())?;
        f.flush()
    }

    /// Build an RMI index file from key→offset pairs. Pairs can be unsorted; we will sort by key.
    pub fn write_from_pairs(path: &std::path::Path, pairs: &[(u64, u64)]) -> std::io::Result<()> {
        use std::io::Write;
        let mut buf: Vec<(u64, u64)> = pairs.to_vec();
        buf.sort_by_key(|(k, _)| *k);
        let count: u64 = buf.len() as u64;
        let mut f = std::fs::File::create(path)?;
        // header
        let mut header = [0u8; 16];
        header[0..8].copy_from_slice(&RMI_MAGIC);
        header[8] = 1u8; // version
        f.write_all(&header)?;
        // count
        f.write_all(&count.to_le_bytes())?;
        // keys
        for (k, _) in &buf { f.write_all(&k.to_le_bytes())?; }
        // offsets
        for (_, o) in &buf { f.write_all(&o.to_le_bytes())?; }
        f.flush()
    }

    pub fn insert_delta(&mut self, key: u64, offset: u64) {
        self.delta.insert(key, offset);
    }

    pub fn delta_get(&self, key: &u64) -> Option<u64> {
        self.delta.get(key).copied()
    }

    /// Temporary: if no learned prediction, fall back to binary search over sorted_keys.
    pub fn predict_get(&self, key: &u64) -> Option<u64> {
        if self.sorted_keys.is_empty() { return None; }
        match self.sorted_keys.binary_search(key) {
            Ok(idx) => self.sorted_offsets.get(idx).copied(),
            Err(_) => None,
        }
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