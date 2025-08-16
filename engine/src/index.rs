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
    // Per-index global epsilon bound placeholder (until per-leaf is implemented)
    epsilon: u64,
}

#[cfg(feature = "learned-index")]
#[repr(C)]
#[derive(Clone, Copy)]
struct RmiFileHeaderV1 {
    magic: [u8; 8], // "KYRO_RMI"
    version: u8,    // 2 (upgraded layout with epsilon + count)
    _pad: [u8; 3],  // align to 12
    epsilon: u32,   // global epsilon
    count: u64,     // number of keys
}

#[cfg(feature = "learned-index")]
const RMI_MAGIC: [u8; 8] = *b"KYRO_RMI";

#[cfg(feature = "learned-index")]
impl RmiIndex {
    pub fn new() -> Self { Self { delta: BTreeMap::new(), sorted_keys: Vec::new(), sorted_offsets: Vec::new(), epsilon: 0 } }

    pub fn load_from_file(path: &std::path::Path) -> Option<Self> {
        use std::io::Read;
        let mut f = std::fs::File::open(path).ok()?;
        let mut hdr_bytes = [0u8; std::mem::size_of::<RmiFileHeaderV1>()];
        f.read_exact(&mut hdr_bytes).ok()?;
        // decode header
        if &hdr_bytes[0..8] != &RMI_MAGIC { return None; }
        if hdr_bytes[8] != 2u8 { return None; }
        let epsilon = u32::from_le_bytes([hdr_bytes[12], hdr_bytes[13], hdr_bytes[14], hdr_bytes[15]]) as u64;
        let mut cnt_arr = [0u8; 8];
        cnt_arr.copy_from_slice(&hdr_bytes[16..24]);
        let count = u64::from_le_bytes(cnt_arr) as usize;

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
        Some(Self { delta: BTreeMap::new(), sorted_keys: keys, sorted_offsets: offs, epsilon })
    }

    pub fn write_from_pairs(path: &std::path::Path, pairs: &[(u64, u64)]) -> std::io::Result<()> {
        use std::io::Write;
        let mut buf: Vec<(u64, u64)> = pairs.to_vec();
        buf.sort_by_key(|(k, _)| *k);
        let count: u64 = buf.len() as u64;
        let epsilon: u32 = 8; // temporary global epsilon until we have per-leaf metadata
        let mut f = std::fs::File::create(path)?;
        // header V2
        let mut header = Vec::with_capacity(std::mem::size_of::<RmiFileHeaderV1>());
        header.extend_from_slice(&RMI_MAGIC);
        header.push(2u8); // version
        header.extend_from_slice(&[0u8; 3]); // pad
        header.extend_from_slice(&epsilon.to_le_bytes());
        header.extend_from_slice(&count.to_le_bytes());
        f.write_all(&header)?;
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

    /// Predict a bounded search window [lo, hi] around the likely position.
    pub fn predict_window(&self, key: &u64) -> Option<(usize, usize)> {
        if self.sorted_keys.is_empty() { return None; }
        // For now, use binary_search index as center; in future use learned model position.
        match self.sorted_keys.binary_search(key) {
            Ok(idx) => {
                let eps = self.epsilon as usize;
                let lo = idx.saturating_sub(eps);
                let hi = (idx + eps).min(self.sorted_keys.len().saturating_sub(1));
                Some((lo, hi))
            }
            Err(idx) => {
                let eps = self.epsilon as usize;
                let lo = idx.saturating_sub(eps);
                let hi = (idx + eps).min(self.sorted_keys.len().saturating_sub(1));
                Some((lo, hi))
            }
        }
    }

    pub fn predict_get(&self, key: &u64) -> Option<u64> {
        if self.sorted_keys.is_empty() { return None; }
        if let Some((lo, hi)) = self.predict_window(key) {
            // bounded probe within [lo, hi]
            let slice = &self.sorted_keys[lo..=hi];
            match slice.binary_search(key) {
                Ok(rel) => self.sorted_offsets.get(lo + rel).copied(),
                Err(_) => None,
            }
        } else {
            None
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
            PrimaryIndex::Rmi(r) => {
                if let Some(v) = r.delta_get(key) {
                    crate::metrics::RMI_HITS_TOTAL.inc();
                    Some(v)
                } else if let Some(v) = r.predict_get(key) {
                    crate::metrics::RMI_HITS_TOTAL.inc();
                    Some(v)
                } else {
                    crate::metrics::RMI_MISSES_TOTAL.inc();
                    None
                }
            }
        }
    }
}