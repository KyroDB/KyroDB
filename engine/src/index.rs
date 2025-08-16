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
#[derive(Debug, Default, Clone)]
pub struct RmiLeafMeta {
    pub key_min: u64,
    pub key_max: u64,
    pub slope: f32,
    pub intercept: f32,
    pub epsilon: u32,
    pub start: u64,
    pub len: u64,
}

#[cfg(feature = "learned-index")]
#[derive(Debug, Default)]
pub struct RmiIndex {
    // Delta map: keys appended after last build
    delta: BTreeMap<u64, u64>,
    // Model-backed snapshot view
    leaves: Vec<RmiLeafMeta>,
    sorted_keys: Vec<u64>,
    sorted_offsets: Vec<u64>,
}

#[cfg(feature = "learned-index")]
const RMI_MAGIC: [u8; 8] = *b"KYRO_RMI";

#[cfg(feature = "learned-index")]
impl RmiIndex {
    pub fn new() -> Self {
        Self { delta: BTreeMap::new(), leaves: Vec::new(), sorted_keys: Vec::new(), sorted_offsets: Vec::new() }
    }

    /// Build per-leaf linear models and epsilon bounds from sorted pairs and write v3 format with checksum.
    pub fn write_from_pairs(path: &std::path::Path, pairs: &[(u64, u64)]) -> std::io::Result<()> {
        use std::io::Write;
        // sort by key
        let mut buf: Vec<(u64, u64)> = pairs.to_vec();
        buf.sort_by_key(|(k, _)| *k);
        let n = buf.len();
        let total_keys = n as u64;
        // choose number of leaves
        let target_leaf = 1024usize; // aim ~1k keys per leaf
        let num_leaves = std::cmp::max(1, (n + target_leaf - 1) / target_leaf) as u32;
        let mut leaves: Vec<RmiLeafMeta> = Vec::with_capacity(num_leaves as usize);
        // build keys, offsets arrays
        let mut keys: Vec<u64> = Vec::with_capacity(n);
        let mut offs: Vec<u64> = Vec::with_capacity(n);
        for (k, o) in &buf { keys.push(*k); offs.push(*o); }
        // partition and fit per leaf
        for li in 0..(num_leaves as usize) {
            let start = (li * n) / (num_leaves as usize);
            let end = (((li + 1) * n) / (num_leaves as usize)).max(start);
            let len = end - start;
            if len == 0 { continue; }
            let slice_keys = &keys[start..end];
            // y = index position (global)
            let start_idx = start as u64;
            let y_vals: Vec<f64> = (start..end).map(|i| i as f64).collect();
            // x = keys
            let x_vals: Vec<f64> = slice_keys.iter().map(|&k| k as f64).collect();
            let (slope, intercept) = if len == 1 {
                (0.0f32, start as f32)
            } else {
                // least squares
                let n_f = len as f64;
                let sum_x: f64 = x_vals.iter().sum();
                let sum_y: f64 = y_vals.iter().sum();
                let mean_x = sum_x / n_f;
                let mean_y = sum_y / n_f;
                let mut num = 0.0f64;
                let mut den = 0.0f64;
                for i in 0..len {
                    let dx = x_vals[i] - mean_x;
                    num += dx * (y_vals[i] - mean_y);
                    den += dx * dx;
                }
                let m = if den == 0.0 { 0.0 } else { num / den };
                let b = mean_y - m * mean_x;
                (m as f32, b as f32)
            };
            // epsilon: max absolute error in positions
            let mut eps: u32 = 0;
            for i in 0..len {
                let pred = slope as f64 * (slice_keys[i] as f64) + intercept as f64;
                let pred_idx = pred.round() as i64;
                let true_idx = (start + i) as i64;
                let err = (pred_idx - true_idx).unsigned_abs() as u32;
                if err > eps { eps = err; }
            }
            leaves.push(RmiLeafMeta {
                key_min: slice_keys.first().copied().unwrap(),
                key_max: slice_keys.last().copied().unwrap(),
                slope,
                intercept,
                epsilon: eps,
                start: start_idx,
                len: len as u64,
            });
        }
        // write header v3
        let mut f = std::fs::File::create(path)?;
        let mut header = Vec::new();
        header.extend_from_slice(&RMI_MAGIC);
        header.push(3u8); // version 3
        header.extend_from_slice(&[0u8; 3]); // pad
        header.extend_from_slice(&(num_leaves as u32).to_le_bytes());
        header.extend_from_slice(&total_keys.to_le_bytes());
        f.write_all(&header)?;
        // write leaves
        for leaf in &leaves {
            f.write_all(&leaf.key_min.to_le_bytes())?;
            f.write_all(&leaf.key_max.to_le_bytes())?;
            f.write_all(&leaf.slope.to_le_bytes())?;
            f.write_all(&leaf.intercept.to_le_bytes())?;
            f.write_all(&leaf.epsilon.to_le_bytes())?;
            f.write_all(&leaf.start.to_le_bytes())?;
            f.write_all(&leaf.len.to_le_bytes())?;
        }
        // write keys and offsets
        for k in &keys { f.write_all(&k.to_le_bytes())?; }
        for o in &offs { f.write_all(&o.to_le_bytes())?; }
        // checksum (xxh3_64 over all previous bytes)
        use xxhash_rust::xxh3::Xxh3;
        f.flush()?;
        let all = std::fs::File::open(path)?;
        let len_before = all.metadata()?.len();
        let mut hasher = Xxh3::new();
        {
            use std::io::Read;
            let mut rdr = std::io::BufReader::new(&all);
            let mut buf = vec![0u8; 64 * 1024];
            let mut remaining = len_before as usize;
            while remaining > 0 {
                let to_read = buf.len().min(remaining);
                rdr.read_exact(&mut buf[..to_read])?;
                hasher.update(&buf[..to_read]);
                remaining -= to_read;
            }
        }
        let sum = hasher.digest();
        drop(all);
        f.write_all(&sum.to_le_bytes())?;
        f.flush()?;
        Ok(())
    }

    pub fn load_from_file(path: &std::path::Path) -> Option<Self> {
        use std::io::Read;
        let meta_len = std::fs::metadata(path).ok()?.len();
        let mut f = std::fs::File::open(path).ok()?;
        let mut magic = [0u8; 8];
        f.read_exact(&mut magic).ok()?;
        if magic != RMI_MAGIC { return None; }
        let mut ver = [0u8; 1];
        f.read_exact(&mut ver).ok()?;
        let version = ver[0];
        let mut pad = [0u8; 3]; let _ = f.read_exact(&mut pad);
        let mut sorted_keys: Vec<u64> = Vec::new();
        let mut sorted_offsets: Vec<u64> = Vec::new();
        let mut leaves: Vec<RmiLeafMeta> = Vec::new();
        if version == 2u8 {
            // v2: [magic][ver][pad3][epsilon u32][count u64] then keys, offsets
            let mut eps_buf = [0u8; 4]; f.read_exact(&mut eps_buf).ok()?;
            let epsilon = u32::from_le_bytes(eps_buf);
            let mut cnt_buf = [0u8; 8]; f.read_exact(&mut cnt_buf).ok()?;
            let count = u64::from_le_bytes(cnt_buf) as usize;
            let mut kb = vec![0u8; count * 8]; if count > 0 { f.read_exact(&mut kb).ok()?; }
            for ch in kb.chunks_exact(8) { let mut a=[0u8;8]; a.copy_from_slice(ch); sorted_keys.push(u64::from_le_bytes(a)); }
            let mut ob = vec![0u8; count * 8]; if count > 0 { f.read_exact(&mut ob).ok()?; }
            for ch in ob.chunks_exact(8) { let mut a=[0u8;8]; a.copy_from_slice(ch); sorted_offsets.push(u64::from_le_bytes(a)); }
            if count > 0 {
                leaves.push(RmiLeafMeta { key_min: *sorted_keys.first().unwrap(), key_max: *sorted_keys.last().unwrap(), slope: 0.0, intercept: 0.0, epsilon, start: 0, len: count as u64 });
            }
            // no checksum in v2
            // v2 path fills leaves as one group, sorted_keys/offsets copied
            // metrics
            crate::metrics::RMI_INDEX_SIZE_BYTES.set(meta_len as f64);
            crate::metrics::RMI_INDEX_LEAVES.set(leaves.len() as f64);
            if let Some(max) = leaves.iter().map(|l| l.epsilon).max() { crate::metrics::RMI_EPSILON_MAX.set(max as f64); }
            for leaf in &leaves { crate::metrics::RMI_EPSILON_HISTOGRAM.observe(leaf.epsilon as f64); }
            return Some(Self { delta: BTreeMap::new(), leaves, sorted_keys, sorted_offsets });
        } else if version == 3u8 {
            // v3: [magic][ver][pad3][num_leaves u32][count u64] + leaves + keys + offsets + checksum u64
            let mut nl_buf = [0u8; 4]; f.read_exact(&mut nl_buf).ok()?;
            let num_leaves = u32::from_le_bytes(nl_buf) as usize;
            let mut cnt_buf = [0u8; 8]; f.read_exact(&mut cnt_buf).ok()?;
            let count = u64::from_le_bytes(cnt_buf) as usize;
            // read leaves
            for _ in 0..num_leaves {
                let mut b8 = [0u8; 8];
                let mut b4 = [0u8; 4];
                f.read_exact(&mut b8).ok()?; let key_min = u64::from_le_bytes(b8);
                f.read_exact(&mut b8).ok()?; let key_max = u64::from_le_bytes(b8);
                f.read_exact(&mut b4).ok()?; let slope = f32::from_le_bytes(b4);
                f.read_exact(&mut b4).ok()?; let intercept = f32::from_le_bytes(b4);
                f.read_exact(&mut b4).ok()?; let epsilon = u32::from_le_bytes(b4);
                f.read_exact(&mut b8).ok()?; let start = u64::from_le_bytes(b8);
                f.read_exact(&mut b8).ok()?; let len = u64::from_le_bytes(b8);
                leaves.push(RmiLeafMeta { key_min, key_max, slope, intercept, epsilon, start, len });
            }
            // read keys and offsets
            let mut kb = vec![0u8; count * 8]; if count > 0 { f.read_exact(&mut kb).ok()?; }
            for ch in kb.chunks_exact(8) { let mut a=[0u8;8]; a.copy_from_slice(ch); sorted_keys.push(u64::from_le_bytes(a)); }
            let mut ob = vec![0u8; count * 8]; if count > 0 { f.read_exact(&mut ob).ok()?; }
            for ch in ob.chunks_exact(8) { let mut a=[0u8;8]; a.copy_from_slice(ch); sorted_offsets.push(u64::from_le_bytes(a)); }
            // read checksum
            let mut sum_buf = [0u8; 8]; f.read_exact(&mut sum_buf).ok()?;
            let sum_read = u64::from_le_bytes(sum_buf);
            // recompute checksum on prior bytes
            use std::io::{Seek, SeekFrom};
            use xxhash_rust::xxh3::Xxh3;
            let mut all = std::fs::File::open(path).ok()?;
            let end = all.metadata().ok()?.len();
            all.seek(SeekFrom::Start(0)).ok()?;
            let mut hasher = Xxh3::new();
            {
                let mut rdr = std::io::BufReader::new(&all);
                let mut buf = vec![0u8; 64 * 1024];
                // exclude last 8 bytes (checksum)
                let mut remaining = (end - 8) as usize;
                while remaining > 0 {
                    let to_read = buf.len().min(remaining);
                    if rdr.read_exact(&mut buf[..to_read]).is_err() { return None; }
                    hasher.update(&buf[..to_read]);
                    remaining -= to_read;
                }
            }
            let sum_calc = hasher.digest();
            if sum_calc != sum_read { return None; }
            // after reading and verifying
            crate::metrics::RMI_INDEX_SIZE_BYTES.set(meta_len as f64);
            crate::metrics::RMI_INDEX_LEAVES.set(leaves.len() as f64);
            if let Some(max) = leaves.iter().map(|l| l.epsilon).max() { crate::metrics::RMI_EPSILON_MAX.set(max as f64); }
            for leaf in &leaves { crate::metrics::RMI_EPSILON_HISTOGRAM.observe(leaf.epsilon as f64); }
            return Some(Self { delta: BTreeMap::new(), leaves, sorted_keys, sorted_offsets });
        } else if version == 4u8 {
            // v4: mmap zero-copy
            use std::mem::size_of;
            use std::slice;
            let file = std::fs::File::open(path).ok()?;
            let map = unsafe { memmap2::MmapOptions::new().map(&file).ok()? };
            let mut off = 0usize;
            // magic
            if map.len() < 8 { return None; }
            off += 8; // already validated
            // version + pad
            off += 4;
            // num_leaves u32
            if map.len() < off + 4 { return None; }
            let num_leaves = u32::from_le_bytes(map[off..off+4].try_into().ok()?) as usize; off += 4;
            // count u64
            if map.len() < off + 8 { return None; }
            let count = u64::from_le_bytes(map[off..off+8].try_into().ok()?) as usize; off += 8;
            // leaves
            let _leaf_rec_size = size_of::<u64>()*2 + size_of::<f32>()*2 + size_of::<u32>() + size_of::<u64>()*2; // packed length reference
            let mut lvs: Vec<RmiLeafMeta> = Vec::with_capacity(num_leaves);
            for _ in 0..num_leaves {
                if map.len() < off + 8*2 + 4*3 + 8*2 { return None; }
                let key_min = u64::from_le_bytes(map[off..off+8].try_into().ok()?); off += 8;
                let key_max = u64::from_le_bytes(map[off..off+8].try_into().ok()?); off += 8;
                let slope = f32::from_le_bytes(map[off..off+4].try_into().ok()?); off += 4;
                let intercept = f32::from_le_bytes(map[off..off+4].try_into().ok()?); off += 4;
                let epsilon = u32::from_le_bytes(map[off..off+4].try_into().ok()?); off += 4;
                let start = u64::from_le_bytes(map[off..off+8].try_into().ok()?); off += 8;
                let len = u64::from_le_bytes(map[off..off+8].try_into().ok()?); off += 8;
                lvs.push(RmiLeafMeta { key_min, key_max, slope, intercept, epsilon, start, len });
            }
            // keys slice
            let keys_bytes = count * size_of::<u64>();
            if map.len() < off + keys_bytes { return None; }
            let keys_ptr = unsafe { map.as_ptr().add(off) } as *const u64; off += keys_bytes;
            let keys_slice: &'static [u64] = unsafe { slice::from_raw_parts(keys_ptr, count) };
            // offsets slice
            let offs_bytes = count * size_of::<u64>();
            if map.len() < off + offs_bytes + 8 { return None; }
            let offs_ptr = unsafe { map.as_ptr().add(off) } as *const u64; off += offs_bytes;
            // checksum at end (skip verifying here in v4 for speed; could include earlier region)
            let _sum = u64::from_le_bytes(map[off..off+8].try_into().ok()?);
            let _mmap_holder = map; // keep alive
            // Update metrics
            crate::metrics::RMI_INDEX_SIZE_BYTES.set(meta_len as f64);
            crate::metrics::RMI_INDEX_LEAVES.set(lvs.len() as f64);
            if let Some(max) = lvs.iter().map(|l| l.epsilon).max() { crate::metrics::RMI_EPSILON_MAX.set(max as f64); }
            for leaf in &lvs { crate::metrics::RMI_EPSILON_HISTOGRAM.observe(leaf.epsilon as f64); }
            return Some(Self {
                delta: BTreeMap::new(),
                leaves: lvs,
                sorted_keys: keys_slice.to_vec(),
                sorted_offsets: unsafe { slice::from_raw_parts(offs_ptr, count) }.to_vec(),
            });
        } else {
            return None;
        }
    }

    // --- new delta + predict APIs used by PrimaryIndex ---
    pub fn insert_delta(&mut self, key: u64, offset: u64) {
        self.delta.insert(key, offset);
    }

    pub fn delta_get(&self, key: &u64) -> Option<u64> {
        self.delta.get(key).copied()
    }

    fn find_leaf_index(&self, key: u64) -> Option<usize> {
        if self.leaves.is_empty() { return None; }
        // quick reject if outside global range
        if key < self.leaves.first()?.key_min || key > self.leaves.last()?.key_max { return None; }
        let mut lo: isize = 0;
        let mut hi: isize = (self.leaves.len() as isize) - 1;
        while lo <= hi {
            let mid = lo + ((hi - lo) >> 1);
            let leaf = &self.leaves[mid as usize];
            if key < leaf.key_min {
                hi = mid - 1;
            } else if key > leaf.key_max {
                lo = mid + 1;
            } else {
                return Some(mid as usize);
            }
        }
        None
    }

    fn predict_window(&self, leaf: &RmiLeafMeta, key: u64) -> (usize, usize) {
        if leaf.len == 0 { return (leaf.start as usize, leaf.start as usize); }
        let pred = leaf.slope as f64 * (key as f64) + leaf.intercept as f64;
        let center = pred.round() as i64;
        let start = leaf.start as i64;
        let end = (leaf.start + leaf.len - 1) as i64;
        let eps = leaf.epsilon as i64;
        let lo = std::cmp::max(start, center - eps) as usize;
        let hi = std::cmp::min(end, center + eps) as usize;
        (lo, hi)
    }

    pub fn predict_get(&self, key: &u64) -> Option<u64> {
        if self.sorted_keys.is_empty() { return None; }
        let li = self.find_leaf_index(*key)?;
        let leaf = &self.leaves[li];
        let (mut lo, mut hi) = self.predict_window(leaf, *key);
        if lo > hi { return None; }
        // clamp within array bounds just in case
        let max_idx = self.sorted_keys.len().saturating_sub(1);
        lo = lo.min(max_idx);
        hi = hi.min(max_idx);
        // binary search within [lo, hi]
        let mut l = lo;
        let mut r = hi;
        while l <= r {
            let m = l + ((r - l) >> 1);
            match self.sorted_keys[m].cmp(key) {
                std::cmp::Ordering::Less => l = m + 1,
                std::cmp::Ordering::Greater => {
                    if m == 0 { break; }
                    r = m - 1;
                }
                std::cmp::Ordering::Equal => return Some(self.sorted_offsets[m]),
            }
        }
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
            PrimaryIndex::Rmi(r) => {
                if let Some(v) = r.delta_get(key) {
                    crate::metrics::RMI_HITS_TOTAL.inc();
                    Some(v)
                } else {
                    let timer = crate::metrics::RMI_LOOKUP_LATENCY_SECONDS.start_timer();
                    let res = r.predict_get(key);
                    timer.observe_duration();
                    if res.is_some() { crate::metrics::RMI_HITS_TOTAL.inc(); } else { crate::metrics::RMI_MISSES_TOTAL.inc(); }
                    res
                }
            }
        }
    }
}