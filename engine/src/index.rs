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
#[derive(Debug)]
enum RmiBacking {
    Owned {
        sorted_keys: Vec<u64>,
        sorted_offsets: Vec<u64>,
    },
    Mmap {
        mmap: memmap2::Mmap,
        keys_off: usize,
        offs_off: usize,
        count: usize,
    },
    // v5: interleaved AoS entries
    MmapAos {
        mmap: memmap2::Mmap,
        entries_off: usize,
        count: usize,
        off_is_u32: bool,
        entry_stride: usize,
    },
}

#[cfg(feature = "learned-index")]
#[derive(Debug)]
pub struct RmiIndex {
    // Delta map: keys appended after last build
    delta: BTreeMap<u64, u64>,
    // Model-backed snapshot view
    leaves: Vec<RmiLeafMeta>,
    // 64K router indexed by top-16 bits of key → leaf index (u32::MAX for none)
    router: Box<[u32; 65536]>,
    backing: RmiBacking,
}

#[cfg(feature = "learned-index")]
const RMI_MAGIC: [u8; 8] = *b"KYRO_RMI";

#[cfg(feature = "learned-index")]
impl RmiIndex {
    pub fn new() -> Self {
        Self {
            delta: BTreeMap::new(),
            leaves: Vec::new(),
            router: Box::new([u32::MAX; 65536]),
            backing: RmiBacking::Owned {
                sorted_keys: Vec::new(),
                sorted_offsets: Vec::new(),
            },
        }
    }

    // Build router from leaves
    fn build_router(leaves: &[RmiLeafMeta]) -> Box<[u32; 65536]> {
        let mut r = Box::new([u32::MAX; 65536]);
        if leaves.is_empty() { return r; }
        for (i, leaf) in leaves.iter().enumerate() {
            let lo = (leaf.key_min >> 48) as usize;
            let hi = (leaf.key_max >> 48) as usize;
            for idx in lo..=hi { r[idx] = i as u32; }
        }
        // Fill holes by carrying nearest previous leaf, then forward fill
        let mut last = 0u32;
        for i in 0..65536 {
            if r[i] == u32::MAX { r[i] = last; } else { last = r[i]; }
        }
        let mut last = *r.last().unwrap();
        for i in (0..65536).rev() {
            if r[i] == u32::MAX { r[i] = last; } else { last = r[i]; }
        }
        r
    }

    // --- helpers unified across backings ---
    #[inline]
    fn count(&self) -> usize {
        match &self.backing {
            RmiBacking::Owned { sorted_keys, .. } => sorted_keys.len(),
            RmiBacking::Mmap { count, .. } => *count,
            RmiBacking::MmapAos { count, .. } => *count,
        }
    }

    #[inline]
    fn key_at(&self, idx: usize) -> u64 {
        match &self.backing {
            RmiBacking::Owned { sorted_keys, .. } => sorted_keys[idx],
            RmiBacking::Mmap { mmap, keys_off, .. } => unsafe {
                let ptr = mmap.as_ptr().add(*keys_off + idx * std::mem::size_of::<u64>()) as *const u64;
                u64::from_le_bytes(std::ptr::read_unaligned(ptr).to_le_bytes())
            },
            RmiBacking::MmapAos { mmap, entries_off, entry_stride, off_is_u32, .. } => unsafe {
                let base = mmap.as_ptr().add(*entries_off + idx * *entry_stride);
                // layout: key u64 at +0, off at +8 or +8..+16
                let key_ptr = base as *const u64;
                if *off_is_u32 {
                    u64::from_le_bytes(std::ptr::read_unaligned(key_ptr).to_le_bytes())
                } else {
                    u64::from_le_bytes(std::ptr::read_unaligned(key_ptr).to_le_bytes())
                }
            },
        }
    }

    #[inline]
    fn off_at(&self, idx: usize) -> u64 {
        match &self.backing {
            RmiBacking::Owned { sorted_offsets, .. } => sorted_offsets[idx],
            RmiBacking::Mmap { mmap, offs_off, .. } => unsafe {
                let ptr = mmap.as_ptr().add(*offs_off + idx * std::mem::size_of::<u64>()) as *const u64;
                u64::from_le_bytes(std::ptr::read_unaligned(ptr).to_le_bytes())
            },
            RmiBacking::MmapAos { mmap, entries_off, entry_stride, off_is_u32, .. } => unsafe {
                let base = mmap.as_ptr().add(*entries_off + idx * *entry_stride);
                if *off_is_u32 {
                    let off_ptr = base.add(8) as *const u32;
                    u32::from_le_bytes(std::ptr::read_unaligned(off_ptr).to_le_bytes()) as u64
                } else {
                    let off_ptr = base.add(8) as *const u64;
                    u64::from_le_bytes(std::ptr::read_unaligned(off_ptr).to_le_bytes())
                }
            },
        }
    }

    // add delta helpers missing after refactor
    pub fn insert_delta(&mut self, key: u64, offset: u64) { self.delta.insert(key, offset); }
    pub fn delta_get(&self, key: &u64) -> Option<u64> { self.delta.get(key).copied() }

    // --- common leaf fit helpers (adaptive epsilon) ---
    fn fit_leaf(keys: &[u64], start: usize, end: usize) -> (f32, f32, u32) {
        let len = end - start;
        if len == 0 { return (0.0, start as f32, 0); }
        if len == 1 { return (0.0, start as f32, 0); }
        let n_f = len as f64;
        let y_vals: Vec<f64> = (start..end).map(|i| i as f64).collect();
        let x_vals: Vec<f64> = keys[start..end].iter().map(|&k| k as f64).collect();
        let sum_x: f64 = x_vals.iter().sum();
        let sum_y: f64 = y_vals.iter().sum();
        let mean_x = sum_x / n_f; let mean_y = sum_y / n_f;
        let mut num = 0.0f64; let mut den = 0.0f64;
        for i in 0..len { let dx = x_vals[i] - mean_x; num += dx * (y_vals[i] - mean_y); den += dx * dx; }
        let m = if den == 0.0 { 0.0 } else { num / den } as f32;
        let b = (mean_y - (m as f64) * mean_x) as f32;
        let mut eps: u32 = 0;
        for (i, &k) in keys[start..end].iter().enumerate() {
            let pred = m as f64 * (k as f64) + b as f64;
            let pred_idx = pred.round() as i64; let true_idx = (start + i) as i64;
            let err = (pred_idx - true_idx).unsigned_abs() as u32; if err > eps { eps = err; }
        }
        (m, b, eps)
    }

    fn build_leaves_adaptive(keys: &[u64], max_eps: u32, min_leaf: usize) -> Vec<RmiLeafMeta> {
        fn rec(keys: &[u64], start: usize, end: usize, max_eps: u32, min_leaf: usize, out: &mut Vec<RmiLeafMeta>) {
            let len = end - start; if len == 0 { return; }
            let (m, b, eps) = RmiIndex::fit_leaf(keys, start, end);
            if eps > max_eps && len > (min_leaf * 2) {
                let mid = start + len / 2;
                rec(keys, start, mid, max_eps, min_leaf, out);
                rec(keys, mid, end, max_eps, min_leaf, out);
            } else {
                out.push(RmiLeafMeta {
                    key_min: keys[start],
                    key_max: keys[end - 1],
                    slope: m,
                    intercept: b,
                    epsilon: eps,
                    start: start as u64,
                    len: len as u64,
                });
            }
        }
        let mut out = Vec::new();
        rec(keys, 0, keys.len(), max_eps, min_leaf, &mut out);
        out
    }

    /// Build per-leaf models from sorted pairs and write v4 format.
    pub fn write_from_pairs(path: &std::path::Path, pairs: &[(u64, u64)]) -> std::io::Result<()> {
        use std::io::Write as _;
        let mut buf: Vec<(u64, u64)> = pairs.to_vec();
        buf.sort_by_key(|(k, _)| *k);
        let n = buf.len();
        let total_keys = n as u64;
        let keys: Vec<u64> = buf.iter().map(|(k, _)| *k).collect();
        let offs: Vec<u64> = buf.iter().map(|(_, o)| *o).collect();
        let env_max_eps = std::env::var("KYRODB_RMI_MAX_EPS").ok().and_then(|s| s.parse::<u32>().ok());
        let leaves = if let Some(max_eps) = env_max_eps {
            let min_leaf = std::env::var("KYRODB_RMI_MIN_LEAF").ok().and_then(|s| s.parse::<usize>().ok()).unwrap_or(256);
            Self::build_leaves_adaptive(&keys, max_eps, min_leaf)
        } else {
            let target_leaf = std::env::var("KYRODB_RMI_TARGET_LEAF").ok().and_then(|s| s.parse::<usize>().ok()).filter(|&v| v > 0).unwrap_or(1024usize);
            let num_leaves = (n.div_ceil(target_leaf)) as usize;
            let mut lvs: Vec<RmiLeafMeta> = Vec::with_capacity(num_leaves);
            for li in 0..num_leaves { let start = (li * n) / num_leaves; let end = (((li + 1) * n) / num_leaves).max(start + 1); let (m, b, eps) = Self::fit_leaf(&keys, start, end); lvs.push(RmiLeafMeta { key_min: keys[start], key_max: keys[end - 1], slope: m, intercept: b, epsilon: eps, start: start as u64, len: (end - start) as u64 }); }
            lvs
        };
        let mut f = std::fs::File::create(path)?;
        let mut header = Vec::new(); header.extend_from_slice(&RMI_MAGIC); header.push(4u8); header.extend_from_slice(&[0u8; 3]); header.extend_from_slice(&(leaves.len() as u32).to_le_bytes()); header.extend_from_slice(&total_keys.to_le_bytes()); f.write_all(&header)?;
        for leaf in &leaves { f.write_all(&leaf.key_min.to_le_bytes())?; f.write_all(&leaf.key_max.to_le_bytes())?; f.write_all(&leaf.slope.to_le_bytes())?; f.write_all(&leaf.intercept.to_le_bytes())?; f.write_all(&leaf.epsilon.to_le_bytes())?; f.write_all(&leaf.start.to_le_bytes())?; f.write_all(&leaf.len.to_le_bytes())?; }
        { use std::io::Seek; let pos = f.stream_position()?; let pad = ((8 - (pos % 8)) % 8) as usize; if pad > 0 { f.write_all(&vec![0u8; pad])?; } }
        for k in &keys { f.write_all(&k.to_le_bytes())?; }
        for o in &offs { f.write_all(&o.to_le_bytes())?; }
        use xxhash_rust::xxh3::Xxh3; f.flush()?; let all = std::fs::File::open(path)?; let len_before = all.metadata()?.len(); let mut hasher = Xxh3::new(); { use std::io::Read; let mut rdr = std::io::BufReader::new(&all); let mut buf_h = vec![0u8; 64 * 1024]; let mut remaining = len_before as usize; while remaining > 0 { let to_read = buf_h.len().min(remaining); rdr.read_exact(&mut buf_h[..to_read])?; hasher.update(&buf_h[..to_read]); remaining -= to_read; } } let sum = hasher.digest(); drop(all); f.write_all(&sum.to_le_bytes())?; f.flush()?; Ok(())
    }

    /// v5 writer: interleave entries (key, off) with optional u32 packing; adaptive leaves if configured.
    pub fn write_from_pairs_v5(path: &std::path::Path, pairs: &[(u64, u64)], pack_u32: bool) -> std::io::Result<()> {
        use std::io::Write as _;
        let mut buf: Vec<(u64, u64)> = pairs.to_vec(); buf.sort_by_key(|(k, _)| *k);
        let n = buf.len(); let total_keys = n as u64; let keys: Vec<u64> = buf.iter().map(|(k, _)| *k).collect();
        let env_max_eps = std::env::var("KYRODB_RMI_MAX_EPS").ok().and_then(|s| s.parse::<u32>().ok());
        let leaves = if let Some(max_eps) = env_max_eps { let min_leaf = std::env::var("KYRODB_RMI_MIN_LEAF").ok().and_then(|s| s.parse::<usize>().ok()).unwrap_or(256); Self::build_leaves_adaptive(&keys, max_eps, min_leaf) } else { let target_leaf = std::env::var("KYRODB_RMI_TARGET_LEAF").ok().and_then(|s| s.parse::<usize>().ok()).filter(|&v| v > 0).unwrap_or(1024usize); let num_leaves = (n.div_ceil(target_leaf)) as usize; let mut lvs: Vec<RmiLeafMeta> = Vec::with_capacity(num_leaves); for li in 0..num_leaves { let start = (li * n) / num_leaves; let end = (((li + 1) * n) / num_leaves).max(start + 1); let (m, b, eps) = Self::fit_leaf(&keys, start, end); lvs.push(RmiLeafMeta { key_min: keys[start], key_max: keys[end - 1], slope: m, intercept: b, epsilon: eps, start: start as u64, len: (end - start) as u64 }); } lvs };
        let mut f = std::fs::File::create(path)?; let mut header = Vec::new(); header.extend_from_slice(&RMI_MAGIC); header.push(5u8); header.extend_from_slice(&[0u8; 3]); header.extend_from_slice(&(leaves.len() as u32).to_le_bytes()); header.extend_from_slice(&total_keys.to_le_bytes()); header.push(if pack_u32 { 4u8 } else { 8u8 }); header.extend_from_slice(&[0u8; 7]); f.write_all(&header)?; for leaf in &leaves { f.write_all(&leaf.key_min.to_le_bytes())?; f.write_all(&leaf.key_max.to_le_bytes())?; f.write_all(&leaf.slope.to_le_bytes())?; f.write_all(&leaf.intercept.to_le_bytes())?; f.write_all(&leaf.epsilon.to_le_bytes())?; f.write_all(&leaf.start.to_le_bytes())?; f.write_all(&leaf.len.to_le_bytes())?; }
        { use std::io::Seek; let pos = f.stream_position()?; let pad = ((8 - (pos % 8)) % 8) as usize; if pad > 0 { f.write_all(&vec![0u8; pad])?; } }
        if pack_u32 { for (k, o) in &buf { f.write_all(&k.to_le_bytes())?; f.write_all(&(*o as u32).to_le_bytes())?; } } else { for (k, o) in &buf { f.write_all(&k.to_le_bytes())?; f.write_all(&o.to_le_bytes())?; } }
        use xxhash_rust::xxh3::Xxh3; f.flush()?; let all = std::fs::File::open(path)?; let len_before = all.metadata()?.len(); let mut hasher = Xxh3::new(); { use std::io::Read; let mut rdr = std::io::BufReader::new(&all); let mut b = vec![0u8; 64 * 1024]; let mut remaining = len_before as usize; while remaining > 0 { let to_read = b.len().min(remaining); rdr.read_exact(&mut b[..to_read])?; hasher.update(&b[..to_read]); remaining -= to_read; } } let sum = hasher.digest(); drop(all); f.write_all(&sum.to_le_bytes())?; f.flush()?; Ok(())
    }

    pub fn load_from_file(path: &std::path::Path) -> Option<Self> {
        use std::io::Read;
        let meta_len = std::fs::metadata(path).ok()?.len();
        let mut f = std::fs::File::open(path).ok()?;
        let mut magic = [0u8; 8]; f.read_exact(&mut magic).ok()?; if magic != RMI_MAGIC { return None; }
        let mut ver = [0u8; 1]; f.read_exact(&mut ver).ok()?; let version = ver[0]; let mut pad = [0u8; 3]; let _ = f.read_exact(&mut pad);
        match version {
            4 => {
                // v4: mmap zero-copy capable with checksum verify and alignment padding
                use std::mem::size_of;
                let file = std::fs::File::open(path).ok()?;
                let map = unsafe { memmap2::MmapOptions::new().map(&file).ok()? };
                // OS hints
                #[cfg(all(unix, not(target_os = "macos")))]
                unsafe {
                    let _ = libc::madvise(map.as_ptr() as *mut _, map.len(), libc::MADV_RANDOM);
                    #[cfg(target_os = "linux")]
                    let _ = libc::madvise(map.as_ptr() as *mut _, map.len(), 14 /* MADV_HUGEPAGE */);
                }
                let mut off = 0usize; if map.len() < 8 { return None; }
                off += 8; off += 4;
                if map.len() < off + 4 { return None; }
                let num_leaves = u32::from_le_bytes(map[off..off + 4].try_into().ok()?) as usize; off += 4;
                if map.len() < off + 8 { return None; }
                let count = u64::from_le_bytes(map[off..off + 8].try_into().ok()?) as usize; off += 8;
                let leaf_rec_size = size_of::<u64>() * 2 + size_of::<f32>() * 2 + size_of::<u32>() + size_of::<u64>() * 2;
                let mut lvs: Vec<RmiLeafMeta> = Vec::with_capacity(num_leaves);
                for _ in 0..num_leaves {
                    if map.len() < off + leaf_rec_size { return None; }
                    let key_min = u64::from_le_bytes(map[off..off + 8].try_into().ok()?); off += 8;
                    let key_max = u64::from_le_bytes(map[off..off + 8].try_into().ok()?); off += 8;
                    let slope = f32::from_le_bytes(map[off..off + 4].try_into().ok()?); off += 4;
                    let intercept = f32::from_le_bytes(map[off..off + 4].try_into().ok()?); off += 4;
                    let epsilon = u32::from_le_bytes(map[off..off + 4].try_into().ok()?); off += 4;
                    let start = u64::from_le_bytes(map[off..off + 8].try_into().ok()?); off += 8;
                    let len = u64::from_le_bytes(map[off..off + 8].try_into().ok()?); off += 8;
                    lvs.push(RmiLeafMeta { key_min, key_max, slope, intercept, epsilon, start, len });
                }
                let pad_bytes = (8 - (off % 8)) & 7; if map.len() < off + pad_bytes { return None; } off += pad_bytes;
                let keys_off = off; let keys_bytes = count.checked_mul(size_of::<u64>())?; if map.len() < off + keys_bytes { return None; } off += keys_bytes;
                let offs_off = off; let offs_bytes = count.checked_mul(size_of::<u64>())?; if map.len() < off + offs_bytes + 8 { return None; } off += offs_bytes;
                let sum_read = u64::from_le_bytes(map[off..off + 8].try_into().ok()?);
                use xxhash_rust::xxh3::Xxh3; let mut hasher = Xxh3::new(); hasher.update(&map[..off]); let sum_calc = hasher.digest(); if sum_calc != sum_read { return None; }
                let aligned = (keys_off % std::mem::align_of::<u64>() == 0) && (offs_off % std::mem::align_of::<u64>() == 0);
                let backing = if aligned { RmiBacking::Mmap { mmap: map, keys_off, offs_off, count } } else {
                    let mut sorted_keys: Vec<u64> = Vec::with_capacity(count);
                    for chunk in (map[keys_off..keys_off + keys_bytes]).chunks_exact(8) { let mut a = [0u8; 8]; a.copy_from_slice(chunk); sorted_keys.push(u64::from_le_bytes(a)); }
                    let mut sorted_offsets: Vec<u64> = Vec::with_capacity(count);
                    for chunk in (map[offs_off..offs_off + offs_bytes]).chunks_exact(8) { let mut a = [0u8; 8]; a.copy_from_slice(chunk); sorted_offsets.push(u64::from_le_bytes(a)); }
                    RmiBacking::Owned { sorted_keys, sorted_offsets }
                };
                crate::metrics::RMI_INDEX_SIZE_BYTES.set(meta_len as f64);
                crate::metrics::RMI_INDEX_LEAVES.set(lvs.len() as f64);
                if let Some(max) = lvs.iter().map(|l| l.epsilon).max() { crate::metrics::RMI_EPSILON_MAX.set(max as f64); }
                for leaf in &lvs { crate::metrics::RMI_EPSILON_HISTOGRAM.observe(leaf.epsilon as f64); }
                let router = Self::build_router(&lvs);
                return Some(Self { delta: BTreeMap::new(), leaves: lvs, router, backing });
            }
            5 => {
                use std::mem::size_of;
                let file = std::fs::File::open(path).ok()?;
                let map = unsafe { memmap2::MmapOptions::new().map(&file).ok()? };
                // OS hints
                #[cfg(all(unix, not(target_os = "macos")))]
                unsafe {
                    let _ = libc::madvise(map.as_ptr() as *mut _, map.len(), libc::MADV_RANDOM);
                    #[cfg(target_os = "linux")]
                    let _ = libc::madvise(map.as_ptr() as *mut _, map.len(), 14 /* MADV_HUGEPAGE */);
                }
                let mut off = 0usize; if map.len() < 8 { return None; }
                off += 8; off += 4; if map.len() < off + 4 { return None; }
                let num_leaves = u32::from_le_bytes(map[off..off + 4].try_into().ok()?) as usize; off += 4;
                if map.len() < off + 8 { return None; }
                let count = u64::from_le_bytes(map[off..off + 8].try_into().ok()?) as usize; off += 8;
                if map.len() < off + 1 + 7 { return None; }
                let offw = map[off]; let off_is_u32 = offw == 4; off += 8;
                let leaf_rec_size = size_of::<u64>() * 2 + size_of::<f32>() * 2 + size_of::<u32>() + size_of::<u64>() * 2;
                let mut lvs: Vec<RmiLeafMeta> = Vec::with_capacity(num_leaves);
                for _ in 0..num_leaves {
                    if map.len() < off + leaf_rec_size { return None; }
                    let key_min = u64::from_le_bytes(map[off..off + 8].try_into().ok()?); off += 8;
                    let key_max = u64::from_le_bytes(map[off..off + 8].try_into().ok()?); off += 8;
                    let slope = f32::from_le_bytes(map[off..off + 4].try_into().ok()?); off += 4;
                    let intercept = f32::from_le_bytes(map[off..off + 4].try_into().ok()?); off += 4;
                    let epsilon = u32::from_le_bytes(map[off..off + 4].try_into().ok()?); off += 4;
                    let start = u64::from_le_bytes(map[off..off + 8].try_into().ok()?); off += 8;
                    let len = u64::from_le_bytes(map[off..off + 8].try_into().ok()?); off += 8;
                    lvs.push(RmiLeafMeta { key_min, key_max, slope, intercept, epsilon, start, len });
                }
                let pad_bytes = (8 - (off % 8)) & 7; if map.len() < off + pad_bytes { return None; } off += pad_bytes;
                let entries_off = off; let entry_stride = if off_is_u32 { 12 } else { 16 }; let total_bytes = count.checked_mul(entry_stride)?; if map.len() < off + total_bytes + 8 { return None; } off += total_bytes;
                let sum_read = u64::from_le_bytes(map[off..off + 8].try_into().ok()?);
                use xxhash_rust::xxh3::Xxh3; let mut hasher = Xxh3::new(); hasher.update(&map[..off]); if hasher.digest() != sum_read { return None; }
                crate::metrics::RMI_INDEX_SIZE_BYTES.set(meta_len as f64);
                crate::metrics::RMI_INDEX_LEAVES.set(lvs.len() as f64);
                if let Some(max) = lvs.iter().map(|l| l.epsilon).max() { crate::metrics::RMI_EPSILON_MAX.set(max as f64); }
                for leaf in &lvs { crate::metrics::RMI_EPSILON_HISTOGRAM.observe(leaf.epsilon as f64); }
                let router = Self::build_router(&lvs);
                let backing = RmiBacking::MmapAos { mmap: map, entries_off, count, off_is_u32, entry_stride };
                return Some(Self { delta: BTreeMap::new(), leaves: lvs, router, backing });
            }
            _ => None,
        }
    }

    // --- probe helpers ---
    #[inline(always)]
    fn prefetch_window(&self, _idx: usize) {
        #[cfg(all(target_arch = "x86_64", target_feature = "sse"))]
        unsafe {
            use core::arch::x86_64::_mm_prefetch;
            use core::arch::x86_64::_MM_HINT_T0;
            match &self.backing {
                RmiBacking::Mmap { mmap, keys_off, .. } => {
                    let base = mmap.as_ptr().add(*keys_off + _idx * std::mem::size_of::<u64>()) as *const i8;
                    _mm_prefetch(base, _MM_HINT_T0);
                    _mm_prefetch(base.add(64), _MM_HINT_T0);
                    _mm_prefetch(base.add(128), _MM_HINT_T0);
                }
                RmiBacking::MmapAos { mmap, entries_off, entry_stride, .. } => {
                    let base = mmap.as_ptr().add(*entries_off + _idx * *entry_stride) as *const i8;
                    _mm_prefetch(base, _MM_HINT_T0);
                    _mm_prefetch(base.add(64), _MM_HINT_T0);
                    _mm_prefetch(base.add(128), _MM_HINT_T0);
                }
                RmiBacking::Owned { sorted_keys, .. } => {
                    let ptr = (sorted_keys.as_ptr() as *const u8).wrapping_add(_idx * std::mem::size_of::<u64>()) as *const i8;
                    _mm_prefetch(ptr, _MM_HINT_T0);
                    _mm_prefetch(ptr.add(64), _MM_HINT_T0);
                    _mm_prefetch(ptr.add(128), _MM_HINT_T0);
                }
            }
        }
    }

    #[inline(always)]
    fn small_window_probe(&self, key: u64, lo: usize, hi: usize) -> Option<u64> {
        let len = hi + 1 - lo;
        if len == 0 { return None; }
        #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
        unsafe {
            // SoA vectorized compare when contiguous u64 keys are available
            if let RmiBacking::Mmap { mmap, keys_off, .. } = &self.backing {
                let mut i = 0usize;
                let base = mmap.as_ptr().add(*keys_off) as *const u64;
                let target = core::arch::x86_64::_mm256_set1_epi64x(key as i64);
                while i + 4 <= len {
                    let ptr = base.add(lo + i) as *const core::arch::x86_64::__m256i;
                    let v = core::arch::x86_64::_mm256_loadu_si256(ptr);
                    let cmp = core::arch::x86_64::_mm256_cmpeq_epi64(v, target);
                    let mask = core::arch::x86_64::_mm256_movemask_pd(core::mem::transmute::<_, core::arch::x86_64::__m256d>(cmp));
                    if mask != 0 {
                        let tz = mask.trailing_zeros() as usize;
                        let idx = lo + i + tz;
                        return Some(self.off_at(idx));
                    }
                    i += 4;
                }
                for j in i..len { let idx = lo + j; if self.key_at(idx) == key { return Some(self.off_at(idx)); } }
                return None;
            }
            if let RmiBacking::Owned { sorted_keys, .. } = &self.backing {
                let mut i = 0usize;
                let base = sorted_keys.as_ptr();
                let target = core::arch::x86_64::_mm256_set1_epi64x(key as i64);
                while i + 4 <= len {
                    let ptr = base.add(lo + i) as *const core::arch::x86_64::__m256i;
                    let v = core::arch::x86_64::_mm256_loadu_si256(ptr);
                    let cmp = core::arch::x86_64::_mm256_cmpeq_epi64(v, target);
                    let mask = core::arch::x86_64::_mm256_movemask_pd(core::mem::transmute::<_, core::arch::x86_64::__m256d>(cmp));
                    if mask != 0 {
                        let tz = mask.trailing_zeros() as usize;
                        let idx = lo + i + tz;
                        return Some(self.off_at(idx));
                    }
                    i += 4;
                }
                for j in i..len { let idx = lo + j; if self.key_at(idx) == key { return Some(self.off_at(idx)); } }
                return None;
            }
        }
        // Fallback scalar unrolled
        let mut i = 0usize;
        while i < len {
            let idx = lo + i; if self.key_at(idx) == key { return Some(self.off_at(idx)); }
            if i + 1 < len { let idx = lo + i + 1; if self.key_at(idx) == key { return Some(self.off_at(idx)); } }
            if i + 2 < len { let idx = lo + i + 2; if self.key_at(idx) == key { return Some(self.off_at(idx)); } }
            if i + 3 < len { let idx = lo + i + 3; if self.key_at(idx) == key { return Some(self.off_at(idx)); } }
            i += 4;
        }
        None
    }

    // Continue with the existing find_leaf_index, predict_window, debug_* and predict_get implementations
    fn find_leaf_index(&self, key: u64) -> Option<usize> {
        if self.leaves.is_empty() { return None; }
        // O(1) router via high-16 bits
        let li = self.router[((key >> 48) & 0xFFFF) as usize] as usize;
        Some(li)
    }

    fn predict_window(&self, leaf: &RmiLeafMeta, key: u64) -> (usize, usize) {
        if leaf.len == 0 { return (leaf.start as usize, leaf.start as usize); }
        let pred = leaf.slope as f64 * (key as f64) + leaf.intercept as f64;
        let center = pred.round() as i64;
        let start = leaf.start as i64; let end = (leaf.start + leaf.len - 1) as i64;
        let eps = leaf.epsilon as i64;
        let lo = std::cmp::max(start, center - eps) as usize;
        let hi = std::cmp::min(end, center + eps) as usize;
        (lo, hi)
    }

    // Public debug helpers for benchmarks
    pub fn debug_find_leaf_index(&self, key: u64) -> Option<usize> { self.find_leaf_index(key) }
    pub fn debug_predict_clamp(&self, key: u64) -> Option<(usize, usize)> {
        let li = self.find_leaf_index(key)?; let leaf = &self.leaves[li];
        let (mut lo, mut hi) = self.predict_window(leaf, key);
        if self.count() == 0 { return None; }
        let max_idx = self.count() - 1; lo = lo.min(max_idx); hi = hi.min(max_idx); Some((lo, hi))
    }
    pub fn debug_probe_only(&self, key: u64, lo: usize, hi: usize) -> Option<u64> {
        if self.count() == 0 { return None; }
        let mut l = lo; let mut r = hi;
        while l <= r {
            let m = l + ((r - l) >> 1);
            let km = self.key_at(m);
            if km < key { l = m + 1; }
            else if km > key { if m == 0 { break; } r = m - 1; }
            else { return Some(self.off_at(m)); }
        }
        None
    }

    pub fn predict_get(&self, key: &u64) -> Option<u64> {
        if self.count() == 0 {
            crate::metrics::RMI_PROBE_LEN.observe(0.0);
            crate::metrics::RMI_MISPREDICTS_TOTAL.inc();
            return None;
        }
        let Some(li) = self.find_leaf_index(*key) else {
            crate::metrics::RMI_PROBE_LEN.observe(0.0);
            crate::metrics::RMI_MISPREDICTS_TOTAL.inc();
            return None;
        };
        let leaf = &self.leaves[li];
        let (mut lo, mut hi) = self.predict_window(leaf, *key);
        if lo > hi { crate::metrics::RMI_PROBE_LEN.observe(0.0); crate::metrics::RMI_MISPREDICTS_TOTAL.inc(); return None; }
        let max_idx = self.count().saturating_sub(1); lo = lo.min(max_idx); hi = hi.min(max_idx);
        // Prefetch predicted window start
        self.prefetch_window(lo);
        let mut steps: u32 = 0;
        let win = hi + 1 - lo;
        if win <= 16 {
            let res = self.small_window_probe(*key, lo, hi);
            crate::metrics::RMI_PROBE_LEN.observe((steps + win as u32) as f64);
            return res;
        }
        // Branch-minimized binary search
        let mut l = lo; let mut r = hi;
        while l <= r {
            steps += 1;
            let m = l + ((r - l) >> 1);
            let km = self.key_at(m);
            if km < *key { l = m + 1; }
            else if km > *key { if m == 0 { break; } r = m - 1; }
            else { crate::metrics::RMI_PROBE_LEN.observe(steps as f64); return Some(self.off_at(m)); }
        }
        crate::metrics::RMI_PROBE_LEN.observe(steps as f64);
        crate::metrics::RMI_MISPREDICTS_TOTAL.inc();
        None
    }
}

#[cfg(feature = "learned-index")]
impl Default for RmiIndex {
    fn default() -> Self { Self::new() }
}

pub enum PrimaryIndex {
    BTree(BTreeIndex),
    #[cfg(feature = "learned-index")]
    Rmi(RmiIndex),
}

impl PrimaryIndex {
    pub fn new_btree() -> Self { PrimaryIndex::BTree(BTreeIndex::new()) }

    #[cfg(feature = "learned-index")]
    pub fn new_rmi() -> Self { PrimaryIndex::Rmi(RmiIndex::new()) }

    pub fn insert(&mut self, key: u64, offset: u64) {
        match self {
            PrimaryIndex::BTree(b) => b.insert(key, offset),
            #[cfg(feature = "learned-index")]
            PrimaryIndex::Rmi(r) => { r.insert_delta(key, offset); }
        }
    }

    pub fn get(&self, key: &u64) -> Option<u64> {
        match self {
            PrimaryIndex::BTree(b) => {
                let res = b.get(key);
                if res.is_some() { crate::metrics::BTREE_READS_TOTAL.inc(); }
                res
            }
            #[cfg(feature = "learned-index")]
            PrimaryIndex::Rmi(r) => {
                if let Some(v) = r.delta_get(key) {
                    crate::metrics::RMI_HITS_TOTAL.inc();
                    crate::metrics::RMI_READS_TOTAL.inc();
                    Some(v)
                } else {
                    let timer = crate::metrics::RMI_LOOKUP_LATENCY_SECONDS.start_timer();
                    let res = r.predict_get(key);
                    timer.observe_duration();
                    if res.is_some() { crate::metrics::RMI_HITS_TOTAL.inc(); crate::metrics::RMI_READS_TOTAL.inc(); }
                    else { crate::metrics::RMI_MISSES_TOTAL.inc(); }
                    res
                }
            }
        }
    }
}
