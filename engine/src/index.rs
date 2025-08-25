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
    // Original leaf vec for diagnostics/testing
    leaves: Vec<RmiLeafMeta>,
    // Router: dynamic size 2^router_bits
    router: Vec<u32>,
    router_bits: u8,
    // Backing for key/offset arrays
    backing: RmiBacking,
    // Fast predictor state (not persisted)
    fx_shift: u32,
    fx_m: Vec<i128>,   // per-leaf fixed-point slope
    fx_b: Vec<i64>,    // per-leaf integer intercept
    // SoA leaf metadata for hot path
    #[allow(dead_code)]
    leaf_key_min: Vec<u64>,
    #[allow(dead_code)]
    leaf_key_max: Vec<u64>,
    #[allow(dead_code)]
    leaf_slope: Vec<f32>,
    #[allow(dead_code)]
    leaf_intercept: Vec<f32>,
    leaf_epsilon: Vec<u32>,
    leaf_start: Vec<u64>,
    leaf_len: Vec<u64>,
}

#[cfg(feature = "learned-index")]
const RMI_MAGIC: [u8; 8] = *b"KYRO_RMI";

#[cfg(feature = "learned-index")]
impl RmiIndex {
    pub fn new() -> Self {
        let bits = std::env::var("KYRODB_RMI_ROUTER_BITS")
            .ok()
            .and_then(|s| s.parse::<u8>().ok())
            .map(|b| b.clamp(8, 24))
            .unwrap_or(16);
        Self {
            delta: BTreeMap::new(),
            leaves: Vec::new(),
            router: vec![u32::MAX; 1usize << bits],
            router_bits: bits,
            backing: RmiBacking::Owned {
                sorted_keys: Vec::new(),
                sorted_offsets: Vec::new(),
            },
            fx_shift: 32u32,
            fx_m: Vec::new(),
            fx_b: Vec::new(),
            leaf_key_min: Vec::new(),
            leaf_key_max: Vec::new(),
            leaf_slope: Vec::new(),
            leaf_intercept: Vec::new(),
            leaf_epsilon: Vec::new(),
            leaf_start: Vec::new(),
            leaf_len: Vec::new(),
        }
    }

    // Build router from leaves, using top router_bits of key
    fn build_router(leaves: &[RmiLeafMeta], bits: u8) -> Vec<u32> {
        let size = 1usize << bits;
        let mut r = vec![u32::MAX; size];
        if leaves.is_empty() { return r; }
        let shift = 64 - bits as u32;
        for (i, leaf) in leaves.iter().enumerate() {
            let lo = (leaf.key_min >> shift) as usize;
            let hi = (leaf.key_max >> shift) as usize;
            let end = hi.min(size - 1);
            for idx in lo..=end { r[idx] = i as u32; }
        }
        // Fill holes by carrying nearest previous leaf, then forward fill
        let mut last = 0u32;
        for i in 0..size {
            if r[i] == u32::MAX { r[i] = last; } else { last = r[i]; }
        }
        let mut last = *r.last().unwrap();
        for i in (0..size).rev() {
            if r[i] == u32::MAX { r[i] = last; } else { last = r[i]; }
        }
        r
    }

    #[inline]
    fn router_index(&self, key: u64) -> usize {
        let shift = 64 - self.router_bits as u32;
        ((key >> shift) & ((1u64 << self.router_bits) - 1)) as usize
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
                u64::from_le(std::ptr::read_unaligned(ptr))
            },
            RmiBacking::MmapAos { mmap, entries_off, entry_stride, .. } => unsafe {
                let ptr = mmap.as_ptr().add(*entries_off + idx * *entry_stride) as *const u64;
                u64::from_le(std::ptr::read_unaligned(ptr))
            },
        }
    }

    #[inline]
    fn off_at(&self, idx: usize) -> u64 {
        match &self.backing {
            RmiBacking::Owned { sorted_offsets, .. } => sorted_offsets[idx],
            RmiBacking::Mmap { mmap, offs_off, .. } => unsafe {
                let ptr = mmap.as_ptr().add(*offs_off + idx * std::mem::size_of::<u64>()) as *const u64;
                u64::from_le(std::ptr::read_unaligned(ptr))
            },
            RmiBacking::MmapAos { mmap, entries_off, entry_stride, off_is_u32, .. } => unsafe {
                let ptr = mmap.as_ptr().add(*entries_off + idx * *entry_stride + 8);
                if *off_is_u32 {
                    u64::from(u32::from_le(std::ptr::read_unaligned(ptr as *const u32)))
                } else {
                    u64::from_le(std::ptr::read_unaligned(ptr as *const u64))
                }
            },
        }
    }

    // add delta helpers missing after refactor
    pub fn insert_delta(&mut self, key: u64, offset: u64) { self.delta.insert(key, offset); }
    pub fn delta_get(&self, key: &u64) -> Option<u64> { self.delta.get(key).copied() }
    /// Snapshot current delta updates as pairs for migration/inspection
    pub fn delta_pairs(&self) -> Vec<(u64, u64)> {
        self.delta.iter().map(|(k, v)| (*k, *v)).collect()
    }

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

    // v5 writer: interleave entries (key, off) with optional u32 packing; adaptive leaves if configured.
    pub fn write_from_pairs_v5(path: &std::path::Path, pairs: &[(u64, u64)], pack_u32: bool) -> std::io::Result<()> {
        use std::io::Write as _;
        let mut buf: Vec<(u64, u64)> = pairs.to_vec(); buf.sort_by_key(|(k, _)| *k);
        let n = buf.len(); let total_keys = n as u64; let keys: Vec<u64> = buf.iter().map(|(k, _)| *k).collect();
        let env_max_eps = std::env::var("KYRODB_RMI_MAX_EPS").ok().and_then(|s| s.parse::<u32>().ok());
        let leaves = if let Some(max_eps) = env_max_eps { let min_leaf = std::env::var("KYRODB_RMI_MIN_LEAF").ok().and_then(|s| s.parse::<usize>().ok()).unwrap_or(256); Self::build_leaves_adaptive(&keys, max_eps, min_leaf) } else { let target_leaf = std::env::var("KYRODB_RMI_TARGET_LEAF").ok().and_then(|s| s.parse::<usize>().ok()).filter(|&v| v > 0).unwrap_or(1024usize); let num_leaves = (n.div_ceil(target_leaf)) as usize; let mut lvs: Vec<RmiLeafMeta> = Vec::with_capacity(num_leaves); for li in 0..num_leaves { let start = (li * n) / num_leaves; let end = (((li + 1) * n) / num_leaves).max(start + 1); let (m, b, eps) = Self::fit_leaf(&keys, start, end); lvs.push(RmiLeafMeta { key_min: keys[start], key_max: keys[end - 1], slope: m, intercept: b, epsilon: eps, start: start as u64, len: (end - start) as u64 }); } lvs };
        // write header
        let mut f = std::fs::File::create(path)?;
        let mut header = Vec::new(); header.extend_from_slice(&RMI_MAGIC); header.push(5u8); header.extend_from_slice(&[0u8; 3]); header.extend_from_slice(&(leaves.len() as u32).to_le_bytes()); header.extend_from_slice(&total_keys.to_le_bytes()); header.push(if pack_u32 { 4u8 } else { 8u8 }); header.extend_from_slice(&[0u8; 7]); f.write_all(&header)?;
        for leaf in &leaves { f.write_all(&leaf.key_min.to_le_bytes())?; f.write_all(&leaf.key_max.to_le_bytes())?; f.write_all(&leaf.slope.to_le_bytes())?; f.write_all(&leaf.intercept.to_le_bytes())?; f.write_all(&leaf.epsilon.to_le_bytes())?; f.write_all(&leaf.start.to_le_bytes())?; f.write_all(&leaf.len.to_le_bytes())?; }
        { use std::io::Seek; let pos = f.stream_position()?; let pad = ((8 - (pos % 8)) % 8) as usize; if pad > 0 { f.write_all(&vec![0u8; pad])?; } }
        // write entries (keep 16-byte stride by padding u32 offsets)
        for &(_k, _off) in &buf {
            f.write_all(&_k.to_le_bytes())?;
            if pack_u32 {
                f.write_all(&(_off as u32).to_le_bytes())?;
                // 4-byte pad to keep 16-byte stride
                f.write_all(&0u32.to_le_bytes())?;
            } else {
                f.write_all(&_off.to_le_bytes())?;
            }
        }
        use xxhash_rust::xxh3::Xxh3; f.flush()?; let all = std::fs::File::open(path)?; let len_before = all.metadata()?.len(); let mut hasher = Xxh3::new(); { use std::io::Read; let mut rdr = std::io::BufReader::new(&all); let mut b = vec![0u8; 64 * 1024]; let mut remaining = len_before as usize; while remaining > 0 { let to_read = b.len().min(remaining); rdr.read_exact(&mut b[..to_read])?; hasher.update(&b[..to_read]); remaining -= to_read; } } let sum = hasher.digest(); drop(all); f.write_all(&sum.to_le_bytes())?; f.flush()?; Ok(())
    }

    /// Auto-select writer based on env and data:
    /// - KYRO_RMI_FORMAT: "5"|"v5"|"aos" -> v5 (default), "4"|"v4"|"soa" -> v4
    /// - KYRO_RMI_PACK_U32: truthy enables u32 packing when offsets fit (ignored for v4)
    pub fn write_from_pairs_auto(path: &std::path::Path, pairs: &[(u64, u64)]) -> std::io::Result<()> {
        let fmt = std::env::var("KYRO_RMI_FORMAT").unwrap_or_else(|_| "5".to_string());
        let fmt_lc = fmt.to_ascii_lowercase();
        let use_v5 = matches!(fmt_lc.as_str(), "5" | "v5" | "aos" | "aos5" | "aosv5" | "default");
        if !use_v5 {
            return Self::write_from_pairs(path, pairs);
        }
        let max_off = pairs.iter().map(|&(_, o)| o).max().unwrap_or(0);
        let pack_env = std::env::var("KYRODB_RMI_PACK_U32").ok();
        let pack_req = pack_env.as_deref().map(|s| {
            let s = s.trim().to_ascii_lowercase();
            s == "1" || s == "true" || s == "yes" || s == "y" || s == "on"
        }).unwrap_or(false);
        let pack_u32 = pack_req && max_off <= u32::MAX as u64;
        Self::write_from_pairs_v5(path, pairs, pack_u32)
    }

    pub fn load_from_file(path: &std::path::Path) -> Option<Self> {
        use std::io::Read;
        let meta_len = std::fs::metadata(path).ok()?.len();
        let mut f = std::fs::File::open(path).ok()?;
        let mut magic = [0u8; 8]; f.read_exact(&mut magic).ok()?; if magic != RMI_MAGIC { return None; }
        let mut ver = [0u8; 1]; f.read_exact(&mut ver).ok()?; let version = ver[0]; let mut pad = [0u8; 3]; let _ = f.read_exact(&mut pad);
        let bits = std::env::var("KYRODB_RMI_ROUTER_BITS").ok().and_then(|s| s.parse::<u8>().ok()).map(|b| b.clamp(8, 24)).unwrap_or(16);
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
                let router = Self::build_router(&lvs, bits);
                // SoA repack + fixed-point
                let mut leaf_key_min = Vec::with_capacity(lvs.len());
                let mut leaf_key_max = Vec::with_capacity(lvs.len());
                let mut leaf_slope = Vec::with_capacity(lvs.len());
                let mut leaf_intercept = Vec::with_capacity(lvs.len());
                let mut leaf_epsilon = Vec::with_capacity(lvs.len());
                let mut leaf_start = Vec::with_capacity(lvs.len());
                let mut leaf_len = Vec::with_capacity(lvs.len());
                let mut fx_m = Vec::with_capacity(lvs.len());
                let mut fx_b = Vec::with_capacity(lvs.len());
                let fx_shift = 32u32;
                for leaf in &lvs {
                    leaf_key_min.push(leaf.key_min);
                    leaf_key_max.push(leaf.key_max);
                    leaf_slope.push(leaf.slope);
                    leaf_intercept.push(leaf.intercept);
                    leaf_epsilon.push(leaf.epsilon);
                    leaf_start.push(leaf.start);
                    leaf_len.push(leaf.len);
                    let m = (leaf.slope as f64 * ((1u128 << fx_shift) as f64)) as i128;
                    let b = leaf.intercept.round() as i64;
                    fx_m.push(m); fx_b.push(b);
                }
                // Optional mlock of leaf SoA pages
                #[cfg(all(unix, not(target_os = "macos")))]
                unsafe {
                    if std::env::var("KYRODB_RMI_MLOCK_LEAVES").ok().as_deref() == Some("1") {
                        use libc::{mlock, munlock};
                        let ptr = leaf_key_min.as_ptr() as *const _ as *const libc::c_void;
                        let len_bytes = leaf_len.len().saturating_mul(std::mem::size_of::<u64>());
                        let _ = mlock(ptr, len_bytes);
                        let _ = munlock(ptr, len_bytes); // leave it advisory; remove this line to keep locked
                    }
                }
                return Some(Self {
                    delta: BTreeMap::new(),
                    leaves: lvs,
                    router,
                    router_bits: bits,
                    backing,
                    fx_shift,
                    fx_m,
                    fx_b,
                    leaf_key_min,
                    leaf_key_max,
                    leaf_slope,
                    leaf_intercept,
                    leaf_epsilon,
                    leaf_start,
                    leaf_len,
                });
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
                // v5 AoS: interleaved entries starting here with fixed 16-byte stride
                let entries_off = off;
                let entry_stride: usize = 16;
                let total_bytes = count.checked_mul(entry_stride)?;
                if map.len() < off + total_bytes + 8 { return None; }
                off += total_bytes;
                let sum_read = u64::from_le_bytes(map[off..off + 8].try_into().ok()?);
                use xxhash_rust::xxh3::Xxh3; let mut hasher = Xxh3::new(); hasher.update(&map[..off]); if hasher.digest() != sum_read { return None; }
                crate::metrics::RMI_INDEX_SIZE_BYTES.set(meta_len as f64);
                crate::metrics::RMI_INDEX_LEAVES.set(lvs.len() as f64);
                if let Some(max) = lvs.iter().map(|l| l.epsilon).max() { crate::metrics::RMI_EPSILON_MAX.set(max as f64); }
                for leaf in &lvs { crate::metrics::RMI_EPSILON_HISTOGRAM.observe(leaf.epsilon as f64); }
                let router = Self::build_router(&lvs, bits);
                // SoA repack + fixed-point
                let mut leaf_key_min = Vec::with_capacity(lvs.len());
                let mut leaf_key_max = Vec::with_capacity(lvs.len());
                let mut leaf_slope = Vec::with_capacity(lvs.len());
                let mut leaf_intercept = Vec::with_capacity(lvs.len());
                let mut leaf_epsilon = Vec::with_capacity(lvs.len());
                let mut leaf_start = Vec::with_capacity(lvs.len());
                let mut leaf_len = Vec::with_capacity(lvs.len());
                let mut fx_m = Vec::with_capacity(lvs.len());
                let mut fx_b = Vec::with_capacity(lvs.len());
                let fx_shift = 32u32;
                for leaf in &lvs {
                    leaf_key_min.push(leaf.key_min);
                    leaf_key_max.push(leaf.key_max);
                    leaf_slope.push(leaf.slope);
                    leaf_intercept.push(leaf.intercept);
                    leaf_epsilon.push(leaf.epsilon);
                    leaf_start.push(leaf.start);
                    leaf_len.push(leaf.len);
                    let m = (leaf.slope as f64 * ((1u128 << fx_shift) as f64)) as i128;
                    let b = leaf.intercept.round() as i64;
                    fx_m.push(m); fx_b.push(b);
                }
                // Optional mlock of leaf SoA pages
                #[cfg(all(unix, not(target_os = "macos")))]
                unsafe {
                    if std::env::var("KYRODB_RMI_MLOCK_LEAVES").ok().as_deref() == Some("1") {
                        use libc::{mlock, munlock};
                        let ptr = leaf_key_min.as_ptr() as *const _ as *const libc::c_void;
                        let len_bytes = leaf_len.len().saturating_mul(std::mem::size_of::<u64>());
                        let _ = mlock(ptr, len_bytes);
                        let _ = munlock(ptr, len_bytes);
                    }
                }
                let backing = RmiBacking::MmapAos { mmap: map, entries_off, count, off_is_u32, entry_stride };
                return Some(Self {
                    delta: BTreeMap::new(),
                    leaves: lvs,
                    router,
                    router_bits: bits,
                    backing,
                    fx_shift,
                    fx_m,
                    fx_b,
                    leaf_key_min,
                    leaf_key_max,
                    leaf_slope,
                    leaf_intercept,
                    leaf_epsilon,
                    leaf_start,
                    leaf_len,
                });
            }
            _ => None,
        }
    }

    // Use SoA predictor data in fast clamp
    #[inline(always)]
    fn predict_clamp_fast(&self, leaf_id: usize, key: u64) -> (usize, usize) {
        let m = unsafe { *self.fx_m.get_unchecked(leaf_id) };
        let b = unsafe { *self.fx_b.get_unchecked(leaf_id) };
        let eps = unsafe { *self.leaf_epsilon.get_unchecked(leaf_id) } as i64;
        let start = unsafe { *self.leaf_start.get_unchecked(leaf_id) } as i64;
        let len = unsafe { *self.leaf_len.get_unchecked(leaf_id) } as i64;
        let pred = (((m * (key as i128)) >> self.fx_shift) as i64) + b;
        let leaf_lo = start;
        let leaf_hi = start + len - 1;
        let mut lo = (pred - eps).clamp(leaf_lo, leaf_hi);
        let mut hi = (pred + eps).clamp(leaf_lo, leaf_hi);
        // widen a touch to absorb rounding errors
        lo = (lo - 1).max(leaf_lo);
        hi = (hi + 1).min(leaf_hi);
        (lo as usize, hi as usize)
    }

    // Router lookup using dynamic bits
    fn find_leaf_index(&self, key: u64) -> Option<usize> {
        if self.leaf_len.is_empty() { return None; }
        Some(self.router[self.router_index(key)] as usize)
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

    // AVX-512 paths (runtime detection)
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx512f")]
    unsafe fn small_window_probe_aos_avx512(&self, entries_off: usize, stride: usize, key: u64, lo: usize, len: usize) -> Option<usize> {
        if let RmiBacking::MmapAos { mmap, .. } = &self.backing {
            let base = mmap.as_ptr().add(entries_off) as *const i8;
            let mut i = 0usize;
            let target = core::arch::x86_64::_mm512_set1_epi64(key as i64);
            while i + 8 <= len {
                let idx0 = ((lo + i + 0) * stride) as i64;
                let idx1 = ((lo + i + 1) * stride) as i64;
                let idx2 = ((lo + i + 2) * stride) as i64;
                let idx3 = ((lo + i + 3) * stride) as i64;
                let idx4 = ((lo + i + 4) * stride) as i64;
                let idx5 = ((lo + i + 5) * stride) as i64;
                let idx6 = ((lo + i + 6) * stride) as i64;
                let idx7 = ((lo + i + 7) * stride) as i64;
                let idx_vec = core::arch::x86_64::_mm512_set_epi64(idx7, idx6, idx5, idx4, idx3, idx2, idx1, idx0);
                let gathered = core::arch::x86_64::_mm512_i64gather_epi64(idx_vec, base as *const i64, 1);
                let cmp = core::arch::x86_64::_mm512_cmpeq_epi64_mask(gathered, target);
                if cmp != 0 {
                    let tz = cmp.trailing_zeros() as usize; // lane index 0..7
                    return Some(lo + i + tz);
                }
                i += 8;
            }
        }
        None
    }

    #[inline(always)]
    fn small_window_probe(&self, key: u64, lo: usize, hi: usize) -> Option<u64> {
        let len = hi + 1 - lo;
        if len == 0 { return None; }
        // x86_64: runtime feature detection in descending order
        #[cfg(target_arch = "x86_64")]
        unsafe {
            if std::arch::is_x86_feature_detected!("avx512f") {
                if let RmiBacking::MmapAos { entries_off, entry_stride, .. } = &self.backing {
                    if let Some(idx) = self.small_window_probe_aos_avx512(*entries_off, *entry_stride, key, lo, len) {
                        return Some(self.off_at(idx));
                    }
                }
            }
            if std::arch::is_x86_feature_detected!("avx2") {
                match &self.backing {
                    RmiBacking::Mmap { keys_off, .. } => {
                        if let Some(idx) = self.small_window_probe_soa_avx2(*keys_off, key, lo, len) {
                            return Some(self.off_at(idx));
                        }
                    }
                    RmiBacking::Owned { .. } => {
                        if let Some(idx) = self.small_window_probe_soa_avx2(0usize, key, lo, len) {
                            return Some(self.off_at(idx));
                        }
                    }
                    RmiBacking::MmapAos { entries_off, entry_stride, .. } => {
                        if let Some(idx) = self.small_window_probe_aos_avx2(*entries_off, *entry_stride, key, lo, len) {
                            return Some(self.off_at(idx));
                        }
                    }
                }
            }
        }
        // aarch64 NEON
        #[cfg(target_arch = "aarch64")]
        unsafe {
            if std::arch::is_aarch64_feature_detected!("neon") {
                if let RmiBacking::MmapAos { .. } = &self.backing {
                    if let Some(idx) = self.small_window_probe_aos_neon(key, lo, len) {
                        return Some(self.off_at(idx));
                    }
                }
            }
        }
        // Scalar fallback
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

    // AVX2 helpers
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx2")]
    unsafe fn small_window_probe_soa_avx2(&self, keys_off: usize, key: u64, lo: usize, len: usize) -> Option<usize> {
        if let RmiBacking::Mmap { mmap, .. } = &self.backing {
            let mut i = 0usize;
            let base = mmap.as_ptr().add(keys_off) as *const u64;
            let target = core::arch::x86_64::_mm256_set1_epi64x(key as i64);
            while i + 4 <= len {
                let ptr = base.add(lo + i) as *const core::arch::x86_64::__m256i;
                let v = core::arch::x86_64::_mm256_loadu_si256(ptr);
                let cmp = core::arch::x86_64::_mm256_cmpeq_epi64(v, target);
                let mask = core::arch::x86_64::_mm256_movemask_pd(core::mem::transmute::<_, core::arch::x86_64::__m256d>(cmp));
                if mask != 0 { let tz = mask.trailing_zeros() as usize; return Some(lo + i + tz); }
                i += 4;
            }
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
                if mask != 0 { let tz = mask.trailing_zeros() as usize; return Some(lo + i + tz); }
                i += 4;
            }
        }
        None
    }

    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx2")]
    unsafe fn small_window_probe_aos_avx2(&self, entries_off: usize, stride: usize, key: u64, lo: usize, len: usize) -> Option<usize> {
        if let RmiBacking::MmapAos { mmap, .. } = &self.backing {
            let base = mmap.as_ptr().add(entries_off) as *const i8;
            let mut i = 0usize;
            let target = core::arch::x86_64::_mm256_set1_epi64x(key as i64);
            while i + 4 <= len {
                // byte offsets for gather
                let idx0 = ((lo + i + 0) * stride) as i64;
                let idx1 = ((lo + i + 1) * stride) as i64;
                let idx2 = ((lo + i + 2) * stride) as i64;
                let idx3 = ((lo + i + 3) * stride) as i64;
                let idx_vec = core::arch::x86_64::_mm256_set_epi64x(idx3, idx2, idx1, idx0);
                let gathered = core::arch::x86_64::_mm256_i64gather_epi64(base as *const i64, idx_vec, 1);
                let cmp = core::arch::x86_64::_mm256_cmpeq_epi64(gathered, target);
                let mask = core::arch::x86_64::_mm256_movemask_pd(core::mem::transmute::<_, core::arch::x86_64::__m256d>(cmp));
                if mask != 0 {
                    let tz = mask.trailing_zeros() as usize; // 0..3, maps to lane
                    return Some(lo + i + tz);
                }
                i += 4;
            }
        }
        None
    }

    // aarch64 NEON helper (limited: pairwise compare by building vector lanes)
    #[cfg(target_arch = "aarch64")]
    #[target_feature(enable = "neon")]
    unsafe fn small_window_probe_aos_neon(&self, key: u64, lo: usize, len: usize) -> Option<usize> {
        use core::arch::aarch64::*;
        let keyv = vdupq_n_u64(key);
        let mut i = 0usize;
        while i + 2 <= len {
            let k0 = self.key_at(lo + i);
            let k1 = self.key_at(lo + i + 1);
            let pair = vsetq_lane_u64(k0, vdupq_n_u64(0), 0);
            let pair = vsetq_lane_u64(k1, pair, 1);
            let cmp = vceqq_u64(pair, keyv);
            let m0 = vgetq_lane_u64(cmp, 0);
            let m1 = vgetq_lane_u64(cmp, 1);
            if m0 == u64::MAX { return Some(lo + i); }
            if m1 == u64::MAX { return Some(lo + i + 1); }
            i += 2;
        }
        while i < len {
            let idx = lo + i; if self.key_at(idx) == key { return Some(idx); }
            i += 1;
        }
        None
    }

    // Debug helpers for benches
    #[inline]
    pub fn debug_find_leaf_index(&self, key: u64) -> Option<usize> {
        self.find_leaf_index(key)
    }

    #[inline]
    pub fn debug_predict_clamp(&self, key: u64) -> Option<(usize, usize)> {
        let leaf = self.find_leaf_index(key)?;
        Some(self.predict_clamp_fast(leaf, key))
    }

    #[inline]
    pub fn debug_probe_only(&self, key: u64, lo: usize, hi: usize) -> Option<u64> {
        self.small_window_probe(key, lo, hi)
    }

    // In predict_get, swap to the fast predictor and keep your SIMD + fallback search:
    #[inline(always)]
    pub fn predict_get(&self, key: &u64) -> Option<u64> {
        if self.count() == 0 { return None; }
        let leaf_id = self.find_leaf_index(*key)?;
        let (lo, hi) = self.predict_clamp_fast(leaf_id, *key);
        self.prefetch_window(lo);
        if let Some(v) = self.small_window_probe(*key, lo, hi) {
            // Record a short probe length (SIMD found within window)
            crate::metrics::RMI_PROBE_LEN.observe(1.0);
            return Some(v);
        }
        let mut l = lo; let mut r = hi; let mut steps: u32 = 0;
        while l <= r {
            steps = steps.saturating_add(1);
            let m = l + ((r - l) >> 1);
            let km = self.key_at(m);
            if km < *key { l = m + 1; }
            else if km > *key { if m == 0 { break; } r = m - 1; }
            else {
                crate::metrics::RMI_PROBE_LEN.observe(steps as f64);
                return Some(self.off_at(m));
            }
        }
        // Miss inside predicted window counts as a mispredict
        crate::metrics::RMI_PROBE_LEN.observe(steps.max(1) as f64);
        crate::metrics::RMI_MISPREDICTS_TOTAL.inc();
        None
    }

    // Warm RMI pages and metadata to reduce first-hit latency.
    pub fn warm(&self) {
        // 1) Touch router (tiny)
        let r = &self.router;
        if !r.is_empty() {
            let _ = r[0];
            let _ = r[(r.len() - 1) / 2];
            let _ = r[r.len() - 1];
        }
        // 2) Advise mmap’d regions and pre-touch a sample of pages
        match &self.backing {
            RmiBacking::Mmap { mmap, keys_off, offs_off, count } => unsafe {
                let base = mmap.as_ptr();
                let _len = mmap.len();
                #[cfg(target_os = "linux")]
                {
                    let _ = libc::madvise(base as *mut _, mmap.len(), libc::MADV_WILLNEED);
                    let _ = libc::madvise(base as *mut _, mmap.len(), libc::MADV_HUGEPAGE);
                }
                let kptr = base.add(*keys_off);
                let optr = base.add(*offs_off);
                let step = (4096usize / core::mem::size_of::<u64>()).max(1);
                let n = *count;
                let mut i = 0usize;
                while i < n {
                    let _ = std::hint::black_box(std::ptr::read_unaligned(kptr.add(i * 8) as *const u64));
                    let _ = std::hint::black_box(std::ptr::read_unaligned(optr.add(i * 8) as *const u64));
                    i = i.saturating_add(step);
                }
            },
            RmiBacking::MmapAos { mmap, entries_off, entry_stride, count, .. } => unsafe {
                let base = mmap.as_ptr().add(*entries_off);
                let _len = mmap.len();
                #[cfg(target_os = "linux")]
                {
                    let _ = libc::madvise(mmap.as_ptr() as *mut _, mmap.len(), libc::MADV_WILLNEED);
                    let _ = libc::madvise(mmap.as_ptr() as *mut _, mmap.len(), libc::MADV_HUGEPAGE);
                }
                let step = (4096usize / *entry_stride).max(1);
                let mut i = 0usize;
                while i < *count {
                    let _ = std::hint::black_box(std::ptr::read_unaligned(base.add(i * *entry_stride) as *const u64));
                    i = i.saturating_add(step);
                }
            },
            RmiBacking::Owned { sorted_keys, sorted_offsets } => {
                let step = (4096usize / core::mem::size_of::<u64>()).max(1);
                let n = core::cmp::min(sorted_keys.len(), sorted_offsets.len());
                let mut i = 0usize;
                while i < n {
                    // SAFETY: we clamp i < n and step forward
                    let _ = std::hint::black_box(unsafe { *sorted_keys.get_unchecked(i) });
                    let _ = std::hint::black_box(unsafe { *sorted_offsets.get_unchecked(i) });
                    i = i.saturating_add(step);
                }
            }
        }
        // 3) Optionally pre-touch first key of each leaf window (cheap predictor)
        for leaf_id in (0..self.leaves.len()).step_by(64) {
            let lo = self.leaves[leaf_id].start as usize;
            if lo < self.count() {
                let _ = std::hint::black_box(self.key_at(lo));
            }
        }
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
                    return Some(v);
                }
                let timer = crate::metrics::RMI_LOOKUP_LATENCY_SECONDS.start_timer();
                let res = r.predict_get(key);
                timer.observe_duration();
                if res.is_some() {
                    crate::metrics::RMI_HITS_TOTAL.inc();
                    crate::metrics::RMI_READS_TOTAL.inc();
                } else {
                    crate::metrics::RMI_MISSES_TOTAL.inc();
                }
                res
            }
        }
    }
}