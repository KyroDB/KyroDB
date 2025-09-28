#![allow(
    clippy::needless_range_loop,
    clippy::unnecessary_cast,
    clippy::needless_return,
    clippy::large_enum_variant
)]
use std::collections::BTreeMap;

/// Basic trait for key → offset lookup.
pub trait Index {
    /// Insert a mapping from key to offset.
    fn insert(&mut self, key: u64, offset: u64);
    /// Lookup a key, returning the log offset if present.
    fn get(&self, key: &u64) -> Option<u64>;
}

/// Naive in-memory BTreeMap based index (single-column primary key).
#[derive(Default, Clone)]
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

#[derive(Clone)]
pub enum PrimaryIndex {
    BTree(BTreeIndex),
    #[cfg(feature = "learned-index")]
    AdaptiveRmi(std::sync::Arc<crate::adaptive_rmi::AdaptiveRMI>),
}

impl PrimaryIndex {
    pub fn new_btree() -> Self {
        PrimaryIndex::BTree(BTreeIndex::new())
    }

    #[cfg(feature = "learned-index")]
    pub fn new_default() -> Self {
        PrimaryIndex::AdaptiveRmi(std::sync::Arc::new(crate::adaptive_rmi::AdaptiveRMI::new()))
    }

    #[cfg(not(feature = "learned-index"))]
    pub fn new_default() -> Self {
        PrimaryIndex::BTree(BTreeIndex::new())
    }

    #[cfg(feature = "learned-index")]
    pub fn new_adaptive_rmi() -> Self {
        PrimaryIndex::AdaptiveRmi(std::sync::Arc::new(crate::adaptive_rmi::AdaptiveRMI::new()))
    }

    #[cfg(feature = "learned-index")]
    pub fn new_adaptive_rmi_from_pairs(pairs: &[(u64, u64)]) -> Self {
        PrimaryIndex::AdaptiveRmi(std::sync::Arc::new(
            crate::adaptive_rmi::AdaptiveRMI::build_from_pairs(pairs),
        ))
    }

    pub fn insert(&mut self, key: u64, offset: u64) {
        match self {
            PrimaryIndex::BTree(b) => b.insert(key, offset),
            #[cfg(feature = "learned-index")]
            PrimaryIndex::AdaptiveRmi(ar) => {
                if let Err(e) = ar.insert(key, offset) {
                    eprintln!("AdaptiveRMI insert error: {}", e);
                }
            }
        }
    }

    pub fn get(&self, key: &u64) -> Option<u64> {
        match self {
            PrimaryIndex::BTree(b) => {
                let res = b.get(key);
                if res.is_some() {
                    crate::metrics::BTREE_READS_TOTAL.inc();
                }
                res
            }
            #[cfg(feature = "learned-index")]
            PrimaryIndex::AdaptiveRmi(ar) => {
                let timer = crate::metrics::RMI_LOOKUP_LATENCY_SECONDS.start_timer();
                let res = ar.lookup(*key);
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

    #[cfg(feature = "learned-index")]
    pub fn is_adaptive_rmi(&self) -> bool {
        matches!(self, PrimaryIndex::AdaptiveRmi(_))
    }

    /// Start background maintenance for adaptive RMI (single instance protection)
    #[cfg(feature = "learned-index")]
    pub fn start_background_maintenance(&self) -> Option<tokio::task::JoinHandle<()>> {
        match self {
            PrimaryIndex::AdaptiveRmi(ar) => {
                // Only start if not already running - prevent multiple background tasks
                static MAINTENANCE_STARTED: std::sync::atomic::AtomicBool =
                    std::sync::atomic::AtomicBool::new(false);

                if MAINTENANCE_STARTED
                    .compare_exchange(
                        false,
                        true,
                        std::sync::atomic::Ordering::SeqCst,
                        std::sync::atomic::Ordering::SeqCst,
                    )
                    .is_ok()
                {
                    Some(ar.clone().start_background_maintenance())
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Get adaptive RMI statistics
    #[cfg(feature = "learned-index")]
    pub fn get_adaptive_stats(&self) -> Option<crate::adaptive_rmi::AdaptiveRMIStats> {
        match self {
            PrimaryIndex::AdaptiveRmi(ar) => Some(ar.get_stats()),
            _ => None,
        }
    }

    #[cfg(feature = "learned-index")]
    pub fn migrate_to_adaptive(&mut self) -> anyhow::Result<()> {
        match self {
            PrimaryIndex::AdaptiveRmi(_) => Ok(()),
            _ => Err(anyhow::anyhow!("Cannot migrate non-adaptive index")),
        }
    }

    /// Force merge of hot buffer (for testing/debugging)
    #[cfg(feature = "learned-index")]
    pub async fn force_merge(&self) -> anyhow::Result<()> {
        match self {
            PrimaryIndex::AdaptiveRmi(ar) => ar.merge_hot_buffer().await,
            _ => Ok(()),
        }
    }
}

#[cfg(feature = "learned-index")]
#[derive(Clone)]
pub struct RmiIndex {
    inner: std::sync::Arc<crate::adaptive_rmi::AdaptiveRMI>,
}

#[cfg(feature = "learned-index")]
impl RmiIndex {
    pub fn new() -> Self {
        Self {
            inner: std::sync::Arc::new(crate::adaptive_rmi::AdaptiveRMI::new()),
        }
    }

    pub fn write_from_pairs<P: AsRef<std::path::Path>>(
        path: P,
        pairs: &[(u64, u64)],
    ) -> std::io::Result<()> {
        let mut dedup = std::collections::BTreeMap::new();
        for &(key, offset) in pairs {
            dedup.insert(key, offset);
        }
        let ordered: Vec<(u64, u64)> = dedup.into_iter().collect();

        let mut file = std::fs::File::create(path)?;
        bincode::serialize_into(&mut file, &ordered)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err))
    }

    pub fn write_from_pairs_auto<P: AsRef<std::path::Path>>(
        path: P,
        pairs: &[(u64, u64)],
    ) -> std::io::Result<()> {
        Self::write_from_pairs(path, pairs)
    }

    pub fn load_from_file<P: AsRef<std::path::Path>>(path: P) -> Option<Self> {
        let file = std::fs::File::open(path).ok()?;
        let reader = std::io::BufReader::new(file);
        let pairs: Vec<(u64, u64)> = bincode::deserialize_from(reader).ok()?;
        let adaptive = crate::adaptive_rmi::AdaptiveRMI::build_from_pairs(&pairs);
        Some(Self {
            inner: std::sync::Arc::new(adaptive),
        })
    }

    pub fn predict_get(&self, key: &u64) -> Option<u64> {
        self.inner.lookup(*key)
    }
}
