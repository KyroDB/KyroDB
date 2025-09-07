//! Phase 0: Memory Management & Resource Tracking
//! 
//! This module implements comprehensive memory management for KyroDB
//! with allocation tracking, bounded resource usage, and cache eviction.

use std::sync::atomic::{AtomicUsize, AtomicU64, Ordering};
use std::sync::Arc;
use std::collections::HashMap;
use parking_lot::{RwLock, Mutex};
use std::time::{Duration, Instant};

/// Maximum memory usage before triggering aggressive cleanup (512 MB)
const MAX_MEMORY_BYTES: usize = 512 * 1024 * 1024;

/// Maximum number of cached index snapshots
const MAX_INDEX_SNAPSHOTS: usize = 16;

/// Cache eviction trigger threshold (80% of max memory)
const EVICTION_THRESHOLD: usize = (MAX_MEMORY_BYTES as f64 * 0.8) as usize;

/// Memory manager that tracks all KyroDB allocations and enforces limits
#[derive(Clone)]
pub struct MemoryManager {
    /// Total bytes allocated across all subsystems
    total_allocated: Arc<AtomicUsize>,
    
    /// Peak memory usage observed
    peak_allocated: Arc<AtomicUsize>,
    
    /// Number of allocation requests
    allocation_count: Arc<AtomicU64>,
    
    /// Number of deallocation requests
    deallocation_count: Arc<AtomicU64>,
    
    /// Index snapshot cache with LRU eviction
    index_cache: Arc<RwLock<IndexCache>>,
    
    /// Buffer pool for reusing allocations
    buffer_pool: Arc<Mutex<BufferPool>>,
    
    /// Memory pressure state
    memory_pressure: Arc<AtomicUsize>, // 0=None, 1=Low, 2=Medium, 3=High
}

/// LRU cache for RMI index snapshots
struct IndexCache {
    snapshots: HashMap<u64, CachedSnapshot>,
    access_order: Vec<u64>, // LRU ordering (most recent last)
    total_size: usize,
}

/// Cached index snapshot with metadata
struct CachedSnapshot {
    data: Vec<u8>, // Serialized index data
    size: usize,
    last_access: Instant,
    generation: u64,
}

/// Buffer pool for reusing memory allocations
struct BufferPool {
    small_buffers: Vec<Vec<u8>>,    // < 4KB
    medium_buffers: Vec<Vec<u8>>,   // 4KB - 64KB  
    large_buffers: Vec<Vec<u8>>,    // > 64KB
    total_pooled: usize,
}

/// Memory allocation result
#[derive(Debug)]
pub enum MemoryResult<T> {
    Success(T),
    OutOfMemory,
    CacheEvicted(T), // Successfully allocated after cache eviction
}

/// Memory pressure levels
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MemoryPressure {
    None = 0,
    Low = 1,    // 60-70% of max memory
    Medium = 2, // 70-80% of max memory  
    High = 3,   // 80%+ of max memory
}

#[derive(Debug)]
pub enum MemoryError {
    AllocationFailed(String),
    CacheEvictionFailed,
    InvalidSize(usize),
    PoolExhausted,
}

impl std::fmt::Display for MemoryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MemoryError::AllocationFailed(msg) => write!(f, "Allocation failed: {}", msg),
            MemoryError::CacheEvictionFailed => write!(f, "Cache eviction failed"),
            MemoryError::InvalidSize(size) => write!(f, "Invalid allocation size: {}", size),
            MemoryError::PoolExhausted => write!(f, "Buffer pool exhausted"),
        }
    }
}

impl std::error::Error for MemoryError {}

impl MemoryManager {
    /// Create a new memory manager with bounded allocations
    pub fn new() -> Self {
        Self {
            total_allocated: Arc::new(AtomicUsize::new(0)),
            peak_allocated: Arc::new(AtomicUsize::new(0)),
            allocation_count: Arc::new(AtomicU64::new(0)),
            deallocation_count: Arc::new(AtomicU64::new(0)),
            index_cache: Arc::new(RwLock::new(IndexCache::new())),
            buffer_pool: Arc::new(Mutex::new(BufferPool::new())),
            memory_pressure: Arc::new(AtomicUsize::new(0)),
        }
    }
    
    /// Allocate memory with bounds checking and pressure monitoring
    pub fn allocate(&self, size: usize) -> MemoryResult<Vec<u8>> {
        if size == 0 {
            return MemoryResult::Success(Vec::new());
        }
        
        // Check if allocation would exceed limits
        let current = self.total_allocated.load(Ordering::Relaxed);
        if current + size > MAX_MEMORY_BYTES {
            // Try cache eviction first
            if self.try_cache_eviction(size).is_ok() {
                // Retry allocation after eviction
                return self.allocate_internal(size, true);
            }
            return MemoryResult::OutOfMemory;
        }
        
        self.allocate_internal(size, false)
    }
    
    /// Internal allocation with eviction tracking
    fn allocate_internal(&self, size: usize, post_eviction: bool) -> MemoryResult<Vec<u8>> {
        // Try buffer pool first for common sizes
        if let Ok(buffer) = self.try_buffer_pool(size) {
            self.track_allocation(size);
            return if post_eviction {
                MemoryResult::CacheEvicted(buffer)
            } else {
                MemoryResult::Success(buffer)
            };
        }
        
        // Allocate new buffer
        let buffer = vec![0u8; size];
        self.track_allocation(size);
        
        if post_eviction {
            MemoryResult::CacheEvicted(buffer)
        } else {
            MemoryResult::Success(buffer)
        }
    }
    
    /// Try to get buffer from pool
    fn try_buffer_pool(&self, size: usize) -> Result<Vec<u8>, MemoryError> {
        let mut pool = self.buffer_pool.lock();
        
        let buffer = match size {
            0..=4096 => pool.small_buffers.pop(),
            4097..=65536 => pool.medium_buffers.pop(),
            _ => pool.large_buffers.pop(),
        };
        
        if let Some(mut buf) = buffer {
            buf.clear();
            buf.resize(size, 0);
            pool.total_pooled = pool.total_pooled.saturating_sub(buf.capacity());
            Ok(buf)
        } else {
            Err(MemoryError::PoolExhausted)
        }
    }
    
    /// Track allocation and update pressure
    fn track_allocation(&self, size: usize) {
        let new_total = self.total_allocated.fetch_add(size, Ordering::Relaxed) + size;
        self.allocation_count.fetch_add(1, Ordering::Relaxed);
        
        // Update peak if necessary
        let current_peak = self.peak_allocated.load(Ordering::Relaxed);
        if new_total > current_peak {
            self.peak_allocated.store(new_total, Ordering::Relaxed);
        }
        
        // Update memory pressure
        self.update_memory_pressure(new_total);
    }
    
    /// Update memory pressure level based on current usage
    fn update_memory_pressure(&self, current_bytes: usize) {
        let pressure = if current_bytes > (MAX_MEMORY_BYTES as f64 * 0.8) as usize {
            MemoryPressure::High
        } else if current_bytes > (MAX_MEMORY_BYTES as f64 * 0.7) as usize {
            MemoryPressure::Medium
        } else if current_bytes > (MAX_MEMORY_BYTES as f64 * 0.6) as usize {
            MemoryPressure::Low
        } else {
            MemoryPressure::None
        };
        
        self.memory_pressure.store(pressure as usize, Ordering::Relaxed);
    }
    
    /// Deallocate memory and potentially return to pool
    pub fn deallocate(&self, buffer: Vec<u8>) {
        let size = buffer.len();
        
        // Try to return to buffer pool
        if self.try_return_to_pool(buffer).is_err() {
            // Pool full or buffer too large, just drop it
        }
        
        self.total_allocated.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            Some(current.saturating_sub(size))
        }).ok();
        self.deallocation_count.fetch_add(1, Ordering::Relaxed);
        
        // Update pressure after deallocation
        let new_total = self.total_allocated.load(Ordering::Relaxed);
        self.update_memory_pressure(new_total);
    }
    
    /// Try to return buffer to pool for reuse
    fn try_return_to_pool(&self, buffer: Vec<u8>) -> Result<(), MemoryError> {
        let mut pool = self.buffer_pool.lock();
        let size = buffer.capacity();
        
        // Limit pool size to prevent unbounded growth
        if pool.total_pooled > MAX_MEMORY_BYTES / 4 {
            return Err(MemoryError::PoolExhausted);
        }
        
        match size {
            0..=4096 if pool.small_buffers.len() < 64 => {
                pool.small_buffers.push(buffer);
                pool.total_pooled += size;
                Ok(())
            }
            4097..=65536 if pool.medium_buffers.len() < 32 => {
                pool.medium_buffers.push(buffer);
                pool.total_pooled += size;
                Ok(())
            }
            _ if pool.large_buffers.len() < 8 => {
                pool.large_buffers.push(buffer);
                pool.total_pooled += size;
                Ok(())
            }
            _ => Err(MemoryError::PoolExhausted),
        }
    }
    
    /// Try cache eviction to free memory
    fn try_cache_eviction(&self, needed_bytes: usize) -> Result<(), MemoryError> {
        let mut cache = self.index_cache.write();
        
        // Calculate how much to evict (at least needed_bytes + 25% buffer)
        let target_eviction = needed_bytes + (needed_bytes / 4);
        let mut evicted = 0;
        
        // Remove oldest entries until we've freed enough space
        while evicted < target_eviction && !cache.access_order.is_empty() {
            let oldest_key = cache.access_order.remove(0);
            if let Some(snapshot) = cache.snapshots.remove(&oldest_key) {
                evicted += snapshot.size;
                cache.total_size -= snapshot.size;
            }
        }
        
        if evicted >= needed_bytes {
            Ok(())
        } else {
            Err(MemoryError::CacheEvictionFailed)
        }
    }
    
    /// Cache an index snapshot with LRU eviction
    pub fn cache_index_snapshot(&self, generation: u64, data: Vec<u8>) -> Result<(), MemoryError> {
        let mut cache = self.index_cache.write();
        let size = data.len();
        
        // Check if we need to evict to make room
        while cache.snapshots.len() >= MAX_INDEX_SNAPSHOTS 
              || cache.total_size + size > EVICTION_THRESHOLD {
            if cache.access_order.is_empty() {
                return Err(MemoryError::CacheEvictionFailed);
            }
            
            let oldest_key = cache.access_order.remove(0);
            if let Some(old_snapshot) = cache.snapshots.remove(&oldest_key) {
                cache.total_size -= old_snapshot.size;
            }
        }
        
        // Add new snapshot
        let snapshot = CachedSnapshot {
            data,
            size,
            last_access: Instant::now(),
            generation,
        };
        
        cache.snapshots.insert(generation, snapshot);
        cache.access_order.push(generation);
        cache.total_size += size;
        
        Ok(())
    }
    
    /// Retrieve cached index snapshot
    pub fn get_cached_snapshot(&self, generation: u64) -> Option<Vec<u8>> {
        let mut cache = self.index_cache.write();
        
        // First, check if the snapshot exists and clone the data
        let data = if let Some(snapshot) = cache.snapshots.get(&generation) {
            Some(snapshot.data.clone())
        } else {
            None
        };
        
        // If found, update access time and reorder
        if data.is_some() {
            if let Some(snapshot) = cache.snapshots.get_mut(&generation) {
                snapshot.last_access = Instant::now();
            }
            
            // Move to end of access order (most recent)
            if let Some(pos) = cache.access_order.iter().position(|&x| x == generation) {
                cache.access_order.remove(pos);
                cache.access_order.push(generation);
            }
        }
        
        data
    }
    
    /// Get current memory pressure level
    pub fn memory_pressure(&self) -> MemoryPressure {
        match self.memory_pressure.load(Ordering::Relaxed) {
            0 => MemoryPressure::None,
            1 => MemoryPressure::Low,
            2 => MemoryPressure::Medium,
            3 => MemoryPressure::High,
            _ => MemoryPressure::High,
        }
    }
    
    /// Get memory usage statistics
    pub fn stats(&self) -> MemoryStats {
        let cache = self.index_cache.read();
        let pool = self.buffer_pool.lock();
        
        MemoryStats {
            total_allocated: self.total_allocated.load(Ordering::Relaxed),
            peak_allocated: self.peak_allocated.load(Ordering::Relaxed),
            allocation_count: self.allocation_count.load(Ordering::Relaxed),
            deallocation_count: self.deallocation_count.load(Ordering::Relaxed),
            cache_size: cache.total_size,
            cache_entries: cache.snapshots.len(),
            pool_size: pool.total_pooled,
            pressure: self.memory_pressure(),
        }
    }
    
    /// Force cleanup of all caches and pools
    pub fn force_cleanup(&self) {
        {
            let mut cache = self.index_cache.write();
            cache.snapshots.clear();
            cache.access_order.clear();
            cache.total_size = 0;
        }
        
        {
            let mut pool = self.buffer_pool.lock();
            pool.small_buffers.clear();
            pool.medium_buffers.clear();
            pool.large_buffers.clear();
            pool.total_pooled = 0;
        }
    }
}

#[derive(Debug, Clone)]
pub struct MemoryStats {
    pub total_allocated: usize,
    pub peak_allocated: usize,
    pub allocation_count: u64,
    pub deallocation_count: u64,
    pub cache_size: usize,
    pub cache_entries: usize,
    pub pool_size: usize,
    pub pressure: MemoryPressure,
}

impl IndexCache {
    fn new() -> Self {
        Self {
            snapshots: HashMap::new(),
            access_order: Vec::new(),
            total_size: 0,
        }
    }
}

impl BufferPool {
    fn new() -> Self {
        Self {
            small_buffers: Vec::new(),
            medium_buffers: Vec::new(),
            large_buffers: Vec::new(),
            total_pooled: 0,
        }
    }
}

impl Default for MemoryManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_basic_allocation() {
        let mgr = MemoryManager::new();
        
        // Test normal allocation
        match mgr.allocate(1024) {
            MemoryResult::Success(buffer) => {
                assert_eq!(buffer.len(), 1024);
                mgr.deallocate(buffer);
            }
            _ => panic!("Allocation should succeed"),
        }
        
        let stats = mgr.stats();
        assert_eq!(stats.allocation_count, 1);
        assert_eq!(stats.deallocation_count, 1);
    }
    
    #[test]
    fn test_memory_pressure() {
        let mgr = MemoryManager::new();
        
        // Should start with no pressure
        assert_eq!(mgr.memory_pressure(), MemoryPressure::None);
        
        // Allocate enough to trigger pressure
        let large_size = MAX_MEMORY_BYTES / 2;
        match mgr.allocate(large_size) {
            MemoryResult::Success(buffer) => {
                assert!(matches!(mgr.memory_pressure(), MemoryPressure::Medium | MemoryPressure::High));
                mgr.deallocate(buffer);
            }
            _ => panic!("Large allocation should succeed"),
        }
    }
    
    #[test]
    fn test_cache_eviction() {
        let mgr = MemoryManager::new();
        
        // Fill cache to capacity
        for i in 0..MAX_INDEX_SNAPSHOTS {
            let data = vec![i as u8; 1024];
            mgr.cache_index_snapshot(i as u64, data).unwrap();
        }
        
        let stats = mgr.stats();
        assert_eq!(stats.cache_entries, MAX_INDEX_SNAPSHOTS);
        
        // Add one more to trigger eviction
        let data = vec![255u8; 1024];
        mgr.cache_index_snapshot(999, data).unwrap();
        
        let stats = mgr.stats();
        assert_eq!(stats.cache_entries, MAX_INDEX_SNAPSHOTS);
    }
    
    #[test]
    fn test_buffer_pool() {
        let mgr = MemoryManager::new();
        
        // Allocate and deallocate to populate pool
        let buffer = match mgr.allocate(2048) {
            MemoryResult::Success(buf) => buf,
            _ => panic!("Allocation should succeed"),
        };
        
        mgr.deallocate(buffer);
        
        // Next allocation should reuse from pool
        let stats_before = mgr.stats();
        match mgr.allocate(2048) {
            MemoryResult::Success(buffer) => {
                let stats_after = mgr.stats();
                assert_eq!(stats_after.allocation_count, stats_before.allocation_count + 1);
                mgr.deallocate(buffer);
            }
            _ => panic!("Pool allocation should succeed"),
        }
    }
    
    #[test]
    fn test_out_of_memory() {
        let mgr = MemoryManager::new();
        
        // Try to allocate more than the limit
        match mgr.allocate(MAX_MEMORY_BYTES + 1) {
            MemoryResult::OutOfMemory => {
                // Expected
            }
            _ => panic!("Should hit out of memory"),
        }
    }
}
