//! Phase 0: Memory Management & Resource Tracking
//! 
//! This module implements comprehensive memory management for KyroDB
//! with allocation tracking, bounded resource usage, and cache eviction.

use std::sync::atomic::{AtomicUsize, AtomicU64, Ordering};
use std::sync::Arc;
use std::collections::HashMap;
use parking_lot::{RwLock, Mutex};
use std::time::Instant;

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
    // generation removed - it was never accessed after being set
}

/// Buffer pool for reusing memory allocations
struct BufferPool {
    small_buffers: Vec<Vec<u8>>,    // < 4KB
    medium_buffers: Vec<Vec<u8>>,   // 4KB - 64KB  
    large_buffers: Vec<Vec<u8>>,    // > 64KB
    total_pooled: usize,
    /// Track if pool is being bypassed due to pressure
    bypass_mode: bool,
    /// Track pool effectiveness metrics
    pool_hits: usize,
    pool_misses: usize,
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

/// Result of pool return operation
enum PoolReturnResult {
    Returned,   // Buffer was returned to pool
    Discarded,  // Buffer was discarded (bypass mode or pool full)
}

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
    
    /// Internal allocation with eviction tracking and robust pool handling
    fn allocate_internal(&self, size: usize, post_eviction: bool) -> MemoryResult<Vec<u8>> {
        let current_pressure = self.memory_pressure();
        
        // Try buffer pool first for common sizes, but respect pressure
        if let Ok(buffer) = self.try_buffer_pool_robust(size, current_pressure) {
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
    
    /// Robust buffer pool access that respects memory pressure
    fn try_buffer_pool_robust(&self, size: usize, pressure: MemoryPressure) -> Result<Vec<u8>, MemoryError> {
        let mut pool = self.buffer_pool.lock();
        
        // Under high memory pressure, bypass pool to force immediate deallocation
        if pressure == MemoryPressure::High && !pool.bypass_mode {
            pool.bypass_mode = true;
            // Clear pool under high pressure to free memory immediately
            pool.small_buffers.clear();
            pool.medium_buffers.clear();
            pool.large_buffers.clear();
            pool.total_pooled = 0;
        }
        
        // Re-enable pool when pressure subsides
        if pressure == MemoryPressure::None && pool.bypass_mode {
            pool.bypass_mode = false;
        }
        
        // If in bypass mode, don't use pool
        if pool.bypass_mode {
            pool.pool_misses += 1;
            return Err(MemoryError::PoolExhausted);
        }
        
        let buffer = match size {
            0..=4096 => pool.small_buffers.pop(),
            4097..=65536 => pool.medium_buffers.pop(),
            _ => pool.large_buffers.pop(),
        };
        
        if let Some(mut buf) = buffer {
            buf.clear();
            buf.resize(size, 0);
            pool.total_pooled = pool.total_pooled.saturating_sub(buf.capacity());
            pool.pool_hits += 1;
            Ok(buf)
        } else {
            pool.pool_misses += 1;
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
    
    /// Deallocate memory with guaranteed pool handling under pressure
    pub fn deallocate(&self, buffer: Vec<u8>) {
        let size = buffer.len();
        
        // Always try to return to buffer pool, but respect pressure settings
        match self.try_return_to_pool_robust(buffer) {
            Ok(PoolReturnResult::Returned) => {
                // Successfully returned to pool
            }
            Ok(PoolReturnResult::Discarded) => {
                // Pool is in bypass mode or full - buffer discarded as intended
            }
            Err(_) => {
                // Pool error - buffer will be dropped normally
            }
        }
        
        self.total_allocated.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            Some(current.saturating_sub(size))
        }).ok();
        self.deallocation_count.fetch_add(1, Ordering::Relaxed);
        
        // Update pressure after deallocation
        let new_total = self.total_allocated.load(Ordering::Relaxed);
        self.update_memory_pressure(new_total);
    }
    
    /// Try to return buffer to pool with robust pressure handling
    fn try_return_to_pool_robust(&self, buffer: Vec<u8>) -> Result<PoolReturnResult, MemoryError> {
        let mut pool = self.buffer_pool.lock();
        let size = buffer.capacity();
        
        // If in bypass mode, discard buffer immediately to prevent memory buildup
        if pool.bypass_mode {
            return Ok(PoolReturnResult::Discarded);
        }
        
        // Limit pool size to prevent unbounded growth
        if pool.total_pooled > MAX_MEMORY_BYTES / 4 {
            return Ok(PoolReturnResult::Discarded);
        }
        
        match size {
            0..=4096 if pool.small_buffers.len() < 64 => {
                pool.small_buffers.push(buffer);
                pool.total_pooled += size;
                Ok(PoolReturnResult::Returned)
            }
            4097..=65536 if pool.medium_buffers.len() < 32 => {
                pool.medium_buffers.push(buffer);
                pool.total_pooled += size;
                Ok(PoolReturnResult::Returned)
            }
            _ if pool.large_buffers.len() < 8 => {
                pool.large_buffers.push(buffer);
                pool.total_pooled += size;
                Ok(PoolReturnResult::Returned)
            }
            _ => Ok(PoolReturnResult::Discarded),
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
    
    /// Get memory usage statistics with pool effectiveness metrics
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
            pool_hits: pool.pool_hits,
            pool_misses: pool.pool_misses,
            pool_bypass_mode: pool.bypass_mode,
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
    pub pool_hits: usize,
    pub pool_misses: usize,
    pub pool_bypass_mode: bool,
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
            bypass_mode: false,
            pool_hits: 0,
            pool_misses: 0,
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
        
        // Allocate enough to trigger Medium pressure (75% of max = 384MB)
        let medium_size = (MAX_MEMORY_BYTES as f64 * 0.75) as usize;
        match mgr.allocate(medium_size) {
            MemoryResult::Success(buffer) => {
                assert_eq!(mgr.memory_pressure(), MemoryPressure::Medium);
                mgr.deallocate(buffer);
                
                // After deallocation, pressure should decrease
                assert_eq!(mgr.memory_pressure(), MemoryPressure::None);
            }
            MemoryResult::CacheEvicted(buffer) => {
                // Also acceptable if cache eviction occurred
                assert!(matches!(mgr.memory_pressure(), MemoryPressure::Medium | MemoryPressure::High));
                mgr.deallocate(buffer);
            }
            MemoryResult::OutOfMemory => {
                // Try a smaller allocation that should definitely trigger pressure
                let smaller_size = (MAX_MEMORY_BYTES as f64 * 0.65) as usize;
                match mgr.allocate(smaller_size) {
                    MemoryResult::Success(buffer) => {
                        assert_eq!(mgr.memory_pressure(), MemoryPressure::Low);
                        mgr.deallocate(buffer);
                    }
                    _ => panic!("Smaller allocation should succeed"),
                }
            }
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
    fn test_buffer_pool_robust() {
        let mgr = MemoryManager::new();
        
        // Test normal pool operation
        let buffer = match mgr.allocate(2048) {
            MemoryResult::Success(buf) => buf,
            _ => panic!("Allocation should succeed"),
        };
        
        // Verify buffer came from fresh allocation
        let stats_before_dealloc = mgr.stats();
        mgr.deallocate(buffer);
        let stats_after_dealloc = mgr.stats();
        
        // Buffer should be returned to pool (unless high pressure)
        let stats_before_realloc = mgr.stats();
        match mgr.allocate(2048) {
            MemoryResult::Success(buffer) => {
                let stats_after_realloc = mgr.stats();
                
                // Check if pool was used (hits should increase if not in bypass mode)
                if !stats_before_realloc.pool_bypass_mode {
                    assert!(stats_after_realloc.pool_hits > stats_before_realloc.pool_hits,
                           "Pool should be used when not in bypass mode");
                }
                
                mgr.deallocate(buffer);
            }
            _ => panic!("Pool allocation should succeed"),
        }
    }
    
    #[test]
    fn test_buffer_pool_pressure_bypass() {
        let mgr = MemoryManager::new();
        
        // Create high memory pressure by allocating close to limit
        let high_pressure_size = (MAX_MEMORY_BYTES as f64 * 0.85) as usize;
        let large_buffer = match mgr.allocate(high_pressure_size) {
            MemoryResult::Success(buf) | MemoryResult::CacheEvicted(buf) => buf,
            MemoryResult::OutOfMemory => {
                // Try smaller allocation if we hit the limit
                match mgr.allocate((MAX_MEMORY_BYTES as f64 * 0.75) as usize) {
                    MemoryResult::Success(buf) | MemoryResult::CacheEvicted(buf) => buf,
                    _ => panic!("Should be able to allocate under pressure"),
                }
            }
        };
        
        // Should now be in high pressure
        assert!(matches!(mgr.memory_pressure(), MemoryPressure::High | MemoryPressure::Medium));
        
        // Allocate a small buffer that would normally use pool
        let small_buffer = match mgr.allocate(1024) {
            MemoryResult::Success(buf) | MemoryResult::CacheEvicted(buf) => buf,
            MemoryResult::OutOfMemory => panic!("Small allocation should succeed"),
        };
        
        // Deallocate small buffer - should be discarded due to pressure, not pooled
        let stats_before = mgr.stats();
        mgr.deallocate(small_buffer);
        let stats_after = mgr.stats();
        
        // Under high pressure, pool should be in bypass mode
        if stats_after.pressure == MemoryPressure::High {
            assert!(stats_after.pool_bypass_mode, "Pool should be in bypass mode under high pressure");
        }
        
        // Clean up
        mgr.deallocate(large_buffer);
    }
    
    #[test]
    fn test_cache_evicted_buffer_handling() {
        let mgr = MemoryManager::new();
        
        // Fill up memory to near the limit to trigger eviction behavior
        let mut buffers = Vec::new();
        let chunk_size = MAX_MEMORY_BYTES / 10;
        
        // Allocate chunks until we approach the limit
        for _ in 0..8 {
            match mgr.allocate(chunk_size) {
                MemoryResult::Success(buf) | MemoryResult::CacheEvicted(buf) => {
                    buffers.push(buf);
                }
                MemoryResult::OutOfMemory => break,
            }
        }
        
        // Try one more large allocation that should trigger cache eviction
        let large_size = chunk_size * 2;
        match mgr.allocate(large_size) {
            MemoryResult::CacheEvicted(buffer) => {
                println!("   ♻️  Allocated after cache eviction");
                
                // FIXED: Proper handling of evicted buffer - verify it's valid
                assert_eq!(buffer.len(), large_size);
                assert!(buffer.capacity() >= large_size);
                
                // Deallocate properly - this should either go to pool or be discarded safely
                mgr.deallocate(buffer);
                
                // Verify deallocation was tracked
                let stats = mgr.stats();
                assert!(stats.deallocation_count > 0);
            }
            MemoryResult::Success(buffer) => {
                // Normal allocation is also acceptable
                mgr.deallocate(buffer);
            }
            MemoryResult::OutOfMemory => {
                // This is acceptable if we truly hit the limit
                println!("   💾  Hit memory limit as expected");
            }
        }
        
        // Clean up all buffers
        for buffer in buffers {
            mgr.deallocate(buffer);
        }
        
        // After cleanup, we should have released most memory
        let final_stats = mgr.stats();
        assert!(final_stats.total_allocated < MAX_MEMORY_BYTES / 4);
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
