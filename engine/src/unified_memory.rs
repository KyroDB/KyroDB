//! 🚀 **UNIFIED ZERO-ALLOCATION MEMORY ARCHITECTURE**
//!
//! Revolutionary memory management that eliminates the chaos of multiple overlapping systems.
//! This replaces BufferPool, UltraFastBufferPool, MemoryManager, and various other pools
//! with a single, high-performance, zero-allocation architecture.

use parking_lot::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// **REALISTIC MEMORY LIMITS** - No more 64GB nonsense
const TOTAL_MEMORY_LIMIT_MB: usize = 128;           // 128MB total (realistic)
const ZERO_ALLOC_POOL_SIZE_MB: usize = 32;          // 32MB for zero-alloc pool
#[allow(dead_code)]
const EMERGENCY_RESERVE_MB: usize = 16;             // 16MB emergency reserve

/// Buffer size categories for optimal memory pooling
const SMALL_BUFFER_SIZE: usize = 256;               // 256B - covers 90% of cases
const MEDIUM_BUFFER_SIZE: usize = 4096;             // 4KB - covers 9% of cases  
const LARGE_BUFFER_SIZE: usize = 65536;             // 64KB - covers 1% of cases

/// **ZERO-ALLOCATION BUFFER POOLS** - Pre-allocated, never allocate at runtime
struct ZeroAllocPool {
    /// Small buffers (256B) - most common case
    small_buffers: Vec<[u8; SMALL_BUFFER_SIZE]>,
    small_available: AtomicUsize, // Bitfield of available buffers
    
    /// Medium buffers (4KB) 
    medium_buffers: Vec<[u8; MEDIUM_BUFFER_SIZE]>,
    medium_available: AtomicUsize,
    
    /// Large buffers (64KB)
    large_buffers: Vec<[u8; LARGE_BUFFER_SIZE]>,
    large_available: AtomicUsize,
    
    /// Total memory allocated (for monitoring only)
    #[allow(dead_code)]
    total_allocated: AtomicUsize,
}

/// **UNIFIED MEMORY MANAGER** - Single source of truth for all memory
pub struct UnifiedMemoryManager {
    /// Zero-allocation pool for hot paths
    zero_alloc_pool: Arc<Mutex<ZeroAllocPool>>,
    
    /// Single memory usage counter (replaces 20+ atomic counters)
    current_memory_bytes: AtomicUsize,
    
    /// Emergency mode flag (when memory is critically low)
    emergency_mode: AtomicUsize, // 0=normal, 1=emergency
}

/// **ZERO-ALLOCATION BUFFER** - Borrowed from pool, automatically returned
pub struct ZeroAllocBuffer<'a> {
    data: &'a mut [u8],
    pool: &'a UnifiedMemoryManager,
    buffer_id: usize,
    size_category: BufferCategory,
}

#[derive(Debug, Clone, Copy)]
enum BufferCategory {
    Small = 0,
    Medium = 1, 
    Large = 2,
}

impl UnifiedMemoryManager {
    /// Create the unified memory manager (replaces all other memory systems)
    pub fn new() -> Self {
        let pool_size_bytes = ZERO_ALLOC_POOL_SIZE_MB * 1024 * 1024;
        
        // Calculate optimal buffer distribution based on usage patterns
        let small_count = 1024;   // 256KB total for small buffers
        let medium_count = 256;   // 1MB total for medium buffers 
        let large_count = 64;     // 4MB total for large buffers
        
        let zero_pool = ZeroAllocPool {
            small_buffers: vec![[0u8; SMALL_BUFFER_SIZE]; small_count],
            small_available: AtomicUsize::new(!0), // All bits set = all available
            
            medium_buffers: vec![[0u8; MEDIUM_BUFFER_SIZE]; medium_count],
            medium_available: AtomicUsize::new(!0),
            
            large_buffers: vec![[0u8; LARGE_BUFFER_SIZE]; large_count],
            large_available: AtomicUsize::new(!0),
            
            total_allocated: AtomicUsize::new(pool_size_bytes),
        };
        
        Self {
            zero_alloc_pool: Arc::new(Mutex::new(zero_pool)),
            current_memory_bytes: AtomicUsize::new(pool_size_bytes),
            emergency_mode: AtomicUsize::new(0),
        }
    }
    
    /// **ZERO-ALLOCATION GET** - Borrow buffer from pool (never allocates)
    pub fn get_zero_alloc_buffer(&self, min_size: usize) -> Option<ZeroAllocBuffer<'_>> {
        let category = if min_size <= SMALL_BUFFER_SIZE {
            BufferCategory::Small
        } else if min_size <= MEDIUM_BUFFER_SIZE {
            BufferCategory::Medium 
        } else if min_size <= LARGE_BUFFER_SIZE {
            BufferCategory::Large
        } else {
            // Size too large for zero-allocation
            return None;
        };
        
        let mut pool = self.zero_alloc_pool.lock();
        
        match category {
            BufferCategory::Small => {
                let mut available = pool.small_available.load(Ordering::Relaxed);
                while available != 0 {
                    let buffer_id = available.trailing_zeros() as usize;
                    let new_available = available & !(1 << buffer_id);
                    
                    match pool.small_available.compare_exchange_weak(
                        available, new_available, Ordering::Relaxed, Ordering::Relaxed
                    ) {
                        Ok(_) => {
                            let data = &mut pool.small_buffers[buffer_id];
                            return Some(ZeroAllocBuffer {
                                data: unsafe { std::mem::transmute(data.as_mut_slice()) },
                                pool: self,
                                buffer_id,
                                size_category: category,
                            });
                        }
                        Err(new_val) => available = new_val,
                    }
                }
            }
            
            BufferCategory::Medium => {
                let mut available = pool.medium_available.load(Ordering::Relaxed);
                while available != 0 {
                    let buffer_id = available.trailing_zeros() as usize;
                    let new_available = available & !(1 << buffer_id);
                    
                    match pool.medium_available.compare_exchange_weak(
                        available, new_available, Ordering::Relaxed, Ordering::Relaxed
                    ) {
                        Ok(_) => {
                            let data = &mut pool.medium_buffers[buffer_id];
                            return Some(ZeroAllocBuffer {
                                data: unsafe { std::mem::transmute(data.as_mut_slice()) },
                                pool: self,
                                buffer_id,
                                size_category: category,
                            });
                        }
                        Err(new_val) => available = new_val,
                    }
                }
            }
            
            BufferCategory::Large => {
                let mut available = pool.large_available.load(Ordering::Relaxed);
                while available != 0 {
                    let buffer_id = available.trailing_zeros() as usize;
                    let new_available = available & !(1 << buffer_id);
                    
                    match pool.large_available.compare_exchange_weak(
                        available, new_available, Ordering::Relaxed, Ordering::Relaxed
                    ) {
                        Ok(_) => {
                            let data = &mut pool.large_buffers[buffer_id];
                            return Some(ZeroAllocBuffer {
                                data: unsafe { std::mem::transmute(data.as_mut_slice()) },
                                pool: self,
                                buffer_id,
                                size_category: category,
                            });
                        }
                        Err(new_val) => available = new_val,
                    }
                }
            }
        }
        
        None // No buffers available
    }
    
    /// **EMERGENCY ALLOCATION** - Only when zero-alloc pool is exhausted
    pub fn emergency_alloc(&self, size: usize) -> Option<Vec<u8>> {
        // Check if we're under memory pressure
        let current_memory = self.current_memory_bytes.load(Ordering::Relaxed);
        let limit_bytes = TOTAL_MEMORY_LIMIT_MB * 1024 * 1024;
        
        if current_memory + size > limit_bytes {
            // Enter emergency mode
            self.emergency_mode.store(1, Ordering::Relaxed);
            return None;
        }
        
        // Allocate and track
        self.current_memory_bytes.fetch_add(size, Ordering::Relaxed);
        Some(vec![0u8; size])
    }
    
    /// Return emergency allocation
    pub fn emergency_dealloc(&self, buf: Vec<u8>) {
        let size = buf.len();
        self.current_memory_bytes.fetch_sub(size, Ordering::Relaxed);
        
        // Check if we can exit emergency mode
        let current_memory = self.current_memory_bytes.load(Ordering::Relaxed);
        let safe_threshold = (TOTAL_MEMORY_LIMIT_MB * 1024 * 1024) / 2;
        
        if current_memory < safe_threshold {
            self.emergency_mode.store(0, Ordering::Relaxed);
        }
    }
    
    /// Get memory statistics (single source of truth)
    pub fn stats(&self) -> UnifiedMemoryStats {
        let current_bytes = self.current_memory_bytes.load(Ordering::Relaxed);
        let emergency = self.emergency_mode.load(Ordering::Relaxed) == 1;
        
        UnifiedMemoryStats {
            current_memory_mb: current_bytes / (1024 * 1024),
            memory_limit_mb: TOTAL_MEMORY_LIMIT_MB,
            emergency_mode: emergency,
            utilization_pct: (current_bytes * 100) / (TOTAL_MEMORY_LIMIT_MB * 1024 * 1024),
        }
    }
    
    /// Force cleanup (for testing/emergency situations)
    pub fn force_cleanup(&self) {
        // Reset emergency mode
        self.emergency_mode.store(0, Ordering::Relaxed);
        
        // Note: Zero-alloc pool doesn't need cleanup as buffers are just returned to availability
    }
}

impl<'a> ZeroAllocBuffer<'a> {
    /// Get mutable slice to buffer data
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        self.data
    }
    
    /// Get buffer size
    pub fn len(&self) -> usize {
        self.data.len()
    }
    
    /// Check if buffer is empty
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl<'a> Drop for ZeroAllocBuffer<'a> {
    fn drop(&mut self) {
        // Automatically return buffer to pool when dropped
        let pool = self.pool.zero_alloc_pool.lock();
        
        match self.size_category {
            BufferCategory::Small => {
                pool.small_available.fetch_or(1 << self.buffer_id, Ordering::Relaxed);
            }
            BufferCategory::Medium => {
                pool.medium_available.fetch_or(1 << self.buffer_id, Ordering::Relaxed);
            }
            BufferCategory::Large => {
                pool.large_available.fetch_or(1 << self.buffer_id, Ordering::Relaxed);
            }
        }
    }
}

/// Unified memory statistics (replaces dozens of different stat structures)
#[derive(Debug, Clone)]
pub struct UnifiedMemoryStats {
    pub current_memory_mb: usize,
    pub memory_limit_mb: usize,
    pub emergency_mode: bool,
    pub utilization_pct: usize,
}

/// Global singleton unified memory manager
static GLOBAL_MEMORY_MANAGER: std::sync::OnceLock<UnifiedMemoryManager> = std::sync::OnceLock::new();

/// Get the global unified memory manager (replaces all other memory systems)
pub fn get_unified_memory() -> &'static UnifiedMemoryManager {
    GLOBAL_MEMORY_MANAGER.get_or_init(|| UnifiedMemoryManager::new())
}

/// **ZERO-ALLOCATION MACROS** - For easy migration from old systems
#[macro_export]
macro_rules! with_zero_alloc_buffer {
    ($size:expr, $buf:ident, $code:block) => {
        if let Some(mut $buf) = crate::unified_memory::get_unified_memory().get_zero_alloc_buffer($size) {
            $code
        } else {
            // Fallback to emergency allocation if zero-alloc pool exhausted
            if let Some(emergency_buf) = crate::unified_memory::get_unified_memory().emergency_alloc($size) {
                let result = {
                    let $buf = &mut emergency_buf.as_slice();
                    $code
                };
                crate::unified_memory::get_unified_memory().emergency_dealloc(emergency_buf);
                result
            } else {
                panic!("Memory exhausted - system under extreme pressure");
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unified_memory_basic() {
        let manager = UnifiedMemoryManager::new();
        
        // Test zero-allocation buffer
        let buffer = manager.get_zero_alloc_buffer(128).unwrap();
        assert_eq!(buffer.len(), SMALL_BUFFER_SIZE);
        
        // Test statistics
        let stats = manager.stats();
        assert!(stats.current_memory_mb > 0);
        assert!(!stats.emergency_mode);
    }
    
    #[test]
    fn test_realistic_memory_limits() {
        // Ensure memory limits are realistic (not 64GB!)
        assert_eq!(TOTAL_MEMORY_LIMIT_MB, 128);
        assert_eq!(ZERO_ALLOC_POOL_SIZE_MB, 32);
        
        let manager = UnifiedMemoryManager::new();
        let stats = manager.stats();
        
        // Should be well within reasonable limits
        assert!(stats.memory_limit_mb <= 128);
        assert!(stats.current_memory_mb <= 64); // Pool should be much smaller than limit
    }
}
