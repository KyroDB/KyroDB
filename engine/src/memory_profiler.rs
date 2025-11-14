//! Memory Profiling Infrastructure
//!
//!
//! Features:
//! - jemalloc as global allocator (optional feature flag)
//! - Memory statistics (allocated, resident, mapped)
//! - Heap dump generation for leak analysis
//! - Zero overhead when feature disabled
//!
//! Usage:
//! ```text
//! cargo build --features jemalloc-profiling
//! MALLOC_CONF="prof:true,prof_active:true,lg_prof_sample:19" ./target/debug/validation_enterprise
//! ```

use std::fmt;

/// Memory statistics snapshot
#[derive(Debug, Clone, Copy)]
pub struct MemoryStats {
    /// Currently allocated bytes
    pub allocated: usize,
    /// Resident memory (RSS) in bytes
    pub resident: usize,
    /// Mapped memory in bytes
    pub mapped: usize,
    /// Retained memory (not released to OS)
    pub retained: usize,
}

impl MemoryStats {
    /// Get memory delta between two snapshots
    pub fn delta(&self, previous: &MemoryStats) -> MemoryStatsDelta {
        MemoryStatsDelta {
            allocated: self.allocated as i64 - previous.allocated as i64,
            resident: self.resident as i64 - previous.resident as i64,
            mapped: self.mapped as i64 - previous.mapped as i64,
            retained: self.retained as i64 - previous.retained as i64,
        }
    }
}

impl fmt::Display for MemoryStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "allocated: {:.1} MB, resident: {:.1} MB, mapped: {:.1} MB, retained: {:.1} MB",
            self.allocated as f64 / 1_048_576.0,
            self.resident as f64 / 1_048_576.0,
            self.mapped as f64 / 1_048_576.0,
            self.retained as f64 / 1_048_576.0,
        )
    }
}

/// Memory statistics delta (change over time)
#[derive(Debug, Clone, Copy)]
pub struct MemoryStatsDelta {
    pub allocated: i64,
    pub resident: i64,
    pub mapped: i64,
    pub retained: i64,
}

impl fmt::Display for MemoryStatsDelta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Δ allocated: {:+.1} MB, Δ resident: {:+.1} MB, Δ mapped: {:+.1} MB, Δ retained: {:+.1} MB",
            self.allocated as f64 / 1_048_576.0,
            self.resident as f64 / 1_048_576.0,
            self.mapped as f64 / 1_048_576.0,
            self.retained as f64 / 1_048_576.0,
        )
    }
}

/// Get current memory statistics
///
/// # Implementation
/// - jemalloc-profiling feature: Uses jemalloc stats
/// - Default: Returns zeros (no profiling overhead)
#[cfg(feature = "jemalloc-profiling")]
pub fn get_memory_stats() -> anyhow::Result<MemoryStats> {
    use tikv_jemalloc_ctl::{epoch, stats};

    // Update jemalloc epoch (refreshes stats)
    epoch::mib()
        .map_err(|e| anyhow::anyhow!("jemalloc epoch error: {:?}", e))?
        .advance()
        .map_err(|e| anyhow::anyhow!("jemalloc advance error: {:?}", e))?;

    Ok(MemoryStats {
        allocated: stats::allocated::mib()
            .map_err(|e| anyhow::anyhow!("jemalloc stats error: {:?}", e))?
            .read()
            .map_err(|e| anyhow::anyhow!("jemalloc read error: {:?}", e))?,
        resident: stats::resident::mib()
            .map_err(|e| anyhow::anyhow!("jemalloc stats error: {:?}", e))?
            .read()
            .map_err(|e| anyhow::anyhow!("jemalloc read error: {:?}", e))?,
        mapped: stats::mapped::mib()
            .map_err(|e| anyhow::anyhow!("jemalloc stats error: {:?}", e))?
            .read()
            .map_err(|e| anyhow::anyhow!("jemalloc read error: {:?}", e))?,
        retained: stats::retained::mib()
            .map_err(|e| anyhow::anyhow!("jemalloc stats error: {:?}", e))?
            .read()
            .map_err(|e| anyhow::anyhow!("jemalloc read error: {:?}", e))?,
    })
}

#[cfg(not(feature = "jemalloc-profiling"))]
pub fn get_memory_stats() -> anyhow::Result<MemoryStats> {
    // No profiling support - return zeros
    Ok(MemoryStats {
        allocated: 0,
        resident: 0,
        mapped: 0,
        retained: 0,
    })
}

/// Dump heap profile to file
///
/// Note: Heap profiling requires MALLOC_CONF environment variable
/// Example: MALLOC_CONF="prof:true,prof_active:true" ./program
///
/// This function is a placeholder for now (prof module not available in tikv-jemalloc-ctl 0.6)
#[cfg(feature = "jemalloc-profiling")]
pub fn dump_heap_profile(filename: &str) -> anyhow::Result<()> {
    println!(
        "Heap profiling requires MALLOC_CONF=\"prof:true,prof_active:true\" environment variable"
    );
    println!("Profile file would be: {}", filename);
    anyhow::bail!("Heap dump not yet implemented (prof module unavailable)")
}

#[cfg(not(feature = "jemalloc-profiling"))]
pub fn dump_heap_profile(_filename: &str) -> anyhow::Result<()> {
    anyhow::bail!("jemalloc-profiling feature not enabled")
}

/// Check for memory leaks by comparing stats over time
///
/// Returns true if potential leak detected (allocated memory growing without bound)
pub fn detect_memory_leak(initial: &MemoryStats, current: &MemoryStats, threshold_mb: f64) -> bool {
    let delta = current.delta(initial);
    let allocated_growth_mb = delta.allocated as f64 / 1_048_576.0;

    allocated_growth_mb > threshold_mb
}

/// Memory profiler for tracking leaks over time
pub struct MemoryProfiler {
    initial: MemoryStats,
    checkpoints: Vec<(String, MemoryStats)>,
}

impl MemoryProfiler {
    /// Create new memory profiler with initial snapshot
    pub fn new() -> anyhow::Result<Self> {
        Ok(Self {
            initial: get_memory_stats()?,
            checkpoints: Vec::new(),
        })
    }

    /// Add checkpoint with label
    pub fn checkpoint(&mut self, label: impl Into<String>) -> anyhow::Result<()> {
        let stats = get_memory_stats()?;
        self.checkpoints.push((label.into(), stats));
        Ok(())
    }

    /// Print memory growth since start
    pub fn print_summary(&self) {
        println!("\n╔════════════════════════════════════════════════════════════════╗");
        println!("║                    MEMORY PROFILE SUMMARY                      ║");
        println!("╚════════════════════════════════════════════════════════════════╝\n");

        println!("Initial: {}", self.initial);

        for (label, stats) in &self.checkpoints {
            let delta = stats.delta(&self.initial);
            println!("\n[{}]", label);
            println!("  Current: {}", stats);
            println!("  Growth:  {}", delta);

            // Flag potential leaks
            if detect_memory_leak(&self.initial, stats, 100.0) {
                println!("  ⚠️  WARNING: Potential memory leak detected (>100 MB growth)");
            }
        }
    }

    /// Get final memory statistics
    pub fn final_stats(&self) -> Option<&MemoryStats> {
        self.checkpoints.last().map(|(_, stats)| stats)
    }
}

impl Default for MemoryProfiler {
    fn default() -> Self {
        Self::new().unwrap_or_else(|_| Self {
            initial: MemoryStats {
                allocated: 0,
                resident: 0,
                mapped: 0,
                retained: 0,
            },
            checkpoints: Vec::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_stats_delta() {
        let initial = MemoryStats {
            allocated: 1_000_000,
            resident: 2_000_000,
            mapped: 3_000_000,
            retained: 500_000,
        };

        let current = MemoryStats {
            allocated: 1_500_000,
            resident: 2_200_000,
            mapped: 3_100_000,
            retained: 600_000,
        };

        let delta = current.delta(&initial);
        assert_eq!(delta.allocated, 500_000);
        assert_eq!(delta.resident, 200_000);
        assert_eq!(delta.mapped, 100_000);
        assert_eq!(delta.retained, 100_000);
    }

    #[test]
    fn test_leak_detection() {
        let initial = MemoryStats {
            allocated: 100_000_000, // 100 MB
            resident: 0,
            mapped: 0,
            retained: 0,
        };

        let no_leak = MemoryStats {
            allocated: 150_000_000, // 150 MB (+50 MB, under threshold)
            resident: 0,
            mapped: 0,
            retained: 0,
        };

        let leak = MemoryStats {
            allocated: 300_000_000, // 300 MB (+200 MB, over threshold)
            resident: 0,
            mapped: 0,
            retained: 0,
        };

        assert!(!detect_memory_leak(&initial, &no_leak, 100.0));
        assert!(detect_memory_leak(&initial, &leak, 100.0));
    }
}
