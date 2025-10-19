# Concurrency Control & Lock Ordering

**Status**: Production-grade concurrency discipline enforced

## Overview

KyroDB uses fine-grained locking with `parking_lot::RwLock` for high concurrency. This document defines the **lock ordering rules** that MUST be followed to prevent deadlocks.

**Violation of lock ordering = deadlock under load** (typically manifests at 10-50 QPS).

---

## Lock Ordering Rules

### **CRITICAL: Global Lock Order**

All locks MUST be acquired in this order (never skip or reverse):

```
1. cache_strategy (RwLock)
2. hot_tier.documents (RwLock) 
3. cold_tier.index (RwLock)
4. cold_tier.embeddings (RwLock)
5. stats (RwLock)
6. access_logger (RwLock)
```

### **Rule 1: Never Hold Multiple Locks Simultaneously**

**BAD** (deadlock risk):
```rust
pub fn bad_example(&self) {
    let cache = self.cache_strategy.read();  // Lock 1
    let mut stats = self.stats.write();      // Lock 2 (DEADLOCK RISK)
    stats.cache_hits += 1;
    // ... use cache ...
} // Both locks released
```

**GOOD** (safe):
```rust
pub fn good_example(&self) {
    let result = {
        let cache = self.cache_strategy.read();
        cache.get_cached(doc_id)
    }; // Lock 1 released
    
    if result.is_some() {
        let mut stats = self.stats.write();  // Lock 2 (now safe)
        stats.cache_hits += 1;
    } // Lock 2 released
}
```

### **Rule 2: Drop Locks Before Calling Methods**

Methods may acquire locks internally. Always drop locks before calling them.

**BAD**:
```rust
pub fn bad_method_call(&self) {
    let cache = self.cache_strategy.read();
    self.flush_hot_tier().unwrap(); // flush_hot_tier may acquire other locks!
    // DEADLOCK RISK
}
```

**GOOD**:
```rust
pub fn good_method_call(&self) {
    let result = {
        let cache = self.cache_strategy.read();
        cache.get_cached(doc_id)
    }; // Release lock
    
    self.flush_hot_tier().unwrap(); // Now safe
}
```

### **Rule 3: Use Scoped Locks**

Always use block scopes `{ }` to limit lock lifetimes:

```rust
pub fn scoped_locks(&self) {
    {
        let cache = self.cache_strategy.read();
        // ... use cache ...
    } // Lock automatically released
    
    {
        let mut stats = self.stats.write();
        // ... update stats ...
    } // Lock automatically released
}
```

---

## Deadlock Detection

### **Debug Builds (Automatic)**

KyroDB enables `parking_lot` deadlock detection in debug builds:

```toml
# Cargo.toml
parking_lot = { version = "0.12", features = ["deadlock_detection"] }
```

**Initialization** (in `main()`):
```rust
// Spawns background thread checking for deadlocks every 10 seconds
kyrodb_engine::init_deadlock_detection();
```

**On Detection**:
```
🚨 DEADLOCK DETECTED 🚨
1 deadlock(s) found:
Deadlock #0
Thread ID: ThreadId(42)
Backtrace:
  ... stack trace of deadlocked threads ...
thread 'main' panicked at 'Deadlock detected - see stderr for details'
```

### **Release Builds (Zero Overhead)**

Deadlock detection is **completely disabled** in release builds (no performance impact).

---

## Common Patterns

### **Pattern 1: Query Path** (tiered_engine.rs::query)

```rust
pub fn query(&self, doc_id: u64, query_embedding: Option<&[f32]>) -> Option<Vec<f32>> {
    // Layer 1: Check cache
    let cached_result = {
        let cache = self.cache_strategy.read();
        cache.get_cached(doc_id)
    }; // Lock released
    
    if let Some(cached) = cached_result {
        {
            let mut stats = self.stats.write();
            stats.cache_hits += 1;
        } // Lock released
        
        if let Some(ref logger) = self.access_logger {
            if let Some(query_emb) = query_embedding {
                logger.write().log_access(doc_id, query_emb);
            }
        } // Lock released
        
        return Some(cached.embedding);
    }
    
    // Continue to Layer 2, Layer 3...
}
```

**Key Points**:
- Each lock acquired and released in isolated scope
- No method calls while holding locks
- Stats updated after cache lock released
- Logger accessed after stats lock released

### **Pattern 2: Cache Admission** (tiered_engine.rs)

```rust
// Cache admission decision (isolated)
let should_cache_decision = {
    let mut cache = self.cache_strategy.write();
    cache.should_cache(doc_id, &embedding)
}; // Lock released

if should_cache_decision {
    let cached = CachedVector {
        doc_id,
        embedding: embedding.clone(),
        distance: 0.0,
        cached_at: Instant::now(),
    };
    // Insert into cache (isolated, no other locks held)
    self.cache_strategy.write().insert_cached(cached);
} // Lock released
```

**Key Points**:
- Decision made in one lock scope
- Lock released before insert operation
- Insert happens in separate lock scope

### **Pattern 3: Stats Updates**

Always update stats in **isolated scopes** after releasing all other locks:

```rust
// CORRECT:
{
    let mut stats = self.stats.write();
    stats.cache_hits += 1;
} // Lock released immediately
```

---

## Testing for Deadlocks

### **Manual Testing**

Run server in debug mode with high concurrency:

```bash
# Build in debug mode with deadlock detection
cargo build --bin kyrodb_server

# Run server
./target/debug/kyrodb_server --port 3030

# Generate concurrent load (separate terminal)
ab -n 10000 -c 50 http://localhost:3030/health
```

If deadlock occurs, server will panic with stack trace.

### **Loom Testing** (Future)

For exhaustive concurrency testing, use `loom`:

```rust
#[cfg(loom)]
use loom::sync::Arc;

#[test]
#[cfg(loom)]
fn test_query_no_deadlock() {
    loom::model(|| {
        let engine = Arc::new(TieredEngine::new(...));
        
        let handles: Vec<_> = (0..2).map(|_| {
            let engine = Arc::clone(&engine);
            loom::thread::spawn(move || {
                engine.query(42, Some(&vec![0.1; 128]));
            })
        }).collect();
        
        for h in handles {
            h.join().unwrap();
        }
    });
}
```

---

## Lock Performance

### **parking_lot vs std::sync**

KyroDB uses `parking_lot::RwLock` for performance:

- **Uncontended acquire**: 10-20ns (vs 30-50ns for std::sync)
- **Read-read contention**: ~2× faster than std::sync
- **Write contention**: ~1.5× faster than std::sync

### **Metrics**

Monitor lock contention via metrics:
```
kyrodb_lock_wait_time_ns{lock="cache_strategy"} 1500
kyrodb_lock_wait_time_ns{lock="stats"} 800
```

---

## Violation Recovery

### **If Deadlock Detected in Production**

1. **Immediate**: Restart affected server
2. **Investigation**: Check logs for `DEADLOCK DETECTED` message
3. **Root Cause**: Identify violated lock ordering rule
4. **Fix**: Refactor code to follow lock ordering rules
5. **Test**: Run with `ab` or production-like load

### **Prevention Checklist**

Before merging concurrency-related code:

- [ ] All locks acquired in global order (cache_strategy → hot_tier → cold_tier → stats → logger)
- [ ] No multiple locks held simultaneously
- [ ] Locks dropped before method calls
- [ ] Scoped locks used (explicit `{ }` blocks)
- [ ] Tested with `cargo build` (debug) + `ab -c 50`

---

## References

- [parking_lot documentation](https://docs.rs/parking_lot/)
- [Rust Deadlock Detection](https://docs.rs/parking_lot/latest/parking_lot/deadlock/index.html)


---

**REMEMBER**: Deadlocks are **100% preventable** with proper lock discipline. Follow the rules religiously.
