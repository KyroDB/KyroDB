## 🔒 **Lock Ordering Documentation**

To prevent deadlocks in KyroDB, always acquire locks in this order:

### **Primary Lock Order (MUST follow this sequence):**

1. **`group_commit_state`** (Mutex) - Group commit batch state
2. **`inner`** (RwLock) - In-memory event log 
3. **`index`** (RwLock) - Primary index (RMI/BTree)
4. **`wal`** (RwLock) - Write-ahead log file
5. **`next_offset`** (RwLock) - Offset counter
6. **`snapshot_payload_index`** (RwLock) - Snapshot index

### **Lock Duration Guidelines:**

- ✅ **Minimize critical sections** - Only hold locks for atomic operations
- ✅ **No I/O while holding locks** - Prepare data outside critical sections
- ✅ **Drop locks ASAP** - Use explicit scopes `{ }` to release early
- ❌ **Never hold multiple write locks** simultaneously unless absolutely necessary
- ❌ **No blocking operations** while holding any write lock

### **Concurrency & Atomicity Guidelines**

Phase 0 objective: eliminate deadlocks and long critical sections while preserving correctness.

### Preferred Patterns

- Atomic index swapping via `ArcSwap`/`Arc` (readers never block writers during swaps)
- One-shot channels for group-commit acknowledgments
- Background tasks use `tokio::spawn`; avoid holding locks across `.await`

### Lock Ordering (when locks are unavoidable)

1. `group_commit_state` (Mutex)
2. `inner` (RwLock)
3. `index` (RwLock)
4. `wal` (RwLock)
5. `next_offset` (RwLock)
6. `snapshot_payload_index` (RwLock)

### Lock Duration

- Do not perform I/O while holding locks
- Prepare data outside critical sections; scope `{}` to drop guards early
- Avoid nested write locks; prefer message passing or atomics

### **Examples:**

**✅ GOOD - Atomic swap:**
```rust
// Prepare outside critical section
let prepared_data = expensive_operation();

// Minimal critical section
{
    let mut guard = self.index.write().await;
    *guard = prepared_data;
}
```

**❌ BAD - Long critical section:**
```rust
let mut guard = self.index.write().await;
let expensive_result = expensive_operation(); // Blocks other threads!
*guard = expensive_result;
```

### Atomic Swap Example (good)
```rust
// Build new index off-thread
let new_index = build_index(snapshot_view);
// Swap atomically
self.primary_index.store(Arc::new(new_index));
```

### Anti-Pattern (bad)
```rust
let mut g = self.index.write();
let built = build_index(snapshot_view); // blocks while holding write lock
*g = built;
```

### Phase 0 Concurrency SLOs

- No deadlocks under mixed read/write with loom tests
- No lock held > 1ms on hot paths (assert in debug, metrics in release)
- No `.await` while holding any lock

### Checklist

- [ ] All heavy work outside locks
- [ ] Lock acquisition order respected
- [ ] Atomics used for hot-path swaps
- [ ] Background tasks do not starve foreground work
