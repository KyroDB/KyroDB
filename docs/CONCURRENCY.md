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

### **Concurrency Safety Checklist:**

- [ ] All expensive operations done outside critical sections
- [ ] Lock acquisition follows the primary order
- [ ] Write locks are held for minimal time
- [ ] No nested lock acquisitions without careful ordering
- [ ] All error paths properly release locks
