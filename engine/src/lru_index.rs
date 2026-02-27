use std::collections::HashMap;
use std::hash::Hash;

#[derive(Debug, Clone, Copy)]
struct LruNode<K> {
    prev: Option<K>,
    next: Option<K>,
}

/// O(1) recency index over keys.
///
/// Stores only key ordering metadata (not values). Callers keep the value map.
/// The oldest key is at `head`, newest key at `tail`.
#[derive(Debug, Clone)]
pub(crate) struct LruIndex<K>
where
    K: Eq + Hash + Copy,
{
    nodes: HashMap<K, LruNode<K>>,
    head: Option<K>,
    tail: Option<K>,
}

impl<K> Default for LruIndex<K>
where
    K: Eq + Hash + Copy,
{
    fn default() -> Self {
        Self {
            nodes: HashMap::new(),
            head: None,
            tail: None,
        }
    }
}

impl<K> LruIndex<K>
where
    K: Eq + Hash + Copy,
{
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            nodes: HashMap::with_capacity(capacity),
            head: None,
            tail: None,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.nodes.len()
    }

    pub(crate) fn contains(&self, key: K) -> bool {
        self.nodes.contains_key(&key)
    }

    pub(crate) fn clear(&mut self) {
        self.nodes.clear();
        self.head = None;
        self.tail = None;
    }

    pub(crate) fn insert_new(&mut self, key: K) {
        if self.contains(key) {
            let _ = self.touch(key);
            return;
        }

        let old_tail = self.tail;
        self.nodes.insert(
            key,
            LruNode {
                prev: old_tail,
                next: None,
            },
        );

        if let Some(tail) = old_tail {
            if let Some(node) = self.nodes.get_mut(&tail) {
                node.next = Some(key);
            }
        } else {
            self.head = Some(key);
        }

        self.tail = Some(key);
    }

    /// Move an existing key to MRU position.
    ///
    /// Returns false if the key does not exist.
    pub(crate) fn touch(&mut self, key: K) -> bool {
        let Some(node) = self.nodes.get(&key).copied() else {
            return false;
        };
        if self.tail == Some(key) {
            return true;
        }

        self.detach(key, node.prev, node.next);

        let old_tail = self.tail;
        if let Some(tail) = old_tail {
            if let Some(tail_node) = self.nodes.get_mut(&tail) {
                tail_node.next = Some(key);
            }
        } else {
            self.head = Some(key);
        }

        if let Some(node) = self.nodes.get_mut(&key) {
            node.prev = old_tail;
            node.next = None;
        }
        self.tail = Some(key);
        true
    }

    pub(crate) fn remove(&mut self, key: K) -> bool {
        let Some(node) = self.nodes.remove(&key) else {
            return false;
        };

        self.detach(key, node.prev, node.next);
        true
    }

    /// Pop the least recently used key.
    pub(crate) fn pop_lru(&mut self) -> Option<K> {
        let key = self.head?;
        let removed = self.remove(key);
        debug_assert!(removed, "head key must exist in nodes map");
        Some(key)
    }

    /// Iterate from most-recent to oldest. Stop early if callback returns false.
    pub(crate) fn for_each_recent<F>(&self, limit: usize, mut f: F)
    where
        F: FnMut(K) -> bool,
    {
        if limit == 0 {
            return;
        }

        let mut current = self.tail;
        let mut seen = 0usize;
        while let Some(key) = current {
            if seen >= limit {
                break;
            }
            seen += 1;
            if !f(key) {
                break;
            }
            current = self.nodes.get(&key).and_then(|node| node.prev);
        }
    }

    fn detach(&mut self, key: K, prev: Option<K>, next: Option<K>) {
        if let Some(prev_key) = prev {
            if let Some(node) = self.nodes.get_mut(&prev_key) {
                node.next = next;
            }
        } else {
            self.head = next;
        }

        if let Some(next_key) = next {
            if let Some(node) = self.nodes.get_mut(&next_key) {
                node.prev = prev;
            }
        } else {
            self.tail = prev;
        }

        if self.head == Some(key) {
            self.head = next;
        }
        if self.tail == Some(key) {
            self.tail = prev;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::LruIndex;

    #[test]
    fn maintains_lru_order() {
        let mut lru = LruIndex::with_capacity(8);
        lru.insert_new(1u64);
        lru.insert_new(2u64);
        lru.insert_new(3u64);
        assert_eq!(lru.pop_lru(), Some(1));
        assert_eq!(lru.pop_lru(), Some(2));
    }

    #[test]
    fn touch_moves_to_mru() {
        let mut lru = LruIndex::with_capacity(8);
        lru.insert_new(1u64);
        lru.insert_new(2u64);
        lru.insert_new(3u64);
        assert!(lru.touch(1));
        assert_eq!(lru.pop_lru(), Some(2));
    }

    #[test]
    fn remove_unlinks_neighbors() {
        let mut lru = LruIndex::with_capacity(8);
        lru.insert_new(1u64);
        lru.insert_new(2u64);
        lru.insert_new(3u64);
        assert!(lru.remove(2));
        assert_eq!(lru.pop_lru(), Some(1));
        assert_eq!(lru.pop_lru(), Some(3));
        assert_eq!(lru.pop_lru(), None);
    }

    #[test]
    fn recent_iteration_from_tail() {
        let mut lru = LruIndex::with_capacity(8);
        lru.insert_new(1u64);
        lru.insert_new(2u64);
        lru.insert_new(3u64);
        lru.touch(1);

        let mut seen = Vec::new();
        lru.for_each_recent(3, |k| {
            seen.push(k);
            true
        });
        assert_eq!(seen, vec![1, 3, 2]);
    }
}
