//! ANN backend abstraction and single runtime graph engine.
//!
//! This module intentionally maintains one authoritative ANN representation
//! (`FlatGraph`) for both mutation and search paths. The previous hybrid mode
//! (separate mutable HNSW + read-optimized flat snapshot) is removed to avoid
//! dual-engine drift and cache/consistency ambiguity.

use parking_lot::RwLock;
use rayon::prelude::*;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::hash::{BuildHasherDefault, Hasher};

use crate::config::DistanceMetric;
use crate::hnsw_index::SearchResult;

const MIN_EF_CONSTRUCTION: usize = 128;
const DENSE_VISITED_MAX_NODES: usize = 262_144;
const SPARSE_VISITED_RETAIN_CAPACITY: usize = 65_536;
const SCRATCH_HEAP_RETAIN_CAPACITY: usize = 32_768;
const PARALLEL_STAGING_THRESHOLD: usize = 128;
const PARALLEL_STAGING_CHUNK_SIZE: usize = 512;

#[derive(Default)]
struct U32IdentityHasher {
    value: u64,
}

impl Hasher for U32IdentityHasher {
    fn finish(&self) -> u64 {
        self.value
    }

    fn write(&mut self, bytes: &[u8]) {
        let mut hash = 0u64;
        for &byte in bytes {
            hash = hash.wrapping_mul(131).wrapping_add(byte as u64);
        }
        self.value = hash;
    }

    fn write_u32(&mut self, i: u32) {
        self.value = i as u64;
    }

    fn write_u64(&mut self, i: u64) {
        self.value = i;
    }

    fn write_usize(&mut self, i: usize) {
        self.value = i as u64;
    }
}

type SparseVisitedSet = HashSet<u32, BuildHasherDefault<U32IdentityHasher>>;

#[inline]
fn metric_distance(distance: DistanceMetric, query: &[f32], candidate: &[f32]) -> f32 {
    match distance {
        DistanceMetric::Euclidean => crate::simd::l2_distance_f32(query, candidate),
        DistanceMetric::Cosine | DistanceMetric::InnerProduct => {
            (1.0 - crate::simd::dot_f32(query, candidate)).max(0.0)
        }
    }
}

#[inline]
fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

#[inline]
fn sampled_level(origin_id: usize, max_layer_limit: usize, m: usize) -> usize {
    if max_layer_limit == 0 {
        return 0;
    }

    // HNSW-style deterministic level sampling:
    // level = floor(-ln(U) * mL), where mL = 1 / ln(M).
    let m = m.max(2) as f64;
    let ml = 1.0 / m.ln();
    let seed = splitmix64(origin_id as u64 ^ 0xA5A5_A5A5_A5A5_A5A5);
    let mut u = ((seed >> 11) as f64) * (1.0 / ((1u64 << 53) as f64));
    if u <= 0.0 {
        u = f64::MIN_POSITIVE;
    } else if u >= 1.0 {
        u = 1.0 - f64::EPSILON;
    }

    ((-u.ln() * ml).floor() as usize).min(max_layer_limit)
}

/// Internal ANN backend contract used by `HnswVectorIndex`.
///
/// Backends must preserve semantics:
/// - input IDs are internal contiguous IDs supplied by the caller
/// - search returns ascending-distance neighbors for the supplied metric
pub(crate) trait AnnBackend: Send + Sync {
    fn name(&self) -> &'static str;
    fn insert(&self, embedding: &[f32], origin_id: usize);
    fn parallel_insert_slice(&self, batch: &[(&[f32], usize)]);
    fn on_sequential_batch_complete(&self) {}
    fn estimated_memory_bytes(&self) -> usize {
        0
    }
    fn search(&self, query: &[f32], k: usize, ef_search: usize) -> Vec<SearchResult>;
}

#[derive(Debug, Clone)]
struct FlatGraph {
    distance: DistanceMetric,
    dimension: usize,
    vectors: Vec<f32>,
    dense_to_origin: Vec<u64>,
    origin_to_dense: HashMap<u64, u32>,
    neighbors_by_layer: Vec<Vec<Vec<u32>>>,
    entry_by_layer: Vec<u32>,
    max_layer: usize,
}

impl FlatGraph {
    fn new_single(distance: DistanceMetric, embedding: &[f32], doc_id: u64) -> Option<Self> {
        if embedding.is_empty() {
            return None;
        }

        let mut origin_to_dense = HashMap::with_capacity(1);
        origin_to_dense.insert(doc_id, 0);

        Some(Self {
            distance,
            dimension: embedding.len(),
            vectors: embedding.to_vec(),
            dense_to_origin: vec![doc_id],
            origin_to_dense,
            neighbors_by_layer: vec![vec![Vec::new()]],
            entry_by_layer: vec![0],
            max_layer: 0,
        })
    }

    fn len(&self) -> usize {
        self.dense_to_origin.len()
    }

    #[inline]
    fn vector_at(&self, dense_id: u32) -> &[f32] {
        let idx = dense_id as usize;
        let start = idx * self.dimension;
        &self.vectors[start..start + self.dimension]
    }

    #[inline]
    fn distance_to(&self, query: &[f32], dense_id: u32) -> f32 {
        let v = self.vector_at(dense_id);
        metric_distance(self.distance, query, v)
    }

    #[inline]
    fn neighbors(&self, layer: usize, dense_id: u32) -> &[u32] {
        &self.neighbors_by_layer[layer][dense_id as usize]
    }

    fn estimated_memory_bytes(&self) -> usize {
        let vectors_bytes = self.vectors.len() * std::mem::size_of::<f32>();
        let dense_to_origin_bytes = self.dense_to_origin.len() * std::mem::size_of::<u64>();
        let entry_by_layer_bytes = self.entry_by_layer.len() * std::mem::size_of::<u32>();

        let map_payload_bytes =
            self.origin_to_dense.len() * (std::mem::size_of::<u64>() + std::mem::size_of::<u32>());
        let map_overhead_bytes = self.origin_to_dense.len() * 16;

        let mut layer_adj_bytes = 0usize;
        for layer in &self.neighbors_by_layer {
            layer_adj_bytes += layer.len() * std::mem::size_of::<Vec<u32>>();
            for nbrs in layer {
                layer_adj_bytes += nbrs.len() * std::mem::size_of::<u32>();
            }
        }

        vectors_bytes
            + dense_to_origin_bytes
            + entry_by_layer_bytes
            + map_payload_bytes
            + map_overhead_bytes
            + layer_adj_bytes
    }

    fn reserve_for_additional(&mut self, additional: usize) {
        if additional == 0 {
            return;
        }
        self.vectors
            .reserve(additional.saturating_mul(self.dimension));
        self.dense_to_origin.reserve(additional);
        self.origin_to_dense.reserve(additional);
        for layer in &mut self.neighbors_by_layer {
            layer.reserve(additional);
        }
    }

    #[inline]
    fn prefetch_dense_vector(&self, dense_id: u32) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};

            let idx = dense_id as usize;
            if idx >= self.len() {
                return;
            }
            let ptr = self.vector_at(dense_id).as_ptr() as *const i8;
            _mm_prefetch(ptr, _MM_HINT_T0);
            if self.dimension > 16 {
                _mm_prefetch(ptr.add(64), _MM_HINT_T0);
            }
        }
        #[cfg(not(target_arch = "x86_64"))]
        let _ = dense_id;
    }

    fn search(&self, query: &[f32], k: usize, ef_search: usize) -> Vec<SearchResult> {
        if self.len() == 0 || k == 0 {
            return Vec::new();
        }

        let ef = ef_search.max(k).min(self.len().max(1));
        let mut entry = self.entry_by_layer[self.max_layer];
        if entry as usize >= self.len() {
            entry = 0;
        }
        let mut entry_dist = self.distance_to(query, entry);

        for layer in (1..=self.max_layer).rev() {
            let (new_entry, new_dist) = self.greedy_descent_layer(query, entry, entry_dist, layer);
            entry = new_entry;
            entry_dist = new_dist;
        }

        self.search_layer0(query, entry, entry_dist, k, ef)
    }

    fn greedy_descent_layer(
        &self,
        query: &[f32],
        mut current: u32,
        mut current_dist: f32,
        layer: usize,
    ) -> (u32, f32) {
        loop {
            let mut improved = false;
            let neighbors = self.neighbors(layer, current);
            for (idx, &nbr) in neighbors.iter().enumerate() {
                if let Some(&next) = neighbors.get(idx + 1) {
                    self.prefetch_dense_vector(next);
                }
                if nbr as usize >= self.len() {
                    continue;
                }
                let d = self.distance_to(query, nbr);
                if d < current_dist {
                    current = nbr;
                    current_dist = d;
                    improved = true;
                }
            }
            if !improved {
                return (current, current_dist);
            }
        }
    }

    fn search_layer0(
        &self,
        query: &[f32],
        entry: u32,
        entry_dist: f32,
        k: usize,
        ef: usize,
    ) -> Vec<SearchResult> {
        FLAT_SEARCH_SCRATCH.with(|scratch_cell| {
            let mut scratch = scratch_cell.borrow_mut();
            scratch.prepare(self.len(), ef);
            scratch.mark_visited(entry);
            scratch.candidates.push(CandidateHeapItem {
                neg_distance: -entry_dist,
                dense_id: entry,
            });
            scratch.results.push(ResultHeapItem {
                distance: entry_dist,
                dense_id: entry,
            });

            while let Some(candidate) = scratch.candidates.pop() {
                let candidate_dist = -candidate.neg_distance;
                if scratch.results.len() >= ef {
                    let worst_dist = scratch
                        .results
                        .peek()
                        .map(|v| v.distance)
                        .unwrap_or(f32::MAX);
                    if candidate_dist > worst_dist {
                        break;
                    }
                }

                let neighbors = self.neighbors(0, candidate.dense_id);
                for (idx, &nbr) in neighbors.iter().enumerate() {
                    if let Some(&next) = neighbors.get(idx + 1) {
                        self.prefetch_dense_vector(next);
                    }
                    if nbr as usize >= self.len() {
                        continue;
                    }
                    if scratch.was_visited(nbr) {
                        continue;
                    }
                    scratch.mark_visited(nbr);
                    let d = self.distance_to(query, nbr);
                    let can_push = scratch.results.len() < ef
                        || scratch
                            .results
                            .peek()
                            .map(|v| d < v.distance)
                            .unwrap_or(true);
                    if can_push {
                        scratch.candidates.push(CandidateHeapItem {
                            neg_distance: -d,
                            dense_id: nbr,
                        });
                        scratch.results.push(ResultHeapItem {
                            distance: d,
                            dense_id: nbr,
                        });
                        if scratch.results.len() > ef {
                            let _ = scratch.results.pop();
                        }
                    }
                }
            }

            let mut out = Vec::with_capacity(scratch.results.len().min(k));
            while let Some(item) = scratch.results.pop() {
                out.push(SearchResult {
                    doc_id: self.dense_to_origin[item.dense_id as usize],
                    distance: item.distance,
                });
            }
            out.sort_by(|a, b| {
                a.distance
                    .partial_cmp(&b.distance)
                    .unwrap_or(Ordering::Equal)
            });
            out.truncate(k);
            out
        })
    }

    /// Beam search at a specified graph layer.
    ///
    /// Standard HNSW `SEARCH-LAYER` subroutine (Malkov & Yashunin 2016).
    /// Returns `(dense_id, distance)` pairs sorted by ascending distance.
    /// Used during construction to find neighbor candidates at each layer.
    fn search_at_layer(
        &self,
        query: &[f32],
        entry: u32,
        entry_dist: f32,
        ef: usize,
        layer: usize,
    ) -> Vec<(u32, f32)> {
        if self.len() == 0 || ef == 0 {
            return Vec::new();
        }
        if layer >= self.neighbors_by_layer.len() {
            return vec![(entry, entry_dist)];
        }
        let ef = ef.max(1).min(self.len());

        FLAT_SEARCH_SCRATCH.with(|scratch_cell| {
            let mut scratch = scratch_cell.borrow_mut();
            scratch.prepare(self.len(), ef);
            scratch.mark_visited(entry);
            scratch.candidates.push(CandidateHeapItem {
                neg_distance: -entry_dist,
                dense_id: entry,
            });
            scratch.results.push(ResultHeapItem {
                distance: entry_dist,
                dense_id: entry,
            });

            while let Some(candidate) = scratch.candidates.pop() {
                let candidate_dist = -candidate.neg_distance;
                if scratch.results.len() >= ef {
                    let worst_dist = scratch
                        .results
                        .peek()
                        .map(|v| v.distance)
                        .unwrap_or(f32::MAX);
                    if candidate_dist > worst_dist {
                        break;
                    }
                }

                let neighbors = self.neighbors(layer, candidate.dense_id);
                for (idx, &nbr) in neighbors.iter().enumerate() {
                    if let Some(&next) = neighbors.get(idx + 1) {
                        self.prefetch_dense_vector(next);
                    }
                    if nbr as usize >= self.len() {
                        continue;
                    }
                    if scratch.was_visited(nbr) {
                        continue;
                    }
                    scratch.mark_visited(nbr);
                    let d = self.distance_to(query, nbr);
                    let can_push = scratch.results.len() < ef
                        || scratch
                            .results
                            .peek()
                            .map(|v| d < v.distance)
                            .unwrap_or(true);
                    if can_push {
                        scratch.candidates.push(CandidateHeapItem {
                            neg_distance: -d,
                            dense_id: nbr,
                        });
                        scratch.results.push(ResultHeapItem {
                            distance: d,
                            dense_id: nbr,
                        });
                        if scratch.results.len() > ef {
                            let _ = scratch.results.pop();
                        }
                    }
                }
            }

            let mut out = Vec::with_capacity(scratch.results.len());
            while let Some(item) = scratch.results.pop() {
                out.push((item.dense_id, item.distance));
            }
            out.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
            out
        })
    }

    #[inline]
    fn distance_between_dense(&self, lhs: u32, rhs: u32) -> f32 {
        metric_distance(self.distance, self.vector_at(lhs), self.vector_at(rhs))
    }

    fn prune_layer_neighbors(&mut self, layer: usize, dense_id: u32, max_neighbors: usize) {
        if self.neighbors_by_layer.is_empty() || max_neighbors == 0 {
            return;
        }
        if layer >= self.neighbors_by_layer.len() {
            return;
        }
        let idx = dense_id as usize;
        if idx >= self.neighbors_by_layer[layer].len() {
            return;
        }
        if self.neighbors_by_layer[layer][idx].len() <= max_neighbors {
            return;
        }

        let current = self.neighbors_by_layer[layer][idx].clone();
        let mut scored = Vec::with_capacity(current.len());
        for nbr in current {
            if nbr == dense_id {
                continue;
            }
            let d = self.distance_between_dense(dense_id, nbr);
            scored.push((d, nbr));
        }
        scored.sort_by(|a, b| {
            a.0.partial_cmp(&b.0)
                .unwrap_or(Ordering::Equal)
                .then_with(|| a.1.cmp(&b.1))
        });

        let mut pruned = Vec::with_capacity(max_neighbors);
        let mut last_kept = None::<u32>;
        for (_, nbr) in scored {
            if last_kept == Some(nbr) {
                continue;
            }
            last_kept = Some(nbr);
            pruned.push(nbr);
            if pruned.len() >= max_neighbors {
                break;
            }
        }
        self.neighbors_by_layer[layer][idx] = pruned;
    }

    /// Connect a freshly-added node using per-layer candidate lists.
    ///
    /// Standard HNSW construction: for each layer the node participates in,
    /// connect to the closest candidates found by `search_at_layer`, then prune
    /// overflow on reverse edges. Replaces the old `incremental_insert` which
    /// only searched at layer 0 and connected upper layers to a single anchor.
    fn connect_with_layer_candidates(
        &mut self,
        doc_id: u64,
        embedding: &[f32],
        inserted_level: usize,
        layer_candidates: &[Vec<(u32, f32)>],
        layer0_max_neighbors: usize,
        upper_layer_max_neighbors: usize,
    ) -> bool {
        if embedding.len() != self.dimension {
            return false;
        }
        if self.origin_to_dense.contains_key(&doc_id) {
            return false;
        }

        let new_dense = self.dense_to_origin.len() as u32;
        self.vectors.extend_from_slice(embedding);
        self.dense_to_origin.push(doc_id);
        self.origin_to_dense.insert(doc_id, new_dense);

        // Ensure layer structure accommodates the inserted level.
        if self.neighbors_by_layer.is_empty() {
            self.neighbors_by_layer = vec![Vec::new()];
            self.entry_by_layer = vec![new_dense];
            self.max_layer = 0;
        }
        if inserted_level > self.max_layer {
            let existing_count = self.dense_to_origin.len() - 1;
            for _ in (self.max_layer + 1)..=inserted_level {
                self.neighbors_by_layer
                    .push(vec![Vec::new(); existing_count]);
                self.entry_by_layer.push(new_dense);
            }
            self.max_layer = inserted_level;
        }
        // Add empty neighbor slot for the new node at every active layer.
        for layer_nbrs in &mut self.neighbors_by_layer {
            layer_nbrs.push(Vec::new());
        }

        // Connect at each layer using ranked candidates from the build plan.
        let effective_level = inserted_level.min(self.max_layer);
        for layer in 0..=effective_level {
            let max_nbrs = if layer == 0 {
                layer0_max_neighbors
            } else {
                upper_layer_max_neighbors
            };
            if max_nbrs == 0 {
                continue;
            }

            let candidates = match layer_candidates.get(layer) {
                Some(c) => c,
                None => continue,
            };

            // Select closest unique candidates capped at max_nbrs.
            let mut selected: Vec<u32> = Vec::with_capacity(max_nbrs);
            for &(dense_id, _dist) in candidates {
                if dense_id == new_dense || dense_id as usize >= self.len() {
                    continue;
                }
                if !selected.contains(&dense_id) {
                    selected.push(dense_id);
                    if selected.len() >= max_nbrs {
                        break;
                    }
                }
            }

            // Forward edges: new_node -> selected neighbors.
            self.neighbors_by_layer[layer][new_dense as usize] = selected.clone();

            // Reverse edges: neighbor -> new_node (bidirectional connectivity).
            for &nbr in &selected {
                let nbr_list = &mut self.neighbors_by_layer[layer][nbr as usize];
                // new_dense was freshly allocated; reverse edges cannot already contain it.
                nbr_list.push(new_dense);
            }

            // Prune overflow on reverse neighbor lists.
            for &nbr in &selected {
                self.prune_layer_neighbors(layer, nbr, max_nbrs);
            }

            // Connectivity guarantee: after pruning, verify at least one reverse
            // edge survives so the new node remains reachable from the rest of the
            // graph. Extreme outliers may lose all reverse edges during pruning
            // because they are the farthest neighbor for every connected node.
            let reachable = selected
                .iter()
                .any(|&nbr| self.neighbors_by_layer[layer][nbr as usize].contains(&new_dense));
            if !reachable {
                if let Some(&closest_nbr) = selected.first() {
                    let nbr_list = &mut self.neighbors_by_layer[layer][closest_nbr as usize];
                    if !nbr_list.contains(&new_dense) {
                        nbr_list.push(new_dense);
                    }
                }
            }
        }

        true
    }
}

#[derive(Copy, Clone, Debug)]
struct CandidateHeapItem {
    neg_distance: f32,
    dense_id: u32,
}

impl Eq for CandidateHeapItem {}

impl PartialEq for CandidateHeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.neg_distance == other.neg_distance && self.dense_id == other.dense_id
    }
}

impl PartialOrd for CandidateHeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CandidateHeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        self.neg_distance
            .partial_cmp(&other.neg_distance)
            .unwrap_or(Ordering::Equal)
            .then_with(|| self.dense_id.cmp(&other.dense_id))
    }
}

#[derive(Copy, Clone, Debug)]
struct ResultHeapItem {
    distance: f32,
    dense_id: u32,
}

impl Eq for ResultHeapItem {}

impl PartialEq for ResultHeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.distance == other.distance && self.dense_id == other.dense_id
    }
}

impl PartialOrd for ResultHeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ResultHeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        self.distance
            .partial_cmp(&other.distance)
            .unwrap_or(Ordering::Equal)
            .then_with(|| self.dense_id.cmp(&other.dense_id))
    }
}

#[derive(Default)]
struct FlatSearchScratch {
    visited_generation: Vec<u32>,
    visited_sparse: SparseVisitedSet,
    current_generation: u32,
    use_sparse_visited: bool,
    candidates: BinaryHeap<CandidateHeapItem>,
    results: BinaryHeap<ResultHeapItem>,
}

impl FlatSearchScratch {
    fn prepare(&mut self, node_count: usize, target_heap_len: usize) {
        self.use_sparse_visited = node_count > DENSE_VISITED_MAX_NODES;
        let target_heap_len = target_heap_len.max(16);

        if self.use_sparse_visited {
            self.visited_sparse.clear();
            if self.visited_sparse.capacity() > SPARSE_VISITED_RETAIN_CAPACITY {
                self.visited_sparse
                    .shrink_to(SPARSE_VISITED_RETAIN_CAPACITY);
            }
            if self.visited_generation.len() > DENSE_VISITED_MAX_NODES {
                self.visited_generation.truncate(DENSE_VISITED_MAX_NODES);
                self.visited_generation.shrink_to(DENSE_VISITED_MAX_NODES);
            }
        } else {
            self.visited_sparse.clear();
            if self.visited_sparse.capacity() > SPARSE_VISITED_RETAIN_CAPACITY {
                self.visited_sparse
                    .shrink_to(SPARSE_VISITED_RETAIN_CAPACITY);
            }
            if self.visited_generation.len() < node_count {
                self.visited_generation.resize(node_count, 0);
            }
            self.current_generation = self.current_generation.wrapping_add(1);
            if self.current_generation == 0 {
                self.visited_generation.fill(0);
                self.current_generation = 1;
            }
        }

        self.candidates.clear();
        self.results.clear();
        let candidates_retain = SCRATCH_HEAP_RETAIN_CAPACITY.max(target_heap_len);
        if self.candidates.capacity() > candidates_retain {
            self.candidates.shrink_to(candidates_retain);
        }
        if self.results.capacity() > candidates_retain {
            self.results.shrink_to(candidates_retain);
        }
        if self.candidates.capacity() < target_heap_len {
            self.candidates
                .reserve(target_heap_len.saturating_sub(self.candidates.capacity()));
        }
        if self.results.capacity() < target_heap_len {
            self.results
                .reserve(target_heap_len.saturating_sub(self.results.capacity()));
        }
    }

    #[inline]
    fn was_visited(&self, dense_id: u32) -> bool {
        if self.use_sparse_visited {
            return self.visited_sparse.contains(&dense_id);
        }
        self.visited_generation
            .get(dense_id as usize)
            .map(|generation| *generation == self.current_generation)
            .unwrap_or(true)
    }

    #[inline]
    fn mark_visited(&mut self, dense_id: u32) {
        if self.use_sparse_visited {
            let _ = self.visited_sparse.insert(dense_id);
            return;
        }
        if let Some(entry) = self.visited_generation.get_mut(dense_id as usize) {
            *entry = self.current_generation;
        }
    }
}

thread_local! {
    static FLAT_SEARCH_SCRATCH: RefCell<FlatSearchScratch> = RefCell::new(FlatSearchScratch::default());
}

/// Single ANN backend implementation.
///
/// This backend keeps one mutable/searchable graph in memory and does not
/// maintain a secondary index representation for queries.
pub(crate) struct SingleGraphBackend {
    distance: DistanceMetric,
    flat_graph: RwLock<Option<FlatGraph>>,
    m: usize,
    ef_construction: usize,
    layer0_neighbor_cap: usize,
    upper_layer_neighbor_cap: usize,
    max_layer_limit: usize,
}

#[derive(Clone, Default)]
struct InsertPlan {
    valid: bool,
    inserted_level: usize,
    /// Per-layer neighbor candidates: `layer_candidates[i]` contains
    /// `(dense_id, distance)` pairs for layer `i`, sorted ascending by distance.
    /// Produced by `build_insert_plan` using per-layer beam search.
    layer_candidates: Vec<Vec<(u32, f32)>>,
}

impl SingleGraphBackend {
    pub(crate) fn new(
        distance: DistanceMetric,
        m: usize,
        _max_elements: usize,
        max_layer: usize,
        ef_construction: usize,
    ) -> Self {
        let m = m.max(4);
        Self {
            distance,
            flat_graph: RwLock::new(None),
            m,
            ef_construction: ef_construction.max(MIN_EF_CONSTRUCTION),
            layer0_neighbor_cap: (2 * m).max(8),
            upper_layer_neighbor_cap: m.max(4),
            max_layer_limit: max_layer.max(1),
        }
    }

    /// Build an insert plan by searching the existing graph at each layer.
    ///
    /// Implements the standard HNSW insert search pattern:
    /// 1. Greedy descent from the top layer down to `inserted_level + 1` to find
    ///    the best entry point.
    /// 2. Beam search with `ef_construction` at each layer from
    ///    `min(inserted_level, max_layer)` down to 0 to collect neighbor
    ///    candidates.
    fn build_insert_plan(
        &self,
        flat: &FlatGraph,
        embedding: &[f32],
        origin_id: usize,
    ) -> InsertPlan {
        let new_doc_id = origin_id as u64;
        if flat.origin_to_dense.contains_key(&new_doc_id) {
            return InsertPlan::default();
        }
        if embedding.len() != flat.dimension {
            return InsertPlan::default();
        }
        if flat.len() == 0 {
            // First element: no neighbors to find.
            return InsertPlan {
                valid: true,
                inserted_level: sampled_level(origin_id, self.max_layer_limit, self.m),
                layer_candidates: Vec::new(),
            };
        }

        let inserted_level = sampled_level(origin_id, self.max_layer_limit, self.m);

        // Phase 1: Greedy descent from the top of the existing graph down to
        // one layer above the node's insert level (standard HNSW approach).
        let top = flat
            .max_layer
            .min(flat.entry_by_layer.len().saturating_sub(1));
        let mut entry = flat.entry_by_layer[top];
        if entry as usize >= flat.len() {
            entry = 0;
        }
        let mut entry_dist = flat.distance_to(embedding, entry);

        for layer in (inserted_level.saturating_add(1)..=top).rev() {
            let (new_entry, new_dist) =
                flat.greedy_descent_layer(embedding, entry, entry_dist, layer);
            entry = new_entry;
            entry_dist = new_dist;
        }

        // Phase 2: At each layer from min(inserted_level, max_layer) down to 0,
        // run a beam search with ef_construction to collect neighbor candidates.
        let search_bottom = inserted_level.min(flat.max_layer);
        let mut layer_candidates: Vec<Vec<(u32, f32)>> = vec![Vec::new(); search_bottom + 1];

        for layer in (0..=search_bottom).rev() {
            let candidates =
                flat.search_at_layer(embedding, entry, entry_dist, self.ef_construction, layer);
            // Use the nearest found candidate as entry for the next lower layer.
            if let Some(&(best_id, best_dist)) = candidates.first() {
                entry = best_id;
                entry_dist = best_dist;
            }
            layer_candidates[layer] = candidates;
        }

        InsertPlan {
            valid: true,
            inserted_level,
            layer_candidates,
        }
    }

    fn apply_insert_plan(
        &self,
        flat: &mut FlatGraph,
        embedding: &[f32],
        origin_id: usize,
        plan: &InsertPlan,
    ) {
        if !plan.valid {
            return;
        }
        flat.connect_with_layer_candidates(
            origin_id as u64,
            embedding,
            plan.inserted_level,
            &plan.layer_candidates,
            self.layer0_neighbor_cap,
            self.upper_layer_neighbor_cap,
        );
    }

    fn insert_into_existing_flat(&self, flat: &mut FlatGraph, embedding: &[f32], origin_id: usize) {
        let plan = self.build_insert_plan(flat, embedding, origin_id);
        self.apply_insert_plan(flat, embedding, origin_id, &plan);
    }
}

impl AnnBackend for SingleGraphBackend {
    fn name(&self) -> &'static str {
        "kyro_single_graph"
    }

    fn insert(&self, embedding: &[f32], origin_id: usize) {
        let mut flat_guard = self.flat_graph.write();
        if let Some(flat) = flat_guard.as_mut() {
            self.insert_into_existing_flat(flat, embedding, origin_id);
            return;
        }

        *flat_guard = FlatGraph::new_single(self.distance, embedding, origin_id as u64);
    }

    fn parallel_insert_slice(&self, batch: &[(&[f32], usize)]) {
        if batch.is_empty() {
            return;
        }

        for chunk in batch.chunks(PARALLEL_STAGING_CHUNK_SIZE) {
            let staged = {
                let flat_guard = self.flat_graph.read();
                flat_guard.as_ref().map(|flat| {
                    if chunk.len() >= PARALLEL_STAGING_THRESHOLD {
                        chunk
                            .par_iter()
                            .map(|(embedding, origin_id)| {
                                self.build_insert_plan(flat, embedding, *origin_id)
                            })
                            .collect::<Vec<_>>()
                    } else {
                        chunk
                            .iter()
                            .map(|(embedding, origin_id)| {
                                self.build_insert_plan(flat, embedding, *origin_id)
                            })
                            .collect::<Vec<_>>()
                    }
                })
            };

            let mut flat_guard = self.flat_graph.write();
            if let Some(flat) = flat_guard.as_mut() {
                flat.reserve_for_additional(chunk.len());
            }
            for (idx, &(embedding, origin_id)) in chunk.iter().enumerate() {
                if let Some(flat) = flat_guard.as_mut() {
                    if let Some(ref staged_plans) = staged {
                        self.apply_insert_plan(flat, embedding, origin_id, &staged_plans[idx]);
                    } else {
                        self.insert_into_existing_flat(flat, embedding, origin_id);
                    }
                } else {
                    *flat_guard = FlatGraph::new_single(self.distance, embedding, origin_id as u64);
                }
            }
        }
    }

    fn estimated_memory_bytes(&self) -> usize {
        self.flat_graph
            .read()
            .as_ref()
            .map(FlatGraph::estimated_memory_bytes)
            .unwrap_or(0)
    }

    fn search(&self, query: &[f32], k: usize, ef_search: usize) -> Vec<SearchResult> {
        self.flat_graph
            .read()
            .as_ref()
            .map(|flat| flat.search(query, k, ef_search))
            .unwrap_or_default()
    }
}

/// Build the default ANN backend implementation.
pub(crate) fn create_default_backend(
    distance: DistanceMetric,
    m: usize,
    max_elements: usize,
    max_layer: usize,
    ef_construction: usize,
) -> Box<dyn AnnBackend> {
    Box::new(SingleGraphBackend::new(
        distance,
        m,
        max_elements,
        max_layer,
        ef_construction,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap as StdHashMap;

    fn make_vec(seed: usize) -> Vec<f32> {
        vec![
            seed as f32 * 0.5 + 1.0,
            (seed % 13) as f32 - 2.0,
            (seed % 17) as f32 * 0.25,
            (seed % 19) as f32 * -0.5,
        ]
    }

    fn seeded_backend() -> SingleGraphBackend {
        let backend = SingleGraphBackend::new(DistanceMetric::Euclidean, 16, 20_000, 8, 200);
        let vectors: Vec<Vec<f32>> = (0..256).map(make_vec).collect();
        let batch: Vec<(&[f32], usize)> = vectors
            .iter()
            .enumerate()
            .map(|(id, v)| (v.as_slice(), id))
            .collect();
        backend.parallel_insert_slice(&batch);
        backend
    }

    fn contains_doc(results: &[SearchResult], doc_id: u64) -> bool {
        results.iter().any(|r| r.doc_id == doc_id)
    }

    fn pseudo_random_vec(seed: usize, dim: usize) -> Vec<f32> {
        let mut out = Vec::with_capacity(dim);
        let mut state = splitmix64(seed as u64 ^ 0xD0E1_F2A3_B4C5_9697);
        for _ in 0..dim {
            state = splitmix64(state);
            let v = ((state & 0xFFFF) as f32 / 32768.0) - 1.0;
            out.push(v);
        }
        out
    }

    #[test]
    fn flat_search_scratch_uses_sparse_visited_for_large_graphs() {
        let mut scratch = FlatSearchScratch::default();
        scratch.prepare(DENSE_VISITED_MAX_NODES + 1, 128);
        assert!(
            scratch.use_sparse_visited,
            "large node counts should switch to sparse visited tracking"
        );

        scratch.mark_visited(42);
        assert!(scratch.was_visited(42));
        assert!(
            scratch.visited_generation.len() <= DENSE_VISITED_MAX_NODES,
            "dense visited array must remain capped in sparse mode"
        );
    }

    #[test]
    fn flat_search_scratch_releases_excess_sparse_and_heap_capacity() {
        let mut scratch = FlatSearchScratch::default();
        scratch.prepare(DENSE_VISITED_MAX_NODES + 1, 128);

        for id in 0..(SPARSE_VISITED_RETAIN_CAPACITY as u32 * 2) {
            scratch.mark_visited(id);
            scratch.candidates.push(CandidateHeapItem {
                neg_distance: -(id as f32),
                dense_id: id,
            });
            scratch.results.push(ResultHeapItem {
                distance: id as f32,
                dense_id: id,
            });
        }

        let sparse_cap_before = scratch.visited_sparse.capacity();
        let candidates_cap_before = scratch.candidates.capacity();
        let results_cap_before = scratch.results.capacity();

        scratch.prepare(DENSE_VISITED_MAX_NODES + 1, 128);
        assert!(
            scratch.visited_sparse.capacity() < sparse_cap_before,
            "sparse visited set should release some retained capacity"
        );
        assert!(
            scratch.visited_sparse.capacity() <= SPARSE_VISITED_RETAIN_CAPACITY * 2,
            "sparse visited set should release excess retained capacity"
        );
        assert!(
            scratch.candidates.capacity() < candidates_cap_before,
            "candidate heap should release some retained capacity"
        );
        assert!(
            scratch.candidates.capacity() <= SCRATCH_HEAP_RETAIN_CAPACITY * 2,
            "candidate heap should release excess retained capacity"
        );
        assert!(
            scratch.results.capacity() < results_cap_before,
            "result heap should release some retained capacity"
        );
        assert!(
            scratch.results.capacity() <= SCRATCH_HEAP_RETAIN_CAPACITY * 2,
            "result heap should release excess retained capacity"
        );
    }

    #[test]
    fn backend_inserts_are_immediately_query_visible() {
        let backend = SingleGraphBackend::new(DistanceMetric::Euclidean, 16, 20_000, 8, 200);
        let point = vec![42.0, -7.0, 1.5, 0.5];

        backend.insert(&point, 9_001);
        backend.insert(&[41.5, -7.2, 1.6, 0.4], 9_002);

        let results = backend.search(&point, 5, 128);
        assert!(!results.is_empty());
        assert!(contains_doc(&results, 9_001));
    }

    #[test]
    fn incremental_insert_extends_graph_and_preserves_searchability() {
        let backend = seeded_backend();
        let initial_flat_len = backend
            .flat_graph
            .read()
            .as_ref()
            .map(|flat| flat.len())
            .unwrap_or(0);

        let incremental = vec![12345.0, -77.0, 13.0, 0.25];
        backend.insert(&incremental, 9_999);

        let flat_guard = backend.flat_graph.read();
        let flat = flat_guard
            .as_ref()
            .expect("single backend must keep graph present after insert");
        assert_eq!(flat.len(), initial_flat_len + 1);
        assert!(flat.origin_to_dense.contains_key(&9_999));
        let new_dense = *flat
            .origin_to_dense
            .get(&9_999)
            .expect("missing new dense id for incremental insert");
        assert!(
            !flat.neighbors_by_layer[0][new_dense as usize].is_empty(),
            "incremental node should have at least one layer-0 edge"
        );

        let results = backend.search(&incremental, 10, 256);
        assert!(contains_doc(&results, 9_999));
    }

    #[test]
    fn duplicate_origin_id_does_not_duplicate_graph_entry() {
        let backend = seeded_backend();
        let before = backend
            .flat_graph
            .read()
            .as_ref()
            .map(|flat| flat.len())
            .unwrap_or(0);

        backend.insert(&[1.0, 2.0, 3.0, 4.0], 42);
        let after = backend
            .flat_graph
            .read()
            .as_ref()
            .map(|flat| flat.len())
            .unwrap_or(0);

        assert_eq!(before, after, "duplicate origin IDs must be ignored");
    }

    #[test]
    fn flat_incremental_insert_extends_upper_layers_when_insert_level_is_high() {
        let mut origin_to_dense = StdHashMap::new();
        origin_to_dense.insert(1u64, 0u32);

        let mut flat = FlatGraph {
            distance: DistanceMetric::Euclidean,
            dimension: 2,
            vectors: vec![0.0, 0.0],
            dense_to_origin: vec![1],
            origin_to_dense,
            neighbors_by_layer: vec![vec![vec![]]],
            entry_by_layer: vec![0],
            max_layer: 0,
        };

        // Per-layer candidates: dense_id=0 is the only existing node.
        let layer_candidates = vec![
            vec![(0u32, 2.0f32)], // layer 0
            vec![(0u32, 2.0f32)], // layer 1
            vec![(0u32, 2.0f32)], // layer 2
        ];
        let ok = flat.connect_with_layer_candidates(2, &[1.0, 1.0], 2, &layer_candidates, 8, 4);
        assert!(ok, "connect_with_layer_candidates should succeed");
        assert_eq!(flat.max_layer, 2);
        assert_eq!(flat.neighbors_by_layer.len(), 3);
        assert_eq!(flat.entry_by_layer.len(), 3);
        assert_eq!(flat.dense_to_origin.len(), 2);
        assert_eq!(*flat.origin_to_dense.get(&2).unwrap_or(&u32::MAX), 1);

        // Layer-0: candidate edge is retained and bidirectional.
        assert!(flat.neighbors_by_layer[0][1].contains(&0));
        assert!(flat.neighbors_by_layer[0][0].contains(&1));

        // Upper layers: connected via per-layer candidates.
        assert!(flat.neighbors_by_layer[1][1].contains(&0));
        assert!(flat.neighbors_by_layer[2][1].contains(&0));
    }

    #[test]
    fn backend_respects_ef_construction_floor() {
        let backend = SingleGraphBackend::new(DistanceMetric::Euclidean, 16, 10_000, 8, 1024);
        assert!(
            backend.ef_construction >= 1024,
            "ef_construction should honor the caller-supplied value"
        );
    }

    #[test]
    fn sampled_level_decreases_as_m_increases() {
        let mut sum_m16 = 0usize;
        let mut sum_m48 = 0usize;
        for origin_id in 0..20_000usize {
            sum_m16 += sampled_level(origin_id, 16, 16);
            sum_m48 += sampled_level(origin_id, 16, 48);
        }

        assert!(
            sum_m16 > sum_m48,
            "larger M should produce fewer high-layer assignments"
        );
    }

    #[test]
    fn backend_recall_regression_guard_against_bruteforce() {
        const DATASET_SIZE: usize = 512;
        const DIM: usize = 16;
        const K: usize = 10;
        const QUERIES: usize = 64;

        let backend = SingleGraphBackend::new(DistanceMetric::Euclidean, 24, DATASET_SIZE, 8, 400);
        let dataset: Vec<Vec<f32>> = (0..DATASET_SIZE)
            .map(|seed| pseudo_random_vec(seed, DIM))
            .collect();
        let batch: Vec<(&[f32], usize)> = dataset
            .iter()
            .enumerate()
            .map(|(id, v)| (v.as_slice(), id))
            .collect();
        backend.parallel_insert_slice(&batch);

        let mut total_hits = 0usize;
        let mut total_expected = 0usize;
        for query_id in 0..QUERIES {
            let query = &dataset[query_id * (DATASET_SIZE / QUERIES)];
            let approx = backend.search(query, K, 1024);

            let mut exact = (0..DATASET_SIZE)
                .map(|idx| {
                    (
                        idx as u64,
                        metric_distance(DistanceMetric::Euclidean, query, &dataset[idx]),
                    )
                })
                .collect::<Vec<_>>();
            exact.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
            exact.truncate(K);

            let exact_ids = exact.into_iter().map(|(id, _)| id).collect::<HashSet<_>>();
            let hits = approx
                .iter()
                .filter(|result| exact_ids.contains(&result.doc_id))
                .count();
            total_hits += hits;
            total_expected += K;
        }

        let recall = total_hits as f64 / total_expected as f64;
        assert!(
            recall >= 0.90,
            "recall regression: expected >= 0.90, got {:.4}",
            recall
        );
    }
}
