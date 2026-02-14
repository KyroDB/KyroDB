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
use std::collections::{BinaryHeap, HashMap};

use crate::config::DistanceMetric;
use crate::hnsw_index::SearchResult;

const MIN_EF_CONSTRUCTION: usize = 128;
const SCRATCH_HEAP_RETAIN_CAPACITY: usize = 32_768;
const VISITED_TOUCHED_RETAIN_CAPACITY: usize = 131_072;
const VISITED_BITSET_RETAIN_WORDS: usize = (8 * 1024 * 1024) / std::mem::size_of::<u64>();
const PARALLEL_STAGING_THRESHOLD: usize = 128;
const PARALLEL_STAGING_CHUNK_SIZE_SMALL: usize = 512;
const PARALLEL_STAGING_CHUNK_SIZE_MEDIUM: usize = 1024;
const PARALLEL_STAGING_CHUNK_SIZE_LARGE: usize = 2048;
#[cfg(target_arch = "x86_64")]
const PREFETCH_CACHELINE_BYTES: usize = 64;
const PREFETCH_NEIGHBOR_LOOKAHEAD_MAX: usize = 2;
const INVALID_DENSE_ID: u32 = u32::MAX;
const DIVERSIFIED_CANDIDATE_BUDGET_MULTIPLIER: usize = 12;

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

#[inline]
fn distance_order(lhs: f32, rhs: f32) -> Ordering {
    lhs.total_cmp(&rhs)
}

#[inline]
fn keep_top_k_by_distance(results: &mut Vec<SearchResult>, k: usize) {
    if k == 0 {
        results.clear();
        return;
    }
    if results.len() > k {
        // Partition around the k-th element (1-indexed) to keep only the best k.
        results.select_nth_unstable_by(k - 1, |a, b| distance_order(a.distance, b.distance));
        results.truncate(k);
    }
    results.sort_unstable_by(|a, b| distance_order(a.distance, b.distance));
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
struct LayerAdjacency {
    cap: usize,
    sparse: bool,
    counts: Vec<u16>,
    neighbors: Vec<u32>,
    dense_to_slot: Vec<u32>,
}

impl LayerAdjacency {
    fn new_dense(cap: usize, node_count: usize) -> Self {
        let cap = cap.max(1);
        Self {
            cap,
            sparse: false,
            counts: vec![0; node_count],
            neighbors: vec![INVALID_DENSE_ID; node_count.saturating_mul(cap)],
            dense_to_slot: Vec::new(),
        }
    }

    fn new_sparse(cap: usize, node_count: usize) -> Self {
        let cap = cap.max(1);
        Self {
            cap,
            sparse: true,
            counts: Vec::new(),
            neighbors: Vec::new(),
            dense_to_slot: vec![INVALID_DENSE_ID; node_count],
        }
    }

    fn reserve_nodes(&mut self, additional: usize) {
        if additional == 0 {
            return;
        }
        if self.sparse {
            self.dense_to_slot.reserve(additional);
            return;
        }
        self.counts.reserve(additional);
        self.neighbors.reserve(additional.saturating_mul(self.cap));
    }

    fn push_node(&mut self, present_in_layer: bool) {
        if self.sparse {
            self.dense_to_slot.push(INVALID_DENSE_ID);
            if present_in_layer {
                let dense_id = (self.dense_to_slot.len() - 1) as u32;
                let _ = self.ensure_slot(dense_id);
            }
            return;
        }

        self.counts.push(0);
        self.neighbors
            .extend(std::iter::repeat_n(INVALID_DENSE_ID, self.cap));
    }

    fn has_dense(&self, dense_id: u32) -> bool {
        self.slot_for_dense(dense_id).is_some()
    }

    fn slot_for_dense(&self, dense_id: u32) -> Option<usize> {
        let dense = dense_id as usize;
        if self.sparse {
            let slot = *self.dense_to_slot.get(dense)?;
            if slot == INVALID_DENSE_ID {
                return None;
            }
            return Some(slot as usize);
        }

        if dense >= self.counts.len() {
            return None;
        }
        Some(dense)
    }

    fn ensure_slot(&mut self, dense_id: u32) -> Option<usize> {
        if !self.sparse {
            let dense = dense_id as usize;
            if dense >= self.counts.len() {
                return None;
            }
            return Some(dense);
        }

        let dense = dense_id as usize;
        if dense >= self.dense_to_slot.len() {
            return None;
        }
        let existing = self.dense_to_slot[dense];
        if existing != INVALID_DENSE_ID {
            return Some(existing as usize);
        }

        let slot = self.counts.len();
        self.counts.push(0);
        self.neighbors
            .extend(std::iter::repeat_n(INVALID_DENSE_ID, self.cap));
        self.dense_to_slot[dense] = slot as u32;
        Some(slot)
    }

    fn neighbors(&self, dense_id: u32) -> &[u32] {
        let Some(slot) = self.slot_for_dense(dense_id) else {
            return &[];
        };
        let count = self.counts[slot] as usize;
        let start = slot.saturating_mul(self.cap);
        &self.neighbors[start..start + count]
    }

    fn set_neighbors(&mut self, dense_id: u32, values: &[u32]) {
        let slot_opt = if self.sparse {
            if values.is_empty() {
                None
            } else {
                self.ensure_slot(dense_id)
            }
        } else {
            self.ensure_slot(dense_id)
        };
        let Some(slot) = slot_opt else {
            return;
        };

        let start = slot.saturating_mul(self.cap);
        let count = values.len().min(self.cap).min(u16::MAX as usize);
        self.counts[slot] = count as u16;
        self.neighbors[start..start + count].copy_from_slice(&values[..count]);
        for slot in &mut self.neighbors[start + count..start + self.cap] {
            *slot = INVALID_DENSE_ID;
        }
    }

    fn estimated_memory_bytes(&self) -> usize {
        self.counts.len() * std::mem::size_of::<u16>()
            + self.neighbors.len() * std::mem::size_of::<u32>()
            + self.dense_to_slot.len() * std::mem::size_of::<u32>()
    }
}

#[derive(Debug, Clone)]
struct FlatGraph {
    distance: DistanceMetric,
    dimension: usize,
    vectors: Vec<f32>,
    dense_to_origin: Vec<u64>,
    origin_to_dense: HashMap<u64, u32>,
    layer0_neighbor_cap: usize,
    upper_layer_neighbor_cap: usize,
    neighbors_by_layer: Vec<LayerAdjacency>,
    entry_by_layer: Vec<u32>,
    max_layer: usize,
}

impl FlatGraph {
    fn new_single(
        distance: DistanceMetric,
        embedding: &[f32],
        doc_id: u64,
        layer0_neighbor_cap: usize,
        upper_layer_neighbor_cap: usize,
    ) -> Option<Self> {
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
            layer0_neighbor_cap: layer0_neighbor_cap.max(1),
            upper_layer_neighbor_cap: upper_layer_neighbor_cap.max(1),
            neighbors_by_layer: vec![LayerAdjacency::new_dense(layer0_neighbor_cap.max(1), 1)],
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
        self.neighbors_by_layer[layer].neighbors(dense_id)
    }

    fn estimated_memory_bytes(&self) -> usize {
        let vectors_bytes = self.vectors.len() * std::mem::size_of::<f32>();
        let dense_to_origin_bytes = self.dense_to_origin.len() * std::mem::size_of::<u64>();
        let entry_by_layer_bytes = self.entry_by_layer.len() * std::mem::size_of::<u32>();

        let map_payload_bytes =
            self.origin_to_dense.len() * (std::mem::size_of::<u64>() + std::mem::size_of::<u32>());
        let map_overhead_bytes = self.origin_to_dense.len() * 16;

        let layer_adj_bytes = self
            .neighbors_by_layer
            .iter()
            .map(LayerAdjacency::estimated_memory_bytes)
            .sum::<usize>();

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
            layer.reserve_nodes(additional);
        }
    }

    #[inline]
    fn prefetch_dense_vector(&self, dense_id: u32, frontier_depth: usize, ef: usize) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};

            let idx = dense_id as usize;
            if idx >= self.len() {
                return;
            }
            let ptr = self.vector_at(dense_id).as_ptr() as *const i8;
            _mm_prefetch(ptr, _MM_HINT_T0);
            let bytes = self.dimension.saturating_mul(std::mem::size_of::<f32>());
            let mut lines = 1usize;
            if bytes > PREFETCH_CACHELINE_BYTES || frontier_depth > (ef / 3).max(32) {
                lines = 2;
            }
            if bytes > (2 * PREFETCH_CACHELINE_BYTES) || frontier_depth > (ef / 2).max(64) {
                lines = 3;
            }
            for line in 1..lines {
                _mm_prefetch(
                    ptr.add(line.saturating_mul(PREFETCH_CACHELINE_BYTES)),
                    _MM_HINT_T0,
                );
            }
        }
        #[cfg(not(target_arch = "x86_64"))]
        let _ = (dense_id, frontier_depth, ef);
    }

    #[inline]
    fn prefetch_neighbor_lookahead(
        &self,
        neighbors: &[u32],
        idx: usize,
        frontier_depth: usize,
        ef: usize,
    ) {
        if let Some(&next) = neighbors.get(idx + 1) {
            self.prefetch_dense_vector(next, frontier_depth, ef);
        }
        let enable_second_hop = self.dimension >= 512 || frontier_depth > (ef / 2).max(64);
        if enable_second_hop && PREFETCH_NEIGHBOR_LOOKAHEAD_MAX >= 2 {
            if let Some(&next2) = neighbors.get(idx + 2) {
                self.prefetch_dense_vector(next2, frontier_depth, ef);
            }
        }
    }

    fn search(&self, query: &[f32], k: usize, ef_search: usize) -> Vec<SearchResult> {
        if self.len() == 0 || k == 0 {
            return Vec::new();
        }
        self.search_fp32(query, k, ef_search)
    }

    fn search_fp32(&self, query: &[f32], k: usize, ef_search: usize) -> Vec<SearchResult> {
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

        self.search_layer0_exact(query, entry, entry_dist, k, ef)
    }

    fn greedy_descent_layer(
        &self,
        query: &[f32],
        mut current: u32,
        mut current_dist: f32,
        layer: usize,
    ) -> (u32, f32) {
        let node_count = self.len();
        loop {
            let mut improved = false;
            let neighbors = self.neighbors(layer, current);
            for (idx, &nbr) in neighbors.iter().enumerate() {
                self.prefetch_neighbor_lookahead(neighbors, idx, 1, 1);
                if nbr as usize >= node_count {
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

    fn search_layer0_exact(
        &self,
        query: &[f32],
        entry: u32,
        entry_dist: f32,
        k: usize,
        ef: usize,
    ) -> Vec<SearchResult> {
        FLAT_SEARCH_SCRATCH.with(|scratch_cell| {
            let mut scratch = scratch_cell.borrow_mut();
            let node_count = self.len();
            scratch.prepare(node_count, ef);
            scratch.mark_visited(entry);
            scratch.candidates.push(CandidateHeapItem {
                neg_distance: -entry_dist,
                dense_id: entry,
            });
            scratch.results.push(ResultHeapItem {
                distance: entry_dist,
                dense_id: entry,
            });
            let mut worst_dist = entry_dist;

            while let Some(candidate) = scratch.candidates.pop() {
                let candidate_dist = -candidate.neg_distance;
                if scratch.results.len() >= ef && candidate_dist > worst_dist {
                    break;
                }

                let neighbors = self.neighbors(0, candidate.dense_id);
                for (idx, &nbr) in neighbors.iter().enumerate() {
                    self.prefetch_neighbor_lookahead(neighbors, idx, scratch.results.len(), ef);
                    if nbr as usize >= node_count {
                        continue;
                    }
                    if !scratch.mark_if_unvisited(nbr) {
                        continue;
                    }
                    let d = self.distance_to(query, nbr);
                    let can_push = scratch.results.len() < ef || d < worst_dist;
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
                        worst_dist = scratch
                            .results
                            .peek()
                            .map(|v| v.distance)
                            .unwrap_or(f32::MAX);
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
            keep_top_k_by_distance(&mut out, k);
            scratch.finish_query();
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
            let node_count = self.len();
            scratch.prepare(node_count, ef);
            scratch.mark_visited(entry);
            scratch.candidates.push(CandidateHeapItem {
                neg_distance: -entry_dist,
                dense_id: entry,
            });
            scratch.results.push(ResultHeapItem {
                distance: entry_dist,
                dense_id: entry,
            });
            let mut worst_dist = entry_dist;

            while let Some(candidate) = scratch.candidates.pop() {
                let candidate_dist = -candidate.neg_distance;
                if scratch.results.len() >= ef && candidate_dist > worst_dist {
                    break;
                }

                let neighbors = self.neighbors(layer, candidate.dense_id);
                for (idx, &nbr) in neighbors.iter().enumerate() {
                    self.prefetch_neighbor_lookahead(neighbors, idx, scratch.results.len(), ef);
                    if nbr as usize >= node_count {
                        continue;
                    }
                    if !scratch.mark_if_unvisited(nbr) {
                        continue;
                    }
                    let d = self.distance_to(query, nbr);
                    let can_push = scratch.results.len() < ef || d < worst_dist;
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
                        worst_dist = scratch
                            .results
                            .peek()
                            .map(|v| v.distance)
                            .unwrap_or(f32::MAX);
                    }
                }
            }

            let mut out = Vec::with_capacity(scratch.results.len());
            while let Some(item) = scratch.results.pop() {
                out.push((item.dense_id, item.distance));
            }
            out.sort_unstable_by(|a, b| distance_order(a.1, b.1));
            scratch.finish_query();
            out
        })
    }

    #[inline]
    fn distance_between_dense(&self, lhs: u32, rhs: u32) -> f32 {
        metric_distance(self.distance, self.vector_at(lhs), self.vector_at(rhs))
    }

    fn set_layer_neighbors(&mut self, layer: usize, dense_id: u32, values: &[u32]) {
        if layer >= self.neighbors_by_layer.len() {
            return;
        }
        self.neighbors_by_layer[layer].set_neighbors(dense_id, values);
    }

    fn select_diverse_neighbors(
        &self,
        candidates: &[(u32, f32)],
        max_neighbors: usize,
    ) -> Vec<u32> {
        if max_neighbors == 0 || candidates.is_empty() {
            return Vec::new();
        }

        let mut selected = Vec::with_capacity(max_neighbors);
        let budget = candidates
            .len()
            .min(max_neighbors.saturating_mul(DIVERSIFIED_CANDIDATE_BUDGET_MULTIPLIER));

        // HNSW neighbor diversity heuristic:
        // accept candidate `c` if it is not closer to any already-selected node
        // than it is to the query node.
        for &(cand, dist_to_query) in candidates.iter().take(budget) {
            if cand as usize >= self.len() {
                continue;
            }
            if selected.contains(&cand) {
                continue;
            }

            let mut diversified = true;
            for &chosen in &selected {
                let dist_to_chosen = self.distance_between_dense(cand, chosen);
                if dist_to_chosen < dist_to_query {
                    diversified = false;
                    break;
                }
            }

            if diversified {
                selected.push(cand);
                if selected.len() >= max_neighbors {
                    return selected;
                }
            }
        }

        // Fallback fill to honor max_neighbors when data geometry is highly clustered.
        if selected.len() < max_neighbors {
            for &(cand, _) in candidates.iter().take(budget) {
                if cand as usize >= self.len() {
                    continue;
                }
                if selected.contains(&cand) {
                    continue;
                }
                selected.push(cand);
                if selected.len() >= max_neighbors {
                    break;
                }
            }
        }

        selected
    }

    fn merge_and_prune_reverse_edge(
        &mut self,
        layer: usize,
        target_dense: u32,
        incoming_dense: u32,
        max_neighbors: usize,
    ) -> bool {
        if layer >= self.neighbors_by_layer.len()
            || target_dense as usize >= self.len()
            || incoming_dense as usize >= self.len()
            || target_dense == incoming_dense
        {
            return false;
        }
        if !self.neighbors_by_layer[layer].has_dense(target_dense) {
            return false;
        }

        let current = self.neighbors(layer, target_dense);
        if current.contains(&incoming_dense) {
            return true;
        }
        if current.len() < max_neighbors {
            let mut merged = Vec::with_capacity(current.len() + 1);
            for &nbr in current {
                if nbr == target_dense || nbr as usize >= self.len() {
                    continue;
                }
                if !merged.contains(&nbr) {
                    merged.push(nbr);
                }
            }
            if !merged.contains(&incoming_dense) {
                merged.push(incoming_dense);
            }
            self.set_layer_neighbors(layer, target_dense, &merged);
            return merged.contains(&incoming_dense);
        }

        let mut scored = Vec::with_capacity(current.len() + 1);
        for &nbr in current {
            if nbr == target_dense || nbr as usize >= self.len() {
                continue;
            }
            if scored.iter().any(|(seen, _)| *seen == nbr) {
                continue;
            }
            scored.push((nbr, self.distance_between_dense(target_dense, nbr)));
        }

        if !scored.iter().any(|(seen, _)| *seen == incoming_dense) {
            scored.push((
                incoming_dense,
                self.distance_between_dense(target_dense, incoming_dense),
            ));
        }

        scored.sort_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0)));

        let mut merged = self.select_diverse_neighbors(&scored, max_neighbors);
        let mut kept_incoming = merged.contains(&incoming_dense);
        if !kept_incoming && merged.len() < max_neighbors {
            merged.push(incoming_dense);
            kept_incoming = true;
        }
        self.set_layer_neighbors(layer, target_dense, &merged);
        kept_incoming
    }

    /// Connect a freshly-added node using per-layer candidate lists.
    ///
    /// Standard HNSW construction: for each layer the node participates in,
    /// connect to the closest candidates found by `search_at_layer`, then prune
    /// overflow on reverse edges. Replaces the old `incremental_insert` which
    /// only searched at layer 0 and connected upper layers to a single anchor.
    #[cfg(test)]
    fn connect_with_layer_candidates(
        &mut self,
        doc_id: u64,
        embedding: &[f32],
        inserted_level: usize,
        layer_candidates: &[Vec<(u32, f32)>],
        layer0_max_neighbors: usize,
        upper_layer_max_neighbors: usize,
    ) -> bool {
        let precompute_level = inserted_level.min(layer_candidates.len().saturating_sub(1));
        let mut selected_neighbors = vec![Vec::new(); precompute_level + 1];
        for (layer, selected_slot) in selected_neighbors
            .iter_mut()
            .enumerate()
            .take(precompute_level + 1)
        {
            let max_nbrs = if layer == 0 {
                layer0_max_neighbors
            } else {
                upper_layer_max_neighbors
            };
            if max_nbrs == 0 {
                continue;
            }
            if let Some(candidates) = layer_candidates.get(layer) {
                *selected_slot = self.select_diverse_neighbors(candidates, max_nbrs);
            }
        }

        self.connect_with_layer_neighbors(
            doc_id,
            embedding,
            inserted_level,
            &selected_neighbors,
            layer0_max_neighbors,
            upper_layer_max_neighbors,
        )
    }

    /// Connect a freshly-added node using pre-selected neighbor IDs per layer.
    ///
    /// This avoids repeating expensive diversity selection while holding the
    /// write lock in staged parallel build flows.
    fn connect_with_layer_neighbors(
        &mut self,
        doc_id: u64,
        embedding: &[f32],
        inserted_level: usize,
        layer_neighbors: &[Vec<u32>],
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
            self.neighbors_by_layer = vec![LayerAdjacency::new_dense(self.layer0_neighbor_cap, 0)];
            self.entry_by_layer = vec![new_dense];
            self.max_layer = 0;
        }
        if inserted_level > self.max_layer {
            let existing_count = self.dense_to_origin.len() - 1;
            for _ in (self.max_layer + 1)..=inserted_level {
                self.neighbors_by_layer.push(LayerAdjacency::new_sparse(
                    self.upper_layer_neighbor_cap,
                    existing_count,
                ));
                self.entry_by_layer.push(new_dense);
            }
            self.max_layer = inserted_level;
        }
        let effective_level = inserted_level.min(self.max_layer);

        // Add neighbor slot metadata for the new node. Layer 0 is dense and always
        // present; upper layers are sparse and only allocate storage for nodes whose
        // sampled level includes that layer.
        for (layer, layer_nbrs) in self.neighbors_by_layer.iter_mut().enumerate() {
            let present_in_layer = layer == 0 || layer <= effective_level;
            layer_nbrs.push_node(present_in_layer);
        }

        for layer in 0..=effective_level {
            let max_nbrs = if layer == 0 {
                layer0_max_neighbors
            } else {
                upper_layer_max_neighbors
            };
            if max_nbrs == 0 {
                continue;
            }

            let mut selected = Vec::with_capacity(max_nbrs);
            if let Some(neighbors) = layer_neighbors.get(layer) {
                for &nbr in neighbors {
                    if nbr == new_dense || nbr as usize >= self.len() {
                        continue;
                    }
                    if selected.contains(&nbr) {
                        continue;
                    }
                    selected.push(nbr);
                    if selected.len() >= max_nbrs {
                        break;
                    }
                }
            }

            // Forward edges: new_node -> selected neighbors.
            self.set_layer_neighbors(layer, new_dense, &selected);

            // Reverse edges: neighbor -> new_node (bidirectional connectivity).
            let mut reachable = false;
            for &nbr in &selected {
                if self.merge_and_prune_reverse_edge(layer, nbr, new_dense, max_nbrs) {
                    reachable = true;
                }
            }

            // Connectivity guarantee: after pruning, verify at least one reverse
            // edge survives so the new node remains reachable from the rest of the
            // graph. Extreme outliers may lose all reverse edges during pruning
            // because they are the farthest neighbor for every connected node.
            if !reachable {
                if let Some(&closest_nbr) = selected.first() {
                    let _ =
                        self.merge_and_prune_reverse_edge(layer, closest_nbr, new_dense, max_nbrs);
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
            .total_cmp(&other.neg_distance)
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
            .total_cmp(&other.distance)
            .then_with(|| self.dense_id.cmp(&other.dense_id))
    }
}

#[derive(Default)]
struct FlatSearchScratch {
    visited_bits: Vec<u64>,
    touched_dense_ids: Vec<u32>,
    trim_visited_after_query: bool,
    candidates: BinaryHeap<CandidateHeapItem>,
    results: BinaryHeap<ResultHeapItem>,
}

impl FlatSearchScratch {
    #[inline]
    fn clear_visited_bits(&mut self) {
        for dense_id in self.touched_dense_ids.drain(..) {
            let word = (dense_id as usize) >> 6;
            let bit = 1u64 << (dense_id as usize & 63);
            if let Some(slot) = self.visited_bits.get_mut(word) {
                *slot &= !bit;
            }
        }
    }

    fn prepare(&mut self, node_count: usize, target_heap_len: usize) {
        self.clear_visited_bits();
        if self.touched_dense_ids.capacity() > VISITED_TOUCHED_RETAIN_CAPACITY {
            self.touched_dense_ids
                .shrink_to(VISITED_TOUCHED_RETAIN_CAPACITY);
        }

        let required_words = node_count.saturating_add(63) / 64;
        if self.visited_bits.len() < required_words {
            self.visited_bits.resize(required_words, 0);
        }
        self.trim_visited_after_query = required_words > VISITED_BITSET_RETAIN_WORDS;
        if !self.trim_visited_after_query
            && self.visited_bits.capacity() > VISITED_BITSET_RETAIN_WORDS
        {
            self.visited_bits.shrink_to(VISITED_BITSET_RETAIN_WORDS);
        }

        let target_heap_len = target_heap_len.max(16);

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
    fn finish_query(&mut self) {
        self.clear_visited_bits();
        if self.trim_visited_after_query {
            self.visited_bits.clear();
            self.visited_bits.shrink_to(VISITED_BITSET_RETAIN_WORDS);
            self.trim_visited_after_query = false;
        }
    }

    #[cfg(test)]
    #[inline]
    fn was_visited(&self, dense_id: u32) -> bool {
        let idx = dense_id as usize;
        let word = idx >> 6;
        let bit = 1u64 << (idx & 63);
        self.visited_bits
            .get(word)
            .map(|slot| (*slot & bit) != 0)
            .unwrap_or(true)
    }

    #[inline]
    fn mark_visited(&mut self, dense_id: u32) {
        let idx = dense_id as usize;
        let word = idx >> 6;
        let bit = 1u64 << (idx & 63);
        let Some(slot) = self.visited_bits.get_mut(word) else {
            return;
        };
        if (*slot & bit) == 0 {
            *slot |= bit;
            self.touched_dense_ids.push(dense_id);
        }
    }

    #[inline]
    fn mark_if_unvisited(&mut self, dense_id: u32) -> bool {
        let idx = dense_id as usize;
        let word = idx >> 6;
        let bit = 1u64 << (idx & 63);
        let Some(slot) = self.visited_bits.get_mut(word) else {
            return false;
        };
        if (*slot & bit) != 0 {
            return false;
        }
        *slot |= bit;
        self.touched_dense_ids.push(dense_id);
        true
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
    /// Per-layer preselected neighbor IDs for the new node.
    /// Each layer list is deduplicated and capped by that layer's max degree.
    layer_neighbors: Vec<Vec<u32>>,
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
    ///    candidates and preselect per-layer neighbors.
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
                layer_neighbors: Vec::new(),
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
        // run a beam search with ef_construction and preselect neighbors.
        let search_bottom = inserted_level.min(flat.max_layer);
        let mut layer_neighbors: Vec<Vec<u32>> = vec![Vec::new(); search_bottom + 1];

        for layer in (0..=search_bottom).rev() {
            let candidates =
                flat.search_at_layer(embedding, entry, entry_dist, self.ef_construction, layer);
            // Use the nearest found candidate as entry for the next lower layer.
            if let Some(&(best_id, best_dist)) = candidates.first() {
                entry = best_id;
                entry_dist = best_dist;
            }
            let max_nbrs = if layer == 0 {
                self.layer0_neighbor_cap
            } else {
                self.upper_layer_neighbor_cap
            };
            if max_nbrs > 0 {
                layer_neighbors[layer] = flat.select_diverse_neighbors(&candidates, max_nbrs);
            }
        }

        InsertPlan {
            valid: true,
            inserted_level,
            layer_neighbors,
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
        flat.connect_with_layer_neighbors(
            origin_id as u64,
            embedding,
            plan.inserted_level,
            &plan.layer_neighbors,
            self.layer0_neighbor_cap,
            self.upper_layer_neighbor_cap,
        );
    }

    fn insert_into_existing_flat(&self, flat: &mut FlatGraph, embedding: &[f32], origin_id: usize) {
        let plan = self.build_insert_plan(flat, embedding, origin_id);
        self.apply_insert_plan(flat, embedding, origin_id, &plan);
    }

    #[inline]
    fn staging_chunk_size(&self, batch: &[(&[f32], usize)]) -> usize {
        let dim = batch
            .first()
            .map(|(embedding, _)| embedding.len())
            .unwrap_or(0);
        let dim_base = if dim >= 768 {
            PARALLEL_STAGING_CHUNK_SIZE_LARGE
        } else if dim >= 256 {
            PARALLEL_STAGING_CHUNK_SIZE_MEDIUM
        } else {
            PARALLEL_STAGING_CHUNK_SIZE_SMALL
        };

        // Larger worker pools benefit from larger chunks to amortize lock handoffs
        // between read-plan and write-apply phases.
        let thread_scaled = rayon::current_num_threads()
            .max(1)
            .saturating_mul(64)
            .clamp(
                PARALLEL_STAGING_CHUNK_SIZE_SMALL,
                PARALLEL_STAGING_CHUNK_SIZE_LARGE * 2,
            );

        dim_base.max(thread_scaled)
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

        *flat_guard = FlatGraph::new_single(
            self.distance,
            embedding,
            origin_id as u64,
            self.layer0_neighbor_cap,
            self.upper_layer_neighbor_cap,
        );
    }

    fn parallel_insert_slice(&self, batch: &[(&[f32], usize)]) {
        if batch.is_empty() {
            return;
        }

        let chunk_size = self.staging_chunk_size(batch);
        for chunk in batch.chunks(chunk_size) {
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
                    *flat_guard = FlatGraph::new_single(
                        self.distance,
                        embedding,
                        origin_id as u64,
                        self.layer0_neighbor_cap,
                        self.upper_layer_neighbor_cap,
                    );
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
    use std::collections::HashSet;

    fn make_vec(seed: usize) -> Vec<f32> {
        vec![
            seed as f32 * 0.5 + 1.0,
            (seed % 13) as f32 - 2.0,
            (seed % 17) as f32 * 0.25,
            (seed % 19) as f32 * -0.5,
        ]
    }

    fn backend_with_defaults(
        distance: DistanceMetric,
        m: usize,
        max_elements: usize,
        max_layer: usize,
        ef_construction: usize,
    ) -> SingleGraphBackend {
        SingleGraphBackend::new(distance, m, max_elements, max_layer, ef_construction)
    }

    fn seeded_backend() -> SingleGraphBackend {
        let backend = backend_with_defaults(DistanceMetric::Euclidean, 16, 20_000, 8, 200);
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

    fn synthetic_manifold_vec(seed: usize, dim: usize, latent_dim: usize) -> Vec<f32> {
        let latent_dim = latent_dim.max(2);
        let latent = pseudo_random_vec(seed ^ 0x9E37, latent_dim);
        let mut out = Vec::with_capacity(dim);
        let mut state = splitmix64(seed as u64 ^ 0xC0FF_EE12_3456_7890);
        for d in 0..dim {
            let a = latent[d % latent_dim];
            let b = latent[(d * 7 + 3) % latent_dim];
            let c = latent[(d * 11 + 1) % latent_dim];
            state = splitmix64(state);
            let noise = ((state & 0x3FF) as f32 / 1024.0 - 0.5) * 0.02;
            out.push(0.65 * a + 0.25 * b + 0.10 * c + noise);
        }
        out
    }

    fn normalize(v: &mut [f32]) {
        let norm_sq: f32 = v.iter().map(|x| x * x).sum();
        if norm_sq > 0.0 {
            let inv = norm_sq.sqrt().recip();
            for x in v {
                *x *= inv;
            }
        }
    }

    fn recall_against_bruteforce(
        backend: &SingleGraphBackend,
        dataset: &[Vec<f32>],
        distance: DistanceMetric,
        query_indices: &[usize],
        k: usize,
        ef_search: usize,
    ) -> f64 {
        let mut total_hits = 0usize;
        let mut total_expected = 0usize;

        for &query_idx in query_indices {
            let query = &dataset[query_idx];
            let approx = backend.search(query, k, ef_search);
            let approx_ids = approx.iter().map(|r| r.doc_id).collect::<HashSet<_>>();

            let mut exact = (0..dataset.len())
                .map(|idx| (idx as u64, metric_distance(distance, query, &dataset[idx])))
                .collect::<Vec<_>>();
            exact.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
            exact.truncate(k);

            let hits = exact
                .into_iter()
                .filter(|(doc_id, _)| approx_ids.contains(doc_id))
                .count();
            total_hits += hits;
            total_expected += k;
        }

        total_hits as f64 / total_expected as f64
    }

    #[test]
    fn flat_search_scratch_uses_bounded_dense_visited_for_large_graphs() {
        let mut scratch = FlatSearchScratch::default();
        let large_node_count = (VISITED_BITSET_RETAIN_WORDS + 1) * 64;
        scratch.prepare(large_node_count, 128);
        assert!(
            scratch.trim_visited_after_query,
            "large node counts should mark visited bitset for post-query trimming"
        );

        scratch.mark_visited(42);
        assert!(scratch.was_visited(42));
        assert!(
            scratch.visited_bits.len() >= large_node_count / 64,
            "visited bitset should expand to cover dense IDs in large graphs"
        );
        scratch.finish_query();
        assert!(
            scratch.visited_bits.capacity() <= VISITED_BITSET_RETAIN_WORDS * 2,
            "visited bitset should release retained capacity after large query"
        );
    }

    #[test]
    fn flat_search_scratch_releases_excess_buffers_and_heap_capacity() {
        let mut scratch = FlatSearchScratch::default();
        let large_node_count = (VISITED_BITSET_RETAIN_WORDS + 4) * 64;
        scratch.prepare(large_node_count, 128);

        for id in 0..(VISITED_TOUCHED_RETAIN_CAPACITY as u32 * 2) {
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

        let touched_cap_before = scratch.touched_dense_ids.capacity();
        let visited_bits_cap_before = scratch.visited_bits.capacity();
        let candidates_cap_before = scratch.candidates.capacity();
        let results_cap_before = scratch.results.capacity();

        scratch.finish_query();
        scratch.prepare(256, 64);
        assert!(
            scratch.touched_dense_ids.capacity() < touched_cap_before,
            "touched dense-id tracking should release some retained capacity"
        );
        assert!(
            scratch.touched_dense_ids.capacity() <= VISITED_TOUCHED_RETAIN_CAPACITY * 2,
            "touched dense-id tracking should release excess retained capacity"
        );
        assert!(
            scratch.visited_bits.capacity() < visited_bits_cap_before,
            "visited bitset should release some retained capacity"
        );
        assert!(
            scratch.visited_bits.capacity() <= VISITED_BITSET_RETAIN_WORDS * 2,
            "visited bitset should release excess retained capacity"
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
    fn keep_top_k_by_distance_keeps_true_top_k() {
        let mut results = vec![
            SearchResult {
                doc_id: 1,
                distance: 3.0,
            },
            SearchResult {
                doc_id: 2,
                distance: 1.0,
            },
            SearchResult {
                doc_id: 3,
                distance: 2.0,
            },
        ];

        keep_top_k_by_distance(&mut results, 2);

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].doc_id, 2);
        assert_eq!(results[1].doc_id, 3);
        assert!(results[0].distance <= results[1].distance);
    }

    #[test]
    fn backend_inserts_are_immediately_query_visible() {
        let backend = backend_with_defaults(DistanceMetric::Euclidean, 16, 20_000, 8, 200);
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
            !flat.neighbors(0, new_dense).is_empty(),
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
        let mut flat = FlatGraph::new_single(DistanceMetric::Euclidean, &[0.0, 0.0], 1, 8, 4)
            .expect("seed node should create a flat graph");

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
        assert!(flat.neighbors(0, 1).contains(&0));
        assert!(flat.neighbors(0, 0).contains(&1));

        // Upper layers: connected via per-layer candidates.
        assert!(flat.neighbors(1, 1).contains(&0));
        assert!(flat.neighbors(2, 1).contains(&0));
    }

    #[test]
    fn sparse_upper_layers_allocate_only_for_member_nodes() {
        let mut flat = FlatGraph::new_single(DistanceMetric::Euclidean, &[0.0, 0.0], 1, 8, 4)
            .expect("seed node should create a flat graph");

        // Introduce one high-level node so upper layers exist.
        let high_layer_candidates = vec![
            vec![(0u32, 1.0f32)],
            vec![(0u32, 1.0f32)],
            vec![(0u32, 1.0f32)],
            vec![(0u32, 1.0f32)],
        ];
        assert!(flat.connect_with_layer_candidates(
            2,
            &[1.0, 1.0],
            3,
            &high_layer_candidates,
            8,
            4
        ));

        // Insert many layer-0-only nodes; sparse upper layers should not allocate
        // per-node neighbor slots for them.
        for i in 0..128u64 {
            let layer0_candidates = vec![vec![(1u32, 1.0f32), (0u32, 2.0f32)]];
            let x = i as f32 * 0.01 + 2.0;
            assert!(flat.connect_with_layer_candidates(
                10_000 + i,
                &[x, x * 0.5],
                0,
                &layer0_candidates,
                8,
                4
            ));
        }

        assert_eq!(flat.neighbors_by_layer.len(), 4);
        let total_nodes = flat.len();
        // Layer 1..3 should keep slot maps sized by dense-id, but neighbor payload
        // should only be allocated for the single high-level member.
        for layer in 1..=3 {
            let adj = &flat.neighbors_by_layer[layer];
            assert_eq!(adj.dense_to_slot.len(), total_nodes);
            assert_eq!(adj.counts.len(), 1, "layer {} should stay sparse", layer);
            assert_eq!(
                adj.neighbors.len(),
                adj.cap,
                "layer {} payload should have one slot",
                layer
            );
        }
    }

    #[test]
    fn backend_respects_ef_construction_floor() {
        let backend = backend_with_defaults(DistanceMetric::Euclidean, 16, 10_000, 8, 1024);
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

        let backend = backend_with_defaults(DistanceMetric::Euclidean, 24, DATASET_SIZE, 8, 400);
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

    #[test]
    fn backend_recall_regression_guard_hard_euclidean_manifold() {
        const DATASET_SIZE: usize = 768;
        const DIM: usize = 256;
        const LATENT_DIM: usize = 24;
        const K: usize = 10;
        const QUERIES: usize = 48;

        let backend = backend_with_defaults(DistanceMetric::Euclidean, 40, DATASET_SIZE, 8, 800);
        let dataset: Vec<Vec<f32>> = (0..DATASET_SIZE)
            .map(|seed| synthetic_manifold_vec(seed, DIM, LATENT_DIM))
            .collect();
        let batch: Vec<(&[f32], usize)> = dataset
            .iter()
            .enumerate()
            .map(|(id, v)| (v.as_slice(), id))
            .collect();
        backend.parallel_insert_slice(&batch);

        let query_indices = (0..QUERIES)
            .map(|i| i * (DATASET_SIZE / QUERIES))
            .collect::<Vec<_>>();
        let recall_ef256 = recall_against_bruteforce(
            &backend,
            &dataset,
            DistanceMetric::Euclidean,
            &query_indices,
            K,
            256,
        );
        let recall_ef1024 = recall_against_bruteforce(
            &backend,
            &dataset,
            DistanceMetric::Euclidean,
            &query_indices,
            K,
            1024,
        );

        assert!(
            recall_ef1024 + f64::EPSILON >= recall_ef256,
            "recall must be monotonic with larger ef_search on hard euclidean manifold (ef256={:.4}, ef1024={:.4})",
            recall_ef256,
            recall_ef1024
        );
        assert!(
            recall_ef1024 >= 0.88,
            "hard euclidean recall regression: expected >= 0.88 at ef=1024, got {:.4}",
            recall_ef1024
        );
    }

    #[test]
    fn backend_recall_regression_guard_hard_cosine_manifold() {
        const DATASET_SIZE: usize = 768;
        const DIM: usize = 192;
        const LATENT_DIM: usize = 28;
        const K: usize = 10;
        const QUERIES: usize = 48;

        let backend = backend_with_defaults(DistanceMetric::Cosine, 48, DATASET_SIZE, 8, 800);
        let mut dataset: Vec<Vec<f32>> = (0..DATASET_SIZE)
            .map(|seed| synthetic_manifold_vec(seed ^ 0xBEEF, DIM, LATENT_DIM))
            .collect();
        for v in &mut dataset {
            normalize(v);
        }
        let batch: Vec<(&[f32], usize)> = dataset
            .iter()
            .enumerate()
            .map(|(id, v)| (v.as_slice(), id))
            .collect();
        backend.parallel_insert_slice(&batch);

        let query_indices = (0..QUERIES)
            .map(|i| i * (DATASET_SIZE / QUERIES))
            .collect::<Vec<_>>();
        let recall_ef256 = recall_against_bruteforce(
            &backend,
            &dataset,
            DistanceMetric::Cosine,
            &query_indices,
            K,
            256,
        );
        let recall_ef1024 = recall_against_bruteforce(
            &backend,
            &dataset,
            DistanceMetric::Cosine,
            &query_indices,
            K,
            1024,
        );

        assert!(
            recall_ef1024 + f64::EPSILON >= recall_ef256,
            "recall must be monotonic with larger ef_search on hard cosine manifold (ef256={:.4}, ef1024={:.4})",
            recall_ef256,
            recall_ef1024
        );
        assert!(
            recall_ef1024 >= 0.84,
            "hard cosine recall regression: expected >= 0.84 at ef=1024, got {:.4}",
            recall_ef1024
        );
    }
}
