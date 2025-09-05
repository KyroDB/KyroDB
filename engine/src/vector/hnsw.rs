//! Native HNSW (Hierarchical Navigable Small World) implementation
//!
//! High-performance vector similarity search using HNSW algorithm with SIMD optimization.
//! This implementation is optimized for real-time search with incremental updates.

use super::distance::{DistanceFunction, SIMDDistance};
use super::storage::{VectorStorage, VectorRecord};
use super::{IndexStats, SearchStats, VectorQuery, VectorSearchResult};
use crate::schema::{DistanceMetric, Document, HnswConfig};
use ahash::{AHashMap, AHashSet};
use anyhow::{Context, Result};
use parking_lot::RwLock;
use rand::Rng;
use smallvec::SmallVec;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Arc;
use std::time::Instant;

/// Node identifier in the HNSW graph
pub type NodeId = usize;

/// Search result with distance and node information
#[derive(Debug, Clone)]
pub struct SearchResult {
    pub node_id: NodeId,
    pub distance: f32,
}

impl PartialEq for SearchResult {
    fn eq(&self, other: &Self) -> bool {
        self.distance == other.distance
    }
}

impl Eq for SearchResult {}

impl PartialOrd for SearchResult {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        // Reverse ordering for min-heap behavior
        other.distance.partial_cmp(&self.distance)
    }
}

impl Ord for SearchResult {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap_or(std::cmp::Ordering::Equal)
    }
}

/// HNSW node representing a point in the graph
#[derive(Debug, Clone)]
struct HnswNode {
    /// Node ID (maps to vector storage index)
    id: NodeId,
    /// Connections at each level (level 0 is the base layer)
    connections: Vec<SmallVec<[NodeId; 16]>>,
    /// Maximum level this node exists at
    max_level: usize,
}

impl HnswNode {
    fn new(id: NodeId, max_level: usize) -> Self {
        let mut connections = Vec::with_capacity(max_level + 1);
        for _ in 0..=max_level {
            connections.push(SmallVec::new());
        }
        
        Self {
            id,
            connections,
            max_level,
        }
    }

    fn add_connection(&mut self, level: usize, neighbor: NodeId) {
        if level <= self.max_level && !self.connections[level].contains(&neighbor) {
            self.connections[level].push(neighbor);
        }
    }

    fn remove_connection(&mut self, level: usize, neighbor: NodeId) {
        if level <= self.max_level {
            if let Some(pos) = self.connections[level].iter().position(|&x| x == neighbor) {
                self.connections[level].remove(pos);
            }
        }
    }

    fn get_connections(&self, level: usize) -> &[NodeId] {
        if level <= self.max_level {
            &self.connections[level]
        } else {
            &[]
        }
    }
}

/// High-performance HNSW index implementation
pub struct HnswIndex {
    /// Vector storage backend
    storage: Arc<RwLock<VectorStorage>>,
    /// HNSW graph nodes
    nodes: RwLock<Vec<Option<HnswNode>>>,
    /// Entry point for search (highest level node)
    entry_point: RwLock<Option<NodeId>>,
    /// Configuration parameters
    config: HnswConfig,
    /// Distance function
    distance_fn: Box<dyn DistanceFunction>,
    /// Node ID to storage index mapping
    node_to_storage: RwLock<AHashMap<NodeId, usize>>,
    /// Next available node ID
    next_node_id: RwLock<NodeId>,
    /// Performance statistics
    stats: RwLock<IndexStats>,
}

impl HnswIndex {
    /// Create a new HNSW index with the given configuration
    pub fn new(config: HnswConfig, dimension: usize) -> Self {
        let storage = Arc::new(RwLock::new(VectorStorage::new(dimension)));
        let distance_fn = SIMDDistance::new(config.distance_metric);

        let stats = IndexStats {
            vector_count: 0,
            dimension,
            memory_usage: 0,
            levels: 0,
            avg_connections: 0.0,
            last_search: None,
        };

        Self {
            storage,
            nodes: RwLock::new(Vec::new()),
            entry_point: RwLock::new(None),
            config,
            distance_fn,
            node_to_storage: RwLock::new(AHashMap::new()),
            next_node_id: RwLock::new(0),
            stats: RwLock::new(stats),
        }
    }

    /// Insert a vector into the index
    pub fn insert(&mut self, record: VectorRecord) -> Result<()> {
        let start_time = Instant::now();
        
        // Insert into storage first
        let mut storage = self.storage.write();
        let storage_index = storage.len();
        storage.insert(record.clone())?;
        drop(storage);

        // Generate level for new node
        let level = self.generate_random_level();
        let node_id = {
            let mut next_id = self.next_node_id.write();
            let id = *next_id;
            *next_id += 1;
            id
        };

        // Create HNSW node
        let node = HnswNode::new(node_id, level);
        
        // Update mappings
        {
            let mut nodes = self.nodes.write();
            if node_id >= nodes.len() {
                nodes.resize(node_id + 1, None);
            }
            nodes[node_id] = Some(node);
        }
        
        {
            let mut mapping = self.node_to_storage.write();
            mapping.insert(node_id, storage_index);
        }

        // Insert into HNSW graph
        self.insert_node(node_id, level, &record.vector)?;
        
        // Update statistics
        {
            let mut stats = self.stats.write();
            stats.vector_count += 1;
            stats.levels = stats.levels.max(level + 1);
            stats.memory_usage = self.calculate_memory_usage();
        }

        Ok(())
    }

    /// Search for k nearest neighbors
    pub fn search(&self, query: &VectorQuery) -> Result<Vec<VectorSearchResult>> {
        let start_time = Instant::now();
        let mut distance_calculations = 0;
        let mut nodes_visited = 0;

        let ef = query.ef.unwrap_or(self.config.ef_search);
        let entry_point = {
            let entry = self.entry_point.read();
            *entry
        };

        if entry_point.is_none() {
            return Ok(Vec::new());
        }

        let entry_point = entry_point.unwrap();
        
        // Get entry point level
        let max_level = {
            let nodes = self.nodes.read();
            nodes[entry_point].as_ref().unwrap().max_level
        };

        // Search from top level to level 1
        let mut current_closest = vec![SearchResult {
            node_id: entry_point,
            distance: self.compute_distance(entry_point, &query.vector)?,
        }];
        distance_calculations += 1;
        nodes_visited += 1;

        for level in (1..=max_level).rev() {
            current_closest = self.search_layer(
                &query.vector,
                &current_closest,
                1,
                level,
                &mut distance_calculations,
                &mut nodes_visited,
            )?;
        }

        // Search level 0 with ef parameter
        let candidates = self.search_layer(
            &query.vector,
            &current_closest,
            ef.max(query.k),
            0,
            &mut distance_calculations,
            &mut nodes_visited,
        )?;

        // Convert to final results
        let storage = self.storage.read();
        let mut results = Vec::new();
        
        for candidate in candidates.into_iter().take(query.k) {
            let storage_index = {
                let mapping = self.node_to_storage.read();
                *mapping.get(&candidate.node_id).unwrap()
            };
            
            if let Some((_, record)) = storage.get_record_by_index(storage_index) {
                // Apply similarity threshold if specified
                if let Some(threshold) = query.similarity_threshold {
                    match query.distance_metric {
                        DistanceMetric::Cosine | DistanceMetric::Euclidean | DistanceMetric::Manhattan => {
                            if candidate.distance > threshold {
                                continue;
                            }
                        }
                        DistanceMetric::DotProduct => {
                            if -candidate.distance < threshold {
                                continue;
                            }
                        }
                    }
                }

                let document = record.to_document()?;
                results.push(VectorSearchResult {
                    id: record.id,
                    score: candidate.distance,
                    document: Some(document),
                });
            }
        }

        // Update search statistics
        {
            let mut stats = self.stats.write();
            stats.last_search = Some(SearchStats {
                duration_micros: start_time.elapsed().as_micros() as u64,
                distance_calculations,
                nodes_visited,
                ef_used: ef,
            });
        }

        Ok(results)
    }

    /// Remove a vector from the index
    pub fn remove(&mut self, id: u64) -> Result<bool> {
        // Find the node ID for this vector ID
        let storage = self.storage.read();
        let storage_index = storage.get_index_by_id(id);
        drop(storage);

        let storage_index = match storage_index {
            Some(idx) => idx,
            None => return Ok(false),
        };

        // Find node ID
        let node_id = {
            let mapping = self.node_to_storage.read();
            mapping.iter()
                .find(|(_, &storage_idx)| storage_idx == storage_index)
                .map(|(&node_id, _)| node_id)
        };

        let node_id = match node_id {
            Some(id) => id,
            None => return Ok(false),
        };

        // Remove from HNSW graph
        self.remove_node(node_id)?;

        // Remove from storage
        let mut storage = self.storage.write();
        storage.remove(id)?;

        // Update statistics
        {
            let mut stats = self.stats.write();
            stats.vector_count = storage.len();
            stats.memory_usage = self.calculate_memory_usage();
        }

        Ok(true)
    }

    /// Get index statistics
    pub fn stats(&self) -> IndexStats {
        let stats = self.stats.read();
        stats.clone()
    }

    /// Get the number of vectors in the index
    pub fn len(&self) -> usize {
        let storage = self.storage.read();
        storage.len()
    }

    /// Check if the index is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    // Private helper methods

    fn insert_node(&self, node_id: NodeId, level: usize, vector: &[f32]) -> Result<()> {
        let entry_point = {
            let entry = self.entry_point.read();
            *entry
        };

        if entry_point.is_none() {
            // First node becomes entry point
            let mut entry = self.entry_point.write();
            *entry = Some(node_id);
            return Ok(());
        }

        let entry_point = entry_point.unwrap();

        // Search for closest points at each level
        let mut current_closest = vec![SearchResult {
            node_id: entry_point,
            distance: self.compute_distance(entry_point, vector)?,
        }];

        let entry_level = {
            let nodes = self.nodes.read();
            nodes[entry_point].as_ref().unwrap().max_level
        };

        // Search from top to target level + 1
        for search_level in ((level + 1)..=entry_level).rev() {
            current_closest = self.search_layer(
                vector,
                &current_closest,
                1,
                search_level,
                &mut 0,
                &mut 0,
            )?;
        }

        // Insert at each level from target down to 0
        for insert_level in (0..=level).rev() {
            let candidates = self.search_layer(
                vector,
                &current_closest,
                self.config.ef_construction,
                insert_level,
                &mut 0,
                &mut 0,
            )?;

            // Select neighbors
            let neighbors = self.select_neighbors(&candidates, self.config.max_connections);
            
            // Add bidirectional connections
            {
                let mut nodes = self.nodes.write();
                
                // First add connection from node to neighbors
                if let Some(node) = nodes[node_id].as_mut() {
                    for neighbor in &neighbors {
                        node.add_connection(insert_level, neighbor.node_id);
                    }
                }
                
                // Then add reverse connections and prune if needed
                for neighbor in &neighbors {
                    if let Some(neighbor_node) = nodes[neighbor.node_id].as_mut() {
                        neighbor_node.add_connection(insert_level, node_id);
                        
                        // Prune connections if necessary
                        if neighbor_node.get_connections(insert_level).len() > self.config.max_connections {
                            // Note: For simplicity, just remove the last connection
                            // In a full implementation, this would be more sophisticated
                            if neighbor_node.max_level >= insert_level {
                                neighbor_node.connections[insert_level].pop();
                            }
                        }
                    }
                }
            }

            current_closest = neighbors;
        }

        // Update entry point if necessary
        if level > entry_level {
            let mut entry = self.entry_point.write();
            *entry = Some(node_id);
        }

        Ok(())
    }

    fn remove_node(&self, node_id: NodeId) -> Result<()> {
        let max_level = {
            let nodes = self.nodes.read();
            if let Some(ref node) = nodes[node_id] {
                node.max_level
            } else {
                return Ok(()); // Node already removed
            }
        };

        // Remove all connections to this node
        for level in 0..=max_level {
            let connections = {
                let nodes = self.nodes.read();
                if let Some(ref node) = nodes[node_id] {
                    node.get_connections(level).to_vec()
                } else {
                    Vec::new()
                }
            };

            let mut nodes = self.nodes.write();
            for neighbor_id in connections {
                if let Some(ref mut neighbor) = nodes[neighbor_id] {
                    neighbor.remove_connection(level, node_id);
                }
            }
        }

        // Remove the node itself
        {
            let mut nodes = self.nodes.write();
            nodes[node_id] = None;
        }

        // Update entry point if necessary
        {
            let entry = self.entry_point.read();
            if *entry == Some(node_id) {
                drop(entry);
                // Find new entry point (highest level remaining node)
                let nodes = self.nodes.read();
                let mut new_entry = None;
                let mut max_level = 0;
                
                for (id, node_opt) in nodes.iter().enumerate() {
                    if let Some(node) = node_opt {
                        if node.max_level >= max_level {
                            max_level = node.max_level;
                            new_entry = Some(id);
                        }
                    }
                }
                
                let mut entry = self.entry_point.write();
                *entry = new_entry;
            }
        }

        // Remove from mappings
        {
            let mut mapping = self.node_to_storage.write();
            mapping.remove(&node_id);
        }

        Ok(())
    }

    fn search_layer(
        &self,
        query: &[f32],
        entry_points: &[SearchResult],
        num_closest: usize,
        level: usize,
        distance_calculations: &mut usize,
        nodes_visited: &mut usize,
    ) -> Result<Vec<SearchResult>> {
        let mut visited = AHashSet::new();
        let mut candidates = BinaryHeap::new();
        let mut w = BinaryHeap::new();

        // Initialize with entry points
        for entry in entry_points {
            let distance = if visited.insert(entry.node_id) {
                *distance_calculations += 1;
                *nodes_visited += 1;
                self.compute_distance(entry.node_id, query)?
            } else {
                entry.distance
            };

            let result = SearchResult {
                node_id: entry.node_id,
                distance,
            };

            candidates.push(Reverse(result.clone()));
            w.push(result);
        }

        while let Some(Reverse(current)) = candidates.pop() {
            // Early termination condition
            if !w.is_empty() && current.distance > w.peek().unwrap().distance {
                break;
            }

            // Explore neighbors
            let neighbors = {
                let nodes = self.nodes.read();
                if let Some(ref node) = nodes[current.node_id] {
                    node.get_connections(level).to_vec()
                } else {
                    Vec::new()
                }
            };

            for &neighbor_id in &neighbors {
                if visited.insert(neighbor_id) {
                    *distance_calculations += 1;
                    *nodes_visited += 1;
                    
                    let distance = self.compute_distance(neighbor_id, query)?;
                    
                    if w.len() < num_closest || distance < w.peek().unwrap().distance {
                        let result = SearchResult {
                            node_id: neighbor_id,
                            distance,
                        };
                        
                        candidates.push(Reverse(result.clone()));
                        w.push(result);
                        
                        if w.len() > num_closest {
                            w.pop();
                        }
                    }
                }
            }
        }

        Ok(w.into_sorted_vec())
    }

    fn select_neighbors(&self, candidates: &[SearchResult], max_connections: usize) -> Vec<SearchResult> {
        // Simple greedy selection (can be improved with more sophisticated heuristics)
        candidates.iter()
            .take(max_connections)
            .cloned()
            .collect()
    }

    fn prune_connections(&self, node_id: NodeId, level: usize, _query: &[f32]) {
        // Simple pruning strategy: remove farthest connection
        let mut nodes = self.nodes.write();
        if let Some(ref mut node) = nodes[node_id] {
            if node.connections[level].len() > self.config.max_connections {
                // For simplicity, just remove the last connection
                // In practice, you'd want to remove the least useful connection
                node.connections[level].pop();
            }
        }
    }

    fn compute_distance(&self, node_id: NodeId, query: &[f32]) -> Result<f32> {
        let storage_index = {
            let mapping = self.node_to_storage.read();
            *mapping.get(&node_id).ok_or_else(|| {
                anyhow::anyhow!("Node {} not found in storage mapping", node_id)
            })?
        };

        let storage = self.storage.read();
        let vector = storage.get_vector_by_index(storage_index)
            .ok_or_else(|| anyhow::anyhow!("Vector not found in storage at index {}", storage_index))?;

        Ok(self.distance_fn.distance(vector, query))
    }

    fn generate_random_level(&self) -> usize {
        let mut rng = rand::thread_rng();
        let mut level = 0;
        while level < 16 && rng.gen::<f32>() < self.config.level_multiplier {
            level += 1;
        }
        level
    }

    fn calculate_memory_usage(&self) -> usize {
        let storage_usage = {
            let storage = self.storage.read();
            storage.memory_usage()
        };
        
        let nodes_usage = {
            let nodes = self.nodes.read();
            nodes.len() * std::mem::size_of::<Option<HnswNode>>()
        };
        
        let mapping_usage = {
            let mapping = self.node_to_storage.read();
            mapping.len() * (std::mem::size_of::<NodeId>() + std::mem::size_of::<usize>())
        };

        storage_usage + nodes_usage + mapping_usage
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{DistanceMetric, HnswConfig};

    #[test]
    fn test_hnsw_basic_operations() {
        let config = HnswConfig::default();
        let mut index = HnswIndex::new(config, 3);

        // Insert some vectors
        let record1 = VectorRecord {
            id: 1,
            vector: vec![1.0, 0.0, 0.0],
            collection: "test".to_string(),
            metadata: Default::default(),
        };

        let record2 = VectorRecord {
            id: 2,
            vector: vec![0.0, 1.0, 0.0],
            collection: "test".to_string(),
            metadata: Default::default(),
        };

        index.insert(record1).unwrap();
        index.insert(record2).unwrap();

        assert_eq!(index.len(), 2);

        // Search
        let query = VectorQuery {
            vector: vec![1.0, 0.0, 0.0],
            k: 1,
            distance_metric: DistanceMetric::Euclidean,
            ef: None,
            similarity_threshold: None,
        };

        let results = index.search(&query).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, 1);

        // Remove
        assert!(index.remove(1).unwrap());
        assert_eq!(index.len(), 1);
    }

    #[test]
    fn test_hnsw_empty_index() {
        let config = HnswConfig::default();
        let index = HnswIndex::new(config, 3);

        let query = VectorQuery {
            vector: vec![1.0, 0.0, 0.0],
            k: 1,
            distance_metric: DistanceMetric::Euclidean,
            ef: None,
            similarity_threshold: None,
        };

        let results = index.search(&query).unwrap();
        assert!(results.is_empty());
    }
}
