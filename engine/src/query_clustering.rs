//! Query clustering for semantic cache optimization
//!
//! Groups similar queries based on embedding cosine similarity. When one query's
//! result is cached, semantically similar queries can benefit from cluster-wide
//! caching decisions, improving hit rates for queries with similar semantic intent.

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

/// Minimum cosine similarity threshold for query clustering
const DEFAULT_SIMILARITY_THRESHOLD: f32 = 0.85;

/// Maximum cluster size to prevent unbounded growth
const MAX_CLUSTER_SIZE: usize = 50;

/// Query cluster identifier
pub type ClusterId = u64;

/// A cluster of semantically similar queries
#[derive(Clone, Debug)]
pub struct QueryCluster {
    pub id: ClusterId,
    pub centroid: Vec<f32>,
    pub member_hashes: Vec<u64>,
    pub access_count: u64,
}

impl QueryCluster {
    fn new(id: ClusterId, embedding: Vec<f32>, query_hash: u64) -> Self {
        Self {
            id,
            centroid: embedding,
            member_hashes: vec![query_hash],
            access_count: 1,
        }
    }

    fn add_member(&mut self, embedding: &[f32], query_hash: u64) {
        if self.member_hashes.len() >= MAX_CLUSTER_SIZE {
            return;
        }

        self.member_hashes.push(query_hash);
        self.update_centroid(embedding);
        self.access_count += 1;
    }

    fn update_centroid(&mut self, new_embedding: &[f32]) {
        let n = self.member_hashes.len() as f32;
        let weight_old = (n - 1.0) / n;
        let weight_new = 1.0 / n;

        for (i, val) in self.centroid.iter_mut().enumerate() {
            if i < new_embedding.len() {
                *val = *val * weight_old + new_embedding[i] * weight_new;
            }
        }

        normalize_vector(&mut self.centroid);
    }

    #[allow(dead_code)]
    fn contains(&self, query_hash: u64) -> bool {
        self.member_hashes.contains(&query_hash)
    }
}

/// Query clustering engine for semantic grouping
pub struct QueryClusterer {
    clusters: Arc<RwLock<Vec<QueryCluster>>>,
    query_to_cluster: Arc<RwLock<HashMap<u64, ClusterId>>>,
    similarity_threshold: f32,
    next_cluster_id: Arc<RwLock<ClusterId>>,
}

impl QueryClusterer {
    pub fn new(similarity_threshold: f32) -> Self {
        Self {
            clusters: Arc::new(RwLock::new(Vec::new())),
            query_to_cluster: Arc::new(RwLock::new(HashMap::new())),
            similarity_threshold: similarity_threshold.clamp(0.0, 1.0),
            next_cluster_id: Arc::new(RwLock::new(0)),
        }
    }

    pub fn with_default_threshold() -> Self {
        Self::new(DEFAULT_SIMILARITY_THRESHOLD)
    }

    /// Add or update a query in the clustering system
    ///
    /// Returns the cluster ID that the query belongs to
    pub fn add_query(&self, query_hash: u64, embedding: &[f32]) -> Option<ClusterId> {
        if embedding.is_empty() {
            return None;
        }

        let normalized = normalize_embedding(embedding);

        // Check if query already belongs to a cluster
        {
            let query_map = self.query_to_cluster.read();
            if let Some(&cluster_id) = query_map.get(&query_hash) {
                return Some(cluster_id);
            }
        }

        // Find best matching cluster
        let best_match = self.find_best_cluster(&normalized);

        match best_match {
            Some((cluster_idx, _similarity)) => {
                let mut clusters = self.clusters.write();
                let cluster = &mut clusters[cluster_idx];
                let cluster_id = cluster.id;

                cluster.add_member(&normalized, query_hash);

                let mut query_map = self.query_to_cluster.write();
                query_map.insert(query_hash, cluster_id);

                Some(cluster_id)
            }
            None => {
                // Create new cluster
                let cluster_id = {
                    let mut next_id = self.next_cluster_id.write();
                    let id = *next_id;
                    *next_id += 1;
                    id
                };

                let new_cluster = QueryCluster::new(cluster_id, normalized, query_hash);

                let mut clusters = self.clusters.write();
                clusters.push(new_cluster);

                let mut query_map = self.query_to_cluster.write();
                query_map.insert(query_hash, cluster_id);

                Some(cluster_id)
            }
        }
    }

    /// Get cluster ID for a query
    pub fn get_cluster_id(&self, query_hash: u64) -> Option<ClusterId> {
        let query_map = self.query_to_cluster.read();
        query_map.get(&query_hash).copied()
    }

    /// Get all queries in the same cluster as the given query
    pub fn get_cluster_members(&self, query_hash: u64) -> Vec<u64> {
        let cluster_id = match self.get_cluster_id(query_hash) {
            Some(id) => id,
            None => return Vec::new(),
        };

        let clusters = self.clusters.read();
        clusters
            .iter()
            .find(|c| c.id == cluster_id)
            .map(|c| c.member_hashes.clone())
            .unwrap_or_default()
    }

    /// Check if two queries belong to the same cluster
    pub fn are_clustered(&self, query_hash1: u64, query_hash2: u64) -> bool {
        let query_map = self.query_to_cluster.read();
        match (query_map.get(&query_hash1), query_map.get(&query_hash2)) {
            (Some(id1), Some(id2)) => id1 == id2,
            _ => false,
        }
    }

    /// Get cluster statistics
    pub fn stats(&self) -> ClusterStats {
        let clusters = self.clusters.read();
        let query_map = self.query_to_cluster.read();

        let total_clusters = clusters.len();
        let total_queries = query_map.len();
        let avg_cluster_size = if total_clusters > 0 {
            total_queries as f64 / total_clusters as f64
        } else {
            0.0
        };

        let max_cluster_size = clusters.iter().map(|c| c.member_hashes.len()).max().unwrap_or(0);

        ClusterStats {
            total_clusters,
            total_queries,
            avg_cluster_size,
            max_cluster_size,
            similarity_threshold: self.similarity_threshold,
        }
    }

    /// Clear all clusters (useful for testing or reset)
    pub fn clear(&self) {
        let mut clusters = self.clusters.write();
        clusters.clear();

        let mut query_map = self.query_to_cluster.write();
        query_map.clear();

        let mut next_id = self.next_cluster_id.write();
        *next_id = 0;
    }

    fn find_best_cluster(&self, embedding: &[f32]) -> Option<(usize, f32)> {
        let clusters = self.clusters.read();

        let mut best_idx = None;
        let mut best_similarity = self.similarity_threshold;

        for (idx, cluster) in clusters.iter().enumerate() {
            if cluster.member_hashes.len() >= MAX_CLUSTER_SIZE {
                continue;
            }

            let similarity = cosine_similarity(embedding, &cluster.centroid);

            if similarity > best_similarity {
                best_similarity = similarity;
                best_idx = Some(idx);
            }
        }

        best_idx.map(|idx| (idx, best_similarity))
    }
}

/// Clustering statistics
#[derive(Debug, Clone)]
pub struct ClusterStats {
    pub total_clusters: usize,
    pub total_queries: usize,
    pub avg_cluster_size: f64,
    pub max_cluster_size: usize,
    pub similarity_threshold: f32,
}

/// Compute cosine similarity between two vectors
///
/// Returns value in range [-1.0, 1.0] where 1.0 is identical direction
#[inline]
fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }

    let mut dot_product = 0.0_f32;
    let mut norm_a = 0.0_f32;
    let mut norm_b = 0.0_f32;

    for i in 0..a.len() {
        dot_product += a[i] * b[i];
        norm_a += a[i] * a[i];
        norm_b += b[i] * b[i];
    }

    if norm_a == 0.0 || norm_b == 0.0 {
        return 0.0;
    }

    dot_product / (norm_a.sqrt() * norm_b.sqrt())
}

/// Normalize a vector to unit length
fn normalize_vector(v: &mut [f32]) {
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        for val in v.iter_mut() {
            *val /= norm;
        }
    }
}

/// Create a normalized copy of an embedding
fn normalize_embedding(embedding: &[f32]) -> Vec<f32> {
    let mut normalized = embedding.to_vec();
    normalize_vector(&mut normalized);
    normalized
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cosine_similarity_identical() {
        let v1 = vec![1.0, 0.0, 0.0];
        let v2 = vec![1.0, 0.0, 0.0];
        let sim = cosine_similarity(&v1, &v2);
        assert!((sim - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_similarity_orthogonal() {
        let v1 = vec![1.0, 0.0, 0.0];
        let v2 = vec![0.0, 1.0, 0.0];
        let sim = cosine_similarity(&v1, &v2);
        assert!(sim.abs() < 1e-6);
    }

    #[test]
    fn test_cosine_similarity_opposite() {
        let v1 = vec![1.0, 0.0, 0.0];
        let v2 = vec![-1.0, 0.0, 0.0];
        let sim = cosine_similarity(&v1, &v2);
        assert!((sim + 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_normalize_vector() {
        let mut v = vec![3.0, 4.0];
        normalize_vector(&mut v);
        let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((norm - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_query_clusterer_basic() {
        let clusterer = QueryClusterer::with_default_threshold();

        let emb1 = vec![1.0, 0.0, 0.0];
        let emb2 = vec![0.99, 0.1, 0.0];
        let emb3 = vec![0.0, 1.0, 0.0];

        let c1 = clusterer.add_query(1, &emb1);
        let c2 = clusterer.add_query(2, &emb2);
        let c3 = clusterer.add_query(3, &emb3);

        assert!(c1.is_some());
        assert!(c2.is_some());
        assert!(c3.is_some());

        assert!(clusterer.are_clustered(1, 2));
        assert!(!clusterer.are_clustered(1, 3));
    }

    #[test]
    fn test_query_clusterer_get_members() {
        let clusterer = QueryClusterer::with_default_threshold();

        let emb1 = vec![1.0, 0.0, 0.0];
        let emb2 = vec![0.99, 0.1, 0.0];

        clusterer.add_query(100, &emb1);
        clusterer.add_query(200, &emb2);

        let members = clusterer.get_cluster_members(100);
        assert_eq!(members.len(), 2);
        assert!(members.contains(&100));
        assert!(members.contains(&200));
    }

    #[test]
    fn test_cluster_stats() {
        let clusterer = QueryClusterer::with_default_threshold();

        let emb1 = vec![1.0, 0.0, 0.0];
        let emb2 = vec![0.0, 1.0, 0.0];

        clusterer.add_query(1, &emb1);
        clusterer.add_query(2, &emb2);

        let stats = clusterer.stats();
        assert_eq!(stats.total_queries, 2);
        assert_eq!(stats.total_clusters, 2);
    }

    #[test]
    fn test_empty_embedding() {
        let clusterer = QueryClusterer::with_default_threshold();
        let result = clusterer.add_query(1, &[]);
        assert!(result.is_none());
    }

    #[test]
    fn test_max_cluster_size_enforcement() {
        let clusterer = QueryClusterer::new(0.99);

        let emb = vec![1.0, 0.0, 0.0];

        for i in 0..MAX_CLUSTER_SIZE + 10 {
            clusterer.add_query(i as u64, &emb);
        }

        let stats = clusterer.stats();
        assert!(stats.max_cluster_size <= MAX_CLUSTER_SIZE);
    }

    #[test]
    fn test_clear_clusters() {
        let clusterer = QueryClusterer::with_default_threshold();

        let emb = vec![1.0, 0.0, 0.0];
        clusterer.add_query(1, &emb);
        clusterer.add_query(2, &emb);

        let stats_before = clusterer.stats();
        assert_eq!(stats_before.total_queries, 2);

        clusterer.clear();

        let stats_after = clusterer.stats();
        assert_eq!(stats_after.total_queries, 0);
        assert_eq!(stats_after.total_clusters, 0);
    }
}
