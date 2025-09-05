use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use tokio::sync::RwLock;

use crate::vector::{VectorQuery, VectorSearchResult};
use crate::text::{TextQuery, TextSearchResult};
use crate::metadata::{MetadataQuery, MetadataSearchResult};
use crate::schema::{DistanceMetric, Value};

/// Hybrid search query combining multiple search modalities
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HybridQuery {
    /// Collection to search in
    pub collection: String,
    /// Vector search component (optional)
    pub vector: Option<VectorQuery>,
    /// Text search component (optional)
    pub text: Option<TextQuery>,
    /// Metadata filter component (optional)
    pub metadata: Option<MetadataQuery>,
    /// Maximum number of results to return
    pub limit: usize,
    /// Scoring configuration
    pub scoring: HybridScoringConfig,
    /// Result fusion strategy
    pub fusion: FusionStrategy,
}

/// Configuration for hybrid search scoring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HybridScoringConfig {
    /// Weight for vector similarity scores (0.0 - 1.0)
    pub vector_weight: f32,
    /// Weight for text relevance scores (0.0 - 1.0)
    pub text_weight: f32,
    /// Weight for metadata match scores (0.0 - 1.0)
    pub metadata_weight: f32,
    /// Boost factor for documents matching all modalities
    pub multi_modal_boost: f32,
    /// Minimum score threshold for results
    pub min_score: Option<f32>,
}

impl Default for HybridScoringConfig {
    fn default() -> Self {
        Self {
            vector_weight: 0.7,
            text_weight: 0.2,
            metadata_weight: 0.1,
            multi_modal_boost: 1.2,
            min_score: None,
        }
    }
}

/// Strategies for fusing results from different search modalities
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FusionStrategy {
    /// Weighted sum of normalized scores
    WeightedSum,
    /// Rank-based fusion (RRF - Reciprocal Rank Fusion)
    RankFusion,
    /// Maximum score across modalities
    MaxScore,
    /// Minimum score across modalities (must match all)
    MinScore,
    /// Product of normalized scores
    Product,
    /// Custom fusion with configurable parameters
    Custom {
        /// Alpha parameter for combining scores
        alpha: f32,
        /// Beta parameter for rank decay
        beta: f32,
    },
}

impl Default for FusionStrategy {
    fn default() -> Self {
        Self::WeightedSum
    }
}

/// Individual result from a specific search modality
#[derive(Debug, Clone)]
struct ModalityResult {
    document_id: u64,
    score: f32,
    rank: usize,
    modality: SearchModality,
}

/// Search modality type
#[derive(Debug, Clone, PartialEq, Eq)]
enum SearchModality {
    Vector,
    Text,
    Metadata,
}

/// Combined hybrid search result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HybridSearchResult {
    /// Document ID
    pub document_id: u64,
    /// Final combined score
    pub score: f32,
    /// Scores from individual modalities
    pub modality_scores: ModalityScores,
    /// Number of modalities that matched this document
    pub modality_count: u8,
    /// Rank in final results
    pub rank: usize,
    /// Optional document metadata
    pub metadata: Option<serde_json::Value>,
    /// Text snippets (if text search was performed)
    pub text_snippets: Option<HashMap<String, String>>,
}

/// Scores from individual search modalities
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModalityScores {
    /// Vector similarity score
    pub vector: Option<f32>,
    /// Text relevance score
    pub text: Option<f32>,
    /// Metadata match score
    pub metadata: Option<f32>,
}

/// Hybrid search executor
pub struct HybridSearchExecutor {
    /// Vector search indexes
    vector_indexes: Arc<RwLock<HashMap<String, Arc<RwLock<crate::vector::HnswIndex>>>>>,
    /// Text search index
    text_index: Arc<crate::text::TextIndexWrapper>,
    /// Metadata search index
    metadata_index: Arc<crate::metadata::MetadataIndex>,
    /// Document store for retrieving metadata
    document_store: Arc<RwLock<HashMap<u64, crate::schema::Document>>>,
}

impl HybridSearchExecutor {
    pub fn new(
        vector_indexes: Arc<RwLock<HashMap<String, Arc<RwLock<crate::vector::HnswIndex>>>>>,
        text_index: Arc<crate::text::TextIndexWrapper>,
        metadata_index: Arc<crate::metadata::MetadataIndex>,
        document_store: Arc<RwLock<HashMap<u64, crate::schema::Document>>>,
    ) -> Self {
        Self {
            vector_indexes,
            text_index,
            metadata_index,
            document_store,
        }
    }
    
    /// Execute hybrid search query
    pub async fn search(&self, query: &HybridQuery) -> Result<Vec<HybridSearchResult>> {
        let mut modality_results: Vec<ModalityResult> = Vec::new();
        
        // Execute vector search if specified
        if let Some(vector_query) = &query.vector {
            let vector_results = self.execute_vector_search(&query.collection, vector_query).await?;
            for (rank, result) in vector_results.into_iter().enumerate() {
                modality_results.push(ModalityResult {
                    document_id: result.id,
                    score: result.score,
                    rank,
                    modality: SearchModality::Vector,
                });
            }
        }
        
        // Execute text search if specified
        if let Some(text_query) = &query.text {
            let text_results = self.execute_text_search(text_query).await?;
            for (rank, result) in text_results.into_iter().enumerate() {
                modality_results.push(ModalityResult {
                    document_id: result.document_id,
                    score: result.score,
                    rank,
                    modality: SearchModality::Text,
                });
            }
        }
        
        // Execute metadata search if specified
        if let Some(metadata_query) = &query.metadata {
            let metadata_results = self.execute_metadata_search(metadata_query).await?;
            for (rank, document_id) in metadata_results.into_iter().enumerate() {
                modality_results.push(ModalityResult {
                    document_id,
                    score: 1.0, // Binary match for metadata
                    rank,
                    modality: SearchModality::Metadata,
                });
            }
        }
        
        // Fuse results from different modalities
        let fused_results = self.fuse_results(modality_results, query).await?;
        
        // Apply final filtering and ranking
        let mut final_results = fused_results;
        
        // Apply minimum score threshold
        if let Some(min_score) = query.scoring.min_score {
            final_results.retain(|r| r.score >= min_score);
        }
        
        // Sort by final score
        final_results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
        
        // Apply limit
        final_results.truncate(query.limit);
        
        // Update ranks in final results
        for (rank, result) in final_results.iter_mut().enumerate() {
            result.rank = rank;
        }
        
        Ok(final_results)
    }
    
    async fn execute_vector_search(
        &self,
        collection: &str,
        query: &VectorQuery,
    ) -> Result<Vec<VectorSearchResult>> {
        let vector_indexes = self.vector_indexes.read().await;
        
        if let Some(vector_index) = vector_indexes.get(collection) {
            let index = vector_index.read().await;
            index.search(query)
        } else {
            Ok(Vec::new())
        }
    }
    
    async fn execute_text_search(&self, query: &TextQuery) -> Result<Vec<TextSearchResult>> {
        self.text_index.search(query).await
    }
    
    async fn execute_metadata_search(&self, query: &MetadataQuery) -> Result<Vec<u64>> {
        self.metadata_index.query(query).await
    }
    
    async fn fuse_results(
        &self,
        modality_results: Vec<ModalityResult>,
        query: &HybridQuery,
    ) -> Result<Vec<HybridSearchResult>> {
        // Group results by document ID
        let mut document_results: HashMap<u64, Vec<ModalityResult>> = HashMap::new();
        
        for result in modality_results {
            document_results
                .entry(result.document_id)
                .or_insert_with(Vec::new)
                .push(result);
        }
        
        let mut hybrid_results = Vec::new();
        
        for (document_id, results) in document_results {
            let hybrid_result = self.compute_hybrid_score(document_id, results, query).await?;
            hybrid_results.push(hybrid_result);
        }
        
        Ok(hybrid_results)
    }
    
    async fn compute_hybrid_score(
        &self,
        document_id: u64,
        results: Vec<ModalityResult>,
        query: &HybridQuery,
    ) -> Result<HybridSearchResult> {
        let mut modality_scores = ModalityScores {
            vector: None,
            text: None,
            metadata: None,
        };
        
        let mut text_snippets = None;
        
        // Extract scores from each modality
        for result in &results {
            match result.modality {
                SearchModality::Vector => {
                    modality_scores.vector = Some(result.score);
                }
                SearchModality::Text => {
                    modality_scores.text = Some(result.score);
                    // Get text snippets from original text search
                    if let Some(text_query) = &query.text {
                        let text_results = self.text_index.search(text_query).await?;
                        for text_result in text_results {
                            if text_result.document_id == document_id {
                                text_snippets = Some(text_result.snippets);
                                break;
                            }
                        }
                    }
                }
                SearchModality::Metadata => {
                    modality_scores.metadata = Some(result.score);
                }
            }
        }
        
        // Compute final score based on fusion strategy
        let final_score = match &query.fusion {
            FusionStrategy::WeightedSum => {
                self.compute_weighted_sum_score(&modality_scores, &query.scoring)
            }
            FusionStrategy::RankFusion => {
                self.compute_rank_fusion_score(&results)
            }
            FusionStrategy::MaxScore => {
                self.compute_max_score(&modality_scores)
            }
            FusionStrategy::MinScore => {
                self.compute_min_score(&modality_scores)
            }
            FusionStrategy::Product => {
                self.compute_product_score(&modality_scores, &query.scoring)
            }
            FusionStrategy::Custom { alpha, beta } => {
                self.compute_custom_score(&modality_scores, &results, *alpha, *beta)
            }
        };
        
        // Apply multi-modal boost
        let modality_count = results.len() as u8;
        let boosted_score = if modality_count > 1 {
            final_score * query.scoring.multi_modal_boost
        } else {
            final_score
        };
        
        // Get document metadata
        let metadata = {
            let document_store = self.document_store.read().await;
            document_store.get(&document_id)
                .map(|doc| serde_json::to_value(&doc.metadata).ok())
                .flatten()
        };
        
        Ok(HybridSearchResult {
            document_id,
            score: boosted_score,
            modality_scores,
            modality_count,
            rank: 0, // Will be set later
            metadata,
            text_snippets,
        })
    }
    
    fn compute_weighted_sum_score(
        &self,
        modality_scores: &ModalityScores,
        config: &HybridScoringConfig,
    ) -> f32 {
        let mut total_score = 0.0;
        let mut total_weight = 0.0;
        
        if let Some(vector_score) = modality_scores.vector {
            total_score += vector_score * config.vector_weight;
            total_weight += config.vector_weight;
        }
        
        if let Some(text_score) = modality_scores.text {
            total_score += text_score * config.text_weight;
            total_weight += config.text_weight;
        }
        
        if let Some(metadata_score) = modality_scores.metadata {
            total_score += metadata_score * config.metadata_weight;
            total_weight += config.metadata_weight;
        }
        
        if total_weight > 0.0 {
            total_score / total_weight
        } else {
            0.0
        }
    }
    
    fn compute_rank_fusion_score(&self, results: &[ModalityResult]) -> f32 {
        // Reciprocal Rank Fusion
        const K: f32 = 60.0; // RRF constant
        
        let mut rrf_score = 0.0;
        for result in results {
            rrf_score += 1.0 / (K + result.rank as f32 + 1.0);
        }
        
        rrf_score
    }
    
    fn compute_max_score(&self, modality_scores: &ModalityScores) -> f32 {
        [
            modality_scores.vector.unwrap_or(0.0),
            modality_scores.text.unwrap_or(0.0),
            modality_scores.metadata.unwrap_or(0.0),
        ]
        .iter()
        .fold(0.0f32, |acc, &x| acc.max(x))
    }
    
    fn compute_min_score(&self, modality_scores: &ModalityScores) -> f32 {
        let scores: Vec<f32> = [
            modality_scores.vector,
            modality_scores.text,
            modality_scores.metadata,
        ]
        .iter()
        .filter_map(|&s| s)
        .collect();
        
        if scores.is_empty() {
            0.0
        } else {
            scores.iter().fold(f32::INFINITY, |acc, &x| acc.min(x))
        }
    }
    
    fn compute_product_score(
        &self,
        modality_scores: &ModalityScores,
        config: &HybridScoringConfig,
    ) -> f32 {
        let mut product = 1.0;
        let mut count = 0;
        
        if let Some(vector_score) = modality_scores.vector {
            product *= vector_score.powf(config.vector_weight);
            count += 1;
        }
        
        if let Some(text_score) = modality_scores.text {
            product *= text_score.powf(config.text_weight);
            count += 1;
        }
        
        if let Some(metadata_score) = modality_scores.metadata {
            product *= metadata_score.powf(config.metadata_weight);
            count += 1;
        }
        
        if count > 0 {
            product.powf(1.0 / count as f32)
        } else {
            0.0
        }
    }
    
    fn compute_custom_score(
        &self,
        modality_scores: &ModalityScores,
        results: &[ModalityResult],
        alpha: f32,
        beta: f32,
    ) -> f32 {
        // Custom scoring: combine weighted scores with rank-based decay
        let weighted_score = self.compute_weighted_sum_score(
            modality_scores,
            &HybridScoringConfig::default(),
        );
        
        let rank_decay: f32 = results
            .iter()
            .map(|r| 1.0 / (1.0 + beta * r.rank as f32))
            .sum::<f32>()
            / results.len() as f32;
        
        alpha * weighted_score + (1.0 - alpha) * rank_decay
    }
}

use std::sync::Arc;

/// Hybrid search statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HybridSearchStats {
    /// Total number of hybrid queries executed
    pub total_queries: u64,
    /// Average query execution time (milliseconds)
    pub avg_query_time_ms: f64,
    /// Number of vector search operations
    pub vector_searches: u64,
    /// Number of text search operations
    pub text_searches: u64,
    /// Number of metadata filter operations
    pub metadata_searches: u64,
    /// Distribution of modality combinations
    pub modality_combinations: HashMap<String, u64>,
}

impl Default for HybridSearchStats {
    fn default() -> Self {
        Self {
            total_queries: 0,
            avg_query_time_ms: 0.0,
            vector_searches: 0,
            text_searches: 0,
            metadata_searches: 0,
            modality_combinations: HashMap::new(),
        }
    }
}
