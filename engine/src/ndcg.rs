//! NDCG (Normalized Discounted Cumulative Gain) Quality Metrics
//!
//! Measures ranking quality for search and cache prediction.
//! Critical for validating that learned cache caches the RIGHT documents,
//! not just popular ones.
//!
//! # Formulas
//!
//! DCG@k = Σ(rel_i / log2(i+1)) for i=1 to k
//! NDCG@k = DCG@k / IDCG@k (normalized by ideal DCG)
//!
//! # Use Cases
//!
//! 1. **Cache Quality**: Do cached docs match future queries?
//! 2. **Search Quality**: Are k-NN results relevant?
//! 3. **Prediction Quality**: Does RMI predict relevant docs?

use std::collections::HashMap;

/// Single ranking result with position and relevance
#[derive(Debug, Clone)]
pub struct RankingResult {
    pub doc_id: u64,
    pub position: usize,  // 0-indexed
    pub relevance: f64,   // Ground truth relevance (0.0 to 1.0+)
}

/// Calculate DCG (Discounted Cumulative Gain) at position k
///
/// DCG@k = Σ(rel_i / log2(i+1)) for i=1 to k
///
/// # Arguments
/// * `results` - Ranked list of results with relevance scores
/// * `k` - Number of top results to consider
///
/// # Returns
/// DCG score (higher is better)
pub fn calculate_dcg(results: &[RankingResult], k: usize) -> f64 {
    results
        .iter()
        .take(k)
        .map(|result| {
            let position = result.position + 1; // Convert to 1-indexed
            result.relevance / (position as f64 + 1.0).log2()
        })
        .sum()
}

/// Calculate ideal DCG (IDCG) for perfect ranking
///
/// IDCG@k = DCG with results sorted by relevance (descending)
///
/// # Arguments
/// * `relevance_scores` - All relevance scores for query
/// * `k` - Number of top results to consider
///
/// # Returns
/// IDCG score (maximum possible DCG)
pub fn calculate_idcg(relevance_scores: &[f64], k: usize) -> f64 {
    let mut sorted_relevance = relevance_scores.to_vec();
    sorted_relevance.sort_by(|a, b| b.partial_cmp(a).unwrap());
    
    sorted_relevance
        .iter()
        .take(k)
        .enumerate()
        .map(|(i, &rel)| rel / ((i + 2) as f64).log2())
        .sum()
}

/// Calculate NDCG (Normalized Discounted Cumulative Gain) at position k
///
/// NDCG@k = DCG@k / IDCG@k
///
/// Score ranges from 0.0 (worst) to 1.0 (perfect ranking).
///
/// # Arguments
/// * `results` - Ranked list of results with relevance scores
/// * `k` - Number of top results to consider (e.g., 10 for NDCG@10)
///
/// # Returns
/// NDCG score (0.0 to 1.0, higher is better)
///
/// # Example
/// ```
/// use kyrodb_engine::ndcg::{RankingResult, calculate_ndcg};
///
/// let results = vec![
///     RankingResult { doc_id: 1, position: 0, relevance: 1.0 },  // Perfect match at top
///     RankingResult { doc_id: 2, position: 1, relevance: 0.5 },
///     RankingResult { doc_id: 3, position: 2, relevance: 0.0 },
/// ];
///
/// let ndcg = calculate_ndcg(&results, 10);
/// assert!(ndcg > 0.9); // High NDCG for relevant doc at position 0
/// ```
pub fn calculate_ndcg(results: &[RankingResult], k: usize) -> f64 {
    if results.is_empty() {
        return 0.0;
    }
    
    let dcg = calculate_dcg(results, k);
    let relevance_scores: Vec<f64> = results.iter().map(|r| r.relevance).collect();
    let idcg = calculate_idcg(&relevance_scores, k);
    
    if idcg == 0.0 {
        return 0.0; // No relevant results
    }
    
    dcg / idcg
}

/// Calculate MRR (Mean Reciprocal Rank)
///
/// MRR = 1 / rank_of_first_relevant_result
///
/// # Arguments
/// * `results` - Ranked list of results with relevance scores
///
/// # Returns
/// MRR score (0.0 to 1.0, higher is better)
pub fn calculate_mrr(results: &[RankingResult]) -> f64 {
    for result in results {
        if result.relevance > 0.0 {
            return 1.0 / (result.position + 1) as f64;
        }
    }
    0.0 // No relevant results found
}

/// Calculate Recall@k
///
/// Recall@k = |relevant_in_top_k| / |total_relevant|
///
/// # Arguments
/// * `results` - Ranked list of results with relevance scores
/// * `k` - Number of top results to consider
/// * `total_relevant` - Total number of relevant documents
///
/// # Returns
/// Recall score (0.0 to 1.0, higher is better)
pub fn calculate_recall_at_k(results: &[RankingResult], k: usize, total_relevant: usize) -> f64 {
    if total_relevant == 0 {
        return 0.0;
    }
    
    let relevant_in_top_k = results
        .iter()
        .take(k)
        .filter(|r| r.relevance > 0.0)
        .count();
    
    relevant_in_top_k as f64 / total_relevant as f64
}

/// Batch calculate NDCG@k for multiple queries
///
/// # Arguments
/// * `query_results` - Map of query_id → ranked results
/// * `k` - Number of top results to consider
///
/// # Returns
/// Average NDCG@k across all queries
pub fn calculate_mean_ndcg(
    query_results: &HashMap<u64, Vec<RankingResult>>,
    k: usize,
) -> f64 {
    if query_results.is_empty() {
        return 0.0;
    }
    
    let total_ndcg: f64 = query_results
        .values()
        .map(|results| calculate_ndcg(results, k))
        .sum();
    
    total_ndcg / query_results.len() as f64
}

/// Cache quality metrics for validation
#[derive(Debug, Clone)]
pub struct CacheQualityMetrics {
    /// NDCG@10 for cache admission policy
    pub ndcg_at_10: f64,
    
    /// MRR (Mean Reciprocal Rank)
    pub mrr: f64,
    
    /// Recall@10
    pub recall_at_10: f64,
    
    /// Number of queries evaluated
    pub num_queries: usize,
    
    /// Average relevance of cached documents
    pub avg_cached_relevance: f64,
}

impl CacheQualityMetrics {
    pub fn new() -> Self {
        Self {
            ndcg_at_10: 0.0,
            mrr: 0.0,
            recall_at_10: 0.0,
            num_queries: 0,
            avg_cached_relevance: 0.0,
        }
    }
}

impl Default for CacheQualityMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dcg_calculation() {
        let results = vec![
            RankingResult { doc_id: 1, position: 0, relevance: 3.0 },
            RankingResult { doc_id: 2, position: 1, relevance: 2.0 },
            RankingResult { doc_id: 3, position: 2, relevance: 1.0 },
        ];
        
        let dcg = calculate_dcg(&results, 3);
        // DCG = 3/log2(2) + 2/log2(3) + 1/log2(4)
        //     = 3.0 + 1.26 + 0.5 = 4.76
        assert!(dcg > 4.7 && dcg < 4.8);
    }

    #[test]
    fn test_idcg_calculation() {
        let relevance_scores = vec![1.0, 2.0, 3.0]; // Unsorted
        let idcg = calculate_idcg(&relevance_scores, 3);
        
        // IDCG should sort to [3.0, 2.0, 1.0]
        // IDCG = 3/log2(2) + 2/log2(3) + 1/log2(4)
        assert!(idcg > 4.7 && idcg < 4.8);
    }

    #[test]
    fn test_ndcg_perfect_ranking() {
        let results = vec![
            RankingResult { doc_id: 1, position: 0, relevance: 3.0 },
            RankingResult { doc_id: 2, position: 1, relevance: 2.0 },
            RankingResult { doc_id: 3, position: 2, relevance: 1.0 },
        ];
        
        let ndcg = calculate_ndcg(&results, 3);
        assert!((ndcg - 1.0).abs() < 0.01); // Perfect ranking = 1.0
    }

    #[test]
    fn test_ndcg_imperfect_ranking() {
        let results = vec![
            RankingResult { doc_id: 1, position: 0, relevance: 1.0 }, // Should be 3rd
            RankingResult { doc_id: 2, position: 1, relevance: 2.0 },
            RankingResult { doc_id: 3, position: 2, relevance: 3.0 }, // Should be 1st
        ];
        
        let ndcg = calculate_ndcg(&results, 3);
        assert!(ndcg < 1.0); // Imperfect ranking
        assert!(ndcg > 0.5); // But not terrible
    }

    #[test]
    fn test_ndcg_no_relevant_results() {
        let results = vec![
            RankingResult { doc_id: 1, position: 0, relevance: 0.0 },
            RankingResult { doc_id: 2, position: 1, relevance: 0.0 },
        ];
        
        let ndcg = calculate_ndcg(&results, 2);
        assert_eq!(ndcg, 0.0);
    }

    #[test]
    fn test_mrr_first_result_relevant() {
        let results = vec![
            RankingResult { doc_id: 1, position: 0, relevance: 1.0 },
            RankingResult { doc_id: 2, position: 1, relevance: 0.0 },
        ];
        
        let mrr = calculate_mrr(&results);
        assert_eq!(mrr, 1.0); // First result is relevant = MRR 1.0
    }

    #[test]
    fn test_mrr_second_result_relevant() {
        let results = vec![
            RankingResult { doc_id: 1, position: 0, relevance: 0.0 },
            RankingResult { doc_id: 2, position: 1, relevance: 1.0 },
        ];
        
        let mrr = calculate_mrr(&results);
        assert_eq!(mrr, 0.5); // Second result = MRR 1/2
    }

    #[test]
    fn test_recall_at_k() {
        let results = vec![
            RankingResult { doc_id: 1, position: 0, relevance: 1.0 },
            RankingResult { doc_id: 2, position: 1, relevance: 0.0 },
            RankingResult { doc_id: 3, position: 2, relevance: 1.0 },
        ];
        
        let recall = calculate_recall_at_k(&results, 2, 2);
        assert_eq!(recall, 0.5); // 1 out of 2 relevant docs in top-2
    }

    #[test]
    fn test_mean_ndcg_multiple_queries() {
        let mut query_results = HashMap::new();
        
        // Query 1: Perfect ranking
        query_results.insert(1, vec![
            RankingResult { doc_id: 1, position: 0, relevance: 1.0 },
        ]);
        
        // Query 2: No relevant results
        query_results.insert(2, vec![
            RankingResult { doc_id: 2, position: 0, relevance: 0.0 },
        ]);
        
        let mean_ndcg = calculate_mean_ndcg(&query_results, 10);
        assert_eq!(mean_ndcg, 0.5); // (1.0 + 0.0) / 2
    }

    #[test]
    fn test_ndcg_at_10_with_binary_relevance() {
        // MS MARCO-style binary relevance (0 or 1)
        let results = vec![
            RankingResult { doc_id: 1, position: 0, relevance: 0.0 },
            RankingResult { doc_id: 2, position: 1, relevance: 0.0 },
            RankingResult { doc_id: 3, position: 2, relevance: 1.0 }, // Target at position 3
            RankingResult { doc_id: 4, position: 3, relevance: 0.0 },
        ];
        
        let ndcg = calculate_ndcg(&results, 10);
        // DCG = 0 + 0 + 1/log2(4) + 0 = 0.5
        // IDCG = 1/log2(2) = 1.0
        // NDCG = 0.5 / 1.0 = 0.5
        assert!((ndcg - 0.5).abs() < 0.01);
    }
}
