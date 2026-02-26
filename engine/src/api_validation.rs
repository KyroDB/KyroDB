use crate::adaptive_oversampling;
use crate::proto::{InsertRequest, SearchRequest};

/// Maximum embedding dimension to prevent DoS attacks.
pub const MAX_EMBEDDING_DIM: usize = 4096;

/// Maximum k value for k-NN search to prevent excessive computation.
pub const MAX_KNN_K: u32 = 1000;

/// Minimum valid document ID (0 is reserved).
pub const MIN_DOC_ID: u64 = 1;

/// Validated search execution parameters derived from a [`SearchRequest`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SearchValidationPlan {
    pub search_k: usize,
    pub ef_search_override: Option<usize>,
}

/// Validate a search request and derive an execution plan.
///
/// Returns an error string suitable for an `INVALID_ARGUMENT` gRPC status.
#[inline]
pub fn validate_search_request(req: &SearchRequest) -> Result<SearchValidationPlan, String> {
    if req.query_embedding.is_empty() {
        return Err("query_embedding cannot be empty".to_string());
    }
    if req.query_embedding.len() > MAX_EMBEDDING_DIM {
        return Err(format!(
            "query_embedding dimension {} exceeds maximum {}",
            req.query_embedding.len(),
            MAX_EMBEDDING_DIM
        ));
    }
    if req.k == 0 {
        return Err("k must be greater than 0".to_string());
    }
    if req.k > MAX_KNN_K {
        return Err(format!("k must be <= {}", MAX_KNN_K));
    }
    if req.ef_search > 10_000 {
        return Err("ef_search must be <= 10000 (0 = server default)".to_string());
    }

    let has_namespace = !req.namespace.is_empty();
    let base_oversampling = if let Some(ref filter) = req.filter {
        adaptive_oversampling::calculate_oversampling_factor(filter)
    } else {
        1
    };
    let oversampling_factor = if has_namespace {
        base_oversampling.saturating_mul(4).min(10)
    } else {
        base_oversampling
    };
    let search_k = (req.k as usize)
        .saturating_mul(oversampling_factor)
        .min(10_000);

    let ef_search_override = if req.ef_search == 0 {
        None
    } else {
        Some(req.ef_search as usize)
    };

    Ok(SearchValidationPlan {
        search_k,
        ef_search_override,
    })
}

/// Validate an insert request.
///
/// Returns an error string suitable for an `INVALID_ARGUMENT` gRPC status.
#[inline]
pub fn validate_insert_request(req: &InsertRequest) -> Result<(), String> {
    if req.doc_id < MIN_DOC_ID {
        return Err(format!("doc_id must be >= {}", MIN_DOC_ID));
    }
    if req.embedding.is_empty() {
        return Err("embedding cannot be empty".to_string());
    }
    if req.embedding.len() > MAX_EMBEDDING_DIM {
        return Err(format!(
            "embedding dimension {} exceeds maximum {}",
            req.embedding.len(),
            MAX_EMBEDDING_DIM
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{metadata_filter::FilterType, ExactMatch, MetadataFilter};

    fn baseline_search_request() -> SearchRequest {
        SearchRequest {
            query_embedding: vec![0.1; 16],
            k: 10,
            min_score: 0.0,
            namespace: String::new(),
            include_embeddings: false,
            ef_search: 0,
            filter: None,
            metadata_filters: Default::default(),
        }
    }

    #[test]
    fn search_validation_rejects_empty_embedding() {
        let mut req = baseline_search_request();
        req.query_embedding.clear();
        let err = validate_search_request(&req).expect_err("empty embedding must fail");
        assert!(err.contains("query_embedding cannot be empty"));
    }

    #[test]
    fn search_validation_rejects_invalid_k() {
        let mut req = baseline_search_request();
        req.k = 0;
        let err = validate_search_request(&req).expect_err("k=0 must fail");
        assert!(err.contains("k must be greater than 0"));

        req.k = MAX_KNN_K + 1;
        let err = validate_search_request(&req).expect_err("oversized k must fail");
        assert!(err.contains("k must be <="));
    }

    #[test]
    fn search_validation_derives_search_plan() {
        let mut req = baseline_search_request();
        req.filter = Some(MetadataFilter {
            filter_type: Some(FilterType::Exact(ExactMatch {
                key: "k".to_string(),
                value: "v".to_string(),
            })),
        });
        req.namespace = "tenant_ns".to_string();
        req.k = 25;
        req.ef_search = 77;

        let plan = validate_search_request(&req).expect("request should validate");
        assert_eq!(plan.ef_search_override, Some(77));
        assert!(plan.search_k >= 25);
        assert!(plan.search_k <= 10_000);
    }

    #[test]
    fn insert_validation_contract() {
        let valid = InsertRequest {
            doc_id: MIN_DOC_ID,
            embedding: vec![0.1; 16],
            metadata: Default::default(),
            namespace: String::new(),
        };
        validate_insert_request(&valid).expect("valid insert must pass");

        let mut bad_doc = valid.clone();
        bad_doc.doc_id = 0;
        assert!(validate_insert_request(&bad_doc).is_err());

        let mut bad_embedding = valid.clone();
        bad_embedding.embedding.clear();
        assert!(validate_insert_request(&bad_embedding).is_err());
    }
}
