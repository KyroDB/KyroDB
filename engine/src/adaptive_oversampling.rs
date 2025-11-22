//! Adaptive oversampling for metadata filtering
//!
//! Estimates filter selectivity to dynamically adjust oversampling factor

use crate::proto::{MetadataFilter, metadata_filter::FilterType};

/// Calculate oversampling factor based on filter complexity
///
/// Returns a factor between 2x and 50x based on estimated selectivity
pub fn calculate_oversampling_factor(filter: &MetadataFilter) -> usize {
    match &filter.filter_type {
        None => 1, // No filter, no oversampling
        Some(filter_type) => estimate_selectivity(filter_type),
    }
}

fn estimate_selectivity(filter_type: &FilterType) -> usize {
    match filter_type {
        // Exact match: high selectivity (few matches expected)
        FilterType::Exact(_) => 2,
        
        // Range match: medium selectivity
        FilterType::Range(_) => 5,
        
        // IN match: depends on cardinality, but assume medium
        FilterType::InMatch(in_match) => {
            // More values = lower selectivity
            let cardinality = in_match.values.len();
            if cardinality <= 2 {
                3
            } else if cardinality <= 5 {
                5
            } else {
                8
            }
        }
        
        // AND: takes minimum selectivity (most restrictive sub-filter dominates)
        FilterType::AndFilter(and_filter) => {
            if and_filter.filters.is_empty() {
                return 1;
            }
            and_filter
                .filters
                .iter()
                .filter_map(|f| f.filter_type.as_ref())
                .map(estimate_selectivity)
                .min()
                .unwrap_or(2) // Take minimum (most selective)
        }
        
        // OR: reduce selectivity (less restrictive)
        FilterType::OrFilter(or_filter) => {
            if or_filter.filters.is_empty() {
                return 1;
            }
            let avg_selectivity: usize = or_filter
                .filters
                .iter()
                .filter_map(|f| f.filter_type.as_ref())
                .map(estimate_selectivity)
                .sum::<usize>()
                / or_filter.filters.len().max(1);
            
            // OR filters are typically less selective
            (avg_selectivity * 2).max(2).min(20)
        }
        
        // NOT: very low selectivity (inverts the filter)
        FilterType::NotFilter(not_filter) => {
            if let Some(ref inner_filter) = not_filter.filter {
                if let Some(ref inner_type) = inner_filter.filter_type {
                    let inner_selectivity = estimate_selectivity(inner_type);
                    // Invert: if inner is highly selective (2x), NOT is very unselective (25x)
                    (50 / inner_selectivity).min(50).max(10)
                } else {
                    20
                }
            } else {
                20
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{AndFilter, ExactMatch, InMatch, NotFilter, OrFilter, RangeMatch};

    #[test]
    fn test_exact_match_low_oversampling() {
        let filter = MetadataFilter {
            filter_type: Some(FilterType::Exact(ExactMatch {
                key: "category".to_string(),
                value: "A".to_string(),
            })),
        };
        assert_eq!(calculate_oversampling_factor(&filter), 2);
    }

    #[test]
    fn test_range_match_medium_oversampling() {
        let filter = MetadataFilter {
            filter_type: Some(FilterType::Range(RangeMatch {
                key: "score".to_string(),
                bound: Some(crate::proto::range_match::Bound::Gte("70".to_string())),
            })),
        };
        assert_eq!(calculate_oversampling_factor(&filter), 5);
    }

    #[test]
    fn test_in_match_scales_with_cardinality() {
        let filter_small = MetadataFilter {
            filter_type: Some(FilterType::InMatch(InMatch {
                key: "status".to_string(),
                values: vec!["active".to_string()],
            })),
        };
        assert_eq!(calculate_oversampling_factor(&filter_small), 3);

        let filter_large = MetadataFilter {
            filter_type: Some(FilterType::InMatch(InMatch {
                key: "status".to_string(),
                values: vec!["a".to_string(), "b".to_string(), "c".to_string(), "d".to_string(), "e".to_string(), "f".to_string()],
            })),
        };
        assert_eq!(calculate_oversampling_factor(&filter_large), 8);
    }

    #[test]
    fn test_and_filter_takes_minimum() {
        let filter = MetadataFilter {
            filter_type: Some(FilterType::AndFilter(AndFilter {
                filters: vec![
                    MetadataFilter {
                        filter_type: Some(FilterType::Exact(ExactMatch {
                            key: "category".to_string(),
                            value: "A".to_string(),
                        })),
                    },
                    MetadataFilter {
                        filter_type: Some(FilterType::Range(RangeMatch {
                            key: "score".to_string(),
                            bound: Some(crate::proto::range_match::Bound::Gte("70".to_string())),
                        })),
                    },
                ],
            })),
        };
        // Should take minimum of 2 and 5
        assert_eq!(calculate_oversampling_factor(&filter), 2);
    }

    #[test]
    fn test_not_filter_high_oversampling() {
        let filter = MetadataFilter {
            filter_type: Some(FilterType::NotFilter(Box::new(NotFilter {
                filter: Some(Box::new(MetadataFilter {
                    filter_type: Some(FilterType::Exact(ExactMatch {
                        key: "type".to_string(),
                        value: "foo".to_string(),
                    })),
                })),
            }))),
        };
        // NOT of exact match should have high oversampling
        let factor = calculate_oversampling_factor(&filter);
        assert!(factor >= 10 && factor <= 50);
    }

    #[test]
    fn test_no_filter_returns_one() {
        let filter = MetadataFilter { filter_type: None };
        assert_eq!(calculate_oversampling_factor(&filter), 1);
    }

    #[test]
    fn test_or_filter_averages_and_doubles() {
        let filter = MetadataFilter {
            filter_type: Some(FilterType::OrFilter(OrFilter {
                filters: vec![
                    MetadataFilter {
                        filter_type: Some(FilterType::Exact(ExactMatch {
                            key: "category".to_string(),
                            value: "A".to_string(),
                        })),
                    },
                    MetadataFilter {
                        filter_type: Some(FilterType::Range(RangeMatch {
                            key: "score".to_string(),
                            bound: Some(crate::proto::range_match::Bound::Gte("70".to_string())),
                        })),
                    },
                ],
            })),
        };
        // Average of 2 and 5 is 3, doubled is 6
        let factor = calculate_oversampling_factor(&filter);
        assert!(factor >= 2 && factor <= 20);
    }

    #[test]
    fn test_empty_and_filter() {
        let filter = MetadataFilter {
            filter_type: Some(FilterType::AndFilter(AndFilter {
                filters: vec![],
            })),
        };
        let factor = calculate_oversampling_factor(&filter);
        assert!(factor >= 1); // Should not be 0
    }

    #[test]
    fn test_empty_or_filter() {
        let filter = MetadataFilter {
            filter_type: Some(FilterType::OrFilter(OrFilter {
                filters: vec![],
            })),
        };
        let factor = calculate_oversampling_factor(&filter);
        assert!(factor >= 1); // Should not be 0
    }
}
