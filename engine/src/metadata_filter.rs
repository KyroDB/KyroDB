use crate::proto::{
    metadata_filter::FilterType, AndFilter, ExactMatch, InMatch, MetadataFilter, NotFilter,
    OrFilter, RangeMatch,
};
use std::collections::HashMap;

/// Evaluates if a document's metadata matches the given filter.
pub fn matches(filter: &MetadataFilter, metadata: &HashMap<String, String>) -> bool {
    match &filter.filter_type {
        Some(FilterType::Exact(f)) => matches_exact(f, metadata),
        Some(FilterType::Range(f)) => matches_range(f, metadata),
        Some(FilterType::InMatch(f)) => matches_in(f, metadata),
        Some(FilterType::AndFilter(f)) => matches_and(f, metadata),
        Some(FilterType::OrFilter(f)) => matches_or(f, metadata),
        Some(FilterType::NotFilter(f)) => matches_not(f, metadata),
        None => true, // Empty filter matches everything
    }
}

fn matches_exact(filter: &ExactMatch, metadata: &HashMap<String, String>) -> bool {
    match metadata.get(&filter.key) {
        Some(val) => val == &filter.value,
        None => false,
    }
}

fn matches_range(filter: &RangeMatch, metadata: &HashMap<String, String>) -> bool {
    let val_str = match metadata.get(&filter.key) {
        Some(v) => v,
        None => return false,
    };

    // Try parsing as number first
    if let (Ok(val_num), Ok(bound_num)) = (val_str.parse::<f64>(), get_bound_value(filter).parse::<f64>()) {
        return match &filter.bound {
            Some(crate::proto::range_match::Bound::Gte(_)) => val_num >= bound_num,
            Some(crate::proto::range_match::Bound::Lte(_)) => val_num <= bound_num,
            Some(crate::proto::range_match::Bound::Gt(_)) => val_num > bound_num,
            Some(crate::proto::range_match::Bound::Lt(_)) => val_num < bound_num,
            None => true,
        };
    }

    // Fallback to string comparison (works for ISO8601 dates)
    let bound_str = get_bound_value(filter);
    match &filter.bound {
        Some(crate::proto::range_match::Bound::Gte(_)) => val_str >= &bound_str,
        Some(crate::proto::range_match::Bound::Lte(_)) => val_str <= &bound_str,
        Some(crate::proto::range_match::Bound::Gt(_)) => val_str > &bound_str,
        Some(crate::proto::range_match::Bound::Lt(_)) => val_str < &bound_str,
        None => true,
    }
}

fn get_bound_value(filter: &RangeMatch) -> String {
    match &filter.bound {
        Some(crate::proto::range_match::Bound::Gte(v)) => v.clone(),
        Some(crate::proto::range_match::Bound::Lte(v)) => v.clone(),
        Some(crate::proto::range_match::Bound::Gt(v)) => v.clone(),
        Some(crate::proto::range_match::Bound::Lt(v)) => v.clone(),
        None => String::new(),
    }
}

fn matches_in(filter: &InMatch, metadata: &HashMap<String, String>) -> bool {
    match metadata.get(&filter.key) {
        Some(val) => filter.values.contains(val),
        None => false,
    }
}

fn matches_and(filter: &AndFilter, metadata: &HashMap<String, String>) -> bool {
    for sub_filter in &filter.filters {
        if !matches(sub_filter, metadata) {
            return false;
        }
    }
    true
}

fn matches_or(filter: &OrFilter, metadata: &HashMap<String, String>) -> bool {
    if filter.filters.is_empty() {
        return false; // Empty OR is false (SQL semantics)
    }
    for sub_filter in &filter.filters {
        if matches(sub_filter, metadata) {
            return true;
        }
    }
    false
}

fn matches_not(filter: &NotFilter, metadata: &HashMap<String, String>) -> bool {
    match &filter.filter {
        Some(sub_filter) => !matches(sub_filter, metadata),
        None => false, // NOT (Empty) -> NOT (True) -> False
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{metadata_filter::FilterType, ExactMatch, MetadataFilter};

    #[test]
    fn test_exact_match() {
        let mut meta = HashMap::new();
        meta.insert("tool".to_string(), "kubectl".to_string());

        let filter = MetadataFilter {
            filter_type: Some(FilterType::Exact(ExactMatch {
                key: "tool".to_string(),
                value: "kubectl".to_string(),
            })),
        };

        assert!(matches(&filter, &meta));

        let filter_fail = MetadataFilter {
            filter_type: Some(FilterType::Exact(ExactMatch {
                key: "tool".to_string(),
                value: "docker".to_string(),
            })),
        };

        assert!(!matches(&filter_fail, &meta));
    }

    #[test]
    fn test_range_match() {
        let mut meta = HashMap::new();
        meta.insert("age".to_string(), "25".to_string());
        meta.insert("date".to_string(), "2023-01-01".to_string());

        // Numeric range
        let filter_gte = MetadataFilter {
            filter_type: Some(FilterType::Range(RangeMatch {
                key: "age".to_string(),
                bound: Some(crate::proto::range_match::Bound::Gte("18".to_string())),
            })),
        };
        assert!(matches(&filter_gte, &meta));

        let filter_lt = MetadataFilter {
            filter_type: Some(FilterType::Range(RangeMatch {
                key: "age".to_string(),
                bound: Some(crate::proto::range_match::Bound::Lt("20".to_string())),
            })),
        };
        assert!(!matches(&filter_lt, &meta));

        // String range (date)
        let filter_date = MetadataFilter {
            filter_type: Some(FilterType::Range(RangeMatch {
                key: "date".to_string(),
                bound: Some(crate::proto::range_match::Bound::Gt("2022-12-31".to_string())),
            })),
        };
        assert!(matches(&filter_date, &meta));
    }

    #[test]
    fn test_in_match() {
        let mut meta = HashMap::new();
        meta.insert("status".to_string(), "active".to_string());

        let filter_in = MetadataFilter {
            filter_type: Some(FilterType::InMatch(InMatch {
                key: "status".to_string(),
                values: vec!["pending".to_string(), "active".to_string()],
            })),
        };
        assert!(matches(&filter_in, &meta));

        let filter_not_in = MetadataFilter {
            filter_type: Some(FilterType::InMatch(InMatch {
                key: "status".to_string(),
                values: vec!["pending".to_string(), "archived".to_string()],
            })),
        };
        assert!(!matches(&filter_not_in, &meta));
    }

    #[test]
    fn test_and_filter() {
        let mut meta = HashMap::new();
        meta.insert("a".to_string(), "1".to_string());
        meta.insert("b".to_string(), "2".to_string());

        let filter = MetadataFilter {
            filter_type: Some(FilterType::AndFilter(AndFilter {
                filters: vec![
                    MetadataFilter {
                        filter_type: Some(FilterType::Exact(ExactMatch {
                            key: "a".to_string(),
                            value: "1".to_string(),
                        })),
                    },
                    MetadataFilter {
                        filter_type: Some(FilterType::Exact(ExactMatch {
                            key: "b".to_string(),
                            value: "2".to_string(),
                        })),
                    },
                ],
            })),
        };
        assert!(matches(&filter, &meta));

        // Empty AND should be true (identity for AND)
        let empty_and = MetadataFilter {
            filter_type: Some(FilterType::AndFilter(AndFilter { filters: vec![] })),
        };
        assert!(matches(&empty_and, &meta));
    }

    #[test]
    fn test_or_filter() {
        let mut meta = HashMap::new();
        meta.insert("a".to_string(), "1".to_string());

        let filter = MetadataFilter {
            filter_type: Some(FilterType::OrFilter(OrFilter {
                filters: vec![
                    MetadataFilter {
                        filter_type: Some(FilterType::Exact(ExactMatch {
                            key: "a".to_string(),
                            value: "1".to_string(),
                        })),
                    },
                    MetadataFilter {
                        filter_type: Some(FilterType::Exact(ExactMatch {
                            key: "a".to_string(),
                            value: "2".to_string(),
                        })),
                    },
                ],
            })),
        };
        assert!(matches(&filter, &meta));

        // Empty OR should be false (identity for OR)
        let empty_or = MetadataFilter {
            filter_type: Some(FilterType::OrFilter(OrFilter { filters: vec![] })),
        };
        assert!(!matches(&empty_or, &meta));
    }

    #[test]
    fn test_not_filter() {
        let mut meta = HashMap::new();
        meta.insert("a".to_string(), "1".to_string());

        let filter = MetadataFilter {
            filter_type: Some(FilterType::NotFilter(Box::new(NotFilter {
                filter: Some(Box::new(MetadataFilter {
                    filter_type: Some(FilterType::Exact(ExactMatch {
                        key: "a".to_string(),
                        value: "2".to_string(),
                    })),
                })),
            }))),
        };
        assert!(matches(&filter, &meta));

        // NOT(None) -> NOT(True) -> False
        let not_empty = MetadataFilter {
            filter_type: Some(FilterType::NotFilter(Box::new(NotFilter { filter: None }))),
        };
        assert!(!matches(&not_empty, &meta));
    }

    #[test]
    fn test_missing_key() {
        let meta = HashMap::new();
        let filter = MetadataFilter {
            filter_type: Some(FilterType::Exact(ExactMatch {
                key: "missing".to_string(),
                value: "val".to_string(),
            })),
        };
        assert!(!matches(&filter, &meta));
    }

    #[test]
    fn test_invalid_numeric_range() {
        let mut meta = HashMap::new();
        meta.insert("val".to_string(), "not_a_number".to_string());

        // Should fall back to string comparison
        let filter = MetadataFilter {
            filter_type: Some(FilterType::Range(RangeMatch {
                key: "val".to_string(),
                bound: Some(crate::proto::range_match::Bound::Gt("abc".to_string())),
            })),
        };
        // "not_a_number" > "abc" is true
        assert!(matches(&filter, &meta));
    }

    #[test]
    fn test_range_match_comprehensive() {
        let mut meta = HashMap::new();
        meta.insert("price".to_string(), "100.5".to_string());

        // Test all bounds
        let cases = vec![
            (crate::proto::range_match::Bound::Gt("100.0".to_string()), true),
            (crate::proto::range_match::Bound::Gt("100.5".to_string()), false),
            (crate::proto::range_match::Bound::Gte("100.5".to_string()), true),
            (crate::proto::range_match::Bound::Lt("101.0".to_string()), true),
            (crate::proto::range_match::Bound::Lt("100.5".to_string()), false),
            (crate::proto::range_match::Bound::Lte("100.5".to_string()), true),
        ];

        for (bound, expected) in cases {
            let filter = MetadataFilter {
                filter_type: Some(FilterType::Range(RangeMatch {
                    key: "price".to_string(),
                    bound: Some(bound),
                })),
            };
            assert_eq!(matches(&filter, &meta), expected, "Failed for bound {:?}", filter);
        }
    }

    #[test]
    fn test_complex_nested_filters() {
        let mut meta = HashMap::new();
        meta.insert("region".to_string(), "us-east".to_string());
        meta.insert("env".to_string(), "prod".to_string());
        meta.insert("version".to_string(), "1.0".to_string());

        // (region=us-east AND env=prod) AND NOT (version=2.0)
        let filter = MetadataFilter {
            filter_type: Some(FilterType::AndFilter(AndFilter {
                filters: vec![
                    MetadataFilter {
                        filter_type: Some(FilterType::AndFilter(AndFilter {
                            filters: vec![
                                MetadataFilter {
                                    filter_type: Some(FilterType::Exact(ExactMatch {
                                        key: "region".to_string(),
                                        value: "us-east".to_string(),
                                    })),
                                },
                                MetadataFilter {
                                    filter_type: Some(FilterType::Exact(ExactMatch {
                                        key: "env".to_string(),
                                        value: "prod".to_string(),
                                    })),
                                },
                            ],
                        })),
                    },
                    MetadataFilter {
                        filter_type: Some(FilterType::NotFilter(Box::new(NotFilter {
                            filter: Some(Box::new(MetadataFilter {
                                filter_type: Some(FilterType::Exact(ExactMatch {
                                    key: "version".to_string(),
                                    value: "2.0".to_string(),
                                })),
                            })),
                        }))),
                    },
                ],
            })),
        };

        assert!(matches(&filter, &meta));
    }
}
