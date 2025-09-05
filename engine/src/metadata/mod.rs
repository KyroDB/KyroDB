use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, BTreeMap};
use std::sync::Arc;
use tokio::sync::RwLock;
use ahash::AHashMap;
use smallvec::SmallVec;
use crate::schema::{Value, FieldType};

/// Metadata query operators
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueryOperator {
    /// Exact equality
    Eq(Value),
    /// Not equal
    Ne(Value),
    /// Greater than
    Gt(Value),
    /// Greater than or equal
    Gte(Value),
    /// Less than
    Lt(Value),
    /// Less than or equal
    Lte(Value),
    /// Value in set
    In(Vec<Value>),
    /// Value not in set
    NotIn(Vec<Value>),
    /// String contains substring
    Contains(String),
    /// String starts with prefix
    StartsWith(String),
    /// String ends with suffix
    EndsWith(String),
    /// String matches regex pattern
    Regex(String),
    /// Field exists
    Exists,
    /// Field does not exist
    NotExists,
    /// Range query (min, max, inclusive_min, inclusive_max)
    Range(Value, Value, bool, bool),
}

/// Logical operators for combining metadata queries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogicalOperator {
    And,
    Or,
    Not,
}

/// Metadata query condition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataCondition {
    pub field: String,
    pub operator: QueryOperator,
}

/// Complex metadata query with logical operators
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetadataQuery {
    /// Single condition
    Condition(MetadataCondition),
    /// Logical combination of queries
    Logical {
        operator: LogicalOperator,
        queries: Vec<MetadataQuery>,
    },
}

impl MetadataQuery {
    /// Create an equality condition
    pub fn eq(field: String, value: Value) -> Self {
        Self::Condition(MetadataCondition {
            field,
            operator: QueryOperator::Eq(value),
        })
    }
    
    /// Create a range condition
    pub fn range(field: String, min: Value, max: Value, inclusive: bool) -> Self {
        Self::Condition(MetadataCondition {
            field,
            operator: QueryOperator::Range(min, max, inclusive, inclusive),
        })
    }
    
    /// Create a text contains condition
    pub fn contains(field: String, text: String) -> Self {
        Self::Condition(MetadataCondition {
            field,
            operator: QueryOperator::Contains(text),
        })
    }
    
    /// Combine queries with AND
    pub fn and(queries: Vec<MetadataQuery>) -> Self {
        Self::Logical {
            operator: LogicalOperator::And,
            queries,
        }
    }
    
    /// Combine queries with OR
    pub fn or(queries: Vec<MetadataQuery>) -> Self {
        Self::Logical {
            operator: LogicalOperator::Or,
            queries,
        }
    }
    
    /// Negate a query
    pub fn not(query: MetadataQuery) -> Self {
        Self::Logical {
            operator: LogicalOperator::Not,
            queries: vec![query],
        }
    }
}

/// Metadata index for a specific field
#[derive(Debug)]
struct FieldIndex {
    field_type: FieldType,
    // Value -> Set of document IDs
    value_index: AHashMap<Value, SmallVec<[u64; 8]>>,
    // Range queries (for numeric fields)
    sorted_values: BTreeMap<String, SmallVec<[u64; 8]>>, // String representation for sorting
    // Text search index (for string fields)
    text_index: Option<AHashMap<String, SmallVec<[u64; 8]>>>, // Word -> document IDs
}

impl FieldIndex {
    fn new(field_type: FieldType) -> Self {
        let has_text_index = matches!(field_type, FieldType::String);
        Self {
            field_type,
            value_index: AHashMap::new(),
            sorted_values: BTreeMap::new(),
            text_index: if has_text_index {
                Some(AHashMap::new())
            } else {
                None
            },
        }
    }
    
    fn insert(&mut self, document_id: u64, value: &Value) {
        // Insert into value index
        self.value_index
            .entry(value.clone())
            .or_insert_with(SmallVec::new)
            .push(document_id);
        
        // Insert into sorted index for range queries
        let sort_key = self.value_to_sort_key(value);
        self.sorted_values
            .entry(sort_key)
            .or_insert_with(SmallVec::new)
            .push(document_id);
        
        // Insert into text index for string fields
        if let (Some(text_index), Value::String(text)) = (&mut self.text_index, value) {
            for word in text.split_whitespace() {
                let word_lower = word.to_lowercase();
                text_index
                    .entry(word_lower)
                    .or_insert_with(SmallVec::new)
                    .push(document_id);
            }
        }
    }
    
    fn remove(&mut self, document_id: u64, value: &Value) {
        // Remove from value index
        if let Some(doc_ids) = self.value_index.get_mut(value) {
            doc_ids.retain(|id| *id != document_id);
            if doc_ids.is_empty() {
                self.value_index.remove(value);
            }
        }
        
        // Remove from sorted index
        let sort_key = self.value_to_sort_key(value);
        if let Some(doc_ids) = self.sorted_values.get_mut(&sort_key) {
            doc_ids.retain(|id| *id != document_id);
            if doc_ids.is_empty() {
                self.sorted_values.remove(&sort_key);
            }
        }
        
        // Remove from text index
        if let (Some(text_index), Value::String(text)) = (&mut self.text_index, value) {
            for word in text.split_whitespace() {
                let word_lower = word.to_lowercase();
                if let Some(doc_ids) = text_index.get_mut(&word_lower) {
                    doc_ids.retain(|id| *id != document_id);
                    if doc_ids.is_empty() {
                        text_index.remove(&word_lower);
                    }
                }
            }
        }
    }
    
    fn value_to_sort_key(&self, value: &Value) -> String {
        match value {
            Value::I64(n) => format!("{:020}", n + i64::MAX), // Offset for proper sorting
            Value::F64(f) => format!("{:020.10}", f.abs() * 1e10), // Simple float sorting
            Value::String(s) => s.clone(),
            Value::Timestamp(ts) => ts.timestamp().to_string(),
            _ => format!("{:?}", value),
        }
    }
    
    fn query(&self, operator: &QueryOperator) -> Vec<u64> {
        match operator {
            QueryOperator::Eq(value) => {
                self.value_index.get(value)
                    .map(|ids| ids.iter().copied().collect())
                    .unwrap_or_default()
            }
            QueryOperator::Ne(value) => {
                let mut all_ids = Vec::new();
                for ids in self.value_index.values() {
                    all_ids.extend(ids.iter());
                }
                let excluded_ids: std::collections::HashSet<_> = 
                    self.value_index.get(value)
                        .map(|ids| ids.iter().copied().collect())
                        .unwrap_or_default();
                all_ids.into_iter()
                    .filter(|id| !excluded_ids.contains(id))
                    .collect()
            }
            QueryOperator::In(values) => {
                let mut result_ids = std::collections::HashSet::new();
                for value in values {
                    if let Some(ids) = self.value_index.get(value) {
                        result_ids.extend(ids.iter());
                    }
                }
                result_ids.into_iter().copied().collect()
            }
            QueryOperator::NotIn(values) => {
                let mut all_ids = Vec::new();
                for ids in self.value_index.values() {
                    all_ids.extend(ids.iter());
                }
                let excluded_ids: std::collections::HashSet<_> = 
                    values.iter()
                        .flat_map(|v| self.value_index.get(v).into_iter().flatten())
                        .copied()
                        .collect();
                all_ids.into_iter()
                    .filter(|id| !excluded_ids.contains(id))
                    .collect()
            }
            QueryOperator::Contains(text) => {
                if let Some(text_index) = &self.text_index {
                    let text_lower = text.to_lowercase();
                    let mut result_ids = std::collections::HashSet::new();
                    
                    // Find all words that contain the search text
                    for (word, ids) in text_index {
                        if word.contains(&text_lower) {
                            result_ids.extend(ids.iter());
                        }
                    }
                    
                    result_ids.into_iter().copied().collect()
                } else {
                    Vec::new()
                }
            }
            QueryOperator::StartsWith(prefix) => {
                if let Some(text_index) = &self.text_index {
                    let prefix_lower = prefix.to_lowercase();
                    let mut result_ids = std::collections::HashSet::new();
                    
                    for (word, ids) in text_index {
                        if word.starts_with(&prefix_lower) {
                            result_ids.extend(ids.iter());
                        }
                    }
                    
                    result_ids.into_iter().copied().collect()
                } else {
                    Vec::new()
                }
            }
            QueryOperator::EndsWith(suffix) => {
                if let Some(text_index) = &self.text_index {
                    let suffix_lower = suffix.to_lowercase();
                    let mut result_ids = std::collections::HashSet::new();
                    
                    for (word, ids) in text_index {
                        if word.ends_with(&suffix_lower) {
                            result_ids.extend(ids.iter());
                        }
                    }
                    
                    result_ids.into_iter().copied().collect()
                } else {
                    Vec::new()
                }
            }
            QueryOperator::Range(min, max, inclusive_min, inclusive_max) => {
                self.range_query(min, max, *inclusive_min, *inclusive_max)
            }
            QueryOperator::Gt(value) => {
                self.range_query(value, &Value::Null, false, false) // Use Null as max (unbounded)
            }
            QueryOperator::Gte(value) => {
                self.range_query(value, &Value::Null, true, false)
            }
            QueryOperator::Lt(value) => {
                self.range_query(&Value::Null, value, false, false) // Use Null as min (unbounded)
            }
            QueryOperator::Lte(value) => {
                self.range_query(&Value::Null, value, false, true)
            }
            QueryOperator::Exists => {
                self.value_index.values()
                    .flat_map(|ids| ids.iter())
                    .copied()
                    .collect()
            }
            QueryOperator::NotExists => {
                // This would require knowing all document IDs in the collection
                // For now, return empty result
                Vec::new()
            }
            QueryOperator::Regex(_pattern) => {
                // TODO: Implement regex matching
                Vec::new()
            }
        }
    }
    
    fn range_query(&self, min: &Value, max: &Value, inclusive_min: bool, inclusive_max: bool) -> Vec<u64> {
        let min_key = if matches!(min, Value::Null) {
            None
        } else {
            Some(self.value_to_sort_key(min))
        };
        
        let max_key = if matches!(max, Value::Null) {
            None
        } else {
            Some(self.value_to_sort_key(max))
        };
        
        let mut result_ids = std::collections::HashSet::new();
        
        for (key, ids) in &self.sorted_values {
            let include = match (&min_key, &max_key) {
                (Some(min_k), Some(max_k)) => {
                    let min_cmp = key.cmp(min_k);
                    let max_cmp = key.cmp(max_k);
                    
                    let min_ok = if inclusive_min {
                        min_cmp >= std::cmp::Ordering::Equal
                    } else {
                        min_cmp == std::cmp::Ordering::Greater
                    };
                    
                    let max_ok = if inclusive_max {
                        max_cmp <= std::cmp::Ordering::Equal
                    } else {
                        max_cmp == std::cmp::Ordering::Less
                    };
                    
                    min_ok && max_ok
                }
                (Some(min_k), None) => {
                    let min_cmp = key.cmp(min_k);
                    if inclusive_min {
                        min_cmp >= std::cmp::Ordering::Equal
                    } else {
                        min_cmp == std::cmp::Ordering::Greater
                    }
                }
                (None, Some(max_k)) => {
                    let max_cmp = key.cmp(max_k);
                    if inclusive_max {
                        max_cmp <= std::cmp::Ordering::Equal
                    } else {
                        max_cmp == std::cmp::Ordering::Less
                    }
                }
                (None, None) => true, // No bounds
            };
            
            if include {
                result_ids.extend(ids.iter());
            }
        }
        
        result_ids.into_iter().copied().collect()
    }
}

/// Metadata indexing system
#[derive(Debug)]
pub struct MetadataIndex {
    // Field name -> field index
    field_indexes: RwLock<AHashMap<String, FieldIndex>>,
    // Document ID -> field values (for updates/deletes)
    document_fields: RwLock<AHashMap<u64, AHashMap<String, Value>>>,
}

impl MetadataIndex {
    pub fn new() -> Self {
        Self {
            field_indexes: RwLock::new(AHashMap::new()),
            document_fields: RwLock::new(AHashMap::new()),
        }
    }
    
    /// Index a document's metadata
    pub async fn index_document(
        &self,
        document_id: u64,
        metadata: &HashMap<String, Value>,
        field_schemas: &HashMap<String, crate::schema::FieldSchema>,
    ) -> Result<()> {
        let mut field_indexes = self.field_indexes.write().await;
        let mut document_fields = self.document_fields.write().await;
        
        // Remove old document if it exists
        if let Some(old_fields) = document_fields.remove(&document_id) {
            for (field, value) in old_fields {
                if let Some(field_index) = field_indexes.get_mut(&field) {
                    field_index.remove(document_id, &value);
                }
            }
        }
        
        // Index new metadata
        let mut new_fields = AHashMap::new();
        
        for (field, value) in metadata {
            if let Some(field_schema) = field_schemas.get(field) {
                // Get or create field index
                let field_index = field_indexes
                    .entry(field.clone())
                    .or_insert_with(|| FieldIndex::new(field_schema.field_type.clone()));
                
                field_index.insert(document_id, value);
                new_fields.insert(field.clone(), value.clone());
            }
        }
        
        document_fields.insert(document_id, new_fields);
        
        Ok(())
    }
    
    /// Remove a document from metadata indexes
    pub async fn remove_document(&self, document_id: u64) -> Result<()> {
        let mut field_indexes = self.field_indexes.write().await;
        let mut document_fields = self.document_fields.write().await;
        
        if let Some(fields) = document_fields.remove(&document_id) {
            for (field, value) in fields {
                if let Some(field_index) = field_indexes.get_mut(&field) {
                    field_index.remove(document_id, &value);
                }
            }
        }
        
        Ok(())
    }
    
    /// Query metadata index
    pub async fn query(&self, query: &MetadataQuery) -> Result<Vec<u64>> {
        let field_indexes = self.field_indexes.read().await;
        Ok(self.execute_query(query, &field_indexes))
    }
    
    fn execute_query(
        &self,
        query: &MetadataQuery,
        field_indexes: &AHashMap<String, FieldIndex>,
    ) -> Vec<u64> {
        match query {
            MetadataQuery::Condition(condition) => {
                if let Some(field_index) = field_indexes.get(&condition.field) {
                    field_index.query(&condition.operator)
                } else {
                    Vec::new()
                }
            }
            MetadataQuery::Logical { operator, queries } => {
                let results: Vec<Vec<u64>> = queries
                    .iter()
                    .map(|q| self.execute_query(q, field_indexes))
                    .collect();
                
                match operator {
                    LogicalOperator::And => {
                        if results.is_empty() {
                            return Vec::new();
                        }
                        
                        let mut intersection: std::collections::HashSet<u64> = 
                            results[0].iter().copied().collect();
                        
                        for result in results.iter().skip(1) {
                            let result_set: std::collections::HashSet<u64> = 
                                result.iter().copied().collect();
                            intersection = intersection
                                .intersection(&result_set)
                                .copied()
                                .collect();
                        }
                        
                        intersection.into_iter().collect()
                    }
                    LogicalOperator::Or => {
                        let mut union = std::collections::HashSet::new();
                        for result in results {
                            union.extend(result);
                        }
                        union.into_iter().collect()
                    }
                    LogicalOperator::Not => {
                        if let Some(first_result) = results.first() {
                            // This is a simplified NOT - would need all document IDs to be complete
                            Vec::new()
                        } else {
                            Vec::new()
                        }
                    }
                }
            }
        }
    }
    
    /// Get statistics about the metadata index
    pub async fn stats(&self) -> Result<MetadataIndexStats> {
        let field_indexes = self.field_indexes.read().await;
        let document_fields = self.document_fields.read().await;
        
        let mut field_stats = HashMap::new();
        let mut total_entries = 0;
        
        for (field, index) in field_indexes.iter() {
            let entries = index.value_index.len();
            field_stats.insert(field.clone(), entries);
            total_entries += entries;
        }
        
        Ok(MetadataIndexStats {
            indexed_documents: document_fields.len(),
            indexed_fields: field_indexes.len(),
            total_index_entries: total_entries,
            field_stats,
        })
    }
}

impl Default for MetadataIndex {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics about metadata indexing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataIndexStats {
    /// Number of documents with indexed metadata
    pub indexed_documents: usize,
    /// Number of indexed fields
    pub indexed_fields: usize,
    /// Total number of index entries across all fields
    pub total_index_entries: usize,
    /// Per-field statistics
    pub field_stats: HashMap<String, usize>,
}

/// Metadata search result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataSearchResult {
    /// Document ID
    pub document_id: u64,
    /// Matched metadata fields
    pub matched_fields: Vec<String>,
}
