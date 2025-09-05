//! Vector storage implementation for Phase B.1
//!
//! This module provides efficient storage for vectors with associated metadata
//! and integration with the existing WAL system.

use crate::schema::{Document, Value};
use ahash::AHashMap;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::sync::Arc;

/// Compact vector record for efficient storage and retrieval
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorRecord {
    /// Document/vector ID
    pub id: u64,
    /// Vector data
    pub vector: Vec<f32>,
    /// Collection name
    pub collection: String,
    /// Compact metadata (only essential fields for fast access)
    pub metadata: CompactMetadata,
}

/// Compact metadata representation for hot path performance
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactMetadata {
    /// Small vector for frequently accessed metadata
    pub quick_access: SmallVec<[(String, CompactValue); 8]>,
    /// Full metadata blob (lazily deserialized)
    pub full_metadata: Option<Vec<u8>>,
}

/// Space-efficient value representation for hot metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CompactValue {
    Bool(bool),
    I32(i32),
    F32(f32),
    ShortString([u8; 16], u8), // 16-byte inline string + length
    LongString(String),
    Timestamp(u64), // Unix nanoseconds
}

impl CompactValue {
    pub fn from_value(value: &Value) -> Self {
        match value {
            Value::Bool(b) => CompactValue::Bool(*b),
            Value::I64(i) => CompactValue::I32(*i as i32), // Truncate for space
            Value::F64(f) => CompactValue::F32(*f as f32), // Truncate for space
            Value::String(s) => {
                if s.len() <= 16 {
                    let mut buf = [0u8; 16];
                    let bytes = s.as_bytes();
                    buf[..bytes.len()].copy_from_slice(bytes);
                    CompactValue::ShortString(buf, bytes.len() as u8)
                } else {
                    CompactValue::LongString(s.clone())
                }
            }
            Value::Timestamp(dt) => CompactValue::Timestamp(dt.timestamp_nanos_opt().unwrap_or(0) as u64),
            _ => CompactValue::LongString(format!("{:?}", value)), // Fallback
        }
    }

    pub fn to_value(&self) -> Value {
        match self {
            CompactValue::Bool(b) => Value::Bool(*b),
            CompactValue::I32(i) => Value::I64(*i as i64),
            CompactValue::F32(f) => Value::F64(*f as f64),
            CompactValue::ShortString(buf, len) => {
                let bytes = &buf[..*len as usize];
                Value::String(String::from_utf8_lossy(bytes).into_owned())
            }
            CompactValue::LongString(s) => Value::String(s.clone()),
            CompactValue::Timestamp(ts) => {
                use chrono::{DateTime, Utc, TimeZone};
                let dt = Utc.timestamp_nanos(*ts as i64);
                Value::Timestamp(dt)
            }
        }
    }
}

impl CompactMetadata {
    pub fn new() -> Self {
        Self {
            quick_access: SmallVec::new(),
            full_metadata: None,
        }
    }

    pub fn from_document_metadata(metadata: &std::collections::HashMap<String, Value>) -> Self {
        let mut quick_access = SmallVec::new();
        
        // Extract frequently accessed fields to quick_access
        let priority_fields = ["user_id", "category", "status", "priority", "timestamp"];
        
        for field in &priority_fields {
            if let Some(value) = metadata.get(*field) {
                quick_access.push((field.to_string(), CompactValue::from_value(value)));
            }
        }
        
        // Serialize full metadata for complete access
        let full_metadata = if metadata.len() > quick_access.len() {
            bincode::serialize(metadata).ok()
        } else {
            None
        };

        Self {
            quick_access,
            full_metadata,
        }
    }

    pub fn get_quick(&self, key: &str) -> Option<&CompactValue> {
        self.quick_access.iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v)
    }
}

/// High-performance vector storage with memory optimization
pub struct VectorStorage {
    /// Dense vector storage (all vectors concatenated)
    vectors: Vec<f32>,
    /// Vector metadata
    metadata: Vec<VectorRecord>,
    /// ID to index mapping
    id_to_index: AHashMap<u64, usize>,
    /// Vector dimension
    dimension: usize,
    /// Memory usage tracking
    memory_usage: usize,
}

impl VectorStorage {
    pub fn new(dimension: usize) -> Self {
        Self {
            vectors: Vec::new(),
            metadata: Vec::new(),
            id_to_index: AHashMap::new(),
            dimension,
            memory_usage: 0,
        }
    }

    pub fn with_capacity(dimension: usize, capacity: usize) -> Self {
        Self {
            vectors: Vec::with_capacity(capacity * dimension),
            metadata: Vec::with_capacity(capacity),
            id_to_index: AHashMap::with_capacity(capacity),
            dimension,
            memory_usage: 0,
        }
    }

    /// Insert or update a vector
    pub fn insert(&mut self, record: VectorRecord) -> Result<()> {
        if record.vector.len() != self.dimension {
            anyhow::bail!(
                "Vector dimension mismatch: expected {}, got {}",
                self.dimension,
                record.vector.len()
            );
        }

        let id = record.id;
        
        if let Some(&existing_index) = self.id_to_index.get(&id) {
            // Update existing vector
            let vector_start = existing_index * self.dimension;
            let vector_end = vector_start + self.dimension;
            self.vectors[vector_start..vector_end].copy_from_slice(&record.vector);
            self.metadata[existing_index] = record;
        } else {
            // Insert new vector
            let new_index = self.metadata.len();
            self.vectors.extend_from_slice(&record.vector);
            self.metadata.push(record);
            self.id_to_index.insert(id, new_index);
            self.update_memory_usage();
        }

        Ok(())
    }

    /// Remove a vector by ID
    pub fn remove(&mut self, id: u64) -> Result<bool> {
        if let Some(&index) = self.id_to_index.get(&id) {
            // Remove from dense storage (expensive operation)
            let vector_start = index * self.dimension;
            let vector_end = vector_start + self.dimension;
            self.vectors.drain(vector_start..vector_end);
            
            // Remove metadata
            self.metadata.remove(index);
            self.id_to_index.remove(&id);
            
            // Update all indices after the removed one
            for (_, idx) in self.id_to_index.iter_mut() {
                if *idx > index {
                    *idx -= 1;
                }
            }
            
            self.update_memory_usage();
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Get vector by ID
    pub fn get_vector(&self, id: u64) -> Option<&[f32]> {
        self.id_to_index.get(&id).map(|&index| {
            let start = index * self.dimension;
            let end = start + self.dimension;
            &self.vectors[start..end]
        })
    }

    /// Get vector and metadata by ID
    pub fn get_record(&self, id: u64) -> Option<(&[f32], &VectorRecord)> {
        self.id_to_index.get(&id).map(|&index| {
            let start = index * self.dimension;
            let end = start + self.dimension;
            (&self.vectors[start..end], &self.metadata[index])
        })
    }

    /// Get vector by index (for HNSW traversal)
    pub fn get_vector_by_index(&self, index: usize) -> Option<&[f32]> {
        if index < self.metadata.len() {
            let start = index * self.dimension;
            let end = start + self.dimension;
            Some(&self.vectors[start..end])
        } else {
            None
        }
    }

    /// Get record by index
    pub fn get_record_by_index(&self, index: usize) -> Option<(&[f32], &VectorRecord)> {
        if index < self.metadata.len() {
            let start = index * self.dimension;
            let end = start + self.dimension;
            Some((&self.vectors[start..end], &self.metadata[index]))
        } else {
            None
        }
    }

    /// Get ID by index
    pub fn get_id_by_index(&self, index: usize) -> Option<u64> {
        self.metadata.get(index).map(|record| record.id)
    }

    /// Get index by ID
    pub fn get_index_by_id(&self, id: u64) -> Option<usize> {
        self.id_to_index.get(&id).copied()
    }

    /// Number of vectors stored
    pub fn len(&self) -> usize {
        self.metadata.len()
    }

    /// Check if storage is empty
    pub fn is_empty(&self) -> bool {
        self.metadata.is_empty()
    }

    /// Vector dimension
    pub fn dimension(&self) -> usize {
        self.dimension
    }

    /// Memory usage in bytes
    pub fn memory_usage(&self) -> usize {
        self.memory_usage
    }

    /// Iterator over all vectors with their IDs
    pub fn iter(&self) -> impl Iterator<Item = (u64, &[f32])> {
        self.metadata.iter().enumerate().map(move |(index, record)| {
            let start = index * self.dimension;
            let end = start + self.dimension;
            (record.id, &self.vectors[start..end])
        })
    }

    /// Iterator over all records
    pub fn iter_records(&self) -> impl Iterator<Item = (u64, &[f32], &VectorRecord)> {
        self.metadata.iter().enumerate().map(move |(index, record)| {
            let start = index * self.dimension;
            let end = start + self.dimension;
            (record.id, &self.vectors[start..end], record)
        })
    }

    /// Compact storage to remove fragmentation
    pub fn compact(&mut self) {
        // Already dense, but we can optimize memory layout
        self.vectors.shrink_to_fit();
        self.metadata.shrink_to_fit();
        self.id_to_index.shrink_to_fit();
        self.update_memory_usage();
    }

    /// Clear all vectors
    pub fn clear(&mut self) {
        self.vectors.clear();
        self.metadata.clear();
        self.id_to_index.clear();
        self.memory_usage = 0;
    }

    fn update_memory_usage(&mut self) {
        self.memory_usage = 
            self.vectors.len() * std::mem::size_of::<f32>() +
            self.metadata.len() * std::mem::size_of::<VectorRecord>() +
            self.id_to_index.len() * (std::mem::size_of::<u64>() + std::mem::size_of::<usize>());
    }
}

impl Default for VectorStorage {
    fn default() -> Self {
        Self::new(128) // Default to 128-dimensional vectors
    }
}

/// Convert Document to VectorRecord for storage
impl VectorRecord {
    pub fn from_document(doc: &Document) -> Result<Self> {
        let embedding = doc.embedding.as_ref()
            .ok_or_else(|| anyhow::anyhow!("Document has no embedding"))?;

        let metadata = CompactMetadata::from_document_metadata(&doc.metadata);

        Ok(Self {
            id: doc.id,
            vector: embedding.clone(),
            collection: doc.collection.clone(),
            metadata,
        })
    }

    pub fn to_document(&self) -> Result<Document> {
        use chrono::Utc;
        use std::collections::HashMap;

        let mut metadata = HashMap::new();
        
        // Convert quick access metadata
        for (key, value) in &self.metadata.quick_access {
            metadata.insert(key.clone(), value.to_value());
        }

        // Deserialize full metadata if available
        if let Some(ref full_meta_bytes) = self.metadata.full_metadata {
            if let Ok(full_meta) = bincode::deserialize::<HashMap<String, Value>>(full_meta_bytes) {
                metadata.extend(full_meta);
            }
        }

        Ok(Document {
            id: self.id,
            collection: self.collection.clone(),
            text: None, // Text content not stored in VectorRecord
            embedding: Some(self.vector.clone()),
            metadata,
            created_at: Utc::now(), // Will be overridden by actual timestamps
            updated_at: Utc::now(),
            version: 1,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::Value;
    use std::collections::HashMap;

    #[test]
    fn test_vector_storage_basic_operations() {
        let mut storage = VectorStorage::new(3);
        
        let record = VectorRecord {
            id: 1,
            vector: vec![1.0, 2.0, 3.0],
            collection: "test".to_string(),
            metadata: CompactMetadata::new(),
        };

        // Insert
        storage.insert(record.clone()).unwrap();
        assert_eq!(storage.len(), 1);

        // Get
        let retrieved = storage.get_vector(1).unwrap();
        assert_eq!(retrieved, &[1.0, 2.0, 3.0]);

        // Remove
        assert!(storage.remove(1).unwrap());
        assert_eq!(storage.len(), 0);
        assert!(storage.get_vector(1).is_none());
    }

    #[test]
    fn test_compact_metadata() {
        let mut metadata_map = HashMap::new();
        metadata_map.insert("user_id".to_string(), Value::I64(123));
        metadata_map.insert("category".to_string(), Value::String("test".to_string()));
        
        let compact = CompactMetadata::from_document_metadata(&metadata_map);
        
        assert!(compact.get_quick("user_id").is_some());
        assert!(compact.get_quick("category").is_some());
        assert!(compact.get_quick("nonexistent").is_none());
    }

    #[test]
    fn test_dimension_validation() {
        let mut storage = VectorStorage::new(3);
        
        let record = VectorRecord {
            id: 1,
            vector: vec![1.0, 2.0], // Wrong dimension
            collection: "test".to_string(),
            metadata: CompactMetadata::new(),
        };

        assert!(storage.insert(record).is_err());
    }
}
