use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use chrono::{DateTime, Utc};

// Phase B.1: Enhanced Data Models for Vector Storage

/// Distance metrics supported for vector similarity computation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DistanceMetric {
    /// Euclidean (L2) distance
    Euclidean,
    /// Cosine similarity (1 - cosine_distance)
    Cosine,
    /// Dot product similarity
    DotProduct,
    /// Manhattan (L1) distance
    Manhattan,
}

impl Default for DistanceMetric {
    fn default() -> Self {
        DistanceMetric::Euclidean
    }
}

/// Multi-modal value types supported in documents
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Bool(bool),
    I64(i64),
    F64(f64),
    String(String),
    Bytes(Vec<u8>),
    Array(Vec<Value>),
    Object(HashMap<String, Value>),
    Vector(Vec<f32>),
    Timestamp(DateTime<Utc>),
}

impl Value {
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Null => "null",
            Value::Bool(_) => "bool",
            Value::I64(_) => "i64",
            Value::F64(_) => "f64",
            Value::String(_) => "string",
            Value::Bytes(_) => "bytes",
            Value::Array(_) => "array",
            Value::Object(_) => "object",
            Value::Vector(_) => "vector",
            Value::Timestamp(_) => "timestamp",
        }
    }
}

/// Configuration for HNSW index
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HnswConfig {
    /// Maximum number of connections per node
    pub max_connections: usize,
    /// Size multiplier for higher levels
    pub level_multiplier: f32,
    /// Distance metric for similarity computation
    pub distance_metric: DistanceMetric,
    /// Search parameter for construction
    pub ef_construction: usize,
    /// Search parameter for queries
    pub ef_search: usize,
    /// Enable SIMD optimization
    pub simd_enabled: bool,
}

impl Default for HnswConfig {
    fn default() -> Self {
        Self {
            max_connections: 16,
            level_multiplier: 1.0 / 2.0_f32.ln(),
            distance_metric: DistanceMetric::Euclidean,
            ef_construction: 200,
            ef_search: 50,
            simd_enabled: true,
        }
    }
}

/// Enhanced collection schema for Phase B.1
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionSchema {
    /// Collection name (unique identifier)
    pub name: String,
    /// Human-readable description
    pub description: Option<String>,
    /// Vector dimension (if this collection stores vectors)
    pub vector_dimension: Option<usize>,
    /// HNSW configuration for vector search
    pub hnsw_config: Option<HnswConfig>,
    /// Whether text search is enabled
    pub text_search_enabled: bool,
    /// Metadata field schemas
    pub metadata_schema: HashMap<String, FieldSchema>,
    /// Whether this collection supports multi-modal queries
    pub multi_modal: bool,
    /// Maximum document size in bytes
    pub max_document_size: usize,
    /// Creation timestamp
    pub created_at: DateTime<Utc>,
    /// Last modification timestamp
    pub updated_at: DateTime<Utc>,
}

/// Schema for individual metadata fields
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldSchema {
    /// Field data type
    pub field_type: FieldType,
    /// Whether this field is indexed for fast lookups
    pub indexed: bool,
    /// Whether this field is required
    pub required: bool,
    /// Default value if not provided
    pub default: Option<Value>,
}

/// Supported field types in metadata
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FieldType {
    String,
    Integer,
    Float,
    Boolean,
    Timestamp,
    Vector,
    Bytes,
    Array(Box<FieldType>),
    Object,
}

/// Multi-modal document structure for Phase B.1
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Document {
    /// Unique document identifier
    pub id: u64,
    /// Collection this document belongs to
    pub collection: String,
    /// Optional text content
    pub text: Option<String>,
    /// Optional vector embedding
    pub embedding: Option<Vec<f32>>,
    /// Structured metadata
    pub metadata: HashMap<String, Value>,
    /// Document creation timestamp
    pub created_at: DateTime<Utc>,
    /// Last update timestamp
    pub updated_at: DateTime<Utc>,
    /// Document version (for optimistic concurrency)
    pub version: u64,
}

impl Document {
    pub fn new(id: u64, collection: String) -> Self {
        let now = Utc::now();
        Self {
            id,
            collection,
            text: None,
            embedding: None,
            metadata: HashMap::new(),
            created_at: now,
            updated_at: now,
            version: 1,
        }
    }

    pub fn with_text(mut self, text: String) -> Self {
        self.text = Some(text);
        self
    }

    pub fn with_embedding(mut self, embedding: Vec<f32>) -> Self {
        self.embedding = Some(embedding);
        self
    }

    pub fn with_metadata(mut self, key: String, value: Value) -> Self {
        self.metadata.insert(key, value);
        self
    }

    pub fn update_timestamp(&mut self) {
        self.updated_at = Utc::now();
        self.version += 1;
    }
}

/// Backward compatibility types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TableKind {
    Kv,
    Vectors { dim: usize },
    Collection { schema: CollectionSchema },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String,
    pub kind: TableKind,
}

/// Enhanced schema registry for Phase B.1
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct SchemaRegistry {
    /// Backward compatibility: legacy table schemas
    pub tables: HashMap<String, TableSchema>,
    /// Phase B.1: Modern collection schemas
    pub collections: HashMap<String, CollectionSchema>,
    /// Registry metadata
    pub version: u32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl SchemaRegistry {
    pub fn new() -> Self {
        let now = Utc::now();
        Self {
            tables: HashMap::new(),
            collections: HashMap::new(),
            version: 1,
            created_at: now,
            updated_at: now,
        }
    }

    pub fn load(path: &std::path::Path) -> Self {
        if path.exists() {
            if let Ok(bytes) = fs::read(path) {
                if let Ok(reg) = serde_json::from_slice::<SchemaRegistry>(&bytes) {
                    return reg;
                }
            }
        }
        Self::new()
    }

    pub fn save(&self, path: &std::path::Path) -> std::io::Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let bytes = serde_json::to_vec_pretty(self).unwrap_or_default();
        fs::write(path, bytes)
    }

    // Legacy table support (backward compatibility)
    pub fn upsert_table(&mut self, schema: TableSchema) {
        self.tables.insert(schema.name.clone(), schema);
        self.update_timestamp();
    }

    pub fn get_vectors_dim(&self, name: &str) -> Option<usize> {
        // Check collections first (Phase B.1)
        if let Some(collection) = self.collections.get(name) {
            return collection.vector_dimension;
        }
        
        // Fallback to legacy tables
        self.tables.get(name).and_then(|t| match &t.kind {
            TableKind::Vectors { dim } => Some(*dim),
            TableKind::Collection { schema } => schema.vector_dimension,
            _ => None,
        })
    }

    pub fn get_default_vectors_dim(&self) -> Option<usize> {
        // Check collections first
        if let Some(dim) = self.collections.values()
            .find_map(|c| c.vector_dimension) {
            return Some(dim);
        }
        
        // Fallback to legacy tables
        self.tables.values().find_map(|t| match &t.kind {
            TableKind::Vectors { dim } => Some(*dim),
            TableKind::Collection { schema } => schema.vector_dimension,
            _ => None,
        })
    }

    // Phase B.1: Collection management
    pub fn create_collection(&mut self, mut schema: CollectionSchema) -> Result<(), String> {
        if self.collections.contains_key(&schema.name) {
            return Err(format!("Collection '{}' already exists", schema.name));
        }

        // Validate schema
        if let Some(dim) = schema.vector_dimension {
            if dim == 0 || dim > 4096 {
                return Err("Vector dimension must be between 1 and 4096".to_string());
            }
        }

        schema.created_at = Utc::now();
        schema.updated_at = schema.created_at;
        
        self.collections.insert(schema.name.clone(), schema);
        self.update_timestamp();
        Ok(())
    }

    pub fn get_collection(&self, name: &str) -> Option<&CollectionSchema> {
        self.collections.get(name)
    }

    pub fn update_collection(&mut self, name: &str, mut schema: CollectionSchema) -> Result<(), String> {
        if !self.collections.contains_key(name) {
            return Err(format!("Collection '{}' does not exist", name));
        }

        schema.updated_at = Utc::now();
        self.collections.insert(name.to_string(), schema);
        self.update_timestamp();
        Ok(())
    }

    pub fn delete_collection(&mut self, name: &str) -> Result<(), String> {
        if self.collections.remove(name).is_none() {
            return Err(format!("Collection '{}' does not exist", name));
        }
        self.update_timestamp();
        Ok(())
    }

    pub fn list_collections(&self) -> Vec<&CollectionSchema> {
        self.collections.values().collect()
    }

    pub fn validate_document(&self, doc: &Document) -> Result<(), String> {
        let collection = self.get_collection(&doc.collection)
            .ok_or_else(|| format!("Collection '{}' does not exist", doc.collection))?;

        // Validate vector dimension
        if let (Some(embedding), Some(expected_dim)) = (&doc.embedding, collection.vector_dimension) {
            if embedding.len() != expected_dim {
                return Err(format!(
                    "Vector dimension mismatch: expected {}, got {}",
                    expected_dim,
                    embedding.len()
                ));
            }
        }

        // Validate required metadata fields
        for (field_name, field_schema) in &collection.metadata_schema {
            if field_schema.required && !doc.metadata.contains_key(field_name) {
                return Err(format!("Required field '{}' is missing", field_name));
            }
        }

        Ok(())
    }

    fn update_timestamp(&mut self) {
        self.updated_at = Utc::now();
        self.version += 1;
    }
}
