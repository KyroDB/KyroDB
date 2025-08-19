use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TableKind {
    Kv,
    Vectors { dim: usize },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String,
    pub kind: TableKind,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct SchemaRegistry {
    pub tables: HashMap<String, TableSchema>,
}

impl SchemaRegistry {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
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

    pub fn upsert_table(&mut self, schema: TableSchema) {
        self.tables.insert(schema.name.clone(), schema);
    }

    pub fn get_vectors_dim(&self, name: &str) -> Option<usize> {
        self.tables.get(name).and_then(|t| match &t.kind {
            TableKind::Vectors { dim } => Some(*dim),
            _ => None,
        })
    }

    pub fn get_default_vectors_dim(&self) -> Option<usize> {
        self.tables.values().find_map(|t| match &t.kind {
            TableKind::Vectors { dim } => Some(*dim),
            _ => None,
        })
    }
}
