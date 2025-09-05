use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::pin::Pin;
use std::future::Future;
use tokio::sync::RwLock;

#[cfg(feature = "fulltext-search")]
use tantivy::{
    collector::TopDocs,
    doc,
    query::{BooleanQuery, FuzzyTermQuery, Occur, Query, QueryParser, TermQuery},
    schema::{Field, Schema, Value as TantivyValue, STORED, TEXT},
    Index, IndexReader, IndexWriter, TantivyDocument,
};

/// Text search configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TextSearchConfig {
    /// Maximum number of documents to index in memory before flushing
    pub memory_budget_mb: usize,
    /// Enable fuzzy search
    pub enable_fuzzy: bool,
    /// Fuzzy search distance
    pub fuzzy_distance: u8,
    /// Index update frequency in seconds
    pub index_refresh_interval: u64,
}

impl Default for TextSearchConfig {
    fn default() -> Self {
        Self {
            memory_budget_mb: 64,
            enable_fuzzy: true,
            fuzzy_distance: 2,
            index_refresh_interval: 5,
        }
    }
}

/// Text search query
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TextQuery {
    /// Text to search for
    pub text: String,
    /// Fields to search in (if empty, searches all text fields)
    pub fields: Vec<String>,
    /// Maximum number of results
    pub limit: usize,
    /// Enable fuzzy search
    pub fuzzy: bool,
    /// Minimum score threshold
    pub min_score: Option<f32>,
}

/// Text search result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TextSearchResult {
    /// Document ID
    pub document_id: u64,
    /// Search score
    pub score: f32,
    /// Matched fields and their snippets
    pub snippets: HashMap<String, String>,
}

/// Text search statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TextSearchStats {
    /// Number of indexed documents
    pub document_count: usize,
    /// Index size on disk (bytes)
    pub index_size_bytes: u64,
    /// Number of indexed terms
    pub term_count: usize,
    /// Last index update timestamp
    pub last_updated: chrono::DateTime<chrono::Utc>,
}

#[cfg(feature = "fulltext-search")]
/// Tantivy-based text search index
pub struct TantivyTextIndex {
    index: Index,
    reader: IndexReader,
    writer: Arc<RwLock<IndexWriter>>,
    schema: Schema,
    fields: TextIndexFields,
    config: TextSearchConfig,
}

#[cfg(feature = "fulltext-search")]
struct TextIndexFields {
    document_id: Field,
    collection: Field,
    text_content: Field,
    title: Field,
    metadata: Field,
}

#[cfg(feature = "fulltext-search")]
impl TantivyTextIndex {
    pub fn new<P: AsRef<Path>>(index_path: P, config: TextSearchConfig) -> Result<Self> {
        use tantivy::schema::*;
        
        let mut schema_builder = Schema::builder();
        
        // Document ID field
        let document_id = schema_builder.add_u64_field("document_id", STORED);
        
        // Collection name field
        let collection = schema_builder.add_text_field("collection", TEXT | STORED);
        
        // Main text content field
        let text_content = schema_builder.add_text_field("text_content", TEXT | STORED);
        
        // Title field for documents
        let title = schema_builder.add_text_field("title", TEXT | STORED);
        
        // Metadata field (JSON string)
        let metadata = schema_builder.add_text_field("metadata", TEXT | STORED);
        
        let schema = schema_builder.build();
        
        let fields = TextIndexFields {
            document_id,
            collection,
            text_content,
            title,
            metadata,
        };
        
        // Create or open index
        let index = if index_path.as_ref().exists() {
            Index::open_in_dir(index_path)?
        } else {
            std::fs::create_dir_all(&index_path)?;
            Index::create_in_dir(index_path, schema.clone())?
        };
        
        let reader = index.reader_builder()
            .reload_policy(tantivy::ReloadPolicy::Manual)
            .try_into()?;
        
        let writer = index.writer(config.memory_budget_mb * 1024 * 1024)?;
        
        Ok(Self {
            index,
            reader,
            writer: Arc::new(RwLock::new(writer)),
            schema,
            fields,
            config,
        })
    }
    
    /// Index a document's text content
    pub async fn index_document(
        &self,
        document_id: u64,
        collection: &str,
        text: &str,
        title: Option<&str>,
        metadata: Option<&serde_json::Value>,
    ) -> Result<()> {
        let mut doc = TantivyDocument::new();
        
        doc.add_u64(self.fields.document_id, document_id);
        doc.add_text(self.fields.collection, collection);
        doc.add_text(self.fields.text_content, text);
        
        if let Some(title) = title {
            doc.add_text(self.fields.title, title);
        }
        
        if let Some(metadata) = metadata {
            doc.add_text(self.fields.metadata, &serde_json::to_string(metadata)?);
        }
        
        let mut writer = self.writer.write().await;
        writer.add_document(doc)?;
        writer.commit()?;
        
        Ok(())
    }
    
    /// Update a document in the text index
    pub async fn update_document(
        &self,
        document_id: u64,
        collection: &str,
        text: &str,
        title: Option<&str>,
        metadata: Option<&serde_json::Value>,
    ) -> Result<()> {
        // Delete existing document
        self.delete_document(document_id).await?;
        
        // Add updated document
        self.index_document(document_id, collection, text, title, metadata).await
    }
    
    /// Delete a document from the text index
    pub async fn delete_document(&self, document_id: u64) -> Result<()> {
        let term = tantivy::Term::from_field_u64(self.fields.document_id, document_id);
        let mut writer = self.writer.write().await;
        writer.delete_term(term);
        writer.commit()?;
        
        Ok(())
    }
    
    /// Search for documents using text query
    pub async fn search(&self, query: &TextQuery) -> Result<Vec<TextSearchResult>> {
        let searcher = self.reader.searcher();
        
        // Build query
        let parsed_query = self.build_query(query)?;
        
        // Execute search
        let top_docs = searcher.search(
            &*parsed_query,
            &TopDocs::with_limit(query.limit),
        )?;
        
        let mut results = Vec::new();
        
        for (score, doc_address) in top_docs {
            // Apply score threshold
            if let Some(min_score) = query.min_score {
                if score < min_score {
                    continue;
                }
            }
            
            let doc: tantivy::TantivyDocument = searcher.doc(doc_address)?;
            
            if let Some(document_id_value) = doc.get_first(self.fields.document_id) {
                if let tantivy::schema::OwnedValue::U64(document_id) = document_id_value {
                    // Extract snippets
                    let mut snippets = HashMap::new();
                    
                    if let Some(text_value) = doc.get_first(self.fields.text_content) {
                        if let tantivy::schema::OwnedValue::Str(text) = text_value {
                            snippets.insert("text".to_string(), self.extract_snippet(text, &query.text));
                        }
                    }
                    
                    if let Some(title_value) = doc.get_first(self.fields.title) {
                        if let tantivy::schema::OwnedValue::Str(title) = title_value {
                            snippets.insert("title".to_string(), self.extract_snippet(title, &query.text));
                        }
                    }
                    
                    results.push(TextSearchResult {
                        document_id: *document_id,
                        score,
                        snippets,
                    });
                }
            }
        }
        
        Ok(results)
    }
    
    /// Build tantivy query from text query
    fn build_query(&self, query: &TextQuery) -> Result<Box<dyn Query>> {
        let query_parser = QueryParser::for_index(
            &self.index,
            vec![self.fields.text_content, self.fields.title, self.fields.metadata],
        );
        
        if query.fuzzy && self.config.enable_fuzzy {
            // Build fuzzy query
            let mut clauses = vec![];
            
            for term in query.text.split_whitespace() {
                let fuzzy_query = FuzzyTermQuery::new(
                    tantivy::Term::from_field_text(self.fields.text_content, term),
                    self.config.fuzzy_distance,
                    true,
                );
                clauses.push((Occur::Should, Box::new(fuzzy_query) as Box<dyn Query>));
            }
            
            Ok(Box::new(BooleanQuery::new(clauses)))
        } else {
            // Parse regular query
            let parsed = query_parser.parse_query(&query.text)?;
            Ok(parsed)
        }
    }
    
    /// Extract snippet around matching text
    fn extract_snippet(&self, text: &str, query_text: &str) -> String {
        const SNIPPET_LENGTH: usize = 150;
        
        // Find first occurrence of query terms
        let query_lower = query_text.to_lowercase();
        let text_lower = text.to_lowercase();
        
        if let Some(pos) = text_lower.find(&query_lower) {
            let start = pos.saturating_sub(SNIPPET_LENGTH / 2);
            let end = (start + SNIPPET_LENGTH).min(text.len());
            
            let mut snippet = text[start..end].to_string();
            
            if start > 0 {
                snippet = format!("...{}", snippet);
            }
            if end < text.len() {
                snippet = format!("{}...", snippet);
            }
            
            snippet
        } else {
            // Return beginning of text if no match found
            let end = SNIPPET_LENGTH.min(text.len());
            let mut snippet = text[..end].to_string();
            if end < text.len() {
                snippet = format!("{}...", snippet);
            }
            snippet
        }
    }
    
    /// Get text search statistics
    pub async fn stats(&self) -> Result<TextSearchStats> {
        let searcher = self.reader.searcher();
        let index_meta = searcher.index().load_metas()?;
        
        let mut total_docs = 0;
        let mut total_size = 0;
        
        for segment_meta in &index_meta.segments {
            total_docs += segment_meta.max_doc();
            // Approximate size calculation
            total_size += segment_meta.max_doc() as u64 * 1024; // Rough estimate
        }
        
        Ok(TextSearchStats {
            document_count: total_docs as usize,
            index_size_bytes: total_size,
            term_count: 0, // Tantivy doesn't expose this easily
            last_updated: chrono::Utc::now(),
        })
    }
}

#[cfg(not(feature = "fulltext-search"))]
/// No-op text index when text search is disabled
pub struct NoOpTextIndex;

#[cfg(not(feature = "fulltext-search"))]
impl NoOpTextIndex {
    pub fn new<P: AsRef<Path>>(_index_path: P, _config: TextSearchConfig) -> Result<Self> {
        Ok(Self)
    }
    
    pub async fn index_document(
        &self,
        _document_id: u64,
        _collection: &str,
        _text: &str,
        _title: Option<&str>,
        _metadata: Option<&serde_json::Value>,
    ) -> Result<()> {
        Ok(())
    }
    
    pub async fn update_document(
        &self,
        _document_id: u64,
        _collection: &str,
        _text: &str,
        _title: Option<&str>,
        _metadata: Option<&serde_json::Value>,
    ) -> Result<()> {
        Ok(())
    }
    
    pub async fn delete_document(&self, _document_id: u64) -> Result<()> {
        Ok(())
    }
    
    pub async fn search(&self, _query: &TextQuery) -> Result<Vec<TextSearchResult>> {
        Ok(vec![])
    }
    
    pub async fn stats(&self) -> Result<TextSearchStats> {
        Ok(TextSearchStats {
            document_count: 0,
            index_size_bytes: 0,
            term_count: 0,
            last_updated: chrono::Utc::now(),
        })
    }
}


/// Text index wrapper enum to avoid dyn trait issues
#[derive(Clone)]
pub enum TextIndexWrapper {
    #[cfg(feature = "fulltext-search")]
    Tantivy(Arc<TantivyTextIndex>),
    #[cfg(not(feature = "fulltext-search"))]
    NoOp(Arc<NoOpTextIndex>),
}

impl TextIndexWrapper {
    pub async fn index_document(
        &self,
        document_id: u64,
        collection: &str,
        text: &str,
        title: Option<&str>,
        metadata: Option<&serde_json::Value>,
    ) -> Result<()> {
        match self {
            #[cfg(feature = "fulltext-search")]
            TextIndexWrapper::Tantivy(index) => {
                index.index_document(document_id, collection, text, title, metadata).await
            }
            #[cfg(not(feature = "fulltext-search"))]
            TextIndexWrapper::NoOp(index) => {
                index.index_document(document_id, collection, text, title, metadata).await
            }
        }
    }
    
    pub async fn search(&self, query: &TextQuery) -> Result<Vec<TextSearchResult>> {
        match self {
            #[cfg(feature = "fulltext-search")]
            TextIndexWrapper::Tantivy(index) => index.search(query).await,
            #[cfg(not(feature = "fulltext-search"))]
            TextIndexWrapper::NoOp(index) => index.search(query).await,
        }
    }
    
    pub async fn stats(&self) -> Result<TextSearchStats> {
        match self {
            #[cfg(feature = "fulltext-search")]
            TextIndexWrapper::Tantivy(index) => index.stats().await,
            #[cfg(not(feature = "fulltext-search"))]
            TextIndexWrapper::NoOp(index) => index.stats().await,
        }
    }
}

/// Create the appropriate text index based on feature flags
pub fn create_text_index<P: AsRef<Path>>(
    index_path: P,
    config: TextSearchConfig,
) -> Result<TextIndexWrapper> {
    #[cfg(feature = "fulltext-search")]
    {
        let index = TantivyTextIndex::new(index_path, config)?;
        Ok(TextIndexWrapper::Tantivy(Arc::new(index)))
    }
    
    #[cfg(not(feature = "fulltext-search"))]
    {
        let index = NoOpTextIndex::new(index_path, config)?;
        Ok(TextIndexWrapper::NoOp(Arc::new(index)))
    }
}
