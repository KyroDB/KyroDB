use anyhow::Result;
use async_trait::async_trait;

pub mod http;
pub mod ultra_fast;

pub use http::HttpClient;
pub use ultra_fast::UltraFastClient;

/// Client configuration
#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub base_url: String,
    pub timeout_secs: u64,
    pub pool_size: usize,
    pub auth_token: Option<String>,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            base_url: "http://127.0.0.1:3030".to_string(),
            timeout_secs: 30,
            pool_size: 1000,
            auth_token: None,
        }
    }
}

/// Unified benchmark client trait
#[async_trait]
pub trait BenchClient: Send + Sync {
    /// PUT operation
    async fn put(&self, key: u64, value: Vec<u8>) -> Result<()>;
    
    /// GET operation
    async fn get(&self, key: u64) -> Result<Option<Vec<u8>>>;
    
    /// Batch lookup (returns offsets)
    async fn lookup_batch(&self, keys: Vec<u64>) -> Result<Vec<(u64, Option<u64>)>>;
    
    /// Build RMI index
    async fn build_rmi(&self) -> Result<()>;
    
    /// Warmup mmap pages
    async fn warmup(&self) -> Result<()>;
    
    /// Create snapshot
    async fn snapshot(&self) -> Result<()>;
    
    /// Health check
    async fn health(&self) -> Result<bool>;
}

/// Client type selection
#[derive(Debug, Clone, Copy)]
pub enum ClientType {
    Http,
    UltraFast,
}

impl ClientType {
    pub fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "http" => Ok(ClientType::Http),
            "ultrafast" | "ultra_fast" | "binary" => Ok(ClientType::UltraFast),
            _ => anyhow::bail!("Unknown client type: {}. Use 'http' or 'ultrafast'", s),
        }
    }
}

/// Client factory
pub async fn create_client(
    client_type: ClientType,
    config: ClientConfig,
) -> Result<Box<dyn BenchClient>> {
    match client_type {
        ClientType::Http => Ok(Box::new(HttpClient::new(config)?)),
        ClientType::UltraFast => Ok(Box::new(UltraFastClient::new(config).await?)),
    }
}
