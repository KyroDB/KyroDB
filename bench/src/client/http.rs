use anyhow::Result;
use async_trait::async_trait;
use std::time::Duration;
use super::{BenchClient, ClientConfig};

/// HTTP client for KyroDB benchmarks
#[derive(Clone)]
pub struct HttpClient {
    client: reqwest::Client,
    base_url: String,
}

impl HttpClient {
    pub fn new(config: ClientConfig) -> Result<Self> {
        let client = reqwest::Client::builder()
            // Connection pooling for maximum throughput
            .pool_max_idle_per_host(config.pool_size)
            .pool_idle_timeout(Duration::from_secs(600))
            .connect_timeout(Duration::from_millis(200))
            .timeout(Duration::from_secs(config.timeout_secs))
            // HTTP/2 optimization for multiplexing
            .http2_prior_knowledge()
            .http2_keep_alive_interval(Duration::from_secs(5))
            .http2_keep_alive_timeout(Duration::from_secs(10))
            .http2_keep_alive_while_idle(true)
            .http2_max_frame_size(Some(1048576)) // 1MB frame size
            // TCP optimization
            .tcp_keepalive(Duration::from_secs(600))
            .tcp_nodelay(true) // Disable Nagle's algorithm for low latency
            // TLS optimization
            .danger_accept_invalid_certs(true) // Skip cert validation for benchmarks
            .use_rustls_tls()
            .build()?;

        Ok(Self {
            client,
            base_url: config.base_url,
        })
    }
}

#[async_trait]
impl BenchClient for HttpClient {
    async fn put(&self, key: u64, value: Vec<u8>) -> Result<()> {
        let url = format!("{}/v1/put_fast/{}", self.base_url, key);
        
        let response = self
            .client
            .post(&url)
            .header("Content-Type", "application/octet-stream")
            .body(value)
            .send()
            .await?;

        if !response.status().is_success() {
            anyhow::bail!(
                "PUT failed with status: {} for key: {}",
                response.status(),
                key
            );
        }

        Ok(())
    }

    async fn get(&self, key: u64) -> Result<Option<Vec<u8>>> {
        let url = format!("{}/v1/get_fast/{}", self.base_url, key);
        
        let response = self.client.get(&url).send().await?;

        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }

        if !response.status().is_success() {
            anyhow::bail!(
                "GET failed with status: {} for key: {}",
                response.status(),
                key
            );
        }

        let bytes = response.bytes().await?;
        Ok(Some(bytes.to_vec()))
    }

    async fn lookup_batch(&self, keys: Vec<u64>) -> Result<Vec<(u64, Option<u64>)>> {
        let url = format!("{}/v1/lookup_batch", self.base_url);
        
        let response = self
            .client
            .post(&url)
            .json(&keys)
            .send()
            .await?;

        if !response.status().is_success() {
            anyhow::bail!("Lookup batch failed with status: {}", response.status());
        }

        let results: Vec<(u64, Option<u64>)> = response.json().await?;
        Ok(results)
    }

    async fn build_rmi(&self) -> Result<()> {
        let url = format!("{}/v1/rmi/build", self.base_url);
        
        let response = self.client.post(&url).send().await?;

        if !response.status().is_success() {
            anyhow::bail!("RMI build failed with status: {}", response.status());
        }

        Ok(())
    }

    async fn warmup(&self) -> Result<()> {
        let url = format!("{}/v1/warmup", self.base_url);
        
        let response = self.client.post(&url).send().await?;

        if !response.status().is_success() {
            anyhow::bail!("Warmup failed with status: {}", response.status());
        }

        Ok(())
    }

    async fn snapshot(&self) -> Result<()> {
        let url = format!("{}/v1/snapshot", self.base_url);
        
        let response = self.client.post(&url).send().await?;

        if !response.status().is_success() {
            anyhow::bail!("Snapshot failed with status: {}", response.status());
        }

        Ok(())
    }

    async fn health(&self) -> Result<bool> {
        let url = format!("{}/health", self.base_url);
        
        match self.client.get(&url).send().await {
            Ok(response) => Ok(response.status().is_success()),
            Err(_) => Ok(false),
        }
    }
}
