use anyhow::Result;
use std::time::Duration;

#[derive(Clone)]
pub struct BenchClient {
    pub http: reqwest::Client,
}

impl BenchClient {
    pub async fn new(protocol: &str, http_addr: &str, _grpc_addr: &str, _auth_token: Option<String>) -> Result<Self> {
        match protocol {
            "http" => {
                let http = reqwest::Client::builder()
                    // ✅ CRITICAL FIX: Aggressive connection pooling for maximum throughput
                    .pool_max_idle_per_host(2000)  // Increased from 1000 
                    .pool_idle_timeout(Duration::from_secs(600))  // Keep connections alive longer
                    .connect_timeout(Duration::from_millis(200))  // Faster connection timeout
                    .timeout(Duration::from_millis(2000))  // Slightly increased total timeout
                    // ✅ CRITICAL FIX: HTTP/2 optimization for multiplexing
                    .http2_prior_knowledge()  // Force HTTP/2 for better multiplexing
                    .http2_keep_alive_interval(Duration::from_secs(5))  // Frequent keepalive
                    .http2_keep_alive_timeout(Duration::from_secs(10))
                    .http2_keep_alive_while_idle(true)
                    .http2_max_frame_size(Some(1048576))  // 1MB frame size for large payloads
                    // ✅ CRITICAL FIX: TCP optimization
                    .tcp_keepalive(Duration::from_secs(600))
                    .tcp_nodelay(true)  // Disable Nagle's algorithm for low latency
                    // ✅ CRITICAL FIX: TLS optimization (if using HTTPS)
                    .danger_accept_invalid_certs(true)  // Skip cert validation for benchmarks
                    .use_rustls_tls()  // Use faster Rust TLS implementation
                    .build()?;
                Ok(BenchClient { http })
            }
            _ => {
                anyhow::bail!("Only 'http' protocol supported in Phase 0");
            }
        }
    }

    pub async fn put(&mut self, key: String, value: String, _token: Option<&str>, base_url: &str) -> Result<()> {
        // Parse key as number (KyroDB expects u64 keys)
        let key_num: u64 = key.parse().unwrap_or_else(|_| {
            // If not a number, hash the key to get a u64
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut hasher = DefaultHasher::new();
            key.hash(&mut hasher);
            hasher.finish()
        });
        
        // Use high-performance binary endpoint instead of JSON
        let url = format!("{}/v1/put_fast/{}", base_url, key_num);
        
        let response = self.http.post(&url)
            .header("Content-Type", "application/octet-stream")
            .body(value.into_bytes())
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("HTTP PUT request failed: {}", e))?;

        if !response.status().is_success() {
            anyhow::bail!("PUT failed with status: {} for key: {}", response.status(), key_num);
        }

        Ok(())
    }

    pub async fn get(&mut self, key: &str, _token: Option<&str>, base_url: &str) -> Result<Option<String>> {
        // Parse key as number (KyroDB expects u64 keys)
        let key_num: u64 = key.parse().unwrap_or_else(|_| {
            // If not a number, hash the key to get a u64
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut hasher = DefaultHasher::new();
            key.hash(&mut hasher);
            hasher.finish()
        });
        
        // Use high-performance binary endpoint for reads too
        let url = format!("{}/v1/get_fast/{}", base_url, key_num);
        let response = self.http.get(&url)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("HTTP GET request failed: {}", e))?;

        match response.status().as_u16() {
            200 => Ok(Some(response.text().await?)),
            404 => Ok(None),
            _ => anyhow::bail!("GET failed with status: {} for key: {}", response.status(), key_num),
        }
    }

    pub async fn get_fast(&mut self, key: &str, _token: Option<&str>, base_url: &str) -> Result<Option<String>> {
        // Parse key as number (KyroDB expects u64 keys)
        let key_num: u64 = key.parse().unwrap_or_else(|_| {
            // If not a number, hash the key to get a u64
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut hasher = DefaultHasher::new();
            key.hash(&mut hasher);
            hasher.finish()
        });
        
        let url = format!("{}/v1/lookup_fast/{}", base_url, key_num);
        let response = self.http.get(&url).send().await?;

        match response.status().as_u16() {
            200 => Ok(Some(response.text().await?)),
            404 => Ok(None),
            _ => anyhow::bail!("GET failed with status: {}", response.status()),
        }
    }

    pub async fn batch_put(&mut self, items: &[(String, String)], _token: Option<&str>, base_url: &str) -> Result<()> {
        // KyroDB doesn't have batch endpoints, so fall back to individual PUTs
        for (key, value) in items {
            self.put(key.clone(), value.clone(), _token, base_url).await?;
        }
        Ok(())
    }

    pub async fn batch_get(&mut self, keys: &[String], _token: Option<&str>, base_url: &str) -> Result<Vec<(String, Option<String>)>> {
        // KyroDB doesn't have batch endpoints, so fall back to individual GETs
        let mut results = Vec::new();
        for key in keys {
            let value = self.get(key, _token, base_url).await?;
            results.push((key.clone(), value));
        }
        Ok(results)
    }

    pub async fn health_check(&mut self, base_url: &str) -> Result<bool> {
        let url = format!("{}/health", base_url);
        let response = self.http.get(&url).send().await?;
        Ok(response.status().is_success())
    }
}
