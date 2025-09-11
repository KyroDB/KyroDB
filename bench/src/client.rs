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
                    .timeout(Duration::from_secs(10))
                    .pool_max_idle_per_host(100)  // Connection pooling
                    .tcp_keepalive(Duration::from_secs(30))
                    .tcp_nodelay(true)
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
        
        let url = format!("{}/v1/put", base_url);
        let body = serde_json::json!({
            "key": key_num,
            "value": value
        });
        
        let response = self.http.post(&url)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await?;

        if !response.status().is_success() {
            anyhow::bail!("PUT failed with status: {}", response.status());
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
        
        let url = format!("{}/v1/lookup?key={}", base_url, key_num);
        let response = self.http.get(&url).send().await?;

        match response.status().as_u16() {
            200 => {
                let result: serde_json::Value = response.json().await?;
                if let Some(value) = result.get("value").and_then(|v| v.as_str()) {
                    Ok(Some(value.to_string()))
                } else {
                    Ok(None)
                }
            },
            404 => Ok(None),
            _ => anyhow::bail!("GET failed with status: {}", response.status()),
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
