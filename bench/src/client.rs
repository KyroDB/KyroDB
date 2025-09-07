use anyhow::Result;
use std::time::Duration;

#[derive(Clone)]
pub struct BenchClient {
    http: reqwest::Client,
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
        let url = format!("{}/kv/{}", base_url, key);
        let response = self.http.put(&url)
            .header("Content-Type", "application/octet-stream")
            .body(value)
            .send()
            .await?;

        if !response.status().is_success() {
            anyhow::bail!("PUT failed with status: {}", response.status());
        }

        Ok(())
    }

    pub async fn get(&mut self, key: &str, _token: Option<&str>, base_url: &str) -> Result<Option<String>> {
        let url = format!("{}/kv/{}", base_url, key);
        let response = self.http.get(&url).send().await?;

        match response.status().as_u16() {
            200 => Ok(Some(response.text().await?)),
            404 => Ok(None),
            _ => anyhow::bail!("GET failed with status: {}", response.status()),
        }
    }

    pub async fn get_fast(&mut self, key: &str, _token: Option<&str>, base_url: &str) -> Result<Option<String>> {
        // For HTTP, get_fast is the same as regular get
        self.get(key, _token, base_url).await
    }

    pub async fn batch_put(&mut self, items: &[(String, String)], _token: Option<&str>, base_url: &str) -> Result<()> {
        // Use HTTP batch endpoint if available, otherwise fall back to individual PUTs
        let batch_data: Vec<serde_json::Value> = items.iter()
            .map(|(k, v)| serde_json::json!({"key": k, "value": v}))
            .collect();

        let url = format!("{}/kv/batch", base_url);
        let response = self.http.put(&url)
            .header("Content-Type", "application/json")
            .json(&batch_data)
            .send()
            .await;

        match response {
            Ok(resp) if resp.status().is_success() => Ok(()),
            _ => {
                // Fallback to individual PUTs
                for (key, value) in items {
                    self.put(key.clone(), value.clone(), _token, base_url).await?;
                }
                Ok(())
            }
        }
    }

    pub async fn batch_get(&mut self, keys: &[String], _token: Option<&str>, base_url: &str) -> Result<Vec<(String, Option<String>)>> {
        // Try batch endpoint first, fallback to individual GETs
        let url = format!("{}/kv/batch", base_url);
        let query_data = serde_json::json!({"keys": keys});
        
        let response = self.http.post(&url)
            .header("Content-Type", "application/json")
            .json(&query_data)
            .send()
            .await;

        match response {
            Ok(resp) if resp.status().is_success() => {
                // Try to parse batch response
                if let Ok(batch_result) = resp.json::<Vec<serde_json::Value>>().await {
                    let mut results = Vec::new();
                    for (i, key) in keys.iter().enumerate() {
                        if let Some(item) = batch_result.get(i) {
                            if let Some(value) = item.get("value").and_then(|v| v.as_str()) {
                                results.push((key.clone(), Some(value.to_string())));
                            } else {
                                results.push((key.clone(), None));
                            }
                        } else {
                            results.push((key.clone(), None));
                        }
                    }
                    return Ok(results);
                }
            }
            _ => {}
        }

        // Fallback to individual GETs
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
