use anyhow::Result;
use std::time::{Duration, Instant};

#[derive(Clone)]
pub enum BenchClient {
    Http(reqwest::Client),
    #[cfg(feature = "grpc")]
    Grpc(crate::grpc_client::GrpcBenchClient),
    #[cfg(feature = "grpc")]
    Both {
        http: reqwest::Client,
        grpc: crate::grpc_client::GrpcBenchClient,
    },
    #[cfg(not(feature = "grpc"))]
    Both {
        http: reqwest::Client,
    },
}

impl BenchClient {
    pub async fn new(protocol: &str, http_addr: &str, grpc_addr: &str, auth_token: Option<String>) -> Result<Self> {
        match protocol {
            "http" => {
                let http = reqwest::Client::builder()
                    .timeout(Duration::from_secs(10))
                    .pool_max_idle_per_host(100)  // Connection pooling
                    .tcp_keepalive(Duration::from_secs(30))
                    .tcp_nodelay(true)
                    .build()?;
                Ok(BenchClient::Http(http))
            }
            #[cfg(feature = "grpc")]
            "grpc" => {
                let grpc = crate::grpc_client::GrpcBenchClient::connect(
                    grpc_addr.to_string(),
                    auth_token
                ).await?;
                Ok(BenchClient::Grpc(grpc))
            }
            #[cfg(feature = "grpc")]
            "both" => {
                let http = reqwest::Client::builder()
                    .timeout(Duration::from_secs(10))
                    .pool_max_idle_per_host(100)
                    .tcp_keepalive(Duration::from_secs(30))
                    .tcp_nodelay(true)
                    .build()?;
                    
                let grpc = crate::grpc_client::GrpcBenchClient::connect(
                    grpc_addr.to_string(),
                    auth_token
                ).await?;
                Ok(BenchClient::Both { http, grpc })
            }
            #[cfg(not(feature = "grpc"))]
            "both" => {
                let http = reqwest::Client::builder()
                    .timeout(Duration::from_secs(10))
                    .pool_max_idle_per_host(100)
                    .tcp_keepalive(Duration::from_secs(30))
                    .tcp_nodelay(true)
                    .build()?;
                Ok(BenchClient::Both { http })
            }
            _ => anyhow::bail!("Invalid protocol: {}", protocol),
        }
    }

    pub async fn put(&mut self, key: u64, value: Vec<u8>, auth_header: Option<&str>, base_url: &str) -> Result<u64> {
        match self {
            BenchClient::Http(client) => {
                Self::http_put(client, key, value, auth_header, base_url).await
            }
            #[cfg(feature = "grpc")]
            BenchClient::Grpc(client) => {
                client.put(key, value).await
            }
            #[cfg(feature = "grpc")]
            BenchClient::Both { grpc, .. } => {
                grpc.put(key, value).await
            }
            #[cfg(not(feature = "grpc"))]
            BenchClient::Both { http } => {
                Self::http_put(http, key, value, auth_header, base_url).await
            }
        }
    }

    pub async fn get(&mut self, key: u64, auth_header: Option<&str>, base_url: &str) -> Result<Option<Vec<u8>>> {
        match self {
            BenchClient::Http(client) => {
                Self::http_get_fast(client, key, auth_header, base_url).await
            }
            #[cfg(feature = "grpc")]
            BenchClient::Grpc(client) => {
                client.get_fast(key).await
            }
            #[cfg(feature = "grpc")]
            BenchClient::Both { grpc, .. } => {
                grpc.get_fast(key).await
            }
            #[cfg(not(feature = "grpc"))]
            BenchClient::Both { http } => {
                Self::http_get_fast(http, key, auth_header, base_url).await
            }
        }
    }

    pub async fn get_fast(&mut self, key: u64, auth_header: Option<&str>, base_url: &str) -> Result<Option<Vec<u8>>> {
        self.get(key, auth_header, base_url).await
    }

    pub async fn warmup(&mut self, auth_header: Option<&str>, base_url: &str) -> Result<()> {
        match self {
            BenchClient::Http(client) => {
                let url = format!("{}/v1/warmup", base_url);
                let mut req = client.post(url);
                if let Some(auth) = auth_header {
                    req = req.header("Authorization", auth);
                }
                let resp = req.send().await?;
                if resp.status().is_success() {
                    Ok(())
                } else {
                    anyhow::bail!("HTTP warmup failed with status: {}", resp.status())
                }
            }
            #[cfg(feature = "grpc")]
            BenchClient::Grpc(client) => {
                // gRPC warmup if available
                Ok(())
            }
            #[cfg(feature = "grpc")]
            BenchClient::Both { http, .. } => {
                let url = format!("{}/v1/warmup", base_url);
                let mut req = http.post(url);
                if let Some(auth) = auth_header {
                    req = req.header("Authorization", auth);
                }
                let resp = req.send().await?;
                if resp.status().is_success() {
                    Ok(())
                } else {
                    anyhow::bail!("HTTP warmup failed with status: {}", resp.status())
                }
            }
            #[cfg(not(feature = "grpc"))]
            BenchClient::Both { http } => {
                let url = format!("{}/v1/warmup", base_url);
                let mut req = http.post(url);
                if let Some(auth) = auth_header {
                    req = req.header("Authorization", auth);
                }
                let resp = req.send().await?;
                if resp.status().is_success() {
                    Ok(())
                } else {
                    anyhow::bail!("HTTP warmup failed with status: {}", resp.status())
                }
            }
        }
    }

    // Helper methods for HTTP operations
    async fn http_put(client: &reqwest::Client, key: u64, value: Vec<u8>, auth_header: Option<&str>, base_url: &str) -> Result<u64> {
        let url = format!("{}/v1/put", base_url);
        let value_str = String::from_utf8_lossy(&value);
        let body = serde_json::json!({ "key": key, "value": value_str });
        
        let mut req = client.post(url).json(&body);
        if let Some(auth) = auth_header {
            req = req.header("Authorization", auth);
        }
        
        let resp = req.send().await?;
        
        if resp.status().is_success() {
            let response_body: serde_json::Value = resp.json().await?;
            Ok(response_body["offset"].as_u64().unwrap_or(0))
        } else {
            anyhow::bail!("HTTP PUT failed with status: {}", resp.status())
        }
    }

    async fn http_get_fast(client: &reqwest::Client, key: u64, auth_header: Option<&str>, base_url: &str) -> Result<Option<Vec<u8>>> {
        let url = format!("{}/v1/get_fast/{}", base_url, key);
        
        let mut req = client.get(url);
        if let Some(auth) = auth_header {
            req = req.header("Authorization", auth);
        }
        
        let resp = req.send().await?;
        
        if resp.status().is_success() {
            let bytes = resp.bytes().await?;
            Ok(Some(bytes.to_vec()))
        } else {
            Ok(None)
        }
    }

    pub async fn health_check(&mut self, auth_header: Option<&str>, base_url: &str) -> Result<String> {
        match self {
            BenchClient::Http(client) => {
                let url = format!("{}/health", base_url);
                let mut req = client.get(url);
                if let Some(auth) = auth_header {
                    req = req.header("Authorization", auth);
                }
                let resp = req.send().await?;
                Ok(resp.text().await?)
            }
            #[cfg(feature = "grpc")]
            BenchClient::Grpc(client) => {
                client.health_check().await
            }
            #[cfg(feature = "grpc")]
            BenchClient::Both { grpc, .. } => {
                grpc.health_check().await
            }
            #[cfg(not(feature = "grpc"))]
            BenchClient::Both { http } => {
                let url = format!("{}/health", base_url);
                let mut req = http.get(url);
                if let Some(auth) = auth_header {
                    req = req.header("Authorization", auth);
                }
                let resp = req.send().await?;
                Ok(resp.text().await?)
            }
        }
    }

    #[cfg(feature = "grpc")]
    pub async fn batch_put_stream(&mut self, batches: Vec<Vec<(u64, Vec<u8>)>>) -> Result<Vec<u64>> {
        match self {
            BenchClient::Grpc(client) => {
                client.batch_put_stream(batches).await
            }
            BenchClient::Both { grpc, .. } => {
                grpc.batch_put_stream(batches).await
            }
            _ => anyhow::bail!("Streaming only supported with gRPC client"),
        }
    }

    // Add MessagePack support for PUT
    async fn http_put_msgpack(&self, client: &reqwest::Client, key: u64, value: Vec<u8>, auth_header: Option<&str>, base_url: &str) -> Result<u64> {
        let url = format!("{}/v1/put", base_url);
        let payload = rmp_serde::to_vec(&(key, value)).unwrap();
        let mut req = client.post(url)
            .header("Content-Type", "application/msgpack")
            .body(payload);
        if let Some(auth) = auth_header {
            req = req.header("Authorization", auth);
        }
        let resp = req.send().await?;
        if resp.status().is_success() {
            let bytes = resp.bytes().await?;
            let response: serde_json::Value = rmp_serde::from_slice(&bytes).unwrap();
            Ok(response["offset"].as_u64().unwrap_or(0))
        } else {
            anyhow::bail!("HTTP PUT failed with status: {}", resp.status())
        }
    }

    // Add batch PUT with MessagePack
    pub async fn batch_put(&mut self, items: Vec<(u64, Vec<u8>)>, auth_header: Option<&str>, base_url: &str) -> Result<Vec<u64>> {
        match self {
            BenchClient::Http(client) => {
                let url = format!("{}/v1/batch_put", base_url);
                let payload = rmp_serde::to_vec(&items).unwrap();
                let mut req = client.post(url)
                    .header("Content-Type", "application/msgpack")
                    .body(payload);
                if let Some(auth) = auth_header {
                    req = req.header("Authorization", auth);
                }
                let resp = req.send().await?;
                if resp.status().is_success() {
                    let bytes = resp.bytes().await?;
                    Ok(rmp_serde::from_slice(&bytes).unwrap())
                } else {
                    anyhow::bail!("HTTP batch PUT failed with status: {}", resp.status())
                }
            }
            #[cfg(feature = "grpc")]
            BenchClient::Grpc(client) => {
                client.batch_put(items).await
            }
            #[cfg(feature = "grpc")]
            BenchClient::Both { grpc, .. } => {
                grpc.batch_put(items).await
            }
            #[cfg(not(feature = "grpc"))]
            BenchClient::Both { http } => {
                let url = format!("{}/v1/batch_put", base_url);
                let payload = rmp_serde::to_vec(&items).unwrap();
                let mut req = http.post(url)
                    .header("Content-Type", "application/msgpack")
                    .body(payload);
                if let Some(auth) = auth_header {
                    req = req.header("Authorization", auth);
                }
                let resp = req.send().await?;
                if resp.status().is_success() {
                    let bytes = resp.bytes().await?;
                    Ok(rmp_serde::from_slice(&bytes).unwrap())
                } else {
                    anyhow::bail!("HTTP batch PUT failed with status: {}", resp.status())
                }
            }
        }
    }
}
