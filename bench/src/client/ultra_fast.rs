use super::{BenchClient, ClientConfig};
use anyhow::Result;
use async_trait::async_trait;
use bb8::Pool;
use bytes::{BufMut, BytesMut};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use url::Url;

/// Ultra-fast binary protocol client with connection pooling
#[derive(Clone)]
pub struct UltraFastClient {
    connection_pool: Option<Arc<Pool<TcpConnectionManager>>>,
    base_url: String,
    http_fallback: reqwest::Client,
}

/// TCP Connection Manager for connection pool
#[derive(Clone)]
pub struct TcpConnectionManager {
    address: String,
}

impl TcpConnectionManager {
    pub fn new(address: String) -> Self {
        Self { address }
    }
}

#[async_trait::async_trait]
impl bb8::ManageConnection for TcpConnectionManager {
    type Connection = TcpStream;
    type Error = std::io::Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let stream = TcpStream::connect(&self.address).await?;
        let _ = stream.set_nodelay(true); // Disable Nagle's algorithm
        Ok(stream)
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        // Send ping frame
        let ping = [0xFF, 0xFF, 0xFF, 0xFF];
        conn.write_all(&ping).await?;

        let mut response = [0u8; 4];
        match tokio::time::timeout(
            std::time::Duration::from_millis(100),
            conn.read_exact(&mut response),
        )
        .await
        {
            Ok(Ok(_)) => Ok(()),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Connection validation failed",
            )),
        }
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        false
    }
}

impl UltraFastClient {
    pub async fn new(config: ClientConfig) -> Result<Self> {
        // Parse URL to derive binary protocol address
        let url = Url::parse(&config.base_url)?;
        let host = url.host_str().unwrap_or("127.0.0.1");
        let http_port = url.port().unwrap_or(3030);
        let binary_port = http_port + 1; // Binary protocol on HTTP + 1

        let binary_address = format!("{}:{}", host, binary_port);

        // Try to create connection pool (may fail if binary protocol not available)
        let connection_pool = {
            let manager = TcpConnectionManager::new(binary_address);
            match bb8::Pool::builder()
                .max_size(config.pool_size as u32)
                .build(manager)
                .await
            {
                Ok(pool) => Some(Arc::new(pool)),
                Err(_) => None,
            }
        };

        // HTTP fallback client
        let http_fallback = reqwest::Client::builder()
            .pool_max_idle_per_host(config.pool_size)
            .timeout(std::time::Duration::from_secs(config.timeout_secs))
            .tcp_nodelay(true)
            .build()?;

        Ok(Self {
            connection_pool,
            base_url: config.base_url,
            http_fallback,
        })
    }

    async fn binary_put(&self, key: u64, value: &[u8]) -> Result<()> {
        if let Some(pool) = &self.connection_pool {
            let mut conn = pool.get().await?;

            // Binary protocol: [OP:1byte][KEY:8bytes][LEN:4bytes][VALUE:nbytes]
            let mut buf = BytesMut::with_capacity(13 + value.len());
            buf.put_u8(0x01); // PUT opcode
            buf.put_u64(key);
            buf.put_u32(value.len() as u32);
            buf.put_slice(value);

            conn.write_all(&buf).await?;

            // Read response: [STATUS:1byte]
            let mut status = [0u8; 1];
            conn.read_exact(&mut status).await?;

            if status[0] != 0x00 {
                anyhow::bail!("Binary PUT failed with status: {}", status[0]);
            }

            Ok(())
        } else {
            anyhow::bail!("Binary protocol connection pool not available")
        }
    }

    async fn binary_get(&self, key: u64) -> Result<Option<Vec<u8>>> {
        if let Some(pool) = &self.connection_pool {
            let mut conn = pool.get().await?;

            // Binary protocol: [OP:1byte][KEY:8bytes]
            let mut buf = BytesMut::with_capacity(9);
            buf.put_u8(0x02); // GET opcode
            buf.put_u64(key);

            conn.write_all(&buf).await?;

            // Read response: [STATUS:1byte][LEN:4bytes][VALUE:nbytes]
            let mut status = [0u8; 1];
            conn.read_exact(&mut status).await?;

            if status[0] == 0x01 {
                // Not found
                return Ok(None);
            }

            if status[0] != 0x00 {
                anyhow::bail!("Binary GET failed with status: {}", status[0]);
            }

            let mut len_buf = [0u8; 4];
            conn.read_exact(&mut len_buf).await?;
            let len = u32::from_be_bytes(len_buf) as usize;

            let mut value = vec![0u8; len];
            conn.read_exact(&mut value).await?;

            Ok(Some(value))
        } else {
            anyhow::bail!("Binary protocol connection pool not available")
        }
    }
}

#[async_trait]
impl BenchClient for UltraFastClient {
    async fn put(&self, key: u64, value: Vec<u8>) -> Result<()> {
        // Try binary protocol first, fallback to HTTP
        match self.binary_put(key, &value).await {
            Ok(()) => Ok(()),
            Err(_) => {
                // HTTP fallback
                let url = format!("{}/v1/put_fast/{}", self.base_url, key);
                let response = self
                    .http_fallback
                    .post(&url)
                    .header("Content-Type", "application/octet-stream")
                    .body(value)
                    .send()
                    .await?;

                if !response.status().is_success() {
                    anyhow::bail!("HTTP PUT failed with status: {}", response.status());
                }

                Ok(())
            }
        }
    }

    async fn get(&self, key: u64) -> Result<Option<Vec<u8>>> {
        // Try binary protocol first, fallback to HTTP
        match self.binary_get(key).await {
            Ok(result) => Ok(result),
            Err(_) => {
                // HTTP fallback
                let url = format!("{}/v1/get_fast/{}", self.base_url, key);
                let response = self.http_fallback.get(&url).send().await?;

                if response.status() == reqwest::StatusCode::NOT_FOUND {
                    return Ok(None);
                }

                if !response.status().is_success() {
                    anyhow::bail!("HTTP GET failed with status: {}", response.status());
                }

                let bytes = response.bytes().await?;
                Ok(Some(bytes.to_vec()))
            }
        }
    }

    async fn lookup_batch(&self, keys: Vec<u64>) -> Result<Vec<(u64, Option<u64>)>> {
        // HTTP only for batch operations
        let url = format!("{}/v1/lookup_batch", self.base_url);
        let response = self.http_fallback.post(&url).json(&keys).send().await?;

        if !response.status().is_success() {
            anyhow::bail!("Lookup batch failed with status: {}", response.status());
        }

        let results: Vec<(u64, Option<u64>)> = response.json().await?;
        Ok(results)
    }

    async fn build_rmi(&self) -> Result<()> {
        let url = format!("{}/v1/rmi/build", self.base_url);
        let response = self.http_fallback.post(&url).send().await?;

        if !response.status().is_success() {
            anyhow::bail!("RMI build failed with status: {}", response.status());
        }

        Ok(())
    }

    async fn warmup(&self) -> Result<()> {
        let url = format!("{}/v1/warmup", self.base_url);
        let response = self.http_fallback.post(&url).send().await?;

        if !response.status().is_success() {
            anyhow::bail!("Warmup failed with status: {}", response.status());
        }

        Ok(())
    }

    async fn snapshot(&self) -> Result<()> {
        let url = format!("{}/v1/snapshot", self.base_url);
        let response = self.http_fallback.post(&url).send().await?;

        if !response.status().is_success() {
            anyhow::bail!("Snapshot failed with status: {}", response.status());
        }

        Ok(())
    }

    async fn health(&self) -> Result<bool> {
        let url = format!("{}/health", self.base_url);

        match self.http_fallback.get(&url).send().await {
            Ok(response) => Ok(response.status().is_success()),
            Err(_) => Ok(false),
        }
    }
}
