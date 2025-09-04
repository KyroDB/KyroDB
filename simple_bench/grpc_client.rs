use anyhow::Result;
use std::time::Duration;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Response, Status};

// Import generated protobuf types
pub mod kyrodb {
    tonic::include_proto!("kyrodb.v1");
}

use kyrodb::kyrodb_client::KyrodbClient;
use kyrodb::{
    BatchGetReq, BatchGetResp, BatchPutReq, BatchPutResp, BatchPutStreamReq, BatchPutStreamResp,
    BuildRmiReq, BuildRmiResp, GetReq, GetResp, HealthReq, HealthResp, PutReq, PutResp,
    SnapshotReq, SnapshotResp,
};

#[derive(Clone)]
pub struct UltraFastGrpcClient {
    client: KyrodbClient<Channel>,
    auth_token: Option<String>,
}

impl UltraFastGrpcClient {
    /// Connect with performance optimizations
    pub async fn connect(addr: String, auth_token: Option<String>) -> Result<Self> {
        let endpoint = Endpoint::from_shared(addr)?
            .timeout(Duration::from_secs(30))
            .tcp_keepalive(Some(Duration::from_secs(30)))
            .tcp_nodelay(true)
            .http2_keep_alive_interval(Duration::from_secs(10))
            .keep_alive_timeout(Duration::from_secs(5))
            .keep_alive_while_idle(true)
            .concurrency_limit(1000);  // High concurrency
            
        let channel = endpoint.connect().await?;
        let client = KyrodbClient::new(channel)
            .max_decoding_message_size(64 * 1024 * 1024)  // 64MB messages
            .max_encoding_message_size(64 * 1024 * 1024);
        
        Ok(Self { client, auth_token })
    }

    /// Single PUT with auth
    pub async fn put(&mut self, key: u64, value: Vec<u8>) -> Result<u64> {
        let mut req = Request::new(PutReq { key, value });
        
        if let Some(token) = &self.auth_token {
            req.metadata_mut().insert(
                "authorization",
                format!("Bearer {}", token).parse()?,
            );
        }
        
        let resp = self.client.put(req).await?;
        Ok(resp.into_inner().offset)
    }

    /// Standard GET
    pub async fn get(&mut self, key: u64) -> Result<Option<Vec<u8>>> {
        let req = Request::new(GetReq { key });
        let resp = self.client.get(req).await?;
        let inner = resp.into_inner();
        
        if inner.found {
            Ok(Some(inner.value))
        } else {
            Ok(None)
        }
    }

    /// Ultra-fast GET using zero-copy path (if available)
    pub async fn get_fast(&mut self, key: u64) -> Result<Option<Vec<u8>>> {
        // Try get_fast first, fallback to regular get if not available
        match self.try_get_fast(key).await {
            Ok(result) => Ok(result),
            Err(_) => self.get(key).await,
        }
    }

    async fn try_get_fast(&mut self, key: u64) -> Result<Option<Vec<u8>>> {
        let req = Request::new(GetFastReq { key });
        let resp = self.client.get_fast(req).await?;
        let inner = resp.into_inner();
        
        if inner.found {
            // For GetFast, we get raw bytes - need to deserialize if needed
            Ok(Some(inner.raw_bytes))
        } else {
            Ok(None)
        }
    }

    /// Batch PUT leveraging group commit
    pub async fn batch_put(&mut self, items: Vec<(u64, Vec<u8>)>) -> Result<Vec<u64>> {
        let batch_items = items
            .into_iter()
            .map(|(key, value)| BatchPutItem { key, value })
            .collect();
            
        let mut req = Request::new(BatchPutReq { items: batch_items });
        
        if let Some(token) = &self.auth_token {
            req.metadata_mut().insert(
                "authorization",
                format!("Bearer {}", token).parse()?,
            );
        }
        
        let resp = self.client.batch_put(req).await?;
        Ok(resp.into_inner().offsets)
    }

    /// Batch GET for efficient reads
    pub async fn batch_get(&mut self, keys: Vec<u64>) -> Result<Vec<(u64, Option<Vec<u8>>)>> {
        let req = Request::new(BatchGetReq { keys });
        let resp = self.client.batch_get(req).await?;
        
        Ok(resp.into_inner().items.into_iter().map(|item| {
            (item.key, if item.found { Some(item.value) } else { None })
        }).collect())
    }

    /// Streaming PUT for maximum throughput
    pub async fn batch_put_stream(&mut self, batches: Vec<Vec<(u64, Vec<u8>)>>) -> Result<Vec<u64>> {
        use futures::stream::StreamExt;
        
        // Create stream with optimal batching
        let stream = futures::stream::iter(
            batches.into_iter().map(|batch| {
                let items = batch.into_iter().map(|(key, value)| {
                    PutItem { key, value }
                }).collect();
                
                BatchPutStreamReq { items }
            })
        );
        
        let mut req = Request::new(stream);
        
        if let Some(token) = &self.auth_token {
            req.metadata_mut().insert(
                "authorization",
                format!("Bearer {}", token).parse()?,
            );
        }
        
        let resp = self.client.batch_put_stream(req).await?;
        Ok(resp.into_inner().offsets)
    }

    /// Health check
    pub async fn health_check(&mut self) -> Result<String> {
        let req = Request::new(HealthReq {});
        let resp = self.client.health(req).await?;
        Ok(resp.into_inner().status)
    }

    /// Trigger RMI build
    pub async fn build_rmi(&mut self) -> Result<String> {
        use kyrodb::{BuildRmiReq, BuildRmiResp};
        
        let mut req = Request::new(BuildRmiReq {});
        
        if let Some(token) = &self.auth_token {
            req.metadata_mut().insert(
                "authorization",
                format!("Bearer {}", token).parse()?,
            );
        }
        
        let resp = self.client.build_rmi(req).await?;
        Ok(resp.into_inner().status)
    }

    /// Force snapshot
    pub async fn snapshot(&mut self) -> Result<String> {
        use kyrodb::{SnapshotReq, SnapshotResp};
        
        let mut req = Request::new(SnapshotReq {});
        
        if let Some(token) = &self.auth_token {
            req.metadata_mut().insert(
                "authorization",
                format!("Bearer {}", token).parse()?,
            );
        }
        
        let resp = self.client.snapshot(req).await?;
        Ok(resp.into_inner().status)
    }
}
