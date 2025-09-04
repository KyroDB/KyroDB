use anyhow::Result;
use std::time::{Duration, Instant};
use tonic::transport::Channel;

// Import generated protobuf types
pub mod pb {
    tonic::include_proto!("kyrodb.v1");
}

use pb::{
    kyrodb_client::KyrodbClient, 
    PutReq, GetReq, GetFastReq, 
    BatchPutReq, BatchGetReq, BatchPutStreamReq,
    batch_put_req::BatchPutItem,
    batch_put_stream_req::PutItem,
    HealthReq,
};

#[derive(Clone)]
pub struct GrpcBenchClient {
    client: KyrodbClient<Channel>,
    auth_token: Option<String>,
}

impl GrpcBenchClient {
    pub async fn connect(addr: String, auth_token: Option<String>) -> Result<Self> {
        let channel = Channel::from_shared(addr)?
            .timeout(Duration::from_secs(30))
            .connect()
            .await?;
            
        let client = KyrodbClient::new(channel);
        
        Ok(Self { client, auth_token })
    }

    pub async fn put(&mut self, key: u64, value: Vec<u8>) -> Result<u64> {
        let mut req = tonic::Request::new(PutReq { key, value });
        
        if let Some(token) = &self.auth_token {
            req.metadata_mut().insert(
                "authorization",
                format!("Bearer {}", token).parse()?,
            );
        }
        
        let resp = self.client.put(req).await?;
        Ok(resp.into_inner().offset)
    }

    pub async fn get_fast(&mut self, key: u64) -> Result<Option<Vec<u8>>> {
        let req = tonic::Request::new(GetFastReq { key });
        let resp = self.client.get_fast(req).await?;
        let inner = resp.into_inner();
        
        if inner.found {
            Ok(Some(inner.value_bytes))
        } else {
            Ok(None)
        }
    }

    pub async fn batch_put(&mut self, items: Vec<(u64, Vec<u8>)>) -> Result<Vec<u64>> {
        let batch_items = items
            .into_iter()
            .map(|(key, value)| BatchPutItem { key, value })
            .collect();
            
        let mut req = tonic::Request::new(BatchPutReq { items: batch_items });
        
        if let Some(token) = &self.auth_token {
            req.metadata_mut().insert(
                "authorization",
                format!("Bearer {}", token).parse()?,
            );
        }
        
        let resp = self.client.batch_put(req).await?;
        Ok(resp.into_inner().offsets)
    }

    pub async fn batch_get(&mut self, keys: Vec<u64>) -> Result<Vec<(u64, Option<Vec<u8>>)>> {
        let req = tonic::Request::new(BatchGetReq { keys });
        let resp = self.client.batch_get(req).await?;
        
        let results = resp
            .into_inner()
            .items
            .into_iter()
            .map(|item| {
                if item.found {
                    (item.key, Some(item.value))
                } else {
                    (item.key, None)
                }
            })
            .collect();
            
        Ok(results)
    }

    // High-performance streaming put for maximum throughput
    pub async fn batch_put_stream(&mut self, batches: Vec<Vec<(u64, Vec<u8>)>>) -> Result<Vec<u64>> {
        use futures::stream::StreamExt;
        
        // Create stream
        let stream = futures::stream::iter(
            batches.into_iter().flat_map(|batch| {
                batch.into_iter().map(|(key, value)| {
                    BatchPutStreamReq { 
                        items: vec![PutItem { key, value }] 
                    }
                })
            })
        );

        let mut req = tonic::Request::new(stream);
        
        if let Some(token) = &self.auth_token {
            req.metadata_mut().insert(
                "authorization",
                format!("Bearer {}", token).parse()?,
            );
        }

        let resp = self.client.batch_put_stream(req).await?;
        Ok(resp.into_inner().offsets)
    }

    pub async fn health_check(&mut self) -> Result<String> {
        let req = tonic::Request::new(HealthReq {});
        let resp = self.client.health(req).await?;
        Ok(resp.into_inner().status)
    }
}
