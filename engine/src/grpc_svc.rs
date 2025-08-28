#[cfg(feature = "grpc")]
use std::sync::Arc;

#[cfg(feature = "grpc")]
use tonic::{Request, Response, Status};

#[cfg(feature = "grpc")]
pub mod pb {
    tonic::include_proto!("kyrodb.v1");
}

#[cfg(feature = "grpc")]
use pb::kyrodb_server::{Kyrodb, KyrodbServer};
#[cfg(feature = "grpc")]
use pb::{GetReq, GetResp, LookupReq, LookupResp};

// Use the engine library types
#[cfg(feature = "grpc")]
use kyrodb_engine::{PersistentEventLog, Record};

#[cfg(feature = "grpc")]
#[derive(Clone)]
pub struct GrpcService {
    log: Arc<PersistentEventLog>,
}

#[cfg(feature = "grpc")]
impl GrpcService {
    pub fn new(log: Arc<PersistentEventLog>) -> Self { Self { log } }
    pub fn into_server(self) -> KyrodbServer<Self> { KyrodbServer::new(self) }
}

#[cfg(feature = "grpc")]
#[tonic::async_trait]
impl Kyrodb for GrpcService {
    async fn lookup(&self, req: Request<LookupReq>) -> Result<Response<LookupResp>, Status> {
        let k = req.into_inner().key;
        let off = self.log.lookup_key(k).await;
        Ok(Response::new(LookupResp { found: off.is_some(), offset: off.unwrap_or(0) }))
    }

    async fn get(&self, req: Request<GetReq>) -> Result<Response<GetResp>, Status> {
        let k = req.into_inner().key;
        if let Some(off) = self.log.lookup_key(k).await {
            if let Some(buf) = self.log.get(off).await {
                if let Ok(rec) = bincode::deserialize::<Record>(&buf) {
                    return Ok(Response::new(GetResp { found: true, value: rec.value }));
                }
            }
        }
        Ok(Response::new(GetResp { found: false, value: vec![] }))
    }
}
