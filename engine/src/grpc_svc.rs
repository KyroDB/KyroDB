#[cfg(feature = "grpc")]
use std::sync::Arc;
use std::pin::Pin;
use futures::Stream;
use tokio_stream::{wrappers::BroadcastStream, StreamExt};
use uuid;

#[cfg(feature = "grpc")]
use tonic::{Request, Response, Status, Streaming};

#[cfg(feature = "grpc")]
pub mod pb {
    tonic::include_proto!("kyrodb.v1");
}

#[cfg(feature = "grpc")]
use pb::kyrodb_server::{Kyrodb, KyrodbServer};
#[cfg(feature = "grpc")]
use pb::{
    GetReq, GetResp, LookupReq, LookupResp, PutReq, PutResp, BatchGetReq, BatchGetResp,
    BatchPutReq, BatchPutResp, AppendReq, AppendResp, BatchAppendReq, BatchAppendResp,
    SubscribeReq, ReplayReq, Event as ProtoEvent, SnapshotReq, SnapshotResp,
    CompactReq, CompactResp, StatsReq, StatsResp, WarmupReq, WarmupResp,
    OffsetReq, OffsetResp, VectorInsertReq, VectorInsertResp, VectorSearchReq, VectorSearchResp,
    SqlReq, SqlResp, BuildRmiReq, BuildRmiResp,
};

// Use the engine library types
#[cfg(feature = "grpc")]
use kyrodb_engine::{PersistentEventLog, Record};

#[cfg(feature = "grpc")]
#[derive(Clone)]
pub struct GrpcService {
    log: Arc<PersistentEventLog>,
    auth_token: Option<String>,
    admin_token: Option<String>,
}

#[cfg(feature = "grpc")]
impl GrpcService {
    pub fn new(log: Arc<PersistentEventLog>) -> Self {
        Self {
            log,
            auth_token: std::env::var("KYRODB_AUTH_TOKEN").ok(),
            admin_token: std::env::var("KYRODB_ADMIN_TOKEN").ok(),
        }
    }

    pub fn new_with_auth(
        log: Arc<PersistentEventLog>,
        auth_token: Option<String>,
        admin_token: Option<String>
    ) -> Self {
        Self {
            log,
            auth_token,
            admin_token,
        }
    }

    pub fn into_server(self) -> KyrodbServer<Self> {
        KyrodbServer::new(self)
    }

    // Authentication helper
    fn authenticate<T>(&self, req: &Request<T>) -> Result<pb::UserRole, Status> {
        let metadata = req.metadata();
        let auth_header = metadata.get("authorization");

        if let Some(auth_value) = auth_header {
            if let Ok(auth_str) = auth_value.to_str() {
                if let Some(token) = auth_str.strip_prefix("Bearer ") {
                    if Some(token) == self.admin_token.as_deref() {
                        return Ok(pb::UserRole::Admin);
                    } else if Some(token) == self.auth_token.as_deref() {
                        return Ok(pb::UserRole::ReadWrite);
                    }
                }
            }
        }

        // No auth required = read-only
        Ok(pb::UserRole::ReadOnly)
    }

    fn require_role<T>(&self, req: &Request<T>, required: pb::UserRole) -> Result<(), Status> {
        let user_role = self.authenticate(req)?;
        if user_role as i32 >= required as i32 {
            Ok(())
        } else {
            Err(Status::permission_denied("insufficient permissions"))
        }
    }
}

#[cfg(feature = "grpc")]
#[tonic::async_trait]
impl Kyrodb for GrpcService {
    async fn lookup(&self, req: Request<LookupReq>) -> Result<Response<LookupResp>, Status> {
        let k = req.into_inner().key;
        let off = self.log.lookup_key(k).await;
        Ok(Response::new(LookupResp {
            found: off.is_some(),
            offset: off.unwrap_or(0),
        }))
    }

    async fn get(&self, req: Request<GetReq>) -> Result<Response<GetResp>, Status> {
        let k = req.into_inner().key;
        if let Some(off) = self.log.lookup_key(k).await {
            if let Some(buf) = self.log.get(off).await {
                if let Ok(rec) = bincode::deserialize::<Record>(&buf) {
                    return Ok(Response::new(GetResp {
                        found: true,
                        value: rec.value,
                    }));
                }
            }
        }
        Ok(Response::new(GetResp {
            found: false,
            value: vec![],
        }))
    }

    async fn put(&self, req: Request<PutReq>) -> Result<Response<PutResp>, Status> {
        self.require_role(&req, pb::UserRole::ReadWrite)?;
        let put_req = req.into_inner();
        let offset = self
            .log
            .append_kv(uuid::Uuid::new_v4(), put_req.key, put_req.value)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(PutResp { offset }))
    }

    async fn batch_get(&self, req: Request<BatchGetReq>) -> Result<Response<BatchGetResp>, Status> {
        let keys = req.into_inner().keys;
        let mut items = Vec::with_capacity(keys.len());

        for key in keys {
            let mut item = pb::batch_get_resp::BatchGetItem {
                key,
                found: false,
                value: vec![],
            };

            if let Some(off) = self.log.lookup_key(key).await {
                if let Some(buf) = self.log.get(off).await {
                    if let Ok(rec) = bincode::deserialize::<Record>(&buf) {
                        item.found = true;
                        item.value = rec.value;
                    }
                }
            }
            items.push(item);
        }

        Ok(Response::new(BatchGetResp { items }))
    }

    async fn batch_put(&self, req: Request<BatchPutReq>) -> Result<Response<BatchPutResp>, Status> {
        self.require_role(&req, pb::UserRole::ReadWrite)?;
        let batch_req = req.into_inner();
        let mut offsets = Vec::with_capacity(batch_req.items.len());

        for item in batch_req.items {
            let offset = self
                .log
                .append_kv(uuid::Uuid::new_v4(), item.key, item.value)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
            offsets.push(offset);
        }

        Ok(Response::new(BatchPutResp { offsets }))
    }

    async fn append(&self, req: Request<AppendReq>) -> Result<Response<AppendResp>, Status> {
        self.require_role(&req, pb::UserRole::ReadWrite)?;
        let append_req = req.into_inner();
        let offset = self
            .log
            .append(uuid::Uuid::new_v4(), append_req.payload)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(AppendResp { offset }))
    }

    async fn batch_append(
        &self,
        req: Request<Streaming<BatchAppendReq>>,
    ) -> Result<Response<BatchAppendResp>, Status> {
        self.require_role(&Request::new(()), pb::UserRole::ReadWrite)?;
        let mut stream = req.into_inner();
        let mut offsets = Vec::new();

        while let Some(result) = stream.message().await.transpose() {
            match result {
                Ok(batch_req) => {
                    let offset = self
                        .log
                        .append(uuid::Uuid::new_v4(), batch_req.payload)
                        .await
                        .map_err(|e| Status::internal(e.to_string()))?;
                    offsets.push(offset);
                }
                Err(e) => {
                    return Err(Status::internal(e.to_string()));
                }
            }
        }

        Ok(Response::new(BatchAppendResp { offsets }))
    }

    type SubscribeStream = Pin<Box<dyn Stream<Item = Result<ProtoEvent, Status>> + Send + 'static>>;

    async fn subscribe(&self, req: Request<SubscribeReq>) -> Result<Response<Self::SubscribeStream>, Status> {
        let subscribe_req = req.into_inner();
        let (past_events, rx) = self.log.subscribe(subscribe_req.from_offset).await;

        let past_stream = futures::stream::iter(
            past_events.into_iter().map(|e| {
                Ok(ProtoEvent {
                    offset: e.offset,
                    timestamp: e.timestamp,
                    request_id: e.request_id.to_string(),
                    payload: e.payload,
                })
            })
        );

        let live_stream = BroadcastStream::new(rx)
            .filter_map(|res| {
                match res {
                    Ok(event) => Some(Ok(ProtoEvent {
                        offset: event.offset,
                        timestamp: event.timestamp,
                        request_id: event.request_id.to_string(),
                        payload: event.payload,
                    })),
                    Err(_) => {
                        kyrodb_engine::metrics::inc_sse_lagged();
                        None
                    }
                }
            });

        let combined = past_stream.chain(live_stream);
        Ok(Response::new(Box::pin(combined)))
    }

    type ReplayStream = Pin<Box<dyn Stream<Item = Result<ProtoEvent, Status>> + Send + 'static>>;

    async fn replay(&self, req: Request<ReplayReq>) -> Result<Response<Self::ReplayStream>, Status> {
        let replay_req = req.into_inner();
        let events = self.log.replay(replay_req.start, replay_req.end).await;

        let stream = futures::stream::iter(events.into_iter().map(|e| {
            Ok(ProtoEvent {
                offset: e.offset,
                timestamp: e.timestamp,
                request_id: e.request_id.to_string(),
                payload: e.payload,
            })
        }));

        Ok(Response::new(Box::pin(stream)))
    }

    async fn snapshot(&self, req: Request<SnapshotReq>) -> Result<Response<SnapshotResp>, Status> {
        self.require_role(&req, pb::UserRole::Admin)?;
        match self.log.snapshot().await {
            Ok(_) => Ok(Response::new(SnapshotResp {
                success: true,
                error: String::new(),
            })),
            Err(e) => Ok(Response::new(SnapshotResp {
                success: false,
                error: e.to_string(),
            })),
        }
    }

    async fn compact(&self, req: Request<CompactReq>) -> Result<Response<CompactResp>, Status> {
        self.require_role(&req, pb::UserRole::Admin)?;
        match self.log.compact_keep_latest_and_snapshot_stats().await {
            Ok(stats) => Ok(Response::new(CompactResp {
                success: true,
                error: String::new(),
                before_bytes: stats.before_bytes,
                after_bytes: stats.after_bytes,
                segments_removed: stats.segments_removed as u64,
                segments_active: stats.segments_active as u64,
                keys_retained: stats.keys_retained as u64,
            })),
            Err(e) => Ok(Response::new(CompactResp {
                success: false,
                error: e.to_string(),
                before_bytes: 0,
                after_bytes: 0,
                segments_removed: 0,
                segments_active: 0,
                keys_retained: 0,
            })),
        }
    }

    async fn get_stats(&self, req: Request<StatsReq>) -> Result<Response<StatsResp>, Status> {
        self.require_role(&req, pb::UserRole::Admin)?;
        let total_events = self.log.get_offset().await;
        let wal_size = self.log.wal_size_bytes();
        let wal_segments = self.log.get_wal_segments().len() as u32;

        let snapshot_size = std::fs::metadata(self.log.data_dir().join("snapshot.bin"))
            .map(|m| m.len())
            .unwrap_or(0);

        let index_type = "btree".to_string(); // TODO: Detect actual index type

        Ok(Response::new(StatsResp {
            total_events,
            wal_size_bytes: wal_size,
            snapshot_size_bytes: snapshot_size,
            wal_segments,
            current_offset: total_events,
            index_type,
            rmi_leaves: 0, // TODO: Get from metrics
            rmi_size_bytes: 0, // TODO: Get from metrics
        }))
    }

    async fn warmup(&self, req: Request<WarmupReq>) -> Result<Response<WarmupResp>, Status> {
        self.require_role(&req, pb::UserRole::Admin)?;
        self.log.warmup().await;
        Ok(Response::new(WarmupResp { status: "ok".to_string() }))
    }

    async fn get_offset(&self, _req: Request<OffsetReq>) -> Result<Response<OffsetResp>, Status> {
        let offset = self.log.get_offset().await;
        Ok(Response::new(OffsetResp { offset }))
    }

    async fn vector_insert(&self, req: Request<VectorInsertReq>) -> Result<Response<VectorInsertResp>, Status> {
        self.require_role(&req, pb::UserRole::ReadWrite)?;
        let vec_req = req.into_inner();
        let offset = self
            .log
            .append_vector(uuid::Uuid::new_v4(), vec_req.key, vec_req.vector)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(VectorInsertResp { offset }))
    }

    async fn vector_search(&self, req: Request<VectorSearchReq>) -> Result<Response<VectorSearchResp>, Status> {
        let search_req = req.into_inner();
        let results = self.log.search_vector_ann(&search_req.query, search_req.k as usize).await;

        let results = results.into_iter().map(|(key, distance)| {
            pb::vector_search_resp::VectorResult { key, distance }
        }).collect();

        Ok(Response::new(VectorSearchResp { results }))
    }

    async fn execute_sql(&self, req: Request<SqlReq>) -> Result<Response<SqlResp>, Status> {
        let sql_req = req.into_inner();
        match crate::sql::execute_sql(&self.log, &sql_req.sql).await {
            Ok(crate::sql::SqlResponse::Ack { offset }) => {
                let ack = pb::SqlAck { offset };
                Ok(Response::new(SqlResp {
                    response: Some(pb::sql_resp::Response::Ack(ack)),
                    error: String::new(),
                }))
            }
            Ok(crate::sql::SqlResponse::Rows(rows)) => {
                let sql_rows = rows.into_iter().map(|(key, value)| {
                    pb::sql_rows::SqlRow { key, value }
                }).collect();
                let rows_resp = pb::SqlRows { rows: sql_rows };
                Ok(Response::new(SqlResp {
                    response: Some(pb::sql_resp::Response::Rows(rows_resp)),
                    error: String::new(),
                }))
            }
            Ok(crate::sql::SqlResponse::VecRows(rows)) => {
                let vec_rows = rows.into_iter().map(|(key, distance)| {
                    pb::sql_vec_rows::SqlVecRow { key, distance }
                }).collect();
                let vec_rows_resp = pb::SqlVecRows { rows: vec_rows };
                Ok(Response::new(SqlResp {
                    response: Some(pb::sql_resp::Response::VecRows(vec_rows_resp)),
                    error: String::new(),
                }))
            }
            Err(e) => Ok(Response::new(SqlResp {
                response: None,
                error: e.to_string(),
            })),
        }
    }

    async fn build_rmi(&self, req: Request<BuildRmiReq>) -> Result<Response<BuildRmiResp>, Status> {
        self.require_role(&req, pb::UserRole::Admin)?;
        #[cfg(feature = "learned-index")]
        {
            let pairs = self.log.collect_key_offset_pairs().await;
            let tmp = self.log.data_dir().join("index-rmi.tmp");
            let dst = self.log.data_dir().join("index-rmi.bin");
            let timer = kyrodb_engine::metrics::RMI_REBUILD_DURATION_SECONDS.start_timer();
            kyrodb_engine::metrics::RMI_REBUILD_IN_PROGRESS.set(1.0);
            kyrodb_engine::metrics::RMI_REBUILD_STALLS_TOTAL.inc();

            let pairs_clone = pairs.clone();
            let tmp_clone = tmp.clone();
            let write_res = tokio::task::spawn_blocking(move || {
                kyrodb_engine::index::RmiIndex::write_from_pairs_auto(
                    &tmp_clone,
                    &pairs_clone,
                )
            })
            .await;

            let mut ok = matches!(write_res, Ok(Ok(())));
            if ok {
                if let Ok(f) = std::fs::OpenOptions::new().read(true).open(&tmp) {
                    let _ = f.sync_all();
                }
                if let Err(e) = std::fs::rename(&tmp, &dst) {
                    eprintln!("❌ RMI rename failed: {}", e);
                    ok = false;
                }
                if let Err(e) = kyrodb_engine::fsync_dir(self.log.data_dir()) {
                    eprintln!("⚠️ fsync data dir after RMI build rename failed: {}", e);
                }
            }
            timer.observe_duration();
            kyrodb_engine::metrics::RMI_REBUILD_IN_PROGRESS.set(0.0);
            Ok(Response::new(BuildRmiResp { ok, count: pairs.len() as u64 }))
        }
        #[cfg(not(feature = "learned-index"))]
        {
            Ok(Response::new(BuildRmiResp {
                ok: false,
                count: 0,
            }))
        }
    }
}
