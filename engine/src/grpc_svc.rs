use futures::Stream;
use std::pin::Pin;
use std::sync::Arc;
#[cfg(feature = "grpc")]
use tokio_stream::{wrappers::BroadcastStream, StreamExt};

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
    AppendReq, AppendResp, BatchAppendReq, BatchAppendResp, BatchGetReq, BatchGetResp, BatchPutReq,
    BatchPutResp, BatchPutStreamReq, BatchPutStreamResp, BuildInfoReq, BuildInfoResp, 
    BuildRmiReq, BuildRmiResp, CompactReq, CompactResp, Event as ProtoEvent, GetFastReq, 
    GetFastResp, GetReq, GetResp, HealthReq, HealthResp, LookupFastReq, LookupFastResp, 
    LookupReq, LookupResp, MetricsReq, MetricsResp, OffsetReq, OffsetResp, PutReq, PutResp, 
    ReplayReq, SnapshotReq, SnapshotResp, SqlReq, SqlResp, StatsReq, StatsResp, SubscribeEvent, 
    SubscribeReq, SubscribeStreamReq, VectorBatchInsertReq, VectorBatchInsertResp, 
    VectorInsertReq, VectorInsertResp, VectorSearchReq, VectorSearchResp, WarmupReq, WarmupResp,
};

// Use the engine library types
#[cfg(feature = "grpc")]
use crate::engine_crate::{PersistentEventLog, deserialize_record_compat, fsync_dir, index};
#[cfg(feature = "grpc")]
use crate::engine_crate::metrics;

#[cfg(feature = "grpc")]
#[derive(Clone)]
pub struct GrpcService {
    log: Arc<PersistentEventLog>,
    auth_token: Option<String>,
    admin_token: Option<String>,
    auth_enabled: bool,
}

#[cfg(feature = "grpc")]
impl GrpcService {
    #[allow(dead_code)]
    pub fn new(log: Arc<PersistentEventLog>) -> Self {
        let auth_token = std::env::var("KYRODB_AUTH_TOKEN").ok();
        let admin_token = std::env::var("KYRODB_ADMIN_TOKEN").ok();
        let auth_enabled = auth_token.is_some() || admin_token.is_some();
        Self { log, auth_token, admin_token, auth_enabled }
    }

    pub fn new_with_auth(
        log: Arc<PersistentEventLog>,
        auth_token: Option<String>,
        admin_token: Option<String>,
    ) -> Self {
        let auth_enabled = auth_token.is_some() || admin_token.is_some();
        Self { log, auth_token, admin_token, auth_enabled }
    }

    #[allow(dead_code)]
    pub fn into_server(self) -> KyrodbServer<Self> {
        KyrodbServer::new(self)
    }

    // Authentication helper
    #[allow(clippy::result_large_err)]
    fn authenticate<T>(&self, req: &Request<T>) -> Result<pb::UserRole, Status> {
        if !self.auth_enabled {
            // No auth configured -> full access
            return Ok(pb::UserRole::Admin);
        }
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

    #[allow(clippy::result_large_err)]
    fn require_role<T>(&self, req: &Request<T>, required: pb::UserRole) -> Result<(), Status> {
        if !self.auth_enabled {
            return Ok(());
        }
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
        
        // Ultra-fast RMI lookup with zero-copy optimization
        if let Some(off) = self.log.lookup_key(k).await {
            if let Some(buf) = self.log.get(off).await {
                // Optimized deserialization with SIMD hints
                if let Some(rec) = deserialize_record_compat(&buf) {
                    #[cfg(not(feature = "bench-no-metrics"))]
                    metrics::RMI_HITS_TOTAL.inc();
                    
                    return Ok(Response::new(GetResp {
                        found: true,
                        value: rec.value,
                    }));
                }
            }
        }
        
        #[cfg(not(feature = "bench-no-metrics"))]
        metrics::RMI_MISSES_TOTAL.inc();
        
        Ok(Response::new(GetResp {
            found: false,
            value: vec![],
        }))
    }

    async fn put(&self, req: Request<PutReq>) -> Result<Response<PutResp>, Status> {
        self.require_role(&req, pb::UserRole::ReadWrite)?;
        let put_req = req.into_inner();
        
        // Leverage group commit for ultra-fast writes
        let offset = self
            .log
            .append_kv(uuid::Uuid::new_v4(), put_req.key, put_req.value)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        #[cfg(not(feature = "bench-no-metrics"))]
        metrics::PUT_OPERATIONS_TOTAL.inc();

        Ok(Response::new(PutResp { offset }))
    }

    async fn batch_get(&self, req: Request<BatchGetReq>) -> Result<Response<BatchGetResp>, Status> {
        let keys = req.into_inner().keys;
        
        // Simplified high-performance batch lookup - no micro-futures overhead
        let mut items = Vec::with_capacity(keys.len());
        
        // Simple iterative lookup - async operations provide natural concurrency
        for key in keys {
            let mut item = pb::batch_get_resp::BatchGetItem {
                key,
                found: false,
                value: vec![],
            };

            // Lock-free RMI lookup path 
            if let Some(off) = self.log.lookup_key(key).await {
                if let Some(buf) = self.log.get(off).await {
                    // Fast-only deserialization
                    if let Some(rec) = deserialize_record_compat(&buf) {
                        item.found = true;
                        item.value = rec.value;
                    }
                }
            }
            items.push(item);
        }
        
        #[cfg(not(feature = "bench-no-metrics"))]
        {
            metrics::BATCH_GET_TOTAL.inc();
            metrics::BATCH_GET_SIZE_HISTOGRAM.observe(items.len() as f64);
        }
        
        Ok(Response::new(BatchGetResp { items }))
    }

    async fn batch_put(&self, req: Request<BatchPutReq>) -> Result<Response<BatchPutResp>, Status> {
        self.require_role(&req, pb::UserRole::ReadWrite)?;
        let batch_req = req.into_inner();
        
        // Group commit optimization: Leverage engine's group commit for maximum throughput
        if batch_req.items.len() > 1 {
            // Use engine's optimized group commit batch processing
            let records: Vec<_> = batch_req.items.into_iter()
                .map(|item| (uuid::Uuid::new_v4(), item.key, item.value))
                .collect();
            
            // Single group commit operation for entire batch
            let offsets = self.log.append_batch_kv(records)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
                
            #[cfg(not(feature = "bench-no-metrics"))]
            {
                metrics::BATCH_PUT_TOTAL.inc();
                metrics::BATCH_PUT_SIZE_HISTOGRAM.observe(offsets.len() as f64);
                metrics::GROUP_COMMIT_UTILIZATION.observe(offsets.len() as f64);
            }
            
            Ok(Response::new(BatchPutResp { offsets }))
        } else {
            // Single item: use regular append with group commit
            let item = batch_req.items.into_iter().next().unwrap();
            let offset = self
                .log
                .append_kv(uuid::Uuid::new_v4(), item.key, item.value)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;

            Ok(Response::new(BatchPutResp { offsets: vec![offset] }))
        }
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

    async fn subscribe(
        &self,
        req: Request<SubscribeReq>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let subscribe_req = req.into_inner();
        let (past_events, rx) = self.log.subscribe(subscribe_req.from_offset).await;

        let past_stream = futures::stream::iter(past_events.into_iter().map(|e| {
            Ok(ProtoEvent {
                offset: e.offset,
                timestamp: e.timestamp,
                request_id: e.request_id.to_string(),
                payload: e.payload,
            })
        }));

        let live_stream = BroadcastStream::new(rx).filter_map(|res| match res {
            Ok(event) => Some(Ok(ProtoEvent {
                offset: event.offset,
                timestamp: event.timestamp,
                request_id: event.request_id.to_string(),
                payload: event.payload,
            })),
            Err(_) => {
                metrics::inc_sse_lagged();
                None
            }
        });

        let combined = past_stream.chain(live_stream);
        Ok(Response::new(Box::pin(combined)))
    }

    type ReplayStream = Pin<Box<dyn Stream<Item = Result<ProtoEvent, Status>> + Send + 'static>>;

    async fn replay(
        &self,
        req: Request<ReplayReq>,
    ) -> Result<Response<Self::ReplayStream>, Status> {
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

    async fn warmup(&self, req: Request<WarmupReq>) -> Result<Response<WarmupResp>, Status> {
        self.require_role(&req, pb::UserRole::Admin)?;
        self.log.warmup().await;
        Ok(Response::new(WarmupResp {
            status: "ok".to_string(),
        }))
    }

    async fn vector_insert(
        &self,
        req: Request<VectorInsertReq>,
    ) -> Result<Response<VectorInsertResp>, Status> {
        self.require_role(&req, pb::UserRole::ReadWrite)?;
        let vec_req = req.into_inner();
        let offset = self
            .log
            .append_vector(uuid::Uuid::new_v4(), vec_req.key, vec_req.vector)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(VectorInsertResp { offset }))
    }

    async fn vector_search(
        &self,
        req: Request<VectorSearchReq>,
    ) -> Result<Response<VectorSearchResp>, Status> {
        let search_req = req.into_inner();
        let results = self
            .log
            .search_vector_ann(&search_req.query, search_req.k as usize)
            .await;

        let results = results
            .into_iter()
            .map(|(key, distance)| pb::vector_search_resp::VectorResult { key, distance })
            .collect();

        Ok(Response::new(VectorSearchResp { results }))
    }

    async fn execute_sql(&self, req: Request<SqlReq>) -> Result<Response<SqlResp>, Status> {
        let _sql_req = req.into_inner();
        // SQL module has been removed for performance optimization
        Ok(Response::new(SqlResp {
            response: None,
            error: "SQL functionality disabled for performance optimization".to_string(),
        }))
    }

    async fn build_rmi(&self, req: Request<BuildRmiReq>) -> Result<Response<BuildRmiResp>, Status> {
        self.require_role(&req, pb::UserRole::Admin)?;
        #[cfg(feature = "learned-index")]
        {
            let pairs = self.log.collect_key_offset_pairs().await;
            let tmp = self.log.data_dir().join("index-rmi.tmp");
            let dst = self.log.data_dir().join("index-rmi.bin");
            let timer = metrics::RMI_REBUILD_DURATION_SECONDS.start_timer();
            metrics::RMI_REBUILD_IN_PROGRESS.set(1.0);
            metrics::RMI_REBUILDS_TOTAL.inc();

            let pairs_clone = pairs.clone();
            let tmp_clone = tmp.clone();
            let write_res = tokio::task::spawn_blocking(move || {
                index::RmiIndex::write_from_pairs_auto(&tmp_clone, &pairs_clone)
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
                if let Err(e) = fsync_dir(self.log.data_dir()) {
                    eprintln!("⚠️ fsync data dir after RMI build rename failed: {}", e);
                }
            }
            timer.observe_duration();
            metrics::RMI_REBUILD_IN_PROGRESS.set(0.0);
            Ok(Response::new(BuildRmiResp {
                ok,
                count: pairs.len() as u64,
            }))
        }
        #[cfg(not(feature = "learned-index"))]
        {
            Ok(Response::new(BuildRmiResp {
                ok: false,
                count: 0,
            }))
        }
    }

    async fn snapshot(&self, req: Request<SnapshotReq>) -> Result<Response<SnapshotResp>, Status> {
        self.require_role(&req, pb::UserRole::Admin)?;
        match self.log.snapshot().await {
            Ok(()) => Ok(Response::new(SnapshotResp {
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

    async fn get_stats(&self, _req: Request<StatsReq>) -> Result<Response<StatsResp>, Status> {
        // Get various stats from the log
        let offset = self.log.get_offset().await;
        let stats = StatsResp {
            total_events: offset,
            wal_size_bytes: 0,      // TODO: implement actual stats
            snapshot_size_bytes: 0, // TODO: implement actual stats
            wal_segments: 0,        // TODO: implement actual stats
            current_offset: offset,
            index_type: "rmi".to_string(),
            rmi_leaves: 0,     // TODO: implement actual stats
            rmi_size_bytes: 0, // TODO: implement actual stats
        };
        Ok(Response::new(stats))
    }

    async fn get_offset(&self, _req: Request<OffsetReq>) -> Result<Response<OffsetResp>, Status> {
        let offset = self.log.get_offset().await;
        Ok(Response::new(OffsetResp { offset }))
    }

    // Fast path methods for maximum performance
    async fn lookup_fast(&self, req: Request<LookupFastReq>) -> Result<Response<LookupFastResp>, Status> {
        let k = req.into_inner().key;
        if let Some(offset) = self.log.lookup_key(k).await {
            Ok(Response::new(LookupFastResp {
                found: true,
                offset_bytes: offset.to_le_bytes().to_vec(),
            }))
        } else {
            Ok(Response::new(LookupFastResp {
                found: false,
                offset_bytes: vec![],
            }))
        }
    }

    async fn get_fast(&self, req: Request<GetFastReq>) -> Result<Response<GetFastResp>, Status> {
        let k = req.into_inner().key;
        if let Some(off) = self.log.lookup_key(k).await {
            if let Some(buf) = self.log.get(off).await {
                // Use optimized deserialization for fast path
                if let Some(rec) = deserialize_record_compat(&buf) {
                    return Ok(Response::new(GetFastResp {
                        found: true,
                        value_bytes: rec.value,
                    }));
                }
            }
        }
        Ok(Response::new(GetFastResp {
            found: false,
            value_bytes: vec![],
        }))
    }

    // High-throughput streaming operations with enhanced batching
    async fn batch_put_stream(
        &self,
        req: Request<Streaming<BatchPutStreamReq>>,
    ) -> Result<Response<BatchPutStreamResp>, Status> {
        self.require_role(&Request::new(()), pb::UserRole::ReadWrite)?;
        let mut stream = req.into_inner();
        let mut total_offsets = Vec::new();
        let mut batch_count = 0;

        // Streaming optimization: Process with maximum group commit efficiency
        const OPTIMAL_BATCH_SIZE: usize = 1000; // Align with group commit window
        let mut pending_records = Vec::with_capacity(OPTIMAL_BATCH_SIZE * 2);

        while let Some(batch_req) = stream.message().await? {
            // Accumulate records for optimal group commit batching
            for item in batch_req.items {
                pending_records.push((uuid::Uuid::new_v4(), item.key, item.value));
                
                // Process when we reach optimal batch size
                if pending_records.len() >= OPTIMAL_BATCH_SIZE {
                    let batch_offsets = self.log.append_batch_kv(pending_records.clone())
                        .await
                        .map_err(|e| Status::internal(e.to_string()))?;
                    
                    total_offsets.extend(batch_offsets);
                    batch_count += 1;
                    pending_records.clear();
                }
            }
        }

        // Process any remaining records
        if !pending_records.is_empty() {
            let final_offsets = self.log.append_batch_kv(pending_records)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
            total_offsets.extend(final_offsets);
            batch_count += 1;
        }

        #[cfg(not(feature = "bench-no-metrics"))]
        {
            metrics::STREAM_BATCHES_PROCESSED_TOTAL.inc_by(batch_count as f64);
            metrics::STREAM_ITEMS_PROCESSED_TOTAL.inc_by(total_offsets.len() as f64);
        }

        Ok(Response::new(BatchPutStreamResp {
            offsets: total_offsets,
            batches_processed: batch_count,
        }))
    }

    async fn vector_batch_insert(
        &self,
        req: Request<Streaming<VectorBatchInsertReq>>,
    ) -> Result<Response<VectorBatchInsertResp>, Status> {
        self.require_role(&Request::new(()), pb::UserRole::ReadWrite)?;
        let mut stream = req.into_inner();
        let mut total_offsets = Vec::new();
        let mut items_processed = 0;

        while let Some(batch_req) = stream.message().await? {
            for item in batch_req.items {
                let offset = self
                    .log
                    .append_vector(uuid::Uuid::new_v4(), item.key, item.vector)
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?;
                total_offsets.push(offset);
                items_processed += 1;
            }
        }

        Ok(Response::new(VectorBatchInsertResp {
            offsets: total_offsets,
            items_processed,
        }))
    }

    // Enhanced streaming with event filtering
    type SubscribeStreamStream = Pin<Box<dyn Stream<Item = Result<SubscribeEvent, Status>> + Send + 'static>>;
    
    async fn subscribe_stream(
        &self,
        req: Request<SubscribeStreamReq>,
    ) -> Result<Response<Self::SubscribeStreamStream>, Status> {
        let subscribe_req = req.into_inner();
        let (past_events, rx) = self.log.subscribe(subscribe_req.from_offset).await;

        // Convert past events to stream
        let past_stream = futures::stream::iter(past_events.into_iter().map(|e| {
            Ok(SubscribeEvent {
                offset: e.offset,
                timestamp: e.timestamp,
                request_id: e.request_id.to_string(),
                event_type: "past".to_string(),
                payload: e.payload,
            })
        }));

        // Convert live events to stream
        let live_stream = BroadcastStream::new(rx).filter_map(|res| match res {
            Ok(event) => Some(Ok(SubscribeEvent {
                offset: event.offset,
                timestamp: event.timestamp,
                request_id: event.request_id.to_string(),
                event_type: "live".to_string(),
                payload: event.payload,
            })),
            Err(_) => {
                metrics::inc_sse_lagged();
                None
            }
        });

        let combined_stream = past_stream.chain(live_stream);
        Ok(Response::new(Box::pin(combined_stream)))
    }

    // Health and monitoring endpoints
    async fn health(&self, _req: Request<HealthReq>) -> Result<Response<HealthResp>, Status> {
        Ok(Response::new(HealthResp {
            status: "ok".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            commit: option_env!("GIT_COMMIT_HASH").unwrap_or("unknown").to_string(),
        }))
    }

    async fn get_build_info(&self, _req: Request<BuildInfoReq>) -> Result<Response<BuildInfoResp>, Status> {
        Ok(Response::new(BuildInfoResp {
            version: env!("CARGO_PKG_VERSION").to_string(),
            name: env!("CARGO_PKG_NAME").to_string(),
            commit: option_env!("GIT_COMMIT_HASH").unwrap_or("unknown").to_string(),
            branch: option_env!("GIT_BRANCH").unwrap_or("unknown").to_string(),
            build_time: option_env!("BUILD_TIME").unwrap_or("unknown").to_string(),
            rust_version: option_env!("RUST_VERSION").unwrap_or("unknown").to_string(),
            target_triple: option_env!("TARGET_TRIPLE").unwrap_or("unknown").to_string(),
            features: option_env!("CARGO_FEATURES").unwrap_or("").to_string(),
        }))
    }

    async fn get_metrics(&self, _req: Request<MetricsReq>) -> Result<Response<MetricsResp>, Status> {
        let metrics_text = metrics::render();
        Ok(Response::new(MetricsResp {
            prometheus_text: metrics_text,
            content_type: "text/plain; version=0.0.4".to_string(),
        }))
    }
}
