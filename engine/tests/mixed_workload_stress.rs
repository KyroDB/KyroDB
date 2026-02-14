use kyrodb_engine::{
    cache_strategy::LruCacheStrategy, persistence::FsyncPolicy, QueryHashCache, TieredEngine,
    TieredEngineConfig,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::sync::RwLock;

fn embedding_for(seed: u64, dim: usize) -> Vec<f32> {
    let mut out = Vec::with_capacity(dim);
    for i in 0..dim {
        let v = (((seed as usize + 1) * (i + 3)) % 997) as f32 / 997.0 + 0.001;
        out.push(v);
    }
    out
}

struct MixedWorkloadProfile {
    initial_docs: u64,
    search_rounds: usize,
    queries_per_round: usize,
    write_ops: u64,
    ef_search: usize,
    write_lock_timeout: Duration,
    overall_timeout: Duration,
}

fn mixed_workload_profile() -> MixedWorkloadProfile {
    match std::env::var("KYRODB_SANITIZER").as_deref() {
        // Thread sanitizer is significantly slower; keep the workload concurrent but bounded.
        Ok("thread") => MixedWorkloadProfile {
            initial_docs: 1_200,
            search_rounds: 72,
            queries_per_round: 16,
            write_ops: 220,
            ef_search: 96,
            write_lock_timeout: Duration::from_secs(6),
            overall_timeout: Duration::from_secs(360),
        },
        _ => MixedWorkloadProfile {
            initial_docs: 3_000,
            search_rounds: 150,
            queries_per_round: 32,
            write_ops: 400,
            ef_search: 128,
            write_lock_timeout: Duration::from_secs(2),
            overall_timeout: Duration::from_secs(90),
        },
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mixed_bulk_search_and_writes_complete_without_deadlock() {
    let temp_dir = TempDir::new().unwrap();
    let dim = 32usize;
    let profile = mixed_workload_profile();

    let config = TieredEngineConfig {
        data_dir: Some(temp_dir.path().to_string_lossy().to_string()),
        fsync_policy: FsyncPolicy::Never,
        embedding_dimension: dim,
        ..Default::default()
    };

    let engine = TieredEngine::new(
        Box::new(LruCacheStrategy::new(4096)),
        Arc::new(QueryHashCache::new(4096, 0.90)),
        Vec::new(),
        Vec::new(),
        config,
    )
    .unwrap();

    for doc_id in 0..profile.initial_docs {
        let mut metadata = HashMap::new();
        metadata.insert("seed".to_string(), doc_id.to_string());
        engine
            .insert(doc_id, embedding_for(doc_id, dim), metadata)
            .unwrap();
    }

    let engine = Arc::new(RwLock::new(engine));

    let search_engine = Arc::clone(&engine);
    let search_task = tokio::spawn(async move {
        for round in 0..profile.search_rounds {
            let mut queries = Vec::with_capacity(profile.queries_per_round);
            for q in 0..profile.queries_per_round {
                let id = ((round * profile.queries_per_round + q) as u64) % profile.initial_docs;
                queries.push(embedding_for(id, dim));
            }

            let guard = search_engine.read().await;
            let results = guard
                .knn_search_batch_with_ef_detailed_scoped(
                    &queries,
                    10,
                    Some(profile.ef_search),
                    u64::from((round % 13) as u32),
                )
                .unwrap();
            assert_eq!(results.len(), queries.len());
            drop(guard);

            tokio::task::yield_now().await;
        }
    });

    let write_engine = Arc::clone(&engine);
    let write_task = tokio::spawn(async move {
        for i in 0..profile.write_ops {
            let mut metadata = HashMap::new();
            metadata.insert("writer".to_string(), "true".to_string());
            metadata.insert("seq".to_string(), i.to_string());

            let guard = tokio::time::timeout(profile.write_lock_timeout, write_engine.write())
                .await
                .expect("timed out acquiring write lock under mixed workload");
            guard
                .insert(10_000 + i, embedding_for(10_000 + i, dim), metadata)
                .unwrap();
            drop(guard);

            if i % 4 == 0 {
                tokio::task::yield_now().await;
            }
        }
    });

    tokio::time::timeout(profile.overall_timeout, async {
        search_task.await.unwrap();
        write_task.await.unwrap();
    })
    .await
    .expect("mixed workload tasks timed out");
}
