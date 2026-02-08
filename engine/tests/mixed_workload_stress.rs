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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mixed_bulk_search_and_writes_complete_without_deadlock() {
    let temp_dir = TempDir::new().unwrap();
    let dim = 32usize;

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

    for doc_id in 0..3000u64 {
        let mut metadata = HashMap::new();
        metadata.insert("seed".to_string(), doc_id.to_string());
        engine
            .insert(doc_id, embedding_for(doc_id, dim), metadata)
            .unwrap();
    }

    let engine = Arc::new(RwLock::new(engine));

    let search_engine = Arc::clone(&engine);
    let search_task = tokio::spawn(async move {
        for round in 0..150usize {
            let mut queries = Vec::with_capacity(32);
            for q in 0..32usize {
                let id = ((round * 32 + q) % 3000) as u64;
                queries.push(embedding_for(id, dim));
            }

            let guard = search_engine.read().await;
            let results = guard
                .knn_search_batch_with_ef_detailed_scoped(
                    &queries,
                    10,
                    Some(128),
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
        for i in 0..400u64 {
            let mut metadata = HashMap::new();
            metadata.insert("writer".to_string(), "true".to_string());
            metadata.insert("seq".to_string(), i.to_string());

            let guard = tokio::time::timeout(Duration::from_secs(2), write_engine.write())
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

    tokio::time::timeout(Duration::from_secs(90), async {
        search_task.await.unwrap();
        write_task.await.unwrap();
    })
    .await
    .expect("mixed workload tasks timed out");
}
