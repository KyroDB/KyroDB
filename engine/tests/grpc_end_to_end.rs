use anyhow::{anyhow, Context, Result};
use std::collections::HashMap;
use std::io::ErrorKind;
use std::net::TcpListener;
use std::path::PathBuf;
use std::time::Duration;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::time::{sleep, Instant};
use tonic::Request;

pub mod kyrodb {
    tonic::include_proto!("kyrodb.v1");
}

use kyrodb::kyro_db_service_client::KyroDbServiceClient;
use kyrodb::{FlushRequest, InsertRequest, QueryRequest, SearchRequest};

type GrpcClient = KyroDbServiceClient<tonic::transport::Channel>;

const TENANT_A_API_KEY: &str = "kyro_tenant_a_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const TENANT_B_API_KEY: &str = "kyro_tenant_b_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

fn normalize(mut v: Vec<f32>) -> Vec<f32> {
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        v.iter_mut().for_each(|x| *x /= norm);
    }
    v
}

async fn wait_for_server(endpoint: &str, timeout: Duration) -> Result<GrpcClient> {
    let deadline = Instant::now() + timeout;
    loop {
        match KyroDbServiceClient::connect(endpoint.to_string()).await {
            Ok(client) => return Ok(client),
            Err(err) => {
                if Instant::now() >= deadline {
                    return Err(anyhow!(
                        "failed to connect to server within {:?}: {err}",
                        timeout
                    ));
                }
                sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

async fn fetch_http_body(port: u16, path: &str) -> Result<String> {
    let addr = format!("127.0.0.1:{port}");
    let mut stream = tokio::net::TcpStream::connect(&addr)
        .await
        .with_context(|| format!("failed to connect to HTTP endpoint {addr}"))?;
    let request = format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .await
        .context("failed to write HTTP request")?;

    let mut buf = Vec::new();
    stream
        .read_to_end(&mut buf)
        .await
        .context("failed to read HTTP response")?;

    let response = String::from_utf8(buf).context("HTTP response was not UTF-8")?;
    let (_, body) = response
        .split_once("\r\n\r\n")
        .context("invalid HTTP response (missing header/body separator)")?;
    Ok(body.to_string())
}

fn extract_prometheus_metric(body: &str, name: &str) -> Option<f64> {
    for line in body.lines() {
        if line.starts_with('#') {
            continue;
        }
        let mut parts = line.split_whitespace();
        let Some(metric_name) = parts.next() else {
            continue;
        };
        if metric_name != name {
            continue;
        }
        let Some(raw_value) = parts.next() else {
            continue;
        };
        let value = raw_value.parse::<f64>().ok()?;
        return Some(value);
    }
    None
}

async fn wait_for_metric_at_least(
    http_port: u16,
    metric_name: &str,
    minimum: f64,
    timeout: Duration,
) -> Result<f64> {
    let deadline = Instant::now() + timeout;
    loop {
        let body = fetch_http_body(http_port, "/metrics").await?;
        if let Some(value) = extract_prometheus_metric(&body, metric_name) {
            if value >= minimum {
                return Ok(value);
            }
        }

        if Instant::now() >= deadline {
            return Err(anyhow!(
                "metric {metric_name} did not reach {minimum} within {:?}",
                timeout
            ));
        }
        sleep(Duration::from_millis(100)).await;
    }
}

fn reserve_port() -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0").context("failed to bind ephemeral port")?;
    let port = listener
        .local_addr()
        .context("failed to read bound address")?
        .port();
    drop(listener);
    Ok(port)
}

fn server_binary() -> Result<PathBuf> {
    if let Ok(path) = std::env::var("CARGO_BIN_EXE_kyrodb_server") {
        return Ok(PathBuf::from(path));
    }

    let current =
        std::env::current_exe().context("failed to detect current test executable path")?;
    let candidate = current
        .parent()
        .and_then(|p| p.parent())
        .map(|dir| dir.join(binary_name()));

    let path = candidate.context("failed to derive kyrodb_server path from current executable")?;

    if path.exists() {
        Ok(path)
    } else {
        Err(anyhow!(
            "kyrodb_server binary not found at {}",
            path.display()
        ))
    }
}

fn binary_name() -> String {
    let base = "kyrodb_server";
    #[cfg(windows)]
    {
        format!("{}.exe", base)
    }
    #[cfg(not(windows))]
    {
        base.to_string()
    }
}

fn create_data_dir(temp_dir: &TempDir) -> Result<PathBuf> {
    let data_dir = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_dir).context("failed to create data directory")?;
    Ok(data_dir)
}

fn write_api_keys_file(temp_dir: &TempDir) -> Result<PathBuf> {
    let api_keys_path = temp_dir.path().join("api_keys.yaml");
    let yaml = format!(
        r#"api_keys:
  - key: {tenant_a_key}
    tenant_id: tenant_a
    tenant_name: Tenant A
    max_qps: 1000
    max_vectors: 10000
    enabled: true
    created_at: "2025-01-01T00:00:00Z"
  - key: {tenant_b_key}
    tenant_id: tenant_b
    tenant_name: Tenant B
    max_qps: 1000
    max_vectors: 10000
    enabled: true
    created_at: "2025-01-01T00:00:00Z"
"#,
        tenant_a_key = TENANT_A_API_KEY,
        tenant_b_key = TENANT_B_API_KEY
    );
    std::fs::write(&api_keys_path, yaml).context("failed to write api_keys.yaml")?;
    Ok(api_keys_path)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn end_to_end_insert_query_search() -> Result<()> {
    if std::env::var("KYRODB_ENABLE_NET_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping grpc end-to-end test (set KYRODB_ENABLE_NET_TESTS=1 to enable)");
        return Ok(());
    }

    let binary = server_binary()?;
    let temp_dir = tempfile::tempdir().context("failed to create temp directory")?;
    let data_dir = create_data_dir(&temp_dir)?;
    let port = reserve_port()?;
    let http_port = reserve_port()?;
    let endpoint = format!("http://127.0.0.1:{port}");

    let mut child = Command::new(binary)
        .env("KYRODB_DATA_DIR", &data_dir)
        .env("KYRODB_PORT", port.to_string())
        // KyroDbConfig reads environment variables via the `config` crate using the
        // `KYRODB__...` prefix with `__` as a separator.
        .env("KYRODB__SERVER__HTTP_PORT", http_port.to_string())
        .kill_on_drop(true)
        .spawn()
        .context("failed to spawn kyrodb_server")?;

    let mut client = wait_for_server(&endpoint, Duration::from_secs(10)).await?;

    let doc_id = 42u64;
    // Use 768 dimensions to match server default config
    let embedding: Vec<f32> = normalize((0..768).map(|i| i as f32 * 0.001).collect());

    let insert_request = InsertRequest {
        doc_id,
        embedding: embedding.clone(),
        metadata: HashMap::new(),
        namespace: String::new(),
    };

    let insert_response = client
        .insert(Request::new(insert_request))
        .await
        .context("insert RPC failed")?;
    assert!(insert_response.get_ref().success, "insert should succeed");

    let flush_response = client
        .flush_hot_tier(Request::new(FlushRequest { force: true }))
        .await
        .context("flush_hot_tier RPC failed")?;
    assert!(
        flush_response.get_ref().success,
        "forced flush should succeed"
    );

    let query_request = QueryRequest {
        doc_id,
        include_embedding: true,
        namespace: String::new(),
    };

    let query_response = client
        .query(Request::new(query_request))
        .await
        .context("query RPC failed")?;
    let query_body = query_response.get_ref();
    assert!(
        query_body.found,
        "inserted document must be returned by query"
    );
    assert_eq!(query_body.embedding.len(), embedding.len());

    let search_request = SearchRequest {
        query_embedding: embedding.clone(),
        k: 1,
        min_score: 0.0,
        namespace: String::new(),
        include_embeddings: false,
        ef_search: 0,
        filter: None,
        metadata_filters: HashMap::new(),
    };

    let search_response = client
        .search(Request::new(search_request))
        .await
        .context("search RPC failed")?;
    let search_body = search_response.get_ref();
    assert!(
        !search_body.results.is_empty(),
        "search should return the inserted vector"
    );
    assert_eq!(
        search_body.results.len() as u32,
        search_body.total_found,
        "total_found should match returned results count"
    );

    if let Err(err) = child.kill().await {
        // INVALID_INPUT is returned when the child process has already exited.
        if err.kind() != ErrorKind::InvalidInput {
            return Err(anyhow!("failed to stop kyrodb_server: {err}"));
        }
    }
    let _ = child.wait().await;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn end_to_end_cross_tenant_search_isolation() -> Result<()> {
    if std::env::var("KYRODB_ENABLE_NET_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping grpc end-to-end test (set KYRODB_ENABLE_NET_TESTS=1 to enable)");
        return Ok(());
    }

    let binary = server_binary()?;
    let temp_dir = tempfile::tempdir().context("failed to create temp directory")?;
    let data_dir = create_data_dir(&temp_dir)?;
    let api_keys_path = write_api_keys_file(&temp_dir)?;
    let port = reserve_port()?;
    let http_port = reserve_port()?;
    let endpoint = format!("http://127.0.0.1:{port}");

    let mut child = Command::new(binary)
        .env("KYRODB_DATA_DIR", &data_dir)
        .env("KYRODB_PORT", port.to_string())
        .env("KYRODB__SERVER__HTTP_PORT", http_port.to_string())
        .env("KYRODB__AUTH__ENABLED", "true")
        .env("KYRODB__AUTH__API_KEYS_FILE", api_keys_path)
        .kill_on_drop(true)
        .spawn()
        .context("failed to spawn kyrodb_server")?;

    let mut client = wait_for_server(&endpoint, Duration::from_secs(10)).await?;

    let mut embedding_a = vec![0.0f32; 768];
    embedding_a[0] = 1.0;
    let embedding_a = normalize(embedding_a);

    let mut embedding_b = vec![0.0f32; 768];
    embedding_b[1] = 1.0;
    let embedding_b = normalize(embedding_b);

    // Insert tenant A doc
    let mut insert_a = Request::new(InsertRequest {
        doc_id: 111,
        embedding: embedding_a.clone(),
        metadata: HashMap::new(),
        namespace: String::new(),
    });
    insert_a
        .metadata_mut()
        .insert("x-api-key", TENANT_A_API_KEY.parse().unwrap());
    client
        .insert(insert_a)
        .await
        .context("insert tenant A failed")?;

    // Insert tenant B doc
    let mut insert_b = Request::new(InsertRequest {
        doc_id: 222,
        embedding: embedding_b.clone(),
        metadata: HashMap::new(),
        namespace: String::new(),
    });
    insert_b
        .metadata_mut()
        .insert("x-api-key", TENANT_B_API_KEY.parse().unwrap());
    client
        .insert(insert_b)
        .await
        .context("insert tenant B failed")?;

    // Flush so both documents are visible in cold-tier search paths.
    let mut flush = Request::new(FlushRequest { force: true });
    flush
        .metadata_mut()
        .insert("x-api-key", TENANT_A_API_KEY.parse().unwrap());
    client
        .flush_hot_tier(flush)
        .await
        .context("flush_hot_tier RPC failed")?;

    // Tenant A searches with tenant B vector: must not see tenant B doc_id.
    let mut search_a = Request::new(SearchRequest {
        query_embedding: embedding_b,
        k: 8,
        min_score: 0.0,
        namespace: String::new(),
        include_embeddings: false,
        ef_search: 0,
        filter: None,
        metadata_filters: HashMap::new(),
    });
    search_a
        .metadata_mut()
        .insert("x-api-key", TENANT_A_API_KEY.parse().unwrap());
    let search_a_resp = client
        .search(search_a)
        .await
        .context("search tenant A failed")?;
    let search_a_body = search_a_resp.get_ref();
    assert!(
        search_a_body.results.iter().all(|r| r.doc_id != 222),
        "tenant A search leaked tenant B doc_id"
    );

    // Tenant B searches with tenant A vector: must not see tenant A doc_id.
    let mut search_b = Request::new(SearchRequest {
        query_embedding: embedding_a,
        k: 8,
        min_score: 0.0,
        namespace: String::new(),
        include_embeddings: false,
        ef_search: 0,
        filter: None,
        metadata_filters: HashMap::new(),
    });
    search_b
        .metadata_mut()
        .insert("x-api-key", TENANT_B_API_KEY.parse().unwrap());
    let search_b_resp = client
        .search(search_b)
        .await
        .context("search tenant B failed")?;
    let search_b_body = search_b_resp.get_ref();
    assert!(
        search_b_body.results.iter().all(|r| r.doc_id != 111),
        "tenant B search leaked tenant A doc_id"
    );

    // Cross-tenant direct query must be denied by visibility filtering.
    let mut query_b_from_a = Request::new(QueryRequest {
        doc_id: 222,
        include_embedding: false,
        namespace: String::new(),
    });
    query_b_from_a
        .metadata_mut()
        .insert("x-api-key", TENANT_A_API_KEY.parse().unwrap());
    let query_b_from_a_resp = client
        .query(query_b_from_a)
        .await
        .context("query tenant B doc using tenant A key failed")?;
    assert!(
        !query_b_from_a_resp.get_ref().found,
        "tenant A should not query tenant B document"
    );

    let mut query_a_from_b = Request::new(QueryRequest {
        doc_id: 111,
        include_embedding: false,
        namespace: String::new(),
    });
    query_a_from_b
        .metadata_mut()
        .insert("x-api-key", TENANT_B_API_KEY.parse().unwrap());
    let query_a_from_b_resp = client
        .query(query_a_from_b)
        .await
        .context("query tenant A doc using tenant B key failed")?;
    assert!(
        !query_a_from_b_resp.get_ref().found,
        "tenant B should not query tenant A document"
    );

    if let Err(err) = child.kill().await {
        if err.kind() != ErrorKind::InvalidInput {
            return Err(anyhow!("failed to stop kyrodb_server: {err}"));
        }
    }
    let _ = child.wait().await;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn end_to_end_hsc_training_and_semantic_metrics() -> Result<()> {
    if std::env::var("KYRODB_ENABLE_NET_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping grpc end-to-end test (set KYRODB_ENABLE_NET_TESTS=1 to enable)");
        return Ok(());
    }

    let binary = server_binary()?;
    let temp_dir = tempfile::tempdir().context("failed to create temp directory")?;
    let data_dir = create_data_dir(&temp_dir)?;
    let port = reserve_port()?;
    let http_port = reserve_port()?;
    let endpoint = format!("http://127.0.0.1:{port}");

    let mut child = Command::new(binary)
        .env("KYRODB_DATA_DIR", &data_dir)
        .env("KYRODB_PORT", port.to_string())
        .env("KYRODB__SERVER__HTTP_PORT", http_port.to_string())
        .env("KYRODB__CACHE__TRAINING_INTERVAL_SECS", "1")
        .env("KYRODB__CACHE__TRAINING_WINDOW_SECS", "120")
        .env("KYRODB__CACHE__MIN_TRAINING_SAMPLES", "8")
        .env("KYRODB__CACHE__LOGGER_WINDOW_SIZE", "10000")
        .env("KYRODB__CACHE__CAPACITY", "64")
        .kill_on_drop(true)
        .spawn()
        .context("failed to spawn kyrodb_server")?;

    let result: Result<()> = async {
        let mut client = wait_for_server(&endpoint, Duration::from_secs(10)).await?;

        // Seed documents for point-query traffic.
        for doc_id in 1u64..=24 {
            let mut embedding = vec![0.0f32; 768];
            embedding[(doc_id as usize) % 64] = 1.0;
            embedding[((doc_id as usize) + 7) % 64] = 0.2;
            let embedding = normalize(embedding);

            client
                .insert(Request::new(InsertRequest {
                    doc_id,
                    embedding,
                    metadata: HashMap::new(),
                    namespace: String::new(),
                }))
                .await
                .with_context(|| format!("insert RPC failed for doc_id={doc_id}"))?;
        }

        // Generate real gRPC point-query traffic so access logger feeds retraining.
        for i in 0..120u64 {
            let doc_id = (i % 24) + 1;
            let response = client
                .query(Request::new(QueryRequest {
                    doc_id,
                    include_embedding: false,
                    namespace: String::new(),
                }))
                .await
                .with_context(|| format!("query RPC failed for doc_id={doc_id}"))?;
            assert!(
                response.get_ref().found,
                "query should find seeded document {doc_id}"
            );
        }

        wait_for_metric_at_least(
            http_port,
            "kyrodb_training_cycles_completed_total",
            1.0,
            Duration::from_secs(20),
        )
        .await?;
        wait_for_metric_at_least(
            http_port,
            "kyrodb_hsc_predictor_trained",
            1.0,
            Duration::from_secs(20),
        )
        .await?;
        wait_for_metric_at_least(
            http_port,
            "kyrodb_hsc_semantic_enabled",
            1.0,
            Duration::from_secs(20),
        )
        .await?;
        wait_for_metric_at_least(
            http_port,
            "kyrodb_hsc_admission_controller_enabled",
            1.0,
            Duration::from_secs(20),
        )
        .await?;

        // Drive post-training admissions on fresh doc_ids so semantic decision path executes.
        for doc_id in 1001u64..=1016 {
            let mut embedding = vec![0.0f32; 768];
            embedding[(doc_id as usize) % 128] = 1.0;
            embedding[((doc_id as usize) + 3) % 128] = 0.1;
            let embedding = normalize(embedding);
            client
                .insert(Request::new(InsertRequest {
                    doc_id,
                    embedding,
                    metadata: HashMap::new(),
                    namespace: String::new(),
                }))
                .await
                .with_context(|| format!("post-training insert failed for doc_id={doc_id}"))?;
        }
        for doc_id in 1001u64..=1016 {
            let response = client
                .query(Request::new(QueryRequest {
                    doc_id,
                    include_embedding: false,
                    namespace: String::new(),
                }))
                .await
                .with_context(|| format!("post-training query failed for doc_id={doc_id}"))?;
            assert!(
                response.get_ref().found,
                "post-training query should find document {doc_id}"
            );
        }

        let semantic_deadline = Instant::now() + Duration::from_secs(15);
        let observed_semantic_decisions = loop {
            let body = fetch_http_body(http_port, "/metrics").await?;
            let fast = extract_prometheus_metric(
                &body,
                "kyrodb_hsc_semantic_fast_path_decisions_total",
            )
            .unwrap_or(0.0);
            let slow = extract_prometheus_metric(
                &body,
                "kyrodb_hsc_semantic_slow_path_decisions_total",
            )
            .unwrap_or(0.0);
            let observed_semantic_decisions = fast + slow;
            if observed_semantic_decisions >= 1.0 {
                break observed_semantic_decisions;
            }
            if Instant::now() >= semantic_deadline {
                return Err(anyhow!(
                    "semantic decision counters did not advance (fast+slow={observed_semantic_decisions})"
                ));
            }
            sleep(Duration::from_millis(100)).await;
        };
        assert!(
            observed_semantic_decisions >= 1.0,
            "expected semantic decision counters to increase after post-training traffic"
        );

        Ok(())
    }
    .await;

    if let Err(err) = child.kill().await {
        if err.kind() != ErrorKind::InvalidInput {
            return Err(anyhow!("failed to stop kyrodb_server: {err}"));
        }
    }
    let _ = child.wait().await;

    result
}
