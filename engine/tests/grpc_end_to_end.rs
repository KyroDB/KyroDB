use anyhow::{anyhow, Context, Result};
use std::collections::HashMap;
use std::io::ErrorKind;
use std::net::TcpListener;
use std::path::PathBuf;
use std::time::Duration;
use tempfile::TempDir;
use tokio::process::Command;
use tokio::time::{sleep, Instant};
use tonic::Request;

pub mod kyrodb {
    tonic::include_proto!("kyrodb.v1");
}

use kyrodb::kyro_db_service_client::KyroDbServiceClient;
use kyrodb::{FlushRequest, InsertRequest, QueryRequest, SearchRequest};

type GrpcClient = KyroDbServiceClient<tonic::transport::Channel>;

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn end_to_end_insert_query_search() -> Result<()> {
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
    let embedding: Vec<f32> = (0..768).map(|i| i as f32 * 0.001).collect();

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
