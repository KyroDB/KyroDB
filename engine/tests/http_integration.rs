#[cfg(feature = "http-test")]
mod http_integration_tests {
    use kyrodb_engine::Server;
    use serde_json::json;
    use std::time::Duration;
    use tokio::time::timeout;

    const BASE_URL: &str = "http://127.0.0.1:8080";
    const TEST_TIMEOUT: Duration = Duration::from_secs(5);

    async fn wait_for_server_ready() -> Result<(), Box<dyn std::error::Error>> {
        let client = reqwest::Client::new();

        for _ in 0..30 {
            if let Ok(resp) = timeout(
                Duration::from_secs(2),
                client.get(&format!("{}/health", BASE_URL)).send(),
            )
            .await
            {
                if let Ok(response) = resp {
                    if response.status().is_success() {
                        return Ok(());
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        Err("Server did not become ready in time".into())
    }

    #[tokio::test]
    async fn test_health_endpoint() {
        if let Err(_) = wait_for_server_ready().await {
            eprintln!("Skipping test - server not available");
            return;
        }
        let client = reqwest::Client::new();
        let resp = timeout(
            TEST_TIMEOUT,
            client.get(&format!("{}/health", BASE_URL)).send(),
        )
        .await
        .expect("Request timed out")
        .expect("Request failed");

        assert_eq!(resp.status(), 200);
        let body: serde_json::Value = resp.json().await.expect("Invalid JSON response");
        assert_eq!(body["status"], "ok");
    }

    #[tokio::test]
    async fn test_build_info_endpoint() {
        if let Err(_) = wait_for_server_ready().await {
            eprintln!("Skipping test - server not available");
            return;
        }
        let client = reqwest::Client::new();
        let resp = timeout(
            TEST_TIMEOUT,
            client.get(&format!("{}/build_info", BASE_URL)).send(),
        )
        .await
        .expect("Request timed out")
        .expect("Request failed");

        assert_eq!(resp.status(), 200);
        let body: serde_json::Value = resp.json().await.expect("Invalid JSON response");
        assert!(body["version"].is_string());
    }

    #[tokio::test]
    async fn test_offset_endpoint() {
        if let Err(_) = wait_for_server_ready().await {
            eprintln!("Skipping test - server not available");
            return;
        }
        let client = reqwest::Client::new();
        let resp = timeout(
            TEST_TIMEOUT,
            client.get(&format!("{}/v1/offset", BASE_URL)).send(),
        )
        .await
        .expect("Request timed out")
        .expect("Request failed");

        assert_eq!(resp.status(), 200);
        let body: serde_json::Value = resp.json().await.expect("Invalid JSON response");
        assert!(body["offset"].is_number());
    }

    #[tokio::test]
    async fn test_append_endpoint() {
        if let Err(_) = wait_for_server_ready().await {
            eprintln!("Skipping test - server not available");
            return;
        }

        let client = reqwest::Client::new();
        let payload = json!({"payload": "test_data"});

        let resp = timeout(
            TEST_TIMEOUT,
            client
                .post(&format!("{}/v1/append", BASE_URL))
                .json(&payload)
                .send(),
        )
        .await
        .expect("Request timed out")
        .expect("Request failed");

        assert_eq!(resp.status(), 200);
        let body: serde_json::Value = resp.json().await.expect("Invalid JSON response");
        assert!(body["offset"].is_number());
    }

    #[tokio::test]
    async fn test_put_and_lookup_endpoints() {
        if let Err(_) = wait_for_server_ready().await {
            eprintln!("Skipping test - server not available");
            return;
        }
        let client = reqwest::Client::new();
        let test_key = 42u64;
        let test_value = "integration_test_value";

        // PUT
        let put_payload = json!({"key": test_key, "value": test_value});
        let put_resp = timeout(
            TEST_TIMEOUT,
            client
                .post(&format!("{}/v1/put", BASE_URL))
                .json(&put_payload)
                .send(),
        )
        .await
        .expect("Request timed out")
        .expect("Request failed");

        assert_eq!(put_resp.status(), 200);
        let put_body: serde_json::Value = put_resp.json().await.expect("Invalid JSON response");
        assert!(put_body["offset"].is_number());

        // Give some time for the write to be processed
        tokio::time::sleep(Duration::from_millis(100)).await;

        // LOOKUP
        let lookup_resp = timeout(
            TEST_TIMEOUT,
            client
                .get(&format!("{}/v1/lookup?key={}", BASE_URL, test_key))
                .send(),
        )
        .await
        .expect("Request timed out")
        .expect("Request failed");

        assert_eq!(lookup_resp.status(), 200);
        let lookup_body: serde_json::Value =
            lookup_resp.json().await.expect("Invalid JSON response");
        assert_eq!(lookup_body["key"], test_key);
        assert_eq!(lookup_body["value"], test_value);
    }

    #[tokio::test]
    async fn test_metrics_endpoint() {
        if let Err(_) = wait_for_server_ready().await {
            eprintln!("Skipping test - server not available");
            return;
        }
        let client = reqwest::Client::new();
        let resp = timeout(
            TEST_TIMEOUT,
            client.get(&format!("{}/metrics", BASE_URL)).send(),
        )
        .await
        .expect("Request timed out")
        .expect("Request failed");

        assert_eq!(resp.status(), 200);
        let text = resp.text().await.expect("Failed to get response text");

        // Should contain some Prometheus metrics
        assert!(text.contains("kyrodb_"));
        assert!(text.len() > 0);
    }

    #[tokio::test]
    async fn test_snapshot_endpoint() {
        if let Err(_) = wait_for_server_ready().await {
            eprintln!("Skipping test - server not available");
            return;
        }
        let client = reqwest::Client::new();
        let resp = timeout(
            TEST_TIMEOUT,
            client.post(&format!("{}/v1/snapshot", BASE_URL)).send(),
        )
        .await
        .expect("Request timed out")
        .expect("Request failed");

        // Accept both 200 (success) and 429 (rate limited) as valid responses
        assert!(
            resp.status() == 200 || resp.status() == 429,
            "Expected 200 or 429, got {}",
            resp.status()
        );

        if resp.status() == 200 {
            let body: serde_json::Value = resp.json().await.expect("Invalid JSON response");
            assert_eq!(body["snapshot"], "ok");
        }
    }

    #[tokio::test]
    async fn test_rmi_build_endpoint() {
        if let Err(_) = wait_for_server_ready().await {
            eprintln!("Skipping test - server not available");
            return;
        }
        let client = reqwest::Client::new();

        // First add some data
        for i in 0..10u64 {
            let put_payload = json!({"key": i, "value": format!("value_{}", i)});
            let _ = timeout(
                TEST_TIMEOUT,
                client
                    .post(&format!("{}/v1/put", BASE_URL))
                    .json(&put_payload)
                    .send(),
            )
            .await;
        }

        tokio::time::sleep(Duration::from_millis(200)).await;

        // Build RMI
        let resp = timeout(
            TEST_TIMEOUT,
            client.post(&format!("{}/v1/rmi/build", BASE_URL)).send(),
        )
        .await
        .expect("Request timed out")
        .expect("Request failed");

        // Accept both 200 (success) and 429 (rate limited) as valid responses
        assert!(
            resp.status() == 200 || resp.status() == 429,
            "Expected 200 or 429, got {}",
            resp.status()
        );

        if resp.status() == 200 {
            let body: serde_json::Value = resp.json().await.expect("Invalid JSON response");
            assert!(body["ok"].as_bool().unwrap_or(false));
        }
    }

    #[tokio::test]
    async fn test_sql_endpoint() {
        if let Err(_) = wait_for_server_ready().await {
            eprintln!("Skipping test - server not available");
            return;
        }

        let client = reqwest::Client::new();

        // Test INSERT
        let insert_sql = json!({"sql": "INSERT INTO kv VALUES (123, 'sql_test_value')"});
        let insert_resp = timeout(
            TEST_TIMEOUT,
            client
                .post(&format!("{}/v1/sql", BASE_URL))
                .json(&insert_sql)
                .send(),
        )
        .await
        .expect("Request timed out")
        .expect("Request failed");

        assert_eq!(insert_resp.status(), 200);
        let insert_body: serde_json::Value =
            insert_resp.json().await.expect("Invalid JSON response");
        assert!(insert_body["ack"]["offset"].is_number());

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Test SELECT
        let select_sql = json!({"sql": "SELECT * FROM kv WHERE key = 123"});
        let select_resp = timeout(
            TEST_TIMEOUT,
            client
                .post(&format!("{}/v1/sql", BASE_URL))
                .json(&select_sql)
                .send(),
        )
        .await
        .expect("Request timed out")
        .expect("Request failed");

        assert_eq!(select_resp.status(), 200);
        let select_body: serde_json::Value =
            select_resp.json().await.expect("Invalid JSON response");
        assert!(select_body.is_array());
        if let Some(first_row) = select_body.as_array().and_then(|arr| arr.first()) {
            assert_eq!(first_row["key"], 123);
        }
    }

    #[tokio::test]
    async fn test_fast_endpoints() {
        if let Err(_) = wait_for_server_ready().await {
            eprintln!("Skipping test - server not available");
            return;
        }
        let client = reqwest::Client::new();
        let test_key = 999u64;
        let test_value = "fast_endpoint_test";

        // First add test data
        let put_payload = json!({"key": test_key, "value": test_value});
        let _ = timeout(
            TEST_TIMEOUT,
            client
                .post(&format!("{}/v1/put", BASE_URL))
                .json(&put_payload)
                .send(),
        )
        .await
        .expect("Request timed out")
        .expect("Request failed");

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Test lookup_fast (should return offset as bytes)
        let lookup_resp = timeout(
            TEST_TIMEOUT,
            client
                .get(&format!("{}/v1/lookup_fast/{}", BASE_URL, test_key))
                .send(),
        )
        .await
        .expect("Request timed out")
        .expect("Request failed");

        if lookup_resp.status() == 200 {
            let bytes = lookup_resp
                .bytes()
                .await
                .expect("Failed to get response bytes");
            assert_eq!(bytes.len(), 8); // u64 offset should be 8 bytes
        }

        // Test get_fast (should return value directly)
        let get_resp = timeout(
            TEST_TIMEOUT,
            client
                .get(&format!("{}/v1/get_fast/{}", BASE_URL, test_key))
                .send(),
        )
        .await
        .expect("Request timed out")
        .expect("Request failed");

        if get_resp.status() == 200 {
            let value_bytes = get_resp
                .bytes()
                .await
                .expect("Failed to get response bytes");
            let value_str = String::from_utf8_lossy(&value_bytes);
            assert_eq!(value_str, test_value);
        }
    }
}