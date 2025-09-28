#[cfg(feature = "http-test")]
mod http_integration_tests {
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
                client.get(&format!("{}/v1/health", BASE_URL)).send(),
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
            client.get(&format!("{}/v1/health", BASE_URL)).send(),
        )
        .await
        .expect("Request timed out")
        .expect("Request failed");

        assert_eq!(resp.status(), 200);
        let body: serde_json::Value = resp.json().await.expect("Invalid JSON response");
        assert_eq!(body["status"], "ok");
    }

    #[tokio::test]
    async fn test_put_fast_and_get_fast() {
        if let Err(_) = wait_for_server_ready().await {
            eprintln!("Skipping test - server not available");
            return;
        }
        let client = reqwest::Client::new();
        let test_key = 42u64;
        let test_value: u64 = 4242;

        let put_resp = timeout(
            TEST_TIMEOUT,
            client
                .post(&format!("{}/v1/put_fast/{}", BASE_URL, test_key))
                .header("Content-Type", "application/octet-stream")
                .body(test_value.to_le_bytes().to_vec())
                .send(),
        )
        .await
        .expect("Request timed out")
        .expect("Request failed");

        assert_eq!(put_resp.status(), 200);

        tokio::time::sleep(Duration::from_millis(50)).await;

        let get_resp = timeout(
            TEST_TIMEOUT,
            client
                .get(&format!("{}/v1/get_fast/{}", BASE_URL, test_key))
                .send(),
        )
        .await
        .expect("Request timed out")
        .expect("Request failed");

        assert_eq!(get_resp.status(), 200);
        let bytes = get_resp.bytes().await.expect("Failed to read value bytes");
        assert_eq!(bytes.len(), 8);
        let returned_value = u64::from_le_bytes(bytes[..8].try_into().unwrap());
        assert_eq!(returned_value, test_value);
    }

    #[tokio::test]
    async fn test_lookup_ultra_endpoint() {
        if let Err(_) = wait_for_server_ready().await {
            eprintln!("Skipping test - server not available");
            return;
        }

        let client = reqwest::Client::new();
        let test_key = 99u64;
        let test_value: u64 = 9001;

        let _ = timeout(
            TEST_TIMEOUT,
            client
                .post(&format!("{}/v1/put_fast/{}", BASE_URL, test_key))
                .header("Content-Type", "application/octet-stream")
                .body(test_value.to_le_bytes().to_vec())
                .send(),
        )
        .await
        .expect("Request timed out")
        .expect("Request failed");

        tokio::time::sleep(Duration::from_millis(50)).await;

        let resp = timeout(
            TEST_TIMEOUT,
            client
                .get(&format!("{}/v1/lookup_ultra/{}", BASE_URL, test_key))
                .send(),
        )
        .await
        .expect("Request timed out")
        .expect("Request failed");

        assert_eq!(resp.status(), 200);
        let body: serde_json::Value = resp.json().await.expect("Invalid JSON response");
        assert_eq!(body["key"], test_key);
        assert_eq!(body["value"], serde_json::json!(test_value));
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

        for key in 0..5u64 {
            let value = key + 100;
            let _ = timeout(
                TEST_TIMEOUT,
                client
                    .post(&format!("{}/v1/put_fast/{}", BASE_URL, key))
                    .header("Content-Type", "application/octet-stream")
                    .body(value.to_le_bytes().to_vec())
                    .send(),
            )
            .await
            .expect("Request timed out")
            .expect("Request failed");
        }

        tokio::time::sleep(Duration::from_millis(100)).await;

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
    async fn test_lookup_batch_endpoint() {
        if let Err(_) = wait_for_server_ready().await {
            eprintln!("Skipping test - server not available");
            return;
        }

        let client = reqwest::Client::new();
        let keys = vec![5u64, 6u64, 7u64];
        for key in &keys {
            let value = key + 1234;
            let _ = timeout(
                TEST_TIMEOUT,
                client
                    .post(&format!("{}/v1/put_fast/{}", BASE_URL, key))
                    .header("Content-Type", "application/octet-stream")
                    .body(value.to_le_bytes().to_vec())
                    .send(),
            )
            .await
            .expect("Request timed out")
            .expect("Request failed");
        }

        tokio::time::sleep(Duration::from_millis(50)).await;

        let resp = timeout(
            TEST_TIMEOUT,
            client
                .post(&format!("{}/v1/lookup_batch", BASE_URL))
                .json(&keys)
                .send(),
        )
        .await
        .expect("Request timed out")
        .expect("Request failed");

        assert_eq!(resp.status(), 200);
        let payload: serde_json::Value = resp.json().await.expect("Invalid JSON response");
        assert!(payload.is_array());
        assert_eq!(payload.as_array().unwrap().len(), keys.len());
    }

    #[tokio::test]
    async fn test_warmup_endpoint() {
        if let Err(_) = wait_for_server_ready().await {
            eprintln!("Skipping test - server not available");
            return;
        }

        let client = reqwest::Client::new();
        let resp = timeout(
            TEST_TIMEOUT,
            client.post(&format!("{}/v1/warmup", BASE_URL)).send(),
        )
        .await
        .expect("Request timed out")
        .expect("Request failed");

        assert_eq!(resp.status(), 200);
        let body: serde_json::Value = resp.json().await.expect("Invalid JSON response");
        assert_eq!(body["warmup"], "initiated");
    }

    #[tokio::test]
    async fn test_compact_endpoint() {
        if let Err(_) = wait_for_server_ready().await {
            eprintln!("Skipping test - server not available");
            return;
        }

        let client = reqwest::Client::new();
        let resp = timeout(
            TEST_TIMEOUT,
            client.post(&format!("{}/v1/compact", BASE_URL)).send(),
        )
        .await
        .expect("Request timed out")
        .expect("Request failed");

        assert!(resp.status().is_success());
    }
}
