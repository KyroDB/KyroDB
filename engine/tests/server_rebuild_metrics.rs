#![cfg(feature = "learned-index")]
use std::time::Duration;

#[tokio::test]
async fn server_background_rebuild_emits_metrics() {
    // spawn server on a random free port
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().to_path_buf();
    let port: u16 = 40443; // fixed high port for CI simplicity; adjust if needed

    // start kyrodb-engine with rebuild triggers: after 50 appends
    let mut cmd = tokio::process::Command::new(std::env::current_exe().unwrap());
    // current_exe points to test bin; compute path to kyrodb-engine in target/debug
    // Fallback to `cargo run`-like path by replacing the file name
    let exe = std::env::current_exe().unwrap();
    let kyro = exe.parent().unwrap().parent().unwrap().join("kyrodb-engine");
    let mut child = tokio::process::Command::new(kyro)
        .arg("--data-dir").arg(data_dir.to_str().unwrap())
        .arg("serve").arg("127.0.0.1").arg(port.to_string())
        .arg("--rmi-rebuild-appends").arg("50")
        .arg("--wal-segment-bytes").arg("1048576")
        .kill_on_drop(true)
        .spawn()
        .expect("spawn server");

    // wait a bit for server to come up
    tokio::time::sleep(Duration::from_millis(800)).await;

    // append 60 kv items via HTTP to trigger rebuild
    let client = reqwest::Client::new();
    for i in 0..60u64 {
        let body = serde_json::json!({"key": i, "value": format!("v{}", i)});
        let url = format!("http://127.0.0.1:{}/put", port);
        let res = client.post(url).json(&body).send().await.unwrap();
        assert!(res.status().is_success());
    }

    // poll /metrics until we see rebuilds_total >= 1 and some rebuild duration bucket has count
    let mut saw_rebuild = false;
    let mut attempts = 0u32;
    while attempts < 50 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        attempts += 1;
        let metrics_url = format!("http://127.0.0.1:{}/metrics", port);
        if let Ok(text) = client.get(metrics_url).send().await.unwrap().text().await {
            if text.contains("kyrodb_rmi_rebuilds_total") && text.contains("kyrodb_rmi_rebuild_duration_seconds_bucket") {
                // check count >= 1
                if text.lines().any(|l| l.starts_with("kyrodb_rmi_rebuilds_total ") && l.split_whitespace().last().unwrap_or("0") != "0") {
                    saw_rebuild = true;
                    break;
                }
            }
        }
    }

    // cleanup server
    let _ = child.kill().await;
    assert!(saw_rebuild, "did not observe rebuild metrics in time");
}
