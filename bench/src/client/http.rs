use super::{BenchClient, ClientConfig};
use anyhow::{Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use reqwest::StatusCode;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::time::{sleep, Duration as TokioDuration};

/// HTTP client for KyroDB benchmarks
#[derive(Clone)]
pub struct HttpClient {
    client: reqwest::Client,
    base_url: String,
    in_flight: Arc<Semaphore>,
    max_retries: usize,
}

impl HttpClient {
    const BASE_BACKOFF_MS: u64 = 5;
    const MAX_BACKOFF_MS: u64 = 250;

    pub fn new(config: ClientConfig) -> Result<Self> {
        let max_in_flight = config.max_in_flight.max(1);
        let client = reqwest::Client::builder()
            // Connection pooling for maximum throughput
            .pool_max_idle_per_host(config.pool_size)
            .pool_idle_timeout(Duration::from_secs(600))
            .connect_timeout(Duration::from_millis(200))
            .timeout(Duration::from_secs(config.timeout_secs))
            // HTTP/2 optimization for multiplexing
            .http2_prior_knowledge()
            .http2_keep_alive_interval(Duration::from_secs(5))
            .http2_keep_alive_timeout(Duration::from_secs(10))
            .http2_keep_alive_while_idle(true)
            .http2_max_frame_size(Some(1048576)) // 1MB frame size
            // TCP optimization
            .tcp_keepalive(Duration::from_secs(600))
            .tcp_nodelay(true) // Disable Nagle's algorithm for low latency
            // TLS optimization
            .danger_accept_invalid_certs(true) // Skip cert validation for benchmarks
            .use_rustls_tls()
            .build()?;

        Ok(Self {
            client,
            base_url: config.base_url,
            in_flight: Arc::new(Semaphore::new(max_in_flight)),
            max_retries: config.max_retries.max(1),
        })
    }

    fn backoff_duration(attempt: usize) -> TokioDuration {
        let capped = attempt.min(8);
        let delay = Self::BASE_BACKOFF_MS.saturating_mul(1u64 << capped);
        TokioDuration::from_millis(delay.min(Self::MAX_BACKOFF_MS))
    }

    fn is_retryable_status(status: StatusCode) -> bool {
        matches!(
            status,
            StatusCode::TOO_MANY_REQUESTS
                | StatusCode::SERVICE_UNAVAILABLE
                | StatusCode::BAD_GATEWAY
                | StatusCode::GATEWAY_TIMEOUT
                | StatusCode::INTERNAL_SERVER_ERROR
        )
    }

    fn is_retryable_error(err: &reqwest::Error) -> bool {
        err.is_timeout() || err.is_connect() || err.is_request()
    }

    async fn send_with_retry<F, G>(
        &self,
        operation: &str,
        mut build_request: F,
        mut accept_status: G,
    ) -> Result<reqwest::Response>
    where
        F: FnMut() -> reqwest::RequestBuilder,
        G: FnMut(StatusCode) -> bool,
    {
        let mut attempt = 0usize;
        loop {
            let permit = self
                .in_flight
                .clone()
                .acquire_owned()
                .await
                .context("HTTP client semaphore closed")?;
            let response_result = build_request().send().await;
            drop(permit);

            match response_result {
                Ok(response) => {
                    let status = response.status();
                    if accept_status(status) {
                        return Ok(response);
                    }

                    if attempt + 1 >= self.max_retries || !Self::is_retryable_status(status) {
                        anyhow::bail!(
                            "{} failed with status {} after {} attempts",
                            operation,
                            status,
                            attempt + 1
                        );
                    }
                }
                Err(err) => {
                    if attempt + 1 >= self.max_retries || !Self::is_retryable_error(&err) {
                        return Err(anyhow::anyhow!(
                            "{} request error after {} attempts: {}",
                            operation,
                            attempt + 1,
                            err
                        ));
                    }
                }
            }

            attempt += 1;
            sleep(Self::backoff_duration(attempt)).await;
        }
    }
}

#[async_trait]
impl BenchClient for HttpClient {
    async fn put(&self, key: u64, value: Vec<u8>) -> Result<()> {
        let url = format!("{}/v1/put_fast/{}", self.base_url, key);
        let payload = Bytes::from(value);

        let response = self
            .send_with_retry(
                "PUT",
                || {
                    let body = payload.clone();
                    self.client
                        .post(&url)
                        .header("Content-Type", "application/octet-stream")
                        .body(body)
                },
                |status| status.is_success(),
            )
            .await?;

        if !response.status().is_success() {
            anyhow::bail!(
                "PUT failed with status: {} for key: {}",
                response.status(),
                key
            );
        }

        Ok(())
    }

    async fn get(&self, key: u64) -> Result<Option<Vec<u8>>> {
        let url = format!("{}/v1/get_fast/{}", self.base_url, key);

        let response = self
            .send_with_retry(
                "GET",
                || self.client.get(&url),
                |status| status.is_success() || status == StatusCode::NOT_FOUND,
            )
            .await?;

        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }

        if !response.status().is_success() {
            anyhow::bail!(
                "GET failed with status: {} for key: {}",
                response.status(),
                key
            );
        }

        let bytes = response.bytes().await?;
        Ok(Some(bytes.to_vec()))
    }

    async fn lookup_batch(&self, keys: Vec<u64>) -> Result<Vec<(u64, Option<u64>)>> {
        let url = format!("{}/v1/lookup_batch", self.base_url);

        let payload = Arc::new(keys);

        let response = self
            .send_with_retry(
                "LOOKUP_BATCH",
                || {
                    let body = payload.clone();
                    self.client.post(&url).json(&*body)
                },
                |status| status.is_success(),
            )
            .await?;

        if !response.status().is_success() {
            anyhow::bail!("Lookup batch failed with status: {}", response.status());
        }

        let results: Vec<(u64, Option<u64>)> = response.json().await?;
        Ok(results)
    }

    async fn build_rmi(&self) -> Result<()> {
        let url = format!("{}/v1/rmi/build", self.base_url);

        let response = self
            .send_with_retry(
                "RMI_BUILD",
                || self.client.post(&url),
                |status| status.is_success(),
            )
            .await?;

        if !response.status().is_success() {
            anyhow::bail!("RMI build failed with status: {}", response.status());
        }

        Ok(())
    }

    async fn warmup(&self) -> Result<()> {
        let url = format!("{}/v1/warmup", self.base_url);

        let response = self
            .send_with_retry(
                "WARMUP",
                || self.client.post(&url),
                |status| status.is_success(),
            )
            .await?;

        if !response.status().is_success() {
            anyhow::bail!("Warmup failed with status: {}", response.status());
        }

        Ok(())
    }

    async fn snapshot(&self) -> Result<()> {
        let url = format!("{}/v1/snapshot", self.base_url);

        let response = self
            .send_with_retry(
                "SNAPSHOT",
                || self.client.post(&url),
                |status| status.is_success(),
            )
            .await?;

        if !response.status().is_success() {
            anyhow::bail!("Snapshot failed with status: {}", response.status());
        }

        Ok(())
    }

    async fn health(&self) -> Result<bool> {
        let url = format!("{}/v1/health", self.base_url);
        match self
            .send_with_retry(
                "HEALTH",
                || self.client.get(&url),
                |status| status.is_success(),
            )
            .await
        {
            Ok(response) => Ok(response.status().is_success()),
            Err(_) => Ok(false),
        }
    }
}
