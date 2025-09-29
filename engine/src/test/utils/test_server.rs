//! Test Server Helpers
//!
//! Utilities for testing without HTTP server (direct API testing)

use crate::PersistentEventLog;
use std::path::PathBuf;
use std::sync::Arc;

/// Test server configuration
#[derive(Clone, Debug)]
pub struct TestServerConfig {
    pub data_dir: PathBuf,
    pub enable_rmi: bool,
    pub group_commit_ms: u64,
    pub snapshot_interval_ops: usize,
}

impl Default for TestServerConfig {
    fn default() -> Self {
        Self {
            data_dir: std::env::temp_dir().join(format!("kyrodb_test_{}", rand::random::<u64>())),
            enable_rmi: true,
            group_commit_ms: 10,
            snapshot_interval_ops: 10000,
        }
    }
}

/// Test database instance (no HTTP server needed)
pub struct TestServer {
    pub log: Arc<PersistentEventLog>,
    pub data_dir: PathBuf,
}

impl TestServer {
    /// Start a test database instance
    pub async fn start(config: TestServerConfig) -> anyhow::Result<Self> {
        // Create data directory
        std::fs::create_dir_all(&config.data_dir)?;

        // Configure via environment variables
        if config.group_commit_ms > 0 {
            std::env::set_var(
                "KYRODB_GROUP_COMMIT_DELAY_MICROS",
                (config.group_commit_ms * 1000).to_string(),
            );
            std::env::set_var("KYRODB_GROUP_COMMIT_ENABLED", "1");
        } else {
            std::env::set_var("KYRODB_GROUP_COMMIT_ENABLED", "0");
        }

        // Create database instance using open()
        let log = Arc::new(PersistentEventLog::open(config.data_dir.clone()).await?);

        Ok(Self {
            log,
            data_dir: config.data_dir,
        })
    }

    /// Start with default configuration
    pub async fn start_default() -> anyhow::Result<Self> {
        // Use synchronous durability for reliable testing
        std::env::set_var("KYRODB_DURABILITY_LEVEL", "enterprise_safe");
        std::env::set_var("KYRODB_GROUP_COMMIT_ENABLED", "0");
        
        // ✅ FORCE LEARNED INDEX: Test the actual system we built
        #[cfg(feature = "learned-index")]
        std::env::set_var("KYRODB_USE_ADAPTIVE_RMI", "true");
        
        #[cfg(not(feature = "learned-index"))]
        std::env::set_var("KYRODB_USE_ADAPTIVE_RMI", "false");
        
        let data_dir = super::temp_data_dir("default");
        let log = Arc::new(PersistentEventLog::open(data_dir.clone()).await?);

        // ✅ Build RMI immediately for tests with learned-index feature
        #[cfg(feature = "learned-index")]
        {
            // If there's any existing data, build RMI from it
            // This ensures tests exercise the actual learned index path
            log.build_rmi().await.ok(); // Ignore errors if no data yet
        }

        Ok(Self {
            log,
            data_dir,
        })
    }

    /// Start with specific group commit delay (in microseconds)
    pub async fn start_with_group_commit(delay_micros: u64) -> anyhow::Result<Self> {
        // Use safe durability with group commit
        std::env::set_var("KYRODB_DURABILITY_LEVEL", "enterprise_safe");
        std::env::set_var("KYRODB_GROUP_COMMIT_DELAY_MICROS", delay_micros.to_string());
        std::env::set_var("KYRODB_GROUP_COMMIT_ENABLED", "1");
        
        // ✅ FORCE LEARNED INDEX: Test actual system
        #[cfg(feature = "learned-index")]
        std::env::set_var("KYRODB_USE_ADAPTIVE_RMI", "true");
        
        #[cfg(not(feature = "learned-index"))]
        std::env::set_var("KYRODB_USE_ADAPTIVE_RMI", "false");
        
        let mut config = TestServerConfig::default();
        config.group_commit_ms = delay_micros / 1000; // Convert to ms for internal use
        
        let server = Self::start(config).await?;
        
        // ✅ Build RMI for learned-index feature
        #[cfg(feature = "learned-index")]
        {
            server.log.build_rmi().await.ok();
        }
        
        Ok(server)
    }

    /// Start with async durability mode (background fsync)
    pub async fn start_with_async_durability() -> anyhow::Result<Self> {
        std::env::set_var("KYRODB_DURABILITY_LEVEL", "enterprise_async");
        std::env::set_var("KYRODB_BACKGROUND_FSYNC_INTERVAL_MS", "10");
        
        // ✅ FORCE LEARNED INDEX: Test actual system
        #[cfg(feature = "learned-index")]
        std::env::set_var("KYRODB_USE_ADAPTIVE_RMI", "true");
        
        #[cfg(not(feature = "learned-index"))]
        std::env::set_var("KYRODB_USE_ADAPTIVE_RMI", "false");
        
        let config = TestServerConfig::default();
        let server = Self::start(config).await?;
        
        // ✅ Build RMI for learned-index feature
        #[cfg(feature = "learned-index")]
        {
            server.log.build_rmi().await.ok();
        }
        
        Ok(server)
    }

    /// Cleanup database files
    pub async fn cleanup(self) {
        drop(self.log);
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        std::fs::remove_dir_all(&self.data_dir).ok();
    }

    /// Shutdown database gracefully
    pub async fn shutdown(self) {
        drop(self.log);
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }
}

/// Create a test database with default config
pub async fn create_test_server() -> anyhow::Result<TestServer> {
    let config = TestServerConfig::default();
    TestServer::start(config).await
}

/// Create multiple test databases
pub async fn create_test_cluster(count: usize) -> anyhow::Result<Vec<TestServer>> {
    let mut servers = Vec::with_capacity(count);
    for i in 0..count {
        let mut config = TestServerConfig::default();
        config.data_dir = std::env::temp_dir().join(format!("kyrodb_test_cluster_{}_{}", i, rand::random::<u64>()));
        servers.push(TestServer::start(config).await?);
    }
    Ok(servers)
}
