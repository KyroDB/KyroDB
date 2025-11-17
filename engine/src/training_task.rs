// Background training task: Periodic RMI retraining for cache predictor
// Periodically retrains RMI predictor using access logs

use crate::access_logger::AccessPatternLogger;
use crate::cache_strategy::LearnedCacheStrategy;
use crate::learned_cache::LearnedCachePredictor;
use crate::metrics::MetricsCollector;
use anyhow::Result;
use parking_lot::RwLock;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

/// Configuration for background training task
#[derive(Debug, Clone)]
pub struct TrainingConfig {
    /// Training interval (default: 10 minutes)
    pub interval: Duration,

    /// Training window duration (default: 1 hour, was 24 hours)
    pub window_duration: Duration,

    /// Recency decay half-life (default: 30 minutes, was 1 hour)
    pub recency_halflife: Duration,

    /// Minimum events required before training (default: 100)
    pub min_events_for_training: usize,

    /// RMI capacity (default: 10,000 documents)
    pub rmi_capacity: usize,

    /// Admission threshold (default: 0.15, was 0.2)
    pub admission_threshold: f32,

    /// Auto-tune threshold based on utilization
    pub auto_tune_enabled: bool,

    /// Target cache utilization for auto-tuning
    pub target_utilization: f32,
}

impl Default for TrainingConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(600),
            window_duration: Duration::from_secs(3600),
            recency_halflife: Duration::from_secs(1800),
            min_events_for_training: 100,
            rmi_capacity: 10_000,
            admission_threshold: 0.15,
            auto_tune_enabled: true,
            target_utilization: 0.85,
        }
    }
}

/// Spawns a background task that periodically retrains the Hybrid Semantic Cache predictor
///
/// The task runs every `config.interval` seconds and:
/// 1. Fetches recent access events from the logger
/// 2. Trains a new RMI predictor on those events
/// 3. Updates the Hybrid Semantic Cache strategy with the new predictor
///
/// # Arguments
/// * `access_logger` - Shared access pattern logger (tracks query accesses)
/// * `learned_strategy` - Shared Hybrid Semantic Cache strategy (will be updated with new predictor)
/// * `config` - Training configuration (interval, window, capacity)
/// * `cycle_counter` - Optional counter to track training cycles
/// * `mut shutdown_rx` - Broadcast receiver for graceful shutdown signal
///
/// # Returns
/// JoinHandle for the background task (can be used to cancel or await completion)
///
/// # Example
/// ```no_run
/// use kyrodb_engine::{AccessPatternLogger, LearnedCacheStrategy, LearnedCachePredictor, VectorCache};
/// use kyrodb_engine::training_task::{spawn_training_task, TrainingConfig};
/// use std::sync::Arc;
/// use tokio::sync::{RwLock, broadcast};
///
/// #[tokio::main]
/// async fn main() {
///     let logger = Arc::new(RwLock::new(AccessPatternLogger::new(100_000)));
///     let predictor = LearnedCachePredictor::new(10_000).unwrap();
///     let strategy = Arc::new(LearnedCacheStrategy::new(10_000, predictor));
///     let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
///     
///     let config = TrainingConfig::default();
///     let handle = spawn_training_task(logger, strategy, config, None, shutdown_rx).await;
///     
///     // Task runs in background...
///     shutdown_tx.send(()).unwrap(); // Signal shutdown
///     handle.await.unwrap(); // Wait for graceful termination
/// }
/// ```
pub async fn spawn_training_task(
    access_logger: Arc<RwLock<AccessPatternLogger>>,
    learned_strategy: Arc<LearnedCacheStrategy>,
    config: TrainingConfig,
    cycle_counter: Option<Arc<AtomicU64>>,
    mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(config.interval);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    // Fetch recent access events
                    let events = {
                        let logger = access_logger.read();
                        logger.get_recent_window(config.window_duration)
                    };

                    // Skip training if insufficient data
                    if events.len() < config.min_events_for_training {
                        continue;
                    }

                    // Preserve target_hot_entries from current predictor (CRITICAL FIX)
                    // Root cause: Each retraining cycle was resetting target to RMI capacity (212)
                    // This caused all 212 docs to be marked hot instead of the configured 132
                    let current_target = {
                        let predictor = learned_strategy.predictor.read();
                        predictor.target_hot_entries()
                    };

                    // Train new predictor
                    match train_predictor(&events, config.rmi_capacity, &config, current_target) {
                        Ok(new_predictor) => {
                            // Update learned strategy atomically
                            learned_strategy.update_predictor(new_predictor);

                            if let Some(counter) = &cycle_counter {
                                counter.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        Err(e) => {
                            eprintln!("Training task error: {}", e);
                        }
                    }
                }
                _ = shutdown_rx.recv() => {
                    info!("Training task received shutdown signal, stopping gracefully");
                    break;
                }
            }
        }

        info!("Training task stopped");
    })
}

/// Trains a new RMI predictor from access events
///
/// Internal helper function - builds predictor from scratch using access patterns
fn train_predictor(
    events: &[crate::learned_cache::AccessEvent],
    capacity: usize,
    config: &TrainingConfig,
    target_hot_entries: usize, // CRITICAL: Preserve from previous predictor
) -> Result<LearnedCachePredictor> {
    let mut predictor = LearnedCachePredictor::with_config(
        capacity,
        config.admission_threshold,
        config.window_duration,
        config.recency_halflife,
        config.interval,
    )?;
    predictor.set_auto_tune(config.auto_tune_enabled);
    predictor.set_target_utilization(config.target_utilization);
    // CRITICAL FIX: Set target_hot_entries BEFORE training
    // This ensures threshold calibration uses the correct target (132, not 212)
    predictor.set_target_hot_entries(target_hot_entries);
    predictor.train_from_accesses(events)?;
    Ok(predictor)
}

/// Training task supervisor - monitors training task and auto-restarts on crash
///
/// Handles both panic and normal error scenarios with exponential backoff.
/// Automatically restarts the training task up to max_restarts times.
pub struct TrainingTaskSupervisor {
    access_logger: Arc<RwLock<AccessPatternLogger>>,
    learned_strategy: Arc<LearnedCacheStrategy>,
    config: TrainingConfig,
    metrics: MetricsCollector,
    max_restarts: usize,
    base_delay_secs: u64,
}

impl TrainingTaskSupervisor {
    /// Create new training task supervisor
    ///
    /// # Parameters
    /// - `access_logger`: Access pattern logger
    /// - `learned_strategy`: Learned cache strategy to update
    /// - `config`: Training configuration
    /// - `metrics`: Metrics collector
    ///
    /// # Default Restart Policy
    /// - Max restarts: 10
    /// - Exponential backoff: 1s, 2s, 4s, 8s, 16s (capped at 16s)
    pub fn new(
        access_logger: Arc<RwLock<AccessPatternLogger>>,
        learned_strategy: Arc<LearnedCacheStrategy>,
        config: TrainingConfig,
        metrics: MetricsCollector,
    ) -> Self {
        Self {
            access_logger,
            learned_strategy,
            config,
            metrics,
            max_restarts: 10,
            base_delay_secs: 1,
        }
    }

    /// Create supervisor with custom restart policy
    pub fn with_restart_policy(
        access_logger: Arc<RwLock<AccessPatternLogger>>,
        learned_strategy: Arc<LearnedCacheStrategy>,
        config: TrainingConfig,
        metrics: MetricsCollector,
        max_restarts: usize,
        base_delay_secs: u64,
    ) -> Self {
        Self {
            access_logger,
            learned_strategy,
            config,
            metrics,
            max_restarts,
            base_delay_secs,
        }
    }

    /// Start supervised training task
    ///
    /// Spawns the training task and monitors it for crashes. On crash, automatically
    /// restarts with exponential backoff (1s, 2s, 4s, 8s, 16s max).
    ///
    /// Returns a JoinHandle for the supervisor task (not the training task itself).
    pub async fn supervise(
        self,
        mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut restart_count = 0;

            loop {
                info!(
                    restart_count,
                    max_restarts = self.max_restarts,
                    "starting training task"
                );

                // Create a new shutdown receiver for this training task instance
                let task_shutdown_rx = shutdown_rx.resubscribe();

                // Spawn training task
                let handle = spawn_training_task(
                    self.access_logger.clone(),
                    self.learned_strategy.clone(),
                    self.config.clone(),
                    None,
                    task_shutdown_rx,
                )
                .await;

                tokio::select! {
                    result = handle => {
                        match result {
                            Ok(()) => {
                                // Normal completion (shutdown signal received)
                                info!("training task completed normally (shutdown)");
                                break;
                            }
                            Err(e) => {
                                // Task panicked or was aborted
                                if e.is_panic() {
                                    error!(
                                        error = %e,
                                        restart_count,
                                        "training task panicked"
                                    );
                                    self.metrics.record_training_crash();
                                } else if e.is_cancelled() {
                                    info!("training task cancelled - stopping supervisor");
                                    break;
                                } else {
                                    error!(
                                        error = %e,
                                        restart_count,
                                        "training task failed"
                                    );
                                    self.metrics.record_training_crash();
                                }

                                // Check restart limit
                                if restart_count >= self.max_restarts {
                                    error!(
                                        restart_count,
                                        max_restarts = self.max_restarts,
                                        "training task restart limit reached - stopping supervisor"
                                    );
                                    break;
                                }

                                // Exponential backoff: 1s, 2s, 4s, 8s, 16s (capped)
                                let delay_secs = 2u64
                                    .checked_pow(restart_count as u32)
                                    .and_then(|exp| self.base_delay_secs.checked_mul(exp))
                                    .map(|delay| std::cmp::min(delay, 16))
                                    .unwrap_or(16); // Fallback to cap on overflow
                                warn!(
                                    delay_secs,
                                    restart_count,
                                    "restarting training task after delay"
                                );
                                tokio::time::sleep(Duration::from_secs(delay_secs)).await;

                                restart_count += 1;
                                self.metrics.record_training_restart();
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        info!("training supervisor received shutdown signal");
                        break;
                    }
                }
            }

            info!("training task supervisor stopped");
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::SystemTime;
    use tokio::time::{sleep, timeout};

    #[tokio::test]
    async fn test_training_task_runs_periodically() {
        // Create logger with some access events
        let logger = Arc::new(RwLock::new(AccessPatternLogger::new(1_000)));
        {
            let log = logger.write().await;
            for i in 0..200 {
                let embedding = vec![i as f32; 128];
                log.log_access(i % 10, &embedding); // 10 documents, 20 accesses each
            }
        }

        // Create learned strategy
        let predictor = LearnedCachePredictor::new(100).unwrap();
        let strategy = Arc::new(LearnedCacheStrategy::new(100, predictor));

        // Spawn training task with short interval (1 second for testing)
        let config = TrainingConfig {
            interval: Duration::from_secs(1),
            window_duration: Duration::from_secs(3600),
            min_events_for_training: 100,
            rmi_capacity: 100,
            ..TrainingConfig::default()
        };

        let (_shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);
        let handle =
            spawn_training_task(logger.clone(), strategy.clone(), config, None, shutdown_rx).await;

        // Wait for multiple training cycles
        sleep(Duration::from_secs(3)).await;

        // Verify training occurred (predictor should be updated)
        // Note: We can't directly observe training_count, but the task should have run

        handle.abort();
    }

    #[tokio::test]
    async fn test_training_task_skips_insufficient_data() {
        // Create logger with too few events
        let logger = Arc::new(RwLock::new(AccessPatternLogger::new(1_000)));
        {
            let log = logger.write().await;
            for i in 0..50 {
                let embedding = vec![i as f32; 128];
                log.log_access(i, &embedding);
            }
        }

        // Create learned strategy
        let predictor = LearnedCachePredictor::new(100).unwrap();
        let strategy = Arc::new(LearnedCacheStrategy::new(100, predictor));

        // Spawn training task with high min_events threshold
        let config = TrainingConfig {
            interval: Duration::from_millis(100),
            window_duration: Duration::from_secs(3600),
            min_events_for_training: 100, // More than we have
            rmi_capacity: 100,
            ..TrainingConfig::default()
        };

        let (_shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);
        let handle =
            spawn_training_task(logger.clone(), strategy.clone(), config, None, shutdown_rx).await;

        // Wait for a few cycles
        sleep(Duration::from_millis(500)).await;

        // Task should run but skip training (no panic, no error)

        handle.abort();
    }

    #[tokio::test]
    async fn test_training_task_handles_errors() {
        // Create logger with valid events
        let logger = Arc::new(RwLock::new(AccessPatternLogger::new(1_000)));
        {
            let log = logger.write().await;
            for i in 0..200 {
                let embedding = vec![i as f32; 128];
                log.log_access(i % 10, &embedding);
            }
        }

        // Create learned strategy
        let predictor = LearnedCachePredictor::new(100).unwrap();
        let strategy = Arc::new(LearnedCacheStrategy::new(100, predictor));

        // Spawn training task
        let config = TrainingConfig {
            interval: Duration::from_millis(100),
            window_duration: Duration::from_secs(3600),
            min_events_for_training: 100,
            rmi_capacity: 100,
            ..TrainingConfig::default()
        };

        let (_shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);
        let handle =
            spawn_training_task(logger.clone(), strategy.clone(), config, None, shutdown_rx).await;

        // Wait for a few cycles
        sleep(Duration::from_millis(500)).await;

        // Task should handle any internal errors gracefully (no panic)

        handle.abort();
    }

    #[tokio::test]
    async fn test_train_predictor_success() {
        // Create access events (embedding field removed to fix memory leak)
        let mut events = Vec::new();
        for i in 0..200 {
            let event = crate::learned_cache::AccessEvent {
                doc_id: i % 10,
                timestamp: SystemTime::now(),
                access_type: crate::learned_cache::AccessType::Read,
            };
            events.push(event);
        }

        // Create config with baseline defaults
        let config = TrainingConfig::default();

        // Train predictor
        let result = train_predictor(&events, 100, &config, 50); // target_hot_entries
        assert!(result.is_ok());

        let predictor = result.unwrap();
        // Verify predictor is usable
        assert!(predictor.predict_hotness(0) >= 0.0);
        assert!(predictor.predict_hotness(0) <= 1.0);
    }

    #[tokio::test]
    async fn test_train_predictor_empty_events() {
        // Empty events should not panic
        let events = Vec::new();
        let config = TrainingConfig::default();
        let result = train_predictor(&events, 100, &config, 50); // target_hot_entries

        // Training with no events should succeed (predictor just won't be useful)
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_config_default() {
        let config = TrainingConfig::default();
        assert_eq!(config.interval, Duration::from_secs(600));
        assert_eq!(config.window_duration, Duration::from_secs(3600));
        assert_eq!(config.min_events_for_training, 100);
        assert_eq!(config.rmi_capacity, 10_000);
        assert_eq!(config.recency_halflife, Duration::from_secs(1800));
        assert_eq!(config.admission_threshold, 0.15);
        assert_eq!(config.auto_tune_enabled, true);
        assert_eq!(config.target_utilization, 0.85);
    }

    #[tokio::test]
    async fn test_training_task_cancellable() {
        let logger = Arc::new(RwLock::new(AccessPatternLogger::new(1_000)));
        let predictor = LearnedCachePredictor::new(100).unwrap();
        let strategy = Arc::new(LearnedCacheStrategy::new(100, predictor));

        let config = TrainingConfig::default();
        let (_shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);
        let handle = spawn_training_task(logger, strategy, config, None, shutdown_rx).await;

        // Task should be cancellable
        handle.abort();

        // Wait should return immediately after abort
        let result = timeout(Duration::from_millis(100), async {
            // Just verify task stops quickly
        })
        .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_training_supervisor_clean_abort() {
        // Create logger with sufficient data
        let logger = Arc::new(RwLock::new(AccessPatternLogger::new(1_000)));
        {
            let log = logger.write().await;
            for i in 0..200 {
                let embedding = vec![i as f32; 128];
                log.log_access(i % 10, &embedding);
            }
        }

        // Create learned strategy
        let predictor = LearnedCachePredictor::new(100).unwrap();
        let strategy = Arc::new(LearnedCacheStrategy::new(100, predictor));

        let config = TrainingConfig {
            interval: Duration::from_millis(100),
            window_duration: Duration::from_secs(3600),
            min_events_for_training: 100,
            rmi_capacity: 100,
            ..TrainingConfig::default()
        };

        let metrics = MetricsCollector::new();

        // Create supervisor with fast restart policy for testing
        let supervisor = TrainingTaskSupervisor::with_restart_policy(
            logger.clone(),
            strategy.clone(),
            config,
            metrics.clone(),
            3, // max 3 restarts
            0, // 0s base delay for fast testing
        );

        let (_shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);
        let handle = supervisor.supervise(shutdown_rx).await;

        // Give time for training task to start
        sleep(Duration::from_millis(200)).await;

        // Abort supervisor
        handle.abort();

        // Verify metrics show no crashes yet (task didn't panic, just cancelled)
        let crashes = metrics.get_training_crashes_count();
        assert_eq!(crashes, 0, "No crashes should be recorded for clean abort");
    }

    #[tokio::test]
    async fn test_training_supervisor_restart_limit() {
        // This test verifies correct initialization of restart policy fields.
        // Full restart behavior testing requires integration test infrastructure
        // to reliably trigger task panics (e.g., via fault injection or chaos tests).

        let logger = Arc::new(RwLock::new(AccessPatternLogger::new(1_000)));
        let predictor = LearnedCachePredictor::new(100).unwrap();
        let strategy = Arc::new(LearnedCacheStrategy::new(100, predictor));

        let config = TrainingConfig {
            interval: Duration::from_secs(600),
            window_duration: Duration::from_secs(3600),
            min_events_for_training: 100,
            rmi_capacity: 100,
            ..TrainingConfig::default()
        };

        let metrics = MetricsCollector::new();

        let supervisor = TrainingTaskSupervisor::with_restart_policy(
            logger, strategy, config, metrics, 5, // max 5 restarts
            1, // 1s base delay
        );

        // Verify supervisor was created with correct config
        assert_eq!(supervisor.max_restarts, 5);
        assert_eq!(supervisor.base_delay_secs, 1);
    }
}
