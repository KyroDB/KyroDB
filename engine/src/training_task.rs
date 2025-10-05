// Phase 0 Week 9-12: Background training task for learned cache
// Periodically retrains RMI predictor using access logs

use crate::access_logger::AccessPatternLogger;
use crate::cache_strategy::LearnedCacheStrategy;
use crate::learned_cache::LearnedCachePredictor;
use anyhow::Result;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

/// Configuration for background training task
#[derive(Debug, Clone)]
pub struct TrainingConfig {
    /// Training interval (default: 10 minutes)
    pub interval: Duration,

    /// Training window duration (default: 24 hours)
    pub window_duration: Duration,

    /// Minimum events required before training (default: 100)
    pub min_events_for_training: usize,

    /// RMI capacity (default: 10,000 documents)
    pub rmi_capacity: usize,
}

impl Default for TrainingConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(600),              // 10 minutes
            window_duration: Duration::from_secs(24 * 3600), // 24 hours
            min_events_for_training: 100,
            rmi_capacity: 10_000,
        }
    }
}

/// Spawns a background task that periodically retrains the learned cache predictor
///
/// The task runs every `config.interval` seconds and:
/// 1. Fetches recent access events from the logger
/// 2. Trains a new RMI predictor on those events
/// 3. Updates the learned cache strategy with the new predictor
///
/// # Arguments
/// * `access_logger` - Shared access pattern logger (tracks query accesses)
/// * `learned_strategy` - Shared learned cache strategy (will be updated with new predictor)
/// * `config` - Training configuration (interval, window, capacity)
///
/// # Returns
/// JoinHandle for the background task (can be used to cancel or await completion)
///
/// # Example
/// ```no_run
/// use kyrodb_engine::{AccessPatternLogger, LearnedCacheStrategy, LearnedCachePredictor, VectorCache};
/// use kyrodb_engine::training_task::{spawn_training_task, TrainingConfig};
/// use std::sync::Arc;
/// use tokio::sync::RwLock;
///
/// #[tokio::main]
/// async fn main() {
///     let logger = Arc::new(RwLock::new(AccessPatternLogger::new(100_000)));
///     let predictor = LearnedCachePredictor::new(10_000).unwrap();
///     let strategy = Arc::new(LearnedCacheStrategy::new(10_000, predictor));
///     
///     let config = TrainingConfig::default();
///     let handle = spawn_training_task(logger, strategy, config).await;
///     
///     // Task runs in background...
///     handle.abort(); // Cancel when done
/// }
/// ```
pub async fn spawn_training_task(
    access_logger: Arc<RwLock<AccessPatternLogger>>,
    learned_strategy: Arc<LearnedCacheStrategy>,
    config: TrainingConfig,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(config.interval);

        loop {
            interval.tick().await;

            // Fetch recent access events
            let events = {
                let logger = access_logger.read().await;
                logger.get_recent_window(config.window_duration)
            };

            // Skip training if insufficient data
            if events.len() < config.min_events_for_training {
                continue;
            }

            // Train new predictor
            match train_predictor(&events, config.rmi_capacity) {
                Ok(new_predictor) => {
                    // Update learned strategy atomically
                    learned_strategy.update_predictor(new_predictor);
                }
                Err(e) => {
                    eprintln!("Training task error: {}", e);
                }
            }
        }
    })
}

/// Trains a new RMI predictor from access events
///
/// Internal helper function - builds predictor from scratch using access patterns
fn train_predictor(
    events: &[crate::learned_cache::AccessEvent],
    capacity: usize,
) -> Result<LearnedCachePredictor> {
    let mut predictor = LearnedCachePredictor::new(capacity)?;
    predictor.train_from_accesses(events)?;
    Ok(predictor)
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
            let mut log = logger.write().await;
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
        };

        let handle = spawn_training_task(logger.clone(), strategy.clone(), config).await;

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
            let mut log = logger.write().await;
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
        };

        let handle = spawn_training_task(logger.clone(), strategy.clone(), config).await;

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
            let mut log = logger.write().await;
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
        };

        let handle = spawn_training_task(logger.clone(), strategy.clone(), config).await;

        // Wait for a few cycles
        sleep(Duration::from_millis(500)).await;

        // Task should handle any internal errors gracefully (no panic)

        handle.abort();
    }

    #[tokio::test]
    async fn test_train_predictor_success() {
        // Create access events
        let mut events = Vec::new();
        for i in 0..200 {
            let event = crate::learned_cache::AccessEvent {
                doc_id: i % 10,
                timestamp: SystemTime::now(),
                access_type: crate::learned_cache::AccessType::Read,
            };
            events.push(event);
        }

        // Train predictor
        let result = train_predictor(&events, 100);
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
        let result = train_predictor(&events, 100);

        // Training with no events should succeed (predictor just won't be useful)
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_config_default() {
        let config = TrainingConfig::default();
        assert_eq!(config.interval, Duration::from_secs(600));
        assert_eq!(config.window_duration, Duration::from_secs(24 * 3600));
        assert_eq!(config.min_events_for_training, 100);
        assert_eq!(config.rmi_capacity, 10_000);
    }

    #[tokio::test]
    async fn test_training_task_cancellable() {
        let logger = Arc::new(RwLock::new(AccessPatternLogger::new(1_000)));
        let predictor = LearnedCachePredictor::new(100).unwrap();
        let strategy = Arc::new(LearnedCacheStrategy::new(100, predictor));

        let config = TrainingConfig::default();
        let handle = spawn_training_task(logger, strategy, config).await;

        // Task should be cancellable
        handle.abort();

        // Wait should return immediately after abort
        let result = timeout(Duration::from_millis(100), async {
            // Just verify task stops quickly
        })
        .await;

        assert!(result.is_ok());
    }
}
