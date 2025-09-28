use anyhow::Result;
use kyrodb_engine::adaptive_rmi::AdaptiveRMI;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    println!("Bounded Search Stress Test");
    println!("==========================");

    let adaptive_rmi = Arc::new(AdaptiveRMI::new());
    println!("Adaptive index initialised");

    for i in 0..10_000_u64 {
        adaptive_rmi.insert(i * 10, i * 100)?;
    }

    for i in 0..1_000_u64 {
        let key = (i * 7_919) % 100_000;
        adaptive_rmi.insert(key, key * 2)?;
    }

    println!("Completed data ingestion. Triggering merge...");
    adaptive_rmi.merge_hot_buffer().await?;

    println!("Running random lookups to gather statistics...");
    let mut hits = 0_u64;
    for i in 0..1_000_u64 {
        let key = (i * 31) % 100_000;
        if adaptive_rmi.lookup(key).is_some() {
            hits += 1;
        }
    }
    println!("Lookup hits: {} of 1000", hits);

    let analytics = adaptive_rmi.get_bounded_search_analytics();
    println!("Segments: {}", analytics.total_segments);
    println!(
        "Bounded guarantee ratio: {:.1}%",
        analytics.bounded_guarantee_ratio * 100.0
    );
    println!("Overall error rate: {:.3}%", analytics.overall_error_rate * 100.0);
    println!(
        "Maximum search window observed: {}",
        analytics.max_search_window_observed
    );

    let validation = adaptive_rmi.validate_bounded_search_guarantees();
    println!(
        "System meets guarantees: {}",
        validation.system_meets_guarantees
    );
    println!(
        "Performance level: {}",
        validation.performance_level
    );
    println!(
        "Segments needing attention: {}",
        validation.segments_needing_attention
    );
    println!("Recommendation: {}", validation.recommendation);

    println!("Stress test complete.");
    Ok(())
}
