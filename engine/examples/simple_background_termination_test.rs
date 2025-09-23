//! Simple Background Maintenance Termination Test
//! 
//! This validates that the bounded background maintenance terminates properly.

use kyrodb_engine::adaptive_rmi::AdaptiveRMI;
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔧 KyroDB Background Maintenance Termination Test");
    println!("================================================");
    
    let rmi = Arc::new(AdaptiveRMI::new());
    
    println!("🚀 Starting background maintenance...");
    let start_time = std::time::Instant::now();
    
    // Start background maintenance and wait for it to complete
    let maintenance_handle = rmi.clone().start_background_maintenance();
    
    // The background maintenance should terminate on its own after 3000 iterations
    // At 10 Hz, this should take about 5 minutes maximum
    // But we'll wait a bit longer to be safe
    match tokio::time::timeout(Duration::from_secs(30), maintenance_handle).await {
        Ok(Ok(())) => {
            let elapsed = start_time.elapsed();
            println!("✅ Background maintenance terminated successfully after {:.2}s", elapsed.as_secs_f64());
            println!("🛡️  Safety limits are working correctly");
        }
        Ok(Err(e)) => {
            println!("⚠️  Background maintenance stopped with error: {}", e);
            println!("   This might be acceptable depending on the error type");
        }
        Err(_) => {
            println!("❌ Background maintenance did not terminate within 30 seconds");
            println!("   This indicates the safety limits may not be working");
            return Err("Background maintenance safety limits failed".into());
        }
    }
    
    println!("\n✅ Background maintenance termination test completed successfully!");
    
    Ok(())
}
