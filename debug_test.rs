// Simple debug test to check if lookups are working at all
use kyrodb_engine::{AdaptiveRMI, Result};

#[tokio::main]
async fn main() -> Result<()> {
    println!("🔍 Debug: Testing basic AdaptiveRMI functionality...");
    
    let rmi = AdaptiveRMI::new();
    
    // Insert a few keys
    println!("📝 Inserting test data...");
    rmi.insert(0, 100).await?;
    rmi.insert(1, 101).await?;
    rmi.insert(10, 110).await?;
    
    println!("✅ Inserted 3 keys: (0,100), (1,101), (10,110)");
    
    // Try to lookup
    println!("🔍 Testing lookups...");
    
    match rmi.lookup(0).await {
        Some(val) => println!("✅ Key 0 found with value: {}", val),
        None => println!("❌ Key 0 not found"),
    }
    
    match rmi.lookup(1).await {
        Some(val) => println!("✅ Key 1 found with value: {}", val),
        None => println!("❌ Key 1 not found"),
    }
    
    match rmi.lookup(10).await {
        Some(val) => println!("✅ Key 10 found with value: {}", val),
        None => println!("❌ Key 10 not found"),
    }
    
    match rmi.lookup(999).await {
        Some(val) => println!("❌ Key 999 found (should not exist): {}", val),
        None => println!("✅ Key 999 correctly not found"),
    }
    
    println!("🎯 Debug test completed");
    Ok(())
}
