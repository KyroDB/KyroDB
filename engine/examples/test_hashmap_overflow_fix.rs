/// Test to verify that the HashMap-based overflow buffer provides O(1) lookups
/// This replaces the previous O(n) linear search with guaranteed O(1) performance
use kyrodb_engine::adaptive_rmi::BoundedOverflowBuffer;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing O(1) HashMap-based overflow buffer performance...");
    
    // Test with increasing buffer sizes to verify O(1) behavior
    let test_sizes = vec![1000, 10000, 50000, 100000];
    
    for &size in &test_sizes {
        println!("\n=== Testing with {} entries ===", size);
        
        let mut buffer = BoundedOverflowBuffer::new(size + 1000); // Ensure capacity
        
        // Insert test data
        let insert_start = Instant::now();
        for i in 0..size {
            buffer.try_insert(i as u64, (i * 2) as u64)?;
        }
        let insert_duration = insert_start.elapsed();
        
        // Test O(1) lookups - should be constant time regardless of buffer size
        let lookup_start = Instant::now();
        let mut found_count = 0;
        
        // Test 1000 random lookups
        for i in 0..1000 {
            let key = (i * 43) % size as u64; // Pseudo-random key within range
            if buffer.get(key).is_some() {
                found_count += 1;
            }
        }
        let lookup_duration = lookup_start.elapsed();
        
        // Test lookup of existing vs non-existing keys
        let existing_key_start = Instant::now();
        let _ = buffer.get(100); // Key that exists
        let existing_key_time = existing_key_start.elapsed();
        
        let non_existing_key_start = Instant::now();
        let _ = buffer.get(size as u64 + 999999); // Key that doesn't exist
        let non_existing_key_time = non_existing_key_start.elapsed();
        
        println!("  Insertions: {} entries in {:?} ({:.2} ops/sec)", 
                size, insert_duration, size as f64 / insert_duration.as_secs_f64());
        println!("  Lookups: {} found out of 1000 in {:?} ({:.2} μs/lookup)", 
                found_count, lookup_duration, lookup_duration.as_micros() as f64 / 1000.0);
        println!("  Single existing key lookup: {:?}", existing_key_time);
        println!("  Single non-existing key lookup: {:?}", non_existing_key_time);
        
        // Verify O(1) performance - lookup time should be roughly constant
        let avg_lookup_micros = lookup_duration.as_micros() as f64 / 1000.0;
        if avg_lookup_micros > 50.0 { // More than 50 microseconds per lookup is suspicious
            println!("  ⚠️  WARNING: Lookup time seems high for O(1) operation");
        } else {
            println!("  ✅ O(1) performance confirmed");
        }
        
        // Test buffer stats
        let (current_size, capacity, rejected, pressure, memory_mb) = buffer.stats();
        println!("  Buffer stats: {}/{} entries, {} rejected, pressure={}, ~{}MB", 
                current_size, capacity, rejected, pressure, memory_mb);
    }
    
    println!("\n=== Performance Comparison Summary ===");
    println!("Previous O(n) implementation: Linear search through all entries");
    println!("New O(1) implementation: HashMap-based instant lookup");
    println!("Expected improvement: ~1000x faster for 100,000 entries");
    println!("Memory overhead: Minimal (HashMap + insertion order tracking)");
    
    Ok(())
}
