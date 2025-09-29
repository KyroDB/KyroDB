#[cfg(test)]
mod tests {
    use anyhow::Result;
    use kyrodb_engine::adaptive_rmi::AdaptiveRMI;

    #[tokio::test]
    async fn test_basic_lookup_debug() -> Result<()> {
        println!("Debug: Testing basic AdaptiveRMI functionality...");

        let rmi = AdaptiveRMI::new();

        // Insert a few keys
        println!("Inserting test data...");
        rmi.insert(0, 100)?;
        rmi.insert(1, 101)?;
        rmi.insert(10, 110)?;

        println!("Inserted 3 keys: (0,100), (1,101), (10,110)");

        // Try to lookup
        println!("Testing lookups...");

        match rmi.lookup_key_ultra_fast(0) {
            Some(val) => {
                println!("Key 0 found with value: {}", val);
                assert_eq!(val, 100);
            }
            None => {
                println!("Key 0 not found");
                panic!("Key 0 should be found!");
            }
        }

        match rmi.lookup_key_ultra_fast(1) {
            Some(val) => {
                println!("Key 1 found with value: {}", val);
                assert_eq!(val, 101);
            }
            None => {
                println!("Key 1 not found");
                panic!("Key 1 should be found!");
            }
        }

        match rmi.lookup_key_ultra_fast(10) {
            Some(val) => {
                println!("Key 10 found with value: {}", val);
                assert_eq!(val, 110);
            }
            None => {
                println!("Key 10 not found");
                panic!("Key 10 should be found!");
            }
        }

        match rmi.lookup_key_ultra_fast(999) {
            Some(val) => {
                println!("Key 999 found (should not exist): {}", val);
                panic!("Key 999 should not be found!");
            }
            None => println!("Key 999 correctly not found"),
        }

        println!("Basic lookup test completed successfully");
        Ok(())
    }
}
