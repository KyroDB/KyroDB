/// Quick test: Verify hash_embedding produces diverse keys
use std::collections::HashSet;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::fs::File;
use std::io::BufReader;

fn hash_embedding(embedding: &[f32]) -> u64 {
    let mut hasher = DefaultHasher::new();
    
    // Hash first 8 floats (or all if fewer than 8)
    for &value in embedding.iter().take(8) {
        value.to_bits().hash(&mut hasher);
    }
    
    hasher.finish()
}

fn main() {
    use ndarray::Array2;
    use ndarray_npy::ReadNpyExt;

    let query_path = "data/ms_marco/query_embeddings_100k.npy";
    println!("Loading {}...", query_path);
    
    let file = File::open(query_path).expect("Failed to open file");
    let reader = BufReader::new(file);
    let arr: Array2<f32> = Array2::read_npy(reader).expect("Failed to read numpy file");
    
    let (num_queries, dim) = arr.dim();
    println!("Loaded {} queries of {}-dim", num_queries, dim);

    // Hash all 300K queries and check for unique hashes
    let mut unique_hashes = HashSet::new();
    let query_embeddings: Vec<Vec<f32>> = arr.outer_iter()
        .map(|row| row.to_vec())
        .collect();

    for embedding in &query_embeddings {
        let hash = hash_embedding(embedding);
        unique_hashes.insert(hash);
    }

    println!("\n=== HASH DIVERSITY TEST ===");
    println!("Total queries:  {}", num_queries);
    println!("Unique hashes:  {}", unique_hashes.len());
    println!("Collision rate: {:.2}%", 
        (1.0 - unique_hashes.len() as f64 / num_queries as f64) * 100.0);

    // For 30 queries/doc scenario:
    // If all 30 queries for a doc have different hashes → LRU should miss 29/30 times
    // If all 30 queries produce SAME hash → LRU should hit every time (current behavior!)
    
    // Check: sample first 30 queries (should be for doc 0)
    println!("\n=== FIRST DOCUMENT (30 queries) ===");
    let mut doc0_hashes = HashSet::new();
    for i in 0..30.min(num_queries) {
        let hash = hash_embedding(&query_embeddings[i]);
        doc0_hashes.insert(hash);
    }
    println!("First 30 queries produce {} unique hashes", doc0_hashes.len());
    
    if doc0_hashes.len() < 30 {
        println!("⚠️  PROBLEM: Hash collisions for same document's queries!");
        println!("⚠️  This explains why LRU hit rate is high!");
    } else {
        println!("✓ Hash diversity is good");
    }
}
