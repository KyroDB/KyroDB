#!/usr/bin/env python3
"""
Generate mock semantic embeddings for KyroDB validation testing.

This creates a realistic dataset with semantic clusters for testing
the hybrid semantic-learned cache without requiring MS MARCO download.

Usage:
    python3 scripts/generate_mock_embeddings.py
    python3 scripts/generate_mock_embeddings.py --size 50000
"""

import argparse
import sys
from pathlib import Path

try:
    import numpy as np
except ImportError:
    print("Error: numpy not installed")
    print("Install: pip install numpy")
    sys.exit(1)


def generate_clustered_embeddings(num_docs: int, embedding_dim: int = 384, num_clusters: int = 20):
    """
    Generate embeddings with semantic clustering.
    
    - Creates topic clusters (like "ML", "databases", "web dev")
    - Each document is assigned to a cluster
    - Embeddings within a cluster are similar
    - L2 normalized for cosine similarity
    """
    print(f"Generating {num_docs:,} embeddings with {num_clusters} semantic clusters...")
    
    # Generate cluster centers
    np.random.seed(42)
    cluster_centers = np.random.randn(num_clusters, embedding_dim).astype(np.float32)
    
    # Normalize cluster centers
    for i in range(num_clusters):
        norm = np.linalg.norm(cluster_centers[i])
        if norm > 0:
            cluster_centers[i] /= norm
    
    # Generate embeddings
    embeddings = np.zeros((num_docs, embedding_dim), dtype=np.float32)
    
    for i in range(num_docs):
        # Assign to cluster (round-robin with some randomness)
        if i < num_docs * 0.2:
            # 20% uniformly distributed (cold traffic)
            cluster_idx = i % num_clusters
        else:
            # 80% follow Zipf distribution (hot topics)
            cluster_idx = int(np.random.zipf(1.5)) % num_clusters
        
        # Start from cluster center
        embedding = cluster_centers[cluster_idx].copy()
        
        # Add Gaussian noise (smaller noise = tighter clusters)
        noise = np.random.randn(embedding_dim).astype(np.float32) * 0.1
        embedding += noise
        
        # L2 normalize
        norm = np.linalg.norm(embedding)
        if norm > 0:
            embedding /= norm
        
        embeddings[i] = embedding
    
    return embeddings, cluster_centers


def generate_passages(num_docs: int):
    """Generate synthetic passages with topic labels."""
    topics = [
        "machine learning algorithms and neural networks",
        "database management systems and SQL",
        "web development frameworks and APIs",
        "cloud computing infrastructure and DevOps",
        "data science and statistical analysis",
        "artificial intelligence and deep learning",
        "software engineering best practices",
        "computer networks and distributed systems",
        "cybersecurity and cryptography",
        "operating systems and kernel development",
        "mobile app development for iOS and Android",
        "blockchain technology and cryptocurrencies",
        "computer graphics and game development",
        "natural language processing and text mining",
        "recommendation systems and personalization",
        "big data processing with Spark and Hadoop",
        "microservices architecture patterns",
        "containerization with Docker and Kubernetes",
        "continuous integration and deployment",
        "performance optimization and profiling"
    ]
    
    passages = []
    doc_ids = []
    
    for i in range(num_docs):
        topic = topics[i % len(topics)]
        
        # Add variety to passages
        templates = [
            f"Document {i} discusses {topic} in detail.",
            f"This article covers {topic} with practical examples.",
            f"A comprehensive guide to {topic} for beginners.",
            f"Advanced techniques in {topic} explained simply.",
            f"Understanding {topic}: a tutorial with code samples.",
        ]
        
        passage = templates[i % len(templates)]
        passages.append(passage)
        doc_ids.append(str(i))
    
    return passages, doc_ids


def save_data(output_dir: Path, embeddings, passages, doc_ids):
    """Save embeddings, passages, and doc IDs to disk."""
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Save embeddings as numpy array
    embeddings_file = output_dir / 'embeddings_100k.npy'
    np.save(embeddings_file, embeddings)
    print(f"Saved embeddings to {embeddings_file}")
    print(f"  Shape: {embeddings.shape}")
    print(f"  Memory: {embeddings.nbytes / 1024 / 1024:.1f} MB")
    
    # Save passages
    passages_file = output_dir / 'passages_100k.txt'
    with open(passages_file, 'w', encoding='utf-8') as f:
        for passage in passages:
            f.write(passage + '\n')
    print(f"Saved passages to {passages_file}")
    
    # Save doc IDs
    doc_ids_file = output_dir / 'doc_ids_100k.txt'
    with open(doc_ids_file, 'w', encoding='utf-8') as f:
        for doc_id in doc_ids:
            f.write(doc_id + '\n')
    print(f"Saved doc IDs to {doc_ids_file}")
    
    return embeddings_file, passages_file, doc_ids_file


def verify_data(embeddings_file: Path, passages_file: Path, doc_ids_file: Path):
    """Verify generated data."""
    print("\nVerifying data...")
    
    # Load embeddings
    embeddings = np.load(embeddings_file)
    print(f"  Embeddings: {embeddings.shape[0]:,} × {embeddings.shape[1]}-dim")
    
    # Count passages
    with open(passages_file, 'r', encoding='utf-8') as f:
        num_passages = sum(1 for _ in f)
    print(f"  Passages: {num_passages:,}")
    
    # Count doc IDs
    with open(doc_ids_file, 'r', encoding='utf-8') as f:
        num_doc_ids = sum(1 for _ in f)
    print(f"  Doc IDs: {num_doc_ids:,}")
    
    # Check consistency
    if embeddings.shape[0] == num_passages == num_doc_ids:
        print("  Status: ✓ All counts match")
        return True
    else:
        print("  Status: ✗ Count mismatch!")
        return False


def compute_cluster_stats(embeddings, num_clusters: int = 20):
    """Compute statistics about embedding clusters."""
    from sklearn.cluster import KMeans
    
    print("\nComputing cluster statistics...")
    
    # Sample for clustering (faster)
    sample_size = min(5000, len(embeddings))
    sample_indices = np.random.choice(len(embeddings), sample_size, replace=False)
    sample = embeddings[sample_indices]
    
    # Run K-Means
    kmeans = KMeans(n_clusters=num_clusters, random_state=42, n_init=10)
    labels = kmeans.fit_predict(sample)
    
    # Compute cluster sizes
    unique, counts = np.unique(labels, return_counts=True)
    print(f"  Cluster sizes: min={counts.min()}, max={counts.max()}, mean={counts.mean():.1f}")
    
    # Compute average intra-cluster similarity
    intra_similarities = []
    for cluster_id in unique:
        cluster_embeddings = sample[labels == cluster_id]
        if len(cluster_embeddings) > 1:
            # Compute pairwise cosine similarities
            similarities = np.dot(cluster_embeddings, cluster_embeddings.T)
            # Average similarity (excluding diagonal)
            mask = ~np.eye(len(cluster_embeddings), dtype=bool)
            avg_sim = similarities[mask].mean()
            intra_similarities.append(avg_sim)
    
    print(f"  Avg intra-cluster similarity: {np.mean(intra_similarities):.3f}")
    print(f"  (Higher = tighter clusters, good for semantic cache testing)")


def main():
    parser = argparse.ArgumentParser(
        description='Generate mock embeddings for KyroDB validation'
    )
    parser.add_argument(
        '--size',
        type=int,
        default=10_000,
        help='Number of documents to generate (default: 10000)'
    )
    parser.add_argument(
        '--clusters',
        type=int,
        default=20,
        help='Number of semantic clusters (default: 20)'
    )
    parser.add_argument(
        '--dim',
        type=int,
        default=384,
        help='Embedding dimension (default: 384)'
    )
    parser.add_argument(
        '--output',
        type=Path,
        default=Path('data/ms_marco'),
        help='Output directory (default: data/ms_marco)'
    )
    parser.add_argument(
        '--stats',
        action='store_true',
        help='Compute cluster statistics (requires scikit-learn)'
    )
    
    args = parser.parse_args()
    
    print("=" * 70)
    print("KyroDB Mock Embedding Generation")
    print("=" * 70)
    print(f"Documents: {args.size:,}")
    print(f"Clusters: {args.clusters}")
    print(f"Dimension: {args.dim}")
    print(f"Output: {args.output}")
    print()
    
    # Generate embeddings
    embeddings, cluster_centers = generate_clustered_embeddings(
        args.size, args.dim, args.clusters
    )
    
    # Generate passages
    passages, doc_ids = generate_passages(args.size)
    
    # Save data
    embeddings_file, passages_file, doc_ids_file = save_data(
        args.output, embeddings, passages, doc_ids
    )
    
    # Verify
    if verify_data(embeddings_file, passages_file, doc_ids_file):
        print("\n" + "=" * 70)
        print("SUCCESS: Mock data ready for validation")
        print("=" * 70)
        
        if args.stats:
            try:
                compute_cluster_stats(embeddings, args.clusters)
            except ImportError:
                print("\nNote: Install scikit-learn for cluster statistics")
                print("  pip install scikit-learn")
        
        print("\nNext steps:")
        print("  1. Build validation binary:")
        print("     cargo build --release --bin validation_enterprise")
        print("  2. Run validation:")
        print("     ./target/release/validation_enterprise validation_phase051_smoke.json")
    else:
        print("\nError: Data verification failed")
        sys.exit(1)


if __name__ == '__main__':
    main()
