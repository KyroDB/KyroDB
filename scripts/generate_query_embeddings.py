#!/usr/bin/env python3
"""
Generate synthetic query embeddings for Phase 0.5.2 semantic validation

Purpose:
- Create paraphrased queries for each passage in the corpus
- Each passage gets 3-5 semantically similar queries with slight variance
- Simulates real RAG workload where different queries map to same document

Output:
- data/ms_marco/query_embeddings_100k.npy: Query embeddings (N×384-dim float32)
- data/ms_marco/queries_100k.txt: Query text (for debugging)
- data/ms_marco/query_to_doc.txt: Query index → doc_id mapping

Usage:
    python3 scripts/generate_query_embeddings.py --size 10000 --queries-per-doc 5
"""

import argparse
import numpy as np
import os
import sys


def generate_synthetic_queries(passages, queries_per_doc=5, seed=0xFACE):
    """
    Generate synthetic query paraphrases for each passage
    
    Templates simulate different ways users might ask for the same document:
    - Direct questions: "What is X?"
    - Explanations: "Explain X"
    - Information seeking: "Tell me about X"
    - Definitions: "Define X"
    - Overviews: "X overview"
    """
    templates = [
        "What is {topic}?",
        "Explain {topic}",
        "Tell me about {topic}",
        "Define {topic}",
        "{topic} overview",
        "Information on {topic}",
        "Learn about {topic}",
        "Details about {topic}",
        "Understanding {topic}",
        "{topic} guide",
    ]
    
    rng = np.random.default_rng(seed)
    queries = []
    query_to_doc = []
    
    for doc_id, passage in enumerate(passages):
        # Extract topic: first 3-5 words of passage
        words = passage.split()[:rng.integers(3, 6)]
        topic = " ".join(words)
        
        # Generate queries_per_doc paraphrases
        # Allow template reuse when queries_per_doc > len(templates)
        num_queries = queries_per_doc
        allow_replace = num_queries > len(templates)
        selected_templates = rng.choice(templates, size=num_queries, replace=allow_replace)
        
        for template in selected_templates:
            query = template.replace("{topic}", topic)
            queries.append(query)
            query_to_doc.append(doc_id)
    
    return queries, query_to_doc


def generate_query_embeddings(queries, doc_embeddings, query_to_doc, noise_stddev=0.05, seed=0xBEEF):
    """
    Generate query embeddings with controlled semantic variance
    
    Strategy:
    - Start with document embedding (ground truth)
    - Add Gaussian noise (stddev=0.05) to simulate paraphrasing variance
    - L2 normalize to keep embeddings on unit sphere
    
    This creates queries that are semantically SIMILAR but NOT IDENTICAL to documents,
    which is exactly what happens in real RAG workloads.
    """
    rng = np.random.default_rng(seed)
    query_embeddings = []
    
    for query_idx, doc_id in enumerate(query_to_doc):
        # Base embedding from document
        base_emb = doc_embeddings[doc_id].copy()
        
        # Add Gaussian noise for semantic variance
        noise = rng.normal(0.0, noise_stddev, size=base_emb.shape)
        query_emb = base_emb + noise
        
        # L2 normalize (keep on unit sphere)
        norm = np.linalg.norm(query_emb)
        if norm > 0:
            query_emb = query_emb / norm
        
        query_embeddings.append(query_emb)
    
    return np.array(query_embeddings, dtype=np.float32)


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic query embeddings")
    parser.add_argument("--size", type=int, default=10000,
                      help="Number of documents (must match embeddings_100k.npy)")
    parser.add_argument("--queries-per-doc", type=int, default=5,
                      help="Max queries per document (actual: 3 to queries-per-doc)")
    parser.add_argument("--noise", type=float, default=0.05,
                      help="Gaussian noise stddev for query variance")
    parser.add_argument("--output-dir", type=str, default="data/ms_marco",
                      help="Output directory")
    args = parser.parse_args()
    
    output_dir = args.output_dir
    os.makedirs(output_dir, exist_ok=True)
    
    embeddings_file = os.path.join(output_dir, "embeddings_100k.npy")
    passages_file = os.path.join(output_dir, "passages_100k.txt")
    
    # Check input files exist
    if not os.path.exists(embeddings_file):
        print(f"ERROR: {embeddings_file} not found", file=sys.stderr)
        print("Run: python3 scripts/generate_mock_embeddings.py --size 10000 first", file=sys.stderr)
        sys.exit(1)
    
    if not os.path.exists(passages_file):
        print(f"ERROR: {passages_file} not found", file=sys.stderr)
        sys.exit(1)
    
    print(f"Loading document embeddings from {embeddings_file}...")
    doc_embeddings = np.load(embeddings_file)
    num_docs, embed_dim = doc_embeddings.shape
    
    if num_docs != args.size:
        print(f"WARNING: Found {num_docs} docs, expected {args.size}", file=sys.stderr)
        print(f"Using actual size: {num_docs}", file=sys.stderr)
        args.size = num_docs
    
    print(f"Loading passages from {passages_file}...")
    with open(passages_file, 'r', encoding='utf-8') as f:
        passages = [line.strip() for line in f if line.strip()]
    
    if len(passages) != num_docs:
        print(f"ERROR: Passage count ({len(passages)}) != embedding count ({num_docs})", file=sys.stderr)
        sys.exit(1)
    
    print(f"\nGenerating synthetic queries ({args.queries_per_doc} per doc)...")
    queries, query_to_doc = generate_synthetic_queries(passages, args.queries_per_doc)
    num_queries = len(queries)
    
    print(f"Generated {num_queries} queries for {num_docs} documents")
    print(f"Average queries per document: {num_queries / num_docs:.2f}")
    
    print(f"\nGenerating query embeddings (noise stddev={args.noise})...")
    query_embeddings = generate_query_embeddings(queries, doc_embeddings, query_to_doc, args.noise)
    
    # Verify dimensions
    assert query_embeddings.shape == (num_queries, embed_dim), \
        f"Shape mismatch: {query_embeddings.shape} != ({num_queries}, {embed_dim})"
    
    # Save outputs
    query_embeddings_file = os.path.join(output_dir, "query_embeddings_100k.npy")
    queries_txt_file = os.path.join(output_dir, "queries_100k.txt")
    query_to_doc_file = os.path.join(output_dir, "query_to_doc.txt")
    
    print(f"\nSaving query embeddings to {query_embeddings_file}...")
    np.save(query_embeddings_file, query_embeddings)
    
    print(f"Saving query text to {queries_txt_file}...")
    with open(queries_txt_file, 'w', encoding='utf-8') as f:
        for query in queries:
            f.write(query + '\n')
    
    print(f"Saving query→doc mapping to {query_to_doc_file}...")
    with open(query_to_doc_file, 'w', encoding='utf-8') as f:
        for doc_id in query_to_doc:
            f.write(f"{doc_id}\n")
    
    # Statistics
    query_emb_size_mb = query_embeddings.nbytes / (1024 * 1024)
    print(f"\nGeneration complete!")
    print(f"  Queries: {num_queries:,}")
    print(f"  Dimensions: {embed_dim}")
    print(f"  Size: {query_emb_size_mb:.1f} MB")
    print(f"  Files:")
    print(f"    - {query_embeddings_file}")
    print(f"    - {queries_txt_file}")
    print(f"    - {query_to_doc_file}")
    
    # Verify semantic similarity
    print(f"\nVerifying semantic similarity...")
    sample_indices = np.random.choice(num_queries, size=min(100, num_queries), replace=False)
    similarities = []
    
    for query_idx in sample_indices:
        doc_id = query_to_doc[query_idx]
        query_emb = query_embeddings[query_idx]
        doc_emb = doc_embeddings[doc_id]
        
        # Cosine similarity
        sim = np.dot(query_emb, doc_emb)
        similarities.append(sim)
    
    avg_sim = np.mean(similarities)
    std_sim = np.std(similarities)
    min_sim = np.min(similarities)
    max_sim = np.max(similarities)
    
    print(f"  Query-Document Similarity (sample of {len(sample_indices)}):")
    print(f"    Mean: {avg_sim:.3f}")
    print(f"    Std:  {std_sim:.3f}")
    print(f"    Min:  {min_sim:.3f}")
    print(f"    Max:  {max_sim:.3f}")
    print(f"\n  Expected: Mean ~0.95-0.99 (high similarity with variance)")
    
    if avg_sim < 0.90:
        print(f"  WARNING: Low similarity detected. Increase noise or check embeddings.", file=sys.stderr)
    elif avg_sim > 0.99:
        print(f"  WARNING: Too similar. Decrease noise for more variance.", file=sys.stderr)
    else:
        print(f"  Status: ✓ Semantic variance looks good")


if __name__ == "__main__":
    main()
