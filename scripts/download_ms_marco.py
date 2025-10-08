#!/usr/bin/env python3
"""
Download and prepare MS MARCO dataset for KyroDB validation.

Phase 0.5.1: Dataset Integration
- Downloads 100K passages from MS MARCO v1.1
- Generates embeddings using Sentence-BERT (all-MiniLM-L6-v2)
- Saves passages, doc IDs, and embeddings for validation tests

Requirements:
    pip install datasets sentence-transformers torch numpy

Usage:
    python3 scripts/download_ms_marco.py
    python3 scripts/download_ms_marco.py --size 50000  # Custom size
"""

import argparse
import sys
from pathlib import Path

try:
    from datasets import load_dataset
    from sentence_transformers import SentenceTransformer
    import numpy as np
except ImportError as e:
    print(f"Error: Missing required package - {e}")
    print("\nInstall dependencies:")
    print("  pip install datasets sentence-transformers torch numpy")
    sys.exit(1)


def download_passages(output_dir: Path, num_passages: int = 100_000):
    """Download MS MARCO passages and save to text files."""
    print(f"Downloading {num_passages:,} passages from MS MARCO v1.1...")
    
    try:
        # Load dataset from Hugging Face
        ds = load_dataset('microsoft/ms_marco', 'v1.1', split=f'train[:{num_passages}]')
        
        # Extract passages and IDs
        passages = []
        doc_ids = []
        
        for row in ds:
            # MS MARCO v1.1 has 'passage' field
            passage_text = row.get('passage', row.get('passage_text', ''))
            if not passage_text:
                continue
                
            passages.append(passage_text)
            
            # Use index as doc_id if not available
            passage_id = row.get('passage_id', len(doc_ids))
            doc_ids.append(passage_id)
        
        print(f"Downloaded {len(passages):,} passages")
        
        # Save passages
        passages_file = output_dir / 'passages_100k.txt'
        with open(passages_file, 'w', encoding='utf-8') as f:
            for passage in passages:
                # Remove newlines to keep one passage per line
                clean_passage = passage.replace('\n', ' ').replace('\r', ' ')
                f.write(clean_passage + '\n')
        
        print(f"Saved passages to {passages_file}")
        
        # Save doc IDs
        doc_ids_file = output_dir / 'doc_ids_100k.txt'
        with open(doc_ids_file, 'w', encoding='utf-8') as f:
            for doc_id in doc_ids:
                f.write(str(doc_id) + '\n')
        
        print(f"Saved doc IDs to {doc_ids_file}")
        
        return passages_file, len(passages)
        
    except Exception as e:
        print(f"Error downloading dataset: {e}")
        print("\nTroubleshooting:")
        print("  1. Check internet connection")
        print("  2. Verify Hugging Face datasets library is installed")
        print("  3. Try a smaller dataset size with --size 10000")
        sys.exit(1)


def generate_embeddings(passages_file: Path, output_dir: Path):
    """Generate embeddings using Sentence-BERT."""
    print("\nGenerating embeddings with all-MiniLM-L6-v2...")
    print("(This may take 5-10 minutes for 100K passages)")
    
    try:
        # Load model (384-dim embeddings)
        model = SentenceTransformer('all-MiniLM-L6-v2')
        print(f"Model loaded: {model.get_sentence_embedding_dimension()}-dimensional embeddings")
        
        # Load passages
        with open(passages_file, 'r', encoding='utf-8') as f:
            passages = [line.strip() for line in f if line.strip()]
        
        print(f"Encoding {len(passages):,} passages...")
        
        # Batch encode (faster than one-by-one)
        embeddings = model.encode(
            passages,
            batch_size=256,
            show_progress_bar=True,
            convert_to_numpy=True,
            normalize_embeddings=True  # L2 normalize for cosine similarity
        )
        
        # Save as numpy array
        embeddings_file = output_dir / 'embeddings_100k.npy'
        np.save(embeddings_file, embeddings)
        
        print(f"\nGenerated {len(embeddings):,} embeddings")
        print(f"Shape: {embeddings.shape}")
        print(f"Memory: {embeddings.nbytes / 1024 / 1024:.1f} MB")
        print(f"Saved to {embeddings_file}")
        
        return embeddings_file
        
    except Exception as e:
        print(f"Error generating embeddings: {e}")
        print("\nTroubleshooting:")
        print("  1. Check if torch is installed: pip install torch")
        print("  2. Verify sufficient disk space (~150MB needed)")
        print("  3. Check if passages file exists and is readable")
        sys.exit(1)


def verify_data(output_dir: Path):
    """Verify downloaded data integrity."""
    print("\nVerifying data...")
    
    passages_file = output_dir / 'passages_100k.txt'
    doc_ids_file = output_dir / 'doc_ids_100k.txt'
    embeddings_file = output_dir / 'embeddings_100k.npy'
    
    # Check files exist
    for file in [passages_file, doc_ids_file, embeddings_file]:
        if not file.exists():
            print(f"Error: Missing file {file}")
            return False
    
    # Load and verify
    with open(passages_file, 'r', encoding='utf-8') as f:
        num_passages = sum(1 for _ in f)
    
    with open(doc_ids_file, 'r', encoding='utf-8') as f:
        num_doc_ids = sum(1 for _ in f)
    
    embeddings = np.load(embeddings_file)
    
    print(f"Passages: {num_passages:,}")
    print(f"Doc IDs: {num_doc_ids:,}")
    print(f"Embeddings: {embeddings.shape[0]:,} × {embeddings.shape[1]}-dim")
    
    if num_passages != num_doc_ids or num_passages != embeddings.shape[0]:
        print("Error: Mismatch in data counts")
        return False
    
    print("Verification passed")
    return True


def main():
    parser = argparse.ArgumentParser(
        description='Download MS MARCO dataset for KyroDB validation'
    )
    parser.add_argument(
        '--size',
        type=int,
        default=100_000,
        help='Number of passages to download (default: 100000)'
    )
    parser.add_argument(
        '--output',
        type=Path,
        default=Path('data/ms_marco'),
        help='Output directory (default: data/ms_marco)'
    )
    parser.add_argument(
        '--skip-embeddings',
        action='store_true',
        help='Skip embedding generation (only download passages)'
    )
    
    args = parser.parse_args()
    
    # Create output directory
    args.output.mkdir(parents=True, exist_ok=True)
    
    print("=" * 70)
    print("KyroDB MS MARCO Dataset Preparation")
    print("=" * 70)
    
    # Download passages
    passages_file, num_passages = download_passages(args.output, args.size)
    
    # Generate embeddings
    if not args.skip_embeddings:
        embeddings_file = generate_embeddings(passages_file, args.output)
        
        # Verify
        if verify_data(args.output):
            print("\n" + "=" * 70)
            print("SUCCESS: Dataset ready for validation")
            print("=" * 70)
            print(f"\nNext steps:")
            print(f"  1. Build validation binary:")
            print(f"     cargo build --release --bin validation_enterprise")
            print(f"  2. Run validation:")
            print(f"     ./target/release/validation_enterprise validation_semantic_smoke.json")
        else:
            print("\nWarning: Data verification failed")
            sys.exit(1)
    else:
        print("\nSkipped embedding generation (--skip-embeddings)")
        print("Run again without --skip-embeddings to generate embeddings")


if __name__ == '__main__':
    main()
