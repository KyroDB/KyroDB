#!/usr/bin/env python3
"""
Download and prepare MS MARCO dataset for KyroDB validation.

Dataset Integration
- Downloads 100K passages from MS MARCO v2.1 (passage ranking)
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
    print(f"Downloading {num_passages:,} passages from MS MARCO v2.1...")
    
    try:
        # Load MS MARCO v2.1 passage ranking dataset
        # This version has the actual passage corpus
        print("Loading dataset (this may take a few minutes)...")
        ds = load_dataset('microsoft/ms_marco', 'v2.1', split='train')
        
    # MS MARCO v2.1 structure (columnar nested dict under 'passages'):
    # {'query': str,
    #  'passages': {
    #      'is_selected': List[int],
    #      'passage_text': List[str],
    #      'url': List[str]
    #  }
    # }
        passages = []
        doc_ids = []
        seen_passages = set()  # Deduplicate passages
        
        print(f"Total samples available: {len(ds):,}")
        print("Extracting unique passages...")
        
        # Extract passages from training data
        for i, row in enumerate(ds):
            if len(passages) >= num_passages:
                break
            
            # Each row has multiple passages; in this dataset, 'passages' is a dict of lists
            p = row.get('passages', {})
            # Safely obtain the aligned list of passage texts
            p_texts = p.get('passage_text', []) if isinstance(p, dict) else []

            for passage_text in p_texts:
                if len(passages) >= num_passages:
                    break

                passage_text = (passage_text or '').strip()

                # Skip empty or duplicate passages
                if not passage_text or passage_text in seen_passages:
                    continue

                seen_passages.add(passage_text)
                passages.append(passage_text)
                doc_ids.append(str(len(passages) - 1))  # Use index as doc_id
            
            # Progress indicator
            if (i + 1) % 1000 == 0:
                print(f"  Processed {i + 1:,} samples, found {len(passages):,} unique passages...")
        
        print(f"Extracted {len(passages):,} unique passages")
        
        if len(passages) == 0:
            print("Error: No passages extracted from dataset")
            sys.exit(1)
        
        # Save passages (one per line)
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
        print("  3. Try: pip install --upgrade datasets")
        print("  4. Try smaller size: --size 10000")
        print("\nAlternative: Use mock embeddings instead:")
        print("  python3 scripts/generate_mock_embeddings.py --size 100000")
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
        num_passages = sum(1 for line in f if line.strip())
    
    with open(doc_ids_file, 'r', encoding='utf-8') as f:
        num_doc_ids = sum(1 for line in f if line.strip())
    
    embeddings = np.load(embeddings_file)
    
    # Handle empty embeddings case
    if embeddings.ndim == 1:
        print("Error: Embeddings array is 1-dimensional (likely empty)")
        print(f"Shape: {embeddings.shape}")
        return False
    
    print(f"Passages: {num_passages:,}")
    print(f"Doc IDs: {num_doc_ids:,}")
    print(f"Embeddings: {embeddings.shape[0]:,} × {embeddings.shape[1]}-dim")
    
    if num_passages == 0:
        print("Error: No passages found")
        return False
    
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
            print("\nNext steps:")
            print("  1. Generate query embeddings:")
            print("     python3 scripts/generate_query_embeddings.py --size", num_passages, "--queries-per-doc 5")
            print("  2. Build validation binary:")
            print("     cargo build --release --bin validation_enterprise")
            print("  3. Run validation:")
            print("     ./target/release/validation_enterprise validation_enterprise.json")
        else:
            print("\nWarning: Data verification failed")
            sys.exit(1)
    else:
        print("\nSkipped embedding generation (--skip-embeddings)")
        print("Run again without --skip-embeddings to generate embeddings")


if __name__ == '__main__':
    main()
