#!/bin/bash
# Phase 0.5.2 Setup and Diagnostic Script
# Checks for required files and generates missing query embeddings

set -e

echo "=========================================="
echo "Phase 0.5.2 Setup - Query Embeddings"
echo "=========================================="
echo ""

# Check if we're in the right directory
if [ ! -f "Cargo.toml" ]; then
    echo "ERROR: Must run from KyroDB root directory"
    exit 1
fi

# Check data directory
echo "1. Checking data directory structure..."
if [ ! -d "data/ms_marco" ]; then
    echo "   Creating data/ms_marco directory..."
    mkdir -p data/ms_marco
fi
echo "   ✓ data/ms_marco exists"
echo ""

# Check document embeddings
echo "2. Checking document embeddings..."
if [ -f "data/ms_marco/embeddings_100k.npy" ]; then
    SIZE=$(du -h data/ms_marco/embeddings_100k.npy | cut -f1)
    echo "   ✓ embeddings_100k.npy exists ($SIZE)"
else
    echo "   ✗ embeddings_100k.npy NOT FOUND"
    echo "   Run: python3 scripts/generate_mock_embeddings.py --size 10000"
    exit 1
fi

if [ -f "data/ms_marco/passages_100k.txt" ]; then
    LINES=$(wc -l < data/ms_marco/passages_100k.txt)
    echo "   ✓ passages_100k.txt exists ($LINES lines)"
else
    echo "   ✗ passages_100k.txt NOT FOUND"
    exit 1
fi
echo ""

# Check query embeddings
echo "3. Checking query embeddings..."
if [ -f "data/ms_marco/query_embeddings_100k.npy" ]; then
    SIZE=$(du -h data/ms_marco/query_embeddings_100k.npy | cut -f1)
    echo "   ✓ query_embeddings_100k.npy exists ($SIZE)"
    QUERY_EXISTS=1
else
    echo "   ✗ query_embeddings_100k.npy NOT FOUND"
    QUERY_EXISTS=0
fi

if [ -f "data/ms_marco/queries_100k.txt" ]; then
    LINES=$(wc -l < data/ms_marco/queries_100k.txt)
    echo "   ✓ queries_100k.txt exists ($LINES lines)"
else
    echo "   ✗ queries_100k.txt NOT FOUND"
    QUERY_EXISTS=0
fi

if [ -f "data/ms_marco/query_to_doc.txt" ]; then
    LINES=$(wc -l < data/ms_marco/query_to_doc.txt)
    echo "   ✓ query_to_doc.txt exists ($LINES lines)"
else
    echo "   ✗ query_to_doc.txt NOT FOUND"
    QUERY_EXISTS=0
fi
echo ""

# Generate query embeddings if missing
if [ $QUERY_EXISTS -eq 0 ]; then
    echo "4. Generating query embeddings..."
    echo "   This will take ~10 seconds..."
    python3 scripts/generate_query_embeddings.py --size 10000 --queries-per-doc 5 --noise 0.02
    
    if [ $? -eq 0 ]; then
        echo "   ✓ Query embeddings generated successfully"
    else
        echo "   ✗ Failed to generate query embeddings"
        exit 1
    fi
else
    echo "4. Query embeddings already exist"
fi
echo ""

# Verify all files
echo "5. Final verification..."
FILES=(
    "data/ms_marco/embeddings_100k.npy"
    "data/ms_marco/passages_100k.txt"
    "data/ms_marco/query_embeddings_100k.npy"
    "data/ms_marco/queries_100k.txt"
    "data/ms_marco/query_to_doc.txt"
)

ALL_GOOD=1
for file in "${FILES[@]}"; do
    if [ -f "$file" ]; then
        SIZE=$(du -h "$file" | cut -f1)
        echo "   ✓ $file ($SIZE)"
    else
        echo "   ✗ $file MISSING"
        ALL_GOOD=0
    fi
done
echo ""

if [ $ALL_GOOD -eq 1 ]; then
    echo "=========================================="
    echo "✓ All files ready for Phase 0.5.2!"
    echo "=========================================="
    echo ""
    echo "You can now run:"
    echo "  ./target/release/validation_enterprise validation_phase052_smoke.json"
    echo ""
else
    echo "=========================================="
    echo "✗ Some files are missing"
    echo "=========================================="
    exit 1
fi
