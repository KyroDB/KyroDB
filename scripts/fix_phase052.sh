#!/bin/bash
# Quick fix for Phase 0.5.2 on Azure VM
# Run this from KyroDB root directory

echo "🔧 Phase 0.5.2 Quick Fix"
echo "========================"
echo ""

# Check if in right directory
if [ ! -f "Cargo.toml" ]; then
    echo "❌ ERROR: Run from KyroDB root directory (cd ~/KyroDB)"
    exit 1
fi

# Check if document embeddings exist
if [ ! -f "data/ms_marco/embeddings_100k.npy" ]; then
    echo "❌ ERROR: Document embeddings not found"
    echo "Run first: python3 scripts/generate_mock_embeddings.py --size 10000"
    exit 1
fi

echo "📊 Generating query embeddings..."
python3 scripts/generate_query_embeddings.py --size 10000 --queries-per-doc 5 --noise 0.02

if [ $? -ne 0 ]; then
    echo ""
    echo "❌ Failed to generate query embeddings"
    echo ""
    echo "Possible fixes:"
    echo "  1. Activate venv: source kyro-venv/bin/activate"
    echo "  2. Install numpy: pip install numpy"
    exit 1
fi

echo ""
echo "🔨 Rebuilding validation binary..."
cargo build --release --bin validation_enterprise >/dev/null 2>&1

if [ $? -ne 0 ]; then
    echo "❌ Build failed"
    exit 1
fi

echo ""
echo "✅ Setup complete!"
echo ""
echo "Now run:"
echo "  ./target/release/validation_enterprise validation_phase052_smoke.json"
echo ""
