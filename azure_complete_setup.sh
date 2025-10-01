#!/usr/bin/env bash
# Complete Azure VM Setup and Benchmark Execution
# Copy-paste this entire script on your Azure VM

set -euo pipefail

echo "═══════════════════════════════════════════════════"
echo "  KyroDB Azure VM - Complete Setup & Benchmark"
echo "═══════════════════════════════════════════════════"
echo ""

# Step 1: System Optimization
echo "Step 1: Optimizing system..."
echo performance | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor > /dev/null
sudo sysctl -w net.core.rmem_max=134217728 > /dev/null
sudo sysctl -w net.core.wmem_max=134217728 > /dev/null
sudo sysctl -w vm.swappiness=10 > /dev/null
ulimit -n 1048576
echo "✅ System optimized"
echo ""

# Step 2: Install dependencies
echo "Step 2: Installing dependencies..."
if ! command -v rustc &> /dev/null; then
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    source $HOME/.cargo/env
    echo "✅ Rust installed"
else
    echo "✅ Rust already installed"
fi

sudo apt update -qq
sudo apt install -y build-essential pkg-config libssl-dev git jq > /dev/null 2>&1
echo "✅ Build tools installed"
echo ""

# Step 3: Clone repository
echo "Step 3: Cloning KyroDB repository..."
if [[ ! -d /data ]]; then
    sudo mkdir -p /data
    sudo chown -R $USER:$USER /data
fi

cd /data
if [[ ! -d KyroDB ]]; then
    git clone https://github.com/vatskishan03/KyroDB.git
    echo "✅ Repository cloned"
else
    echo "✅ Repository already exists"
    cd KyroDB
    git pull
fi
cd KyroDB
echo ""

# Step 4: Configure build
echo "Step 4: Configuring optimized build..."
mkdir -p .cargo
cat > .cargo/config.toml << 'EOF'
[profile.release]
opt-level = 3
lto = "fat"
codegen-units = 1
panic = "abort"
strip = true

[build]
rustflags = ["-C", "target-cpu=native"]
EOF
echo "✅ Build configuration created"
echo ""

# Step 5: Build binaries
echo "Step 5: Building KyroDB (this takes 25-35 minutes)..."
echo "Started at: $(date)"
cargo build -p kyrodb-engine --release --features learned-index
cargo build -p kyrodb-bench --release
echo "✅ Build complete at: $(date)"
echo ""

# Step 6: Prepare data directory
echo "Step 6: Preparing data directory..."
mkdir -p /data/kyrodb_data
chmod 755 /data/kyrodb_data
echo "✅ Data directory ready"
echo ""

# Step 7: Set environment variables
echo "Step 7: Setting environment variables..."
export KYRODB_WARM_ON_START=1
export KYRODB_DISABLE_HTTP_LOG=1
export KYRODB_GROUP_COMMIT_DELAY_MICROS=100
export KYRODB_FSYNC_POLICY=data
export KYRODB_RMI_TARGET_LEAF=2048
export KYRODB_USE_ADAPTIVE_RMI=true
export RUST_LOG=error
echo "✅ Environment configured"
echo ""

# Step 8: Start server
echo "Step 8: Starting KyroDB server..."
./target/release/kyrodb-engine \
    --data-dir /data/kyrodb_data \
    serve 0.0.0.0 3030 \
    > /tmp/kyrodb_server.log 2>&1 &

SERVER_PID=$!
echo $SERVER_PID > /tmp/kyrodb.pid
echo "Server PID: $SERVER_PID"
echo ""

# Step 9: Wait for server startup
echo "Step 9: Waiting for server to become healthy..."
MAX_RETRIES=15
RETRY=0

while [[ $RETRY -lt $MAX_RETRIES ]]; do
    sleep 2
    
    if curl -sf http://127.0.0.1:3030/v1/health > /dev/null 2>&1; then
        echo "✅ Server is healthy!"
        break
    fi
    
    RETRY=$((RETRY + 1))
    echo "  Attempt $RETRY/$MAX_RETRIES..."
done

if [[ $RETRY -eq $MAX_RETRIES ]]; then
    echo "❌ Server failed to start. Check logs:"
    tail -50 /tmp/kyrodb_server.log
    exit 1
fi
echo ""

# Step 10: Verify server
echo "Step 10: Verifying server configuration..."
echo "Health check:"
curl -s http://127.0.0.1:3030/v1/health | jq .
echo ""
echo "Build info:"
curl -s http://127.0.0.1:3030/build_info | jq .
echo ""

# Step 11: Run benchmarks
echo "═══════════════════════════════════════════════════"
echo "  Starting Comprehensive Benchmark Suite"
echo "═══════════════════════════════════════════════════"
echo ""

./bench/run_complete_suite.sh all

BENCHMARK_EXIT=$?

echo ""
echo "═══════════════════════════════════════════════════"
if [[ $BENCHMARK_EXIT -eq 0 ]]; then
    echo "  ✅ BENCHMARKS COMPLETED SUCCESSFULLY!"
else
    echo "  ❌ Benchmarks failed (exit code: $BENCHMARK_EXIT)"
fi
echo "═══════════════════════════════════════════════════"
echo ""

# Show results
LATEST_RESULT=$(ls -td bench/results/complete_* 2>/dev/null | head -1)
if [[ -n "$LATEST_RESULT" ]]; then
    echo "Results saved to: $LATEST_RESULT"
    echo ""
    
    if [[ -f "$LATEST_RESULT/SUMMARY.md" ]]; then
        echo "Summary:"
        cat "$LATEST_RESULT/SUMMARY.md"
    fi
fi

echo ""
echo "Server is still running. To stop:"
echo "  kill $(cat /tmp/kyrodb.pid)"
echo ""
echo "To view server logs:"
echo "  tail -f /tmp/kyrodb_server.log"

exit $BENCHMARK_EXIT
