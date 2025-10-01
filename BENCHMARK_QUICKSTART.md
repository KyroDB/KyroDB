# KyroDB Benchmark Suite - Quick Start Guide

## ✅ System Status: READY FOR AZURE DEPLOYMENT

---

## 🎯 What We Accomplished

1. **Fixed All Compilation Errors** ✅
   - Resolved 12 AVX2 intrinsic errors
   - Code compiles on both macOS and Linux
   - SIMD optimizations working correctly

2. **Validated Benchmark Infrastructure** ✅
   - Fixed simulated workload bug
   - Real HTTP client calls working
   - 100% success rate on all operations

3. **Initial Performance Data** ✅
   - Local MacBook: ~9K ops/sec
   - P50 latency: ~2ms  
   - P99 latency: ~16ms
   - Stable under sustained load

---

## 🚀 Quick Benchmark Commands

### 1. Start Server (Correct Syntax)
```bash
# Kill any existing instances
pkill -9 -f kyrodb-engine

# Start fresh server - NOTE: --data-dir BEFORE serve subcommand
./target/release/kyrodb-engine \
    --data-dir ./data \
    serve 127.0.0.1 3030 \
    > /tmp/kyrodb.log 2>&1 &

# Wait for startup and verify
sleep 3
curl -s http://127.0.0.1:3030/v1/health
```

### 2. Run Single Test
```bash
cargo run -p kyrodb-bench --release -- \
    --phase http \
    --workload read_heavy \
    --workers 32 \
    --duration 30 \
    --key-count 100000 \
    --value-size 512 \
    --distribution zipf \
    --format csv
```

### 3. Run Complete Suite
```bash
./bench/run_complete_suite.sh all
```

---

## 📊 Expected Results

### Local Development Machine
- **Throughput:** 5-10K ops/sec
- **Latency (P50):** 1-3ms
- **Latency (P99):** 10-20ms

### Azure VM (Standard_D16s_v5)
- **Throughput:** 100-150K ops/sec
- **Latency (P50):** <500μs
- **Latency (P99):** <5ms

---

## 🔧 Azure VM Setup

### Step 1: Provision VM
```bash
az vm create \
    --resource-group kyrodb-benchmarks \
    --name kyrodb-bench-vm \
    --image Ubuntu2204 \
    --size Standard_D16s_v5 \
    --admin-username azureuser \
    --generate-ssh-keys \
    --public-ip-sku Standard \
    --os-disk-size-gb 512 \
    --data-disk-sizes-gb 1024 \
    --storage-sku Premium_LRS
```

### Step 2: System Optimization
```bash
# SSH into VM
ssh azureuser@<VM_IP>

# CPU performance mode
echo performance | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor

# Network tuning
sudo sysctl -w net.core.rmem_max=134217728
sudo sysctl -w net.core.wmem_max=134217728
sudo sysctl -w vm.swappiness=10

# File descriptors
ulimit -n 1048576
```

### Step 3: Install Dependencies
```bash
# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source $HOME/.cargo/env

# Install build tools
sudo apt update
sudo apt install -y build-essential pkg-config libssl-dev git
```

### Step 4: Clone and Build
```bash
cd /data
git clone https://github.com/vatskishan03/KyroDB.git
cd KyroDB

# Create .cargo/config.toml for maximum optimization
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

# Build (takes 25-35 minutes)
cargo build -p kyrodb-engine --release --features learned-index
cargo build -p kyrodb-bench --release
```

### Step 5: Run Benchmarks
```bash
# Start server with optimal settings
export KYRODB_WARM_ON_START=1
export KYRODB_DISABLE_HTTP_LOG=1
export KYRODB_GROUP_COMMIT_DELAY_MICROS=100
export KYRODB_FSYNC_POLICY=data
export RUST_LOG=error

./target/release/kyrodb-engine serve 0.0.0.0 3030 > /tmp/kyrodb.log 2>&1 &

# Wait for startup and verify
sleep 5
curl -s http://127.0.0.1:3030/v1/health | jq .

# Run comprehensive benchmarks
./bench/run_complete_suite.sh all

# Results will be in bench/results/
```

---

## 📈 Benchmark Phases

### Phase 1: Workload Characterization (30 min)
Tests different workload patterns:
- **Read-heavy** (95% reads): Cache/analytics workloads
- **Write-heavy** (10% reads): Logging/ingestion workloads
- **Mixed** (50/50): Balanced OLTP workloads

With distributions:
- **Uniform**: Even access across all keys
- **Zipf (skew=1.2)**: Realistic 80/20 hot/cold pattern

### Phase 2: Concurrency Scaling (20 min)
Tests with different worker counts:
- 8, 16, 32, 64, 128, 256 workers
- Finds optimal concurrency level
- Validates linear scaling

### Phase 3: Dataset Scaling (20 min)
Tests with different dataset sizes:
- 100K, 500K, 1M, 5M, 10M keys
- Validates RMI scaling behavior
- Measures memory usage

### Phase 4: RMI vs BTree Comparison (60 min)
Compares learned index vs traditional BTree:
1. Build with `--features learned-index` (RMI)
2. Run full benchmark suite
3. Build without learned-index (BTree)
4. Run same benchmarks
5. Generate comparison report

**Expected RMI Advantage:** 2-4x throughput improvement

---

## 🔍 Analyzing Results

### View Results
```bash
# Latest results directory
cd bench/results
ls -lt | head -5

# View summary
cat <latest_dir>/SUMMARY.md

# View CSV data
cat <latest_dir>/complete_results.csv | column -t -s,
```

### Key Metrics to Watch
- **Throughput (ops/sec)**: Higher is better
- **P50 Latency (μs)**: Median response time
- **P99 Latency (μs)**: 99th percentile (tail latency)
- **Success Rate**: Should be 100%

### Troubleshooting

**Low Throughput:**
```bash
# Check CPU usage
htop

# Check disk I/O
iostat -x 1

# Check CPU frequency
cat /proc/cpuinfo | grep MHz

# Verify performance mode
cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor
```

**High Latency:**
```bash
# Check for I/O wait
iostat -x 1

# Check memory pressure
free -h

# Check swap usage
cat /proc/swaps
```

**Server Crashes:**
```bash
# Check OOM killer
dmesg | grep -i "out of memory"

# Check server logs
tail -100 /tmp/kyrodb.log

# Reduce workers if needed
# Edit benchmark script: --workers 64 (instead of 128)
```

---

## 📝 Performance Targets

| Metric | Target | Measured | Status |
|--------|--------|----------|--------|
| Read Throughput (1M keys) | >100K ops/sec | TBD | 🔄 |
| Write Throughput | >50K ops/sec | TBD | 🔄 |
| P50 Latency | <500μs | TBD | 🔄 |
| P99 Latency | <5ms | TBD | 🔄 |
| RMI vs BTree Advantage | >2x | TBD | 🔄 |
| Memory Usage (1M keys) | <2GB | TBD | 🔄 |

---

## 🎯 Success Checklist

### Pre-Deployment
- [x] Code compiles without errors
- [x] Benchmarks run with real HTTP calls
- [x] Initial performance data collected
- [x] Azure deployment guide created

### Azure VM Deployment
- [ ] VM provisioned and optimized
- [ ] Code built with maximum optimization
- [ ] Server starts and responds correctly
- [ ] Benchmarks complete successfully

### Analysis
- [ ] Results documented
- [ ] RMI vs BTree comparison complete
- [ ] Performance targets met or explained
- [ ] Optimization opportunities identified

### Documentation
- [ ] Update README with performance numbers
- [ ] Add to Implementation.md (Phase 0 completion)
- [ ] Create performance tuning guide
- [ ] Archive results for historical tracking

---

## 🚀 You're Ready!

Everything is set up and tested. The code is ready for production-scale benchmarking on Azure VM.

**Next Step:** Deploy to Azure VM and run comprehensive benchmarks!

**Questions?** Review `BENCHMARK_STATUS.md` for detailed technical information.

---

**Good luck with the Azure benchmarks!** 🎉
