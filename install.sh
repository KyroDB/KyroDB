#!/usr/bin/env bash
# filepath: /Users/kishan/Desktop/Codes/Project/ProjectKyro/install.sh
set -euo pipefail

# 🚀 KyroDB Installation Script
# Installs KyroDB engine with optimal configuration for production use

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# Installation configuration
KYRODB_VERSION="${KYRODB_VERSION:-latest}"
INSTALL_DIR="${INSTALL_DIR:-/usr/local/bin}"
DATA_DIR="${KYRODB_DATA_DIR:-/var/lib/kyrodb}"
CONFIG_DIR="${KYRODB_CONFIG_DIR:-/etc/kyrodb}"
LOG_DIR="${KYRODB_LOG_DIR:-/var/log/kyrodb}"
USER="${KYRODB_USER:-kyrodb}"
GROUP="${KYRODB_GROUP:-kyrodb}"

# Performance optimizations
CPU_CORES=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo "4")
MEMORY_GB=$(free -g 2>/dev/null | awk '/^Mem:/{print $2}' || echo "8")

echo -e "${BOLD}${BLUE}🚀 KyroDB High-Performance Database Engine Installer${NC}"
echo -e "${BLUE}====================================================${NC}"
echo -e "Version: ${KYRODB_VERSION}"
echo -e "Install directory: ${INSTALL_DIR}"
echo -e "Data directory: ${DATA_DIR}"
echo -e "System: $(uname -s) $(uname -m)"
echo -e "CPU cores: ${CPU_CORES}"
echo -e "Memory: ${MEMORY_GB}GB"
echo ""

# Function: Check if running as root when needed
check_root() {
    if [[ $EUID -eq 0 ]]; then
        echo -e "${YELLOW}⚠️  Running as root - will install system-wide${NC}"
        return 0
    else
        echo -e "${YELLOW}ℹ️  Running as user - will install to user directories${NC}"
        INSTALL_DIR="${HOME}/.local/bin"
        DATA_DIR="${HOME}/.local/share/kyrodb"
        CONFIG_DIR="${HOME}/.config/kyrodb"
        LOG_DIR="${HOME}/.local/share/kyrodb/logs"
        return 1
    fi
}

# Function: Install system dependencies
install_dependencies() {
    echo -e "${YELLOW}📦 Installing system dependencies...${NC}"
    
    if command -v apt-get >/dev/null 2>&1; then
        # Ubuntu/Debian
        if check_root; then
            apt-get update
            apt-get install -y curl wget build-essential pkg-config libssl-dev
        else
            echo -e "${YELLOW}ℹ️  Skipping system packages (requires root). Ensure you have: curl, wget, build-essential, pkg-config, libssl-dev${NC}"
        fi
    elif command -v yum >/dev/null 2>&1; then
        # RHEL/CentOS
        if check_root; then
            yum update -y
            yum groupinstall -y "Development Tools"
            yum install -y curl wget openssl-devel pkg-config
        else
            echo -e "${YELLOW}ℹ️  Skipping system packages (requires root). Ensure you have development tools installed${NC}"
        fi
    elif command -v brew >/dev/null 2>&1; then
        # macOS
        brew install curl wget pkg-config openssl
    else
        echo -e "${YELLOW}⚠️  Unknown package manager. Please ensure curl, wget, and build tools are installed${NC}"
    fi
}

# Function: Install or update Rust
install_rust() {
    echo -e "${YELLOW}🦀 Setting up Rust toolchain...${NC}"
    
    if command -v rustc >/dev/null 2>&1; then
        local rust_version=$(rustc --version | cut -d' ' -f2)
        echo -e "${GREEN}✅ Rust already installed: ${rust_version}${NC}"
        
        # Check if version is recent enough
        if rustc --version | grep -E "(1\.[7-9][0-9]|1\.[8-9][0-9]|[2-9]\.)"; then
            echo -e "${GREEN}✅ Rust version is compatible${NC}"
        else
            echo -e "${YELLOW}⬆️  Updating Rust to latest version...${NC}"
            rustup update stable
        fi
    else
        echo -e "${YELLOW}📥 Installing Rust...${NC}"
        curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable
        source "${HOME}/.cargo/env"
    fi
    
    # Ensure we have the latest stable
    rustup default stable
    rustup update
    
    # Add useful components
    rustup component add clippy rustfmt
    
    echo -e "${GREEN}✅ Rust toolchain ready: $(rustc --version)${NC}"
}

# Function: Create system user and directories
setup_system() {
    echo -e "${YELLOW}🏗️  Setting up system directories...${NC}"
    
    # Create user if running as root and user doesn't exist
    if check_root && ! id "$USER" >/dev/null 2>&1; then
        echo -e "${YELLOW}👤 Creating kyrodb user...${NC}"
        if command -v useradd >/dev/null 2>&1; then
            useradd --system --no-create-home --shell /bin/false "$USER"
        elif command -v adduser >/dev/null 2>&1; then
            adduser --system --no-create-home --shell /bin/false "$USER"
        fi
    fi
    
    # Create directories
    echo -e "${YELLOW}📁 Creating directories...${NC}"
    mkdir -p "$INSTALL_DIR"
    mkdir -p "$DATA_DIR"
    mkdir -p "$CONFIG_DIR"
    mkdir -p "$LOG_DIR"
    
    # Set permissions
    if check_root; then
        chown -R "$USER:$GROUP" "$DATA_DIR" "$LOG_DIR" 2>/dev/null || true
        chmod 755 "$DATA_DIR" "$LOG_DIR"
        chmod 644 "$CONFIG_DIR" 2>/dev/null || true
    fi
    
    echo -e "${GREEN}✅ System setup complete${NC}"
}

# Function: Build KyroDB from source
build_kyrodb() {
    echo -e "${YELLOW}🔨 Building KyroDB from source...${NC}"
    
    local build_dir
    if [[ -d "$(pwd)/Cargo.toml" ]] && grep -q "kyrodb" "$(pwd)/Cargo.toml"; then
        # We're already in KyroDB directory
        build_dir="$(pwd)"
        echo -e "${GREEN}✅ Using current directory: ${build_dir}${NC}"
    else
        # Clone repository
        build_dir="/tmp/kyrodb-build-$$"
        echo -e "${YELLOW}📥 Cloning KyroDB repository...${NC}"
        git clone https://github.com/vatskishan03/KyroDB.git "$build_dir"
        cd "$build_dir"
    fi
    
    # Detect optimal build features based on CPU
    local cpu_features=""
    if grep -q "avx512" /proc/cpuinfo 2>/dev/null; then
        cpu_features="simd-avx512"
        echo -e "${GREEN}🚀 AVX512 detected - enabling advanced SIMD optimizations${NC}"
    elif grep -q "avx2" /proc/cpuinfo 2>/dev/null; then
        cpu_features="simd-avx2"
        echo -e "${GREEN}🚀 AVX2 detected - enabling SIMD optimizations${NC}"
    else
        echo -e "${YELLOW}ℹ️  No advanced SIMD support detected${NC}"
    fi
    
    # Set optimal build environment
    export CARGO_TARGET_DIR="${build_dir}/target"
    export RUSTFLAGS="-C target-cpu=native -C opt-level=3 -C lto=fat"
    
    # Build with optimal features
    local build_features="learned-index"
    if [[ -n "$cpu_features" ]]; then
        build_features="${build_features},${cpu_features}"
    fi
    
    echo -e "${YELLOW}⚡ Building KyroDB engine with features: ${build_features}${NC}"
    cargo build -p kyrodb-engine --release --features "$build_features"
    
    echo -e "${YELLOW}⚡ Building benchmark tools...${NC}"
    cargo build -p bench --release
    
    # Install binaries
    echo -e "${YELLOW}📦 Installing binaries...${NC}"
    cp "${build_dir}/target/release/kyrodb-engine" "$INSTALL_DIR/"
    cp "${build_dir}/target/release/bench" "$INSTALL_DIR/kyrodb-bench"
    
    # Make executable
    chmod +x "$INSTALL_DIR/kyrodb-engine"
    chmod +x "$INSTALL_DIR/kyrodb-bench"
    
    # Cleanup temporary build directory
    if [[ "$build_dir" == "/tmp/kyrodb-build-"* ]]; then
        rm -rf "$build_dir"
    fi
    
    echo -e "${GREEN}✅ KyroDB built and installed successfully${NC}"
}

# Function: Generate optimal configuration
generate_config() {
    echo -e "${YELLOW}⚙️  Generating optimal configuration...${NC}"
    
    # Calculate optimal settings based on system resources
    local wal_max_bytes=$((MEMORY_GB * 1024 * 1024 * 1024 / 4))  # 25% of RAM
    local rmi_target_leaf=$((CPU_CORES * 512))  # Scale with CPU cores
    local group_commit_delay=$((CPU_CORES > 8 ? 50 : 100))  # Faster on high-core systems
    
    cat > "$CONFIG_DIR/kyrodb.toml" << EOF
# KyroDB Configuration - Optimized for ${CPU_CORES} cores, ${MEMORY_GB}GB RAM
# Generated: $(date)

[server]
host = "127.0.0.1"
port = 3030
data_dir = "$DATA_DIR"

[performance]
# WAL settings
wal_max_bytes = $wal_max_bytes
wal_segment_bytes = 268435456  # 256MB segments
group_commit_delay_micros = $group_commit_delay

# RMI optimization
rmi_target_leaf = $rmi_target_leaf
rmi_rebuild_appends = 100000
rmi_rebuild_ratio = 0.1

# Background operations
auto_snapshot_secs = 3600  # Hourly snapshots
compact_interval_secs = 1800  # 30min compaction check

[security]
# Authentication disabled by default for performance
# Uncomment and set tokens for production:
# auth_token = "your-read-write-token"
# admin_token = "your-admin-token"

[logging]
level = "info"
log_dir = "$LOG_DIR"

[features]
# Performance features
warm_on_start = true
benchmark_mode = false  # Set to true for benchmarking

# Rate limiting (disabled for maximum performance)
enable_rate_limiting = false
EOF

    echo -e "${GREEN}✅ Configuration generated: ${CONFIG_DIR}/kyrodb.toml${NC}"
}

# Function: Create systemd service (Linux only)
create_systemd_service() {
    if [[ "$(uname -s)" != "Linux" ]] || ! check_root; then
        return 0
    fi
    
    echo -e "${YELLOW}🔧 Creating systemd service...${NC}"
    
    cat > /etc/systemd/system/kyrodb.service << EOF
[Unit]
Description=KyroDB High-Performance Database Engine
Documentation=https://github.com/vatskishan03/KyroDB
After=network.target
Wants=network.target

[Service]
Type=exec
User=$USER
Group=$GROUP
ExecStart=$INSTALL_DIR/kyrodb-engine serve 127.0.0.1 3030 \\
    --data-dir $DATA_DIR \\
    --auto-snapshot-secs 3600 \\
    --wal-max-bytes $((MEMORY_GB * 1024 * 1024 * 1024 / 4)) \\
    --rmi-rebuild-appends 100000 \\
    --compact-interval-secs 1800
ExecReload=/bin/kill -HUP \$MAINPID
KillMode=mixed
Restart=always
RestartSec=5

# Performance optimizations
LimitNOFILE=1048576
LimitNPROC=1048576

# Security hardening
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=$DATA_DIR $LOG_DIR

# Environment
Environment=RUST_LOG=info
Environment=KYRODB_WARM_ON_START=1
Environment=KYRODB_FSYNC_POLICY=data

[Install]
WantedBy=multi-user.target
EOF

    systemctl daemon-reload
    systemctl enable kyrodb
    
    echo -e "${GREEN}✅ Systemd service created and enabled${NC}"
}

# Function: Create performance tuning script
create_performance_script() {
    echo -e "${YELLOW}⚡ Creating performance optimization script...${NC}"
    
    cat > "$INSTALL_DIR/kyrodb-tune" << 'EOF'
#!/bin/bash
# KyroDB Performance Tuning Script

set -euo pipefail

echo "🚀 KyroDB Performance Tuning"
echo "=========================="

# Check if running as root for system-level optimizations
if [[ $EUID -eq 0 ]]; then
    echo "⚡ Applying system-level optimizations..."
    
    # Increase file descriptor limits
    echo "kyrodb soft nofile 1048576" >> /etc/security/limits.conf
    echo "kyrodb hard nofile 1048576" >> /etc/security/limits.conf
    
    # TCP optimizations for high concurrency
    echo 'net.core.somaxconn = 65535' >> /etc/sysctl.conf
    echo 'net.core.netdev_max_backlog = 5000' >> /etc/sysctl.conf
    echo 'net.ipv4.tcp_max_syn_backlog = 65535' >> /etc/sysctl.conf
    sysctl -p
    
    # Set CPU governor to performance
    if [[ -f /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor ]]; then
        echo performance | tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor > /dev/null
        echo "✅ CPU governor set to performance"
    fi
    
    echo "✅ System-level optimizations applied"
else
    echo "ℹ️  Run as root for system-level optimizations"
fi

# User-level optimizations
echo "⚙️  User-level optimizations..."

# Set ulimits for current session
ulimit -n 1048576 2>/dev/null || echo "⚠️  Could not set file descriptor limit"

echo "✅ Performance tuning complete"
echo ""
echo "🎯 For maximum performance:"
echo "  • Use SSD storage for data directory"
echo "  • Ensure adequate RAM (8GB+ recommended)"
echo "  • Consider CPU pinning for dedicated servers"
echo "  • Monitor with: kyrodb-bench and /metrics endpoint"
EOF

    chmod +x "$INSTALL_DIR/kyrodb-tune"
    echo -e "${GREEN}✅ Performance tuning script created${NC}"
}

# Function: Create quick start script
create_quick_start() {
    echo -e "${YELLOW}📝 Creating quick start script...${NC}"
    
    cat > "$INSTALL_DIR/kyrodb-quickstart" << EOF
#!/bin/bash
# KyroDB Quick Start Script

set -euo pipefail

BLUE='\033[0;34m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "\${BLUE}🚀 KyroDB Quick Start\${NC}"
echo -e "\${BLUE}==================\${NC}"

# Check if KyroDB is running
if pgrep -f kyrodb-engine > /dev/null; then
    echo -e "\${GREEN}✅ KyroDB is already running\${NC}"
    PID=\$(pgrep -f kyrodb-engine)
    echo -e "   Process ID: \${PID}"
    echo -e "   Listening on: http://127.0.0.1:3030"
else
    echo -e "\${YELLOW}🚀 Starting KyroDB...\${NC}"
    
    # Ensure data directory exists
    mkdir -p "$DATA_DIR"
    
    # Start KyroDB in background
    nohup "$INSTALL_DIR/kyrodb-engine" serve 127.0.0.1 3030 \\
        --data-dir "$DATA_DIR" \\
        --auto-snapshot-secs 3600 \\
        --wal-max-bytes $((MEMORY_GB * 1024 * 1024 * 1024 / 4)) \\
        --rmi-rebuild-appends 100000 > "$LOG_DIR/kyrodb.log" 2>&1 &
    
    # Wait for startup
    echo -e "\${YELLOW}⏳ Waiting for startup...\${NC}"
    for i in {1..30}; do
        if curl -s http://127.0.0.1:3030/health > /dev/null 2>&1; then
            echo -e "\${GREEN}✅ KyroDB started successfully!\${NC}"
            break
        fi
        sleep 1
    done
fi

echo ""
echo -e "\${GREEN}📚 Quick Examples:\${NC}"
echo ""
echo -e "\${YELLOW}# Store a key-value pair:\${NC}"
echo "curl -X POST http://127.0.0.1:3030/v1/put -H 'Content-Type: application/json' -d '{\"key\": 123, \"value\": \"hello world\"}'"
echo ""
echo -e "\${YELLOW}# Retrieve a value:\${NC}"
echo "curl 'http://127.0.0.1:3030/v1/lookup?key=123'"
echo ""
echo -e "\${YELLOW}# High-performance binary API:\${NC}"
echo "curl -X POST http://127.0.0.1:3030/v1/put_fast/456 --data-binary 'binary data'"
echo "curl http://127.0.0.1:3030/v1/get_fast/456"
echo ""
echo -e "\${YELLOW}# Health check:\${NC}"
echo "curl http://127.0.0.1:3030/health"
echo ""
echo -e "\${YELLOW}# Performance metrics:\${NC}"
echo "curl http://127.0.0.1:3030/metrics"
echo ""
echo -e "\${GREEN}🎯 Performance Testing:\${NC}"
echo "kyrodb-bench --base http://127.0.0.1:3030 --load-n 100000 --read-seconds 30"
echo ""
echo -e "\${GREEN}📋 Useful Commands:\${NC}"
echo "• View logs: tail -f $LOG_DIR/kyrodb.log"
echo "• Stop server: pkill kyrodb-engine"
echo "• Performance tuning: kyrodb-tune"
if check_root; then
echo "• System service: systemctl start kyrodb"
fi
EOF

    chmod +x "$INSTALL_DIR/kyrodb-quickstart"
    echo -e "${GREEN}✅ Quick start script created${NC}"
}

# Function: Verify installation
verify_installation() {
    echo -e "${YELLOW}🔍 Verifying installation...${NC}"
    
    # Check binaries
    if [[ -x "$INSTALL_DIR/kyrodb-engine" ]]; then
        echo -e "${GREEN}✅ kyrodb-engine binary installed${NC}"
        echo -e "   Version: $($INSTALL_DIR/kyrodb-engine --version 2>/dev/null || echo 'Unknown')"
    else
        echo -e "${RED}❌ kyrodb-engine binary not found${NC}"
        return 1
    fi
    
    if [[ -x "$INSTALL_DIR/kyrodb-bench" ]]; then
        echo -e "${GREEN}✅ kyrodb-bench binary installed${NC}"
    else
        echo -e "${RED}❌ kyrodb-bench binary not found${NC}"
        return 1
    fi
    
    # Check directories
    for dir in "$DATA_DIR" "$CONFIG_DIR" "$LOG_DIR"; do
        if [[ -d "$dir" ]]; then
            echo -e "${GREEN}✅ Directory exists: $dir${NC}"
        else
            echo -e "${RED}❌ Directory missing: $dir${NC}"
            return 1
        fi
    done
    
    # Check configuration
    if [[ -f "$CONFIG_DIR/kyrodb.toml" ]]; then
        echo -e "${GREEN}✅ Configuration file created${NC}"
    else
        echo -e "${YELLOW}⚠️  Configuration file missing${NC}"
    fi
    
    echo -e "${GREEN}✅ Installation verification complete${NC}"
}

# Function: Display completion message
display_completion() {
    echo ""
    echo -e "${BOLD}${GREEN}🎉 KyroDB Installation Complete!${NC}"
    echo -e "${GREEN}=================================${NC}"
    echo ""
    echo -e "${BOLD}📍 Installation Summary:${NC}"
    echo -e "   • Binaries: ${INSTALL_DIR}/kyrodb-engine, ${INSTALL_DIR}/kyrodb-bench"
    echo -e "   • Data directory: ${DATA_DIR}"
    echo -e "   • Configuration: ${CONFIG_DIR}/kyrodb.toml"
    echo -e "   • Logs: ${LOG_DIR}"
    echo ""
    echo -e "${BOLD}🚀 Quick Start:${NC}"
    echo -e "   ${YELLOW}kyrodb-quickstart${NC}  # Start KyroDB and see examples"
    echo ""
    echo -e "${BOLD}🎯 Performance:${NC}"
    echo -e "   ${YELLOW}kyrodb-tune${NC}        # Apply performance optimizations"
    echo -e "   ${YELLOW}kyrodb-bench --base http://127.0.0.1:3030 --load-n 100000${NC}"
    echo ""
    if check_root; then
        echo -e "${BOLD}🔧 System Service:${NC}"
        echo -e "   ${YELLOW}systemctl start kyrodb${NC}   # Start as system service"
        echo -e "   ${YELLOW}systemctl enable kyrodb${NC}  # Enable auto-start"
        echo ""
    fi
    echo -e "${BOLD}📚 Resources:${NC}"
    echo -e "   • Documentation: https://github.com/vatskishan03/KyroDB"
    echo -e "   • Health check: http://127.0.0.1:3030/health"
    echo -e "   • Metrics: http://127.0.0.1:3030/metrics"
    echo ""
    echo -e "${GREEN}🚀 KyroDB is ready for high-performance AI workloads!${NC}"
}

# Main installation flow
main() {
    echo -e "${YELLOW}🔍 Checking system requirements...${NC}"
    
    # Minimum system checks
    if [[ $(uname -m) != "x86_64" && $(uname -m) != "aarch64" ]]; then
        echo -e "${RED}❌ Unsupported architecture: $(uname -m)${NC}"
        exit 1
    fi
    
    if [[ $MEMORY_GB -lt 2 ]]; then
        echo -e "${YELLOW}⚠️  Low memory detected (${MEMORY_GB}GB). 4GB+ recommended for optimal performance${NC}"
    fi
    
    # Execute installation steps
    check_root
    install_dependencies
    install_rust
    setup_system
    build_kyrodb
    generate_config
    create_systemd_service
    create_performance_script
    create_quick_start
    verify_installation
    display_completion
}

# Handle Ctrl+C gracefully
trap 'echo -e "\n${RED}❌ Installation interrupted${NC}"; exit 1' INT

# Check for help flag
if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
    echo "KyroDB Installation Script"
    echo ""
    echo "Environment variables:"
    echo "  KYRODB_VERSION       Version to install (default: latest)"
    echo "  INSTALL_DIR          Binary installation directory"
    echo "  KYRODB_DATA_DIR      Data directory"
    echo "  KYRODB_CONFIG_DIR    Configuration directory"
    echo "  KYRODB_LOG_DIR       Log directory"
    echo "  KYRODB_USER          System user (when running as root)"
    echo ""
    echo "Usage: bash install.sh"
    exit 0
fi

# Run main installation
main "$@"