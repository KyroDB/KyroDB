#!/usr/bin/env bash
set -euo pipefail

# KyroDB Large-Scale Benchmark Runner
# Runs 10M and 50M key benchmarks with uniform and Zipf distributions

# Configuration
BASE_URL=${BASE_URL:-"http://127.0.0.1:3030"}
COMMIT=$(git rev-parse --short HEAD)
RESULTS_DIR="bench/results/${COMMIT}"
VAL_BYTES=${VAL_BYTES:-64}
LOAD_CONCURRENCY=${LOAD_CONCURRENCY:-128}
READ_CONCURRENCY=${READ_CONCURRENCY:-256}
READ_SECONDS=${READ_SECONDS:-30}
WARMUP_SECONDS=${WARMUP_SECONDS:-5}

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    # Check if server is running
    if ! curl -s "${BASE_URL}/health" > /dev/null; then
        log_error "Server not running at ${BASE_URL}"
        log_info "Start server with: RUST_LOG=error KYRODB_WARM_ON_START=1 target/release/kyrodb-engine serve 127.0.0.1 3030"
        exit 1
    fi
    
    # Check if bench binary exists
    if [[ ! -f "target/release/bench" ]]; then
        log_error "Bench binary not found. Run: cargo build -p bench --release"
        exit 1
    fi
    
    # Create results directory
    mkdir -p "${RESULTS_DIR}"
    
    log_success "Prerequisites check passed"
}

# Wait for server to be ready
wait_for_server() {
    log_info "Waiting for server to be ready..."
    local max_attempts=30
    local attempt=1
    
    while [[ $attempt -le $max_attempts ]]; do
        if curl -s "${BASE_URL}/health" > /dev/null; then
            log_success "Server is ready"
            return 0
        fi
        log_info "Attempt $attempt/$max_attempts - waiting for server..."
        sleep 2
        ((attempt++))
    done
    
    log_error "Server failed to become ready after $max_attempts attempts"
    exit 1
}

# Run a single benchmark
run_benchmark() {
    local scale=$1
    local distribution=$2
    local zipf_theta=${3:-1.1}
    
    local dist_suffix=""
    local zipf_args=""
    if [[ "$distribution" == "zipf" ]]; then
        dist_suffix="_zipf_${zipf_theta}"
        zipf_args="--zipf-theta ${zipf_theta}"
    fi
    
    local output_file="${RESULTS_DIR}/http_${distribution}_${scale}${dist_suffix}.csv"
    local log_file="${RESULTS_DIR}/http_${distribution}_${scale}${dist_suffix}.log"
    
    log_info "Running ${scale} keys, ${distribution} distribution${zipf_args:+ with theta ${zipf_theta}}..."
    
    # Capture start time
    local start_time=$(date +%s)
    
    # Run benchmark
    if target/release/bench \
        --base "${BASE_URL}" \
        --load-n "${scale}" \
        --val-bytes "${VAL_BYTES}" \
        --load-concurrency "${LOAD_CONCURRENCY}" \
        --read-concurrency "${READ_CONCURRENCY}" \
        --read-seconds "${READ_SECONDS}" \
        --dist "${distribution}" \
        ${zipf_args} \
        --out-csv "${output_file}" \
        --regime warm 2>&1 | tee "${log_file}"; then
        
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        
        # Extract key metrics from log
        local rps=$(grep -o 'rps=[0-9.]*' "${log_file}" | tail -1 | cut -d'=' -f2 || echo "N/A")
        local p99=$(grep -o 'p99_us=[0-9.]*' "${log_file}" | tail -1 | cut -d'=' -f2 || echo "N/A")
        local p95=$(grep -o 'p95_us=[0-9.]*' "${log_file}" | tail -1 | cut -d'=' -f2 || echo "N/A")
        local p50=$(grep -o 'p50_us=[0-9.]*' "${log_file}" | tail -1 | cut -d'=' -f2 || echo "N/A")
        
        log_success "Benchmark completed in ${duration}s"
        log_info "Results: RPS=${rps}, P50=${p50}μs, P95=${p95}μs, P99=${p99}μs"
        
        # Save summary
        echo "${scale},${distribution},${zipf_theta},${rps},${p50},${p95},${p99},${duration}" >> "${RESULTS_DIR}/summary.csv"
        
    else
        log_error "Benchmark failed for ${scale} keys, ${distribution} distribution"
        return 1
    fi
}

# Run all benchmarks
run_all_benchmarks() {
    log_info "Starting comprehensive benchmark suite..."
    
    # Create summary file
    echo "scale,distribution,zipf_theta,rps,p50_us,p95_us,p99_us,duration_s" > "${RESULTS_DIR}/summary.csv"
    
    # Define benchmark configurations
    local benchmarks=(
        "1000000:uniform"
        "1000000:zipf:1.1"
        "1000000:zipf:1.5"
        "10000000:uniform"
        "10000000:zipf:1.1"
        "10000000:zipf:1.5"
        "50000000:uniform"
        "50000000:zipf:1.1"
        "50000000:zipf:1.5"
    )
    
    local total_benchmarks=${#benchmarks[@]}
    local current=0
    
    for benchmark in "${benchmarks[@]}"; do
        ((current++))
        IFS=':' read -r scale distribution zipf_theta <<< "${benchmark}"
        zipf_theta=${zipf_theta:-1.1}
        
        log_info "Benchmark $current/$total_benchmarks: ${scale} keys, ${distribution} distribution"
        
        if run_benchmark "${scale}" "${distribution}" "${zipf_theta}"; then
            log_success "Benchmark $current/$total_benchmarks completed successfully"
        else
            log_error "Benchmark $current/$total_benchmarks failed"
            # Continue with other benchmarks
        fi
        
        # Brief pause between benchmarks
        sleep 5
    done
}

# Generate plots
generate_plots() {
    log_info "Generating plots..."
    
    if [[ -f "comp.py" ]]; then
        python3 comp.py
        log_success "Plots generated"
    else
        log_warning "comp.py not found, skipping plot generation"
    fi
}

# Collect system info
collect_system_info() {
    log_info "Collecting system information..."
    
    {
        echo "=== System Information ==="
        echo "Date: $(date)"
        echo "Commit: ${COMMIT}"
        echo "Base URL: ${BASE_URL}"
        echo "Val bytes: ${VAL_BYTES}"
        echo "Load concurrency: ${LOAD_CONCURRENCY}"
        echo "Read concurrency: ${READ_CONCURRENCY}"
        echo "Read seconds: ${READ_SECONDS}"
        echo ""
        echo "=== Hardware ==="
        echo "CPU: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || echo 'Unknown')"
        echo "Cores: $(sysctl -n hw.ncpu 2>/dev/null || echo 'Unknown')"
        echo "Memory: $(sysctl -n hw.memsize 2>/dev/null | awk '{print $0/1024/1024/1024 " GB"}' || echo 'Unknown')"
        echo ""
        echo "=== Environment ==="
        env | grep -E '^(KYRODB_|RUST_)' || echo "No KyroDB environment variables found"
    } > "${RESULTS_DIR}/system_info.txt"
    
    log_success "System information collected"
}

# Main execution
main() {
    log_info "Starting KyroDB Large-Scale Benchmark Suite"
    log_info "Results will be saved to: ${RESULTS_DIR}"
    
    check_prerequisites
    wait_for_server
    collect_system_info
    run_all_benchmarks
    generate_plots
    
    log_success "Benchmark suite completed!"
    log_info "Results available in: ${RESULTS_DIR}"
    log_info "Summary: ${RESULTS_DIR}/summary.csv"
}

# Handle script arguments
case "${1:-}" in
    "help"|"-h"|"--help")
        echo "Usage: $0 [help|check|run|plots]"
        echo ""
        echo "Commands:"
        echo "  help    - Show this help message"
        echo "  check   - Check prerequisites only"
        echo "  run     - Run all benchmarks"
        echo "  plots   - Generate plots only"
        echo ""
        echo "Environment variables:"
        echo "  BASE_URL           - Server URL (default: http://127.0.0.1:3030)"
        echo "  VAL_BYTES          - Value size in bytes (default: 64)"
        echo "  LOAD_CONCURRENCY   - Load concurrency (default: 128)"
        echo "  READ_CONCURRENCY   - Read concurrency (default: 256)"
        echo "  READ_SECONDS       - Read duration (default: 30)"
        echo "  WARMUP_SECONDS     - Warmup duration (default: 5)"
        exit 0
        ;;
    "check")
        check_prerequisites
        exit 0
        ;;
    "run")
        main
        exit 0
        ;;
    "plots")
        generate_plots
        exit 0
        ;;
    "")
        main
        exit 0
        ;;
    *)
        log_error "Unknown command: $1"
        echo "Use '$0 help' for usage information"
        exit 1
        ;;
esac 