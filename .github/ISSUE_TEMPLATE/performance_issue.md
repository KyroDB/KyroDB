---
name: Performance Issue
about: Report performance problems or regressions
title: "[PERF] "
labels: ["performance", "triage"]
assignees: []

---

## Performance Issue Description
Describe the performance problem you're experiencing.

## Benchmark Results
Please include relevant benchmark data:

### Current Performance
```
# Operation: [e.g., GET /v1/get_fast/123]
# Throughput: [ops/sec]
# Latency P50: [ms]
# Latency P95: [ms]
# Latency P99: [ms]
```

### Expected Performance
What performance do you expect?

## Test Setup
- **Dataset Size**: [e.g., 1M keys, 64B values]
- **Workload**: [e.g., 100% reads, uniform distribution]
- **Concurrency**: [e.g., 64 concurrent clients]
- **Duration**: [e.g., 30 seconds]
- **Hardware**: [CPU, RAM, Storage type]

## Environment Details
- **OS**: [e.g., Linux 6.1]
- **KyroDB Version**: [e.g., 0.1.0]
- **Build Options**: [e.g., --release, SIMD features]
- **Configuration**: Key environment variables

## Profiling Data
If available, include:
- CPU profiles
- Memory usage
- I/O statistics
- System metrics

## Steps to Reproduce
1. Setup with specific configuration
2. Load data
3. Run benchmark
4. Observe performance metrics

## Additional Context
- Is this a regression from previous versions?
- Any specific data patterns that affect performance?
- Expected vs actual resource usage?

## Checklist
- [ ] Performance data is reproducible
- [ ] Environment details are complete
- [ ] Benchmark methodology is sound
