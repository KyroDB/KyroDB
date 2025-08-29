# Changelog

All notable changes to KyroDB will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Initial public release of KyroDB
- Core KV storage with durable WAL + snapshots
- Learned index (RMI) implementation with SIMD probe optimization
- HTTP API with JSON and binary endpoints
- Prometheus metrics integration
- Comprehensive benchmarking suite
- Go CLI orchestrator for database management
- Fuzzing targets for critical components
- Docker support with docker-compose

### Changed
- N/A (initial release)

### Deprecated
- N/A

### Removed
- N/A

### Fixed
- N/A

### Security
- N/A

## [0.1.0] - 2025-01-XX

### Added
- **Core Engine**: High-performance Rust-based database engine
- **Durability**: WAL (Write-Ahead Log) with configurable fsync policies
- **Snapshots**: Atomic snapshot creation with crash recovery
- **RMI Index**: Learned index implementation with 2-stage linear models
- **SIMD Support**: AVX2/AVX-512/NEON optimizations for probe operations
- **HTTP API**: RESTful API with JSON and octet-stream endpoints
- **Vector Search**: Optional HNSW-based ANN vector search
- **Metrics**: Prometheus integration with comprehensive metrics
- **Rate Limiting**: Configurable per-IP rate limits
- **Authentication**: Bearer token authentication support
- **CLI Tools**: Go-based orchestrator for database operations
- **Benchmarking**: Comprehensive benchmarking suite with automated plotting
- **Testing**: Extensive test coverage including fuzzing and chaos testing
- **Docker**: Containerized deployment with docker-compose

### Technical Features
- **Performance**: Sub-millisecond p99 latency for point lookups
- **Scalability**: Handles millions of keys with stable performance
- **Reliability**: Crash-safe with atomic operations and recovery
- **Observability**: Detailed metrics and health endpoints
- **Developer Experience**: Comprehensive documentation and examples

### Known Limitations
- Single-node architecture (clustering planned for future releases)
- No multi-key transactions (ACID transactions planned)
- Limited SQL dialect (KV-focused operations only)

---

## Release Notes

### Version Numbering
KyroDB follows [Semantic Versioning](https://semver.org/):
- **MAJOR** version for incompatible API changes
- **MINOR** version for backwards-compatible functionality additions
- **PATCH** version for backwards-compatible bug fixes

### Support Policy
- Latest minor version receives active support
- Security fixes provided for last 2 minor versions
- Breaking changes announced 3 months in advance

### Migration Guide
For upgrading between versions, see [MIGRATION.md](MIGRATION.md) (forthcoming).

---

## Contributing to Changelog

When contributing to KyroDB:
- Keep entries brief but descriptive
- Group similar changes together
- Use present tense for new features ("Add feature") and past tense for fixes ("Fix bug")
- Reference issue numbers when applicable
- Update the "Unreleased" section for current development

Example entry:
```
### Added
- Add support for custom RMI epsilon bounds (#123)

### Fixed
- Fix memory leak in WAL rotation (#456)
```

---

For more information about KyroDB's roadmap and upcoming features, see [visiondocument.md](visiondocument.md).
