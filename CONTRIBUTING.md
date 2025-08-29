# Contributing to KyroDB

Thank you for your interest in contributing to KyroDB! We welcome contributions from the community and are grateful for your help in making KyroDB better.

## Table of Contents
- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Project Structure](#project-structure)
- [Contributing Guidelines](#contributing-guidelines)
- [Testing](#testing)
- [Submitting Changes](#submitting-changes)
- [Community](#community)

## Code of Conduct

This project follows a code of conduct to ensure a welcoming environment for all contributors. By participating, you agree to:
- Be respectful and inclusive
- Focus on constructive feedback
- Accept responsibility for mistakes
- Show empathy towards other contributors
- Help create a positive community

## Getting Started

### Prerequisites
- **Rust**: Latest stable version (1.70+)
- **Go**: 1.21+ (for orchestrator CLI)
- **Python**: 3.8+ (for benchmarking and plotting)
- **Git**: Latest version

### Quick Setup
```bash
# Clone the repository
git clone https://github.com/vatskishan03/KyroDB.git
cd KyroDB

# Build the engine
cargo build -p engine --release

# Build the orchestrator (optional)
cd orchestrator && go build -o kyrodbctl

# Run tests
cargo test -p engine
```

## Development Setup

### Environment Setup
```bash
# Install Rust toolchain
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.cargo/env

# Install Go (if not already installed)
# Follow instructions at https://golang.org/dl/

# Install Python dependencies
pip install matplotlib numpy pandas
```

### IDE Setup
We recommend using:
- **VS Code** with Rust Analyzer extension
- **CLion** with Rust plugin
- **IntelliJ IDEA** with Rust plugin

## Project Structure

```
KyroDB/
├── engine/                 # Main database engine (Rust)
│   ├── src/
│   │   ├── lib.rs         # Core engine implementation
│   │   ├── main.rs        # HTTP server and CLI
│   │   ├── index.rs       # RMI (learned index) implementation
│   │   └── ...
│   ├── tests/             # Integration tests
│   └── fuzz/              # Fuzzing targets
├── bench/                  # Benchmarking suite
├── orchestrator/           # Go CLI tool
├── docs/                   # Documentation
├── .github/               # CI/CD workflows
└── tests/                 # End-to-end tests
```

## Contributing Guidelines

### Code Style
- **Rust**: Follow the official Rust style guidelines. Use `rustfmt` and `clippy`.
- **Go**: Follow standard Go formatting with `gofmt`.
- **Python**: Follow PEP 8 style guidelines.

### Commit Messages
Use clear, descriptive commit messages following this format:
```
type(scope): description

[optional body]

[optional footer]
```

Types:
- `feat`: New features
- `fix`: Bug fixes
- `docs`: Documentation
- `style`: Code style changes
- `refactor`: Code refactoring
- `test`: Testing
- `chore`: Maintenance

### Branch Naming
- `feature/description`: New features
- `fix/description`: Bug fixes
- `docs/description`: Documentation
- `refactor/description`: Code refactoring

## Testing

### Running Tests
```bash
# Run all engine tests
cargo test -p engine

# Run specific test
cargo test -p engine test_name

# Run with features
cargo test -p engine --features failpoints

# Run fuzzing (requires nightly)
cargo +nightly fuzz run rmi_probe
```

### Writing Tests
- Unit tests should be colocated with the code they test
- Integration tests go in the `tests/` directory
- Add fuzz targets for critical components
- Include both positive and negative test cases

## Submitting Changes

### Pull Request Process
1. Fork the repository
2. Create a feature branch from `main`
3. Make your changes following the guidelines above
4. Add tests for new functionality
5. Ensure all tests pass
6. Update documentation if needed
7. Submit a pull request

### PR Requirements
- [ ] Tests pass (`cargo test -p engine`)
- [ ] Code follows style guidelines (`cargo fmt`, `cargo clippy`)
- [ ] Documentation updated
- [ ] Commit messages follow conventions
- [ ] PR description explains the changes
- [ ] No breaking changes without discussion

### Review Process
- All PRs require review from maintainers
- Reviews focus on code quality, correctness, and adherence to project goals
- Be open to feedback and ready to make changes
- Once approved, a maintainer will merge your PR

## Community

### Getting Help
- **Issues**: Use GitHub issues for bugs and feature requests
- **Discussions**: Use GitHub discussions for questions and ideas
- **Discord/Slack**: Join our community chat (link TBD)

### Recognition
Contributors are recognized in:
- CHANGELOG.md for significant contributions
- GitHub's contributor insights
- Release notes

Thank you for contributing to KyroDB! 🚀
