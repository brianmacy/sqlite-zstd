# Contributing to sqlite-zstd

Thank you for your interest in contributing! This document provides guidelines for contributing to the project.

## Development Setup

### Prerequisites

- Rust 1.70+ with edition 2024 support
- cargo (comes with Rust)
- For testing with real SQLite: Homebrew SQLite on macOS or standard SQLite on Linux

### Clone and Build

```bash
git clone https://github.com/brianmacy/sqlite-zstd.git
cd sqlite-zstd

# Build library
cargo build

# Build loadable extension
cargo build --release --features loadable_extension

# Run tests
cargo test

# Run benchmarks
cargo bench

# Run examples
cargo run --example basic_usage
cargo run --example on_conflict
```

## Code Standards

### Required Checks

All contributions must pass these checks:

1. **Formatting**: `cargo fmt -- --check`
   - Run `cargo fmt` to auto-fix

2. **Clippy**: `cargo clippy --all-targets --all-features -- -D warnings`
   - Must pass with **zero warnings**
   - This is a hard requirement

3. **Tests**: `cargo test`
   - All tests must pass (100% pass rate required)
   - Add tests for new functionality

4. **Documentation**: All public APIs must be documented
   - Use `///` doc comments
   - Include examples in doc comments where appropriate

### Code Style

- **Rust Edition**: 2024
- **Formatting**: Standard rustfmt configuration
- **Imports**: Alphabetically ordered (rustfmt handles this)
- **Error Handling**: Use `Result<T, String>` for user-facing errors, `Result<T>` internally
- **Unsafe Code**: Minimize unsafe usage, document safety requirements
- **Comments**: Explain *why*, not *what* (code should be self-documenting)

### Testing Requirements

- **Unit tests**: Add tests for new functions in the same file
- **Integration tests**: Add tests for new user-facing functionality
- **No mock tests**: Use real implementations only (per project standards)
- **100% pass rate**: All tests must pass before committing

Example test structure:
```rust
#[test]
fn test_feature_name() {
    let conn = setup_test_db();
    // Test setup

    // Execute operation

    // Verify results with assertions
}
```

## Pull Request Process

1. **Fork** the repository
2. **Create a branch** for your feature: `git checkout -b feature/my-feature`
3. **Make your changes** following the code standards above
4. **Add tests** for your changes
5. **Run all checks**:
   ```bash
   cargo fmt
   cargo clippy --all-targets --all-features -- -D warnings
   cargo test
   cargo doc --no-deps
   ```
6. **Commit** with a clear message following [Conventional Commits](https://www.conventionalcommits.org/)
7. **Push** to your fork
8. **Open a Pull Request** with:
   - Clear description of the change
   - Why the change is needed
   - Any breaking changes noted
   - Test results

### Commit Message Format

```
<type>: <description>

[optional body]

[optional footer]
```

Types:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `refactor`: Code refactoring
- `test`: Test additions/changes
- `chore`: Build/tooling changes
- `perf`: Performance improvements

Example:
```
feat: Add support for BLOB column compression

Extend virtual table to handle BLOB columns in addition to TEXT.
Adds new tests for BLOB compression roundtrip.

Closes #123
```

## Areas for Contribution

### High Priority

- **Platform testing**: Verify on different OS/architectures
- **Performance optimization**: Improve compression speed
- **Error messages**: Make error messages more helpful
- **Edge cases**: Find and fix edge cases

### Medium Priority

- **Additional examples**: Real-world use cases
- **Documentation**: Improve clarity, add diagrams
- **Benchmarks**: More comprehensive benchmarking scenarios
- **CLI tool**: Standalone compression utility

### Low Priority

- **Compression levels**: Make compression level configurable per-table
- **Statistics**: Enhanced statistics and monitoring
- **Migration tools**: Tools for converting between compression modes

## Architecture Overview

Understanding the architecture will help with contributions:

```
src/
├── lib.rs              # Entry point, SQL functions, enable/disable
├── compression.rs      # Marker byte compression protocol
└── vtab/              # Virtual table implementation
    ├── mod.rs         # Module exports
    ├── zstd_vtab.rs   # VTab/UpdateVTab traits
    ├── cursor.rs      # SELECT query cursor
    └── conflict.rs    # ON CONFLICT handling
```

Key concepts:
- **Virtual tables**: SQLite mechanism for custom table implementations
- **Marker byte protocol**: Smart compression (0x00=raw, 0x01=compressed)
- **UpdateVTab trait**: Enables INSERT/UPDATE/DELETE operations
- **Cursor**: Handles SELECT query iteration with decompression
- **best_index()**: Enables WHERE clause optimization

## Testing

### Running Tests

```bash
# All tests
cargo test

# Specific test
cargo test test_name

# With output
cargo test -- --nocapture

# Integration tests only
cargo test --test '*'

# Doc tests
cargo test --doc
```

### Writing Tests

Tests live in the same file as the code they test:
- Unit tests in `#[cfg(test)] mod tests { ... }`
- Place near the code being tested
- Use descriptive names: `test_feature_with_condition`
- Test both success and failure cases

### Test Coverage

We maintain 100% test coverage. When adding new code:
1. Add tests for the happy path
2. Add tests for error conditions
3. Add tests for edge cases
4. Verify all tests pass

## Benchmarking

Run benchmarks to verify performance:

```bash
cargo bench
```

Benchmark results are in `target/criterion/` with HTML reports.

When making performance-related changes:
1. Run benchmarks before changes
2. Make your changes
3. Run benchmarks after changes
4. Compare results to ensure no regression

## Documentation

### Inline Documentation

All public items must have doc comments:

```rust
/// Compress text using zstd with marker byte protocol.
///
/// Returns a vector with marker byte (0x00 or 0x01) followed by data.
/// Small strings (< 64 bytes) are stored raw with marker 0x00.
///
/// # Arguments
/// * `text` - Text to compress
/// * `level` - Compression level (1-22)
///
/// # Returns
/// Vector containing marker byte + compressed or raw data
///
/// # Errors
/// Returns error if compression fails
pub fn compress_with_marker(text: &str, level: i32) -> Result<Vec<u8>, String> {
    // ...
}
```

### Building Documentation

```bash
# Build docs
cargo doc --no-deps

# Build and open in browser
cargo doc --no-deps --open
```

## Getting Help

- **Issues**: Open an issue for bugs or feature requests
- **Discussions**: Use GitHub Discussions for questions
- **Code review**: Maintainers will review PRs and provide feedback

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.
