# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

SQLite extension for seamless TEXT field compression using Zstandard (zstd). Written in Rust, compiles to a dynamic library loadable by SQLite.

## Build Commands

```bash
# Build library (for testing)
cargo build

# Build loadable extension
cargo build --features loadable_extension

# Build release extension
cargo build --release --features loadable_extension

# Run all tests
cargo test

# Run a specific test
cargo test test_name

# Run tests with output
cargo test -- --nocapture

# Lint with clippy (must pass with no warnings)
cargo clippy --all-targets --all-features -- -D warnings

# Format code
cargo fmt

# Check formatting without modifying
cargo fmt -- --check
```

## macOS Development

The system SQLite (`/usr/bin/sqlite3`) has extension loading disabled and will crash. Use Homebrew's SQLite:

```bash
brew install sqlite
/opt/homebrew/opt/sqlite/bin/sqlite3 test.db
```

Then load: `.load ./target/release/libsqlite_zstd`

## Architecture

This is a SQLite loadable extension that provides transparent compression/decompression of TEXT fields using the zstd algorithm.

### Key Components (Expected Structure)

- **src/lib.rs**: Extension entry point, exports `sqlite3_extension_init` function
- **FFI Layer**: Uses `sqlite3_sys` or `rusqlite` for SQLite C API bindings
- **Compression Module**: Wraps the `zstd` crate for compression operations
- **SQL Functions**: Custom SQL functions registered with SQLite (e.g., `zstd_compress()`, `zstd_decompress()`)

### SQLite Extension Pattern

The extension follows the standard SQLite loadable extension pattern:
1. Export `sqlite3_extension_init` as the entry point
2. Register custom functions/virtual tables via SQLite's extension API
3. Compile to `.so` (Linux), `.dylib` (macOS), or `.dll` (Windows)

### Loading the Extension

```sql
.load ./target/release/libsqlite_zstd
-- or with explicit entry point
.load ./target/release/libsqlite_zstd sqlite3_extension_init
```

## Dependencies (Expected)

- `zstd` or `zstd-safe`: Zstandard compression
- `sqlite3_sys` or `rusqlite`: SQLite bindings with bundled feature for extension support

## Notes

- The .gitignore currently contains C/C++ patterns; should be updated to include Rust patterns (`/target/`, `Cargo.lock` if library)
- Extension must be compiled with `crate-type = ["cdylib"]` in Cargo.toml
- Rust edition 2024 per project standards
