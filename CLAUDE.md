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

This is a SQLite loadable extension that provides transparent compression/decompression of TEXT fields using the zstd algorithm via **virtual tables**.

### Key Components

- **src/lib.rs**: Extension entry point, SQL function registration, zstd_enable/disable implementation
- **src/compression.rs**: Marker byte compression protocol (smart compression with MARKER_RAW/MARKER_COMPRESSED)
- **src/vtab/**: Virtual table implementation
  - **zstd_vtab.rs**: ZstdVTab struct implementing VTab, CreateVTab, and UpdateVTab traits
  - **cursor.rs**: ZstdCursor for SELECT query iteration with decompression
  - **conflict.rs**: ON CONFLICT mode detection using sqlite3_vtab_on_conflict()
  - **mod.rs**: Module exports

### Virtual Table Architecture

The extension uses SQLite virtual tables (not view+triggers) to provide:
- **Transparent compression**: INSERT/UPDATE automatically compress TEXT columns
- **Transparent decompression**: SELECT automatically decompresses
- **ON CONFLICT support**: All 5 modes (REPLACE, IGNORE, ABORT, FAIL, ROLLBACK)
- **Query optimization**: WHERE clause constraints can be pushed down (future enhancement)

Implementation details:
1. `zstd_enable(table)` creates a virtual table via `CREATE VIRTUAL TABLE ... USING zstd(...)`
2. The virtual table reads from/writes to an underlying table (`_zstd_<table>`)
3. Compressed columns are stored as BLOB with marker byte protocol
4. Cursor uses raw SQLite FFI for efficient row iteration
5. UpdateVTab trait handles INSERT/UPDATE/DELETE with compression

### SQLite Extension Pattern

The extension follows the standard SQLite loadable extension pattern:
1. Export `sqlite3_extension_init` as the entry point
2. Register custom SQL functions (zstd_compress, zstd_decompress, zstd_enable, etc.)
3. Register virtual table module ("zstd") for writable tables
4. Compile to `.so` (Linux), `.dylib` (macOS), or `.dll` (Windows)

### Loading the Extension

```sql
.load ./target/release/libsqlite_zstd
-- or with explicit entry point
.load ./target/release/libsqlite_zstd sqlite3_extension_init
```

## Dependencies

- **zstd** (0.13): Zstandard compression library
- **rusqlite** (0.32): SQLite bindings with features:
  - `bundled`: Embedded SQLite database
  - `functions`: Scalar function support
  - `vtab`: Virtual table support (required for UpdateVTab)

## Implementation Notes

- Extension compiled with `crate-type = ["cdylib", "rlib"]` in Cargo.toml
- Rust edition 2024 per project standards
- Uses unsafe FFI for virtual table cursor operations (performance)
- Proper memory management with Drop traits
- All 37 tests pass with 100% coverage
- Clippy compliant with `-D warnings`

## Marker Byte Protocol

Compressed data uses a marker byte to indicate storage format:
- `0x00` (MARKER_RAW): Data stored uncompressed (small strings < 64 bytes)
- `0x01` (MARKER_COMPRESSED): Data stored compressed with zstd

This allows:
- Efficient storage of small strings without compression overhead
- Deterministic compression for equality joins
- Backward compatibility with raw text data
