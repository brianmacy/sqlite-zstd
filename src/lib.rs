//! SQLite extension for seamless TEXT field compression using Zstandard (zstd).
//!
//! This extension provides transparent compression/decompression of TEXT columns
//! through virtual tables with smart compression (marker byte protocol).
//!
//! # Features
//!
//! - **Transparent compression**: INSERT/UPDATE automatically compress TEXT columns
//! - **Transparent decompression**: SELECT automatically decompresses
//! - **ON CONFLICT support**: All 5 SQLite conflict modes (REPLACE, IGNORE, ABORT, FAIL, ROLLBACK)
//! - **Query optimization**: WHERE clause constraints pushed to underlying table
//! - **Smart compression**: Small strings (< 64 bytes) stored raw, large strings compressed
//! - **Deterministic**: Same input produces same output (enables equality joins)
//!
//! # Quick Start
//!
//! ```rust
//! use rusqlite::Connection;
//!
//! let conn = Connection::open_in_memory()?;
//! sqlite_zstd::register_functions(&conn)?;
//!
//! // Create table
//! conn.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, content TEXT)", [])?;
//!
//! // Enable compression
//! conn.query_row("SELECT zstd_enable('docs', 'content')", [], |_| Ok(()))?;
//!
//! // Use normally - compression is automatic
//! conn.execute("INSERT INTO docs (content) VALUES (?)", ["Large text..."])?;
//! let content: String = conn.query_row("SELECT content FROM docs WHERE id = 1", [], |row| row.get(0))?;
//! # Ok::<(), rusqlite::Error>(())
//! ```
//!
//! # Architecture
//!
//! The extension uses SQLite virtual tables to intercept all operations:
//! - `zstd_enable(table)` creates a virtual table that wraps the original table
//! - Writes (INSERT/UPDATE) compress TEXT columns before storage
//! - Reads (SELECT) decompress columns on retrieval
//! - Original table renamed to `_zstd_<table>` and stores compressed data
//!
//! # Performance
//!
//! - Compression: 869 MiB/s - 3.5 GiB/s
//! - Decompression: 1.17 - 4.1 GiB/s
//! - INSERT: ~333K rows/second
//! - SELECT (filtered): ~333K queries/second
//! - Space savings: 60-99% depending on data type

mod compression;
mod vtab;

use compression::{DEFAULT_COMPRESSION_LEVEL, compress_with_marker, decompress_with_marker};
use rusqlite::functions::FunctionFlags;
use rusqlite::types::{ToSqlOutput, Value, ValueRef};
use rusqlite::{Connection, Result};

#[cfg(feature = "loadable_extension")]
use rusqlite::ffi;
#[cfg(feature = "loadable_extension")]
use std::ffi::c_int;
#[cfg(feature = "loadable_extension")]
use std::os::raw::c_char;

/// Metadata table name for storing compression configuration
const CONFIG_TABLE: &str = "_zstd_config";

/// Prefix for renamed tables
const TABLE_PREFIX: &str = "_zstd_";

// =============================================================================
// Low-level SQL Function Implementations (without marker byte)
// =============================================================================

/// Compress text using zstd (raw, no marker byte).
/// SQL: zstd_compress(text) or zstd_compress(text, level)
fn zstd_compress_impl(text: &str, level: i32) -> std::result::Result<Vec<u8>, String> {
    zstd::encode_all(text.as_bytes(), level).map_err(|e| format!("zstd compression failed: {}", e))
}

/// Decompress zstd-compressed blob back to text (raw, no marker byte).
/// SQL: zstd_decompress(blob)
fn zstd_decompress_impl(data: &[u8]) -> std::result::Result<String, String> {
    let decompressed =
        zstd::decode_all(data).map_err(|e| format!("zstd decompression failed: {}", e))?;
    String::from_utf8(decompressed)
        .map_err(|e| format!("decompressed data is not valid UTF-8: {}", e))
}

// =============================================================================
// Table Management Functions
// =============================================================================

/// Create the config table if it doesn't exist.
fn ensure_config_table(conn: &Connection) -> std::result::Result<(), String> {
    conn.execute(
        &format!(
            "CREATE TABLE IF NOT EXISTS {} (
                table_name TEXT NOT NULL,
                column_name TEXT NOT NULL,
                compression_level INTEGER NOT NULL DEFAULT {},
                PRIMARY KEY (table_name, column_name)
            )",
            CONFIG_TABLE, DEFAULT_COMPRESSION_LEVEL
        ),
        [],
    )
    .map_err(|e| format!("failed to create config table: {}", e))?;
    Ok(())
}

/// Get all TEXT columns from a table's schema.
fn get_text_columns(conn: &Connection, table: &str) -> std::result::Result<Vec<String>, String> {
    let mut stmt = conn
        .prepare(&format!("PRAGMA table_info('{}')", table))
        .map_err(|e| format!("failed to get table info: {}", e))?;

    let columns: Vec<String> = stmt
        .query_map([], |row| {
            let name: String = row.get(1)?;
            let col_type: String = row.get(2)?;
            Ok((name, col_type))
        })
        .map_err(|e| format!("failed to query table info: {}", e))?
        .filter_map(|r| r.ok())
        .filter(|(_, col_type)| {
            let upper = col_type.to_uppercase();
            upper == "TEXT" || upper == "CLOB" || upper.starts_with("CLOB(")
        })
        .map(|(name, _)| name)
        .collect();

    if columns.is_empty() {
        return Err(format!("table '{}' has no TEXT/CLOB columns", table));
    }

    Ok(columns)
}

/// Get all columns from a table's schema with their types and pk status.
/// Returns Vec<(name, type, is_pk)>
fn get_all_columns_with_pk(
    conn: &Connection,
    table: &str,
) -> std::result::Result<Vec<(String, String, bool)>, String> {
    let mut stmt = conn
        .prepare(&format!("PRAGMA table_info('{}')", table))
        .map_err(|e| format!("failed to get table info: {}", e))?;

    let columns: Vec<(String, String, bool)> = stmt
        .query_map([], |row| {
            let name: String = row.get(1)?;
            let col_type: String = row.get(2)?;
            let pk: i32 = row.get(5)?;
            Ok((name, col_type, pk != 0))
        })
        .map_err(|e| format!("failed to query table info: {}", e))?
        .filter_map(|r| r.ok())
        .collect();

    if columns.is_empty() {
        return Err(format!("table '{}' not found or has no columns", table));
    }

    Ok(columns)
}

// =============================================================================
// Enable/Disable Functions
// =============================================================================

/// Enable compression for a table using virtual tables.
fn zstd_enable_impl(
    conn: &Connection,
    table: &str,
    columns: Option<Vec<String>>,
) -> std::result::Result<String, String> {
    // Validate table name (prevent SQL injection)
    if !table.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Err("invalid table name".to_string());
    }

    let raw_table = format!("{}{}", TABLE_PREFIX, table);

    // Check if it's already a zstd virtual table
    let is_vtab: bool = conn
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE name=? AND sql LIKE 'CREATE VIRTUAL TABLE%'",
            [table],
            |_| Ok(true),
        )
        .unwrap_or(false);

    if is_vtab {
        return Err(format!("table '{}' is already a zstd virtual table", table));
    }

    // Check if it's a view (views can't be compressed)
    let is_view: bool = conn
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE name=? AND type='view'",
            [table],
            |_| Ok(true),
        )
        .unwrap_or(false);

    if is_view {
        return Err(format!("'{}' is a view, not a table", table));
    }

    // Check that a table with this name exists
    let table_exists: bool = conn
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE name=? AND type='table'",
            [table],
            |_| Ok(true),
        )
        .unwrap_or(false);

    if !table_exists {
        return Err(format!("table '{}' does not exist", table));
    }

    // Get all columns with types and PRIMARY KEY information
    let all_columns_with_pk = get_all_columns_with_pk(conn, table)?;
    let all_columns: Vec<(String, String)> = all_columns_with_pk
        .iter()
        .map(|(name, typ, _)| (name.clone(), typ.clone()))
        .collect();

    // Helper to check if type is TEXT-like (TEXT, CLOB, CLOB(n))
    let is_text_type = |col_type: &str| -> bool {
        let upper = col_type.to_uppercase();
        upper == "TEXT" || upper == "CLOB" || upper.starts_with("CLOB(")
    };

    // Determine which columns to compress
    let compress_columns: Vec<String> = match columns {
        Some(cols) => {
            // Validate specified columns exist and are TEXT/CLOB
            for col in &cols {
                let found = all_columns.iter().find(|(name, _)| name == col);
                match found {
                    Some((_, col_type)) if is_text_type(col_type) => {}
                    Some((_, col_type)) => {
                        return Err(format!(
                            "column '{}' is type '{}', not TEXT/CLOB",
                            col, col_type
                        ));
                    }
                    None => {
                        return Err(format!("column '{}' not found in table '{}'", col, table));
                    }
                }
            }
            cols
        }
        None => get_text_columns(conn, table)?,
    };

    // Create config table
    ensure_config_table(conn)?;

    // Begin transaction
    conn.execute("BEGIN TRANSACTION", [])
        .map_err(|e| format!("failed to begin transaction: {}", e))?;

    let result = (|| -> std::result::Result<String, String> {
        // Note: vtab module is registered in register_functions(), called during initialization

        // Rename original table to underlying table
        conn.execute(
            &format!("ALTER TABLE \"{}\" RENAME TO \"{}\"", table, raw_table),
            [],
        )
        .map_err(|e| format!("failed to rename table: {}", e))?;

        // Build schema string: "col1:TYPE1:PK|col2:TYPE2|..." (PK suffix for primary keys)
        // Use | as delimiter because commas are interpreted as SQL argument separators
        let schema_str = all_columns_with_pk
            .iter()
            .map(|(name, col_type, is_pk)| {
                if *is_pk {
                    format!("{}:{}:PK", name, col_type)
                } else {
                    format!("{}:{}", name, col_type)
                }
            })
            .collect::<Vec<_>>()
            .join("|");

        // Build compressed columns string: "col1|col2|..."
        let compressed_cols_str = compress_columns.join("|");

        // Create virtual table
        // Format: CREATE VIRTUAL TABLE name USING zstd(underlying, cols, schema)
        // Note: Don't use quotes around arguments - they become part of the argument value!
        let create_vtab = format!(
            "CREATE VIRTUAL TABLE \"{}\" USING zstd({}, {}, {})",
            table, raw_table, compressed_cols_str, schema_str
        );
        conn.execute(&create_vtab, [])
            .map_err(|e| format!("failed to create virtual table: {}", e))?;

        // Store config
        for col in &compress_columns {
            conn.execute(
                &format!(
                    "INSERT INTO {} (table_name, column_name, compression_level) VALUES (?, ?, ?)",
                    CONFIG_TABLE
                ),
                rusqlite::params![table, col, DEFAULT_COMPRESSION_LEVEL],
            )
            .map_err(|e| format!("failed to store config: {}", e))?;
        }

        Ok(format!(
            "Enabled compression on {} column(s): {}",
            compress_columns.len(),
            compress_columns.join(", ")
        ))
    })();

    match result {
        Ok(msg) => {
            conn.execute("COMMIT", [])
                .map_err(|e| format!("failed to commit: {}", e))?;
            Ok(msg)
        }
        Err(e) => {
            let _ = conn.execute("ROLLBACK", []);
            Err(e)
        }
    }
}

/// Disable compression for a table or specific column.
fn zstd_disable_impl(
    conn: &Connection,
    table: &str,
    column: Option<&str>,
) -> std::result::Result<String, String> {
    // Validate table name
    if !table.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Err("invalid table name".to_string());
    }

    let raw_table = format!("{}{}", TABLE_PREFIX, table);

    // Check if compression is enabled
    let config_exists: bool = conn
        .query_row(
            &format!(
                "SELECT 1 FROM {} WHERE table_name = ? LIMIT 1",
                CONFIG_TABLE
            ),
            [table],
            |_| Ok(true),
        )
        .unwrap_or(false);

    if !config_exists {
        return Err(format!("compression not enabled on table '{}'", table));
    }

    conn.execute("BEGIN TRANSACTION", [])
        .map_err(|e| format!("failed to begin transaction: {}", e))?;

    let result = (|| -> std::result::Result<String, String> {
        match column {
            Some(col) => {
                // Disable single column
                // Get remaining compressed columns
                let mut stmt = conn
                    .prepare(&format!(
                        "SELECT column_name FROM {} WHERE table_name = ?",
                        CONFIG_TABLE
                    ))
                    .map_err(|e| format!("failed to query config: {}", e))?;

                let columns: Vec<String> = stmt
                    .query_map([table], |row| row.get(0))
                    .map_err(|e| format!("failed to get columns: {}", e))?
                    .filter_map(|r| r.ok())
                    .collect();

                if !columns.contains(&col.to_string()) {
                    return Err(format!("column '{}' is not compressed", col));
                }

                if columns.len() == 1 {
                    // Last column, disable entire table
                    drop(stmt);
                    return zstd_disable_table(conn, table, &raw_table);
                }

                // Remove column from config
                conn.execute(
                    &format!(
                        "DELETE FROM {} WHERE table_name = ? AND column_name = ?",
                        CONFIG_TABLE
                    ),
                    rusqlite::params![table, col],
                )
                .map_err(|e| format!("failed to remove config: {}", e))?;

                // Decompress the column in the underlying table
                conn.execute(
                    &format!(
                        "UPDATE \"{}\" SET \"{}\" = zstd_decompress_marked(\"{}\")",
                        raw_table, col, col
                    ),
                    [],
                )
                .map_err(|e| format!("failed to decompress column: {}", e))?;

                // Drop and recreate virtual table with updated column list
                let remaining_columns: Vec<String> =
                    columns.into_iter().filter(|c| c != col).collect();

                // Get all columns from underlying table with PK info
                let all_columns_with_pk = get_all_columns_with_pk(conn, &raw_table)?;

                // Drop existing virtual table
                conn.execute(&format!("DROP TABLE \"{}\"", table), [])
                    .map_err(|e| format!("failed to drop virtual table: {}", e))?;

                // Build new schema string with PK info (use | delimiter)
                let schema_str = all_columns_with_pk
                    .iter()
                    .map(|(name, col_type, is_pk)| {
                        if *is_pk {
                            format!("{}:{}:PK", name, col_type)
                        } else {
                            format!("{}:{}", name, col_type)
                        }
                    })
                    .collect::<Vec<_>>()
                    .join("|");

                let compressed_cols_str = remaining_columns.join("|");

                // Recreate virtual table
                let create_vtab = format!(
                    "CREATE VIRTUAL TABLE \"{}\" USING zstd({}, {}, {})",
                    table, raw_table, compressed_cols_str, schema_str
                );
                conn.execute(&create_vtab, [])
                    .map_err(|e| format!("failed to recreate virtual table: {}", e))?;

                Ok(format!("Disabled compression on column '{}'", col))
            }
            None => {
                // Disable entire table
                zstd_disable_table(conn, table, &raw_table)
            }
        }
    })();

    match result {
        Ok(msg) => {
            conn.execute("COMMIT", [])
                .map_err(|e| format!("failed to commit: {}", e))?;
            Ok(msg)
        }
        Err(e) => {
            let _ = conn.execute("ROLLBACK", []);
            Err(e)
        }
    }
}

/// Disable compression on entire table - helper function (virtual table version).
fn zstd_disable_table(
    conn: &Connection,
    table: &str,
    raw_table: &str,
) -> std::result::Result<String, String> {
    // Get compressed columns
    let mut stmt = conn
        .prepare(&format!(
            "SELECT column_name FROM {} WHERE table_name = ?",
            CONFIG_TABLE
        ))
        .map_err(|e| format!("failed to query config: {}", e))?;

    let columns: Vec<String> = stmt
        .query_map([table], |row| row.get(0))
        .map_err(|e| format!("failed to get columns: {}", e))?
        .filter_map(|r| r.ok())
        .collect();

    drop(stmt);

    // Decompress all compressed columns in underlying table
    for col in &columns {
        conn.execute(
            &format!(
                "UPDATE \"{}\" SET \"{}\" = zstd_decompress_marked(\"{}\")",
                raw_table, col, col
            ),
            [],
        )
        .map_err(|e| format!("failed to decompress column '{}': {}", col, e))?;
    }

    // Drop virtual table
    conn.execute(&format!("DROP TABLE IF EXISTS \"{}\"", table), [])
        .map_err(|e| format!("failed to drop virtual table: {}", e))?;

    // Rename underlying table back to original name
    conn.execute(
        &format!("ALTER TABLE \"{}\" RENAME TO \"{}\"", raw_table, table),
        [],
    )
    .map_err(|e| format!("failed to rename table: {}", e))?;

    // Remove from config
    conn.execute(
        &format!("DELETE FROM {} WHERE table_name = ?", CONFIG_TABLE),
        [table],
    )
    .map_err(|e| format!("failed to remove config: {}", e))?;

    Ok(format!(
        "Disabled compression on table '{}' ({} columns)",
        table,
        columns.len()
    ))
}

/// List compressed columns in a table.
fn zstd_columns_impl(conn: &Connection, table: &str) -> std::result::Result<String, String> {
    ensure_config_table(conn)?;

    let mut stmt = conn
        .prepare(&format!(
            "SELECT column_name FROM {} WHERE table_name = ? ORDER BY column_name",
            CONFIG_TABLE
        ))
        .map_err(|e| format!("failed to query config: {}", e))?;

    let columns: Vec<String> = stmt
        .query_map([table], |row| row.get(0))
        .map_err(|e| format!("failed to get columns: {}", e))?
        .filter_map(|r| r.ok())
        .collect();

    Ok(columns.join(", "))
}

/// Get compression statistics for a table.
fn zstd_stats_impl(conn: &Connection, table: &str) -> std::result::Result<String, String> {
    let raw_table = format!("{}{}", TABLE_PREFIX, table);

    // Check if compression is enabled
    ensure_config_table(conn)?;

    let mut stmt = conn
        .prepare(&format!(
            "SELECT column_name FROM {} WHERE table_name = ?",
            CONFIG_TABLE
        ))
        .map_err(|e| format!("failed to query config: {}", e))?;

    let columns: Vec<String> = stmt
        .query_map([table], |row| row.get(0))
        .map_err(|e| format!("failed to get columns: {}", e))?
        .filter_map(|r| r.ok())
        .collect();

    drop(stmt);

    if columns.is_empty() {
        return Err(format!("compression not enabled on table '{}'", table));
    }

    let mut stats = Vec::new();
    for col in &columns {
        // Get compressed size (includes marker byte)
        let compressed_size: i64 = conn
            .query_row(
                &format!(
                    "SELECT COALESCE(SUM(LENGTH(\"{}\")), 0) FROM \"{}\"",
                    col, raw_table
                ),
                [],
                |row| row.get(0),
            )
            .map_err(|e| format!("failed to get compressed size: {}", e))?;

        // Get decompressed size
        let decompressed_size: i64 = conn
            .query_row(
                &format!(
                    "SELECT COALESCE(SUM(LENGTH(zstd_decompress_marked(\"{}\"))), 0) FROM \"{}\"",
                    col, raw_table
                ),
                [],
                |row| row.get(0),
            )
            .map_err(|e| format!("failed to get decompressed size: {}", e))?;

        let ratio = if decompressed_size > 0 {
            (compressed_size as f64 / decompressed_size as f64) * 100.0
        } else {
            0.0
        };

        stats.push(format!(
            "{}: {} -> {} ({:.1}%)",
            col, decompressed_size, compressed_size, ratio
        ));
    }

    Ok(stats.join("; "))
}

// =============================================================================
// SQLite Extension Registration
// =============================================================================

/// Register all zstd functions with the SQLite connection.
///
/// This is the main entry point for using the extension from Rust code.
/// Call this function once per connection to enable all zstd functionality.
///
/// # Registered Functions
///
/// - `zstd_compress(text)` - Compress text to BLOB
/// - `zstd_compress(text, level)` - Compress with specific level (1-22)
/// - `zstd_decompress(blob)` - Decompress BLOB to text
/// - `zstd_enable(table, ...)` - Enable compression on table/columns
/// - `zstd_disable(table [, column])` - Disable compression
/// - `zstd_columns(table)` - List compressed columns
/// - `zstd_stats(table)` - Get compression statistics
///
/// Internal functions (used by virtual table):
/// - `zstd_compress_marked(text)` - Compress with marker byte
/// - `zstd_decompress_marked(blob)` - Decompress with marker byte
///
/// # Example
///
/// ```rust
/// use rusqlite::Connection;
///
/// let conn = Connection::open_in_memory()?;
/// sqlite_zstd::register_functions(&conn)?;
///
/// // Now all zstd functions are available
/// conn.execute("CREATE TABLE docs (id INTEGER, content TEXT)", [])?;
/// conn.query_row("SELECT zstd_enable('docs', 'content')", [], |_| Ok(()))?;
/// # Ok::<(), rusqlite::Error>(())
/// ```
///
/// # Errors
///
/// Returns error if function registration fails (rare - usually indicates
/// SQLite version incompatibility or memory issues).
pub fn register_functions(conn: &Connection) -> Result<()> {
    // Register virtual table module FIRST
    // This must happen during initialization so the module is available
    // for any connection that might call zstd_enable()
    vtab::register_module(conn)?;

    // zstd_compress(text) and zstd_compress(text, level) - raw, no marker
    conn.create_scalar_function(
        "zstd_compress",
        -1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let arg_count = ctx.len();
            if !(1..=2).contains(&arg_count) {
                return Err(rusqlite::Error::UserFunctionError(
                    "zstd_compress requires 1 or 2 arguments".into(),
                ));
            }

            let text = ctx.get_raw(0);
            let text = match text {
                ValueRef::Text(s) => std::str::from_utf8(s)
                    .map_err(|e| rusqlite::Error::UserFunctionError(e.to_string().into()))?,
                ValueRef::Null => return Ok(ToSqlOutput::Owned(Value::Null)),
                _ => {
                    return Err(rusqlite::Error::UserFunctionError(
                        "zstd_compress: first argument must be TEXT".into(),
                    ));
                }
            };

            let level = if arg_count == 2 {
                ctx.get::<i32>(1)?
            } else {
                DEFAULT_COMPRESSION_LEVEL
            };

            match zstd_compress_impl(text, level) {
                Ok(compressed) => Ok(ToSqlOutput::Owned(Value::Blob(compressed))),
                Err(e) => Err(rusqlite::Error::UserFunctionError(e.into())),
            }
        },
    )?;

    // zstd_decompress(blob) - raw, no marker
    conn.create_scalar_function(
        "zstd_decompress",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let data = ctx.get_raw(0);
            let data = match data {
                ValueRef::Blob(b) => b,
                ValueRef::Null => return Ok(ToSqlOutput::Owned(Value::Null)),
                _ => {
                    return Err(rusqlite::Error::UserFunctionError(
                        "zstd_decompress: argument must be BLOB".into(),
                    ));
                }
            };

            match zstd_decompress_impl(data) {
                Ok(text) => Ok(ToSqlOutput::Owned(Value::Text(text))),
                Err(e) => Err(rusqlite::Error::UserFunctionError(e.into())),
            }
        },
    )?;

    // zstd_compress_marked(text) - with marker byte, used internally
    conn.create_scalar_function(
        "zstd_compress_marked",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let text = ctx.get_raw(0);
            let text = match text {
                ValueRef::Text(s) => std::str::from_utf8(s)
                    .map_err(|e| rusqlite::Error::UserFunctionError(e.to_string().into()))?,
                ValueRef::Null => return Ok(ToSqlOutput::Owned(Value::Null)),
                _ => {
                    return Err(rusqlite::Error::UserFunctionError(
                        "zstd_compress_marked: argument must be TEXT".into(),
                    ));
                }
            };

            match compress_with_marker(text, DEFAULT_COMPRESSION_LEVEL) {
                Ok(compressed) => Ok(ToSqlOutput::Owned(Value::Blob(compressed))),
                Err(e) => Err(rusqlite::Error::UserFunctionError(e.into())),
            }
        },
    )?;

    // zstd_decompress_marked(blob) - with marker byte, used internally
    conn.create_scalar_function(
        "zstd_decompress_marked",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let data = ctx.get_raw(0);
            let data = match data {
                ValueRef::Blob(b) => b,
                ValueRef::Null => return Ok(ToSqlOutput::Owned(Value::Null)),
                // If it's already text (not compressed), return as-is
                ValueRef::Text(s) => {
                    let text = std::str::from_utf8(s)
                        .map_err(|e| rusqlite::Error::UserFunctionError(e.to_string().into()))?;
                    return Ok(ToSqlOutput::Owned(Value::Text(text.to_string())));
                }
                _ => {
                    return Err(rusqlite::Error::UserFunctionError(
                        "zstd_decompress_marked: argument must be BLOB or TEXT".into(),
                    ));
                }
            };

            match decompress_with_marker(data) {
                Ok(text) => Ok(ToSqlOutput::Owned(Value::Text(text))),
                Err(e) => Err(rusqlite::Error::UserFunctionError(e.into())),
            }
        },
    )?;

    // zstd_enable(table) or zstd_enable(table, col1, col2, ...)
    conn.create_scalar_function("zstd_enable", -1, FunctionFlags::SQLITE_UTF8, |ctx| {
        let arg_count = ctx.len();
        if arg_count < 1 {
            return Err(rusqlite::Error::UserFunctionError(
                "zstd_enable requires at least 1 argument".into(),
            ));
        }

        let table: String = ctx.get(0)?;
        let columns: Option<Vec<String>> = if arg_count > 1 {
            let mut cols = Vec::new();
            for i in 1..arg_count {
                cols.push(ctx.get(i)?);
            }
            Some(cols)
        } else {
            None
        };

        // Safety: We're within a scalar function context, connection is valid
        let conn_ref = unsafe { ctx.get_connection()? };

        match zstd_enable_impl(&conn_ref, &table, columns) {
            Ok(msg) => Ok(ToSqlOutput::Owned(Value::Text(msg))),
            Err(e) => Err(rusqlite::Error::UserFunctionError(e.into())),
        }
    })?;

    // zstd_disable(table) or zstd_disable(table, column)
    conn.create_scalar_function("zstd_disable", -1, FunctionFlags::SQLITE_UTF8, |ctx| {
        let arg_count = ctx.len();
        if !(1..=2).contains(&arg_count) {
            return Err(rusqlite::Error::UserFunctionError(
                "zstd_disable requires 1 or 2 arguments".into(),
            ));
        }

        let table: String = ctx.get(0)?;
        let column: Option<String> = if arg_count == 2 {
            Some(ctx.get(1)?)
        } else {
            None
        };

        // Safety: We're within a scalar function context, connection is valid
        let conn_ref = unsafe { ctx.get_connection()? };

        match zstd_disable_impl(&conn_ref, &table, column.as_deref()) {
            Ok(msg) => Ok(ToSqlOutput::Owned(Value::Text(msg))),
            Err(e) => Err(rusqlite::Error::UserFunctionError(e.into())),
        }
    })?;

    // zstd_columns(table)
    conn.create_scalar_function("zstd_columns", 1, FunctionFlags::SQLITE_UTF8, |ctx| {
        let table: String = ctx.get(0)?;

        // Safety: We're within a scalar function context, connection is valid
        let conn_ref = unsafe { ctx.get_connection()? };

        match zstd_columns_impl(&conn_ref, &table) {
            Ok(result) => Ok(ToSqlOutput::Owned(Value::Text(result))),
            Err(e) => Err(rusqlite::Error::UserFunctionError(e.into())),
        }
    })?;

    // zstd_stats(table)
    conn.create_scalar_function("zstd_stats", 1, FunctionFlags::SQLITE_UTF8, |ctx| {
        let table: String = ctx.get(0)?;

        // Safety: We're within a scalar function context, connection is valid
        let conn_ref = unsafe { ctx.get_connection()? };

        match zstd_stats_impl(&conn_ref, &table) {
            Ok(result) => Ok(ToSqlOutput::Owned(Value::Text(result))),
            Err(e) => Err(rusqlite::Error::UserFunctionError(e.into())),
        }
    })?;

    Ok(())
}

// =============================================================================
// SQLite Loadable Extension Entry Point
// =============================================================================

/// Entry point for SQLite loadable extension.
///
/// # Safety
/// This function is called by SQLite when loading the extension.
#[cfg(feature = "loadable_extension")]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite3_extension_init(
    db: *mut ffi::sqlite3,
    _pz_err_msg: *mut *mut c_char,
    p_api: *mut ffi::sqlite3_api_routines,
) -> c_int {
    // Initialize the SQLite API
    if unsafe { ffi::rusqlite_extension_init2(p_api) }.is_err() {
        return ffi::SQLITE_ERROR;
    }

    // Wrap the raw pointer in a Connection
    let conn = match unsafe { Connection::from_handle(db) } {
        Ok(c) => c,
        Err(_) => return ffi::SQLITE_ERROR,
    };

    // Register our functions
    match register_functions(&conn) {
        Ok(_) => {
            // Don't drop the connection - SQLite owns it
            std::mem::forget(conn);
            ffi::SQLITE_OK
        }
        Err(_) => ffi::SQLITE_ERROR,
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compression::{MARKER_COMPRESSED, MARKER_RAW};
    use rusqlite::Connection;

    fn setup_test_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        register_functions(&conn).unwrap();
        conn
    }

    // -------------------------------------------------------------------------
    // zstd_compress tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_zstd_compress_basic() {
        let conn = setup_test_db();
        let result: Vec<u8> = conn
            .query_row("SELECT zstd_compress('Hello, World!')", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert!(!result.is_empty(), "Compressed result should not be empty");
    }

    #[test]
    fn test_zstd_compress_with_level() {
        let conn = setup_test_db();
        let result: Vec<u8> = conn
            .query_row("SELECT zstd_compress('Hello, World!', 19)", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert!(!result.is_empty(), "Compressed result should not be empty");
    }

    #[test]
    fn test_zstd_compress_null() {
        let conn = setup_test_db();
        let result: Option<Vec<u8>> = conn
            .query_row("SELECT zstd_compress(NULL)", [], |row| row.get(0))
            .unwrap();
        assert!(result.is_none(), "Compressing NULL should return NULL");
    }

    #[test]
    fn test_zstd_compress_empty_string() {
        let conn = setup_test_db();
        let result: Vec<u8> = conn
            .query_row("SELECT zstd_compress('')", [], |row| row.get(0))
            .unwrap();
        assert!(
            !result.is_empty(),
            "Compressed empty string should produce valid zstd frame"
        );
    }

    #[test]
    fn test_zstd_compress_large_text() {
        let conn = setup_test_db();
        let large_text = "x".repeat(100_000);
        let result: Vec<u8> = conn
            .query_row("SELECT zstd_compress(?)", [&large_text], |row| row.get(0))
            .unwrap();
        assert!(
            result.len() < large_text.len(),
            "Compressed size should be smaller than original for repetitive data"
        );
    }

    // -------------------------------------------------------------------------
    // zstd_decompress tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_zstd_decompress_basic() {
        let conn = setup_test_db();
        let result: String = conn
            .query_row(
                "SELECT zstd_decompress(zstd_compress('Hello, World!'))",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(result, "Hello, World!");
    }

    #[test]
    fn test_zstd_decompress_null() {
        let conn = setup_test_db();
        let result: Option<String> = conn
            .query_row("SELECT zstd_decompress(NULL)", [], |row| row.get(0))
            .unwrap();
        assert!(result.is_none(), "Decompressing NULL should return NULL");
    }

    #[test]
    fn test_zstd_decompress_empty_string_roundtrip() {
        let conn = setup_test_db();
        let result: String = conn
            .query_row("SELECT zstd_decompress(zstd_compress(''))", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(result, "");
    }

    #[test]
    fn test_zstd_roundtrip_unicode() {
        let conn = setup_test_db();
        let unicode_text = "Hello, 世界! 🎉 Привет мир!";
        let result: String = conn
            .query_row(
                "SELECT zstd_decompress(zstd_compress(?))",
                [unicode_text],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(result, unicode_text);
    }

    #[test]
    fn test_zstd_decompress_invalid_data() {
        let conn = setup_test_db();
        let result = conn.query_row("SELECT zstd_decompress(X'DEADBEEF')", [], |row| {
            row.get::<_, String>(0)
        });
        assert!(result.is_err(), "Decompressing invalid data should fail");
    }

    // -------------------------------------------------------------------------
    // Marker byte compression tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_compress_marked_small_string() {
        let conn = setup_test_db();
        // Small string should be stored raw with marker byte
        let result: Vec<u8> = conn
            .query_row("SELECT zstd_compress_marked('Hi')", [], |row| row.get(0))
            .unwrap();
        assert_eq!(result[0], MARKER_RAW, "Small string should use raw marker");
        assert_eq!(&result[1..], b"Hi", "Raw data should follow marker");
    }

    #[test]
    fn test_compress_marked_large_string() {
        let conn = setup_test_db();
        // Large repetitive string should be compressed
        let large_text = "x".repeat(1000);
        let result: Vec<u8> = conn
            .query_row("SELECT zstd_compress_marked(?)", [&large_text], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(
            result[0], MARKER_COMPRESSED,
            "Large string should use compressed marker"
        );
        assert!(
            result.len() < large_text.len(),
            "Compressed size should be smaller"
        );
    }

    #[test]
    fn test_decompress_marked_roundtrip() {
        let conn = setup_test_db();
        // Test both small and large strings
        for text in &["Hi", "Hello, World!", &"x".repeat(1000)] {
            let result: String = conn
                .query_row(
                    "SELECT zstd_decompress_marked(zstd_compress_marked(?))",
                    [text],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(&result, *text, "Roundtrip should preserve data");
        }
    }

    #[test]
    fn test_decompress_marked_handles_text() {
        let conn = setup_test_db();
        // If given TEXT instead of BLOB, should return as-is
        let result: String = conn
            .query_row("SELECT zstd_decompress_marked('Hello')", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(result, "Hello");
    }

    // -------------------------------------------------------------------------
    // zstd_enable tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_zstd_enable_all_columns() {
        let conn = setup_test_db();
        conn.execute(
            "CREATE TABLE documents (id INTEGER PRIMARY KEY, title TEXT, content TEXT)",
            [],
        )
        .unwrap();

        conn.query_row("SELECT zstd_enable('documents')", [], |_| Ok(()))
            .unwrap();

        // Verify the virtual table exists
        let vtab_exists: bool = conn
            .query_row(
                "SELECT 1 FROM sqlite_master WHERE name='documents' AND sql LIKE 'CREATE VIRTUAL TABLE%'",
                [],
                |_| Ok(true),
            )
            .unwrap_or(false);
        assert!(vtab_exists, "Virtual table should be created");

        // Verify the underlying table exists
        let raw_table_exists: i32 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='_zstd_documents'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(raw_table_exists, 1, "Underlying table should exist");
    }

    #[test]
    fn test_zstd_enable_specific_columns() {
        let conn = setup_test_db();
        conn.execute(
            "CREATE TABLE documents (id INTEGER PRIMARY KEY, title TEXT, content TEXT, metadata TEXT)",
            [],
        )
        .unwrap();

        conn.query_row(
            "SELECT zstd_enable('documents', 'content', 'metadata')",
            [],
            |_| Ok(()),
        )
        .unwrap();

        // Insert data
        conn.execute(
            "INSERT INTO documents (title, content, metadata) VALUES ('Test', 'Large content', '{}')",
            [],
        )
        .unwrap();

        // Verify title is not compressed (stored as-is in raw table)
        let raw_title: String = conn
            .query_row("SELECT title FROM _zstd_documents", [], |row| row.get(0))
            .unwrap();
        assert_eq!(
            raw_title, "Test",
            "Uncompressed column should be stored as-is"
        );
    }

    #[test]
    fn test_zstd_enable_insert_select_roundtrip() {
        let conn = setup_test_db();
        conn.execute(
            "CREATE TABLE documents (id INTEGER PRIMARY KEY, title TEXT, content TEXT)",
            [],
        )
        .unwrap();

        conn.query_row("SELECT zstd_enable('documents', 'content')", [], |_| Ok(()))
            .unwrap();

        // Insert through the view
        conn.execute(
            "INSERT INTO documents (title, content) VALUES ('My Doc', 'This is the content')",
            [],
        )
        .unwrap();

        // Select through the view - should auto-decompress
        let (title, content): (String, String) = conn
            .query_row("SELECT title, content FROM documents", [], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })
            .unwrap();

        assert_eq!(title, "My Doc");
        assert_eq!(content, "This is the content");
    }

    #[test]
    fn test_zstd_enable_update() {
        let conn = setup_test_db();
        conn.execute(
            "CREATE TABLE documents (id INTEGER PRIMARY KEY, content TEXT)",
            [],
        )
        .unwrap();

        conn.query_row("SELECT zstd_enable('documents', 'content')", [], |_| Ok(()))
            .unwrap();

        conn.execute("INSERT INTO documents (content) VALUES ('Original')", [])
            .unwrap();

        conn.execute("UPDATE documents SET content = 'Updated' WHERE id = 1", [])
            .unwrap();

        let content: String = conn
            .query_row("SELECT content FROM documents WHERE id = 1", [], |row| {
                row.get(0)
            })
            .unwrap();

        assert_eq!(content, "Updated");
    }

    #[test]
    fn test_zstd_enable_delete() {
        let conn = setup_test_db();
        conn.execute(
            "CREATE TABLE documents (id INTEGER PRIMARY KEY, content TEXT)",
            [],
        )
        .unwrap();

        conn.query_row("SELECT zstd_enable('documents', 'content')", [], |_| Ok(()))
            .unwrap();

        conn.execute("INSERT INTO documents (content) VALUES ('To delete')", [])
            .unwrap();

        conn.execute("DELETE FROM documents WHERE id = 1", [])
            .unwrap();

        let count: i32 = conn
            .query_row("SELECT COUNT(*) FROM documents", [], |row| row.get(0))
            .unwrap();

        assert_eq!(count, 0);
    }

    // -------------------------------------------------------------------------
    // zstd_disable tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_zstd_disable_table() {
        let conn = setup_test_db();
        conn.execute(
            "CREATE TABLE documents (id INTEGER PRIMARY KEY, content TEXT)",
            [],
        )
        .unwrap();

        conn.query_row("SELECT zstd_enable('documents', 'content')", [], |_| Ok(()))
            .unwrap();

        conn.execute(
            "INSERT INTO documents (content) VALUES ('Test content')",
            [],
        )
        .unwrap();

        conn.query_row("SELECT zstd_disable('documents')", [], |_| Ok(()))
            .unwrap();

        // Verify the original table is restored
        let table_exists: i32 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='documents'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(table_exists, 1, "Original table should be restored");

        // Verify data is preserved and decompressed
        let content: String = conn
            .query_row("SELECT content FROM documents", [], |row| row.get(0))
            .unwrap();
        assert_eq!(content, "Test content");
    }

    #[test]
    fn test_zstd_disable_single_column() {
        let conn = setup_test_db();
        conn.execute(
            "CREATE TABLE documents (id INTEGER PRIMARY KEY, content TEXT, metadata TEXT)",
            [],
        )
        .unwrap();

        conn.query_row(
            "SELECT zstd_enable('documents', 'content', 'metadata')",
            [],
            |_| Ok(()),
        )
        .unwrap();

        conn.query_row(
            "SELECT zstd_disable('documents', 'content')",
            [],
            |_| Ok(()),
        )
        .unwrap();

        // metadata should still be compressed
        let columns: String = conn
            .query_row("SELECT zstd_columns('documents')", [], |row| row.get(0))
            .unwrap();
        assert_eq!(columns, "metadata");
    }

    // -------------------------------------------------------------------------
    // zstd_columns tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_zstd_columns() {
        let conn = setup_test_db();
        conn.execute(
            "CREATE TABLE documents (id INTEGER PRIMARY KEY, title TEXT, content TEXT, metadata TEXT)",
            [],
        )
        .unwrap();

        conn.query_row(
            "SELECT zstd_enable('documents', 'content', 'metadata')",
            [],
            |_| Ok(()),
        )
        .unwrap();

        let columns: String = conn
            .query_row("SELECT zstd_columns('documents')", [], |row| row.get(0))
            .unwrap();

        // Should list both compressed columns
        assert!(columns.contains("content"));
        assert!(columns.contains("metadata"));
        assert!(!columns.contains("title"));
    }

    #[test]
    fn test_zstd_columns_no_compression() {
        let conn = setup_test_db();
        conn.execute(
            "CREATE TABLE documents (id INTEGER PRIMARY KEY, content TEXT)",
            [],
        )
        .unwrap();

        let result: String = conn
            .query_row("SELECT zstd_columns('documents')", [], |row| row.get(0))
            .unwrap();

        // Should return empty string for non-compressed table
        assert!(
            result.is_empty(),
            "Should return empty string for non-compressed table"
        );
    }

    // -------------------------------------------------------------------------
    // zstd_stats tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_zstd_stats() {
        let conn = setup_test_db();
        conn.execute(
            "CREATE TABLE documents (id INTEGER PRIMARY KEY, content TEXT)",
            [],
        )
        .unwrap();

        conn.query_row("SELECT zstd_enable('documents', 'content')", [], |_| Ok(()))
            .unwrap();

        // Insert some data
        let large_content = "x".repeat(10_000);
        conn.execute(
            "INSERT INTO documents (content) VALUES (?)",
            [&large_content],
        )
        .unwrap();

        let stats: String = conn
            .query_row("SELECT zstd_stats('documents')", [], |row| row.get(0))
            .unwrap();

        // Stats should contain size information
        assert!(!stats.is_empty(), "Stats should not be empty");
        assert!(stats.contains("content"), "Stats should mention the column");
    }

    // -------------------------------------------------------------------------
    // Raw table equality join tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_zstd_raw_equality_join() {
        let conn = setup_test_db();

        // Create two tables with compressed columns
        conn.execute(
            "CREATE TABLE docs_a (id INTEGER PRIMARY KEY, content TEXT)",
            [],
        )
        .unwrap();
        conn.execute(
            "CREATE TABLE docs_b (id INTEGER PRIMARY KEY, content TEXT)",
            [],
        )
        .unwrap();

        conn.query_row("SELECT zstd_enable('docs_a', 'content')", [], |_| Ok(()))
            .unwrap();
        conn.query_row("SELECT zstd_enable('docs_b', 'content')", [], |_| Ok(()))
            .unwrap();

        // Insert matching content (large enough to be compressed)
        let matching_text = "matching text ".repeat(100);
        conn.execute("INSERT INTO docs_a (content) VALUES (?)", [&matching_text])
            .unwrap();
        conn.execute("INSERT INTO docs_b (content) VALUES (?)", [&matching_text])
            .unwrap();

        // Join using raw tables directly for efficient comparison
        let count: i32 = conn
            .query_row(
                "SELECT COUNT(*) FROM _zstd_docs_a a JOIN _zstd_docs_b b ON a.content = b.content",
                [],
                |row| row.get(0),
            )
            .unwrap();

        assert_eq!(
            count, 1,
            "Should find matching row via compressed comparison"
        );
    }

    #[test]
    fn test_zstd_raw_non_matching() {
        let conn = setup_test_db();

        conn.execute(
            "CREATE TABLE docs_a (id INTEGER PRIMARY KEY, content TEXT)",
            [],
        )
        .unwrap();
        conn.execute(
            "CREATE TABLE docs_b (id INTEGER PRIMARY KEY, content TEXT)",
            [],
        )
        .unwrap();

        conn.query_row("SELECT zstd_enable('docs_a', 'content')", [], |_| Ok(()))
            .unwrap();
        conn.query_row("SELECT zstd_enable('docs_b', 'content')", [], |_| Ok(()))
            .unwrap();

        // Insert different content
        conn.execute("INSERT INTO docs_a (content) VALUES ('text a')", [])
            .unwrap();
        conn.execute("INSERT INTO docs_b (content) VALUES ('text b')", [])
            .unwrap();

        // Join using raw tables
        let count: i32 = conn
            .query_row(
                "SELECT COUNT(*) FROM _zstd_docs_a a JOIN _zstd_docs_b b ON a.content = b.content",
                [],
                |row| row.get(0),
            )
            .unwrap();

        assert_eq!(
            count, 0,
            "Should not find matching rows for different content"
        );
    }

    // -------------------------------------------------------------------------
    // Compression determinism tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_compression_deterministic() {
        let conn = setup_test_db();

        let compressed1: Vec<u8> = conn
            .query_row("SELECT zstd_compress('Hello, World!')", [], |row| {
                row.get(0)
            })
            .unwrap();

        let compressed2: Vec<u8> = conn
            .query_row("SELECT zstd_compress('Hello, World!')", [], |row| {
                row.get(0)
            })
            .unwrap();

        assert_eq!(
            compressed1, compressed2,
            "Same input should produce same compressed output"
        );
    }

    #[test]
    fn test_compression_level_affects_output() {
        let conn = setup_test_db();
        let large_text = "x".repeat(10_000);

        let compressed_low: Vec<u8> = conn
            .query_row("SELECT zstd_compress(?, 1)", [&large_text], |row| {
                row.get(0)
            })
            .unwrap();

        let compressed_high: Vec<u8> = conn
            .query_row("SELECT zstd_compress(?, 22)", [&large_text], |row| {
                row.get(0)
            })
            .unwrap();

        assert!(
            compressed_high.len() <= compressed_low.len(),
            "Higher compression level should produce same or smaller output"
        );
    }

    // -------------------------------------------------------------------------
    // Small string fallback tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_small_string_not_compressed() {
        let conn = setup_test_db();
        conn.execute(
            "CREATE TABLE documents (id INTEGER PRIMARY KEY, content TEXT)",
            [],
        )
        .unwrap();

        conn.query_row("SELECT zstd_enable('documents', 'content')", [], |_| Ok(()))
            .unwrap();

        // Insert small string
        conn.execute("INSERT INTO documents (content) VALUES ('Hi')", [])
            .unwrap();

        // Check raw storage - should have MARKER_RAW
        let raw_content: Vec<u8> = conn
            .query_row("SELECT content FROM _zstd_documents", [], |row| row.get(0))
            .unwrap();

        assert_eq!(
            raw_content[0], MARKER_RAW,
            "Small string should be stored raw"
        );
        assert_eq!(&raw_content[1..], b"Hi", "Raw content should match");

        // Verify roundtrip still works
        let content: String = conn
            .query_row("SELECT content FROM documents", [], |row| row.get(0))
            .unwrap();
        assert_eq!(content, "Hi");
    }

    #[test]
    fn test_large_string_compressed() {
        let conn = setup_test_db();
        conn.execute(
            "CREATE TABLE documents (id INTEGER PRIMARY KEY, content TEXT)",
            [],
        )
        .unwrap();

        conn.query_row("SELECT zstd_enable('documents', 'content')", [], |_| Ok(()))
            .unwrap();

        // Insert large repetitive string
        let large_text = "x".repeat(1000);
        conn.execute("INSERT INTO documents (content) VALUES (?)", [&large_text])
            .unwrap();

        // Check raw storage - should have MARKER_COMPRESSED
        let raw_content: Vec<u8> = conn
            .query_row("SELECT content FROM _zstd_documents", [], |row| row.get(0))
            .unwrap();

        assert_eq!(
            raw_content[0], MARKER_COMPRESSED,
            "Large string should be compressed"
        );
        assert!(
            raw_content.len() < large_text.len(),
            "Compressed size should be smaller"
        );

        // Verify roundtrip still works
        let content: String = conn
            .query_row("SELECT content FROM documents", [], |row| row.get(0))
            .unwrap();
        assert_eq!(content, large_text);
    }

    // -------------------------------------------------------------------------
    // Phase 4: WHERE clause optimization tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_where_equality_filter() {
        let conn = setup_test_db();
        conn.execute(
            "CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT, content TEXT)",
            [],
        )
        .unwrap();

        conn.query_row("SELECT zstd_enable('docs', 'content')", [], |_| Ok(()))
            .unwrap();

        // Insert test data
        conn.execute(
            "INSERT INTO docs (id, title, content) VALUES (1, 'First', 'Content 1')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO docs (id, title, content) VALUES (2, 'Second', 'Content 2')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO docs (id, title, content) VALUES (3, 'Third', 'Content 3')",
            [],
        )
        .unwrap();

        // Test WHERE clause with equality
        let title: String = conn
            .query_row("SELECT title FROM docs WHERE id = 2", [], |row| row.get(0))
            .unwrap();
        assert_eq!(title, "Second");

        // Test WHERE clause on compressed column
        let content: String = conn
            .query_row(
                "SELECT content FROM docs WHERE title = 'Third'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(content, "Content 3");
    }

    #[test]
    fn test_where_multiple_conditions() {
        let conn = setup_test_db();
        conn.execute(
            "CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT, content TEXT)",
            [],
        )
        .unwrap();

        conn.query_row("SELECT zstd_enable('docs', 'content')", [], |_| Ok(()))
            .unwrap();

        // Insert test data
        for i in 1..=10 {
            conn.execute(
                "INSERT INTO docs (id, title, content) VALUES (?, ?, ?)",
                rusqlite::params![i, format!("Title {}", i), format!("Content {}", i)],
            )
            .unwrap();
        }

        // Test multiple WHERE conditions
        let count: i32 = conn
            .query_row("SELECT COUNT(*) FROM docs WHERE id > 5", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 5);

        // Test with decompression
        let results: Vec<String> = conn
            .prepare("SELECT content FROM docs WHERE id >= 8")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0], "Content 8");
        assert_eq!(results[1], "Content 9");
        assert_eq!(results[2], "Content 10");
    }

    #[test]
    fn test_where_no_results() {
        let conn = setup_test_db();
        conn.execute(
            "CREATE TABLE docs (id INTEGER PRIMARY KEY, content TEXT)",
            [],
        )
        .unwrap();

        conn.query_row("SELECT zstd_enable('docs', 'content')", [], |_| Ok(()))
            .unwrap();

        conn.execute("INSERT INTO docs (id, content) VALUES (1, 'Test')", [])
            .unwrap();

        // Query that matches nothing
        let count: i32 = conn
            .query_row("SELECT COUNT(*) FROM docs WHERE id = 999", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_explain_query_plan() {
        let conn = setup_test_db();
        conn.execute(
            "CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT, content TEXT)",
            [],
        )
        .unwrap();

        conn.query_row("SELECT zstd_enable('docs', 'content')", [], |_| Ok(()))
            .unwrap();

        // Get query plan for filtered query
        let plan: String = conn
            .query_row(
                "EXPLAIN QUERY PLAN SELECT * FROM docs WHERE id = 1",
                [],
                |row| {
                    // The detail column contains the plan info
                    row.get::<_, String>(3).or_else(|_| row.get(2))
                },
            )
            .unwrap_or_default();

        // Verify the plan shows virtual table usage
        // The exact plan format varies, but it should mention the virtual table
        println!("Query plan: {}", plan);
        // We don't assert on the plan content as it's implementation-dependent
        // The important thing is the query executes correctly with constraints
    }

    // -------------------------------------------------------------------------
    // UPSERT and ON CONFLICT DO NOTHING tests (for Senzing integration)
    // -------------------------------------------------------------------------

    #[test]
    fn test_sqlite_version() {
        let conn = setup_test_db();
        let version: String = conn
            .query_row("SELECT sqlite_version()", [], |row| row.get(0))
            .unwrap();
        println!("SQLite version: {}", version);

        // Check if version supports UPSERT for virtual tables (3.35.0+)
        let parts: Vec<&str> = version.split('.').collect();
        if parts.len() >= 2 {
            let major: i32 = parts[0].parse().unwrap_or(0);
            let minor: i32 = parts[1].parse().unwrap_or(0);
            println!("Major: {}, Minor: {}", major, minor);

            if major > 3 || (major == 3 && minor >= 35) {
                println!("✓ SQLite version supports UPSERT for virtual tables");
            } else {
                println!(
                    "✗ SQLite version may not support UPSERT for virtual tables (need 3.35.0+)"
                );
            }
        }
    }

    #[test]
    fn test_insert_or_ignore_workaround() {
        // NOTE: Modern UPSERT syntax (ON CONFLICT DO NOTHING) is not supported
        // for virtual tables in SQLite. Use INSERT OR IGNORE instead.
        let conn = setup_test_db();
        conn.execute(
            "CREATE TABLE obs_ent (id INTEGER PRIMARY KEY, features TEXT)",
            [],
        )
        .unwrap();

        conn.query_row("SELECT zstd_enable('obs_ent', 'features')", [], |_| Ok(()))
            .unwrap();

        // Insert initial record
        conn.execute(
            "INSERT INTO obs_ent (id, features) VALUES (1, 'feature1')",
            [],
        )
        .unwrap();

        // Use INSERT OR IGNORE instead of ON CONFLICT DO NOTHING
        // These are functionally equivalent - both silently ignore duplicates
        conn.execute(
            "INSERT OR IGNORE INTO obs_ent (id, features) VALUES (1, 'feature2')",
            [],
        )
        .unwrap();

        // Verify original value is unchanged
        let features: String = conn
            .query_row("SELECT features FROM obs_ent WHERE id = 1", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(
            features, "feature1",
            "INSERT OR IGNORE should not modify existing row"
        );

        // Verify only one row exists
        let count: i32 = conn
            .query_row("SELECT COUNT(*) FROM obs_ent", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_insert_or_ignore_composite_key() {
        // Test INSERT OR IGNORE with composite primary key
        let conn = setup_test_db();

        conn.execute(
            "CREATE TABLE records (
                source_id INTEGER,
                record_key TEXT,
                json_data TEXT,
                PRIMARY KEY (source_id, record_key)
            )",
            [],
        )
        .unwrap();

        conn.query_row("SELECT zstd_enable('records', 'json_data')", [], |_| Ok(()))
            .unwrap();

        // Insert initial records
        for i in 1..=10 {
            conn.execute(
                "INSERT INTO records (source_id, record_key, json_data) VALUES (1, ?, ?)",
                rusqlite::params![format!("KEY{}", i), format!("{{\"data\": {}}}", i)],
            )
            .unwrap();
        }

        // Attempt duplicate inserts with INSERT OR IGNORE
        // These should all succeed without error (duplicates silently ignored)
        for i in 1..=10 {
            conn.execute(
                "INSERT OR IGNORE INTO records (source_id, record_key, json_data)
                 VALUES (1, ?, ?)",
                rusqlite::params![format!("KEY{}", i), format!("{{\"updated\": {}}}", i)],
            )
            .unwrap();
        }

        // Verify original values unchanged
        let json: String = conn
            .query_row(
                "SELECT json_data FROM records WHERE record_key = 'KEY1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            json, "{\"data\": 1}",
            "INSERT OR IGNORE preserved original data"
        );

        // Verify still only 10 rows
        let count: i32 = conn
            .query_row("SELECT COUNT(*) FROM records", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 10);
    }

    #[test]
    fn test_insert_or_ignore_with_primary_key() {
        let conn = setup_test_db();
        conn.execute("CREATE TABLE cache (key TEXT PRIMARY KEY, value TEXT)", [])
            .unwrap();

        conn.query_row("SELECT zstd_enable('cache', 'value')", [], |_| Ok(()))
            .unwrap();

        // Insert initial
        conn.execute(
            "INSERT INTO cache (key, value) VALUES ('config', 'initial')",
            [],
        )
        .unwrap();

        // Try duplicate with INSERT OR IGNORE
        conn.execute(
            "INSERT OR IGNORE INTO cache (key, value) VALUES ('config', 'ignored')",
            [],
        )
        .unwrap();

        // Verify original unchanged
        let value: String = conn
            .query_row("SELECT value FROM cache WHERE key = 'config'", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(value, "initial");
    }

    // -------------------------------------------------------------------------
    // WITHOUT ROWID table tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_without_rowid_basic_insert_select() {
        let conn = setup_test_db();

        // Create a WITHOUT ROWID table with TEXT primary key
        conn.execute(
            "CREATE TABLE kv_store (
                key TEXT PRIMARY KEY,
                value TEXT
            ) WITHOUT ROWID",
            [],
        )
        .unwrap();

        conn.query_row("SELECT zstd_enable('kv_store', 'value')", [], |_| Ok(()))
            .unwrap();

        // Insert data
        conn.execute(
            "INSERT INTO kv_store (key, value) VALUES ('config', 'some configuration data')",
            [],
        )
        .unwrap();

        conn.execute(
            "INSERT INTO kv_store (key, value) VALUES ('settings', 'user settings')",
            [],
        )
        .unwrap();

        // Verify select works
        let value: String = conn
            .query_row(
                "SELECT value FROM kv_store WHERE key = 'config'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(value, "some configuration data");

        // Verify count
        let count: i32 = conn
            .query_row("SELECT COUNT(*) FROM kv_store", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn test_without_rowid_update() {
        let conn = setup_test_db();

        conn.execute(
            "CREATE TABLE kv_store (
                key TEXT PRIMARY KEY,
                value TEXT
            ) WITHOUT ROWID",
            [],
        )
        .unwrap();

        conn.query_row("SELECT zstd_enable('kv_store', 'value')", [], |_| Ok(()))
            .unwrap();

        // Insert initial data
        conn.execute(
            "INSERT INTO kv_store (key, value) VALUES ('test_key', 'original value')",
            [],
        )
        .unwrap();

        // Update the value
        conn.execute(
            "UPDATE kv_store SET value = 'updated value' WHERE key = 'test_key'",
            [],
        )
        .unwrap();

        // Verify update worked
        let value: String = conn
            .query_row(
                "SELECT value FROM kv_store WHERE key = 'test_key'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(value, "updated value");
    }

    #[test]
    fn test_without_rowid_delete() {
        let conn = setup_test_db();

        conn.execute(
            "CREATE TABLE kv_store (
                key TEXT PRIMARY KEY,
                value TEXT
            ) WITHOUT ROWID",
            [],
        )
        .unwrap();

        conn.query_row("SELECT zstd_enable('kv_store', 'value')", [], |_| Ok(()))
            .unwrap();

        // Insert data
        conn.execute(
            "INSERT INTO kv_store (key, value) VALUES ('to_delete', 'will be deleted')",
            [],
        )
        .unwrap();

        // Delete
        conn.execute("DELETE FROM kv_store WHERE key = 'to_delete'", [])
            .unwrap();

        // Verify deletion
        let count: i32 = conn
            .query_row("SELECT COUNT(*) FROM kv_store", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_without_rowid_composite_key() {
        let conn = setup_test_db();

        // Create WITHOUT ROWID table with composite primary key
        conn.execute(
            "CREATE TABLE metrics (
                source TEXT,
                metric_name TEXT,
                value TEXT,
                PRIMARY KEY (source, metric_name)
            ) WITHOUT ROWID",
            [],
        )
        .unwrap();

        conn.query_row("SELECT zstd_enable('metrics', 'value')", [], |_| Ok(()))
            .unwrap();

        // Insert data with composite keys
        conn.execute(
            "INSERT INTO metrics (source, metric_name, value) VALUES ('server1', 'cpu', '45%')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO metrics (source, metric_name, value) VALUES ('server1', 'memory', '8GB')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO metrics (source, metric_name, value) VALUES ('server2', 'cpu', '60%')",
            [],
        )
        .unwrap();

        // Verify select with composite key
        let value: String = conn
            .query_row(
                "SELECT value FROM metrics WHERE source = 'server1' AND metric_name = 'cpu'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(value, "45%");

        // Verify count
        let count: i32 = conn
            .query_row("SELECT COUNT(*) FROM metrics", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 3);
    }

    #[test]
    fn test_without_rowid_composite_key_update() {
        let conn = setup_test_db();

        conn.execute(
            "CREATE TABLE metrics (
                source TEXT,
                metric_name TEXT,
                value TEXT,
                PRIMARY KEY (source, metric_name)
            ) WITHOUT ROWID",
            [],
        )
        .unwrap();

        conn.query_row("SELECT zstd_enable('metrics', 'value')", [], |_| Ok(()))
            .unwrap();

        conn.execute(
            "INSERT INTO metrics (source, metric_name, value) VALUES ('server1', 'cpu', '45%')",
            [],
        )
        .unwrap();

        // Update with composite key
        conn.execute(
            "UPDATE metrics SET value = '95%' WHERE source = 'server1' AND metric_name = 'cpu'",
            [],
        )
        .unwrap();

        let value: String = conn
            .query_row(
                "SELECT value FROM metrics WHERE source = 'server1' AND metric_name = 'cpu'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(value, "95%");
    }

    #[test]
    fn test_without_rowid_insert_or_ignore() {
        let conn = setup_test_db();

        conn.execute(
            "CREATE TABLE kv_store (
                key TEXT PRIMARY KEY,
                value TEXT
            ) WITHOUT ROWID",
            [],
        )
        .unwrap();

        conn.query_row("SELECT zstd_enable('kv_store', 'value')", [], |_| Ok(()))
            .unwrap();

        // Insert initial value
        conn.execute(
            "INSERT INTO kv_store (key, value) VALUES ('config', 'original')",
            [],
        )
        .unwrap();

        // Try to insert duplicate - should be ignored
        conn.execute(
            "INSERT OR IGNORE INTO kv_store (key, value) VALUES ('config', 'duplicate')",
            [],
        )
        .unwrap();

        // Verify original value preserved
        let value: String = conn
            .query_row(
                "SELECT value FROM kv_store WHERE key = 'config'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(value, "original");

        // Verify only one row
        let count: i32 = conn
            .query_row("SELECT COUNT(*) FROM kv_store", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_without_rowid_insert_or_replace() {
        let conn = setup_test_db();

        conn.execute(
            "CREATE TABLE kv_store (
                key TEXT PRIMARY KEY,
                value TEXT
            ) WITHOUT ROWID",
            [],
        )
        .unwrap();

        conn.query_row("SELECT zstd_enable('kv_store', 'value')", [], |_| Ok(()))
            .unwrap();

        // Insert initial value
        conn.execute(
            "INSERT INTO kv_store (key, value) VALUES ('config', 'original')",
            [],
        )
        .unwrap();

        // Replace with new value
        conn.execute(
            "INSERT OR REPLACE INTO kv_store (key, value) VALUES ('config', 'replaced')",
            [],
        )
        .unwrap();

        // Verify value was replaced
        let value: String = conn
            .query_row(
                "SELECT value FROM kv_store WHERE key = 'config'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(value, "replaced");

        // Verify still only one row
        let count: i32 = conn
            .query_row("SELECT COUNT(*) FROM kv_store", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_without_rowid_integer_pk() {
        let conn = setup_test_db();

        // WITHOUT ROWID with INTEGER PRIMARY KEY (NOT an alias for rowid)
        conn.execute(
            "CREATE TABLE items (
                id INTEGER PRIMARY KEY,
                data TEXT
            ) WITHOUT ROWID",
            [],
        )
        .unwrap();

        conn.query_row("SELECT zstd_enable('items', 'data')", [], |_| Ok(()))
            .unwrap();

        // Insert data
        conn.execute("INSERT INTO items (id, data) VALUES (100, 'item 100')", [])
            .unwrap();
        conn.execute("INSERT INTO items (id, data) VALUES (200, 'item 200')", [])
            .unwrap();

        // Verify select
        let data: String = conn
            .query_row("SELECT data FROM items WHERE id = 100", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(data, "item 100");

        // Verify update
        conn.execute("UPDATE items SET data = 'updated 100' WHERE id = 100", [])
            .unwrap();

        let data: String = conn
            .query_row("SELECT data FROM items WHERE id = 100", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(data, "updated 100");

        // Verify delete
        conn.execute("DELETE FROM items WHERE id = 100", [])
            .unwrap();

        let count: i32 = conn
            .query_row("SELECT COUNT(*) FROM items", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_without_rowid_large_compressed_value() {
        let conn = setup_test_db();

        conn.execute(
            "CREATE TABLE documents (
                doc_id TEXT PRIMARY KEY,
                content TEXT
            ) WITHOUT ROWID",
            [],
        )
        .unwrap();

        conn.query_row("SELECT zstd_enable('documents', 'content')", [], |_| Ok(()))
            .unwrap();

        // Insert large content that will be compressed
        let large_content = "x".repeat(10_000);
        conn.execute(
            "INSERT INTO documents (doc_id, content) VALUES ('doc1', ?)",
            [&large_content],
        )
        .unwrap();

        // Verify roundtrip
        let content: String = conn
            .query_row(
                "SELECT content FROM documents WHERE doc_id = 'doc1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(content, large_content);

        // Verify compression happened (check raw table)
        let raw_content: Vec<u8> = conn
            .query_row("SELECT content FROM _zstd_documents", [], |row| row.get(0))
            .unwrap();
        assert_eq!(
            raw_content[0], MARKER_COMPRESSED,
            "Large content should be compressed"
        );
        assert!(
            raw_content.len() < large_content.len(),
            "Compressed size should be smaller"
        );
    }

    #[test]
    fn test_without_rowid_multiple_text_columns() {
        let conn = setup_test_db();

        conn.execute(
            "CREATE TABLE records (
                id TEXT PRIMARY KEY,
                field1 TEXT,
                field2 TEXT,
                field3 TEXT
            ) WITHOUT ROWID",
            [],
        )
        .unwrap();

        // Enable compression on multiple columns
        conn.query_row(
            "SELECT zstd_enable('records', 'field1', 'field2', 'field3')",
            [],
            |_| Ok(()),
        )
        .unwrap();

        // Insert data
        conn.execute(
            "INSERT INTO records (id, field1, field2, field3) VALUES ('rec1', 'value1', 'value2', 'value3')",
            [],
        )
        .unwrap();

        // Verify all fields roundtrip correctly
        let (f1, f2, f3): (String, String, String) = conn
            .query_row(
                "SELECT field1, field2, field3 FROM records WHERE id = 'rec1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(f1, "value1");
        assert_eq!(f2, "value2");
        assert_eq!(f3, "value3");

        // Update one field
        conn.execute(
            "UPDATE records SET field2 = 'updated2' WHERE id = 'rec1'",
            [],
        )
        .unwrap();

        let f2_updated: String = conn
            .query_row("SELECT field2 FROM records WHERE id = 'rec1'", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(f2_updated, "updated2");
    }
}
