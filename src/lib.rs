//! SQLite extension for seamless TEXT field compression using Zstandard (zstd).
//!
//! This extension provides transparent compression/decompression of TEXT columns
//! through a view/trigger mechanism with smart compression (marker byte protocol).

use rusqlite::functions::FunctionFlags;
use rusqlite::types::{ToSqlOutput, Value, ValueRef};
use rusqlite::{Connection, Result};

#[cfg(feature = "loadable_extension")]
use rusqlite::ffi;
#[cfg(feature = "loadable_extension")]
use std::ffi::c_int;
#[cfg(feature = "loadable_extension")]
use std::os::raw::c_char;

/// Default compression level (zstd range is 1-22, 3 is default)
const DEFAULT_COMPRESSION_LEVEL: i32 = 3;

/// Metadata table name for storing compression configuration
const CONFIG_TABLE: &str = "_zstd_config";

/// Prefix for renamed tables
const TABLE_PREFIX: &str = "_zstd_";

/// Marker bytes for stored values
const MARKER_RAW: u8 = 0x00;
const MARKER_COMPRESSED: u8 = 0x01;

/// Minimum size threshold for compression (bytes). Strings smaller than this
/// are stored raw since compression overhead would outweigh benefits.
const MIN_COMPRESS_SIZE: usize = 64;

// =============================================================================
// Compression/Decompression with Marker Byte
// =============================================================================

/// Compress text if beneficial, prepending marker byte.
/// Returns MARKER_RAW + raw bytes if compression isn't beneficial,
/// or MARKER_COMPRESSED + compressed bytes otherwise.
fn compress_with_marker(text: &str, level: i32) -> std::result::Result<Vec<u8>, String> {
    let bytes = text.as_bytes();

    // Skip compression for small strings
    if bytes.len() < MIN_COMPRESS_SIZE {
        let mut result = Vec::with_capacity(1 + bytes.len());
        result.push(MARKER_RAW);
        result.extend_from_slice(bytes);
        return Ok(result);
    }

    // Try compression
    let compressed =
        zstd::encode_all(bytes, level).map_err(|e| format!("zstd compression failed: {}", e))?;

    // Use compressed only if it's actually smaller (accounting for marker byte)
    if compressed.len() < bytes.len() {
        let mut result = Vec::with_capacity(1 + compressed.len());
        result.push(MARKER_COMPRESSED);
        result.extend_from_slice(&compressed);
        Ok(result)
    } else {
        let mut result = Vec::with_capacity(1 + bytes.len());
        result.push(MARKER_RAW);
        result.extend_from_slice(bytes);
        Ok(result)
    }
}

/// Decompress data with marker byte.
/// Handles both MARKER_RAW (returns as-is) and MARKER_COMPRESSED (decompresses).
fn decompress_with_marker(data: &[u8]) -> std::result::Result<String, String> {
    if data.is_empty() {
        return Err("empty data".to_string());
    }

    match data[0] {
        MARKER_RAW => String::from_utf8(data[1..].to_vec())
            .map_err(|e| format!("invalid UTF-8 in raw data: {}", e)),
        MARKER_COMPRESSED => {
            let decompressed = zstd::decode_all(&data[1..])
                .map_err(|e| format!("zstd decompression failed: {}", e))?;
            String::from_utf8(decompressed)
                .map_err(|e| format!("decompressed data is not valid UTF-8: {}", e))
        }
        marker => Err(format!("unknown marker byte: 0x{:02x}", marker)),
    }
}

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

/// Get all columns from a table's schema with their types.
fn get_all_columns(
    conn: &Connection,
    table: &str,
) -> std::result::Result<Vec<(String, String)>, String> {
    let mut stmt = conn
        .prepare(&format!("PRAGMA table_info('{}')", table))
        .map_err(|e| format!("failed to get table info: {}", e))?;

    let columns: Vec<(String, String)> = stmt
        .query_map([], |row| {
            let name: String = row.get(1)?;
            let col_type: String = row.get(2)?;
            Ok((name, col_type))
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

/// Enable compression for a table.
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

    // Check if already enabled (view exists with this name)
    let existing: Option<String> = conn
        .query_row(
            "SELECT name FROM sqlite_master WHERE type='view' AND name=?",
            [table],
            |row| row.get(0),
        )
        .ok();

    if existing.is_some() {
        return Err(format!(
            "table '{}' is already compressed or is a view",
            table
        ));
    }

    // Get all columns and their types
    let all_columns = get_all_columns(conn, table)?;

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
        // Rename original table
        conn.execute(
            &format!("ALTER TABLE \"{}\" RENAME TO \"{}\"", table, raw_table),
            [],
        )
        .map_err(|e| format!("failed to rename table: {}", e))?;

        // Build SELECT list for view (decompress compressed columns)
        // Include rowid for UPDATE/DELETE triggers to work
        let mut select_list: Vec<String> = vec![format!("\"{}\".rowid AS rowid", raw_table)];
        select_list.extend(all_columns.iter().map(|(name, _)| {
            if compress_columns.contains(name) {
                format!("zstd_decompress_marked(\"{}\") AS \"{}\"", name, name)
            } else {
                format!("\"{}\"", name)
            }
        }));

        // Create view
        let create_view = format!(
            "CREATE VIEW \"{}\" AS SELECT {} FROM \"{}\"",
            table,
            select_list.join(", "),
            raw_table
        );
        conn.execute(&create_view, [])
            .map_err(|e| format!("failed to create view: {}", e))?;

        // Build column lists for triggers
        let column_names: Vec<String> = all_columns
            .iter()
            .map(|(name, _)| format!("\"{}\"", name))
            .collect();
        let insert_values: Vec<String> = all_columns
            .iter()
            .map(|(name, _)| {
                if compress_columns.contains(name) {
                    format!("zstd_compress_marked(NEW.\"{}\")", name)
                } else {
                    format!("NEW.\"{}\"", name)
                }
            })
            .collect();

        // Create INSERT trigger
        let insert_trigger = format!(
            "CREATE TRIGGER \"_zstd_{}_insert\" INSTEAD OF INSERT ON \"{}\"
            BEGIN
                INSERT INTO \"{}\" ({}) VALUES ({});
            END",
            table,
            table,
            raw_table,
            column_names.join(", "),
            insert_values.join(", ")
        );
        conn.execute(&insert_trigger, [])
            .map_err(|e| format!("failed to create insert trigger: {}", e))?;

        // Create UPDATE trigger
        let update_sets: Vec<String> = all_columns
            .iter()
            .map(|(name, _)| {
                if compress_columns.contains(name) {
                    format!("\"{}\" = zstd_compress_marked(NEW.\"{}\")", name, name)
                } else {
                    format!("\"{}\" = NEW.\"{}\"", name, name)
                }
            })
            .collect();

        let update_trigger = format!(
            "CREATE TRIGGER \"_zstd_{}_update\" INSTEAD OF UPDATE ON \"{}\"
            BEGIN
                UPDATE \"{}\" SET {} WHERE rowid = OLD.rowid;
            END",
            table,
            table,
            raw_table,
            update_sets.join(", ")
        );
        conn.execute(&update_trigger, [])
            .map_err(|e| format!("failed to create update trigger: {}", e))?;

        // Create DELETE trigger
        let delete_trigger = format!(
            "CREATE TRIGGER \"_zstd_{}_delete\" INSTEAD OF DELETE ON \"{}\"
            BEGIN
                DELETE FROM \"{}\" WHERE rowid = OLD.rowid;
            END",
            table, table, raw_table
        );
        conn.execute(&delete_trigger, [])
            .map_err(|e| format!("failed to create delete trigger: {}", e))?;

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

                // Decompress the column in the raw table
                conn.execute(
                    &format!(
                        "UPDATE \"{}\" SET \"{}\" = zstd_decompress_marked(\"{}\")",
                        raw_table, col, col
                    ),
                    [],
                )
                .map_err(|e| format!("failed to decompress column: {}", e))?;

                // Recreate view and triggers with updated column list
                let remaining_columns: Vec<String> =
                    columns.into_iter().filter(|c| c != col).collect();

                recreate_view_and_triggers(conn, table, &raw_table, &remaining_columns)?;

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

/// Disable compression on entire table - helper function.
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

    // Decompress all compressed columns
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

    // Drop triggers
    conn.execute(
        &format!("DROP TRIGGER IF EXISTS \"_zstd_{}_insert\"", table),
        [],
    )
    .map_err(|e| format!("failed to drop insert trigger: {}", e))?;
    conn.execute(
        &format!("DROP TRIGGER IF EXISTS \"_zstd_{}_update\"", table),
        [],
    )
    .map_err(|e| format!("failed to drop update trigger: {}", e))?;
    conn.execute(
        &format!("DROP TRIGGER IF EXISTS \"_zstd_{}_delete\"", table),
        [],
    )
    .map_err(|e| format!("failed to drop delete trigger: {}", e))?;

    // Drop view
    conn.execute(&format!("DROP VIEW IF EXISTS \"{}\"", table), [])
        .map_err(|e| format!("failed to drop view: {}", e))?;

    // Rename table back
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

/// Recreate view and triggers after modifying compressed columns.
fn recreate_view_and_triggers(
    conn: &Connection,
    table: &str,
    raw_table: &str,
    compress_columns: &[String],
) -> std::result::Result<(), String> {
    // Get all columns
    let all_columns = get_all_columns(conn, raw_table)?;

    // Drop existing view and triggers
    conn.execute(
        &format!("DROP TRIGGER IF EXISTS \"_zstd_{}_insert\"", table),
        [],
    )
    .map_err(|e| format!("failed to drop insert trigger: {}", e))?;
    conn.execute(
        &format!("DROP TRIGGER IF EXISTS \"_zstd_{}_update\"", table),
        [],
    )
    .map_err(|e| format!("failed to drop update trigger: {}", e))?;
    conn.execute(
        &format!("DROP TRIGGER IF EXISTS \"_zstd_{}_delete\"", table),
        [],
    )
    .map_err(|e| format!("failed to drop delete trigger: {}", e))?;
    conn.execute(&format!("DROP VIEW IF EXISTS \"{}\"", table), [])
        .map_err(|e| format!("failed to drop view: {}", e))?;

    // Recreate view with rowid for UPDATE/DELETE triggers
    let mut select_list: Vec<String> = vec![format!("\"{}\".rowid AS rowid", raw_table)];
    select_list.extend(all_columns.iter().map(|(name, _)| {
        if compress_columns.contains(name) {
            format!("zstd_decompress_marked(\"{}\") AS \"{}\"", name, name)
        } else {
            format!("\"{}\"", name)
        }
    }));

    let create_view = format!(
        "CREATE VIEW \"{}\" AS SELECT {} FROM \"{}\"",
        table,
        select_list.join(", "),
        raw_table
    );
    conn.execute(&create_view, [])
        .map_err(|e| format!("failed to create view: {}", e))?;

    // Recreate triggers
    let column_names: Vec<String> = all_columns
        .iter()
        .map(|(name, _)| format!("\"{}\"", name))
        .collect();
    let insert_values: Vec<String> = all_columns
        .iter()
        .map(|(name, _)| {
            if compress_columns.contains(name) {
                format!("zstd_compress_marked(NEW.\"{}\")", name)
            } else {
                format!("NEW.\"{}\"", name)
            }
        })
        .collect();

    let insert_trigger = format!(
        "CREATE TRIGGER \"_zstd_{}_insert\" INSTEAD OF INSERT ON \"{}\"
        BEGIN
            INSERT INTO \"{}\" ({}) VALUES ({});
        END",
        table,
        table,
        raw_table,
        column_names.join(", "),
        insert_values.join(", ")
    );
    conn.execute(&insert_trigger, [])
        .map_err(|e| format!("failed to create insert trigger: {}", e))?;

    let update_sets: Vec<String> = all_columns
        .iter()
        .map(|(name, _)| {
            if compress_columns.contains(name) {
                format!("\"{}\" = zstd_compress_marked(NEW.\"{}\")", name, name)
            } else {
                format!("\"{}\" = NEW.\"{}\"", name, name)
            }
        })
        .collect();

    let update_trigger = format!(
        "CREATE TRIGGER \"_zstd_{}_update\" INSTEAD OF UPDATE ON \"{}\"
        BEGIN
            UPDATE \"{}\" SET {} WHERE rowid = OLD.rowid;
        END",
        table,
        table,
        raw_table,
        update_sets.join(", ")
    );
    conn.execute(&update_trigger, [])
        .map_err(|e| format!("failed to create update trigger: {}", e))?;

    let delete_trigger = format!(
        "CREATE TRIGGER \"_zstd_{}_delete\" INSTEAD OF DELETE ON \"{}\"
        BEGIN
            DELETE FROM \"{}\" WHERE rowid = OLD.rowid;
        END",
        table, table, raw_table
    );
    conn.execute(&delete_trigger, [])
        .map_err(|e| format!("failed to create delete trigger: {}", e))?;

    Ok(())
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
pub fn register_functions(conn: &Connection) -> Result<()> {
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

        // Verify the view exists
        let view_exists: i32 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='view' AND name='documents'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(view_exists, 1, "View should be created");

        // Verify the raw table exists
        let raw_table_exists: i32 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='_zstd_documents'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(raw_table_exists, 1, "Raw table should exist");
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
}
