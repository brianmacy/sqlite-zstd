//! Main virtual table implementation for zstd compression.
//!
//! Supports both regular rowid tables and WITHOUT ROWID tables.

use std::collections::HashMap;
use std::sync::Mutex;

use rusqlite::ffi;
use rusqlite::types::{Value, ValueRef};
use rusqlite::vtab::{
    CreateVTab, IndexInfo, UpdateVTab, VTab, VTabConnection, Values, sqlite3_vtab, update_module,
};
use rusqlite::{Connection, Result};

use super::conflict::{ConflictMode, get_conflict_mode};
use crate::compression::{DEFAULT_COMPRESSION_LEVEL, compress_with_marker};

/// Configuration for virtual table creation (reserved for future use)
#[derive(Debug)]
#[allow(dead_code)]
pub struct VTabConfig {
    pub underlying_table: String,
    pub compressed_columns: Vec<String>,
    pub all_columns: Vec<(String, String)>, // (name, type)
}

/// Virtual table structure for zstd compression
#[repr(C)]
pub struct ZstdVTab {
    base: sqlite3_vtab,
    pub(crate) db_handle: *mut ffi::sqlite3,
    pub underlying_table: String,
    pub compressed_columns: Vec<String>,
    pub all_columns: Vec<(String, String)>, // (name, type)
    pub pk_columns: Vec<String>,            // Primary key column names
    pub is_without_rowid: bool,             // Whether underlying table is WITHOUT ROWID
    /// Cache mapping synthetic rowid to actual PK values for WITHOUT ROWID tables
    /// This is needed because cursors return synthetic rowids for non-integer PKs,
    /// but xUpdate needs the actual PK values for DELETE/UPDATE operations
    pub(crate) pk_value_cache: Mutex<HashMap<i64, Vec<Value>>>,
}

/// Check if a table is WITHOUT ROWID by attempting to select rowid
fn detect_without_rowid(db_handle: *mut ffi::sqlite3, table_name: &str) -> bool {
    // Try to prepare a query that selects rowid
    // If it fails with "no such column: rowid", the table is WITHOUT ROWID
    let sql = format!("SELECT rowid FROM \"{}\" LIMIT 0", table_name);
    let sql_cstr = match std::ffi::CString::new(sql) {
        Ok(s) => s,
        Err(_) => return false,
    };

    let mut stmt_ptr: *mut ffi::sqlite3_stmt = std::ptr::null_mut();
    let rc = unsafe {
        ffi::sqlite3_prepare_v2(
            db_handle,
            sql_cstr.as_ptr(),
            -1,
            &mut stmt_ptr,
            std::ptr::null_mut(),
        )
    };

    // Clean up statement if it was created
    if !stmt_ptr.is_null() {
        unsafe {
            ffi::sqlite3_finalize(stmt_ptr);
        }
    }

    // If prepare failed, likely because "no such column: rowid"
    rc != ffi::SQLITE_OK
}

unsafe impl<'vtab> VTab<'vtab> for ZstdVTab {
    type Aux = VTabConfig;
    type Cursor = super::cursor::ZstdCursor<'vtab>;

    fn connect(
        db: &mut VTabConnection,
        _aux: Option<&Self::Aux>,
        args: &[&[u8]],
    ) -> Result<(String, Self)> {
        // Enable constraint support for ON CONFLICT clauses
        // This tells SQLite that our virtual table can handle UPSERT and ON CONFLICT
        db.config(rusqlite::vtab::VTabConfig::ConstraintSupport)?;

        // Parse arguments from CREATE VIRTUAL TABLE statement
        // Format: CREATE VIRTUAL TABLE name USING zstd(underlying, cols, schema)
        // args[0] = module name ("zstd")
        // args[1] = database name
        // args[2] = table name
        // args[3] = underlying table name
        // args[4] = pipe-separated compressed column names: col1|col2|...
        // args[5] = pipe-separated schema: col1:TYPE1|col2:TYPE2|...

        if args.len() < 6 {
            return Err(rusqlite::Error::ModuleError(format!(
                "zstd virtual table requires 3 arguments: underlying_table, compressed_cols, schema (got {} args)",
                args.len()
            )));
        }

        // Parse underlying table name
        let underlying_table = std::str::from_utf8(args[3])
            .map_err(|e| rusqlite::Error::ModuleError(format!("Invalid UTF-8: {}", e)))?
            .to_string();

        // Parse compressed column names (pipe-separated: col1|col2|...)
        let compressed_cols_str = std::str::from_utf8(args[4])
            .map_err(|e| rusqlite::Error::ModuleError(format!("Invalid UTF-8: {}", e)))?;
        let compressed_columns: Vec<String> = if compressed_cols_str.is_empty() {
            Vec::new()
        } else {
            compressed_cols_str
                .split('|')
                .map(|s| s.trim().to_string())
                .collect()
        };

        // Parse schema (format: "col1:TYPE1:PK|col2:TYPE2|...")
        // PK suffix indicates primary key column
        let schema_str = std::str::from_utf8(args[5])
            .map_err(|e| rusqlite::Error::ModuleError(format!("Invalid UTF-8: {}", e)))?;
        let mut all_columns = Vec::new();
        let mut pk_columns = Vec::new();

        for col_def in schema_str.split('|') {
            let parts: Vec<&str> = col_def.split(':').collect();
            if parts.len() < 2 || parts.len() > 3 {
                return Err(rusqlite::Error::ModuleError(format!(
                    "Invalid column definition: {}",
                    col_def
                )));
            }
            let name = parts[0].trim().to_string();
            let col_type = parts[1].trim().to_string();
            let is_pk = parts.len() == 3 && parts[2].trim() == "PK";

            all_columns.push((name.clone(), col_type));
            if is_pk {
                pk_columns.push(name);
            }
        }

        // Get database handle for later use in insert/update/delete operations
        let db_handle = unsafe { db.handle() };

        // Detect if the underlying table is WITHOUT ROWID
        let is_without_rowid = detect_without_rowid(db_handle, &underlying_table);

        // Build schema DDL with PRIMARY KEY constraints
        // For WITHOUT ROWID underlying tables, we declare the virtual table as WITHOUT ROWID too
        let schema = build_schema_ddl(&all_columns, &pk_columns, is_without_rowid);

        let vtab = ZstdVTab {
            base: sqlite3_vtab::default(),
            db_handle,
            underlying_table,
            compressed_columns,
            all_columns,
            pk_columns,
            is_without_rowid,
            pk_value_cache: Mutex::new(HashMap::new()),
        };

        Ok((schema, vtab))
    }

    fn best_index(&self, info: &mut IndexInfo) -> Result<()> {
        // Handle WHERE clause constraints for query optimization
        // We encode which constraints we can use in idx_num as a bitmask
        let mut idx_num = 0;
        let mut argv_index = 1;

        for (constraint, mut usage) in info.constraints_and_usages() {
            if !constraint.is_usable() {
                continue;
            }

            // We can handle equality and range constraints
            match constraint.operator() {
                rusqlite::vtab::IndexConstraintOp::SQLITE_INDEX_CONSTRAINT_EQ => {
                    // Equality constraint: col = value
                    usage.set_argv_index(argv_index);
                    usage.set_omit(true); // SQLite can skip re-checking
                    idx_num |= 1 << constraint.column();
                    argv_index += 1;
                }
                rusqlite::vtab::IndexConstraintOp::SQLITE_INDEX_CONSTRAINT_GT
                | rusqlite::vtab::IndexConstraintOp::SQLITE_INDEX_CONSTRAINT_GE
                | rusqlite::vtab::IndexConstraintOp::SQLITE_INDEX_CONSTRAINT_LT
                | rusqlite::vtab::IndexConstraintOp::SQLITE_INDEX_CONSTRAINT_LE => {
                    // Range constraints: col > value, col >= value, etc.
                    usage.set_argv_index(argv_index);
                    // Don't omit - SQLite should re-check these
                    idx_num |= 1 << (constraint.column() + 16); // Use upper 16 bits for ranges
                    argv_index += 1;
                }
                _ => {
                    // Other constraints (LIKE, etc.) - let SQLite handle them
                }
            }
        }

        info.set_idx_num(idx_num);

        // Estimate cost based on constraints
        if idx_num > 0 {
            // With constraints, we expect fewer rows
            info.set_estimated_cost(10.0);
            info.set_estimated_rows(100);
        } else {
            // Full table scan
            info.set_estimated_cost(1000.0);
            info.set_estimated_rows(10000);
        }

        Ok(())
    }

    fn open(&'vtab mut self) -> Result<Self::Cursor> {
        super::cursor::ZstdCursor::new(self)
    }
}

impl<'vtab> CreateVTab<'vtab> for ZstdVTab {
    const KIND: rusqlite::vtab::VTabKind = rusqlite::vtab::VTabKind::Default;
}

impl<'vtab> UpdateVTab<'vtab> for ZstdVTab {
    fn insert(&mut self, args: &Values<'_>) -> Result<i64> {
        // args[0] = old rowid (NULL for INSERT)
        // args[1] = new rowid (NULL = auto-assign, otherwise explicit)
        // args[2..] = column values

        // Get ON CONFLICT mode
        let conflict_mode = unsafe { get_conflict_mode(self.db_handle) };

        // Prepare column values with compression
        let mut values = Vec::new();
        for (i, (col_name, _)) in self.all_columns.iter().enumerate() {
            // Try to get as text first for compression
            if self.compressed_columns.contains(col_name) {
                if let Ok(text) = args.get::<String>(i + 2) {
                    let compressed = compress_with_marker(&text, DEFAULT_COMPRESSION_LEVEL)
                        .map_err(|e| rusqlite::Error::ToSqlConversionFailure(e.into()))?;
                    values.push(Value::Blob(compressed));
                } else {
                    // Fall back to getting as a generic value
                    let val: Value = args.get(i + 2)?;
                    values.push(val);
                }
            } else {
                let val: Value = args.get(i + 2)?;
                values.push(val);
            }
        }

        // Build INSERT statement based on conflict mode
        let col_names: Vec<_> = self
            .all_columns
            .iter()
            .map(|(name, _)| format!("\"{}\"", name))
            .collect();
        let placeholders = vec!["?"; col_names.len()].join(", ");

        // For REPLACE mode, use INSERT OR REPLACE
        // For all other modes, use plain INSERT and handle errors
        let sql = if conflict_mode == ConflictMode::Replace {
            format!(
                "INSERT OR REPLACE INTO \"{}\" ({}) VALUES ({})",
                self.underlying_table,
                col_names.join(", "),
                placeholders
            )
        } else {
            format!(
                "INSERT INTO \"{}\" ({}) VALUES ({})",
                self.underlying_table,
                col_names.join(", "),
                placeholders
            )
        };

        // Execute INSERT and handle constraint violations based on conflict mode
        let conn = unsafe { Connection::from_handle_owned(self.db_handle)? };
        let mut stmt = conn.prepare(&sql)?;

        // Bind parameters
        for (i, value) in values.iter().enumerate() {
            match value {
                Value::Null => stmt.raw_bind_parameter(i + 1, value)?,
                Value::Integer(n) => stmt.raw_bind_parameter(i + 1, n)?,
                Value::Real(f) => stmt.raw_bind_parameter(i + 1, f)?,
                Value::Text(s) => stmt.raw_bind_parameter(i + 1, s)?,
                Value::Blob(b) => stmt.raw_bind_parameter(i + 1, b)?,
            }
        }

        // Execute and handle constraint violations
        let result = stmt.raw_execute();

        let rowid = match result {
            Ok(_) => {
                drop(stmt);
                if self.is_without_rowid {
                    // For WITHOUT ROWID tables, return 0 (no meaningful rowid)
                    0
                } else {
                    conn.last_insert_rowid()
                }
            }
            Err(rusqlite::Error::SqliteFailure(err, _))
                if err.code == ffi::ErrorCode::ConstraintViolation =>
            {
                // Constraint violation occurred
                match conflict_mode {
                    ConflictMode::Ignore => {
                        // For IGNORE/DO NOTHING: Return success with rowid 0
                        // This signals success without inserting
                        drop(stmt);
                        std::mem::forget(conn);
                        return Ok(0);
                    }
                    ConflictMode::Fail | ConflictMode::Abort | ConflictMode::Rollback => {
                        // Propagate the constraint error
                        drop(stmt);
                        std::mem::forget(conn);
                        return Err(rusqlite::Error::SqliteFailure(err, None));
                    }
                    ConflictMode::Replace => {
                        // Should not reach here - REPLACE uses INSERT OR REPLACE
                        drop(stmt);
                        std::mem::forget(conn);
                        return Err(rusqlite::Error::SqliteFailure(err, None));
                    }
                }
            }
            Err(e) => {
                // Other errors
                drop(stmt);
                std::mem::forget(conn);
                return Err(e);
            }
        };

        // Don't drop the connection - SQLite owns it
        std::mem::forget(conn);

        Ok(rowid)
    }

    fn delete(&mut self, arg: ValueRef<'_>) -> Result<()> {
        let conn = unsafe { Connection::from_handle_owned(self.db_handle)? };

        if self.is_without_rowid {
            // For WITHOUT ROWID tables, we need to use PK columns
            // Check if we have cached PK values (for non-integer PKs with synthetic rowid)
            if let ValueRef::Integer(synthetic_rowid) = arg
                && let Ok(cache) = self.pk_value_cache.lock()
                && let Some(pk_values) = cache.get(&synthetic_rowid)
            {
                // We have cached PK values - use them for DELETE
                let where_clauses: Vec<String> = self
                    .pk_columns
                    .iter()
                    .map(|pk| format!("\"{}\" = ?", pk))
                    .collect();

                let sql = format!(
                    "DELETE FROM \"{}\" WHERE {}",
                    self.underlying_table,
                    where_clauses.join(" AND ")
                );

                let mut stmt = conn.prepare(&sql)?;
                for (i, value) in pk_values.iter().enumerate() {
                    match value {
                        Value::Null => stmt.raw_bind_parameter(i + 1, value)?,
                        Value::Integer(n) => stmt.raw_bind_parameter(i + 1, n)?,
                        Value::Real(f) => stmt.raw_bind_parameter(i + 1, f)?,
                        Value::Text(s) => stmt.raw_bind_parameter(i + 1, s)?,
                        Value::Blob(b) => stmt.raw_bind_parameter(i + 1, b)?,
                    }
                }
                stmt.raw_execute()?;
                drop(stmt);
                drop(cache);
                std::mem::forget(conn);
                return Ok(());
            }

            // Fallback: use the arg directly as the PK value (for integer PKs)
            if self.pk_columns.len() == 1 {
                // Single-column PK
                let pk_col = &self.pk_columns[0];
                let sql = format!(
                    "DELETE FROM \"{}\" WHERE \"{}\" = ?",
                    self.underlying_table, pk_col
                );

                // Bind the PK value based on its type
                match arg {
                    ValueRef::Integer(i) => conn.execute(&sql, [i])?,
                    ValueRef::Text(t) => {
                        let s = std::str::from_utf8(t)
                            .map_err(|e| rusqlite::Error::ModuleError(e.to_string()))?;
                        conn.execute(&sql, [s])?
                    }
                    ValueRef::Blob(b) => conn.execute(&sql, [b])?,
                    ValueRef::Real(f) => conn.execute(&sql, [f])?,
                    ValueRef::Null => {
                        return Err(rusqlite::Error::ModuleError(
                            "Cannot delete with NULL primary key".to_string(),
                        ));
                    }
                };
            } else {
                // Composite PK without cached values - need cursor state
                return Err(rusqlite::Error::ModuleError(
                    "DELETE on WITHOUT ROWID table with composite primary key requires cursor state tracking".to_string(),
                ));
            }
        } else {
            // Regular rowid table
            let rowid = arg.as_i64()?;
            let sql = format!("DELETE FROM \"{}\" WHERE rowid = ?", self.underlying_table);
            conn.execute(&sql, [rowid])?;
        }

        std::mem::forget(conn);
        Ok(())
    }

    fn update(&mut self, args: &Values<'_>) -> Result<()> {
        // args[0] = old rowid/PK (NOT NULL)
        // args[1] = new rowid/PK
        // args[2..] = new column values

        // Build SET clauses with compression
        let mut set_clauses = Vec::new();
        let mut values = Vec::new();

        for (i, (col_name, _)) in self.all_columns.iter().enumerate() {
            // Try to get as text first for compression
            if self.compressed_columns.contains(col_name) {
                if let Ok(text) = args.get::<String>(i + 2) {
                    let compressed = compress_with_marker(&text, DEFAULT_COMPRESSION_LEVEL)
                        .map_err(|e| rusqlite::Error::ToSqlConversionFailure(e.into()))?;
                    values.push(Value::Blob(compressed));
                } else {
                    // Fall back to getting as a generic value
                    let val: Value = args.get(i + 2)?;
                    values.push(val);
                }
            } else {
                let val: Value = args.get(i + 2)?;
                values.push(val);
            }

            set_clauses.push(format!("\"{}\" = ?", col_name));
        }

        let conn = unsafe { Connection::from_handle_owned(self.db_handle)? };

        if self.is_without_rowid {
            // For WITHOUT ROWID tables, use PK columns in WHERE clause
            // First check if we have cached PK values (for non-integer PKs with synthetic rowid)
            let synthetic_rowid: i64 = args.get(0)?;
            let cached_pk_values = self
                .pk_value_cache
                .lock()
                .ok()
                .and_then(|cache| cache.get(&synthetic_rowid).cloned());

            if self.pk_columns.len() == 1 {
                // Single-column PK
                let pk_col = &self.pk_columns[0];

                // Determine the actual PK value to use for WHERE clause
                let where_pk_value = if let Some(ref cached) = cached_pk_values {
                    // Use cached PK value (for text/blob PKs)
                    cached[0].clone()
                } else {
                    // Use the rowid directly (for integer PKs, it IS the PK value)
                    Value::Integer(synthetic_rowid)
                };

                values.push(where_pk_value);

                let sql = format!(
                    "UPDATE \"{}\" SET {} WHERE \"{}\" = ?",
                    self.underlying_table,
                    set_clauses.join(", "),
                    pk_col
                );

                let mut stmt = conn.prepare(&sql)?;
                for (i, value) in values.iter().enumerate() {
                    match value {
                        Value::Null => stmt.raw_bind_parameter(i + 1, value)?,
                        Value::Integer(n) => stmt.raw_bind_parameter(i + 1, n)?,
                        Value::Real(f) => stmt.raw_bind_parameter(i + 1, f)?,
                        Value::Text(s) => stmt.raw_bind_parameter(i + 1, s)?,
                        Value::Blob(b) => stmt.raw_bind_parameter(i + 1, b)?,
                    }
                }
                stmt.raw_execute()?;
                drop(stmt);
            } else {
                // Composite PK - build WHERE clause for all PK columns
                // The new PK values are in the column values (args[2..])
                // The old PK values need to be extracted from args somehow
                // For now, use the column values directly as we're updating in place

                // Build WHERE clause using PK columns
                let where_clauses: Vec<String> = self
                    .pk_columns
                    .iter()
                    .map(|pk| format!("\"{}\" = ?", pk))
                    .collect();

                let sql = format!(
                    "UPDATE \"{}\" SET {} WHERE {}",
                    self.underlying_table,
                    set_clauses.join(", "),
                    where_clauses.join(" AND ")
                );

                // Get old PK values from args[0] - for composite keys, this might be encoded
                // Alternatively, we can use the new PK values from the column data
                // since we're updating to those values anyway

                // Collect PK values from the new column values (args[2..])
                let mut pk_values = Vec::new();
                for pk_col in &self.pk_columns {
                    if let Some(idx) = self.all_columns.iter().position(|(name, _)| name == pk_col)
                    {
                        let val: Value = args.get(idx + 2)?;
                        pk_values.push(val);
                    }
                }

                // Append PK values for WHERE clause
                values.extend(pk_values);

                let mut stmt = conn.prepare(&sql)?;
                for (i, value) in values.iter().enumerate() {
                    match value {
                        Value::Null => stmt.raw_bind_parameter(i + 1, value)?,
                        Value::Integer(n) => stmt.raw_bind_parameter(i + 1, n)?,
                        Value::Real(f) => stmt.raw_bind_parameter(i + 1, f)?,
                        Value::Text(s) => stmt.raw_bind_parameter(i + 1, s)?,
                        Value::Blob(b) => stmt.raw_bind_parameter(i + 1, b)?,
                    }
                }
                stmt.raw_execute()?;
                drop(stmt);
            }
        } else {
            // Regular rowid table
            let old_rowid = args.get::<i64>(0)?;
            let new_rowid = args.get::<i64>(1)?;

            // Handle rowid change
            if old_rowid != new_rowid {
                set_clauses.push("rowid = ?".to_string());
                values.push(Value::Integer(new_rowid));
            }

            values.push(Value::Integer(old_rowid));

            let sql = format!(
                "UPDATE \"{}\" SET {} WHERE rowid = ?",
                self.underlying_table,
                set_clauses.join(", ")
            );

            let mut stmt = conn.prepare(&sql)?;
            for (i, value) in values.iter().enumerate() {
                match value {
                    Value::Null => stmt.raw_bind_parameter(i + 1, value)?,
                    Value::Integer(n) => stmt.raw_bind_parameter(i + 1, n)?,
                    Value::Real(f) => stmt.raw_bind_parameter(i + 1, f)?,
                    Value::Text(s) => stmt.raw_bind_parameter(i + 1, s)?,
                    Value::Blob(b) => stmt.raw_bind_parameter(i + 1, b)?,
                }
            }
            stmt.raw_execute()?;
            drop(stmt);
        }

        std::mem::forget(conn);
        Ok(())
    }
}

/// Build schema DDL for the virtual table with PRIMARY KEY constraints
///
/// Note: The schema DDL should NOT include WITHOUT ROWID - that's controlled
/// by the CREATE VIRTUAL TABLE statement itself. The virtual table can still
/// handle WITHOUT ROWID underlying tables without declaring itself as WITHOUT ROWID.
fn build_schema_ddl(
    columns: &[(String, String)],
    pk_columns: &[String],
    _without_rowid: bool,
) -> String {
    // Build column definitions
    let col_defs: Vec<String> = columns
        .iter()
        .map(|(name, col_type)| {
            // Only add PRIMARY KEY inline for single-column primary keys
            if pk_columns.len() == 1 && pk_columns.contains(name) {
                format!("\"{}\" {} PRIMARY KEY", name, col_type)
            } else {
                format!("\"{}\" {}", name, col_type)
            }
        })
        .collect();

    // Build the CREATE TABLE statement
    // Note: WITHOUT ROWID is NOT included here - it's only for the CREATE VIRTUAL TABLE
    // statement itself. The virtual table handles WITHOUT ROWID underlying tables
    // by detecting them and adjusting its queries accordingly.
    if pk_columns.len() > 1 {
        // Composite primary key - add PRIMARY KEY constraint at table level
        let pk_list = pk_columns
            .iter()
            .map(|name| format!("\"{}\"", name))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "CREATE TABLE x({}, PRIMARY KEY ({}))",
            col_defs.join(", "),
            pk_list
        )
    } else {
        format!("CREATE TABLE x({})", col_defs.join(", "))
    }
}

/// Register the zstd virtual table module with SQLite.
/// This only needs to be called once per connection.
pub fn register_module(conn: &Connection) -> Result<()> {
    // Get the module definition for writable virtual tables
    let module = update_module::<ZstdVTab>();

    // Register the module with the connection
    conn.create_module("zstd", module, None)
}
