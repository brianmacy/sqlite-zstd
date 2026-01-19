//! Main virtual table implementation for zstd compression.

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

        // Build schema DDL with PRIMARY KEY constraints
        let schema = build_schema_ddl(&all_columns, &pk_columns);

        // Get database handle for later use in insert/update/delete operations
        let db_handle = unsafe { db.handle() };

        let vtab = ZstdVTab {
            base: sqlite3_vtab::default(),
            db_handle,
            underlying_table,
            compressed_columns,
            all_columns,
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
                conn.last_insert_rowid()
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
        let rowid = arg.as_i64()?;
        let sql = format!("DELETE FROM \"{}\" WHERE rowid = ?", self.underlying_table);

        let conn = unsafe { Connection::from_handle_owned(self.db_handle)? };
        conn.execute(&sql, [rowid])?;
        std::mem::forget(conn);

        Ok(())
    }

    fn update(&mut self, args: &Values<'_>) -> Result<()> {
        // args[0] = old rowid (NOT NULL)
        // args[1] = new rowid
        // args[2..] = new column values

        let old_rowid = args.get::<i64>(0)?;
        let new_rowid = args.get::<i64>(1)?;

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

        // Execute UPDATE
        let conn = unsafe { Connection::from_handle_owned(self.db_handle)? };
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
        drop(stmt); // Drop statement before forgetting connection
        std::mem::forget(conn);

        Ok(())
    }
}

/// Build schema DDL for the virtual table with PRIMARY KEY constraints
fn build_schema_ddl(columns: &[(String, String)], pk_columns: &[String]) -> String {
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

    // If composite primary key, add PRIMARY KEY constraint at table level
    if pk_columns.len() > 1 {
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
