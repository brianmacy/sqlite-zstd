//! Cursor implementation for zstd virtual table.

use rusqlite::Result;
use rusqlite::ffi;
use rusqlite::vtab::{Context, VTabCursor, sqlite3_vtab_cursor};
use std::marker::PhantomData;
use std::os::raw::c_int;

use super::zstd_vtab::ZstdVTab;
use crate::compression::decompress_with_marker;

/// Cursor for iterating through zstd virtual table rows
#[repr(C)]
pub struct ZstdCursor<'vtab> {
    base: sqlite3_vtab_cursor,
    vtab: &'vtab ZstdVTab,
    stmt: Option<*mut ffi::sqlite3_stmt>,
    current_rowid: i64,
    eof: bool,
    _phantom: PhantomData<&'vtab ZstdVTab>,
}

impl<'vtab> ZstdCursor<'vtab> {
    pub fn new(vtab: &'vtab ZstdVTab) -> Result<Self> {
        Ok(ZstdCursor {
            base: sqlite3_vtab_cursor::default(),
            vtab,
            stmt: None,
            current_rowid: 0,
            eof: true,
            _phantom: PhantomData,
        })
    }
}

impl Drop for ZstdCursor<'_> {
    fn drop(&mut self) {
        // Clean up statement if it exists
        if let Some(stmt) = self.stmt {
            unsafe {
                ffi::sqlite3_finalize(stmt);
            }
        }
    }
}

unsafe impl VTabCursor for ZstdCursor<'_> {
    fn filter(
        &mut self,
        idx_num: c_int,
        _idx_str: Option<&str>,
        args: &rusqlite::vtab::Values<'_>,
    ) -> Result<()> {
        // Clean up any existing statement
        if let Some(stmt) = self.stmt.take() {
            unsafe {
                ffi::sqlite3_finalize(stmt);
            }
        }

        // Build SELECT query with optional WHERE clause
        let col_list = std::iter::once("rowid".to_string())
            .chain(
                self.vtab
                    .all_columns
                    .iter()
                    .map(|(name, _)| format!("\"{}\"", name)),
            )
            .collect::<Vec<_>>()
            .join(", ");

        // Build WHERE clause based on idx_num bitmask
        let mut where_clauses = Vec::new();
        let mut bind_values = Vec::new();

        if idx_num > 0 {
            let mut arg_idx = 0;

            // Check for equality constraints (lower 16 bits)
            for (col_idx, (col_name, _)) in self.vtab.all_columns.iter().enumerate() {
                if (idx_num & (1 << col_idx)) != 0 {
                    // This column has an equality constraint
                    where_clauses.push(format!("\"{}\" = ?", col_name));
                    if let Ok(val) = args.get::<rusqlite::types::Value>(arg_idx) {
                        bind_values.push(val);
                    }
                    arg_idx += 1;
                }
            }

            // Range constraints would be in upper 16 bits (future enhancement)
        }

        let sql = if where_clauses.is_empty() {
            format!(
                "SELECT {} FROM \"{}\"",
                col_list, self.vtab.underlying_table
            )
        } else {
            format!(
                "SELECT {} FROM \"{}\" WHERE {}",
                col_list,
                self.vtab.underlying_table,
                where_clauses.join(" AND ")
            )
        };

        // Prepare statement using raw SQLite API
        let mut stmt_ptr: *mut ffi::sqlite3_stmt = std::ptr::null_mut();
        let sql_cstr = std::ffi::CString::new(sql).map_err(|_| {
            rusqlite::Error::ModuleError("Failed to convert SQL to CString".to_string())
        })?;

        let rc = unsafe {
            ffi::sqlite3_prepare_v2(
                self.vtab.db_handle,
                sql_cstr.as_ptr(),
                -1,
                &mut stmt_ptr,
                std::ptr::null_mut(),
            )
        };

        if rc != ffi::SQLITE_OK {
            return Err(rusqlite::Error::SqliteFailure(
                ffi::Error::new(rc),
                Some("Failed to prepare SELECT statement".to_string()),
            ));
        }

        // Bind constraint values
        for (i, value) in bind_values.iter().enumerate() {
            let rc = match value {
                rusqlite::types::Value::Null => unsafe {
                    ffi::sqlite3_bind_null(stmt_ptr, (i + 1) as c_int)
                },
                rusqlite::types::Value::Integer(n) => unsafe {
                    ffi::sqlite3_bind_int64(stmt_ptr, (i + 1) as c_int, *n)
                },
                rusqlite::types::Value::Real(f) => unsafe {
                    ffi::sqlite3_bind_double(stmt_ptr, (i + 1) as c_int, *f)
                },
                rusqlite::types::Value::Text(s) => {
                    let c_str = std::ffi::CString::new(s.as_str()).map_err(|_| {
                        rusqlite::Error::ModuleError("Invalid string for binding".to_string())
                    })?;
                    unsafe {
                        ffi::sqlite3_bind_text(
                            stmt_ptr,
                            (i + 1) as c_int,
                            c_str.as_ptr(),
                            -1,
                            ffi::SQLITE_TRANSIENT(),
                        )
                    }
                }
                rusqlite::types::Value::Blob(b) => unsafe {
                    ffi::sqlite3_bind_blob(
                        stmt_ptr,
                        (i + 1) as c_int,
                        b.as_ptr() as *const _,
                        b.len() as c_int,
                        ffi::SQLITE_TRANSIENT(),
                    )
                },
            };

            if rc != ffi::SQLITE_OK {
                return Err(rusqlite::Error::SqliteFailure(
                    ffi::Error::new(rc),
                    Some(format!("Failed to bind parameter {}", i + 1)),
                ));
            }
        }

        self.stmt = Some(stmt_ptr);

        // Fetch first row
        self.next()
    }

    fn next(&mut self) -> Result<()> {
        if let Some(stmt) = self.stmt {
            let rc = unsafe { ffi::sqlite3_step(stmt) };

            match rc {
                ffi::SQLITE_ROW => {
                    // Read rowid (first column)
                    self.current_rowid = unsafe { ffi::sqlite3_column_int64(stmt, 0) };
                    self.eof = false;
                }
                ffi::SQLITE_DONE => {
                    self.eof = true;
                }
                _ => {
                    return Err(rusqlite::Error::SqliteFailure(
                        ffi::Error::new(rc),
                        Some("Failed to step statement".to_string()),
                    ));
                }
            }
        } else {
            self.eof = true;
        }

        Ok(())
    }

    fn eof(&self) -> bool {
        self.eof
    }

    fn column(&self, ctx: &mut Context, col: c_int) -> Result<()> {
        let stmt = self
            .stmt
            .ok_or_else(|| rusqlite::Error::ModuleError("No statement available".to_string()))?;

        // col is 0-indexed in the virtual table
        // but in our SELECT query, column 0 is rowid, so actual columns start at 1
        let stmt_col = col + 1;

        // Get column name to check if it needs decompression
        let (col_name, _) = &self.vtab.all_columns[col as usize];
        let needs_decompression = self.vtab.compressed_columns.contains(col_name);

        unsafe {
            let col_type = ffi::sqlite3_column_type(stmt, stmt_col);

            match col_type {
                ffi::SQLITE_NULL => {
                    ctx.set_result(&rusqlite::types::Null)?;
                }
                ffi::SQLITE_INTEGER => {
                    let val = ffi::sqlite3_column_int64(stmt, stmt_col);
                    ctx.set_result(&val)?;
                }
                ffi::SQLITE_FLOAT => {
                    let val = ffi::sqlite3_column_double(stmt, stmt_col);
                    ctx.set_result(&val)?;
                }
                ffi::SQLITE_TEXT => {
                    let text_ptr = ffi::sqlite3_column_text(stmt, stmt_col);
                    let text_len = ffi::sqlite3_column_bytes(stmt, stmt_col);
                    if !text_ptr.is_null() && text_len > 0 {
                        let text_slice = std::slice::from_raw_parts(text_ptr, text_len as usize);
                        let text_str = std::str::from_utf8(text_slice).map_err(|_| {
                            rusqlite::Error::ModuleError("Invalid UTF-8".to_string())
                        })?;
                        ctx.set_result(&text_str)?;
                    } else {
                        ctx.set_result(&"")?;
                    }
                }
                ffi::SQLITE_BLOB => {
                    let blob_ptr = ffi::sqlite3_column_blob(stmt, stmt_col);
                    let blob_len = ffi::sqlite3_column_bytes(stmt, stmt_col);

                    if !blob_ptr.is_null() && blob_len > 0 {
                        let blob_slice =
                            std::slice::from_raw_parts(blob_ptr as *const u8, blob_len as usize);

                        // If this column needs decompression, decompress it
                        if needs_decompression {
                            match decompress_with_marker(blob_slice) {
                                Ok(decompressed) => {
                                    ctx.set_result(&decompressed)?;
                                }
                                Err(_) => {
                                    // If decompression fails, it might be raw text
                                    // (for legacy data or data that wasn't compressed)
                                    // Try to interpret as UTF-8
                                    match std::str::from_utf8(blob_slice) {
                                        Ok(text) => ctx.set_result(&text)?,
                                        Err(_) => {
                                            // If all else fails, return as blob
                                            ctx.set_result(&blob_slice)?;
                                        }
                                    }
                                }
                            }
                        } else {
                            // Not a compressed column, return as blob
                            ctx.set_result(&blob_slice)?;
                        }
                    } else {
                        ctx.set_result(&Vec::<u8>::new())?;
                    }
                }
                _ => {
                    return Err(rusqlite::Error::ModuleError(format!(
                        "Unknown column type: {}",
                        col_type
                    )));
                }
            }
        }

        Ok(())
    }

    fn rowid(&self) -> Result<i64> {
        Ok(self.current_rowid)
    }
}
