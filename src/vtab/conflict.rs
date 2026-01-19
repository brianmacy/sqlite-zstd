//! ON CONFLICT mode detection for virtual table operations.
//!
//! This module provides safe wrappers around the sqlite3_vtab_on_conflict()
//! function to detect which conflict resolution mode is active during
//! INSERT or UPDATE operations.

use rusqlite::ffi;

/// ON CONFLICT resolution modes supported by SQLite
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictMode {
    /// ROLLBACK - abort the SQL statement with an error and roll back the transaction
    Rollback,
    /// ABORT - abort the current SQL statement with an error (default)
    Abort,
    /// FAIL - abort the current SQL statement with an error but don't roll back
    Fail,
    /// IGNORE - skip the row that caused the constraint violation
    Ignore,
    /// REPLACE - delete pre-existing rows that cause the constraint violation
    Replace,
}

impl ConflictMode {
    /// Convert to SQL clause text
    pub fn to_sql_clause(self) -> &'static str {
        match self {
            ConflictMode::Rollback => "OR ROLLBACK",
            ConflictMode::Abort => "",
            ConflictMode::Fail => "OR FAIL",
            ConflictMode::Ignore => "OR IGNORE",
            ConflictMode::Replace => "OR REPLACE",
        }
    }
}

/// Get the ON CONFLICT resolution mode for the current INSERT/UPDATE operation.
///
/// # Safety
/// Must be called from within UpdateVTab::insert() or UpdateVTab::update()
/// with a valid database handle.
///
/// # Arguments
/// * `db` - Valid SQLite database handle (from within a virtual table operation)
///
/// # Returns
/// The current conflict resolution mode, defaults to Abort if unknown
pub unsafe fn get_conflict_mode(db: *mut ffi::sqlite3) -> ConflictMode {
    let code = unsafe { ffi::sqlite3_vtab_on_conflict(db) };
    match code {
        ffi::SQLITE_ROLLBACK => ConflictMode::Rollback,
        ffi::SQLITE_ABORT => ConflictMode::Abort,
        ffi::SQLITE_FAIL => ConflictMode::Fail,
        ffi::SQLITE_IGNORE => ConflictMode::Ignore,
        ffi::SQLITE_REPLACE => ConflictMode::Replace,
        _ => ConflictMode::Abort, // Default to ABORT for unknown codes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_conflict_mode_to_sql() {
        assert_eq!(ConflictMode::Rollback.to_sql_clause(), "OR ROLLBACK");
        assert_eq!(ConflictMode::Abort.to_sql_clause(), "");
        assert_eq!(ConflictMode::Fail.to_sql_clause(), "OR FAIL");
        assert_eq!(ConflictMode::Ignore.to_sql_clause(), "OR IGNORE");
        assert_eq!(ConflictMode::Replace.to_sql_clause(), "OR REPLACE");
    }

    #[test]
    fn test_conflict_mode_equality() {
        assert_eq!(ConflictMode::Replace, ConflictMode::Replace);
        assert_ne!(ConflictMode::Replace, ConflictMode::Ignore);
    }
}
