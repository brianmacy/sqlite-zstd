//! Main virtual table implementation for zstd compression.

use rusqlite::vtab::{IndexInfo, VTab, VTabConnection, sqlite3_vtab};
use rusqlite::{Connection, Result};

/// Configuration for virtual table creation
#[derive(Debug)]
pub struct VTabConfig {
    pub underlying_table: String,
    pub compressed_columns: Vec<String>,
    pub all_columns: Vec<(String, String)>, // (name, type)
}

/// Virtual table structure for zstd compression
#[repr(C)]
pub struct ZstdVTab {
    base: sqlite3_vtab,
    pub underlying_table: String,
    pub compressed_columns: Vec<String>,
    pub all_columns: Vec<(String, String)>, // (name, type)
}

unsafe impl<'vtab> VTab<'vtab> for ZstdVTab {
    type Aux = VTabConfig;
    type Cursor = super::cursor::ZstdCursor<'vtab>;

    fn connect(
        _db: &mut VTabConnection,
        aux: Option<&Self::Aux>,
        _args: &[&[u8]],
    ) -> Result<(String, Self)> {
        // Get configuration from aux
        let config = aux
            .ok_or_else(|| rusqlite::Error::ModuleError("No configuration provided".to_string()))?;

        // Get column information from config
        let all_columns = config.all_columns.clone();

        // Build schema DDL
        let schema = build_schema_ddl(&all_columns);

        let vtab = ZstdVTab {
            base: sqlite3_vtab::default(),
            underlying_table: config.underlying_table.clone(),
            compressed_columns: config.compressed_columns.clone(),
            all_columns,
        };

        Ok((schema, vtab))
    }

    fn best_index(&self, _info: &mut IndexInfo) -> Result<()> {
        // Basic implementation - no optimization yet (Phase 4)
        Ok(())
    }

    fn open(&'vtab mut self) -> Result<Self::Cursor> {
        super::cursor::ZstdCursor::new(self)
    }
}

/// Build schema DDL for the virtual table
fn build_schema_ddl(columns: &[(String, String)]) -> String {
    let col_defs: Vec<String> = columns
        .iter()
        .map(|(name, col_type)| format!("\"{}\" {}", name, col_type))
        .collect();

    format!("CREATE TABLE x({})", col_defs.join(", "))
}

/// Register the zstd virtual table module with SQLite
pub fn register_module(_conn: &Connection) -> Result<()> {
    // TODO: Implement module registration
    // This will be called from zstd_enable_impl
    Ok(())
}
