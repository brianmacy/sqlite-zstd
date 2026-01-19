//! Virtual table implementation for zstd compression.
//!
//! This module contains the virtual table implementation that replaces
//! the view+triggers architecture with full SQLite virtual table support.

pub mod conflict;
pub mod cursor;
pub mod zstd_vtab;

// Public API exports (used by lib.rs and potentially external code)
#[allow(unused_imports)]
pub use conflict::{ConflictMode, get_conflict_mode};
#[allow(unused_imports)]
pub use cursor::ZstdCursor;
#[allow(unused_imports)]
pub use zstd_vtab::{VTabConfig, ZstdVTab, register_module};
