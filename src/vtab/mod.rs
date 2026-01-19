//! Virtual table implementation for zstd compression.
//!
//! This module contains the virtual table implementation that replaces
//! the view+triggers architecture.
//!
//! Note: This module is currently under development (Phase 2-5).
//! Dead code warnings are suppressed until implementation is complete.

#![allow(dead_code)]
#![allow(unused_imports)]

pub mod conflict;
pub mod cursor;
pub mod zstd_vtab;

pub use conflict::{get_conflict_mode, ConflictMode};
pub use cursor::ZstdCursor;
pub use zstd_vtab::{register_module, VTabConfig, ZstdVTab};
