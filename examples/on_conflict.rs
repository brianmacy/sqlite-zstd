//! ON CONFLICT example for sqlite-zstd extension.
//!
//! This example demonstrates:
//! - INSERT OR REPLACE functionality
//! - INSERT OR IGNORE functionality
//! - How virtual tables enable ON CONFLICT support

use rusqlite::{Connection, Result};

fn main() -> Result<()> {
    println!("=== SQLite-zstd ON CONFLICT Example ===\n");

    // Create in-memory database
    let conn = Connection::open_in_memory()?;

    // Register zstd functions
    sqlite_zstd::register_functions(&conn)?;

    // Create a cache table with unique constraint
    println!("1. Creating cache table with unique key...");
    conn.execute(
        "CREATE TABLE cache (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at INTEGER
        )",
        [],
    )?;
    println!("   ✓ Table created\n");

    // Enable compression on the value column
    println!("2. Enabling compression on 'value' column...");
    conn.query_row("SELECT zstd_enable('cache', 'value')", [], |_| Ok(()))?;
    println!("   ✓ Compression enabled\n");

    // INSERT OR REPLACE example
    println!("3. Testing INSERT OR REPLACE...");

    // First insert
    conn.execute(
        "INSERT INTO cache (key, value, updated_at) VALUES ('config', 'initial value', 1000)",
        [],
    )?;
    println!("   ✓ Initial insert: key='config', value='initial value'");

    // Replace with new value
    conn.execute(
        "INSERT OR REPLACE INTO cache (key, value, updated_at) VALUES ('config', 'updated value', 2000)",
        [],
    )?;
    println!("   ✓ Replaced: key='config', value='updated value'");

    let value: String = conn.query_row(
        "SELECT value FROM cache WHERE key = 'config'",
        [],
        |row| row.get(0),
    )?;
    println!("   ✓ Current value: '{}'\n", value);

    // INSERT OR IGNORE example
    println!("4. Testing INSERT OR IGNORE...");

    conn.execute(
        "INSERT INTO cache (key, value, updated_at) VALUES ('user1', 'Alice', 3000)",
        [],
    )?;
    println!("   ✓ Initial insert: key='user1', value='Alice'");

    // Try to insert duplicate - will be ignored
    conn.execute(
        "INSERT OR IGNORE INTO cache (key, value, updated_at) VALUES ('user1', 'Bob', 4000)",
        [],
    )?;
    println!("   ✓ Duplicate insert ignored (no error)");

    let user: String = conn.query_row(
        "SELECT value FROM cache WHERE key = 'user1'",
        [],
        |row| row.get(0),
    )?;
    println!("   ✓ Value unchanged: '{}' (not 'Bob')\n", user);

    // Count total rows
    let count: i32 = conn.query_row("SELECT COUNT(*) FROM cache", [], |row| row.get(0))?;
    println!("5. Total cache entries: {}\n", count);

    // Show all cache entries
    println!("6. All cache entries:");
    let mut stmt = conn.prepare("SELECT key, value, updated_at FROM cache ORDER BY key")?;
    let entries = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i32>(2)?,
        ))
    })?;

    for entry in entries {
        let (key, value, timestamp) = entry?;
        println!("   {} = '{}' (timestamp: {})", key, value, timestamp);
    }
    println!();

    // Demonstrate UPSERT pattern (INSERT OR REPLACE is perfect for caches)
    println!("7. UPSERT pattern (cache update)...");
    for (key, value, ts) in [
        ("config", "newest value", 5000),
        ("user2", "David", 5001),
        ("user1", "Alice Updated", 5002),
    ] {
        conn.execute(
            "INSERT OR REPLACE INTO cache (key, value, updated_at) VALUES (?, ?, ?)",
            rusqlite::params![key, value, ts],
        )?;
        println!("   ✓ Upserted: {} = '{}'", key, value);
    }
    println!();

    // Final count
    let final_count: i32 = conn.query_row("SELECT COUNT(*) FROM cache", [], |row| row.get(0))?;
    println!("8. Final cache size: {} entries\n", final_count);

    // Show compression stats
    let stats: String = conn.query_row("SELECT zstd_stats('cache')", [], |row| row.get(0))?;
    println!("9. Compression statistics:");
    println!("   {}\n", stats);

    println!("=== Example Complete ===");
    println!("ON CONFLICT clauses work seamlessly with compressed virtual tables!");

    Ok(())
}
