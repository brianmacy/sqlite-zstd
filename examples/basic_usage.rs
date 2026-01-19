//! Basic usage example for sqlite-zstd extension.
//!
//! This example demonstrates:
//! - Enabling compression on a table
//! - Basic INSERT/SELECT operations
//! - Automatic compression/decompression
//! - Checking compression statistics

use rusqlite::{Connection, Result};

fn main() -> Result<()> {
    println!("=== SQLite-zstd Basic Usage Example ===\n");

    // Create in-memory database
    let conn = Connection::open_in_memory()?;

    // Register zstd functions
    sqlite_zstd::register_functions(&conn)?;

    // Create a sample table
    println!("1. Creating table 'articles'...");
    conn.execute(
        "CREATE TABLE articles (
            id INTEGER PRIMARY KEY,
            title TEXT,
            author TEXT,
            content TEXT,
            published_date TEXT
        )",
        [],
    )?;
    println!("   ✓ Table created\n");

    // Enable compression on content column only
    println!("2. Enabling zstd compression on 'content' column...");
    let result: String = conn.query_row(
        "SELECT zstd_enable('articles', 'content')",
        [],
        |row| row.get(0),
    )?;
    println!("   ✓ {}\n", result);

    // Insert some sample data
    println!("3. Inserting sample articles...");
    let large_content = "Lorem ipsum dolor sit amet. ".repeat(100); // ~2.8KB

    conn.execute(
        "INSERT INTO articles (title, author, content, published_date) VALUES (?, ?, ?, ?)",
        rusqlite::params![
            "Introduction to Rust",
            "Alice",
            &large_content,
            "2024-01-15"
        ],
    )?;

    conn.execute(
        "INSERT INTO articles (title, author, content, published_date) VALUES (?, ?, ?, ?)",
        rusqlite::params![
            "Advanced SQLite",
            "Bob",
            &large_content,
            "2024-02-20"
        ],
    )?;

    conn.execute(
        "INSERT INTO articles (title, author, content, published_date) VALUES (?, ?, ?, ?)",
        rusqlite::params![
            "Data Compression",
            "Charlie",
            &large_content,
            "2024-03-10"
        ],
    )?;

    println!("   ✓ Inserted 3 articles\n");

    // Query data (automatic decompression)
    println!("4. Querying articles (automatic decompression)...");
    let mut stmt = conn.prepare("SELECT id, title, author FROM articles")?;
    let articles = stmt.query_map([], |row| {
        Ok((
            row.get::<_, i32>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
        ))
    })?;

    for article in articles {
        let (id, title, author) = article?;
        println!("   - Article {}: '{}' by {}", id, title, author);
    }
    println!();

    // Check compression statistics
    println!("5. Compression statistics:");
    let stats: String = conn.query_row("SELECT zstd_stats('articles')", [], |row| row.get(0))?;
    println!("   {}\n", stats);

    // Verify content can be read
    println!("6. Reading compressed content...");
    let content: String = conn.query_row(
        "SELECT content FROM articles WHERE title = 'Introduction to Rust'",
        [],
        |row| row.get(0),
    )?;
    println!("   ✓ Content length: {} bytes", content.len());
    println!("   ✓ First 50 chars: {}...\n", &content[..50]);

    // List compressed columns
    println!("7. Introspection:");
    let columns: String = conn.query_row("SELECT zstd_columns('articles')", [], |row| row.get(0))?;
    println!("   Compressed columns: {}\n", columns);

    // Update a row
    println!("8. Updating article (automatic re-compression)...");
    conn.execute(
        "UPDATE articles SET content = ? WHERE id = 1",
        ["Updated content!"],
    )?;
    println!("   ✓ Article 1 updated\n");

    // Verify update worked
    let updated: String = conn.query_row(
        "SELECT content FROM articles WHERE id = 1",
        [],
        |row| row.get(0),
    )?;
    println!("   ✓ New content: {}\n", updated);

    println!("=== Example Complete ===");
    println!("All operations succeeded with transparent compression/decompression!");

    Ok(())
}
