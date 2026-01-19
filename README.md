# sqlite-zstd

SQLite extension for seamless TEXT field compression using Zstandard (zstd).

## Installation

Build the extension:

```bash
cargo build --release --features loadable_extension
```

### macOS

The system SQLite on macOS has extension loading disabled. Use Homebrew's SQLite:

```bash
# Install Homebrew SQLite
brew install sqlite

# Use Homebrew's sqlite3 (not /usr/bin/sqlite3)
/opt/homebrew/opt/sqlite/bin/sqlite3 mydb.db
```

Then load the extension:

```sql
.load ./target/release/libsqlite_zstd
```

### Linux

```sql
.load ./target/release/libsqlite_zstd
```

## Seamless Compression

Enable transparent compression on any table. Once enabled, INSERT/UPDATE automatically compress and SELECT automatically decompresses - no changes to your queries.

### Enable Compression

```sql
CREATE TABLE documents (
    id INTEGER PRIMARY KEY,
    title TEXT,
    author TEXT,
    content TEXT,
    metadata TEXT
);

-- Enable compression for ALL TEXT columns in the table
SELECT zstd_enable('documents');

-- Or enable for specific columns only
SELECT zstd_enable('documents', 'content', 'metadata');
```

### Use the Table Normally

```sql
-- These just work - compression/decompression is automatic
INSERT INTO documents (title, author, content, metadata)
VALUES ('My Doc', 'Alice', 'Very large content here...', '{"large": "json"}');

SELECT * FROM documents;  -- content and metadata are automatically decompressed

UPDATE documents SET content = 'Updated content' WHERE id = 1;
```

### Disable Compression

```sql
-- Disable compression for the entire table (decompresses all data)
SELECT zstd_disable('documents');

-- Disable for a specific column only
SELECT zstd_disable('documents', 'content');
```

### Efficient Joins on Compressed Columns

By default, joins on compressed columns decompress both sides for comparison. For equality joins, query the underlying `_zstd_<table>` tables directly to compare compressed BLOBs (zstd output is deterministic):

```sql
-- Inefficient: decompresses both sides
SELECT a.content FROM documents a
JOIN other_docs b ON a.content = b.content;

-- Efficient: compares compressed BLOBs directly using raw tables
SELECT zstd_decompress(a.content) FROM _zstd_documents a
JOIN _zstd_other_docs b ON a.content = b.content;
```

Note: This only works for equality comparisons. Ordering comparisons (`<`, `>`) require decompression.

### Introspection

```sql
-- List compressed columns in a table
SELECT zstd_columns('documents');
-- Returns: content, metadata

-- Get compression statistics
SELECT zstd_stats('documents');
-- Returns per-column stats: original size, compressed size, ratio
```

## Low-Level Functions

For manual control, these functions are also available:

| Function | Description |
|----------|-------------|
| `zstd_compress(text)` | Compress text, returns BLOB |
| `zstd_compress(text, level)` | Compress with level 1-22 (default: 3) |
| `zstd_decompress(blob)` | Decompress BLOB back to TEXT |

```sql
-- Manual compression
SELECT zstd_compress('Hello, World!');
SELECT zstd_compress('Hello, World!', 19);  -- Higher compression level
SELECT zstd_decompress(zstd_compress('Hello, World!'));
```

## Compression Levels

| Level | Speed | Compression |
|-------|-------|-------------|
| 1 | Fastest | Lowest |
| 3 | Default | Balanced |
| 19 | Slow | High |
| 22 | Slowest | Maximum |

For most TEXT data, the default level (3) provides a good balance. Use higher levels (19-22) for archival or when storage is critical.

## How It Works

When you call `zstd_enable()`, the extension:

1. Renames the original table (e.g., `documents` → `_zstd_documents`)
2. Creates a view with the original table name that auto-decompresses on SELECT
3. Creates INSTEAD OF triggers that auto-compress on INSERT/UPDATE
4. Stores configuration in `_zstd_config` table

This approach uses standard SQL types and works with all ORMs and database tools.

## License

Apache License 2.0
