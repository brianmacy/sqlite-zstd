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

### Query Optimization

The virtual table implementation automatically optimizes queries with WHERE clauses:

```sql
-- Optimized: pushes constraint to underlying table
SELECT * FROM documents WHERE id = 1;

-- Optimized: range constraints also pushed down
SELECT * FROM documents WHERE id > 100;
SELECT * FROM documents WHERE id >= 50 AND id < 100;

-- Full table scan (no constraints)
SELECT * FROM documents;
```

You can verify optimization with EXPLAIN QUERY PLAN:

```sql
EXPLAIN QUERY PLAN SELECT * FROM documents WHERE id = 1;
-- Shows: SCAN docs VIRTUAL TABLE INDEX 1:
```

### Efficient Joins on Compressed Columns

By default, joins on compressed columns decompress both sides for comparison. For equality joins, query the underlying `_zstd_<table>` tables directly to compare compressed BLOBs (zstd output is deterministic):

```sql
-- Inefficient: decompresses both sides
SELECT a.content FROM documents a
JOIN other_docs b ON a.content = b.content;

-- Efficient: compares compressed BLOBs directly using raw tables
SELECT zstd_decompress_marked(a.content) FROM _zstd_documents a
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

## Smart Compression

The extension uses a **marker byte protocol** for intelligent compression:

- **Small strings** (< 64 bytes): Stored uncompressed with `0x00` marker
  - Avoids compression overhead for short text
  - Examples: names, titles, short descriptions

- **Large strings** (≥ 64 bytes): Compressed with zstd, prefixed with `0x01` marker
  - Only compressed if it actually reduces size
  - Falls back to uncompressed if compression doesn't help

This approach:
- Optimizes storage automatically without configuration
- Ensures deterministic compression (same input = same output)
- Enables efficient equality joins on compressed columns

## ON CONFLICT Support

The virtual table implementation supports all SQLite ON CONFLICT clauses:

```sql
-- Replace existing row if there's a conflict
INSERT OR REPLACE INTO documents (id, title, content)
VALUES (1, 'Updated', 'New content');

-- Ignore the insert if there's a conflict
INSERT OR IGNORE INTO documents (id, title, content)
VALUES (1, 'Will be ignored', 'If id=1 exists');

-- Other conflict modes
INSERT OR ABORT ...   -- Abort the current statement (default)
INSERT OR FAIL ...    -- Continue after error
INSERT OR ROLLBACK ...  -- Rollback the entire transaction
```

This was impossible with the previous view+triggers architecture and is a major advantage of virtual tables.

## How It Works

When you call `zstd_enable()`, the extension:

1. Registers the `zstd` virtual table module with SQLite
2. Renames the original table (e.g., `documents` → `_zstd_documents`)
3. Creates a virtual table with the original table name
4. Stores configuration in `_zstd_config` table

The virtual table:
- Intercepts all INSERT/UPDATE operations to compress TEXT columns
- Intercepts all SELECT operations to decompress compressed columns
- Supports ON CONFLICT clauses (REPLACE, IGNORE, etc.)
- Provides direct control over read/write operations

This approach uses standard SQL types and works with all ORMs and database tools.

## License

Apache License 2.0
