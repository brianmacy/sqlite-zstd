# Known Issues and Limitations

## UPSERT Syntax Not Supported

### Issue

Modern UPSERT syntax (`INSERT ... ON CONFLICT DO NOTHING/UPDATE`) is **not supported** for virtual tables in SQLite. This is a **SQLite core limitation**, not a bug in this extension.

**Does NOT work:**
```sql
-- ❌ Modern UPSERT syntax - NOT SUPPORTED
INSERT INTO compressed_table (id, data)
VALUES (1, 'value')
ON CONFLICT DO NOTHING;

-- ❌ Also not supported
INSERT INTO compressed_table (id, data)
VALUES (1, 'value')
ON CONFLICT (id) DO UPDATE SET data = excluded.data;
```

**Error message:**
```
UPSERT not implemented for virtual table "compressed_table"
```

### Workaround

Use the older `INSERT OR IGNORE` and `INSERT OR REPLACE` syntax instead:

**✅ Supported equivalent syntax:**
```sql
-- ✅ Use INSERT OR IGNORE (same as ON CONFLICT DO NOTHING)
INSERT OR IGNORE INTO compressed_table (id, data)
VALUES (1, 'value');

-- ✅ Use INSERT OR REPLACE (same as ON CONFLICT REPLACE)
INSERT OR REPLACE INTO compressed_table (id, data)
VALUES (1, 'value');

-- ✅ Other conflict modes also supported
INSERT OR ABORT ...    -- Default behavior
INSERT OR FAIL ...     -- Continue after constraint violation
INSERT OR ROLLBACK ...  -- Rollback entire transaction
```

### Functional Equivalence

| Modern UPSERT | Legacy Syntax | Support |
|---------------|---------------|---------|
| `ON CONFLICT DO NOTHING` | `INSERT OR IGNORE` | ✅ Fully supported |
| `ON CONFLICT REPLACE` | `INSERT OR REPLACE` | ✅ Fully supported |
| `ON CONFLICT DO UPDATE` | See workaround below | ❌ Not available |
| `ON CONFLICT ... RETURNING` | Two statements | ❌ Not available |

**Note:** `INSERT OR IGNORE` and `ON CONFLICT DO NOTHING` are functionally identical - both silently skip the insert if a conflict occurs. The only difference is syntax.

### Workaround for ON CONFLICT DO UPDATE

`ON CONFLICT DO UPDATE` allows partial row updates (only changing specific columns). Since `INSERT OR REPLACE` replaces the entire row, partial updates require application logic:

**Pattern 1: Check-then-update (two statements)**
```sql
-- Try insert first, then update if it failed
INSERT OR IGNORE INTO table (id, data, counter) VALUES (?, ?, 1);
-- If insert was ignored (row exists), update specific columns
UPDATE table SET data = ?, counter = counter + 1 WHERE id = ? AND changes() = 0;
```

**Pattern 2: Conditional with existing values**
```sql
-- First, try to get existing row
SELECT counter FROM table WHERE id = ?;
-- Then INSERT OR REPLACE with computed values
INSERT OR REPLACE INTO table (id, data, counter) VALUES (?, ?, ?);
```

**Pattern 3: Use a transaction for atomicity**
```sql
BEGIN;
INSERT OR IGNORE INTO table (id, data, counter) VALUES (?, ?, 1);
UPDATE table SET counter = counter + 1 WHERE id = ? AND changes() = 0;
COMMIT;
```

### Workaround for RETURNING Clause

The `RETURNING` clause (SQLite 3.35.0+) cannot be combined with UPSERT on virtual tables. Use a separate SELECT:

```sql
-- Instead of: INSERT ... ON CONFLICT DO UPDATE ... RETURNING *
INSERT OR REPLACE INTO table (id, data) VALUES (?, ?);
SELECT * FROM table WHERE id = ?;
```

### Impact

**Applications using modern UPSERT syntax must be updated to use legacy conflict syntax.**

Example migration:
```sql
-- Before (doesn't work with virtual tables)
INSERT INTO table (id, data) VALUES (?, ?) ON CONFLICT DO NOTHING;

-- After (works with virtual tables)
INSERT OR IGNORE INTO table (id, data) VALUES (?, ?);
```

### Why This Limitation Exists

SQLite's virtual table API (`xUpdate` method) does not currently support the modern UPSERT syntax introduced in SQLite 3.24.0 (2018). The `sqlite3_vtab_on_conflict()` function only reports the legacy conflict modes (ROLLBACK, ABORT, FAIL, IGNORE, REPLACE), not the newer UPSERT modes.

This is a **SQLite architectural limitation** that affects all virtual table implementations, not just this extension. Other virtual table implementations (such as FTS5, sqlite-vec, etc.) have the same limitation.

### Future Support

SQLite may add UPSERT support for virtual tables in a future version. When that happens, this extension can be updated to support it. Track SQLite development:
- [SQLite Virtual Tables Documentation](https://www.sqlite.org/vtab.html)
- [SQLite Forum - UPSERT discussions](https://sqlite.org/forum/forum)

### Testing

The extension includes comprehensive tests for INSERT OR IGNORE with:
- Single column primary keys
- Composite primary keys
- All conflict modes (IGNORE, REPLACE, ABORT, FAIL, ROLLBACK)

Run tests:
```bash
cargo test test_insert_or_ignore
```

## Composite Primary Keys

### Support

Composite primary keys **ARE fully supported**. The virtual table correctly:
- ✅ Preserves PRIMARY KEY constraints from the original table
- ✅ Enforces constraints on INSERT/UPDATE
- ✅ Works with INSERT OR IGNORE for composite keys
- ✅ Works with INSERT OR REPLACE for composite keys

Example:
```sql
CREATE TABLE records (
    source_id INTEGER,
    record_key TEXT,
    data TEXT,
    PRIMARY KEY (source_id, record_key)
);

SELECT zstd_enable('records', 'data');

-- Both columns are part of the primary key - works correctly
INSERT OR IGNORE INTO records VALUES (1, 'KEY1', 'value');
INSERT OR IGNORE INTO records VALUES (1, 'KEY1', 'duplicate'); -- Silently ignored
```

## Summary

- ✅ **All legacy ON CONFLICT modes supported** (INSERT OR IGNORE, INSERT OR REPLACE, etc.)
- ✅ **Composite primary keys supported**
- ❌ **Modern UPSERT syntax not supported** (SQLite limitation, not extension bug)
- ✅ **Simple workaround available** (use INSERT OR IGNORE instead)
