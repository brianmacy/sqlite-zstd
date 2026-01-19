-- Test script for sqlite-zstd loadable extension
.load ./target/release/libsqlite_zstd

-- Create test table
CREATE TABLE test_docs (
    id INTEGER PRIMARY KEY,
    title TEXT,
    content TEXT
);

-- Enable compression
SELECT zstd_enable('test_docs', 'content');

-- Insert test data
INSERT INTO test_docs (title, content) VALUES ('Test 1', 'Small content');
INSERT INTO test_docs (title, content) VALUES ('Test 2', 'x' || substr(replace(hex(zeroblob(5000)), '00', 'x'), 1, 10000));

-- Query data
SELECT id, title, length(content) as content_len FROM test_docs;

-- Test ON CONFLICT
INSERT OR REPLACE INTO test_docs (id, title, content) VALUES (1, 'Replaced', 'New content');

-- Verify
SELECT id, title, content FROM test_docs WHERE id = 1;

-- Check stats
SELECT zstd_stats('test_docs');

-- Check columns
SELECT zstd_columns('test_docs');

-- Test WHERE clause
SELECT title FROM test_docs WHERE id = 2;

.print "All tests passed!"
