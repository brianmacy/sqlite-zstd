use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId, Throughput};
use rusqlite::Connection;

fn setup_db() -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    sqlite_zstd::register_functions(&conn).unwrap();
    conn
}

fn benchmark_compression(c: &mut Criterion) {
    let mut group = c.benchmark_group("compression");

    // Test different text sizes
    let sizes = vec![
        ("32B", 32),
        ("64B", 64),
        ("128B", 128),
        ("1KB", 1024),
        ("10KB", 10_240),
        ("100KB", 102_400),
        ("1MB", 1_048_576),
    ];

    for (name, size) in sizes {
        let text = "x".repeat(size);
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_with_input(BenchmarkId::new("compress", name), &text, |b, text| {
            let conn = setup_db();
            b.iter(|| {
                let _: Vec<u8> = conn
                    .query_row("SELECT zstd_compress(?)", [text], |row| row.get(0))
                    .unwrap();
            });
        });

        let conn = setup_db();
        let compressed: Vec<u8> = conn
            .query_row("SELECT zstd_compress(?)", [&text], |row| row.get(0))
            .unwrap();

        group.bench_with_input(BenchmarkId::new("decompress", name), &compressed, |b, data| {
            let conn = setup_db();
            b.iter(|| {
                let _: String = conn
                    .query_row("SELECT zstd_decompress(?)", [data], |row| row.get(0))
                    .unwrap();
            });
        });
    }

    group.finish();
}

fn benchmark_vtable_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("vtable_operations");

    // INSERT benchmark
    group.bench_function("insert_1000_rows", |b| {
        b.iter(|| {
            let conn = setup_db();
            conn.execute(
                "CREATE TABLE docs (id INTEGER PRIMARY KEY, content TEXT)",
                [],
            )
            .unwrap();
            conn.query_row("SELECT zstd_enable('docs', 'content')", [], |_| Ok(()))
                .unwrap();

            for i in 1..=1000 {
                conn.execute(
                    "INSERT INTO docs (id, content) VALUES (?, ?)",
                    rusqlite::params![i, format!("Content for document {}", i)],
                )
                .unwrap();
            }
        });
    });

    // SELECT benchmark
    group.bench_function("select_full_scan", |b| {
        let conn = setup_db();
        conn.execute(
            "CREATE TABLE docs (id INTEGER PRIMARY KEY, content TEXT)",
            [],
        )
        .unwrap();
        conn.query_row("SELECT zstd_enable('docs', 'content')", [], |_| Ok(()))
            .unwrap();

        for i in 1..=1000 {
            conn.execute(
                "INSERT INTO docs (id, content) VALUES (?, ?)",
                rusqlite::params![i, format!("Content for document {}", i)],
            )
            .unwrap();
        }

        b.iter(|| {
            let count: i32 = conn
                .query_row("SELECT COUNT(*) FROM docs", [], |row| row.get(0))
                .unwrap();
            black_box(count);
        });
    });

    // SELECT with WHERE clause benchmark
    group.bench_function("select_with_where", |b| {
        let conn = setup_db();
        conn.execute(
            "CREATE TABLE docs (id INTEGER PRIMARY KEY, content TEXT)",
            [],
        )
        .unwrap();
        conn.query_row("SELECT zstd_enable('docs', 'content')", [], |_| Ok(()))
            .unwrap();

        for i in 1..=1000 {
            conn.execute(
                "INSERT INTO docs (id, content) VALUES (?, ?)",
                rusqlite::params![i, format!("Content for document {}", i)],
            )
            .unwrap();
        }

        b.iter(|| {
            let content: String = conn
                .query_row("SELECT content FROM docs WHERE id = 500", [], |row| {
                    row.get(0)
                })
                .unwrap();
            black_box(content);
        });
    });

    // UPDATE benchmark
    group.bench_function("update_100_rows", |b| {
        let conn = setup_db();
        conn.execute(
            "CREATE TABLE docs (id INTEGER PRIMARY KEY, content TEXT)",
            [],
        )
        .unwrap();
        conn.query_row("SELECT zstd_enable('docs', 'content')", [], |_| Ok(()))
            .unwrap();

        for i in 1..=1000 {
            conn.execute(
                "INSERT INTO docs (id, content) VALUES (?, ?)",
                rusqlite::params![i, format!("Content {}", i)],
            )
            .unwrap();
        }

        b.iter(|| {
            for i in 1..=100 {
                conn.execute(
                    "UPDATE docs SET content = ? WHERE id = ?",
                    rusqlite::params![format!("Updated content {}", i), i],
                )
                .unwrap();
            }
        });
    });

    group.finish();
}

criterion_group!(benches, benchmark_compression, benchmark_vtable_operations);
criterion_main!(benches);
