//! Experiment: how much does the `prepare_cached` statement-cache lookup cost on the
//! operator-storage hot path, vs holding one prepared statement and only binding?
//!
//! Three strategies for the same point `get`/`set` against a storage-shaped table:
//!   - `fresh`  — `conn.prepare(SQL)` every call (recompiles; the pre-optimization path).
//!   - `cached` — `conn.prepare_cached(SQL)` every call (what `OpStorage` does now).
//!   - `held`   — prepare once, then bind + step each call (the "prepare ahead, just
//!     bind" path we'd need raw-FFI/self-referential code to reach inside `OpStorage`).
//!
//!   cargo run -p rindle-sqlite --release --example bench_stmt_strategy_rs --features fast-alloc

use std::hint::black_box;
use std::time::Instant;

use rusqlite::{params, Connection, OptionalExtension};

#[cfg(feature = "fast-alloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const KEYS: usize = 200_000;

const GET_SQL: &str = r#"SELECT "val" FROM storage WHERE "op" = ?1 AND "key" = ?2"#;
const SET_SQL: &str = r#"
    INSERT INTO storage ("op", "key", "val") VALUES (?1, ?2, ?3)
    ON CONFLICT("op", "key") DO UPDATE SET "val" = excluded."val""#;

fn bench(name: &str, mut body: impl FnMut()) {
    let warm = Instant::now();
    while warm.elapsed().as_nanos() < 200_000_000 {
        body();
    }
    let mut best = f64::INFINITY;
    for _ in 0..6 {
        let mut iters: u64 = 1;
        loop {
            let start = Instant::now();
            for _ in 0..iters {
                body();
            }
            let dt = start.elapsed().as_nanos() as f64;
            if dt >= 100_000_000.0 {
                best = best.min(dt / iters as f64);
                break;
            }
            iters *= 2;
        }
    }
    println!("{name}|{best:.1}");
}

fn keys() -> Vec<String> {
    (0..KEYS).map(|i| format!("g:{i:08}")).collect()
}

fn main() {
    println!("# prepared-statement strategy — {KEYS} keys (val = 9-byte blob)");
    println!("# name|ns_per_op  (lower is better)");

    let conn = Connection::open("").expect("open temp db");
    conn.execute_batch(
        r#"
        PRAGMA journal_mode = OFF;
        PRAGMA synchronous = OFF;
        PRAGMA temp_store = MEMORY;
        PRAGMA locking_mode = EXCLUSIVE;
        CREATE TABLE storage (
            "op"  INTEGER NOT NULL,
            "key" TEXT NOT NULL,
            "val" BLOB NOT NULL,
            PRIMARY KEY("op", "key")
        ) WITHOUT ROWID;
        BEGIN;
        "#,
    )
    .expect("init");

    let ks = keys();
    let blob: Vec<u8> = vec![1u8; 9]; // stand-in for an encoded Reduce { count }
    {
        let mut stmt = conn.prepare(SET_SQL).unwrap();
        for (i, k) in ks.iter().enumerate() {
            stmt.execute(params![1i64, k, &blob]).unwrap();
            black_box(i);
        }
    }
    let n = ks.len();

    // --- get ---------------------------------------------------------------
    let mut i = 0usize;
    bench("get_fresh", || {
        i = if i + 1 == n { 0 } else { i + 1 };
        let v = conn
            .prepare(GET_SQL)
            .unwrap()
            .query_row(params![1i64, &ks[i]], |r| r.get::<_, Vec<u8>>(0))
            .optional()
            .unwrap();
        black_box(v);
    });

    let mut i = 0usize;
    bench("get_cached", || {
        i = if i + 1 == n { 0 } else { i + 1 };
        let v = conn
            .prepare_cached(GET_SQL)
            .unwrap()
            .query_row(params![1i64, &ks[i]], |r| r.get::<_, Vec<u8>>(0))
            .optional()
            .unwrap();
        black_box(v);
    });

    {
        let mut get = conn.prepare(GET_SQL).unwrap();
        let mut i = 0usize;
        bench("get_held", || {
            i = if i + 1 == n { 0 } else { i + 1 };
            let v = get
                .query_row(params![1i64, &ks[i]], |r| r.get::<_, Vec<u8>>(0))
                .optional()
                .unwrap();
            black_box(v);
        });
    }

    // --- set (overwrite) ---------------------------------------------------
    let mut i = 0usize;
    bench("set_fresh", || {
        i = if i + 1 == n { 0 } else { i + 1 };
        conn.prepare(SET_SQL)
            .unwrap()
            .execute(params![1i64, &ks[i], &blob])
            .unwrap();
    });

    let mut i = 0usize;
    bench("set_cached", || {
        i = if i + 1 == n { 0 } else { i + 1 };
        conn.prepare_cached(SET_SQL)
            .unwrap()
            .execute(params![1i64, &ks[i], &blob])
            .unwrap();
    });

    {
        let mut set = conn.prepare(SET_SQL).unwrap();
        let mut i = 0usize;
        bench("set_held", || {
            i = if i + 1 == n { 0 } else { i + 1 };
            set.execute(params![1i64, &ks[i], &blob]).unwrap();
        });
    }
}
