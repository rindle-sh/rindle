//! The headline SQLite claim turned into an assertion: **zero heap allocations
//! per row on the transient path, even for strings.** A counting global
//! allocator measures Rust-side heap traffic across two scans of the same 5000-
//! row table:
//!
//!   1. a **transient filtered scan** — read both columns (an int and a string)
//!      zero-copy, compare the string bytewise (`BINARY`), drop the row — must
//!      vend with **ZERO** allocations per row, and
//!   2. an **owning scan** — call `to_owned_row()` each row (the forced copy at
//!      the Node boundary) — which DOES allocate (≥ one per row).
//!
//! SQLite's own C allocations go through `sqlite3_malloc` → system `malloc`, NOT
//! Rust's `#[global_allocator]`, so the counter sees only the engine (Rust) side
//! — exactly what "the engine vends zero-copy" means. Both scans live in ONE
//! `#[test]` (the `ALLOCS` counter is process-global and cargo runs `#[test]`s
//! concurrently; a single test = a single measuring thread = a clean counter).

use std::alloc::{GlobalAlloc, Layout, System};
use std::cmp::Ordering;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

use rusqlite::Connection;

use rindle::value::{compare_values, RowRef, RowStream, Value};
use rindle_sqlite::sqlite::{select_sql, ColType, SqliteRowStream};

static ALLOCS: AtomicUsize = AtomicUsize::new(0);

struct Counting;

// SAFETY: a thin pass-through to the system allocator that only bumps a counter.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, AtomicOrdering::Relaxed);
        System.alloc(l)
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        System.dealloc(p, l)
    }
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, AtomicOrdering::Relaxed);
        System.alloc_zeroed(l)
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, AtomicOrdering::Relaxed);
        System.realloc(p, l, n)
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

fn allocs() -> usize {
    ALLOCS.load(AtomicOrdering::Relaxed)
}

const N: i64 = 5000;

fn seed(conn: &Connection) {
    conn.execute_batch("CREATE TABLE t(id INTEGER, name TEXT);")
        .unwrap();
    conn.execute_batch("BEGIN").unwrap();
    {
        let mut ins = conn
            .prepare("INSERT INTO t(id, name) VALUES (?1, ?2)")
            .unwrap();
        for k in 0..N {
            // non-inline strings (> 15 bytes) so a copy would be a real heap alloc.
            ins.execute(rusqlite::params![k, format!("the-name-of-row-{k:08}")])
                .unwrap();
        }
    }
    conn.execute_batch("COMMIT").unwrap();
}

#[test]
fn transient_scan_is_zero_alloc_owning_is_not() {
    let conn = Connection::open_in_memory().unwrap();
    seed(&conn);
    let types = [ColType::Int, ColType::Text];
    let sql = select_sql("t", &["id", "name"]);

    // (1) Transient filtered scan: read id + name zero-copy, compare the string
    //     bytewise, drop the row. ZERO heap allocations per row.
    {
        let mut stmt = conn.prepare(&sql).unwrap();
        let mut sq = SqliteRowStream::new(stmt.query([]).unwrap(), &types);
        // Warm up one row so query/first-step one-time setup is OUTSIDE the
        // measured region; the claim is about the per-row vend cost.
        assert!(sq.next_row().is_some());

        let needle: &[u8] = b"the-name-of-row-00004999";
        let before = allocs();
        let mut rows = 0usize;
        let mut hits = 0usize;
        while let Some(r) = sq.next_row() {
            // read both columns (zero-copy) ...
            let _id = r.col(0);
            // ... and compare the string bytewise against a fixed needle.
            if compare_values(r.col(1), Value::Str(needle)) == Ordering::Equal {
                hits += 1;
            }
            rows += 1;
        }
        let after = allocs();
        std::hint::black_box(hits);

        assert_eq!(rows, (N - 1) as usize, "scanned the rest of the table");
        assert_eq!(hits, 1, "found the one matching row, bytewise");
        assert_eq!(
            after - before,
            0,
            "transient SQLite scan (read + bytewise compare, no owning) must \
             allocate ZERO per row, including for strings (got {})",
            after - before
        );
    }

    // (2) Owning each row via to_owned_row(): the forced Node-boundary copy —
    //     this DOES allocate (an Arc spine + a string per row).
    {
        let mut stmt = conn.prepare(&sql).unwrap();
        let mut sq = SqliteRowStream::new(stmt.query([]).unwrap(), &types);
        assert!(sq.next_row().is_some()); // same warmup

        let before = allocs();
        let mut last = None;
        let mut rows = 0usize;
        while let Some(r) = sq.next_row() {
            last = Some(r.to_owned_row());
            rows += 1;
        }
        let after = allocs();
        std::hint::black_box(&last);

        assert_eq!(rows, (N - 1) as usize);
        assert!(
            after - before >= rows,
            "owning each row must allocate (the forced copy at the Node boundary): \
             expected ≥ {rows} allocations, got {}",
            after - before
        );
        eprintln!(
            "transient path: 0 allocs/row · owning path: {} allocs for {rows} rows",
            after - before
        );
    }
}
