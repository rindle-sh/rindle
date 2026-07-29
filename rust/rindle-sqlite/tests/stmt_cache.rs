//! Tests for the prepared-statement cache (`src/stmt_cache.rs`, spec `05` §4.3).
//!
//! The cache is a thin wrapper over rusqlite's per-connection `prepare_cached`, so
//! these tests do NOT re-prove rusqlite's own LRU/reuse machinery (tested upstream
//! in `rusqlite::cache`). They prove the properties the *port* depends on:
//!
//! 1. The "remove while in use" / non-reentrant discipline (`05` §3.10):
//!    concurrent checkouts — same SQL or different, nested arbitrarily deep — are
//!    INDEPENDENT statements, so the self-join / reentrant-fetch pattern is correct.
//! 2. RAII release (README Primitive #2): dropping a `PooledStmt` resets the cursor
//!    (so the connection is free for a write — no "database is busy", no `finally`)
//!    and the open-cursor count returns to zero on every path, including early
//!    `break`, `discard`, and a return while another cursor is still live.
//! 3. The `with` (port of JS `use`) helper, the error path, and the capacity bound.

use std::rc::Rc;

use rusqlite::Connection;

use rindle_sqlite::stmt_cache::{StatementCache, DEFAULT_CAPACITY};

/// `t(id)` seeded with `1..=5`, wrapped in a `StatementCache`.
fn seeded_cache() -> StatementCache {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch("CREATE TABLE t(id INTEGER); INSERT INTO t VALUES (1),(2),(3),(4),(5);")
        .unwrap();
    StatementCache::new(Rc::new(conn))
}

/// Drain `SELECT id` from a freshly checked-out statement into a `Vec`.
fn drain_ids(cache: &StatementCache, sql: &str) -> Vec<i64> {
    let mut stmt = cache.get(sql).unwrap();
    let ids = stmt
        .query_map([], |r| r.get::<_, i64>(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    ids
}

// ---------------------------------------------------------------------------
// Core correctness & accounting
// ---------------------------------------------------------------------------

#[test]
fn checkout_use_return_is_balanced_and_correct() {
    let cache = seeded_cache();
    let sql = "SELECT id FROM t ORDER BY id";

    assert_eq!(cache.open_cursors(), 0);

    // Three sequential get/use/return cycles all yield the same correct result,
    // and the open-cursor count is back to zero after each (RAII return on scope
    // exit). The 2nd/3rd cycles are served from rusqlite's cache.
    for _ in 0..3 {
        assert_eq!(drain_ids(&cache, sql), vec![1, 2, 3, 4, 5]);
        assert_eq!(cache.open_cursors(), 0, "statement returned after the scan");
    }
}

#[test]
fn with_runs_against_a_statement_and_returns_it() {
    // Port of JS `StatementCache.use` (`statement-cache.ts:103-110`).
    let cache = seeded_cache();
    let sql = "SELECT count(*) FROM t";

    let count: i64 = cache
        .with(sql, |stmt| stmt.query_row([], |r| r.get(0)).unwrap())
        .unwrap();

    assert_eq!(count, 5);
    assert_eq!(cache.open_cursors(), 0, "released when `with` returned");
}

#[test]
fn get_error_leaves_accounting_untouched() {
    // `get` increments the open-cursor count only AFTER a successful `prepare_cached`
    // (the `?` returns first). A failed prepare must therefore NOT leak a count.
    let cache = seeded_cache();

    let err = cache.get("SELECT FROM (((nonsense");
    assert!(
        err.is_err(),
        "invalid SQL must surface as Err, not a silent miss"
    );
    assert_eq!(
        cache.open_cursors(),
        0,
        "a failed prepare must not increment the open-cursor count"
    );

    // The cache is still usable after the error.
    assert_eq!(
        drain_ids(&cache, "SELECT id FROM t ORDER BY id"),
        vec![1, 2, 3, 4, 5]
    );
}

// ---------------------------------------------------------------------------
// Reentrancy & independence (the self-join crux, `05` §3.10)
// ---------------------------------------------------------------------------

#[test]
fn concurrent_same_sql_checkout_is_independent() {
    // Two checkouts of the SAME sql, alive at once, must be INDEPENDENT statements
    // — A's was removed from the cache on checkout, so B is prepared fresh. We
    // prove it the strong way, by INTERLEAVING the two cursors step-for-step: if
    // they aliased one sqlite3_stmt, interleaved stepping would scramble both.
    let cache = seeded_cache();
    let sql = "SELECT id FROM t ORDER BY id";

    let mut a = cache.get(sql).unwrap();
    let mut b = cache.get(sql).unwrap();
    assert_eq!(cache.open_cursors(), 2, "two cursors checked out at once");

    let mut rows_a = a.query([]).unwrap();
    let mut rows_b = b.query([]).unwrap();

    // Interleave: A row, B row, A row, B row, … Each must independently yield 1..=5.
    let mut seq_a = Vec::new();
    let mut seq_b = Vec::new();
    loop {
        let mut progressed = false;
        if let Some(r) = rows_a.next().unwrap() {
            seq_a.push(r.get::<_, i64>(0).unwrap());
            progressed = true;
        }
        if let Some(r) = rows_b.next().unwrap() {
            seq_b.push(r.get::<_, i64>(0).unwrap());
            progressed = true;
        }
        if !progressed {
            break;
        }
    }
    assert_eq!(seq_a, vec![1, 2, 3, 4, 5], "A's interleaved scan is intact");
    assert_eq!(seq_b, vec![1, 2, 3, 4, 5], "B's interleaved scan is intact");

    drop(rows_a);
    drop(a);
    drop(rows_b);
    drop(b);
    assert_eq!(cache.open_cursors(), 0, "both cursors released");
}

#[test]
fn concurrent_different_sql_is_independent() {
    // The same independence, but the two checkouts are DIFFERENT SQL (asc vs desc)
    // — distinct cache keys, distinct prepared statements. B's reversed scan, taken
    // while A is mid-iteration, must not perturb A's forward order.
    let cache = seeded_cache();

    let mut a = cache.get("SELECT id FROM t ORDER BY id").unwrap();
    let mut rows_a = a.query([]).unwrap();
    let first_a: i64 = rows_a.next().unwrap().unwrap().get(0).unwrap();
    assert_eq!(first_a, 1, "A is ascending");

    {
        let mut b = cache.get("SELECT id FROM t ORDER BY id DESC").unwrap();
        assert_eq!(cache.open_cursors(), 2);
        let mut rows_b = b.query([]).unwrap();
        let first_b: i64 = rows_b.next().unwrap().unwrap().get(0).unwrap();
        assert_eq!(first_b, 5, "B is descending, from a fresh statement");
    }
    assert_eq!(cache.open_cursors(), 1, "B released; A still open");

    let second_a: i64 = rows_a.next().unwrap().unwrap().get(0).unwrap();
    assert_eq!(second_a, 2, "A's ascending order was untouched by B");

    drop(rows_a);
    drop(a);
    assert_eq!(cache.open_cursors(), 0);
}

#[test]
fn three_deep_nesting_stays_independent() {
    // Three cursors over the same SQL, alive simultaneously (A→B→C). Each yields
    // its own first row, C advances while A and B sit paused, and B then A resume
    // exactly where they left off — proving none share cursor state, at depth 3.
    let cache = seeded_cache();
    let sql = "SELECT id FROM t ORDER BY id";

    let mut a = cache.get(sql).unwrap();
    let mut rows_a = a.query([]).unwrap();
    assert_eq!(rows_a.next().unwrap().unwrap().get::<_, i64>(0).unwrap(), 1);

    {
        let mut b = cache.get(sql).unwrap();
        let mut rows_b = b.query([]).unwrap();
        assert_eq!(rows_b.next().unwrap().unwrap().get::<_, i64>(0).unwrap(), 1);

        {
            let mut c = cache.get(sql).unwrap();
            assert_eq!(cache.open_cursors(), 3, "A, B, C all checked out");
            let mut rows_c = c.query([]).unwrap();
            assert_eq!(rows_c.next().unwrap().unwrap().get::<_, i64>(0).unwrap(), 1);
            // C advances a second step while A and B are paused mid-scan.
            assert_eq!(rows_c.next().unwrap().unwrap().get::<_, i64>(0).unwrap(), 2);
        }
        assert_eq!(cache.open_cursors(), 2, "C released");
        // B resumes from where it paused.
        assert_eq!(rows_b.next().unwrap().unwrap().get::<_, i64>(0).unwrap(), 2);
    }
    assert_eq!(cache.open_cursors(), 1, "B released");
    // A resumes from where it paused.
    assert_eq!(rows_a.next().unwrap().unwrap().get::<_, i64>(0).unwrap(), 2);

    drop(rows_a);
    drop(a);
    assert_eq!(cache.open_cursors(), 0);
}

#[test]
fn get_inside_an_active_scan_is_reentrant_and_balanced() {
    // The realistic reentrancy pattern that motivates the whole cache: a per-row
    // step checks out ANOTHER statement *while the outer scan is mid-step* (e.g.
    // resolving related data during a fetch). The outer statement was removed from
    // rusqlite's cache on checkout, so the inner `get` cannot alias it; the cache's
    // RefCell is borrowed only transiently (checkout + return), never across a vend,
    // so no `already borrowed` panic. The count climbs to 2 inside the nested
    // checkout and unwinds cleanly each row.
    let cache = seeded_cache();
    let outer = "SELECT id FROM t ORDER BY id";
    let inner = "SELECT count(*) FROM t";

    let mut a = cache.get(outer).unwrap();
    assert_eq!(cache.open_cursors(), 1);

    let mut products = Vec::new();
    let mut rows = a.query([]).unwrap();
    while let Some(r) = rows.next().unwrap() {
        let id: i64 = r.get(0).unwrap();
        // Reentrant checkout DURING the outer vend.
        let mut inner_stmt = cache.get(inner).unwrap();
        assert_eq!(
            cache.open_cursors(),
            2,
            "outer + inner open together mid-row"
        );
        let cnt: i64 = inner_stmt.query_row([], |row| row.get(0)).unwrap();
        products.push(id * cnt);
        // `inner_stmt` drops at the end of the loop body → open back to 1.
    }
    drop(rows);
    drop(a);

    assert_eq!(
        cache.open_cursors(),
        0,
        "all released after nested reentrancy"
    );
    assert_eq!(
        products,
        vec![5, 10, 15, 20, 25],
        "id * count(*), count == 5"
    );
}

#[test]
fn deeply_nested_with_closures_balance() {
    // `with` calls nested three deep — each does an implicit get + return around its
    // closure. The innermost runs with all three checked out; everything unwinds to
    // zero. This is the ergonomic face of the reentrancy the loops above test.
    let cache = seeded_cache();

    let result = cache
        .with("SELECT id FROM t WHERE id = 1", |a| {
            let id_a: i64 = a.query_row([], |r| r.get(0)).unwrap();
            let (id_b, id_c) = cache
                .with("SELECT id FROM t WHERE id = 2", |b| {
                    let id_b: i64 = b.query_row([], |r| r.get(0)).unwrap();
                    let id_c: i64 = cache
                        .with("SELECT id FROM t WHERE id = 3", |c| {
                            assert_eq!(cache.open_cursors(), 3, "all three with()s open");
                            c.query_row([], |r| r.get(0)).unwrap()
                        })
                        .unwrap();
                    (id_b, id_c)
                })
                .unwrap();
            (id_a, id_b, id_c)
        })
        .unwrap();

    assert_eq!(result, (1, 2, 3));
    assert_eq!(
        cache.open_cursors(),
        0,
        "every with() returned its statement"
    );
}

// ---------------------------------------------------------------------------
// RAII release — the consequence the JS `finally` exists for
// ---------------------------------------------------------------------------

#[test]
fn raii_release_on_early_break_frees_connection_for_write() {
    // Port of `sqlite_leaf.rs::raii_releases_statement_on_early_break`, proving the
    // *consequence* the JS `finally → rowIterator.return?.()` exists for: after an
    // early break, the connection must be free to WRITE. A read statement left
    // mid-step (SQLITE_ROW) holds a lock → a write would fail "database is locked".
    let cache = seeded_cache();
    let sql = "SELECT id FROM t ORDER BY id";

    {
        let mut stmt = cache.get(sql).unwrap();
        assert_eq!(cache.open_cursors(), 1);
        let mut rows = stmt.query([]).unwrap();
        // Read only the first 2 of 5 rows, then break — cursor left mid-scan.
        let mut seen = 0;
        while rows.next().unwrap().is_some() {
            seen += 1;
            if seen == 2 {
                break;
            }
        }
        assert_eq!(cache.open_cursors(), 1, "still open mid-scan");
        // `rows` then `stmt` drop at end of scope → cursor reset, statement returned.
    }
    assert_eq!(
        cache.open_cursors(),
        0,
        "released on early break, no finally"
    );

    // The payoff: the connection is free, so a write succeeds.
    let n = cache
        .conn()
        .execute("INSERT INTO t VALUES (6)", [])
        .expect("write must succeed: the read cursor was reset on drop");
    assert_eq!(n, 1);
    assert_eq!(
        drain_ids(&cache, sql),
        vec![1, 2, 3, 4, 5, 6],
        "the write landed and a fresh cached read sees it"
    );
}

#[test]
fn return_while_another_cursor_is_live_does_not_deadlock() {
    // Drop A (returning its statement to the cache — a transient `RefCell::borrow_mut`
    // in rusqlite's `cache_stmt`) WHILE B's cursor is still mid-iteration. Because
    // B's statement was removed from the cache on checkout, B is not borrowing the
    // RefCell, so A's return can't double-borrow. B keeps yielding correctly after.
    let cache = seeded_cache();
    let sql = "SELECT id FROM t ORDER BY id";

    let a = cache.get(sql).unwrap();
    let mut b = cache.get(sql).unwrap();
    assert_eq!(cache.open_cursors(), 2);
    let mut rows_b = b.query([]).unwrap();
    assert_eq!(rows_b.next().unwrap().unwrap().get::<_, i64>(0).unwrap(), 1);

    drop(a); // returns A's statement to the cache while rows_b is live
    assert_eq!(cache.open_cursors(), 1, "A returned; B still open");

    assert_eq!(
        rows_b.next().unwrap().unwrap().get::<_, i64>(0).unwrap(),
        2,
        "B's cursor survived A's return-to-cache"
    );
    drop(rows_b);
    drop(b);
    assert_eq!(cache.open_cursors(), 0);
}

// ---------------------------------------------------------------------------
// discard — drop without returning to the cache
// ---------------------------------------------------------------------------

#[test]
fn discard_does_not_poison_accounting() {
    // `discard` drops the statement WITHOUT returning it to the cache (the JS "not
    // an error to fail to call return"), but the open-cursor count is still balanced,
    // and the next checkout still works.
    let cache = seeded_cache();
    let sql = "SELECT id FROM t ORDER BY id";

    let stmt = cache.get(sql).unwrap();
    assert_eq!(cache.open_cursors(), 1);
    stmt.discard();
    assert_eq!(cache.open_cursors(), 0, "discard still balances accounting");

    // A fresh checkout (prepared anew, since the discarded one was not cached)
    // still produces correct results.
    assert_eq!(drain_ids(&cache, sql), vec![1, 2, 3, 4, 5]);
    assert_eq!(cache.open_cursors(), 0);
}

#[test]
fn discard_while_another_cursor_is_live() {
    // discard (unlike drop) skips the cache return, but must still decrement the
    // open-cursor count by exactly one — leaving B's count intact and B usable.
    let cache = seeded_cache();
    let sql = "SELECT id FROM t ORDER BY id";

    let a = cache.get(sql).unwrap();
    let mut b = cache.get(sql).unwrap();
    assert_eq!(cache.open_cursors(), 2);
    let mut rows_b = b.query([]).unwrap();
    assert_eq!(rows_b.next().unwrap().unwrap().get::<_, i64>(0).unwrap(), 1);

    a.discard();
    assert_eq!(
        cache.open_cursors(),
        1,
        "discard decremented A only; B still open"
    );

    assert_eq!(rows_b.next().unwrap().unwrap().get::<_, i64>(0).unwrap(), 2);
    drop(rows_b);
    drop(b);
    assert_eq!(cache.open_cursors(), 0);
}

// ---------------------------------------------------------------------------
// Capacity / eviction (LRU bound — `05` §13 Q7 deviation from JS drop(n))
// ---------------------------------------------------------------------------

#[test]
fn capacity_zero_disables_caching_but_stays_correct() {
    // Capacity 0 means every `get` prepares fresh and nothing is retained — a valid
    // (if cache-defeating) configuration. Correctness must be unaffected.
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch("CREATE TABLE t(id INTEGER); INSERT INTO t VALUES (1),(2),(3);")
        .unwrap();
    let cache = StatementCache::with_capacity(Rc::new(conn), 0);
    let sql = "SELECT id FROM t ORDER BY id";

    assert_eq!(drain_ids(&cache, sql), vec![1, 2, 3]);
    assert_eq!(drain_ids(&cache, sql), vec![1, 2, 3]);
    assert_eq!(cache.open_cursors(), 0);

    // Re-enabling the cache mid-life is fine (resolves §13 Q7 tuning).
    cache.set_capacity(DEFAULT_CAPACITY);
    assert_eq!(drain_ids(&cache, sql), vec![1, 2, 3]);
}

#[test]
fn evicted_statement_is_reprepared_fresh() {
    // With a single LRU slot, a second distinct SQL evicts the first. Re-checking
    // out the evicted SQL must re-prepare it fresh and still be correct — i.e. an
    // evicted (finalized) statement never aliases a later prepare.
    let cache = seeded_cache();
    cache.set_capacity(1);

    let asc = "SELECT id FROM t ORDER BY id";
    let desc = "SELECT id FROM t ORDER BY id DESC";

    assert_eq!(drain_ids(&cache, asc), vec![1, 2, 3, 4, 5]); // slot = asc
    assert_eq!(drain_ids(&cache, desc), vec![5, 4, 3, 2, 1]); // evicts asc, slot = desc
    assert_eq!(drain_ids(&cache, asc), vec![1, 2, 3, 4, 5]); // asc re-prepared fresh
    assert_eq!(cache.open_cursors(), 0);
}
