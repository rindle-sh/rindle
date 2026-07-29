//! Phase 3 — the cross-check, plus step-fallibility and RAII for the SQLite
//! leaf. The headline proof is `cross_check_*`: ONE generic function over the
//! lending [`RowStream`]/[`RowRef`] traits, driven by BOTH the rusqlite
//! step-buffer cursor (`SqliteRowStream`) and the held-`Rc` [`BTreeCursor`] —
//! the two *hardest* and most *opposite* row provenances (a transient buffer
//! reused every step vs. an `Rc`-pinned snapshot), satisfying one trait.

use std::cell::Cell;
use std::rc::Rc;

use rusqlite::Connection;

use rindle::btree::BTree;
use rindle::value::{owned_row, ColId, OwnedValue, RowRef, RowStream, Sort, Value};
use rindle_sqlite::sqlite::{select_sql, ColType, SqliteError, SqliteRowStream, StmtGuard};

// ---------------------------------------------------------------------------
// The generic consumers — written ONCE, against the trait, used over BOTH
// backends. This is the union the spike exists to prove.
// ---------------------------------------------------------------------------

/// Sum column `c` as ints over any lending row stream.
fn sum_col<S: RowStream>(s: &mut S, c: ColId) -> i64 {
    let mut sum = 0;
    while let Some(r) = s.next_row() {
        if let Value::Int(i) = r.col(c) {
            sum += i;
        }
    }
    sum
}

/// Collect column `c` as owned strings (the escape copy) over any row stream.
fn collect_strs<S: RowStream>(s: &mut S, c: ColId) -> Vec<String> {
    let mut out = Vec::new();
    while let Some(r) = s.next_row() {
        if let Value::Str(b) = r.col(c) {
            out.push(String::from_utf8(b.to_vec()).expect("utf8"));
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Cross-check: one trait, two backends, identical results.
// ---------------------------------------------------------------------------

#[test]
fn cross_check_ints_one_trait_two_backends() {
    // --- SQLite leaf (borrowed step buffer) ---
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "CREATE TABLE t(id INTEGER, name TEXT);
         INSERT INTO t VALUES (1,'a'),(2,'bb'),(3,'ccc');",
    )
    .unwrap();
    let types = [ColType::Int, ColType::Text];
    let sql = select_sql("t", &["id", "name"]);
    let mut stmt = conn.prepare(&sql).unwrap();
    let mut sq = SqliteRowStream::new(stmt.query([]).unwrap(), &types);
    let sqlite_sum = sum_col(&mut sq, 0);

    // --- Memory leaf (held-Rc BTreeCursor) ---
    let sort: Sort = vec![(0, true)];
    let mut t = BTree::new();
    for (id, name) in [(1, "a"), (2, "bb"), (3, "ccc")] {
        t.add(
            owned_row(vec![OwnedValue::Int(id), OwnedValue::str(name)]),
            &sort,
        );
    }
    let mut bc = t.values_from(None, true, &sort);
    let btree_sum = sum_col(&mut bc, 0);

    assert_eq!(sqlite_sum, 6);
    assert_eq!(btree_sum, 6);
    assert_eq!(
        sqlite_sum, btree_sum,
        "the SAME generic sum_col drove both backends to the same answer"
    );
}

#[test]
fn cross_check_strings_one_trait_two_backends() {
    let names = ["alpha", "beta", "gamma"];

    // --- SQLite leaf ---
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch("CREATE TABLE t(name TEXT);").unwrap();
    for n in names {
        conn.execute("INSERT INTO t(name) VALUES (?1)", [n])
            .unwrap();
    }
    let types = [ColType::Text];
    let sql = select_sql("t", &["name"]);
    let mut stmt = conn.prepare(&sql).unwrap();
    let mut sq = SqliteRowStream::new(stmt.query([]).unwrap(), &types);
    let mut sqlite_names = collect_strs(&mut sq, 0);
    sqlite_names.sort();

    // --- Memory leaf ---
    let sort: Sort = vec![(0, true)];
    let mut t = BTree::new();
    for n in names {
        t.add(owned_row(vec![OwnedValue::str(n)]), &sort);
    }
    let mut bc = t.values_from(None, true, &sort);
    let btree_names = collect_strs(&mut bc, 0); // already sorted (btree order)

    assert_eq!(sqlite_names, vec!["alpha", "beta", "gamma"]);
    assert_eq!(
        sqlite_names, btree_names,
        "string columns vend identically (bytes) across both backends"
    );
}

// ---------------------------------------------------------------------------
// The lending contract, made vivid: SQLite reuses ONE column buffer across
// steps, so a borrowed `Str` is invalidated by the next `next_row`. The
// compiler forbids holding it across the step; `to_owned_row()` escapes it.
// ---------------------------------------------------------------------------

#[test]
fn step_buffer_is_reused_so_owning_is_what_escapes_it() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "CREATE TABLE t(name TEXT);
         INSERT INTO t VALUES ('first'),('second');",
    )
    .unwrap();
    let types = [ColType::Text];
    let sql = select_sql("t", &["name"]);
    let mut stmt = conn.prepare(&sql).unwrap();
    let mut sq = SqliteRowStream::new(stmt.query([]).unwrap(), &types);

    // Row 1: capture the buffer POINTER (a Copy usize — NOT the borrow, which the
    // borrow checker would forbid holding across the next step), and own the row.
    let r1 = sq.next_row().unwrap();
    let ptr1 = match r1.col(0) {
        Value::Str(b) => b.as_ptr() as usize,
        _ => unreachable!(),
    };
    let owned1 = r1.to_owned_row(); // the forced copy — escapes the step buffer

    // Row 2: same column buffer, REUSED at the same address (the classic SQLite
    // "valid only until the next step"). The owned row 1 is unaffected.
    let r2 = sq.next_row().unwrap();
    let ptr2 = match r2.col(0) {
        Value::Str(b) => b.as_ptr() as usize,
        _ => unreachable!(),
    };
    let owned2 = r2.to_owned_row();

    assert_eq!(
        ptr1, ptr2,
        "SQLite reuses ONE column buffer across steps — the borrow MUST be transient"
    );
    assert!(matches!(owned1.col(0), rindle::value::Value::Str(b"first")));
    assert!(matches!(
        owned2.col(0),
        rindle::value::Value::Str(b"second")
    ));
}

// ---------------------------------------------------------------------------
// Step fallibility (handoff decision 1 / 05 OQ-9): parked, then resurfaced.
// ---------------------------------------------------------------------------

#[test]
fn step_error_is_parked_and_resurfaced_not_swallowed() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "CREATE TABLE t(id INTEGER, x INTEGER);
         INSERT INTO t VALUES (1,10),(2,20),(3,-9223372036854775808),(4,40);",
    )
    .unwrap();
    // `abs(x)` raises an integer-overflow step error exactly at x = i64::MIN
    // (row id=3). No ORDER BY ⇒ rowid order, so the error lands mid-scan.
    let types = [ColType::Int, ColType::Int];
    let mut stmt = conn.prepare("SELECT id, abs(x) FROM t").unwrap();
    let mut sq = SqliteRowStream::new(stmt.query([]).unwrap(), &types);

    let mut ids = Vec::new();
    while let Some(r) = sq.next_row() {
        if let Value::Int(i) = r.col(0) {
            ids.push(i);
        }
    }

    // Rows before the failing step vended cleanly; the bad step parked an error
    // and ended the stream — NOT a premature/silent end.
    assert_eq!(
        ids,
        vec![1, 2],
        "rows before the overflow vended; the error was not premature"
    );
    assert!(
        matches!(sq.take_error(), Some(SqliteError::Step(_))),
        "the sqlite3_step error must be parked and resurfaced at the owning boundary"
    );
    // And it is taken exactly once.
    assert!(sq.take_error().is_none(), "error consumed by take_error");
}

// ---------------------------------------------------------------------------
// RAII cursor cleanup (Primitive #2): the statement is released on Drop —
// including on an early `break` — with no `finally`.
// ---------------------------------------------------------------------------

#[test]
fn raii_releases_statement_on_early_break() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch("CREATE TABLE t(id INTEGER); INSERT INTO t VALUES (1),(2),(3),(4),(5);")
        .unwrap();
    let types = [ColType::Int];
    let open = Rc::new(Cell::new(0i64));

    {
        let mut stmt = conn.prepare("SELECT id FROM t").unwrap();
        let mut sq = SqliteRowStream::guarded(
            stmt.query([]).unwrap(),
            &types,
            StmtGuard::new(open.clone()),
        );
        assert_eq!(open.get(), 1, "cursor counted as open");

        // Read only the first 2 rows, then break early (a `Take`-style consumer).
        let mut seen = 0;
        while let Some(r) = sq.next_row() {
            if let Value::Int(_) = r.col(0) {
                seen += 1;
            }
            if seen == 2 {
                break;
            }
        }
        assert_eq!(open.get(), 1, "still open mid-scan");
        // `sq` drops here at end of scope — Rows resets the statement, guard fires.
    }

    assert_eq!(
        open.get(),
        0,
        "RAII released the statement on early break — no finally, no 'database is busy'"
    );
}
