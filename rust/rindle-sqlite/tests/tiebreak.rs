//! In-engine sort tiebreak for the SQLite source (`designs-implemented/204-IN-ENGINE-SORT-TIEBREAK-DESIGN.md`).
//!
//! These drive [`TableSource::fetch`] directly against a **rowid** table with a **TEXT
//! primary key** and an unsuffixed `(ord)` index — the shape the adapter is *for*: the
//! `(ord)` index is physically `(ord, rowid)` and cannot serve the engine's `(ord, k)`
//! order, so SQL orders by `ord` alone and the adapter re-applies the `k` tiebreak in
//! engine.
//!
//! The differential net is a fresh SQL query with the true lexicographic bound. The
//! sharpest case is a **`Basis::After` seek that lands inside an equal-`ord` run**: if
//! the SQL start bound were passed through verbatim (strict `ord > c`) instead of being
//! relaxed to inclusive-on-the-prefix (`ord >= c`), the equal-`ord` tail would be
//! dropped and the fetch would diverge from the oracle.

use std::rc::Rc;

use rusqlite::Connection;

use rindle::change::{Basis, FetchRequest, Start};
use rindle::{owned_row as orow, OwnedRow, OwnedValue, Source, ValueType};
use rindle_sqlite::{ColumnDef, TableSource};

/// `kv(k TEXT PRIMARY KEY, ord INTEGER)` on a rowid table (NOT `WITHOUT ROWID`), plus
/// an unsuffixed `(ord)` index — so the tiebreak adapter engages.
fn kv() -> (Rc<Connection>, TableSource) {
    let conn = Rc::new(Connection::open_in_memory().unwrap());
    conn.execute_batch(
        "CREATE TABLE kv (k TEXT NOT NULL PRIMARY KEY, ord INTEGER NOT NULL);
         CREATE INDEX ix_kv_ord ON kv (ord);",
    )
    .unwrap();
    let cols = vec![
        ColumnDef {
            name: "k".into(),
            ty: ValueType::String,
            optional: false,
        },
        ColumnDef {
            name: "ord".into(),
            ty: ValueType::Number,
            optional: false,
        },
    ];
    // PK = column 0 (`k`).
    let ts = TableSource::new(conn.clone(), "kv", cols, vec![0]);
    (conn, ts)
}

fn seed(conn: &Connection, rows: &[(&str, i64)]) {
    for (k, ord) in rows {
        conn.execute(
            "INSERT INTO kv (k, ord) VALUES (?1, ?2)",
            rusqlite::params![k, ord],
        )
        .unwrap();
    }
}

/// The equal-`ord` runs at ord=1 (`a,b,c`) and ord=2 (`k,m,z`) are what exercise the
/// in-engine tiebreak and the boundary seeks.
fn seed_runs(conn: &Connection) {
    seed(
        conn,
        &[
            ("m", 2),
            ("a", 1),
            ("c", 1),
            ("b", 1),
            ("z", 2),
            ("k", 2),
            ("q", 3),
        ],
    );
}

/// A full row `[k, ord]`; `ord` is a Number column, vended as f64, so the start row
/// must use `Float` to match `compare_values` (mixing Int/Float trips its guard).
fn row(k: &str, ord: i64) -> OwnedRow {
    orow(vec![OwnedValue::str(k), OwnedValue::Float(ord as f64)])
}

fn k_of(row: &OwnedRow) -> String {
    match row.col(0) {
        rindle::value::Value::Str(b) => String::from_utf8_lossy(b).to_string(),
        other => panic!("expected a TEXT pk in column 0, got {other:?}"),
    }
}

/// The ground-truth oracle: run `sql` (selecting `k` first) against the same snapshot.
fn oracle(conn: &Connection, sql: &str) -> Vec<String> {
    let mut stmt = conn.prepare(sql).unwrap();
    stmt.query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

/// Fetch `req` on an `ORDER BY ord` (asc) connection and collect the TEXT pks.
fn fetch_keys(ts: &TableSource, req: &FetchRequest) -> Vec<String> {
    // Connection sort is the engine's resolved total order: (ord asc, k asc).
    let c = ts.connect(Some(vec![(1, true), (0, true)]), None, vec![]);
    let out: Vec<String> = ts.fetch(c, req).map(|r| k_of(&r)).collect();
    assert!(ts.take_fetch_error().is_none(), "fetch parked an error");
    assert_eq!(ts.cursors_open(), 0, "cursor released after drain");
    out
}

#[test]
fn full_scan_applies_pk_tiebreak() {
    let (conn, ts) = kv();
    seed_runs(&conn);
    assert_eq!(
        fetch_keys(&ts, &FetchRequest::all()),
        oracle(&conn, "SELECT k FROM kv ORDER BY ord, k"),
        "forward scan must apply the (ord, k) tiebreak in-engine"
    );
}

#[test]
fn full_scan_reverse_applies_pk_tiebreak() {
    let (conn, ts) = kv();
    seed_runs(&conn);
    let req = FetchRequest {
        reverse: true,
        ..Default::default()
    };
    assert_eq!(
        fetch_keys(&ts, &req),
        oracle(&conn, "SELECT k FROM kv ORDER BY ord DESC, k DESC"),
        "reverse scan flips the whole (ord, k) order, appended pk included"
    );
}

#[test]
fn seek_at_inside_run() {
    let (conn, ts) = kv();
    seed_runs(&conn);
    // At (ord=2, k='m'): includes the start row and the rest of the ord=2 run.
    let req = FetchRequest {
        start: Some(Start {
            row: row("m", 2),
            basis: Basis::At,
        }),
        ..Default::default()
    };
    assert_eq!(
        fetch_keys(&ts, &req),
        oracle(
            &conn,
            "SELECT k FROM kv WHERE ord > 2 OR (ord = 2 AND k >= 'm') ORDER BY ord, k"
        ),
    );
}

#[test]
fn seek_after_inside_run() {
    let (conn, ts) = kv();
    seed_runs(&conn);
    // After (ord=2, k='m') lands INSIDE the ord=2 run: 'z' (ord=2, k>'m') must survive.
    // This is the case the start-bound relaxation exists for — a verbatim strict
    // `ord > 2` bound would wrongly drop the ord=2 tail.
    let req = FetchRequest {
        start: Some(Start {
            row: row("m", 2),
            basis: Basis::After,
        }),
        ..Default::default()
    };
    assert_eq!(
        fetch_keys(&ts, &req),
        oracle(
            &conn,
            "SELECT k FROM kv WHERE ord > 2 OR (ord = 2 AND k > 'm') ORDER BY ord, k"
        ),
    );
}

#[test]
fn seek_after_reverse_inside_run() {
    let (conn, ts) = kv();
    seed_runs(&conn);
    // Reverse After (ord=2, k='z') lands inside the ord=2 run (descending): 'm' and 'k'
    // (ord=2, k<'z') must survive — the reverse analogue of the relaxation.
    let req = FetchRequest {
        reverse: true,
        start: Some(Start {
            row: row("z", 2),
            basis: Basis::After,
        }),
        ..Default::default()
    };
    assert_eq!(
        fetch_keys(&ts, &req),
        oracle(
            &conn,
            "SELECT k FROM kv WHERE ord < 2 OR (ord = 2 AND k < 'z') ORDER BY ord DESC, k DESC"
        ),
    );
}
