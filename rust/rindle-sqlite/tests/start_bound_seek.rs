//! **Execution test: a keyset start-bound fetch SEEKS the index, it does not
//! filter-scan the partition.**
//!
//! `build_select_query` lowers a `Start` bound as an OR-of-ANDs lexicographic
//! comparison. That disjunction is *not* sargable, so on its own SQLite satisfies
//! the `ORDER BY` by scanning the index from the far end and filter-discarding every
//! row before the window boundary — `O(partition)` work per fetch. Since a `Take`
//! window is maintained with exactly such a boundary-anchored fetch on every insert,
//! that made a single `Add` into a large sorted partition cost work proportional to
//! the whole partition (measured 1.67M → 6.30M VM steps as the bench table grew
//! 812 → 2712 rows).
//!
//! The fix prepends a redundant, sargable `o[0] <op>= v0` term on the (non-nullable)
//! leading sort column — entailed by the disjunction, so result-preserving — which
//! lets SQLite seek the index range instead. This test pins the *behavioural*
//! consequence: the SQLite work for a fixed-size window fetch stays ~constant as the
//! partition grows 8× (a scan would grow ~8×), while returning exactly the right
//! rows. It fails if the sargable prefix ever regresses.

use rusqlite::{params_from_iter, Connection, StatementStatus};

use rindle::change::{Basis, Start};
use rindle::value::Sort;
use rindle::{owned_row, OwnedValue};
use rindle_sqlite::{build_select_query, ColumnDef};

const WINDOW: i64 = 50;

fn col(name: &str) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        ty: rindle::value::ValueType::Number,
        optional: false,
    }
}

/// `t(seq, id)` with a covering `(seq DESC, id ASC)` index, seeded with `n` rows
/// where `seq == id == 1..=n` (so the top-`WINDOW` by `seq DESC` is ids `n-49..=n`).
fn seed(n: i64) -> Connection {
    let conn = Connection::open_in_memory().expect("open");
    conn.execute_batch(
        "CREATE TABLE t(seq REAL NOT NULL, id REAL NOT NULL);
         CREATE INDEX ix ON t(seq DESC, id ASC);",
    )
    .expect("schema");
    conn.execute(
        "WITH RECURSIVE g(k) AS (SELECT 1 UNION ALL SELECT k+1 FROM g WHERE k < ?1)
         INSERT INTO t SELECT CAST(k AS REAL), CAST(k AS REAL) FROM g;",
        [n],
    )
    .expect("seed");
    conn
}

/// Run the boundary-anchored window-maintenance fetch (`reverse`, `At` the current
/// window boundary — the `Take` displacement shape) over an `n`-row partition and
/// return `(ids returned, SQLite VM steps taken)`.
fn window_fetch(n: i64) -> (Vec<i64>, i64) {
    // The window boundary = the WINDOW-th highest seq, i.e. seq == id == n-49.
    let boundary = (n - (WINDOW - 1)) as f64;
    let columns = [col("seq"), col("id")]; // ColId: seq = 0, id = 1
    let order: Sort = vec![(0, false), (1, true)]; // seq DESC, id ASC (index order)
    let start = Start {
        row: owned_row(vec![
            OwnedValue::Float(boundary),
            OwnedValue::Float(boundary),
        ]),
        basis: Basis::At,
    };

    let q = build_select_query(
        "t",
        &columns,
        None, // no partition constraint — the sort/start bound alone must seek
        &[],  // no multi-constraints
        None, // no residual filter
        Some(&order),
        true, // reverse: fetch toward the top of the window (the displacement direction)
        Some(&start),
    );

    let conn = seed(n);
    let mut stmt = conn.prepare(&q.sql).expect("prepare");
    let ids: Vec<i64> = stmt
        .query_map(params_from_iter(q.params.iter()), |r| {
            Ok(r.get::<_, f64>(1)? as i64)
        })
        .expect("query")
        .collect::<Result<_, _>>()
        .expect("rows");
    // VMSTEP accumulates across every `sqlite3_step`; read it after the drain.
    let vm = stmt.reset_status(StatementStatus::VmStep) as i64;
    (ids, vm)
}

/// The plan SQLite actually chose, for diagnostics + a coarse seek assertion.
fn query_plan(n: i64) -> String {
    let boundary = (n - (WINDOW - 1)) as f64;
    let columns = [col("seq"), col("id")];
    let order: Sort = vec![(0, false), (1, true)];
    let start = Start {
        row: owned_row(vec![
            OwnedValue::Float(boundary),
            OwnedValue::Float(boundary),
        ]),
        basis: Basis::At,
    };
    let q = build_select_query(
        "t",
        &columns,
        None,
        &[],
        None,
        Some(&order),
        true,
        Some(&start),
    );
    let conn = seed(n);
    let mut stmt = conn
        .prepare(&format!("EXPLAIN QUERY PLAN {}", q.sql))
        .expect("prepare eqp");
    stmt.query_map(params_from_iter(q.params.iter()), |r| r.get::<_, String>(3))
        .expect("eqp")
        .collect::<Result<Vec<_>, _>>()
        .expect("eqp rows")
        .join(" | ")
}

#[test]
fn window_fetch_returns_exactly_the_top_window() {
    let (mut ids, _) = window_fetch(2000);
    ids.sort_unstable();
    let expected: Vec<i64> = (2000 - (WINDOW - 1)..=2000).collect();
    assert_eq!(
        ids, expected,
        "the boundary-anchored fetch must return exactly the top-{WINDOW} rows"
    );
}

#[test]
fn window_fetch_cost_is_flat_as_partition_grows() {
    // Same fixed-size (WINDOW-row) result, partition grown 8×. A seek is ~O(log n) so
    // the SQLite work is essentially constant; a filter-scan of the OR-of-ANDs bound
    // would be ~O(partition) and grow ~8×.
    let (ids_small, vm_small) = window_fetch(1_000);
    let (ids_large, vm_large) = window_fetch(8_000);

    assert_eq!(ids_small.len(), WINDOW as usize, "small: WINDOW rows");
    assert_eq!(ids_large.len(), WINDOW as usize, "large: WINDOW rows");

    eprintln!(
        "window fetch VM steps: n=1000 -> {vm_small}, n=8000 -> {vm_large} | plan: {}",
        query_plan(8_000)
    );

    // Non-growth: an index seek keeps this ~flat; a partition scan would roughly
    // octuple it. `< 2×` cleanly separates the two (seek ≈ 1×, scan ≈ 8×).
    assert!(
        vm_large < vm_small * 2,
        "window-maintenance fetch scales with partition size — the start bound is not \
         seeking. VM steps: n=1000 -> {vm_small}, n=8000 -> {vm_large} (expected ~flat). \
         Plan: {}",
        query_plan(8_000)
    );
}
