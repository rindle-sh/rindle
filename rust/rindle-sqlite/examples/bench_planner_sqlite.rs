//! `bench_planner_sqlite` — the planner's per-pattern re-estimation cost against the
//! **real** [`SqliteCostModel`], and the win from wrapping it in
//! [`rindle_planner::CachingCostModel`].
//!
//! The sibling `rindle-planner/examples/bench_planner.rs` measures the same thing with
//! a *simulated* per-call latency (a busy-spin). This one uses real
//! `sqlite3_prepare_v2` + scanstatus reads over an `ANALYZE`'d database, so the ms are
//! measured, not modeled. It confirms the design-doc claim: the cache turns a
//! worst-case registration from a many-prepare stall into a handful of prepares.
//!
//! Per `(shape, n)` it reports `estimate()` call count and `plan_ast` wall-time, bare
//! vs cached. See `PLANNER-INTEGRATION-DESIGN.md` §4.
//!
//! Run: `cargo run -p rindle-sqlite --example bench_planner_sqlite --release`

use std::cell::Cell;
use std::rc::Rc;
use std::time::Instant;

use rindle::{
    Ast, Condition, CorrelatedSubquery, CorrelatedSubqueryCondition, Correlation, Dir, ExistsOp,
    OrderPart,
};
use rindle_planner::{plan_ast, CachingCostModel, ConnectionCostModel, CostModelCost};
use rindle_sqlite::SqliteCostModel;
use rusqlite::{params, Connection};

// ---------------------------------------------------------------------------
// A call-counting decorator over any cost model (to count real prepares).
// ---------------------------------------------------------------------------

struct Counting<M: ConnectionCostModel> {
    inner: M,
    calls: Cell<u64>,
}

impl<M: ConnectionCostModel> Counting<M> {
    fn new(inner: M) -> Self {
        Self {
            inner,
            calls: Cell::new(0),
        }
    }
    fn calls(&self) -> u64 {
        self.calls.get()
    }
}

impl<M: ConnectionCostModel> ConnectionCostModel for Counting<M> {
    fn estimate(
        &self,
        table: &str,
        sort: &[OrderPart],
        filters: Option<&Condition>,
        constraint: Option<&rindle_planner::PlannerConstraint>,
    ) -> CostModelCost {
        self.calls.set(self.calls.get() + 1);
        self.inner.estimate(table, sort, filters, constraint)
    }
}

// ---------------------------------------------------------------------------
// Database: a chain of uniform tables t0..tN, ANALYZE'd, so estimate() does real work.
// ---------------------------------------------------------------------------

const MAX_N: usize = 9;
const ROOT_ROWS: i64 = 2000;
const CHILD_ROWS: i64 = 300;

fn setup_db() -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    for i in 0..=MAX_N {
        // Every table: id PK + an indexed parentID (the correlation column). t0 is the
        // root (parentID unused); t1..tN reference the parent id-space.
        conn.execute_batch(&format!(
            "CREATE TABLE t{i} (id INTEGER PRIMARY KEY, parentID INTEGER, body TEXT);
             CREATE INDEX t{i}_parentID ON t{i}(parentID);"
        ))
        .unwrap();
    }
    // t0: the large parent.
    for id in 0..ROOT_ROWS {
        conn.execute(
            "INSERT INTO t0 (id, parentID, body) VALUES (?, 0, 'x')",
            params![id],
        )
        .unwrap();
    }
    // t1..tN: smaller, each row pointing at a parent id (skewed: children are selective).
    for i in 1..=MAX_N {
        for id in 0..CHILD_ROWS {
            let parent = id % ROOT_ROWS;
            conn.execute(
                &format!("INSERT INTO t{i} (id, parentID, body) VALUES (?, ?, 'x')"),
                params![id, parent],
            )
            .unwrap();
        }
    }
    conn.execute_batch("ANALYZE;").unwrap();
    conn
}

// ---------------------------------------------------------------------------
// AST shapes (shared with the planner-crate bench).
// ---------------------------------------------------------------------------

fn order_id() -> Vec<OrderPart> {
    vec![OrderPart("id".into(), Dir::Asc)]
}

fn exists_cond(child_table: &str, subquery_where: Option<Condition>) -> Condition {
    Condition::CorrelatedSubquery(CorrelatedSubqueryCondition {
        related: CorrelatedSubquery {
            correlation: Correlation {
                parent_field: vec!["id".into()],
                child_field: vec!["parentID".into()],
            },
            subquery: Box::new(Ast {
                table: child_table.into(),
                order_by: order_id(),
                r#where: subquery_where,
                ..Default::default()
            }),
            system: None,
        },
        op: ExistsOp::Exists,
        flip: None,
        scalar: None,
        plan_id: None,
    })
}

/// `t0 WHERE EXISTS(t1 WHERE EXISTS(t2 ...))` — `n` nested flippable joins.
fn shape_nested(n: usize) -> Ast {
    let mut inner = None;
    for depth in (1..=n).rev() {
        inner = Some(exists_cond(&format!("t{depth}"), inner));
    }
    Ast {
        table: "t0".into(),
        order_by: order_id(),
        r#where: inner,
        ..Default::default()
    }
}

fn combine(op_and: bool, conds: Vec<Condition>) -> Option<Condition> {
    match conds.len() {
        0 => None,
        1 => conds.into_iter().next(),
        _ => Some(if op_and {
            Condition::And { conditions: conds }
        } else {
            Condition::Or { conditions: conds }
        }),
    }
}

/// `t0 WHERE EXISTS(t1) AND ... AND EXISTS(tn)`.
fn shape_and(n: usize) -> Ast {
    let conds = (1..=n)
        .map(|i| exists_cond(&format!("t{i}"), None))
        .collect();
    Ast {
        table: "t0".into(),
        order_by: order_id(),
        r#where: combine(true, conds),
        ..Default::default()
    }
}

/// `t0 WHERE EXISTS(t1) OR ... OR EXISTS(tn)` — the FanOut/FanIn (FOFI) path.
fn shape_or(n: usize) -> Ast {
    let conds = (1..=n)
        .map(|i| exists_cond(&format!("t{i}"), None))
        .collect();
    Ast {
        table: "t0".into(),
        order_by: order_id(),
        r#where: combine(false, conds),
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// Driver
// ---------------------------------------------------------------------------

const REPS: u32 = 10;

type ShapeFn = fn(usize) -> Ast;

fn bench_shape(name: &str, build: ShapeFn, conn: &Rc<Connection>) {
    println!("\n## shape: {name}  (real SqliteCostModel)");
    println!(
        "{:>3} | {:>10} {:>10} | {:>12} {:>10} | {:>8}",
        "n", "calls", "ms", "calls(cache)", "ms(cache)", "speedup"
    );
    println!("{}", "-".repeat(70));

    for n in 1..=MAX_N {
        let ast = build(n);

        // --- bare: real prepares, one per estimate, re-run per pattern ---
        let mut calls_bare = 0;
        let t = Instant::now();
        for _ in 0..REPS {
            let m = Rc::new(Counting::new(SqliteCostModel::new(conn.clone())));
            let _ = plan_ast(&ast, m.clone());
            calls_bare = m.calls();
        }
        let ms_bare = t.elapsed().as_secs_f64() * 1000.0 / REPS as f64;

        // --- cached: the shipping CachingCostModel wrapping the same model ---
        let mut calls_cached = 0;
        let t = Instant::now();
        for _ in 0..REPS {
            let m = Rc::new(CachingCostModel::new(Counting::new(SqliteCostModel::new(
                conn.clone(),
            ))));
            let _ = plan_ast(&ast, m.clone());
            calls_cached = m.inner().calls();
        }
        let ms_cached = t.elapsed().as_secs_f64() * 1000.0 / REPS as f64;

        let speedup = ms_bare / ms_cached;
        println!(
            "{n:>3} | {calls_bare:>10} {ms_bare:>10.3} | {calls_cached:>12} {ms_cached:>10.3} | {speedup:>7.1}x"
        );
    }
}

fn main() {
    println!("rindle-planner end-to-end benchmark over real SqliteCostModel");
    println!(
        "  DB: t0={ROOT_ROWS} rows, t1..t{MAX_N}={CHILD_ROWS} rows each, all ANALYZE'd; REPS={REPS}"
    );
    println!("  'calls' = real estimate() invocations (each a prepare + scanstatus read)");

    let conn = Rc::new(setup_db());
    bench_shape("nested EXISTS", shape_nested, &conn);
    bench_shape("flat AND-of-EXISTS", shape_and, &conn);
    bench_shape("flat OR-of-EXISTS (FOFI)", shape_or, &conn);
}
