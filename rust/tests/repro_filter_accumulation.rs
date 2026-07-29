//! REPRO + regression guard for OR-branch constraint accumulation.
//!
//! An `OR` branch whose only constraint is an equality (e.g. on the primary key)
//! used to perform a FULL TABLE SCAN of the shared source connection, because the
//! leaf equality was applied as a filter-chain predicate rather than pushed into the
//! source `fetch` as a constraint. A `CountingSource` instruments how many rows the
//! `issue` source vends, so the tests below pin the seek-vs-scan behavior.
//!
//! The load-bearing case is `issue WHERE (id = 5 OR EXISTS_flipped(comments))`: the
//! flipped branch is child-driven (never scans the parent), so before the fix the
//! whole scan was wasted work on the `id = 5` branch. After the fix that branch seeks.
//!
//! Run: `cargo test --features testkit --test repro_filter_accumulation -- --nocapture`

use std::cell::Cell;
use std::rc::Rc;

use rindle::change::{FetchRequest, OutEdge, RowFlow, SourceChange};
use rindle::error::RindleError;
use rindle::graph::{ConnId, Graph, Source};
use rindle::source_common::{Connection, ConnectionFilters};
use rindle::value::{owned_row as row, ColId, OwnedValue as V, Sort, SourceSchema};
use rindle::{build_pipeline, MemorySource};
use rindle::{
    Ast, Condition, CorrelatedSubquery, CorrelatedSubqueryCondition, Correlation, Dir, ExistsOp,
    Lit, Op, OrderPart, SimpleCondition, ValuePosition,
};

/// A `Source` wrapper that counts the rows its `fetch` actually vends.
struct CountingSource {
    inner: MemorySource,
    rows_scanned: Rc<Cell<usize>>,
    fetches: Rc<Cell<usize>>,
}

impl CountingSource {
    fn new(inner: MemorySource) -> (CountingSource, Rc<Cell<usize>>, Rc<Cell<usize>>) {
        let rows_scanned = Rc::new(Cell::new(0));
        let fetches = Rc::new(Cell::new(0));
        (
            CountingSource {
                inner,
                rows_scanned: rows_scanned.clone(),
                fetches: fetches.clone(),
            },
            rows_scanned,
            fetches,
        )
    }
}

impl Source for CountingSource {
    fn connect(
        &self,
        sort: Option<Sort>,
        filters: Option<ConnectionFilters>,
        split_edit_keys: Vec<ColId>,
    ) -> ConnId {
        self.inner.connect(sort, filters, split_edit_keys)
    }

    fn fetch<'g>(&'g self, conn: ConnId, req: &FetchRequest) -> RowFlow<'g> {
        self.fetches.set(self.fetches.get() + 1);
        let counter = self.rows_scanned.clone();
        // NB: this counts the rows the connection *vends to the pipeline above it*
        // (post the source's own pushed-down predicate). When the connection carries
        // NO pushed predicate — the OR-collapsed `or(eq, exists)` case this repro
        // targets — that equals the rows pulled from the index, so it is a faithful
        // scan-cost proxy for the eq branch. With a pushed predicate it is an
        // upper-bounded proxy (emits <= index pulls), still small once the seek lands.
        Box::new(self.inner.fetch(conn, req).inspect(move |_r| {
            counter.set(counter.get() + 1);
        }))
    }

    fn conn_sort(&self, conn: ConnId) -> Sort {
        self.inner.conn_sort(conn)
    }
    fn schema(&self) -> &rindle::value::Schema {
        self.inner.schema()
    }
    fn destroy(&self, conn: ConnId) {
        self.inner.destroy(conn)
    }
    fn cursors_open(&self) -> i64 {
        self.inner.cursors_open()
    }
    fn try_push(
        &self,
        change: SourceChange,
        push_one: &dyn Fn(&Connection, SourceChange),
        strict: bool,
    ) -> Result<(), RindleError> {
        self.inner.try_push(change, push_one, strict)
    }
    fn take_error(&self) -> Option<RindleError> {
        self.inner.take_error()
    }
    fn set_conn_output(&self, conn: ConnId, edge: OutEdge) {
        self.inner.set_conn_output(conn, edge)
    }
}

fn issue_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "x"], vec![0], vec![(0, true)])
}
fn comment_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "issueID"], vec![0], vec![(0, true)])
}

fn id_eq(v: i64) -> Condition {
    Condition::Simple(SimpleCondition {
        op: Op::Eq,
        left: ValuePosition::Column { name: "id".into() },
        right: ValuePosition::Literal {
            value: Lit::Number(v as f64),
        },
    })
}

fn exists_comments(flip: bool) -> Condition {
    Condition::CorrelatedSubquery(CorrelatedSubqueryCondition {
        related: CorrelatedSubquery {
            correlation: Correlation {
                parent_field: vec!["id".into()],
                child_field: vec!["issueID".into()],
            },
            subquery: Box::new(Ast {
                table: "comment".into(),
                alias: Some("comments".into()),
                order_by: vec![OrderPart("id".into(), Dir::Asc)],
                ..Default::default()
            }),
            system: None,
        },
        op: ExistsOp::Exists,
        flip: if flip { Some(true) } else { None },
        scalar: None,
        plan_id: None,
    })
}

fn x_eq(v: i64) -> Condition {
    Condition::Simple(SimpleCondition {
        op: Op::Eq,
        left: ValuePosition::Column { name: "x".into() },
        right: ValuePosition::Literal {
            value: Lit::Number(v as f64),
        },
    })
}

fn issue_where(cond: Condition) -> Ast {
    Ast {
        table: "issue".into(),
        order_by: vec![OrderPart("id".into(), Dir::Asc)],
        r#where: Some(cond),
        ..Default::default()
    }
}

/// Build a graph over a CountingSource(issue) + memory(comment), run `build_pipeline`,
/// fetch the top with no constraint, and return (result_rows, rows_scanned, fetches).
fn run(ast: &Ast, n_issues: i64, comments: Vec<Vec<V>>) -> (usize, usize, usize) {
    let mut g = Graph::new();

    let issues: Vec<_> = (1..=n_issues)
        .map(|i| row(vec![V::Int(i), V::Int(i)]))
        .collect();
    let (counting, rows_scanned, fetches) =
        CountingSource::new(MemorySource::new(issue_schema().into_schema(), issues));
    let issue_node = g.add_dyn_source(Box::new(counting));

    let comment_rows: Vec<_> = comments.into_iter().map(row).collect();
    let comment_node = g.add_source(comment_schema(), comment_rows);

    let resolve = |table: &str| -> Option<(rindle::NodeId, SourceSchema)> {
        match table {
            "issue" => Some((issue_node, issue_schema())),
            "comment" => Some((comment_node, comment_schema())),
            _ => None,
        }
    };

    let top = build_pipeline(&mut g, ast, &resolve).expect("build");
    let result: Vec<_> = g.fetch(top, &FetchRequest::all()).collect();
    (result.len(), rows_scanned.get(), fetches.get())
}

#[test]
fn baseline_pure_eq_pk_vends_one_row() {
    // `issue WHERE id = 5` over 100 issues: a pure leaf, pushed to the connection as
    // its `predicate`. (This engine does NOT hoist a `pk_constraint`, so the source
    // actually full-scans the index and the predicate filters to 1 — the connection
    // vends 1 row. Sanity check that the harness wiring is correct.)
    let (results, scanned, fetches) = run(&issue_where(id_eq(5)), 100, vec![]);
    println!(
        "[baseline id=5]            results={results} issue_rows_vended={scanned} issue_fetches={fetches}"
    );
    assert_eq!(results, 1, "only issue 5 matches");
}

#[test]
fn or_eq_pk_flipped_exists_full_scans_the_eq_branch() {
    // `issue WHERE (id = 5 OR EXISTS_flipped(comments))` over 100 issues, no
    // comments at all. Result is just issue 5. But the `id = 5` branch fetches the
    // SHARED issue connection with NO constraint → FULL SCAN of all 100 issues.
    let (results, scanned, fetches) = run(
        &issue_where(Condition::Or {
            conditions: vec![id_eq(5), exists_comments(true)],
        }),
        100,
        vec![],
    );
    println!(
        "[or id=5 / flipped exists] results={results} issue_rows_vended={scanned} issue_fetches={fetches}"
    );
    assert_eq!(results, 1, "only issue 5 matches (no comments exist)");
    // REGRESSION GUARD: with OR-branch constraint accumulation, the eq(pk) branch
    // now SEEKS the shared connection instead of full-scanning it. Before the fix
    // this scanned all 100 rows.
    assert!(
        scanned <= 2,
        "eq(pk) OR branch should SEEK the shared connection, not full-scan; scanned={scanned}"
    );
    let _ = fetches;
}

#[test]
fn or_eq_pk_nonflipped_exists_scan() {
    // `issue WHERE (id = 5 OR EXISTS(comments))` (non-flipped): the filter-fan
    // path. One shared FilterStart scan (inherently full — EXISTS is unbounded).
    let (results, scanned, fetches) = run(
        &issue_where(Condition::Or {
            conditions: vec![id_eq(5), exists_comments(false)],
        }),
        100,
        vec![],
    );
    println!(
        "[or id=5 / nonflipped ex ] results={results} issue_rows_vended={scanned} issue_fetches={fetches}"
    );
    assert_eq!(results, 1, "only issue 5 matches (no comments exist)");
}

// --- interaction with a filter ALREADY present on the shared connection ----
// `and(eq(x, V), or(eq(id, 5), flipped_exists))`: the flip-free `eq(x, V)` IS pushed
// to the connection as its `predicate` (the OR collapses). The eq branch's injected
// `{id:5}` constraint drives the index SEEK; the connection's `eq(x, V)` predicate
// then still filters the sought row. The two compose by AND — the seek changes WHICH
// rows are read, the predicate still decides which pass. (Rows are x == id here.)

fn and_x_eq_or_id5_flipped(x: i64) -> Ast {
    issue_where(Condition::And {
        conditions: vec![
            x_eq(x),
            Condition::Or {
                conditions: vec![id_eq(5), exists_comments(true)],
            },
        ],
    })
}

#[test]
fn connection_predicate_composes_with_the_seek_keeping_the_row() {
    // x == 5 selects exactly row 5 (x == id), which also satisfies id = 5 → kept.
    let (results, scanned, fetches) = run(&and_x_eq_or_id5_flipped(5), 100, vec![]);
    println!(
        "[and x=5 / (id=5 | flip) ] results={results} issue_rows_vended={scanned} issue_fetches={fetches}"
    );
    assert_eq!(results, 1, "row 5 satisfies both eq(x,5) and eq(id,5)");
    assert!(scanned <= 2, "eq branch still seeks; vended={scanned}");
}

#[test]
fn connection_predicate_filters_out_the_sought_row() {
    // x == 999 matches NO row, so even the sought id = 5 row is dropped by the
    // connection predicate → empty. Proves the existing predicate still applies
    // after the seek (the constraint never bypasses it).
    let (results, scanned, fetches) = run(&and_x_eq_or_id5_flipped(999), 100, vec![]);
    println!(
        "[and x=999 / (id=5 | flip)] results={results} issue_rows_vended={scanned} issue_fetches={fetches}"
    );
    assert_eq!(
        results, 0,
        "no row has x = 999, so the predicate drops the sought row"
    );
}
