#![cfg(feature = "testkit")]

//! **Dataflow laziness on hydration** — the graph fills a `LIMIT` window by
//! short-circuiting the leaf cursor, *not* by draining the whole source.
//!
//! `Take::initial_fetch` (the hydration path, `src/op/take.rs`) pulls from the lazy
//! `g.fetch(input, …)` stream and `break`s the moment the window reaches `limit`.
//! Because every source's `fetch` returns a *lazy* [`RowFlow`], that break abandons
//! the underlying cursor — a SQLite statement or a memory-source B+tree cursor — so a
//! `LIMIT 5` hydrate over a 1000-row table pulls ~5 rows, never 1000. The
//! source-level `tests/source_common.rs` suite proves the overlay/constraint chain is
//! pull-driven; this test proves it end-to-end through `build_pipeline` + the `Take`
//! operator, **over both backends** via the shared `*_with_backend` harness.
//!
//! A [`CountingSource`](rindle_sqlite::testkit::CountingSource) decorator counts the
//! rows each leaf actually vends, so the same assertion runs over the in-memory
//! `MemorySource` and the real SQLite `TableSource`. Run with `--features testkit`.

use std::cell::Cell;
use std::rc::Rc;

use rindle::testkit::{run_fetch_test_ast_with_backend, SourceBackend, TableSpec};
use rindle::value::{OwnedValue as V, SourceSchema};
use rindle::{
    Ast, Condition, CorrelatedSubquery, CorrelatedSubqueryCondition, Correlation, Dir, ExistsOp,
    OrderPart,
};
use rindle_sqlite::testkit::{
    counting_memory_backend, counting_memory_backend_for, counting_sqlite_backend,
    counting_sqlite_backend_for, PullCounter,
};

/// Source rows — far larger than the window, so "drained the table" and "filled the
/// window" differ by two orders of magnitude.
const N: i64 = 1000;
/// The top-level `LIMIT`.
const LIMIT: u32 = 5;

/// One `issue` table of `N` rows keyed (and sorted) by `id`.
fn big_issue_specs() -> Vec<TableSpec> {
    let schema = SourceSchema::new(vec!["id"], vec![0], vec![(0, true)]);
    let rows: Vec<Vec<V>> = (0..N).map(|i| vec![V::Int(i)]).collect();
    vec![TableSpec::new("issue", schema, rows)]
}

/// `issue (limit LIMIT) order by id` — a top-level limit, lowered to an unpartitioned
/// `Take` over the source connection.
fn limit_ast() -> Ast {
    Ast {
        table: "issue".into(),
        order_by: vec![OrderPart("id".into(), Dir::Asc)],
        limit: Some(LIMIT),
        ..Default::default()
    }
}

/// Hydrate the limited query over `backend_of(counter)` and assert (a) the window is
/// correct and (b) the leaf was pulled ~`LIMIT` times, not drained.
fn assert_lazy_hydrate(backend_of: impl Fn(PullCounter) -> SourceBackend, label: &str) {
    let pulled: PullCounter = Rc::new(Cell::new(0));
    let tree = run_fetch_test_ast_with_backend(
        big_issue_specs(),
        backend_of(pulled.clone()),
        &limit_ast(),
    )
    .expect("hydrate the limited query");

    // Correctness: the window is exactly the first LIMIT rows by id.
    let ids: Vec<i64> = tree
        .iter()
        .map(|n| match &n.row.col(0).to_owned() {
            V::Int(i) => *i,
            other => panic!("{label}: expected Int id, got {other:?}"),
        })
        .collect();
    assert_eq!(
        ids,
        (0..LIMIT as i64).collect::<Vec<_>>(),
        "{label}: window"
    );

    // Laziness: filling the window short-circuits the leaf cursor. `initial_fetch`
    // breaks the instant `size == limit`, so it pulls exactly LIMIT rows; allow a
    // tiny constant of slack for any one-row look-ahead, but NOT the whole table.
    assert!(
        pulled.get() <= LIMIT as usize + 2,
        "{label}: lazy hydrate should pull ~{LIMIT} of {N} rows, but pulled {} \
         (the graph drained the cursor instead of stopping at the limit)",
        pulled.get()
    );
}

#[test]
fn memory_hydrate_stays_lazy_under_limit() {
    assert_lazy_hydrate(counting_memory_backend, "memory");
}

#[test]
fn sqlite_hydrate_stays_lazy_under_limit() {
    assert_lazy_hydrate(counting_sqlite_backend, "sqlite");
}

// =========================================================================
// Partitioned (subquery) + EXISTS: the per-partition limiter is lazy too.
// A parent with PARENTS rows, each owning `children_per` children. The child
// leaf must be pulled only a small CONSTANT per partition (bounded by the
// limiter), *independent of the group size* — never the whole group. We prove
// that directly: hydrate over a small AND a large group and assert the child
// pull count is identical (a draining graph would scale with the group). We
// count ONLY the child ("comment") leaf to isolate it from the parent.
// =========================================================================

/// Parents (fixed); the children-per-parent count is varied per run.
const PARENTS: i64 = 3;
/// The limited-relationship `LIMIT`.
const SUB_LIMIT: u32 = 2;

/// `issue` (PARENTS rows) + `comment` (PARENTS × `children_per` rows), each comment
/// correlated to its issue by `issueID` and ordered within a partition by `created`.
fn parent_child_specs(children_per: i64) -> Vec<TableSpec> {
    let issue_s = SourceSchema::new(vec!["id"], vec![0], vec![(0, true)]);
    let comment_s = SourceSchema::new(
        vec!["id", "issueID", "created"],
        vec![0],
        vec![(2, true), (0, true)],
    );
    let issues: Vec<Vec<V>> = (1..=PARENTS).map(|i| vec![V::Int(i)]).collect();
    let mut comments: Vec<Vec<V>> = Vec::new();
    let mut cid = 0i64;
    for issue in 1..=PARENTS {
        for created in 0..children_per {
            comments.push(vec![V::Int(cid), V::Int(issue), V::Int(created)]);
            cid += 1;
        }
    }
    vec![
        TableSpec::new("issue", issue_s, issues),
        TableSpec::new("comment", comment_s, comments),
    ]
}

/// `issue { comments (limit SUB_LIMIT) }` — a limited relationship, lowered to a
/// `Take` PARTITIONED by `issueID`.
fn subquery_ast() -> Ast {
    Ast {
        table: "issue".into(),
        order_by: vec![OrderPart("id".into(), Dir::Asc)],
        related: vec![CorrelatedSubquery {
            correlation: Correlation {
                parent_field: vec!["id".into()],
                child_field: vec!["issueID".into()],
            },
            subquery: Box::new(Ast {
                table: "comment".into(),
                alias: Some("comments".into()),
                order_by: vec![OrderPart("created".into(), Dir::Asc)],
                limit: Some(SUB_LIMIT),
                ..Default::default()
            }),
            system: None,
        }],
        ..Default::default()
    }
}

/// `issue WHERE EXISTS(comments)` — lowered to `Cap(EXISTS_LIMIT) + Exists`, so each
/// parent's child probe is bounded to EXISTS_LIMIT (3).
fn exists_ast() -> Ast {
    Ast {
        table: "issue".into(),
        order_by: vec![OrderPart("id".into(), Dir::Asc)],
        r#where: Some(Condition::CorrelatedSubquery(CorrelatedSubqueryCondition {
            related: CorrelatedSubquery {
                correlation: Correlation {
                    parent_field: vec!["id".into()],
                    child_field: vec!["issueID".into()],
                },
                subquery: Box::new(Ast {
                    table: "comment".into(),
                    alias: Some("comments".into()),
                    order_by: vec![OrderPart("created".into(), Dir::Asc)],
                    ..Default::default()
                }),
                system: None,
            },
            op: ExistsOp::Exists,
            flip: None,
            scalar: None,
            plan_id: None,
        })),
        ..Default::default()
    }
}

/// Hydrate `ast` over `child_backend(counter)` (counting only the `comment` leaf) with
/// `children_per` children per partition; return the child pull count (asserting the
/// query did real work — all PARENTS survive).
fn child_pulls(
    child_backend: &impl Fn(PullCounter) -> SourceBackend,
    ast: &Ast,
    children_per: i64,
    label: &str,
) -> usize {
    let pulled: PullCounter = Rc::new(Cell::new(0));
    let tree = run_fetch_test_ast_with_backend(
        parent_child_specs(children_per),
        child_backend(pulled.clone()),
        ast,
    )
    .expect("hydrate the partitioned query");
    assert_eq!(tree.len(), PARENTS as usize, "{label}: parent rows");
    pulled.get()
}

/// The shared body: the per-partition limiter pulls a CONSTANT number of child rows,
/// independent of how many children each partition holds. Hydrate over a small group
/// (`SMALL`) and a 10× group (`BIG`) and assert the pull count is unchanged — a graph
/// that drained each partition would pull ~10× more on the big group.
fn assert_partition_pulls_are_group_independent(
    child_backend: impl Fn(PullCounter) -> SourceBackend,
    ast: &Ast,
    label: &str,
) {
    const SMALL: i64 = 50;
    const BIG: i64 = 500;
    let small = child_pulls(&child_backend, ast, SMALL, label);
    let big = child_pulls(&child_backend, ast, BIG, label);
    assert_eq!(
        small, big,
        "{label}: child pulls must be bounded by the limiter, not the group size — \
         pulled {small} over {SMALL}/partition but {big} over {BIG}/partition \
         (the graph is draining each partition's group instead of stopping at the limit)"
    );
    // Sanity: the constant really is small (≤ the parent count × a tiny per-partition
    // bound), so equality above isn't a degenerate "both drained identically".
    assert!(
        small <= PARENTS as usize * 10,
        "{label}: expected a small per-partition constant, pulled {small}"
    );
}

#[test]
fn memory_partitioned_subquery_hydrate_is_lazy() {
    assert_partition_pulls_are_group_independent(
        |c| counting_memory_backend_for(Some("comment"), c),
        &subquery_ast(),
        "memory/subquery",
    );
}

#[test]
fn sqlite_partitioned_subquery_hydrate_is_lazy() {
    assert_partition_pulls_are_group_independent(
        |c| counting_sqlite_backend_for(Some("comment"), c),
        &subquery_ast(),
        "sqlite/subquery",
    );
}

#[test]
fn memory_exists_hydrate_is_lazy() {
    assert_partition_pulls_are_group_independent(
        |c| counting_memory_backend_for(Some("comment"), c),
        &exists_ast(),
        "memory/exists",
    );
}

#[test]
fn sqlite_exists_hydrate_is_lazy() {
    assert_partition_pulls_are_group_independent(
        |c| counting_sqlite_backend_for(Some("comment"), c),
        &exists_ast(),
        "sqlite/exists",
    );
}
