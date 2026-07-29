#![cfg(feature = "testkit")]

//! Tests for the node-level union OR fan ([`UnionFanOut`](rindle::op::UnionFanOut) /
//! [`UnionFanIn`](rindle::op::UnionFanIn) + `push_accumulated_changes`,
//! `union-fan-{out,in}.ts`, spec `06` §3.6).
//!
//! Hand-wired as `source → UnionFanOut → [branch A, branch B] → UnionFanIn` with two
//! **pass-through** filter branches (`FilterStart → FilterEnd`, empty chain). Both
//! branches keep every row, so the union exercises its two jobs in isolation:
//!   * **fetch** — k-way-merge the branch fetches and **dedup** the identical rows;
//!   * **push** — broadcast the change to both branches, **accumulate** their (owned)
//!     outputs, and **collapse** the cross-branch duplicates to a single change.
//!
//! The builder's OR-with-flipped-subquery lowering (the real driver of this fan) is
//! covered end-to-end by the AST tests once it lands.

use rindle::change::{OutEdge, Port, SourceChange};
use rindle::graph::{Graph, NodeId};
use rindle::testkit::{
    add, cleaf, cnode, edit, rel, remove, run_fetch_test, run_fetch_test_ast, run_push_test,
    run_push_test_ast, CaughtChange, CaughtNode, SourceSpec, TableSpec,
};
use rindle::value::{owned_row as row, OwnedValue as V, Schema, SourceSchema};
use rindle::{
    Ast, Condition, CorrelatedSubquery, CorrelatedSubqueryCondition, Correlation, Dir, ExistsOp,
    Lit, Op, OrderPart, SimpleCondition, ValuePosition,
};

// The pipeline `Schema` for the UnionFanIn (a view/pipeline-shape consumer).
fn item_schema() -> Schema {
    Schema::new(vec!["id"], vec![0], vec![(0, true)])
}
// The same table as a SOURCE schema (fed to `SourceSpec::new`).
fn item_source_schema() -> SourceSchema {
    SourceSchema::new(vec!["id"], vec![0], vec![(0, true)])
}
fn it(id: i64) -> Vec<V> {
    vec![V::Int(id)]
}

/// `source → UnionFanOut → [pass-through A, pass-through B] → UnionFanIn`.
fn build_union(g: &mut Graph, srcs: &[NodeId]) -> NodeId {
    let conn = g.connect(srcs[0], Some(vec![(0, true)]), None, vec![]);
    let ufo = g.add_union_fan_out(conn);

    // Two empty filter chains over the fan-out (each passes every row through).
    let fs_a = g.add_filter_start(ufo);
    let fe_a = g.add_filter_end(fs_a);
    g.set_chain_head(fs_a, fe_a);
    let fs_b = g.add_filter_start(ufo);
    let fe_b = g.add_filter_end(fs_b);
    g.set_chain_head(fs_b, fe_b);

    let ufi = g.add_union_fan_in(ufo, vec![fe_a, fe_b], vec![vec![], vec![]], item_schema());
    g.set_output(fe_a, ufi);
    g.set_output(fe_b, ufi);

    g.set_conn_output(
        conn,
        OutEdge {
            node: ufo,
            port: Port::Single,
        },
    );
    g.set_union_fan(
        ufo,
        vec![
            OutEdge {
                node: fs_a,
                port: Port::Single,
            },
            OutEdge {
                node: fs_b,
                port: Port::Single,
            },
        ],
        ufi,
    );
    ufi
}

fn sources(items: Vec<Vec<V>>) -> Vec<SourceSpec> {
    vec![SourceSpec::new(item_source_schema(), items)]
}

fn push(items: Vec<Vec<V>>, p: (usize, SourceChange)) -> Vec<CaughtChange> {
    run_push_test(sources(items), build_union, vec![p], false).pushes
}

#[test]
fn fetch_merges_and_dedups_branches() {
    // Both branches yield {1,2,3}; the merge dedups to one of each (not doubled).
    let tree: Vec<CaughtNode> = run_fetch_test(sources(vec![it(1), it(2), it(3)]), build_union);
    assert_eq!(tree, vec![cleaf(it(1)), cleaf(it(2)), cleaf(it(3))]);
}

#[test]
fn push_add_collapses_duplicate_branch_forwards() {
    // Both branches forward Add(2); the fan-in collapses them to a single Add.
    assert_eq!(
        push(vec![it(1)], (0, SourceChange::Add(row(it(2))))),
        vec![add(cleaf(it(2)))]
    );
}

#[test]
fn push_remove_collapses() {
    assert_eq!(
        push(vec![it(1), it(2)], (0, SourceChange::Remove(row(it(2))))),
        vec![remove(cleaf(it(2)))]
    );
}

#[test]
fn push_edit_collapses() {
    // Both branches preserve the Edit (no predicate splits it) → one Edit out.
    assert_eq!(
        push(
            vec![it(1)],
            (
                0,
                SourceChange::Edit {
                    row: row(it(1)),
                    old: row(it(1))
                }
            ),
        ),
        vec![edit(it(1), it(1))]
    );
}

// --- end-to-end through build_pipeline: `where (x = 1 OR EXISTS_flipped(comments))` --

// issue(id, x); PK id. The normalized `comments_0` slot is query-derived. comment(id, issueID).
fn issue_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "x"], vec![0], vec![(0, true)])
}
fn comment_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "issueID"], vec![0], vec![(0, true)])
}
fn ir(id: i64, x: i64) -> Vec<V> {
    vec![V::Int(id), V::Int(x)]
}
fn cr(id: i64, issue: i64) -> Vec<V> {
    vec![V::Int(id), V::Int(issue)]
}
fn issue_with(id: i64, x: i64, comments: Vec<Vec<V>>) -> CaughtNode {
    cnode(
        ir(id, x),
        vec![(rel(0), comments.into_iter().map(cleaf).collect())],
    )
}

/// `issue WHERE (x = 1 OR EXISTS flipped(comments))` — a top-level OR with one simple
/// leaf branch and one flipped EXISTS branch → the union fan.
fn or_union_ast() -> Ast {
    Ast {
        table: "issue".into(),
        order_by: vec![OrderPart("id".into(), Dir::Asc)],
        r#where: Some(Condition::Or {
            conditions: vec![
                Condition::Simple(SimpleCondition {
                    op: Op::Eq,
                    left: ValuePosition::Column { name: "x".into() },
                    right: ValuePosition::Literal {
                        value: Lit::Number(1.0),
                    },
                }),
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
                    flip: Some(true),
                    scalar: None,
                    plan_id: None,
                }),
            ],
        }),
        ..Default::default()
    }
}

fn ast_sources(issues: Vec<Vec<V>>, comments: Vec<Vec<V>>) -> Vec<TableSpec> {
    vec![
        TableSpec::new("issue", issue_schema(), issues),
        TableSpec::new("comment", comment_schema(), comments),
    ]
}

#[test]
fn ast_or_union_fetch_merges_simple_and_flipped() {
    // issue 1 (x=1) matches the simple branch (no comments rel attached); issue 3 has
    // a comment, matching the flipped branch (comments ride along). issue 2 (x=5, no
    // comments) matches neither → dropped. The merge orders + dedups by id.
    let tree = run_fetch_test_ast(
        ast_sources(vec![ir(1, 1), ir(2, 5), ir(3, 5)], vec![cr(10, 3)]),
        &or_union_ast(),
    )
    .unwrap();
    assert_eq!(
        tree,
        vec![cleaf(ir(1, 1)), issue_with(3, 5, vec![cr(10, 3)])]
    );
}

#[test]
fn ast_or_union_push_simple_only_branch() {
    // Add issue 4 (x=1, no comments): only the simple branch keeps it → one Add, with
    // the comments relationship empty-filled by add_empty_relationships.
    let res = run_push_test_ast(
        ast_sources(vec![ir(1, 1)], vec![]),
        &or_union_ast(),
        vec![("issue", SourceChange::Add(row(ir(4, 1))))],
        false,
    )
    .unwrap();
    assert_eq!(res.pushes, vec![add(issue_with(4, 1, vec![]))]);
}

#[test]
fn ast_or_union_push_flipped_only_branch() {
    // Add issue 5 (x=5) that has a pre-existing comment: only the flipped branch keeps
    // it (the inner-join finds comment 50) → one Add carrying the comment.
    let res = run_push_test_ast(
        ast_sources(vec![ir(1, 1)], vec![cr(50, 5)]),
        &or_union_ast(),
        vec![("issue", SourceChange::Add(row(ir(5, 5))))],
        false,
    )
    .unwrap();
    assert_eq!(res.pushes, vec![add(issue_with(5, 5, vec![cr(50, 5)]))]);
}

/// `or_union_ast` but ordered by the non-PK `x` (DESC) with the simple predicate
/// `x = 5` — the shape that exposed the fan-in-sort bug.
fn or_union_ast_x5_desc() -> Ast {
    let mut ast = or_union_ast();
    ast.order_by = vec![OrderPart("x".into(), Dir::Desc)];
    if let Some(Condition::Or { conditions }) = &mut ast.r#where {
        if let Condition::Simple(sc) = &mut conditions[0] {
            sc.right = ValuePosition::Literal {
                value: Lit::Number(5.0),
            };
        }
    }
    ast
}

#[test]
fn ast_or_union_fetch_respects_non_pk_order_by() {
    // Regression: the fan-in must merge in the connection's RESOLVED `order_by` order
    // (x desc, id asc — PK-completed), not the base PK sort. issue 1 (x=5, comment 10)
    // matches BOTH branches and must dedup to ONE; without the resolved sort the
    // branch streams are mis-merged and issue 1 leaks twice / out of order.
    let tree = run_fetch_test_ast(
        ast_sources(
            vec![ir(1, 5), ir(2, 5), ir(3, 9)],
            vec![cr(10, 1), cr(20, 3)],
        ),
        &or_union_ast_x5_desc(),
    )
    .unwrap();
    let ids: Vec<i64> = tree
        .iter()
        .map(|n| match &n.row.col(0).to_owned() {
            V::Int(i) => *i,
            _ => panic!("expected Int id"),
        })
        .collect();
    // x desc → issue 3 (x=9) first; then x=5 tie broken by id asc → issue 1, issue 2.
    // issue 1 appears once (deduped), not twice.
    assert_eq!(ids, vec![3, 1, 2]);
}

#[test]
fn ast_or_union_push_both_branches_merge_relationships() {
    // Add issue 6 (x=1) that ALSO has a comment: BOTH branches keep it (simple via
    // x=1, flipped via comment 60). The fan-in collapses the two Adds to one,
    // unioning the relationships → Add(issue 6) carrying the comment.
    let res = run_push_test_ast(
        ast_sources(vec![ir(1, 1)], vec![cr(60, 6)]),
        &or_union_ast(),
        vec![("issue", SourceChange::Add(row(ir(6, 1))))],
        false,
    )
    .unwrap();
    assert_eq!(res.pushes, vec![add(issue_with(6, 1, vec![cr(60, 6)]))]);
}

// --- flipped-EXISTS CHILD push (the union fan-in's internal-change dedup, mode 2) ---
//
// A comment push reaches the FlippedJoin's CHILD port directly (NOT via the fan-out
// broadcast), so its emitted parent Add/Remove arrives at the UnionFanIn while the
// fan-out is idle -> `push_internal_change`. These exercise the cross-branch existence
// dedup (forward iff no OTHER branch keeps the row).

#[test]
fn ast_or_union_child_add_flips_a_non_leaf_row_to_add() {
    // issue 2 (x=5) is in NEITHER branch at hydrate. Adding its first comment flips the
    // flipped branch on; the simple branch (x=1) doesn't keep it, so the internal Add
    // forwards (count == 1, only the flipped branch has it).
    let res = run_push_test_ast(
        ast_sources(vec![ir(1, 1), ir(2, 5)], vec![]),
        &or_union_ast(),
        vec![("comment", SourceChange::Add(row(cr(50, 2))))],
        false,
    )
    .unwrap();
    assert_eq!(res.initial, vec![cleaf(ir(1, 1))]);
    assert_eq!(res.pushes, vec![add(issue_with(2, 5, vec![cr(50, 2)]))]);
}

#[test]
fn ast_or_union_child_add_on_a_simple_matched_row_is_suppressed() {
    // issue 1 (x=1) is ALREADY kept by the simple branch. Adding its first comment flips
    // the flipped branch on too, but the simple branch still has the row (count == 2) ->
    // the internal Add is suppressed (no duplicate; the union fan does not surface the
    // EXISTS-relationship update for an already-present row).
    let res = run_push_test_ast(
        ast_sources(vec![ir(1, 1)], vec![]),
        &or_union_ast(),
        vec![("comment", SourceChange::Add(row(cr(51, 1))))],
        false,
    )
    .unwrap();
    assert_eq!(res.initial, vec![cleaf(ir(1, 1))]);
    assert_eq!(res.pushes, Vec::<CaughtChange>::new());
}

#[test]
fn ast_or_union_child_remove_last_comment_on_a_non_leaf_row_to_remove() {
    // issue 2 (x=5) is in ONLY via the flipped branch. Removing its last comment flips
    // it off; no branch keeps it (count == 0) -> forward the Remove (carrying the removed
    // comment, the FlippedJoin's last copy).
    let res = run_push_test_ast(
        ast_sources(vec![ir(1, 1), ir(2, 5)], vec![cr(50, 2)]),
        &or_union_ast(),
        vec![("comment", SourceChange::Remove(row(cr(50, 2))))],
        false,
    )
    .unwrap();
    assert_eq!(
        res.initial,
        vec![cleaf(ir(1, 1)), issue_with(2, 5, vec![cr(50, 2)])]
    );
    assert_eq!(res.pushes, vec![remove(issue_with(2, 5, vec![cr(50, 2)]))]);
}

#[test]
fn ast_or_union_child_remove_on_a_simple_matched_row_is_suppressed() {
    // issue 1 (x=1) is kept by the simple branch AND has a comment (flipped). Removing
    // the comment flips the flipped branch off, but the simple branch still keeps the row
    // (count == 1) -> the internal Remove is suppressed (the row stays via x=1).
    let res = run_push_test_ast(
        ast_sources(vec![ir(1, 1)], vec![cr(51, 1)]),
        &or_union_ast(),
        vec![("comment", SourceChange::Remove(row(cr(51, 1))))],
        false,
    )
    .unwrap();
    assert_eq!(res.pushes, Vec::<CaughtChange>::new());
}
