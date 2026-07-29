#![cfg(feature = "testkit")]

//! Tests for [`FlippedJoin`](rindle::op::FlippedJoin) (`flipped-join.ts`, spec `06`
//! §3.2) — the flipped, child-driven inner join.
//!
//! Wired by hand as `issues ⟕flip comments` → Catch, so the operator is exercised
//! in isolation (the `build_pipeline` flipped-EXISTS lowering is covered by the AST
//! tests at the bottom). Pushes drive the real source so the child port's reentrant
//! parent fetch + the in-flight overlay are exercised.
//!
//! **Differential anchor:** a `FlippedJoin`'s inner-join drop *is* the
//! `where EXISTS(rel)` gate, so for the shared scenarios these expectations are
//! identical to `tests/exists_ops.rs` (which builds the non-flipped `Join` +
//! `Exists` pipeline). Same input + same output = the two lowerings agree.

use rindle::change::{OutEdge, Port, SourceChange};
use rindle::graph::{Graph, NodeId};
use rindle::testkit::{
    add, child, cleaf, cnode, edit, rel, remove, run_fetch_test, run_fetch_test_ast, run_push_test,
    run_push_test_ast, CaughtChange, CaughtNode, SourceSpec, TableSpec,
};
use rindle::value::{owned_row as row, OwnedValue as V, RelDef, RelId, Schema, SourceSchema};
use rindle::{
    Ast, Condition, CorrelatedSubquery, CorrelatedSubqueryCondition, Correlation, Dir, ExistsOp,
    OrderPart,
};

// issue(id, x); PK id. The "comments" relationship is slot 0 (RelId(0)).
fn issue_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "x"], vec![0], vec![(0, true)])
}
// comment(id, issueID); PK id; joins to issue on comment.issueID == issue.id.
fn comment_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "issueID"], vec![0], vec![(0, true)])
}
// The hierarchical VIEW schema for `issue { comments }` (slot 0 = "comments"),
// used where a materialized View needs the in-view relationship.
fn issue_view_schema() -> Schema {
    Schema::new(vec!["id", "x"], vec![0], vec![(0, true)]).with_relationships(vec![
        RelDef::related(
            "comments",
            Schema::new(vec!["id", "issueID"], vec![0], vec![(0, true)]),
        ),
    ])
}
fn ir(id: i64, x: i64) -> Vec<V> {
    vec![V::Int(id), V::Int(x)]
}
fn cr(id: i64, issue: i64) -> Vec<V> {
    vec![V::Int(id), V::Int(issue)]
}

/// issues(0) ⟕flip comments(1) on issue.id == comment.issueID, rel "comments".
/// Returns the FlippedJoin (the op the runner sinks into a Catch). The comment
/// connection registers `issueID` (col 1) as a split-edit key — exactly how the
/// builder wires a correlation key — so a child key edit splits into Remove+Add.
fn build_flipped(g: &mut Graph, srcs: &[NodeId]) -> NodeId {
    let pconn = g.connect(srcs[0], Some(vec![(0, true)]), None, vec![]);
    let cconn = g.connect(srcs[1], Some(vec![(0, true)]), None, vec![1]);
    let fj = g.add_flipped_join_slot(pconn, cconn, vec![0], vec![1], RelId(0));
    g.set_conn_output(
        pconn,
        OutEdge {
            node: fj,
            port: Port::JoinParent,
        },
    );
    g.set_conn_output(
        cconn,
        OutEdge {
            node: fj,
            port: Port::JoinChild,
        },
    );
    fj
}

fn sources(issues: Vec<Vec<V>>, comments: Vec<Vec<V>>) -> Vec<SourceSpec> {
    vec![
        SourceSpec::new(issue_schema(), issues),
        SourceSpec::new(comment_schema(), comments),
    ]
}

fn fetch(issues: Vec<Vec<V>>, comments: Vec<Vec<V>>) -> Vec<CaughtNode> {
    run_fetch_test(sources(issues, comments), build_flipped)
}

/// Run one source push (source index 0 = issues, 1 = comments).
fn push(issues: Vec<Vec<V>>, comments: Vec<Vec<V>>, p: (usize, SourceChange)) -> Vec<CaughtChange> {
    run_push_test(sources(issues, comments), build_flipped, vec![p], false).pushes
}

// An issue node with its comments relationship (slot 0).
fn issue_with(id: i64, x: i64, comments: Vec<Vec<V>>) -> CaughtNode {
    cnode(
        ir(id, x),
        vec![(rel(0), comments.into_iter().map(cleaf).collect())],
    )
}

// --- fetch (child-driven inner join) ---------------------------------------

#[test]
fn fetch_keeps_only_parents_with_a_child() {
    // issues 1,2,3; only issue 1 has comments → the inner join keeps just issue 1,
    // carrying its comments. (Identical to exists_ops::fetch_keeps_only_parents.)
    let tree = fetch(
        vec![ir(1, 100), ir(2, 200), ir(3, 300)],
        vec![cr(10, 1), cr(11, 1)],
    );
    assert_eq!(tree, vec![issue_with(1, 100, vec![cr(10, 1), cr(11, 1)])]);
}

#[test]
fn fetch_drops_all_when_no_children() {
    assert!(fetch(vec![ir(1, 100), ir(2, 200)], vec![]).is_empty());
}

#[test]
fn fetch_groups_children_under_distinct_parents() {
    // Two parents, one child each (distinct join keys) → both appear in parent
    // order, each grouped with its own child. Exercises the canonical-key grouping.
    let tree = fetch(vec![ir(1, 100), ir(2, 200)], vec![cr(20, 2), cr(10, 1)]);
    assert_eq!(
        tree,
        vec![
            issue_with(1, 100, vec![cr(10, 1)]),
            issue_with(2, 200, vec![cr(20, 2)]),
        ]
    );
}

#[test]
fn fetch_child_with_null_fk_is_dropped() {
    // A comment whose issueID is null produces no join constraint → no parent.
    let tree = fetch(vec![ir(1, 100)], vec![cr(10, 1), vec![V::Int(11), V::Null]]);
    assert_eq!(tree, vec![issue_with(1, 100, vec![cr(10, 1)])]);
}

#[test]
fn fetch_child_pointing_at_absent_parent_drops() {
    // Comment 10 points at issue 99, which doesn't exist → the batched parent fetch
    // returns nothing → no output (inner join, child-derived parent set).
    assert!(fetch(vec![ir(1, 100)], vec![cr(10, 99)]).is_empty());
}

/// Same wiring as [`build_flipped`] but forces a tiny IN-batch chunk size, so the
/// fetch takes the per-window + k-way-merge path instead of one batched fetch.
fn build_flipped_chunked(g: &mut Graph, srcs: &[NodeId]) -> NodeId {
    let pconn = g.connect(srcs[0], Some(vec![(0, true)]), None, vec![]);
    let cconn = g.connect(srcs[1], Some(vec![(0, true)]), None, vec![1]);
    let fj = g.add_flipped_join_slot_with_chunk_size(pconn, cconn, vec![0], vec![1], RelId(0), 2);
    g.set_conn_output(
        pconn,
        OutEdge {
            node: fj,
            port: Port::JoinParent,
        },
    );
    g.set_conn_output(
        cconn,
        OutEdge {
            node: fj,
            port: Port::JoinChild,
        },
    );
    fj
}

#[test]
fn fetch_chunked_matches_unchunked() {
    // 5 distinct parent keys → with chunk_size 2 the IN-batch is sliced into 3
    // windows, each its own parent fetch, k-way-merged. The result must be identical
    // to the single-batch path (the chunking-equivalence invariant,
    // flipped-join.push.test.ts:4351).
    let issues: Vec<Vec<V>> = (1..=5i64).map(|i| ir(i, i * 100)).collect();
    let comments: Vec<Vec<V>> = (1..=5i64).map(|i| cr(i * 10, i)).collect();
    let expected: Vec<CaughtNode> = (1..=5i64)
        .map(|i| issue_with(i, i * 100, vec![cr(i * 10, i)]))
        .collect();

    let unchunked = run_fetch_test(sources(issues.clone(), comments.clone()), build_flipped);
    let chunked = run_fetch_test(sources(issues, comments), build_flipped_chunked);
    assert_eq!(unchunked, expected);
    assert_eq!(
        chunked, expected,
        "chunked fetch must equal the unchunked fetch"
    );
}

/// Like [`build_flipped_chunked`] but the **parent connection is sorted by a NON-PK
/// column** (col 1, the `x` field) — i.e. the per-query `order_by` differs from the
/// base table's primary sort. This is the case `fetch_chunked_matches_unchunked`
/// could not exercise (it sorts the parent by its PK, so the two orders coincide).
fn build_flipped_chunked_xsort(g: &mut Graph, srcs: &[NodeId]) -> NodeId {
    let pconn = g.connect(srcs[0], Some(vec![(1, true), (0, true)]), None, vec![]);
    let cconn = g.connect(srcs[1], Some(vec![(0, true)]), None, vec![1]);
    let fj = g.add_flipped_join_slot_with_chunk_size(pconn, cconn, vec![0], vec![1], RelId(0), 2);
    g.set_conn_output(
        pconn,
        OutEdge {
            node: fj,
            port: Port::JoinParent,
        },
    );
    g.set_conn_output(
        cconn,
        OutEdge {
            node: fj,
            port: Port::JoinChild,
        },
    );
    fj
}

/// The unchunked counterpart of [`build_flipped_chunked_xsort`] (single-batch parent
/// fetch, same non-PK sort) — the order-faithful ground truth.
fn build_flipped_xsort(g: &mut Graph, srcs: &[NodeId]) -> NodeId {
    let pconn = g.connect(srcs[0], Some(vec![(1, true), (0, true)]), None, vec![]);
    let cconn = g.connect(srcs[1], Some(vec![(0, true)]), None, vec![1]);
    let fj = g.add_flipped_join_slot(pconn, cconn, vec![0], vec![1], RelId(0));
    g.set_conn_output(
        pconn,
        OutEdge {
            node: fj,
            port: Port::JoinParent,
        },
    );
    g.set_conn_output(
        cconn,
        OutEdge {
            node: fj,
            port: Port::JoinChild,
        },
    );
    fj
}

#[test]
fn fetch_chunked_merges_in_resolved_order_not_pk_order() {
    // REGRESSION (chinook flip sweep): the chunked fetch k-way-merges the per-window
    // parent streams. Each window streams in the connection's RESOLVED order (here
    // `order by x`), so the merge must compare on that order — NOT the base table's PK
    // sort. The bug used `input_schema(parent).sort` (PK = col 0) instead of
    // `input_sort(parent)` (col 1), so the merge interleaved the 3 windows by id and a
    // downstream top-N kept the wrong rows. PK-order (id) and x-order DISAGREE here, so
    // a PK-keyed merge produces a visibly wrong order.
    //
    // issues (id, x): x-order is 2,4,5,3,1 — deliberately not the id-order 1..5.
    let issues = vec![ir(1, 50), ir(2, 10), ir(3, 40), ir(4, 20), ir(5, 30)];
    let comments: Vec<Vec<V>> = (1..=5i64).map(|i| cr(i * 10, i)).collect();
    // Expected = the parents in ascending `x`, each with its one comment.
    let expected: Vec<CaughtNode> = [(2, 10), (4, 20), (5, 30), (3, 40), (1, 50)]
        .into_iter()
        .map(|(id, x)| issue_with(id, x, vec![cr(id * 10, id)]))
        .collect();

    let unchunked = run_fetch_test(
        sources(issues.clone(), comments.clone()),
        build_flipped_xsort,
    );
    let chunked = run_fetch_test(sources(issues, comments), build_flipped_chunked_xsort);
    assert_eq!(
        unchunked, expected,
        "unchunked must stream in resolved x-order"
    );
    assert_eq!(
        chunked, expected,
        "chunked fetch must k-way-merge in the resolved x-order, not PK order"
    );
}

// --- push: child port (existence flips) ------------------------------------

#[test]
fn child_add_makes_parent_appear() {
    // issue 1 has no comments (absent from the inner join); adding its first comment
    // makes it appear, carrying the new comment. (= exists_ops::child_add_makes_parent_appear)
    assert_eq!(
        push(
            vec![ir(1, 100), ir(2, 200)],
            vec![],
            (1, SourceChange::Add(row(cr(10, 1))))
        ),
        vec![add(issue_with(1, 100, vec![cr(10, 1)]))]
    );
}

#[test]
fn child_add_to_existing_passes_through() {
    // issue 1 already has a comment: not an existence flip → forward a Child change
    // (the relationship changed). (= exists_ops::child_add_to_existing_passes_through)
    assert_eq!(
        push(
            vec![ir(1, 100)],
            vec![cr(10, 1)],
            (1, SourceChange::Add(row(cr(11, 1))))
        ),
        vec![child(ir(1, 100), rel(0), add(cleaf(cr(11, 1))))]
    );
}

#[test]
fn child_remove_makes_parent_disappear() {
    // Removing issue 1's only comment flips it out → Remove(issue 1) carrying the
    // just-removed comment (the sole related child, JS `[change[NODE]]`).
    // (= exists_ops::child_remove_makes_parent_disappear)
    assert_eq!(
        push(
            vec![ir(1, 100)],
            vec![cr(10, 1)],
            (1, SourceChange::Remove(row(cr(10, 1))))
        ),
        vec![remove(issue_with(1, 100, vec![cr(10, 1)]))]
    );
}

#[test]
fn child_remove_with_remainder_passes_through() {
    // issue 1 keeps a comment after the remove: not a flip → forward a Child change.
    // (= exists_ops::child_remove_with_remainder_passes_through)
    assert_eq!(
        push(
            vec![ir(1, 100)],
            vec![cr(10, 1), cr(11, 1)],
            (1, SourceChange::Remove(row(cr(10, 1)))),
        ),
        vec![child(ir(1, 100), rel(0), remove(cleaf(cr(10, 1))))]
    );
}

#[test]
fn child_add_pointing_at_absent_parent_no_output() {
    // A new comment for a nonexistent issue 99 → reentrant parent fetch empty → no
    // output (inner join: no parent to attach to).
    assert_eq!(
        push(
            vec![ir(1, 100)],
            vec![],
            (1, SourceChange::Add(row(cr(10, 99))))
        ),
        vec![]
    );
}

#[test]
fn child_key_edit_splits_into_remove_then_add() {
    // Editing a comment's issueID (the join/correlation key) is split by the source
    // into Remove(old)+Add(new). The flipped join sees the comment leave issue 1
    // (its sole child → issue 1 disappears) and arrive at issue 2 (its first child →
    // issue 2 appears).
    let pushes = push(
        vec![ir(1, 100), ir(2, 200)],
        vec![cr(10, 1)],
        (
            1,
            SourceChange::Edit {
                row: row(cr(10, 2)),
                old: row(cr(10, 1)),
            },
        ),
    );
    assert_eq!(
        pushes,
        vec![
            remove(issue_with(1, 100, vec![cr(10, 1)])),
            add(issue_with(2, 200, vec![cr(10, 2)])),
        ]
    );
}

// --- push: parent port (inner-join drop) -----------------------------------

#[test]
fn parent_add_without_children_is_dropped() {
    // A newly-added issue with no comments isn't in the inner join → dropped.
    // (= exists_ops::parent_add_without_children_is_dropped)
    assert_eq!(
        push(
            vec![ir(1, 100)],
            vec![cr(10, 1)],
            (0, SourceChange::Add(row(ir(2, 200))))
        ),
        vec![]
    );
}

#[test]
fn parent_add_with_children_appears() {
    // Adding an issue that already has comments in the source → it enters the inner
    // join, carrying its comments.
    assert_eq!(
        push(
            vec![],
            vec![cr(10, 1)],
            (0, SourceChange::Add(row(ir(1, 100))))
        ),
        vec![add(issue_with(1, 100, vec![cr(10, 1)]))]
    );
}

#[test]
fn parent_remove_with_children_forwards() {
    // Removing an issue that has comments → forward the Remove with its relationship.
    // (= exists_ops::parent_remove_with_children_forwards)
    assert_eq!(
        push(
            vec![ir(1, 100)],
            vec![cr(10, 1)],
            (0, SourceChange::Remove(row(ir(1, 100))))
        ),
        vec![remove(issue_with(1, 100, vec![cr(10, 1)]))]
    );
}

#[test]
fn parent_remove_without_children_is_dropped() {
    // The issue was never in the inner join (no comments) → removing it is a no-op.
    assert_eq!(
        push(
            vec![ir(1, 100)],
            vec![],
            (0, SourceChange::Remove(row(ir(1, 100))))
        ),
        vec![]
    );
}

#[test]
fn parent_nonkey_edit_with_children_forwards() {
    // Edit issue 1's non-key `x`; the join key (id) is unchanged and issue 1 has a
    // comment → forward the Edit (Catch records only the two rows for an Edit).
    assert_eq!(
        push(
            vec![ir(1, 100)],
            vec![cr(10, 1)],
            (
                0,
                SourceChange::Edit {
                    row: row(ir(1, 101)),
                    old: row(ir(1, 100))
                }
            ),
        ),
        vec![edit(ir(1, 100), ir(1, 101))]
    );
}

// --- View materialization (the inner join end to end) ----------------------

#[test]
fn view_materializes_inner_join_and_tracks_flips() {
    // issues 1,2,3; comments under 1 and 2 → the view shows only issues 1 and 2.
    let mut g = Graph::new();
    let issues_src = g.add_source(
        issue_schema(),
        vec![row(ir(1, 100)), row(ir(2, 200)), row(ir(3, 300))],
    );
    let comments_src = g.add_source(comment_schema(), vec![row(cr(10, 1)), row(cr(20, 2))]);
    let pconn = g.connect(issues_src, Some(vec![(0, true)]), None, vec![]);
    let cconn = g.connect(comments_src, Some(vec![(0, true)]), None, vec![1]);
    let fj = g.add_flipped_join_slot(pconn, cconn, vec![0], vec![1], RelId(0));
    let view = g.add_view(fj, issue_view_schema());
    g.set_conn_output(
        pconn,
        OutEdge {
            node: fj,
            port: Port::JoinParent,
        },
    );
    g.set_conn_output(
        cconn,
        OutEdge {
            node: fj,
            port: Port::JoinChild,
        },
    );
    g.set_output(fj, view);

    g.hydrate(view);
    assert_eq!(g.dump_view(view), vec![(1, vec![10]), (2, vec![20])]);

    // Add a comment for issue 3 → issue 3 appears (existence flip).
    g.source_push(comments_src, SourceChange::Add(row(cr(30, 3))));
    assert_eq!(
        g.dump_view(view),
        vec![(1, vec![10]), (2, vec![20]), (3, vec![30])]
    );

    // Remove issue 1's only comment → issue 1 disappears.
    g.source_push(comments_src, SourceChange::Remove(row(cr(10, 1))));
    assert_eq!(g.dump_view(view), vec![(2, vec![20]), (3, vec![30])]);

    // A second comment for issue 2 rides along (not a flip).
    g.source_push(comments_src, SourceChange::Add(row(cr(21, 2))));
    assert_eq!(g.dump_view(view), vec![(2, vec![20, 21]), (3, vec![30])]);
}

// --- end-to-end through build_pipeline (flipped EXISTS lowering) -----------

/// `issue WHERE EXISTS(comments)` with `flip: true` — lowered to a bare
/// `FlippedJoin` (the inner-join drop is the gate).
fn flipped_exists_ast() -> Ast {
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
                    order_by: vec![OrderPart("id".into(), Dir::Asc)],
                    ..Default::default()
                }),
                system: None,
            },
            op: ExistsOp::Exists,
            flip: Some(true),
            scalar: None,
            plan_id: None,
        })),
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
fn ast_flipped_exists_fetch_keeps_parents_with_a_child() {
    // Same result as the non-flipped EXISTS lowering (exists_ops): only issue 1 has
    // comments, and they ride along on the kept node.
    let tree = run_fetch_test_ast(
        ast_sources(
            vec![ir(1, 100), ir(2, 200), ir(3, 300)],
            vec![cr(10, 1), cr(11, 1)],
        ),
        &flipped_exists_ast(),
    )
    .unwrap();
    assert_eq!(tree, vec![issue_with(1, 100, vec![cr(10, 1), cr(11, 1)])]);
}

#[test]
fn ast_flipped_exists_push_child_add_makes_parent_appear() {
    // No issue has comments at hydrate; adding issue 1's first comment flips it in
    // through the full lowered FlippedJoin pipeline → Add(issue 1).
    let res = run_push_test_ast(
        ast_sources(vec![ir(1, 100), ir(2, 200)], vec![]),
        &flipped_exists_ast(),
        vec![("comment", SourceChange::Add(row(cr(10, 1))))],
        false,
    )
    .unwrap();
    assert!(res.initial.is_empty());
    assert_eq!(res.pushes, vec![add(issue_with(1, 100, vec![cr(10, 1)]))]);
}
