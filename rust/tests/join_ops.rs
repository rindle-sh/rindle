//! Tests for the finished `Join` push arms (parent edit, child edit) and the
//! relationship-preservation fix in `process_parent_node` (nested joins).
//!
//! These exercise behavior the spike previously left `unimplemented!()` or buggy:
//!   * `parent_edit_*` / `child_edit_*` — the join's Edit arms (`join.ts#pushParent`
//!     / `#pushChild`), which assert the join key is unchanged and forward an Edit.
//!   * `nested_join_preserves_inner_relationship` — `process_parent_node` now
//!     APPENDS this join's relationship to the parent node instead of replacing its
//!     `rels`, so a join stacked on another join keeps the inner relationship
//!     (`join.ts:298` `{...parentNodeRelations, [rel]: childStream}`).

use rindle::change::{OutEdge, Port};
use rindle::graph::{Graph, NodeId};
use rindle::value::{owned_row as row, OwnedValue as V, RelDef, RelId, Schema, SourceSchema};
use rindle::SourceChange;

// issue(id, val); PK id; relationship "comments" (slot 0) and "labels" (slot 1),
// for the nested test. VIEW schema (fed to `add_view`): the production `View`
// materializes both relationships, so each hop carries its (leaf) child schema.
fn issues_schema() -> Schema {
    Schema::new(vec!["id", "val"], vec![0], vec![(0, true)]).with_relationships(vec![
        RelDef::related("comments", comments_schema()),
        RelDef::related("labels", labels_schema()),
    ])
}
// SOURCE schema for the issues table (no relationships / singular).
fn issues_source() -> SourceSchema {
    SourceSchema::new(vec!["id", "val"], vec![0], vec![(0, true)])
}
// comment(id, issue, val); PK id; joins to issue on `issue == issue.id`. The VIEW
// child schema used inside `issues_schema`'s relationships.
fn comments_schema() -> Schema {
    Schema::new(vec!["id", "issue", "val"], vec![0], vec![(0, true)])
}
// SOURCE schema for the comments table.
fn comments_source() -> SourceSchema {
    SourceSchema::new(vec!["id", "issue", "val"], vec![0], vec![(0, true)])
}
// label(id, issue); PK id. VIEW child schema.
fn labels_schema() -> Schema {
    Schema::new(vec!["id", "issue"], vec![0], vec![(0, true)])
}
// SOURCE schema for the labels table.
fn labels_source() -> SourceSchema {
    SourceSchema::new(vec!["id", "issue"], vec![0], vec![(0, true)])
}

fn issue(id: i64, val: i64) -> Vec<V> {
    vec![V::Int(id), V::Int(val)]
}
fn comment(id: i64, issue: i64, val: i64) -> Vec<V> {
    vec![V::Int(id), V::Int(issue), V::Int(val)]
}

/// issues ⟕ comments (rel "comments") → View. Returns (graph, issues_src,
/// comments_src, view).
fn build_single(issues: Vec<Vec<V>>, comments: Vec<Vec<V>>) -> (Graph, NodeId, NodeId, NodeId) {
    let mut g = Graph::new();
    let issues_src = g.add_source(issues_source(), issues.into_iter().map(row).collect());
    let comments_src = g.add_source(comments_source(), comments.into_iter().map(row).collect());

    let pconn = g.connect(issues_src, Some(vec![(0, true)]), None, vec![]);
    let cconn = g.connect(comments_src, Some(vec![(0, true)]), None, vec![]);

    // issue.id (col 0) == comment.issue (col 1); "comments" is slot 0.
    let join = g.add_join_slot(pconn, cconn, vec![0], vec![1], RelId(0));
    let view = g.add_view(join, issues_schema());

    g.set_conn_output(
        pconn,
        OutEdge {
            node: join,
            port: Port::JoinParent,
        },
    );
    g.set_conn_output(
        cconn,
        OutEdge {
            node: join,
            port: Port::JoinChild,
        },
    );
    g.set_output(join, view);
    (g, issues_src, comments_src, view)
}

#[test]
fn parent_edit_updates_row_and_preserves_children() {
    let (g, issues_src, _c, view) = build_single(vec![issue(1, 100)], vec![comment(10, 1, 500)]);
    g.hydrate(view);
    assert_eq!(
        g.dump_view_rows(view),
        vec![(vec![1, 100], vec![vec![10, 1, 500]])]
    );

    // Edit issue 1's non-key `val` 100 -> 101. id (the join key) is unchanged, so
    // the join forwards an Edit; the comment child is unaffected.
    g.source_push(
        issues_src,
        SourceChange::Edit {
            row: row(issue(1, 101)),
            old: row(issue(1, 100)),
        },
    );
    assert_eq!(
        g.dump_view_rows(view),
        vec![(vec![1, 101], vec![vec![10, 1, 500]])]
    );
}

#[test]
fn child_edit_updates_child_under_parent() {
    let (g, _i, comments_src, view) = build_single(vec![issue(1, 100)], vec![comment(10, 1, 500)]);
    g.hydrate(view);
    assert_eq!(
        g.dump_view_rows(view),
        vec![(vec![1, 100], vec![vec![10, 1, 500]])]
    );

    // Edit comment 10's non-key `val` 500 -> 501. The `issue` join key is unchanged,
    // so the join re-enters the parent fetch and re-emits issue 1 with the updated
    // child spliced in via the edit overlay (remove old, add new).
    g.source_push(
        comments_src,
        SourceChange::Edit {
            row: row(comment(10, 1, 501)),
            old: row(comment(10, 1, 500)),
        },
    );
    assert_eq!(
        g.dump_view_rows(view),
        vec![(vec![1, 100], vec![vec![10, 1, 501]])]
    );
}

#[test]
fn parent_edit_that_changes_join_key_panics() {
    let (g, issues_src, _c, view) = build_single(vec![issue(1, 100)], vec![]);
    g.hydrate(view);
    // Editing the id (the join key) is not a simple edit — the join asserts it.
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        g.source_push(
            issues_src,
            SourceChange::Edit {
                row: row(issue(2, 100)),
                old: row(issue(1, 100)),
            },
        );
    }));
    assert!(
        res.is_err(),
        "editing the join key must panic the parent-edit assert"
    );
}

#[test]
fn nested_join_preserves_inner_relationship() {
    // Stack two joins: (issues ⟕ comments) ⟕ labels. The OUTER join (labels) runs
    // `process_parent_node` on issue nodes that ALREADY carry the inner "comments"
    // relationship; the fix appends rather than replaces, so both survive. Tested
    // on the fetch/hydrate path (stacked-join PUSH wiring is a later concern).
    let mut g = Graph::new();
    let issues_src = g.add_source(issues_source(), vec![row(issue(1, 100))]);
    let comments_src = g.add_source(comments_source(), vec![row(comment(10, 1, 500))]);
    let labels_src = g.add_source(labels_source(), vec![row(vec![V::Int(20), V::Int(1)])]);

    let pconn = g.connect(issues_src, Some(vec![(0, true)]), None, vec![]);
    let cconn = g.connect(comments_src, Some(vec![(0, true)]), None, vec![]);
    let lconn = g.connect(labels_src, Some(vec![(0, true)]), None, vec![]);

    // inner: issue.id == comment.issue, rel "comments" (slot 0)
    let join1 = g.add_join_slot(pconn, cconn, vec![0], vec![1], RelId(0));
    // outer: issue.id == label.issue, rel "labels" (slot 1); parent is join1's output.
    let join2 = g.add_join_slot(join1, lconn, vec![0], vec![1], RelId(1));
    let view = g.add_view(join2, issues_schema());

    g.hydrate(view);

    // The view flattens all of an issue's relationships into `children`. Issue 1
    // must show BOTH the comment (10) and the label (20). If the outer join had
    // dropped the inner relationship (the old spike bug), only [20] would appear.
    assert_eq!(g.dump_view(view), vec![(1, vec![10, 20])]);
}

#[test]
fn stacked_join_push_routes_parent_and_child_changes() {
    // The PUSH counterpart of `nested_join_preserves_inner_relationship`: two
    // sibling joins stacked — (issues ⟕ comments) ⟕ labels — now wired for push by
    // the generalized join output edge. join1's output carries `Port::JoinParent`,
    // so a parent change flows up through BOTH joins, and a child add on either
    // relationship routes a Child change to the view. (Pins the raw graph wiring
    // contract that `build_pipeline`'s join chaining relies on.)
    let mut g = Graph::new();
    let issues_src = g.add_source(issues_source(), vec![row(issue(1, 100))]);
    let comments_src = g.add_source(comments_source(), vec![row(comment(10, 1, 500))]);
    let labels_src = g.add_source(labels_source(), vec![row(vec![V::Int(20), V::Int(1)])]);

    let pconn = g.connect(issues_src, Some(vec![(0, true)]), None, vec![]);
    let cconn = g.connect(comments_src, Some(vec![(0, true)]), None, vec![]);
    let lconn = g.connect(labels_src, Some(vec![(0, true)]), None, vec![]);

    // inner: issue.id == comment.issue, rel "comments" (slot 0); outer: issue.id ==
    // label.issue, rel "labels" (slot 1), whose PARENT is join1.
    let join1 = g.add_join_slot(pconn, cconn, vec![0], vec![1], RelId(0));
    let join2 = g.add_join_slot(join1, lconn, vec![0], vec![1], RelId(1));
    let view = g.add_view(join2, issues_schema());

    // Parent (issue) → join1 parent port; join1 → join2 PARENT port (the new
    // capability); each child connection → its own join's child port; join2 → view.
    g.set_conn_output(
        pconn,
        OutEdge {
            node: join1,
            port: Port::JoinParent,
        },
    );
    g.set_out_edge(
        join1,
        OutEdge {
            node: join2,
            port: Port::JoinParent,
        },
    );
    g.set_conn_output(
        cconn,
        OutEdge {
            node: join1,
            port: Port::JoinChild,
        },
    );
    g.set_conn_output(
        lconn,
        OutEdge {
            node: join2,
            port: Port::JoinChild,
        },
    );
    g.set_output(join2, view);

    g.hydrate(view);
    assert_eq!(g.dump_view(view), vec![(1, vec![10, 20])]);

    // Parent add (issue 2): flows up through join1 (attaches comments) and join2
    // (attaches labels), landing with both child relationships empty.
    g.source_push(issues_src, SourceChange::Add(row(issue(2, 200))));
    assert_eq!(g.dump_view(view), vec![(1, vec![10, 20]), (2, vec![])]);

    // Child add on the INNER join (a comment for issue 2): a Child(comments) that
    // join2 forwards untouched (parent-port passthrough) to the view.
    g.source_push(comments_src, SourceChange::Add(row(comment(11, 2, 600))));
    assert_eq!(g.dump_view(view), vec![(1, vec![10, 20]), (2, vec![11])]);

    // Child add on the OUTER join (a label for issue 1): a Child(labels) on join2's
    // own child port, routed straight to the view.
    g.source_push(
        labels_src,
        SourceChange::Add(row(vec![V::Int(21), V::Int(1)])),
    );
    assert_eq!(
        g.dump_view(view),
        vec![(1, vec![10, 20, 21]), (2, vec![11])]
    );
}
