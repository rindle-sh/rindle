#![cfg(feature = "testkit")]

//! Operator-level tests for `Reduce` (`op/reduce.rs`, `REDUCE-DESIGN.md`).
//!
//! **Global** (no `GROUP BY`) `count(*)`: fetch compute-on-first-sight (incl. empty =
//! `0`) and the push matrix — `Add`/`Remove` move the counter and emit an `Edit` of
//! the single immortal row; `Edit`/`Child` are invariant and emit nothing.
//!
//! **Grouped** (top-level `GROUP BY`, eager regime): one `[group, count]` row per
//! group — fetch yields one row per group in key order; push births a new group
//! (`Add`), shifts an existing one (`Edit`), and kills a group at count `0`
//! (`Remove`); a same-group `Edit` is a no-op; a partition-key-changing `Edit` is
//! split upstream into Remove+Add and moves the count between groups.

use rindle::change::{Change, FetchRequest, Node, OutEdge, Port, SourceChange};
use rindle::graph::{Graph, NodeId};
use rindle::op::Reduce;
use rindle::testkit::{
    add, cleaf, edit, rel, remove, run_fetch_test, run_push_test, CaughtChange, SourceSpec,
};
use rindle::value::{owned_row as row, OwnedValue as V, RelDef, RelId, Schema, SourceSchema};

// row = (id PK, val); sort = (id asc).
fn schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "val"], vec![0], vec![(0, true)])
}
fn sort() -> Vec<(usize, bool)> {
    vec![(0, true)]
}
fn r(id: i64, val: i64) -> Vec<V> {
    vec![V::Int(id), V::Int(val)]
}

/// id 1,2,3 — the standard fixture (count = 3).
fn abc() -> Vec<Vec<V>> {
    vec![r(1, 10), r(2, 20), r(3, 30)]
}

/// A build closure wiring `conn → reduce(count)` over source 0. The runner sinks
/// the returned op into a `Catch`.
fn reduce_build() -> impl FnOnce(&mut Graph, &[NodeId]) -> NodeId {
    move |g, srcs| {
        let conn = g.connect(srcs[0], Some(sort()), None, vec![]);
        let storage = g.alloc_storage();
        let reduce = g.add_reduce(Reduce::count(conn, storage));
        g.set_conn_output(
            conn,
            OutEdge {
                node: reduce,
                port: Port::Single,
            },
        );
        reduce
    }
}

/// Run one push over the ABC fixture and return the caught downstream change stream.
fn push_one(change: SourceChange) -> Vec<CaughtChange> {
    run_push_test(
        vec![SourceSpec::new(schema(), abc())],
        reduce_build(),
        vec![(0, change)],
        false,
    )
    .pushes
}

fn add_c(id: i64, val: i64) -> SourceChange {
    SourceChange::Add(row(r(id, val)))
}
fn remove_c(id: i64, val: i64) -> SourceChange {
    SourceChange::Remove(row(r(id, val)))
}
fn edit_c(old: Vec<V>, new: Vec<V>) -> SourceChange {
    SourceChange::Edit {
        row: row(new),
        old: row(old),
    }
}

// --- fetch -----------------------------------------------------------------

#[test]
fn fetch_counts_all_rows() {
    // The synthetic aggregate row is the single count column: count(*) over A,B,C.
    let initial = run_fetch_test(vec![SourceSpec::new(schema(), abc())], reduce_build());
    assert_eq!(initial, vec![cleaf(vec![V::Int(3)])]);
}

#[test]
fn fetch_of_empty_is_zero_row() {
    // Global count(*) is one row even on empty input (SQL semantics): value 0.
    let initial = run_fetch_test(vec![SourceSpec::new(schema(), vec![])], reduce_build());
    assert_eq!(initial, vec![cleaf(vec![V::Int(0)])]);
}

// --- Add / Remove (the invertible counter) ---------------------------------

#[test]
fn add_bumps_count_via_edit() {
    // Adding a row edits the aggregate row from 3 to 4 (Add, not Remove+Add: the
    // global row already exists from hydrate and never dies).
    assert_eq!(
        push_one(add_c(4, 40)),
        vec![edit(vec![V::Int(3)], vec![V::Int(4)])]
    );
}

#[test]
fn remove_drops_count_via_edit() {
    // Removing a row edits the aggregate row from 3 to 2.
    assert_eq!(
        push_one(remove_c(2, 20)),
        vec![edit(vec![V::Int(3)], vec![V::Int(2)])]
    );
}

#[test]
fn remove_last_row_edits_to_zero() {
    // Draining to empty edits to the zero row — the global row does NOT get removed.
    let pushes = run_push_test(
        vec![SourceSpec::new(schema(), vec![r(1, 10)])],
        reduce_build(),
        vec![(0, remove_c(1, 10))],
        false,
    )
    .pushes;
    assert_eq!(pushes, vec![edit(vec![V::Int(1)], vec![V::Int(0)])]);
}

// --- Edit / Child (count(*) invariant) -------------------------------------

#[test]
fn edit_leaves_count_unchanged_emits_nothing() {
    // An in-place edit (same PK) does not change count(*): nothing is emitted.
    assert_eq!(push_one(edit_c(r(2, 20), r(2, 99))), vec![]);
}

#[test]
fn child_change_is_swallowed() {
    // A Child change is a mutation *within* an already-counted row; count(*) is
    // unaffected, so Reduce emits nothing. Pushed directly (a Child needs no source
    // overlay) after hydrating the accumulator.
    let mut g = Graph::new();
    let src = g.add_source(schema(), abc().into_iter().map(row).collect());
    let conn = g.connect(src, Some(sort()), None, vec![]);
    let storage = g.alloc_storage();
    let reduce = g.add_reduce(Reduce::count(conn, storage));
    let catch = g.add_catch(reduce, false);
    g.set_conn_output(
        conn,
        OutEdge {
            node: reduce,
            port: Port::Single,
        },
    );
    g.set_output(reduce, catch);
    let _ = g.catch_fetch(catch, &FetchRequest::all()); // hydrate the accumulator

    let child_change = Change::Child {
        node: Node::leaf(row(r(2, 20))),
        rel: rel(0),
        child: Box::new(Change::Add(Node::leaf(row(vec![V::Int(99), V::Int(0)])))),
    };
    g.push_at(reduce, child_change, Port::Single);

    assert_eq!(g.catch_pushes(catch), vec![]);
}

// --- end-to-end through the production View differ -------------------------

#[test]
fn materializes_and_edits_through_production_view() {
    // v1's other tests use the Catch oracle; this pins the real View differ, whose
    // edit-location uses the output schema's sort + primary_key. Reduce's global
    // output schema has BOTH empty (a singleton), so this proves the empty-sort /
    // empty-PK row survives binary_search + make_id and edits in place.
    use rindle::value::Schema;

    let mut g = Graph::new();
    let src = g.add_source(schema(), abc().into_iter().map(row).collect());
    let conn = g.connect(src, Some(sort()), None, vec![]);
    let storage = g.alloc_storage();
    let reduce = g.add_reduce(Reduce::count(conn, storage));
    let vschema = Schema::new(vec!["count"], Vec::new(), Vec::new());
    let view = g.add_view(reduce, vschema.clone());
    g.set_conn_output(
        conn,
        OutEdge {
            node: reduce,
            port: Port::Single,
        },
    );
    g.set_output(reduce, view);
    g.hydrate(view);

    let caught = |g: &Graph| rindle::testkit::view_data_to_caught(&g.view_data(view), &vschema);

    // Hydrate: the single count row materializes at 3.
    assert_eq!(caught(&g), vec![cleaf(vec![V::Int(3)])]);

    // Add → the lone row edits in place to 4 (not a second row).
    g.source_push(src, add_c(4, 40));
    g.flush_view(view);
    assert_eq!(caught(&g), vec![cleaf(vec![V::Int(4)])]);

    // Drain back below the seed → 2; still exactly one row.
    g.source_push(src, remove_c(1, 10));
    g.flush_view(view);
    g.source_push(src, remove_c(2, 20));
    g.flush_view(view);
    assert_eq!(caught(&g), vec![cleaf(vec![V::Int(2)])]);
}

// ===========================================================================
// Grouped (top-level GROUP BY, eager regime)
// ===========================================================================

// comment = (id PK, issueID, val); group by issueID; conn sorted by id, split-edit
// on issueID so a partition-key-changing edit decomposes into Remove+Add upstream.
fn cschema() -> SourceSchema {
    SourceSchema::new(vec!["id", "issueID", "val"], vec![0], vec![(0, true)])
}
fn csort() -> Vec<(usize, bool)> {
    vec![(0, true)]
}
fn cm(id: i64, issue: i64, val: i64) -> Vec<V> {
    vec![V::Int(id), V::Int(issue), V::Int(val)]
}
/// group 10 → count 2 (c1, c2); group 20 → count 1 (c3).
fn comments() -> Vec<Vec<V>> {
    vec![cm(1, 10, 100), cm(2, 10, 200), cm(3, 20, 300)]
}

fn grouped_build() -> impl FnOnce(&mut Graph, &[NodeId]) -> NodeId {
    move |g, srcs| {
        let conn = g.connect(srcs[0], Some(csort()), None, vec![1]); // split-edit issueID
        let storage = g.alloc_storage();
        let reduce = g.add_reduce(Reduce::count_by(conn, storage, vec![1], vec!["issueID"]));
        g.set_conn_output(
            conn,
            OutEdge {
                node: reduce,
                port: Port::Single,
            },
        );
        reduce
    }
}

fn gpush(change: SourceChange) -> Vec<CaughtChange> {
    run_push_test(
        vec![SourceSpec::new(cschema(), comments())],
        grouped_build(),
        vec![(0, change)],
        false,
    )
    .pushes
}
fn cadd(id: i64, issue: i64, val: i64) -> SourceChange {
    SourceChange::Add(row(cm(id, issue, val)))
}
fn cremove(id: i64, issue: i64, val: i64) -> SourceChange {
    SourceChange::Remove(row(cm(id, issue, val)))
}
fn cedit(old: Vec<V>, new: Vec<V>) -> SourceChange {
    SourceChange::Edit {
        row: row(new),
        old: row(old),
    }
}
/// `[issueID, count]` aggregate row.
fn g_row(issue: i64, count: i64) -> Vec<V> {
    vec![V::Int(issue), V::Int(count)]
}

#[test]
fn grouped_fetch_one_row_per_group() {
    let initial = run_fetch_test(
        vec![SourceSpec::new(cschema(), comments())],
        grouped_build(),
    );
    assert_eq!(initial, vec![cleaf(g_row(10, 2)), cleaf(g_row(20, 1))]);
}

#[test]
fn grouped_add_to_existing_group_edits() {
    assert_eq!(
        gpush(cadd(4, 10, 400)),
        vec![edit(g_row(10, 2), g_row(10, 3))]
    );
}

#[test]
fn grouped_add_new_group_is_born() {
    assert_eq!(gpush(cadd(5, 30, 500)), vec![add(cleaf(g_row(30, 1)))]);
}

#[test]
fn grouped_remove_shrinks_group_edits() {
    assert_eq!(
        gpush(cremove(1, 10, 100)),
        vec![edit(g_row(10, 2), g_row(10, 1))]
    );
}

#[test]
fn grouped_remove_last_in_group_dies() {
    assert_eq!(
        gpush(cremove(3, 20, 300)),
        vec![remove(cleaf(g_row(20, 1)))]
    );
}

#[test]
fn grouped_same_group_edit_emits_nothing() {
    // val changes, issueID stays 10: no split, count unchanged → nothing.
    assert_eq!(gpush(cedit(cm(1, 10, 100), cm(1, 10, 999))), vec![]);
}

#[test]
fn grouped_cross_group_edit_moves_count() {
    // issueID 10 → 20: split-edit upstream into Remove(@10)+Add(@20); group 10
    // shrinks 2→1, group 20 grows 1→2.
    assert_eq!(
        gpush(cedit(cm(1, 10, 100), cm(1, 20, 100))),
        vec![
            edit(g_row(10, 2), g_row(10, 1)),
            edit(g_row(20, 1), g_row(20, 2)),
        ]
    );
}

#[test]
fn grouped_through_production_view_births_and_dies() {
    use rindle::value::Schema;

    let mut g = Graph::new();
    let src = g.add_source(cschema(), comments().into_iter().map(row).collect());
    let conn = g.connect(src, Some(csort()), None, vec![1]);
    let storage = g.alloc_storage();
    let reduce = g.add_reduce(Reduce::count_by(conn, storage, vec![1], vec!["issueID"]));
    let vschema = Schema::new(vec!["issueID", "count"], vec![0], vec![(0, true)]);
    let view = g.add_view(reduce, vschema.clone());
    g.set_conn_output(
        conn,
        OutEdge {
            node: reduce,
            port: Port::Single,
        },
    );
    g.set_output(reduce, view);
    g.hydrate(view);

    let caught = |g: &Graph| rindle::testkit::view_data_to_caught(&g.view_data(view), &vschema);
    assert_eq!(caught(&g), vec![cleaf(g_row(10, 2)), cleaf(g_row(20, 1))]);

    // Birth group 30.
    g.source_push(src, cadd(5, 30, 500));
    g.flush_view(view);
    assert_eq!(
        caught(&g),
        vec![
            cleaf(g_row(10, 2)),
            cleaf(g_row(20, 1)),
            cleaf(g_row(30, 1))
        ]
    );

    // Kill group 20 (its only comment removed).
    g.source_push(src, cremove(3, 20, 300));
    g.flush_view(view);
    assert_eq!(caught(&g), vec![cleaf(g_row(10, 2)), cleaf(g_row(30, 1))]);
}

// ===========================================================================
// Grouped — lazy / constrained regime (relationship aggregate, §8.1)
// ===========================================================================
//
// The lazy reducer is fetched one group at a time (constrained on the output's group
// column, position 0 = issueID). These tests drive it manually: build
// source → conn → reduce(lazy) → catch, do constrained `catch_fetch`s to observe
// groups, then `source_push` and read `catch_pushes`.

/// Build a lazy grouped reducer over a comment source and return its pieces.
fn lazy_graph() -> (Graph, NodeId, NodeId, NodeId) {
    let mut g = Graph::new();
    let src = g.add_source(cschema(), comments().into_iter().map(row).collect());
    let conn = g.connect(src, Some(csort()), None, vec![1]);
    let storage = g.alloc_storage();
    let reduce = g.add_reduce(Reduce::count_by(conn, storage, vec![1], vec!["issueID"]).lazy());
    let catch = g.add_catch(reduce, false);
    g.set_conn_output(
        conn,
        OutEdge {
            node: reduce,
            port: Port::Single,
        },
    );
    g.set_output(reduce, catch);
    (g, src, reduce, catch)
}

/// A fetch constrained to one group (issueID at output position 0).
fn group_constraint(issue: i64) -> FetchRequest {
    FetchRequest::with_constraint(vec![(0, V::Int(issue))])
}

#[test]
fn lazy_constrained_fetch_folds_one_group() {
    let (g, _src, _reduce, catch) = lazy_graph();
    // Group 10 has two comments → one [10, 2] row; the empty group 30 → no row.
    assert_eq!(
        g.catch_fetch(catch, &group_constraint(10)),
        vec![cleaf(g_row(10, 2))]
    );
    assert_eq!(g.catch_fetch(catch, &group_constraint(30)), vec![]);
}

#[test]
fn lazy_add_to_unobserved_group_is_dropped() {
    // Never fetched group 99: the push is dropped (the next fetch would fold it).
    let (g, src, _reduce, catch) = lazy_graph();
    g.source_push(src, cadd(7, 99, 700));
    assert_eq!(g.catch_pushes(catch), vec![]);
}

#[test]
fn lazy_birth_after_observing_empty_group() {
    // Observe empty group 30 (folds + persists count 0), then add its first comment:
    // the count-0 slot makes this a 0 → 1 birth (Add), not a dropped push.
    let (g, src, _reduce, catch) = lazy_graph();
    assert_eq!(g.catch_fetch(catch, &group_constraint(30)), vec![]);
    g.source_push(src, cadd(8, 30, 800));
    assert_eq!(g.catch_pushes(catch), vec![add(cleaf(g_row(30, 1)))]);
}

#[test]
fn lazy_death_keeps_zero_then_rebirths() {
    // Observe group 20 (count 1). Remove its only comment → death (Remove), but the
    // slot is kept at 0; adding a comment back re-births it (Add), not dropped.
    let (g, src, _reduce, catch) = lazy_graph();
    assert_eq!(
        g.catch_fetch(catch, &group_constraint(20)),
        vec![cleaf(g_row(20, 1))]
    );
    g.source_push(src, cremove(3, 20, 300));
    g.source_push(src, cadd(9, 20, 900));
    assert_eq!(
        g.catch_pushes(catch),
        vec![remove(cleaf(g_row(20, 1))), add(cleaf(g_row(20, 1)))]
    );
}

#[test]
fn lazy_add_and_remove_on_observed_group_shifts() {
    // Observe group 10 (count 2), then add and remove within it → two Edits.
    let (g, src, _reduce, catch) = lazy_graph();
    let _ = g.catch_fetch(catch, &group_constraint(10));
    g.source_push(src, cadd(10, 10, 1000)); // 2 -> 3
    g.source_push(src, cremove(1, 10, 100)); // 3 -> 2
    assert_eq!(
        g.catch_pushes(catch),
        vec![
            edit(g_row(10, 2), g_row(10, 3)),
            edit(g_row(10, 3), g_row(10, 2)),
        ]
    );
}

// ===========================================================================
// Tier-1 singular-relationship attachment (REDUCE-DESIGN.md §9, checklist #1)
// ===========================================================================
//
// `issue { commentCount: count(comments) }` is the *grouped* aggregate (§8)
// partitioned by `issueID`, attached to each issue as a **singular** relationship
// by a parent join: the lazy reduce is the top of the child subtree, and the join's
// `JoinChild` port lifts each per-group `Add`/`Edit`/`Remove` into a
// `Change::Child{…}` on the issue, which the production `View` propagates.
//
// This milestone wires that dataflow and pins the `Change::Child{…Edit…}`
// propagation (plus birth/death). The *scalar projection* — unwrapping the one-row
// `[issueID, count]` child into a named scalar field, and substituting `0` for an
// empty (childless) group — is checklist #2 and is NOT done here: a childless issue
// surfaces as an **empty** relationship, and a counted one as a one-row
// `[issueID, count]` child.

// issue = (id PK, val); sort by id. The join parent.
fn ischema() -> SourceSchema {
    SourceSchema::new(vec!["id", "val"], vec![0], vec![(0, true)])
}
fn iss(id: i64, val: i64) -> Vec<V> {
    vec![V::Int(id), V::Int(val)]
}
/// issues 10, 20, 30 — group 10 has 2 comments, 20 has 1, 30 has none (empty).
fn issues() -> Vec<Vec<V>> {
    vec![iss(10, 100), iss(20, 200), iss(30, 300)]
}

/// The reduce's output / view child schema: `[issueID, count]`, PK + sort = issueID,
/// marked `.singular` (the `.one()` shape the §9 projection will later unwrap).
fn agg_child_schema() -> Schema {
    let mut s = Schema::new(vec!["issueID", "count"], vec![0], vec![(0, true)]);
    s.singular = true;
    s
}

/// The issue view schema with one singular `commentCount` relationship (slot 0)
/// whose child is the aggregate row.
fn issues_view_schema() -> Schema {
    Schema::new(vec!["id", "val"], vec![0], vec![(0, true)])
        .with_relationships(vec![RelDef::related("commentCount", agg_child_schema())])
}

/// Build `issues ⟕ count(comments by issueID) → View`, the §9 attachment. The lazy
/// reduce feeds the join's `JoinChild` port; the join attaches it as the singular
/// `commentCount` slot (RelId 0). Returns (graph, issues_src, comments_src, view).
fn attach_graph() -> (Graph, NodeId, NodeId, NodeId) {
    let mut g = Graph::new();
    let issues_src = g.add_source(ischema(), issues().into_iter().map(row).collect());
    let comments_src = g.add_source(cschema(), comments().into_iter().map(row).collect());

    let pconn = g.connect(issues_src, Some(vec![(0, true)]), None, vec![]);
    let cconn = g.connect(comments_src, Some(csort()), None, vec![1]); // split-edit issueID
    let storage = g.alloc_storage();
    // Lazy: the parent join fetches one group at a time (the relationship regime).
    let reduce = g.add_reduce(Reduce::count_by(cconn, storage, vec![1], vec!["issueID"]).lazy());

    // issue.id (col 0) == reduce.issueID (output col 0); "commentCount" is slot 0.
    let join = g.add_join_slot(pconn, reduce, vec![0], vec![0], RelId(0));
    let view = g.add_view(join, issues_view_schema());

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
            node: reduce,
            port: Port::Single,
        },
    );
    g.set_out_edge(
        reduce,
        OutEdge {
            node: join,
            port: Port::JoinChild,
        },
    );
    g.set_output(join, view);
    (g, issues_src, comments_src, view)
}

#[test]
fn attach_hydrates_one_count_per_issue() {
    // Each issue carries its count as a one-row child; the childless issue 30 has an
    // empty relationship (the "0 for an absent group" lives in the §9 projection,
    // checklist #2 — not the operator).
    let (g, _i, _c, view) = attach_graph();
    g.hydrate(view);
    assert_eq!(
        g.dump_view_rows(view),
        vec![
            (vec![10, 100], vec![vec![10, 2]]),
            (vec![20, 200], vec![vec![20, 1]]),
            (vec![30, 300], vec![]),
        ]
    );
}

#[test]
fn attach_comment_add_edits_count_under_parent() {
    // Adding a comment to issue 10 (2 → 3) propagates as Change::Child{…Edit…}: the
    // issue's one aggregate child edits in place 2 → 3.
    let (g, _i, comments_src, view) = attach_graph();
    g.hydrate(view);
    g.source_push(comments_src, cadd(4, 10, 400));
    g.flush_view(view);
    assert_eq!(
        g.dump_view_rows(view),
        vec![
            (vec![10, 100], vec![vec![10, 3]]),
            (vec![20, 200], vec![vec![20, 1]]),
            (vec![30, 300], vec![]),
        ]
    );
}

#[test]
fn attach_comment_remove_edits_count_under_parent() {
    // Removing one of issue 10's two comments (2 → 1) edits its child in place.
    let (g, _i, comments_src, view) = attach_graph();
    g.hydrate(view);
    g.source_push(comments_src, cremove(1, 10, 100));
    g.flush_view(view);
    assert_eq!(
        g.dump_view_rows(view),
        vec![
            (vec![10, 100], vec![vec![10, 1]]),
            (vec![20, 200], vec![vec![20, 1]]),
            (vec![30, 300], vec![]),
        ]
    );
}

#[test]
fn attach_first_comment_births_child_on_empty_issue() {
    // Issue 30 starts childless (empty relationship). Its first comment is a 0 → 1
    // birth (Change::Child{…Add…}): the relationship gains its one [30, 1] row.
    let (g, _i, comments_src, view) = attach_graph();
    g.hydrate(view);
    g.source_push(comments_src, cadd(5, 30, 500));
    g.flush_view(view);
    assert_eq!(
        g.dump_view_rows(view),
        vec![
            (vec![10, 100], vec![vec![10, 2]]),
            (vec![20, 200], vec![vec![20, 1]]),
            (vec![30, 300], vec![vec![30, 1]]),
        ]
    );
}

#[test]
fn attach_last_comment_removal_dies_to_empty_child() {
    // Removing issue 20's only comment is a 1 → 0 death (Change::Child{…Remove…}):
    // its relationship drains back to empty.
    let (g, _i, comments_src, view) = attach_graph();
    g.hydrate(view);
    g.source_push(comments_src, cremove(3, 20, 300));
    g.flush_view(view);
    assert_eq!(
        g.dump_view_rows(view),
        vec![
            (vec![10, 100], vec![vec![10, 2]]),
            (vec![20, 200], vec![]),
            (vec![30, 300], vec![]),
        ]
    );
}

#[test]
fn attach_cross_issue_comment_move_edits_both_counts() {
    // Re-assigning comment 1 from issue 10 to issue 20 (split-edit on issueID →
    // Remove(@10) + Add(@20)): issue 10's child edits 2 → 1, issue 20's edits 1 → 2.
    let (g, _i, comments_src, view) = attach_graph();
    g.hydrate(view);
    g.source_push(comments_src, cedit(cm(1, 10, 100), cm(1, 20, 100)));
    g.flush_view(view);
    assert_eq!(
        g.dump_view_rows(view),
        vec![
            (vec![10, 100], vec![vec![10, 1]]),
            (vec![20, 200], vec![vec![20, 2]]),
            (vec![30, 300], vec![]),
        ]
    );
}

// ===========================================================================
// Sum / Avg — the other two invertible aggregates (REDUCE-DESIGN.md §5/§6)
// ===========================================================================
//
// Unlike `count(*)`, `sum`/`avg` read the summed column's value, so an **in-place
// `Edit`** (same PK/group, changed value) DOES move them (and emits) — the case a
// count no-ops. A row that is `NULL` in the summed column bumps `count(*)` but not
// `sum`/`avg`, so a sum-only reducer emits nothing for it. Empty input ⇒ `sum`/`avg`
// are `NULL` (SQL), not `0`.

/// A row with a `Float` value cell (`schema()` is `(id, val)`; val has no affinity).
fn rf(id: i64, f: f64) -> Vec<V> {
    vec![V::Int(id), V::Float(f)]
}
/// A row that is `NULL` in the summed column.
fn rn(id: i64) -> Vec<V> {
    vec![V::Int(id), V::Null]
}

fn sum_build() -> impl FnOnce(&mut Graph, &[NodeId]) -> NodeId {
    move |g, srcs| {
        let conn = g.connect(srcs[0], Some(sort()), None, vec![]);
        let storage = g.alloc_storage();
        let reduce = g.add_reduce(Reduce::sum(conn, storage, 1)); // sum(val)
        g.set_conn_output(
            conn,
            OutEdge {
                node: reduce,
                port: Port::Single,
            },
        );
        reduce
    }
}
fn avg_build() -> impl FnOnce(&mut Graph, &[NodeId]) -> NodeId {
    move |g, srcs| {
        let conn = g.connect(srcs[0], Some(sort()), None, vec![]);
        let storage = g.alloc_storage();
        let reduce = g.add_reduce(Reduce::avg(conn, storage, 1)); // avg(val)
        g.set_conn_output(
            conn,
            OutEdge {
                node: reduce,
                port: Port::Single,
            },
        );
        reduce
    }
}

/// Run one push over a given source fixture + build and return the caught stream.
fn push_over(
    rows: Vec<Vec<V>>,
    build: impl FnOnce(&mut Graph, &[NodeId]) -> NodeId,
    change: SourceChange,
) -> Vec<CaughtChange> {
    run_push_test(
        vec![SourceSpec::new(schema(), rows)],
        build,
        vec![(0, change)],
        false,
    )
    .pushes
}

// --- global sum ------------------------------------------------------------

#[test]
fn sum_fetch_sums_values() {
    // sum(val) over 10, 20, 30 = 60, emitted as an integer (all-integer inputs).
    let initial = run_fetch_test(vec![SourceSpec::new(schema(), abc())], sum_build());
    assert_eq!(initial, vec![cleaf(vec![V::Int(60)])]);
}

#[test]
fn sum_of_empty_is_null() {
    // SQL `sum` of no rows is NULL (not 0) — the immortal global row carries NULL.
    let initial = run_fetch_test(vec![SourceSpec::new(schema(), vec![])], sum_build());
    assert_eq!(initial, vec![cleaf(vec![V::Null])]);
}

#[test]
fn sum_add_moves_via_edit() {
    // Adding (4, 40): 60 → 100.
    assert_eq!(
        push_over(abc(), sum_build(), add_c(4, 40)),
        vec![edit(vec![V::Int(60)], vec![V::Int(100)])]
    );
}

#[test]
fn sum_remove_moves_via_edit() {
    // Removing (2, 20): 60 → 40.
    assert_eq!(
        push_over(abc(), sum_build(), remove_c(2, 20)),
        vec![edit(vec![V::Int(60)], vec![V::Int(40)])]
    );
}

#[test]
fn sum_in_place_edit_moves_the_sum() {
    // The count-invariant case that DOES move a sum: (2, 20) → (2, 99) shifts the sum
    // by +79 (−20 +99): 60 → 139. (A `count(*)` would emit nothing here.)
    assert_eq!(
        push_over(abc(), sum_build(), edit_c(r(2, 20), r(2, 99))),
        vec![edit(vec![V::Int(60)], vec![V::Int(139)])]
    );
}

#[test]
fn sum_add_null_value_emits_nothing() {
    // A row that is NULL in the summed column changes count(*) but not sum — a sum-only
    // reducer emits nothing (no Edit{old == new} flicker).
    assert_eq!(
        push_over(abc(), sum_build(), SourceChange::Add(row(rn(4)))),
        vec![]
    );
}

#[test]
fn sum_drain_to_empty_is_null() {
    // Draining the last row edits the immortal row's sum to NULL (SQL empty sum).
    assert_eq!(
        push_over(vec![r(1, 10)], sum_build(), remove_c(1, 10)),
        vec![edit(vec![V::Int(10)], vec![V::Null])]
    );
}

#[test]
fn sum_promotes_to_float_when_a_float_contributes() {
    // Mixed int + float inputs sum as a float (matching SQLite's typing): 10 + 2.5.
    let initial = run_fetch_test(
        vec![SourceSpec::new(schema(), vec![r(1, 10), rf(2, 2.5)])],
        sum_build(),
    );
    assert_eq!(initial, vec![cleaf(vec![V::Float(12.5)])]);
}

#[test]
fn sum_demotes_back_to_int_when_the_float_is_removed() {
    // Removing the only float value demotes the sum back to an integer (invertible
    // float_count): {10, 2.5} sum 12.5 (float) → remove 2.5 → 10 (int).
    assert_eq!(
        push_over(
            vec![r(1, 10), rf(2, 2.5)],
            sum_build(),
            SourceChange::Remove(row(rf(2, 2.5)))
        ),
        vec![edit(vec![V::Float(12.5)], vec![V::Int(10)])]
    );
}

// --- global avg ------------------------------------------------------------

#[test]
fn avg_fetch_is_mean_as_float() {
    // avg(val) over 10, 20, 30 = 20.0 (SQL `avg` is always real).
    let initial = run_fetch_test(vec![SourceSpec::new(schema(), abc())], avg_build());
    assert_eq!(initial, vec![cleaf(vec![V::Float(20.0)])]);
}

#[test]
fn avg_of_empty_is_null() {
    let initial = run_fetch_test(vec![SourceSpec::new(schema(), vec![])], avg_build());
    assert_eq!(initial, vec![cleaf(vec![V::Null])]);
}

#[test]
fn avg_add_recomputes_mean() {
    // (10+20+30+40)/4 = 25.0.
    assert_eq!(
        push_over(abc(), avg_build(), add_c(4, 40)),
        vec![edit(vec![V::Float(20.0)], vec![V::Float(25.0)])]
    );
}

#[test]
fn avg_ignores_null_in_denominator() {
    // avg's denominator is count(col) (non-NULL), not count(*): adding a NULL-val row
    // leaves the mean unchanged → nothing emitted.
    assert_eq!(
        push_over(abc(), avg_build(), SourceChange::Add(row(rn(4)))),
        vec![]
    );
}

// --- grouped sum / avg -----------------------------------------------------

fn grouped_sum_build() -> impl FnOnce(&mut Graph, &[NodeId]) -> NodeId {
    move |g, srcs| {
        let conn = g.connect(srcs[0], Some(csort()), None, vec![1]); // split-edit issueID
        let storage = g.alloc_storage();
        // sum(val) — val is col 2 of the comment row.
        let reduce = g.add_reduce(Reduce::sum_by(conn, storage, vec![1], vec!["issueID"], 2));
        g.set_conn_output(
            conn,
            OutEdge {
                node: reduce,
                port: Port::Single,
            },
        );
        reduce
    }
}

fn gsum_push(change: SourceChange) -> Vec<CaughtChange> {
    run_push_test(
        vec![SourceSpec::new(cschema(), comments())],
        grouped_sum_build(),
        vec![(0, change)],
        false,
    )
    .pushes
}
/// `[issueID, sum]` aggregate row.
fn gs_row(issue: i64, sum: i64) -> Vec<V> {
    vec![V::Int(issue), V::Int(sum)]
}

#[test]
fn grouped_sum_fetch_one_row_per_group() {
    // group 10 → 100+200 = 300; group 20 → 300.
    let initial = run_fetch_test(
        vec![SourceSpec::new(cschema(), comments())],
        grouped_sum_build(),
    );
    assert_eq!(
        initial,
        vec![cleaf(gs_row(10, 300)), cleaf(gs_row(20, 300))]
    );
}

#[test]
fn grouped_sum_add_to_group_edits() {
    // group 10: 300 → 700 (add val 400).
    assert_eq!(
        gsum_push(cadd(4, 10, 400)),
        vec![edit(gs_row(10, 300), gs_row(10, 700))]
    );
}

#[test]
fn grouped_sum_new_group_is_born() {
    assert_eq!(
        gsum_push(cadd(5, 30, 500)),
        vec![add(cleaf(gs_row(30, 500)))]
    );
}

#[test]
fn grouped_sum_same_group_edit_moves_sum() {
    // val 100 → 999 within group 10: count steady but sum 300 → 1199 → EMITS an Edit
    // (the case a grouped `count` no-ops).
    assert_eq!(
        gsum_push(cedit(cm(1, 10, 100), cm(1, 10, 999))),
        vec![edit(gs_row(10, 300), gs_row(10, 1199))]
    );
}

#[test]
fn grouped_sum_last_row_dies() {
    // Group 20 drains (its only comment removed) → Remove of the [20, 300] row.
    assert_eq!(
        gsum_push(cremove(3, 20, 300)),
        vec![remove(cleaf(gs_row(20, 300)))]
    );
}

#[test]
fn grouped_sum_cross_group_move_edits_both() {
    // issueID 10 → 20 (split-edit): group 10 loses 100 (300 → 200), group 20 gains 100
    // (300 → 400).
    assert_eq!(
        gsum_push(cedit(cm(1, 10, 100), cm(1, 20, 100))),
        vec![
            edit(gs_row(10, 300), gs_row(10, 200)),
            edit(gs_row(20, 300), gs_row(20, 400)),
        ]
    );
}

#[test]
fn grouped_avg_fetch_one_row_per_group() {
    let build = move |g: &mut Graph, srcs: &[NodeId]| {
        let conn = g.connect(srcs[0], Some(csort()), None, vec![1]);
        let storage = g.alloc_storage();
        let reduce = g.add_reduce(Reduce::avg_by(conn, storage, vec![1], vec!["issueID"], 2));
        g.set_conn_output(
            conn,
            OutEdge {
                node: reduce,
                port: Port::Single,
            },
        );
        reduce
    };
    // group 10 → (100+200)/2 = 150.0; group 20 → 300/1 = 300.0.
    let initial = run_fetch_test(vec![SourceSpec::new(cschema(), comments())], build);
    assert_eq!(
        initial,
        vec![
            cleaf(vec![V::Int(10), V::Float(150.0)]),
            cleaf(vec![V::Int(20), V::Float(300.0)]),
        ]
    );
}

// ===========================================================================
// §5.4 group-state-key canonicalization (design 226)
// ===========================================================================

// (id PK, grp, val) — grouped sum(val) by grp; grp cells arrive mixed-plane.
fn mschema() -> SourceSchema {
    SourceSchema::new(vec!["id", "grp", "val"], vec![0], vec![(0, true)])
}
fn mixed_plane_rows() -> Vec<Vec<V>> {
    vec![
        vec![V::Int(1), V::Int(5), V::Int(10)],
        vec![V::Int(2), V::Float(5.0), V::Int(20)],
    ]
}
fn sum_by_build() -> impl FnOnce(&mut Graph, &[NodeId]) -> NodeId {
    move |g, srcs| {
        let conn = g.connect(srcs[0], Some(csort()), None, vec![1]); // split-edit grp
        let storage = g.alloc_storage();
        let reduce = g.add_reduce(Reduce::sum_by(conn, storage, vec![1], vec!["grp"], 2));
        g.set_conn_output(
            conn,
            OutEdge {
                node: reduce,
                port: Port::Single,
            },
        );
        reduce
    }
}

/// `Int(5)` and `Float(5.0)` are `values_equal`, so they are ONE group sharing one
/// accumulator slot — the same equivalence the comparators and the join `CanonVal`
/// implement (§5.4 names "join/group hash keys"), and what the SQLite oracle's
/// `GROUP BY` answers (5 = 5.0). Under the old per-variant encoding they folded
/// into two groups whose group cells compare Equal — duplicate-PK output rows.
#[test]
fn mixed_plane_group_cells_fold_into_one_group() {
    let initial = run_fetch_test(
        vec![SourceSpec::new(mschema(), mixed_plane_rows())],
        sum_by_build(),
    );
    assert_eq!(initial, vec![cleaf(vec![V::Int(5), V::Int(30)])]);
}

/// The sharp corollary: a plane-crossing Edit (`Int(5)` → `Float(5.0)` on the group
/// column) is NOT split-edited upstream (`values_equal` says the key did not
/// change), so `grouped_edit` must find the SAME state under the new row's key.
/// Under the old encoding it keyed a missing slot, silently dropped the edit, and
/// left the accumulator stale — view-after-write ≠ fresh-query.
#[test]
fn a_plane_crossing_edit_shifts_the_shared_accumulator() {
    let pushes = run_push_test(
        vec![SourceSpec::new(mschema(), mixed_plane_rows())],
        sum_by_build(),
        vec![(
            0,
            SourceChange::Edit {
                row: row(vec![V::Int(1), V::Float(5.0), V::Int(11)]),
                old: row(vec![V::Int(1), V::Int(5), V::Int(10)]),
            },
        )],
        false,
    )
    .pushes;
    assert_eq!(
        pushes,
        vec![edit(
            vec![V::Float(5.0), V::Int(30)],
            vec![V::Float(5.0), V::Int(31)]
        )],
        "the shared slot shifts 30 → 31; a dropped edit here is the stale-accumulator bug"
    );
}

/// `-0.0` keeps float identity — its own group beside the `0`/`0.0` class — matching
/// the comparator's carve-out (`float_int_class(-0.0)` is None; §5.1 total order).
#[test]
fn negative_zero_keeps_its_own_group_slot() {
    let groups = run_fetch_test(
        vec![SourceSpec::new(
            mschema(),
            vec![
                vec![V::Int(1), V::Float(-0.0), V::Int(1)],
                vec![V::Int(2), V::Int(0), V::Int(2)],
                vec![V::Int(3), V::Float(0.0), V::Int(4)],
            ],
        )],
        sum_by_build(),
    );
    let two_groups_either_order = groups
        == vec![
            cleaf(vec![V::Float(-0.0), V::Int(1)]),
            cleaf(vec![V::Int(0), V::Int(6)]),
        ]
        || groups
            == vec![
                cleaf(vec![V::Int(0), V::Int(6)]),
                cleaf(vec![V::Float(-0.0), V::Int(1)]),
            ];
    assert!(
        two_groups_either_order,
        "-0.0 alone (sum 1) beside the 0 ≡ 0.0 class (sum 6): {groups:?}"
    );
}
