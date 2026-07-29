//! Integration proof for the guarded push fan-out (`designs/205-GUARDED-PUSH-FANOUT-DESIGN.md`):
//! a source write to a value a connection's `where col = ?` guard does **not** match
//! must leave that connection's view exactly as a fresh query would — i.e. skipping
//! the connection is observationally identical to running its (rejecting) predicate.
//!
//! Every push below also runs the debug-only `debug_assert_index_sound` net inside
//! `gen_push` (this is a debug test build), so a guard that wrongly skipped a
//! *matching* connection would panic here rather than silently drop a delta. The
//! assertions on `dump_view` are the view-after-write == fresh-query contract: each
//! expected list is precisely what a from-scratch query over the committed rows yields.

use std::rc::Rc;

use rindle::change::{OutEdge, Port};
use rindle::graph::{Graph, NodeId};
use rindle::push_index::PushGuard;
use rindle::value::{owned_row as row, ColId, OwnedRow, OwnedValue as Value, Schema, SourceSchema};
use rindle::{CmpOp, CompiledPredicate, ConnectionFilters, SourceChange};

const ID: usize = 0;
const PROJ: usize = 1;

fn schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "proj"], vec![ID], vec![(ID, true)]) // order by id asc
}
fn view_schema() -> Schema {
    Schema::new(vec!["id", "proj"], vec![ID], vec![(ID, true)])
}
fn r(id: i64, proj: i64) -> Vec<Value> {
    vec![Value::Int(id), Value::Int(proj)]
}

/// A connection filter `where col == val`, with the matching equality guard — exactly
/// the pair `build_connection_filters` produces, hand-built here so the test needs no
/// query-builder plumbing.
fn guarded_filters(col: ColId, val: i64) -> ConnectionFilters {
    let cp = CompiledPredicate::Cmp {
        col,
        op: CmpOp::Eq,
        value: Value::Int(val),
    };
    ConnectionFilters {
        predicate: Rc::new(move |row: &OwnedRow| cp.eval(row)),
        pk_constraint: None,
        fully_applied: true,
        sql_condition: None,
        push_guard: Some(PushGuard {
            col,
            values: vec![Value::Int(val)],
        }),
    }
}

/// source `t(id, proj)` → conn(`where proj = guard_val`) → View. Returns (graph, src, view).
fn build(initial: Vec<Vec<Value>>, guard_val: i64) -> (Graph, NodeId, NodeId) {
    let mut g = Graph::new();
    let src = g.add_source(schema(), initial.into_iter().map(row).collect());
    let filters = guarded_filters(PROJ, guard_val);
    let conn = g.connect(src, Some(schema().sort), Some(filters), vec![]);
    let view = g.add_view(conn, view_schema());
    g.set_conn_output(
        conn,
        OutEdge {
            node: view,
            port: Port::Single,
        },
    );
    (g, src, view)
}

/// Just the top-row ids of the materialized view.
fn view_ids(g: &Graph, view: NodeId) -> Vec<i64> {
    g.dump_view(view).into_iter().map(|(id, _)| id).collect()
}

#[test]
fn hydrate_only_shows_guard_matching_rows() {
    let (g, _src, view) = build(vec![r(1, 42), r(2, 7), r(3, 42)], 42);
    g.hydrate(view);
    assert_eq!(view_ids(&g, view), vec![1, 3]);
}

#[test]
fn non_matching_add_is_skipped_and_view_unchanged() {
    let (g, src, view) = build(vec![r(1, 42), r(3, 42)], 42);
    g.hydrate(view);
    assert_eq!(view_ids(&g, view), vec![1, 3]);

    // proj = 7 does not match the `proj = 42` guard: the index skips this connection.
    // The view must be identical to a fresh query (still {1, 3}). If the skip were
    // unsound, `debug_assert_index_sound` would panic during this push.
    g.source_push(src, SourceChange::Add(row(r(4, 7))));
    assert_eq!(view_ids(&g, view), vec![1, 3]);
}

#[test]
fn matching_add_reaches_the_connection() {
    let (g, src, view) = build(vec![r(1, 42), r(3, 42)], 42);
    g.hydrate(view);
    g.source_push(src, SourceChange::Add(row(r(5, 42))));
    assert_eq!(view_ids(&g, view), vec![1, 3, 5]);
}

#[test]
fn matching_remove_reaches_the_connection() {
    let (g, src, view) = build(vec![r(1, 42), r(3, 42)], 42);
    g.hydrate(view);
    g.source_push(src, SourceChange::Remove(row(r(1, 42))));
    assert_eq!(view_ids(&g, view), vec![3]);
}

#[test]
fn edit_leaving_the_guard_set_is_visited_via_old_side() {
    // Edit proj 42 -> 7 on id 3: the row LEAVES the guard set. The two-sided Edit
    // candidate lookup must still visit the connection (via the old proj=42 side), so
    // filter_push turns it into a Remove and the view drops id 3.
    let (g, src, view) = build(vec![r(1, 42), r(3, 42)], 42);
    g.hydrate(view);
    g.source_push(
        src,
        SourceChange::Edit {
            row: row(r(3, 7)),
            old: row(r(3, 42)),
        },
    );
    assert_eq!(view_ids(&g, view), vec![1]);
}

#[test]
fn edit_entering_the_guard_set_is_visited_via_new_side() {
    // Edit proj 7 -> 42 on id 2: the row ENTERS the guard set. Visited via the new
    // proj=42 side; filter_push turns it into an Add and the view gains id 2.
    let (g, src, view) = build(vec![r(1, 42), r(2, 7), r(3, 42)], 42);
    g.hydrate(view);
    g.source_push(
        src,
        SourceChange::Edit {
            row: row(r(2, 42)),
            old: row(r(2, 7)),
        },
    );
    assert_eq!(view_ids(&g, view), vec![1, 2, 3]);
}

#[test]
fn two_disjoint_guarded_views_each_see_only_their_own_writes() {
    // Two connections on ONE source, guarded on different values, each to its own
    // view. A write to one project must reach only that project's view — the other is
    // skipped by the index, and its view stays a correct fresh query.
    let mut g = Graph::new();
    let src = g.add_source(schema(), vec![row(r(1, 10)), row(r(2, 20))]);

    let mk_view = |g: &mut Graph, val: i64| {
        let filters = guarded_filters(PROJ, val);
        let conn = g.connect(src, Some(schema().sort), Some(filters), vec![]);
        let view = g.add_view(conn, view_schema());
        g.set_conn_output(
            conn,
            OutEdge {
                node: view,
                port: Port::Single,
            },
        );
        view
    };
    let view10 = mk_view(&mut g, 10);
    let view20 = mk_view(&mut g, 20);
    g.hydrate(view10);
    g.hydrate(view20);
    assert_eq!(view_ids(&g, view10), vec![1]);
    assert_eq!(view_ids(&g, view20), vec![2]);

    // Write to project 20: only view20 changes.
    g.source_push(src, SourceChange::Add(row(r(3, 20))));
    assert_eq!(view_ids(&g, view10), vec![1], "proj=10 view untouched");
    assert_eq!(view_ids(&g, view20), vec![2, 3], "proj=20 view updated");

    // Write to project 10: only view10 changes.
    g.source_push(src, SourceChange::Add(row(r(4, 10))));
    assert_eq!(view_ids(&g, view10), vec![1, 4]);
    assert_eq!(view_ids(&g, view20), vec![2, 3]);
}
