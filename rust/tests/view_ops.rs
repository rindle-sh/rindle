//! End-to-end tests for the production `ArrayView` **shell** (`09` §9.4): the
//! hydrate → flush → listener → result-type surface, driven through the real
//! pipeline (source → connection → View). The pure-differ contracts (rc,
//! reference stability, COW, edit-move) are covered by the unit tests in
//! `src/view/tests.rs`; the AST→view→dump path is covered by `src/builder.rs`'s
//! pipeline tests (which now run through this production View).

use std::cell::RefCell;
use std::rc::Rc;

use rindle::change::{OutEdge, Port};
use rindle::graph::{Graph, NodeId};
use rindle::value::{owned_row as row, OwnedValue as V, Schema, SourceSchema};
use rindle::{ResultType, ViewData};

// The view/pipeline `Schema` (consumed by `add_view_with`).
fn schema() -> Schema {
    Schema::new(vec!["id", "val"], vec![0], vec![(0, true)])
}
// The same table as a SOURCE schema (fed to `add_source`).
fn source_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "val"], vec![0], vec![(0, true)])
}

fn irow(id: i64, val: i64) -> Vec<V> {
    vec![V::Int(id), V::Int(val)]
}

/// source(issues) → connection → View. Returns (graph, source, view).
fn build(initial: Vec<Vec<V>>, rt: ResultType) -> (Graph, NodeId, NodeId) {
    let mut g = Graph::new();
    let src = g.add_source(source_schema(), initial.into_iter().map(row).collect());
    let conn = g.connect(src, Some(schema().sort), None, vec![]);
    let view = g.add_view_with(conn, schema(), false, rt);
    g.set_conn_output(
        conn,
        OutEdge {
            node: view,
            port: Port::Single,
        },
    );
    (g, src, view)
}

fn data_len(d: &ViewData) -> usize {
    d.items.len()
}

/// The recorded fires: `(top-level length, result type)` per listener call.
type FireLog = Rc<RefCell<Vec<(usize, ResultType)>>>;

/// A listener recording (top-level length, result type) on each fire.
fn recorder() -> (rindle::Listener, FireLog) {
    let log: FireLog = Rc::new(RefCell::new(Vec::new()));
    let l2 = Rc::clone(&log);
    let listener: rindle::Listener = Box::new(move |d: &ViewData, rt: ResultType| {
        l2.borrow_mut().push((data_len(d), rt));
    });
    (listener, log)
}

#[test]
fn hydrate_builds_tree_and_fires_no_listener_when_none_registered() {
    let (g, _src, view) = build(vec![irow(1, 5), irow(2, 9)], ResultType::Complete);
    g.hydrate(view);
    // Hydrate flushes, but there are no listeners yet → nothing observed; the tree
    // is materialized.
    assert_eq!(
        g.dump_view_rows(view),
        vec![(vec![1, 5], vec![]), (vec![2, 9], vec![])]
    );
}

#[test]
fn add_listener_fires_immediately_with_hydrated_snapshot() {
    let (g, _src, view) = build(vec![irow(1, 5), irow(2, 9)], ResultType::Complete);
    g.hydrate(view);
    let (listener, log) = recorder();
    g.view_add_listener(view, listener);
    // Fires once immediately with the current (hydrated) snapshot.
    assert_eq!(*log.borrow(), vec![(2, ResultType::Complete)]);
}

#[test]
fn push_then_flush_fires_once() {
    let (g, src, view) = build(vec![irow(1, 5)], ResultType::Complete);
    g.hydrate(view);
    let (listener, log) = recorder();
    g.view_add_listener(view, listener); // immediate fire: len 1
    assert_eq!(log.borrow().len(), 1);

    // Two pushes before a flush → the changes apply but listeners are NOT fired
    // until flush (one fire, not two).
    g.source_push(src, rindle::SourceChange::Add(row(irow(2, 9))));
    g.source_push(src, rindle::SourceChange::Add(row(irow(3, 7))));
    assert_eq!(log.borrow().len(), 1, "no fire before flush");
    // The tree already reflects both adds (the legacy synchronous-read contract).
    assert_eq!(
        g.dump_view(view),
        vec![(1, vec![]), (2, vec![]), (3, vec![])]
    );

    g.flush_view(view);
    assert_eq!(log.borrow().len(), 2, "one fire on flush");
    assert_eq!(log.borrow()[1], (3, ResultType::Complete));

    // A flush with no pending changes is a no-op (no fire).
    g.flush_view(view);
    assert_eq!(log.borrow().len(), 2, "no-op flush does not fire");
}

#[test]
fn result_type_transitions_unknown_to_complete() {
    let (g, _src, view) = build(vec![irow(1, 5)], ResultType::Unknown);
    g.hydrate(view);
    assert_eq!(g.view_result_type(view), ResultType::Unknown);
    let (listener, log) = recorder();
    g.view_add_listener(view, listener); // immediate fire: Unknown
    assert_eq!(log.borrow()[0], (1, ResultType::Unknown));

    // Async resolution → Complete fires listeners out of band.
    g.set_view_result_type(view, ResultType::Complete);
    assert_eq!(g.view_result_type(view), ResultType::Complete);
    assert_eq!(
        log.borrow().last().copied(),
        Some((1, ResultType::Complete))
    );
}

#[test]
fn result_type_error_path() {
    let (g, _src, view) = build(vec![irow(1, 5)], ResultType::Unknown);
    g.hydrate(view);
    let (listener, log) = recorder();
    g.view_add_listener(view, listener);
    g.set_view_result_type(view, ResultType::Error);
    assert_eq!(g.view_result_type(view), ResultType::Error);
    // Data is preserved through the error transition.
    assert_eq!(log.borrow().last().copied(), Some((1, ResultType::Error)));
}

#[test]
fn empty_hydrate_yields_empty_tree() {
    let (g, _src, view) = build(vec![], ResultType::Complete);
    g.hydrate(view);
    assert!(g.dump_view(view).is_empty());
    let (listener, log) = recorder();
    g.view_add_listener(view, listener);
    assert_eq!(*log.borrow(), vec![(0, ResultType::Complete)]);
}

#[test]
fn push_updates_view_and_remove_drops_row() {
    let (g, src, view) = build(vec![irow(1, 5), irow(2, 9)], ResultType::Complete);
    g.hydrate(view);
    g.source_push(src, rindle::SourceChange::Remove(row(irow(1, 5))));
    assert_eq!(g.dump_view(view), vec![(2, vec![])]);
    g.source_push(
        src,
        rindle::SourceChange::Edit {
            row: row(irow(2, 42)),
            old: row(irow(2, 9)),
        },
    );
    assert_eq!(g.dump_view_rows(view), vec![(vec![2, 42], vec![])]);
}
