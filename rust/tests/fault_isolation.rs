//! WS02.5 — `Graph::source_push_isolated` contains a residual panic to one mutation
//! (returns `RindleError` instead of aborting) and leaves the graph usable for the next
//! push (no poisoned `RefCell`).
//!
//! Gated to debug builds: the residual panic this triggers is the non-strict
//! `gen_push` `debug_assert!` (stripped in release). The `catch_unwind` machinery it
//! exercises is identical in both profiles; only the trigger is debug-only.

#![cfg(debug_assertions)]

use rindle::{owned_row, Graph, OwnedRow, OwnedValue, SourceChange, SourceSchema};

fn schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "v"], vec![0], vec![(0, true)])
}

fn row(id: i64, v: i64) -> OwnedRow {
    owned_row(vec![OwnedValue::Int(id), OwnedValue::Int(v)])
}

#[test]
fn isolated_push_catches_residual_panic_and_graph_survives() {
    let mut g = Graph::new();
    let src = g.add_source(schema(), vec![row(1, 10)]);

    // Validation is OFF (default), so an ADD of an existing PK trips the `gen_push`
    // `debug_assert!(!exists)` — a residual panic. `source_push_isolated` must catch
    // it as an error rather than unwinding past the boundary / aborting.
    let caught = g.source_push_isolated(src, SourceChange::Add(row(1, 99)));
    assert!(
        caught.is_err(),
        "source_push_isolated should surface the residual panic as Err, got {caught:?}"
    );

    // The graph is not poisoned: a subsequent valid push still succeeds (the
    // debug_assert fires before any state mutation, so the graph is clean here).
    assert!(g
        .source_push_isolated(src, SourceChange::Add(row(2, 20)))
        .is_ok());
}

#[test]
fn isolated_push_forwards_ok_when_no_panic() {
    let mut g = Graph::new();
    let src = g.add_source(schema(), vec![row(1, 10)]);
    // A clean push returns Ok through the boundary.
    assert!(g
        .source_push_isolated(src, SourceChange::Add(row(2, 20)))
        .is_ok());
}
