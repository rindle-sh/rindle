#![cfg(feature = "testkit")]

//! The §5.3 integer-`sum` overflow contract (design 226, Stage C5).
//!
//! The accumulator is **i128** — a delta-order transient past `i64::MAX` neither
//! wraps nor errors, and a `Remove` of `i64::MIN` is safe — while the **emitted**
//! total is bounded: the moment a stored group's all-integer set total leaves the
//! i64 range, every `try_*` boundary raises a typed
//! [`RindleError::UnsupportedValue`], and the moment the state shrinks back into
//! range the error clears (SQLite's *result* contract: a fresh `SELECT sum(…)`
//! over that state errors every time; over the corrected state it does not).

use rindle::change::{FetchRequest, OutEdge, Port, SourceChange};
use rindle::graph::{Graph, NodeId};
use rindle::op::Reduce;
use rindle::value::{owned_row as row, OwnedValue as V, SourceSchema};
use rindle::RindleError;

/// One graph: memory source `(id PK, val)` → `sum(val)` → catch sink, hydrated.
fn sum_graph(initial: Vec<Vec<V>>) -> (Graph, NodeId, NodeId) {
    let mut g = Graph::new();
    let src = g.add_source(
        SourceSchema::new(vec!["id", "val"], vec![0], vec![(0, true)]),
        initial.into_iter().map(row).collect(),
    );
    let conn = g.connect(src, Some(vec![(0, true)]), None, vec![]);
    let storage = g.alloc_storage();
    let reduce = g.add_reduce(Reduce::sum(conn, storage, 1));
    g.set_conn_output(
        conn,
        OutEdge {
            node: reduce,
            port: Port::Single,
        },
    );
    let catch = g.add_catch(reduce, false);
    g.set_sink_edge(reduce, catch);
    g.catch_fetch(catch, &FetchRequest::all());
    (g, src, reduce)
}

fn r(id: i64, val: i64) -> Vec<V> {
    vec![V::Int(id), V::Int(val)]
}

/// The current emitted aggregate cell (a global reduce's row is just `[<agg>]`),
/// read fresh through the reduce.
fn current_sum(g: &Graph, reduce: NodeId) -> V {
    let rows = g
        .try_fetch_all(reduce, &FetchRequest::all())
        .expect("fetch the aggregate row");
    rows[0].row.col(0).to_owned()
}

#[test]
fn remove_of_i64_min_neither_panics_nor_wraps() {
    let (g, src, reduce) = sum_graph(vec![r(1, i64::MIN), r(2, 7)]);
    // Pre-i128 this Remove computed `-1 * i64::MIN` — an overflow panic in debug,
    // a wrap in release.
    g.try_source_push(src, SourceChange::Remove(row(r(1, i64::MIN))))
        .expect("removing the i64::MIN row is a legal write");
    assert!(matches!(current_sum(&g, reduce), V::Int(7)));
}

#[test]
fn a_write_pushing_the_set_total_out_of_i64_fails_typed_and_recovery_clears_it() {
    let (g, src, reduce) = sum_graph(vec![r(1, i64::MAX)]);

    // The offending write: total becomes MAX + 1 — outside i64. The push fails
    // with the §5.3 typed error, not a wrapped or rounded number.
    let err = g
        .try_source_push(src, SourceChange::Add(row(r(2, 1))))
        .expect_err("the write that pushes the set total out of range must fail");
    match err {
        RindleError::UnsupportedValue(msg) => {
            assert!(msg.contains("sum overflow"), "typed message, got: {msg}")
        }
        other => panic!("expected UnsupportedValue, got {other:?}"),
    }

    // The error is STATE-based, not sticky: while the stored total stays out of
    // range every boundary raises (a fresh SQLite sum over this state errors
    // too)…
    let refetch = g.try_fetch_all(reduce, &FetchRequest::all());
    assert!(
        matches!(refetch, Err(RindleError::UnsupportedValue(_))),
        "reads over an unrepresentable total raise the same typed error"
    );

    // …and the compensating write clears it: the total returns to i64::MAX and
    // both pushes and reads succeed again.
    g.try_source_push(src, SourceChange::Remove(row(r(2, 1))))
        .expect("the write that brings the total back into range succeeds");
    assert!(matches!(current_sum(&g, reduce), V::Int(i64::MAX)));
}

#[test]
fn a_cross_push_transient_fails_the_offending_write_then_heals_exactly() {
    // A transaction whose Add transiently overflows before its Remove: each change
    // is its own write boundary (the replica derive loop pushes per change), so
    // the Add fails — the §5.3 per-write contract — but the i128 accumulator
    // absorbed the transient EXACTLY, so the compensating Remove lands on the true
    // total, not on wrapped garbage, and the state-based check clears. (The host's
    // fault path re-hydrates over the corrected source state and heals the same
    // way; an i64 accumulator would have wrapped and stayed silently corrupt.)
    let (g, src, reduce) = sum_graph(vec![r(1, i64::MAX)]);
    assert!(g
        .try_source_push(src, SourceChange::Add(row(r(2, 5))))
        .is_err());
    g.try_source_push(src, SourceChange::Remove(row(r(1, i64::MAX))))
        .expect("the compensating remove brings the total into range");
    assert!(matches!(current_sum(&g, reduce), V::Int(5)));
}

#[test]
fn a_single_edit_near_the_boundary_folds_both_sides_before_emitting() {
    // An Edit folds Remove(old)+Add(new) into the accumulator and emits ONCE —
    // no intermediate total exists to trip on.
    let (g, src, reduce) = sum_graph(vec![r(1, i64::MAX), r(2, 0)]);
    g.try_source_push(
        src,
        SourceChange::Edit {
            row: row(r(2, -5)),
            old: row(r(2, 0)),
        },
    )
    .expect("an in-range edit beside i64::MAX succeeds");
    let expect = i64::MAX - 5;
    assert!(matches!(current_sum(&g, reduce), V::Int(v) if v == expect));
}

#[test]
fn a_float_contribution_keeps_the_sum_f64_and_never_int_errors() {
    // Once any float contributes, `sum` emits f64 (SQL) — the integer bound does
    // not apply, however large the integer side is.
    let (g, src, reduce) = sum_graph(vec![r(1, i64::MAX), r(2, i64::MAX)]);
    // Two MAXes, all-integer: out of range → boundary errors.
    assert!(g.try_fetch_all(reduce, &FetchRequest::all()).is_err());
    // A float row demotes the whole sum to f64: the error clears.
    g.try_source_push(src, SourceChange::Add(row(vec![V::Int(3), V::Float(0.5)])))
        .expect("a float contribution makes the sum f64 — no integer bound");
    assert!(matches!(current_sum(&g, reduce), V::Float(_)));
}

#[test]
fn avg_stays_f64_and_never_errors() {
    let mut g = Graph::new();
    let src = g.add_source(
        SourceSchema::new(vec!["id", "val"], vec![0], vec![(0, true)]),
        vec![row(r(1, i64::MAX)), row(r(2, i64::MAX))],
    );
    let conn = g.connect(src, Some(vec![(0, true)]), None, vec![]);
    let storage = g.alloc_storage();
    let reduce = g.add_reduce(Reduce::avg(conn, storage, 1));
    g.set_conn_output(
        conn,
        OutEdge {
            node: reduce,
            port: Port::Single,
        },
    );
    let catch = g.add_catch(reduce, false);
    g.set_sink_edge(reduce, catch);
    g.catch_fetch(catch, &FetchRequest::all());

    let rows = g
        .try_fetch_all(reduce, &FetchRequest::all())
        .expect("avg is always f64 — no integer bound (design 226 §5.3)");
    match rows[0].row.col(0).to_owned() {
        V::Float(f) => assert!((f - i64::MAX as f64).abs() < 1e4),
        other => panic!("avg must be Float, got {other:?}"),
    }
}

/// Per-group tracking: one out-of-range group fails every boundary; shrinking it
/// back into range clears via the stored-state transition (`write_at`, true→false).
/// The death-while-overflowed decrement (`del_at`, true→None) is not exercisable
/// through per-row deltas — a count-1 all-Int group is always in range, so every
/// group passes through an in-range state before it can die; that arm is
/// defensively-correct dead code.
#[test]
fn grouped_sum_tracks_overflow_per_group_and_clears_when_the_group_shrinks() {
    // (id PK, grp, val) — grouped sum(val) by grp.
    let mut g = Graph::new();
    let src = g.add_source(
        SourceSchema::new(vec!["id", "grp", "val"], vec![0], vec![(0, true)]),
        vec![
            row(vec![V::Int(1), V::Int(10), V::Int(i64::MAX)]),
            row(vec![V::Int(2), V::Int(20), V::Int(7)]),
        ],
    );
    let conn = g.connect(src, Some(vec![(0, true)]), None, vec![1]);
    let storage = g.alloc_storage();
    let reduce = g.add_reduce(Reduce::sum_by(conn, storage, vec![1], vec!["grp"], 2));
    g.set_conn_output(
        conn,
        OutEdge {
            node: reduce,
            port: Port::Single,
        },
    );
    let catch = g.add_catch(reduce, false);
    g.set_sink_edge(reduce, catch);
    g.catch_fetch(catch, &FetchRequest::all());

    // Overflow group 10 (MAX + 1); group 20 stays fine — but ANY unrepresentable
    // group fails the boundary.
    let err = g.try_source_push(
        src,
        SourceChange::Add(row(vec![V::Int(3), V::Int(10), V::Int(1)])),
    );
    assert!(matches!(err, Err(RindleError::UnsupportedValue(_))));
    assert!(
        g.try_fetch_all(reduce, &FetchRequest::all()).is_err(),
        "one unrepresentable group is enough to fail every boundary"
    );

    // Shrink group 10 back into range (remove the MAX row → total 1): the
    // per-group tracking clears and both groups read fine again.
    g.try_source_push(
        src,
        SourceChange::Remove(row(vec![V::Int(1), V::Int(10), V::Int(i64::MAX)])),
    )
    .expect("the write that brings the group back into range succeeds");
    let rows = g
        .try_fetch_all(reduce, &FetchRequest::all())
        .expect("both groups representable again");
    let sums: Vec<V> = rows.iter().map(|n| n.row.col(1).to_owned()).collect();
    assert!(
        matches!(sums.as_slice(), [V::Int(1), V::Int(7)]),
        "grp 10 → 1, grp 20 → 7; got {sums:?}"
    );
}

/// Design 226 §5.3 at the PRODUCTION registration boundary: the change-sink cold
/// drain (`Engine::register_query`, wasm `Db::query`) is a `try_*` boundary too.
/// Registering a `sum` over data whose set total is ALREADY outside i64 must fail
/// with the typed error — never return Ok with a `NULL` aggregate cell — and the
/// registration error arm's teardown must clear the overflow state the drain
/// armed, leaving the shared graph healthy for unrelated writes (no per-write
/// fault storm).
#[test]
fn hydrating_an_already_out_of_range_sum_fails_typed_and_teardown_heals_the_graph() {
    let mut g = Graph::new();
    let src = g.add_source(
        SourceSchema::new(vec!["id", "val"], vec![0], vec![(0, true)]),
        vec![r(1, i64::MAX), r(2, 1)].into_iter().map(row).collect(),
    );

    // Register through the production shape: recorded build → change sink → cold
    // drain, exactly as `Engine::register_query` (the catch-based helpers above
    // exercise the testkit path; this pins the change-sink one).
    g.begin_recording();
    let conn = g.connect(src, Some(vec![(0, true)]), None, vec![]);
    let storage = g.alloc_storage();
    let reduce = g.add_reduce(Reduce::sum(conn, storage, 1));
    g.set_conn_output(
        conn,
        OutEdge {
            node: reduce,
            port: Port::Single,
        },
    );
    let sink = g.add_change_sink(reduce);
    g.set_sink_edge(reduce, sink);
    let err = g
        .try_hydrate_change_sink(sink)
        .expect_err("registering a sum whose set total is already outside i64 must fail typed");
    match err {
        RindleError::UnsupportedValue(msg) => {
            assert!(msg.contains("sum overflow"), "typed message, got: {msg}")
        }
        other => panic!("expected UnsupportedValue, got {other:?}"),
    }

    // The registration error arm tears the partial pipeline down — and with it the
    // overflow state the drain armed: unrelated writes on the shared graph succeed.
    let manifest = g.take_recording();
    g.destroy_pipeline(&manifest);
    g.try_source_push(src, SourceChange::Add(row(r(3, 5))))
        .expect("the graph is not poisoned after the failed registration is torn down");
}
