#![cfg(feature = "testkit")]

//! Phase 1 of EXISTS-under-OR: **`Change::Child` flows through the filter fan**
//! (`FanOut`/`FanIn` + leaf `Filter`s), the spec-06/07 dataflow that was previously
//! `panic!("…out of scope")`.
//!
//! Two primitives are exercised here on a HAND-WIRED fan (no builder):
//!   * **filter-push of a Child** (`filter-push.ts`): a leaf `Filter` passes a Child
//!     through unchanged iff its predicate holds on the change's node row.
//!   * **CHILD-precedence collapse** (`push-accumulated.ts` CHILD case): a Child
//!     broadcast to N branches collapses to **one** Child when any branch preserved
//!     it (`Identity` keep-first dedup), and is dropped when no branch keeps it.
//!
//! The `Exists`-branch conversion (Child → Add/Remove on a membership flip) and its
//! relationship-preservation are covered by the direct `collapse_accumulated` unit
//! tests in `src/graph.rs` and land in Phase 2 (the builder lowering). A `Catch`
//! sink is used (not `Collector`, which skips Child).

use rindle::change::{OutEdge, Port};
use rindle::graph::{Graph, NodeId};
use rindle::testkit::CaughtChange;
use rindle::value::{owned_row as row, OwnedValue as V, RelId, SourceSchema};
use rindle::{Change, CmpOp, CompiledPredicate, Node};

const ID: usize = 0;
const KIND: usize = 1;
const FLAG: usize = 2;

fn schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "kind", "flag"], vec![ID], vec![(ID, true)])
}
fn r(id: i64, kind: &str, flag: i64) -> Vec<V> {
    vec![V::Int(id), V::str(kind), V::Int(flag)]
}
fn pred_kind_x() -> CompiledPredicate {
    CompiledPredicate::Cmp {
        col: KIND,
        op: CmpOp::Eq,
        value: V::str("x"),
    }
}
fn pred_flag_gt_10() -> CompiledPredicate {
    CompiledPredicate::Cmp {
        col: FLAG,
        op: CmpOp::Gt,
        value: V::Int(10),
    }
}

/// A `Change::Child` over a leaf parent `row`, carrying an `Add(child_row)` under
/// relationship slot 0 (the nested sub-change the fan must preserve verbatim).
fn child_change<'g>(parent: Vec<V>, child_id: i64) -> Change<'g> {
    Change::Child {
        node: Node::leaf(row(parent)),
        rel: RelId(0),
        child: Box::new(Change::Add(Node::leaf(row(vec![
            V::Int(child_id),
            V::str("c"),
            V::Int(0),
        ])))),
    }
}

/// Assert the catch recorded exactly one `Child{row, rel:0, Add(child_id)}`.
fn assert_one_child(g: &Graph, catch: NodeId, parent_id: i64, child_id: i64) {
    let pushes = g.catch_pushes(catch);
    assert_eq!(pushes.len(), 1, "expected exactly one collapsed change");
    match &pushes[0] {
        CaughtChange::Child {
            row: prow,
            rel,
            change,
        } => {
            assert_eq!(int0(prow), parent_id, "parent row");
            assert_eq!(rel.0, 0, "rel slot preserved");
            match change.as_ref() {
                CaughtChange::Add(n) => {
                    assert_eq!(int0(&n.row), child_id, "nested child preserved")
                }
                other => panic!("expected nested Add, got {other:?}"),
            }
        }
        other => panic!("expected a Child change, got {other:?}"),
    }
}

fn int0(row: &rindle::value::OwnedRow) -> i64 {
    match &row.col(0).to_owned() {
        V::Int(i) => *i,
        o => panic!("expected Int col0, got {o:?}"),
    }
}

/// `source -> conn -> FilterStart -> FanOut -> [Filter(kind=x), Filter(flag>10)]
///  -> FanIn -> FilterEnd -> Catch`. Returns (graph, FilterStart, Catch).
fn or_fan() -> (Graph, NodeId, NodeId) {
    let mut g = Graph::new();
    let src = g.add_source(schema(), Vec::new());
    let conn = g.connect(src, Some(schema().sort), None, vec![]);
    let fs = g.add_filter_start(conn);
    let fe = g.add_filter_end(fs);

    let fan_out = g.add_fan_out(fs);
    let fan_in = g.add_fan_in(fan_out);
    let a = g.add_filter(fan_out, pred_kind_x());
    let b = g.add_filter(fan_out, pred_flag_gt_10());
    g.set_output(a, fan_in);
    g.set_output(b, fan_in);
    g.set_fan(fan_out, vec![a, b], fan_in);
    g.set_output(fan_in, fe);

    let catch = g.add_catch(fe, false);
    g.set_chain_head(fs, fan_out);
    g.set_output(fe, catch);
    g.set_conn_output(
        conn,
        OutEdge {
            node: fs,
            port: Port::Single,
        },
    );
    (g, fs, catch)
}

#[test]
fn child_passes_one_branch_collapses_to_one_child() {
    // (1,"x",0): branch A (kind=x) keeps the Child, branch B (flag>10) drops it.
    // The kept Child is emitted exactly once.
    let (g, fs, catch) = or_fan();
    g.push_at(fs, child_change(r(1, "x", 0), 100), Port::Single);
    assert_one_child(&g, catch, 1, 100);
}

#[test]
fn child_matching_both_branches_is_deduped_to_one() {
    // (30,"x",30): BOTH branches keep the Child; CHILD-precedence keep-first dedups
    // to a single Child (without it the view would see the child change twice).
    let (g, fs, catch) = or_fan();
    g.push_at(fs, child_change(r(30, "x", 30), 200), Port::Single);
    assert_one_child(&g, catch, 30, 200);
}

#[test]
fn child_matching_no_branch_is_dropped() {
    // (5,"y",0): neither branch's predicate holds on the parent row → the Child is
    // dropped by every branch → nothing collapses out.
    let (g, fs, catch) = or_fan();
    g.push_at(fs, child_change(r(5, "y", 0), 300), Port::Single);
    assert!(
        g.catch_pushes(catch).is_empty(),
        "child dropped by all branches"
    );
}

#[test]
fn child_through_a_plain_filter_gates_on_node_row() {
    // No fan: source -> FilterStart -> Filter(kind=x) -> FilterEnd -> Catch. A Child
    // passes through iff pred(node.row) (filter-push), carrying its sub-change.
    let mut g = Graph::new();
    let src = g.add_source(schema(), Vec::new());
    let conn = g.connect(src, Some(schema().sort), None, vec![]);
    let fs = g.add_filter_start(conn);
    let fe = g.add_filter_end(fs);
    let filt = g.add_filter(fs, pred_kind_x());
    g.set_output(filt, fe);
    let catch = g.add_catch(fe, false);
    g.set_chain_head(fs, filt);
    g.set_output(fe, catch);
    g.set_conn_output(
        conn,
        OutEdge {
            node: fs,
            port: Port::Single,
        },
    );

    g.push_at(fs, child_change(r(1, "x", 0), 100), Port::Single); // kind=x → passes
    g.push_at(fs, child_change(r(2, "y", 0), 101), Port::Single); // kind=y → dropped
    assert_one_child(&g, catch, 1, 100);
}
