//! The multi-port chassis proof.
//!
//! `Join` exercised the arena's *single-output* path. The filter sub-graph and
//! the OR fan need two patterns it never did, and **five** operators share them
//! (`FilterStart`, `Filter`, `FanOut`, `FanIn`, `FilterEnd` — and the
//! `FilterProbe` stand-in for `Exists`). This file proves the chassis once:
//!
//!   * **The `begin_filter`/`filter`/`end_filter` lifecycle** — a `FilterOperator`
//!     gates (`filter(node) -> bool`), it does not vend; the one scan lives in
//!     `FilterStart::fetch`, bracketed by begin/end. `end_filter` MUST run even
//!     when the fetch stream is abandoned early — the RAII (`Drop`) contract
//!     (Primitive #2; `07` §6.4). A `FilterProbe` records the calls so we can
//!     assert it.
//!   * **Multi-port wiring** — `FanOut` (1 input, N outputs) replays a change to
//!     every branch; `FanIn` (N inputs, 1 output) collapses what the branches
//!     forward back to exactly one change (the OR dedup, `06` §3.5), including
//!     the Edit reconstruction.
//!
//! All over the same arena + `NodeId` graph, shared `&Graph` borrows only.

use rindle::change::{OutEdge, Port};
use rindle::graph::{Graph, NodeId};
use rindle::value::{owned_row as row, OwnedValue as Value, SourceSchema};
use rindle::{Change, CmpOp, CollectedChange, CompiledPredicate, FetchRequest, Node, SourceChange};

const ID: usize = 0;
const KIND: usize = 1;
const FLAG: usize = 2;

fn schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "kind", "flag"], vec![ID], vec![(ID, true)]) // order by id asc
}

/// `t(id, kind, flag)` row.
fn r(id: i64, kind: &str, flag: i64) -> Vec<Value> {
    vec![Value::Int(id), Value::str(kind), Value::Int(flag)]
}

/// The two distinct OR-branch predicates used throughout: `kind == "x"` and
/// `flag > 10`. Distinct so the fan dedup has something to dedup.
fn pred_kind_x() -> CompiledPredicate {
    CompiledPredicate::Cmp {
        col: KIND,
        op: CmpOp::Eq,
        value: Value::str("x"),
    }
}
fn pred_flag_gt_10() -> CompiledPredicate {
    CompiledPredicate::Cmp {
        col: FLAG,
        op: CmpOp::Gt,
        value: Value::Int(10),
    }
}

/// Collect the `id` column of every node a fetch yields, in stream order.
fn fetch_ids(g: &Graph, node: NodeId) -> Vec<i64> {
    g.fetch(node, &FetchRequest::all())
        .map(|n| match &n.row.col(ID).to_owned() {
            Value::Int(i) => *i,
            other => panic!("expected Int id, got {other:?}"),
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Pipeline builders
// ---------------------------------------------------------------------------

/// `source -> conn -> FilterStart -> <head> -> FilterEnd -> Collector`, with the
/// connection's output edge wired into the FilterStart once the caller builds the
/// chain interior and calls [`Scaffold::finish`].
fn scaffold(initial: Vec<Vec<Value>>) -> Scaffold {
    let mut g = Graph::new();
    let src = g.add_source(schema(), initial.into_iter().map(row).collect());
    let conn = g.connect(src, Some(schema().sort), None, vec![]);
    let fs = g.add_filter_start(conn);
    let fe = g.add_filter_end(fs);
    let coll = g.add_collector(fe);
    Scaffold {
        g,
        src,
        conn,
        fs,
        fe,
        coll,
    }
}

struct Scaffold {
    g: Graph,
    src: NodeId,
    conn: NodeId,
    fs: NodeId,
    fe: NodeId,
    coll: NodeId,
}

impl Scaffold {
    /// Wire `FilterStart -> head`, `FilterEnd -> Collector`, and the conn edge,
    /// completing a chain whose interior (head..FilterEnd) the caller built.
    fn finish(&self, head: NodeId) {
        self.g.set_chain_head(self.fs, head);
        self.g.set_output(self.fe, self.coll);
        self.g.set_conn_output(
            self.conn,
            OutEdge {
                node: self.fs,
                port: Port::Single,
            },
        );
    }
}

// ---------------------------------------------------------------------------
// 1. Plain filter chain (the simplest chassis use)
// ---------------------------------------------------------------------------

#[test]
fn plain_filter_chain_fetch_drops_nonmatching() {
    // source -> FilterStart -> Filter(kind=="x") -> FilterEnd -> Collector
    let mut s = scaffold(vec![r(1, "x", 0), r(2, "y", 0), r(3, "x", 0)]);
    let filter = s.g.add_filter(s.fs, pred_kind_x());
    s.g.set_output(filter, s.fe);
    s.finish(filter);

    // Fetching the FilterEnd delegates to FilterStart's bracketed scan; only the
    // kind=="x" rows survive, in sort order.
    assert_eq!(fetch_ids(&s.g, s.fe), vec![1, 3]);
}

#[test]
fn plain_filter_push_gates_and_splits() {
    let mut s = scaffold(vec![]);
    let filter = s.g.add_filter(s.fs, pred_kind_x());
    s.g.set_output(filter, s.fe);
    s.finish(filter);

    // Passing row forwards; failing row is dropped.
    s.g.push_at(
        s.fs,
        Change::Add(Node::leaf(row(r(1, "x", 0)))),
        Port::Single,
    );
    s.g.push_at(
        s.fs,
        Change::Add(Node::leaf(row(r(2, "y", 0)))),
        Port::Single,
    );
    assert_eq!(
        s.g.collector_changes(s.coll),
        vec![CollectedChange::Add(row(r(1, "x", 0)))]
    );

    // Edit split (`07` §3.4): pred(old)=true, pred(new)=false -> Remove(old).
    s.g.push_at(
        s.fs,
        Change::Edit {
            node: Node::leaf(row(r(1, "y", 0))),
            old: Node::leaf(row(r(1, "x", 0))),
        },
        Port::Single,
    );
    assert_eq!(
        s.g.collector_changes(s.coll),
        vec![
            CollectedChange::Add(row(r(1, "x", 0))),
            CollectedChange::Remove(row(r(1, "x", 0))),
        ]
    );
}

#[test]
fn source_push_drives_filter_chain_end_to_end() {
    // The "wired into the arena" proof on the PUSH path: a real source_push fans
    // out to the connection, which pushes into the FilterStart.
    let mut s = scaffold(vec![]);
    let filter = s.g.add_filter(s.fs, pred_kind_x());
    s.g.set_output(filter, s.fe);
    s.finish(filter);

    s.g.source_push(s.src, SourceChange::Add(row(r(1, "x", 0))));
    s.g.source_push(s.src, SourceChange::Add(row(r(2, "y", 0)))); // filtered out

    assert_eq!(
        s.g.collector_changes(s.coll),
        vec![CollectedChange::Add(row(r(1, "x", 0)))]
    );
}

// ---------------------------------------------------------------------------
// 2. The begin/filter/end lifecycle + the Drop guard (RAII)
// ---------------------------------------------------------------------------

#[test]
fn lifecycle_fires_once_per_scan() {
    // source -> FilterStart -> FilterProbe -> FilterEnd
    let mut s = scaffold(vec![r(1, "x", 0), r(2, "y", 0), r(3, "x", 0)]);
    let probe = s.g.add_filter_probe(s.fs);
    s.g.set_output(probe, s.fe);
    s.finish(probe);

    // Drain a full fetch: begin once, filter once per row (3), end once.
    let rows: Vec<_> = fetch_ids(&s.g, s.fe);
    assert_eq!(rows, vec![1, 2, 3]); // probe passes everything through
    assert_eq!(s.g.probe_counts(probe), (1, 3, 1));
}

#[test]
fn end_filter_runs_on_early_stream_drop() {
    // The RAII contract: abandon the fetch after ONE row; end_filter must still
    // run (begin == end), via the EndFilterGuard living in the stream's closure.
    let mut s = scaffold(vec![r(1, "x", 0), r(2, "y", 0), r(3, "x", 0)]);
    let probe = s.g.add_filter_probe(s.fs);
    s.g.set_output(probe, s.fe);
    s.finish(probe);

    {
        let mut stream = s.g.fetch(s.fe, &FetchRequest::all());
        let _first = stream.next().expect("at least one row");
        // begin ran eagerly; exactly one node has been gated so far; end has NOT
        // run yet (the stream is still alive).
        assert_eq!(s.g.probe_counts(probe), (1, 1, 0));
        // `stream` drops here -> EndFilterGuard::drop -> end_filter.
    }
    assert_eq!(
        s.g.probe_counts(probe),
        (1, 1, 1),
        "end_filter must run on early drop (begin balanced by end)"
    );
}

#[test]
fn end_filter_runs_even_if_stream_never_polled() {
    let mut s = scaffold(vec![r(1, "x", 0)]);
    let probe = s.g.add_filter_probe(s.fs);
    s.g.set_output(probe, s.fe);
    s.finish(probe);

    {
        let _stream = s.g.fetch(s.fe, &FetchRequest::all());
        // begin runs eagerly when the fetch is constructed; nothing filtered.
        assert_eq!(s.g.probe_counts(probe), (1, 0, 0));
    } // dropped unpolled
    assert_eq!(s.g.probe_counts(probe), (1, 0, 1));
}

// ---------------------------------------------------------------------------
// 3. The OR fan: multi-port fetch (OR) + push (dedup + edit reconstruction)
// ---------------------------------------------------------------------------

/// Build the OR pipeline on top of a scaffold:
///   FilterStart -> FanOut -> [ <maybe probe> Filter(kind=="x") -> FanIn,
///                              Filter(flag>10)            -> FanIn ]
///   FanIn -> FilterEnd -> Collector
/// Returns the FanOut, FanIn, and (if `with_probe`) the probe at branch A's head.
fn wire_or(s: &mut Scaffold, with_probe: bool) -> (NodeId, NodeId, Option<NodeId>) {
    let fan_out = s.g.add_fan_out(s.fs);
    let fan_in = s.g.add_fan_in(fan_out);

    // Branch A: optionally led by a probe (to observe begin/end traversing INTO a
    // fan branch), then the kind=="x" filter.
    let (branch_a_head, probe) = if with_probe {
        let probe = s.g.add_filter_probe(fan_out);
        let fa = s.g.add_filter(probe, pred_kind_x());
        s.g.set_output(probe, fa);
        s.g.set_output(fa, fan_in);
        (probe, Some(probe))
    } else {
        let fa = s.g.add_filter(fan_out, pred_kind_x());
        s.g.set_output(fa, fan_in);
        (fa, None)
    };

    // Branch B: flag > 10.
    let branch_b = s.g.add_filter(fan_out, pred_flag_gt_10());
    s.g.set_output(branch_b, fan_in);

    s.g.set_fan(fan_out, vec![branch_a_head, branch_b], fan_in);
    s.g.set_output(fan_in, s.fe);
    s.finish(fan_out);
    (fan_out, fan_in, probe)
}

#[test]
fn or_fan_fetch_keeps_the_union() {
    //   (1,x,0)  -> A passes              -> kept
    //   (5,y,0)  -> neither               -> dropped
    //   (20,y,30)-> B passes (flag 30)    -> kept
    //   (30,x,5) -> A passes (and not B)  -> kept
    let mut s = scaffold(vec![
        r(1, "x", 0),
        r(5, "y", 0),
        r(20, "y", 30),
        r(30, "x", 5),
    ]);
    let _ = wire_or(&mut s, false);
    assert_eq!(fetch_ids(&s.g, s.fe), vec![1, 20, 30]);
}

#[test]
fn lifecycle_traverses_fan_branches() {
    // begin_filter/end_filter must walk INTO each branch (multi-port topology),
    // not just a linear chain. A probe at branch A's head proves it.
    let mut s = scaffold(vec![r(1, "x", 0), r(5, "y", 0), r(30, "x", 5)]);
    let (_fo, _fi, probe) = wire_or(&mut s, true);
    let probe = probe.unwrap();

    let rows = fetch_ids(&s.g, s.fe);
    assert_eq!(rows, vec![1, 30]); // 5,y,0 dropped by both branches
                                   // begin/end fired exactly once across the whole loop, balanced; the probe
                                   // gated every scanned row (3) as branch A's OR term.
    assert_eq!(s.g.probe_counts(probe), (1, 3, 1));
}

#[test]
fn fan_in_dedups_a_doubly_matched_add() {
    // (30,x,30) matches BOTH branches. Without the collapse the view would see
    // the Add twice; the FanIn must emit it exactly ONCE (`06` §3.5).
    let mut s = scaffold(vec![]);
    let _ = wire_or(&mut s, false);

    s.g.push_at(
        s.fs,
        Change::Add(Node::leaf(row(r(30, "x", 30)))),
        Port::Single,
    );
    assert_eq!(
        s.g.collector_changes(s.coll),
        vec![CollectedChange::Add(row(r(30, "x", 30)))],
        "doubly-matched Add must be deduped to a single Add"
    );
}

#[test]
fn fan_in_singly_matched_add_passes_once() {
    // (1,x,0) matches only branch A; still exactly one Add.
    let mut s = scaffold(vec![]);
    let _ = wire_or(&mut s, false);
    s.g.push_at(
        s.fs,
        Change::Add(Node::leaf(row(r(1, "x", 0)))),
        Port::Single,
    );
    assert_eq!(
        s.g.collector_changes(s.coll),
        vec![CollectedChange::Add(row(r(1, "x", 0)))]
    );
}

#[test]
fn fan_in_unmatched_add_is_dropped() {
    let mut s = scaffold(vec![]);
    let _ = wire_or(&mut s, false);
    s.g.push_at(
        s.fs,
        Change::Add(Node::leaf(row(r(5, "y", 0)))),
        Port::Single,
    );
    assert!(s.g.collector_changes(s.coll).is_empty());
}

#[test]
fn fan_in_reconstructs_an_edit_from_add_plus_remove() {
    // The gem: an Edit where the PK (id) is stable but one branch turns it into a
    // Remove and the other into an Add must be RECONSTRUCTED as an Edit
    // (`push-accumulated.ts:202-213`).
    //   old = (7,"x",0):  A=kind=="x" TRUE,  B=flag>10 FALSE
    //   new = (7,"y",20): A=kind=="x" FALSE, B=flag>10 TRUE
    //   branch A: pred(old)&&!pred(new) -> Remove(old)
    //   branch B: !pred(old)&&pred(new) -> Add(new)
    //   collapse(Edit): both survive -> Edit{node:new, old}
    let mut s = scaffold(vec![]);
    let _ = wire_or(&mut s, false);

    s.g.push_at(
        s.fs,
        Change::Edit {
            node: Node::leaf(row(r(7, "y", 20))),
            old: Node::leaf(row(r(7, "x", 0))),
        },
        Port::Single,
    );

    assert_eq!(
        s.g.collector_changes(s.coll),
        vec![CollectedChange::Edit {
            row: row(r(7, "y", 20)),
            old: row(r(7, "x", 0)),
        }],
        "an edit split across branches into Add+Remove must reconstruct the Edit"
    );
}

#[test]
fn fan_in_edit_that_only_one_branch_keeps_emits_a_remove() {
    // old=(7,"x",0): A TRUE, B FALSE ; new=(7,"z",0): A FALSE, B FALSE.
    //   branch A: pred(old)&&!pred(new) -> Remove(old)
    //   branch B: drop
    //   collapse(Edit): only Remove survives -> Remove(old)
    let mut s = scaffold(vec![]);
    let _ = wire_or(&mut s, false);
    s.g.push_at(
        s.fs,
        Change::Edit {
            node: Node::leaf(row(r(7, "z", 0))),
            old: Node::leaf(row(r(7, "x", 0))),
        },
        Port::Single,
    );
    assert_eq!(
        s.g.collector_changes(s.coll),
        vec![CollectedChange::Remove(row(r(7, "x", 0)))]
    );
}

// ---------------------------------------------------------------------------
// 4. Gaps closed after the adversarial review: the Remove paths, the remaining
//    Edit-collapse arms, the plain-Filter split matrix, N>2 / continuation
//    topology, and the two signature safety properties (reentrancy, panic Drop).
// ---------------------------------------------------------------------------

#[test]
fn plain_filter_remove_gates() {
    let mut s = scaffold(vec![]);
    let filter = s.g.add_filter(s.fs, pred_kind_x());
    s.g.set_output(filter, s.fe);
    s.finish(filter);

    // A direct Remove (not an edit-split product): passing row forwards, failing
    // row drops — the `Change::Remove` arms of filter_chain_push.
    s.g.push_at(
        s.fs,
        Change::Remove(Node::leaf(row(r(1, "x", 0)))),
        Port::Single,
    );
    s.g.push_at(
        s.fs,
        Change::Remove(Node::leaf(row(r(2, "y", 0)))),
        Port::Single,
    );
    assert_eq!(
        s.g.collector_changes(s.coll),
        vec![CollectedChange::Remove(row(r(1, "x", 0)))]
    );
}

#[test]
fn plain_filter_edit_covers_all_four_split_arms() {
    let mut s = scaffold(vec![]);
    let filter = s.g.add_filter(s.fs, pred_kind_x());
    s.g.set_output(filter, s.fe);
    s.finish(filter);

    // (pred(old), pred(new)) -> emitted, the full §3.4 table:
    let edit = |old: Vec<Value>, new: Vec<Value>| Change::Edit {
        node: Node::leaf(row(new)),
        old: Node::leaf(row(old)),
    };
    s.g.push_at(s.fs, edit(r(1, "x", 0), r(1, "x", 1)), Port::Single); // (T,T) -> Edit
    s.g.push_at(s.fs, edit(r(2, "y", 0), r(2, "x", 0)), Port::Single); // (F,T) -> Add
    s.g.push_at(s.fs, edit(r(3, "x", 0), r(3, "y", 0)), Port::Single); // (T,F) -> Remove
    s.g.push_at(s.fs, edit(r(4, "y", 0), r(4, "z", 0)), Port::Single); // (F,F) -> drop

    assert_eq!(
        s.g.collector_changes(s.coll),
        vec![
            CollectedChange::Edit {
                row: row(r(1, "x", 1)),
                old: row(r(1, "x", 0)),
            },
            CollectedChange::Add(row(r(2, "x", 0))),
            CollectedChange::Remove(row(r(3, "x", 0))),
        ]
    );
}

#[test]
fn fan_in_dedups_a_doubly_matched_remove() {
    // The Remove analogue of fan_in_dedups_a_doubly_matched_add: a Remove matching
    // BOTH branches must collapse to a single Remove (the ChangeType::Remove arm).
    let mut s = scaffold(vec![]);
    let _ = wire_or(&mut s, false);
    s.g.push_at(
        s.fs,
        Change::Remove(Node::leaf(row(r(30, "x", 30)))),
        Port::Single,
    );
    assert_eq!(
        s.g.collector_changes(s.coll),
        vec![CollectedChange::Remove(row(r(30, "x", 30)))]
    );
}

#[test]
fn fan_in_unmatched_remove_is_dropped() {
    let mut s = scaffold(vec![]);
    let _ = wire_or(&mut s, false);
    s.g.push_at(
        s.fs,
        Change::Remove(Node::leaf(row(r(5, "y", 0)))),
        Port::Single,
    );
    assert!(s.g.collector_changes(s.coll).is_empty());
}

#[test]
fn fan_in_edit_kept_by_branches_supersedes() {
    // old=(7,"x",20), new=(7,"x",30): branch A (kind=="x") and branch B (flag>10)
    // BOTH keep the Edit whole -> two Edits accumulate -> Identity keep-first ->
    // a single Edit (the `if let Some(e) = edit` collapse arm).
    let mut s = scaffold(vec![]);
    let _ = wire_or(&mut s, false);
    s.g.push_at(
        s.fs,
        Change::Edit {
            node: Node::leaf(row(r(7, "x", 30))),
            old: Node::leaf(row(r(7, "x", 20))),
        },
        Port::Single,
    );
    assert_eq!(
        s.g.collector_changes(s.coll),
        vec![CollectedChange::Edit {
            row: row(r(7, "x", 30)),
            old: row(r(7, "x", 20)),
        }]
    );
}

#[test]
fn fan_in_edit_becomes_add_when_both_branches_add() {
    // old=(7,"y",0), new=(7,"x",20): branch A (F,T)->Add, branch B (F,T)->Add.
    // No Remove survives -> the (Some,None) collapse arm -> a single Add(new).
    let mut s = scaffold(vec![]);
    let _ = wire_or(&mut s, false);
    s.g.push_at(
        s.fs,
        Change::Edit {
            node: Node::leaf(row(r(7, "x", 20))),
            old: Node::leaf(row(r(7, "y", 0))),
        },
        Port::Single,
    );
    assert_eq!(
        s.g.collector_changes(s.coll),
        vec![CollectedChange::Add(row(r(7, "x", 20)))]
    );
}

#[test]
fn fan_in_edit_dropped_by_all_branches() {
    // old=(7,"z",0), new=(7,"w",0): every branch drops both old and new -> empty
    // accumulation -> the (None,None) collapse arm -> nothing emitted.
    let mut s = scaffold(vec![]);
    let _ = wire_or(&mut s, false);
    s.g.push_at(
        s.fs,
        Change::Edit {
            node: Node::leaf(row(r(7, "w", 0))),
            old: Node::leaf(row(r(7, "z", 0))),
        },
        Port::Single,
    );
    assert!(s.g.collector_changes(s.coll).is_empty());
}

#[test]
fn or_fan_three_branches_dedups_triple_match() {
    // N>2 branches: a row matching ALL three must still collapse to ONE change
    // (keep-first drops the two later duplicates). Exercises the N-branch
    // accumulate/begin/end/OR loops at a count the 2-branch tests can't.
    let mut s = scaffold(vec![]);
    let fan_out = s.g.add_fan_out(s.fs);
    let fan_in = s.g.add_fan_in(fan_out);
    let a = s.g.add_filter(fan_out, pred_kind_x());
    let b = s.g.add_filter(fan_out, pred_flag_gt_10());
    let c = s.g.add_filter(
        fan_out,
        CompiledPredicate::Cmp {
            col: ID,
            op: CmpOp::Gt,
            value: Value::Int(100),
        },
    );
    s.g.set_output(a, fan_in);
    s.g.set_output(b, fan_in);
    s.g.set_output(c, fan_in);
    s.g.set_fan(fan_out, vec![a, b, c], fan_in);
    s.g.set_output(fan_in, s.fe);
    s.finish(fan_out);

    // (200,"x",50): A(kind) ✓, B(flag>10) ✓, C(id>100) ✓ — all three.
    s.g.push_at(
        s.fs,
        Change::Add(Node::leaf(row(r(200, "x", 50)))),
        Port::Single,
    );
    assert_eq!(
        s.g.collector_changes(s.coll),
        vec![CollectedChange::Add(row(r(200, "x", 50)))]
    );
}

#[test]
fn lifecycle_traverses_post_fan_continuation() {
    // begin/filter/end must reach a stateful link in the POST-fan continuation
    // (past the FanIn), and the `any && cont` short-circuit must skip the
    // continuation for a row no branch passes.
    let mut s = scaffold(vec![r(1, "x", 0), r(5, "y", 0), r(30, "x", 5)]);
    let fan_out = s.g.add_fan_out(s.fs);
    let fan_in = s.g.add_fan_in(fan_out);
    let a = s.g.add_filter(fan_out, pred_kind_x());
    let b = s.g.add_filter(fan_out, pred_flag_gt_10());
    s.g.set_output(a, fan_in);
    s.g.set_output(b, fan_in);
    s.g.set_fan(fan_out, vec![a, b], fan_in);
    // A probe sits in the continuation: FanIn -> probe -> FilterEnd.
    let cont_probe = s.g.add_filter_probe(fan_in);
    s.g.set_output(fan_in, cont_probe);
    s.g.set_output(cont_probe, s.fe);
    s.finish(fan_out);

    assert_eq!(fetch_ids(&s.g, s.fe), vec![1, 30]); // (5,y,0) passes neither branch
                                                    // begin/end once; the continuation probe gated ONLY the two OR-passing rows
                                                    // (1 and 30) — row 5 short-circuited at `any` before reaching `cont`.
    assert_eq!(s.g.probe_counts(cont_probe), (1, 2, 1));
}

#[test]
fn reentrant_fetch_during_push_through_fan_sees_overlay() {
    // The make-or-break property, now through the multi-port chassis: a push in
    // flight through the OR fan, with a downstream sink that reentrantly fetches
    // the SAME pipeline mid-push, must (a) see the in-flight row via the source
    // overlay and (b) NOT hit a BorrowMutError — proving no RefCell borrow (incl.
    // FanOut.outputs, re-borrowed by begin/filter/end during the reentrant fetch)
    // is held across a vend.
    let mut s = scaffold(vec![]);
    let _ = wire_or(&mut s, false);
    s.g.set_collector_fetch_on_push(s.coll, true);

    s.g.source_push(s.src, SourceChange::Add(row(r(30, "x", 30))));

    assert_eq!(
        s.g.collector_changes(s.coll),
        vec![CollectedChange::Add(row(r(30, "x", 30)))]
    );
    let fetched = s.g.collector_fetched(s.coll);
    assert_eq!(
        fetched.len(),
        1,
        "one reentrant fetch, for the one collapsed Add"
    );
    let ids: Vec<i64> = fetched[0]
        .iter()
        .map(|row| match &row.col(ID).to_owned() {
            Value::Int(i) => *i,
            o => panic!("{o:?}"),
        })
        .collect();
    assert_eq!(
        ids,
        vec![30],
        "reentrant fetch saw the in-flight row through the fan"
    );
}

#[test]
fn end_filter_runs_on_panic_unwind_through_stream() {
    // The "(even on panic)" half of the §6.4 Drop contract: a panic unwinding
    // through the live fetch stream still runs end_filter (the guard's Drop fires
    // during unwind). The probe counters live in the arena, so they survive.
    let mut s = scaffold(vec![r(1, "x", 0), r(2, "y", 0)]);
    let probe = s.g.add_filter_probe(s.fs);
    s.g.set_output(probe, s.fe);
    s.finish(probe);

    let g = &s.g;
    let fe = s.fe;
    let prev = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {})); // silence the expected panic print
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut stream = g.fetch(fe, &FetchRequest::all());
        let _first = stream.next().expect("one row");
        panic!("simulated consumer panic mid-iteration");
        // `stream` drops while unwinding -> EndFilterGuard::drop -> end_filter.
    }));
    std::panic::set_hook(prev);

    assert!(result.is_err(), "the panic propagated out");
    assert_eq!(
        s.g.probe_counts(probe),
        (1, 1, 1),
        "end_filter must run even when a panic unwinds through the stream"
    );
}
