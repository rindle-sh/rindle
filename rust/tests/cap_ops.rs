#![cfg(feature = "testkit")]

//! Operator-level tests for `Cap` (`op/cap.rs` / `cap.ts`, spec `07` §3.8): the
//! unordered, PK-set-membership EXISTS-child limiter — the point-lookup fetch and
//! the push branches (Add / Remove-with-refill / Child / Edit pk-swap). Add/Remove
//! drive the real source push (`run_push_test`, overlay-correct so the refill sees
//! post-change state); Child/Edit (which do no reentrant fetch) use direct
//! `push_at`. Expected outputs anchored to the JS `cap.push.test.ts` oracle.
//!
//! These exercise an **unpartitioned** Cap (the partition keying is shared with
//! `Take` — already covered in `take_ops.rs`); the partitioned path lands tested
//! end-to-end when `build_pipeline` lowers `where`-EXISTS (Cap's only real caller).

use rindle::change::{Change, FetchRequest, Node, OutEdge, Port, SourceChange};
use rindle::graph::{Graph, NodeId};
use rindle::op::Cap;
use rindle::testkit::{
    add, child, cleaf, edit, rel, remove, run_fetch_test, run_push_test, CaughtChange, CaughtNode,
    SourceSpec,
};
use rindle::value::{owned_row as row, OwnedValue as V, SourceSchema};

// row = (id PK, text); the input conn is ordered by id so the refill scan ("first
// untracked row") is deterministic (Cap itself is order-insensitive).
fn cap_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "text"], vec![0], vec![(0, true)])
}
fn r(id: i64, text: i64) -> Vec<V> {
    vec![V::Int(id), V::Int(text)]
}

fn cap_build(limit: u32) -> impl FnOnce(&mut Graph, &[NodeId]) -> NodeId {
    move |g, srcs| {
        let conn = g.connect(srcs[0], Some(vec![(0, true)]), None, vec![]);
        let storage = g.alloc_storage();
        let cap = g.add_cap(Cap::new(conn, storage, limit, None, vec![0])); // PK = [id]
        g.set_conn_output(
            conn,
            OutEdge {
                node: cap,
                port: Port::Single,
            },
        );
        cap
    }
}

/// Run one source push over `rows` at `limit`, return the caught change stream.
fn push_one(limit: u32, rows: Vec<Vec<V>>, change: SourceChange) -> Vec<CaughtChange> {
    run_push_test(
        vec![SourceSpec::new(cap_schema(), rows)],
        cap_build(limit),
        vec![(0, change)],
        false,
    )
    .pushes
}

/// conn → cap → catch, hydrated. Returns (graph, cap, catch) for `push_at` tests.
fn manual(rows: Vec<Vec<V>>, limit: u32) -> (Graph, NodeId, NodeId) {
    let mut g = Graph::new();
    let src = g.add_source(cap_schema(), rows.into_iter().map(row).collect());
    let conn = g.connect(src, Some(vec![(0, true)]), None, vec![]);
    let storage = g.alloc_storage();
    let cap = g.add_cap(Cap::new(conn, storage, limit, None, vec![0]));
    let catch = g.add_catch(cap, false);
    g.set_conn_output(
        conn,
        OutEdge {
            node: cap,
            port: Port::Single,
        },
    );
    g.set_output(cap, catch);
    let _ = g.catch_fetch(catch, &FetchRequest::all()); // hydrate the cap state
    (g, cap, catch)
}

fn ids(nodes: &[CaughtNode]) -> Vec<i64> {
    nodes
        .iter()
        .map(|n| match &n.row.col(0).to_owned() {
            V::Int(i) => *i,
            other => panic!("expected Int id, got {other:?}"),
        })
        .collect()
}

fn add_c(id: i64, text: i64) -> SourceChange {
    SourceChange::Add(row(r(id, text)))
}
fn remove_c(id: i64, text: i64) -> SourceChange {
    SourceChange::Remove(row(r(id, text)))
}

// --- fetch -----------------------------------------------------------------

#[test]
fn cap_fetch_hydrates_then_point_lookups() {
    // First fetch hydrates (tracks the first 3 PKs); the second uses PK point
    // lookups — both yield the same tracked rows.
    let (g, _cap, catch) = manual(vec![r(1, 10), r(2, 20), r(3, 30), r(4, 40), r(5, 50)], 3);
    // The hydrate above already ran one fetch; run two more to compare paths.
    let again = g.catch_fetch(catch, &FetchRequest::all());
    assert_eq!(ids(&again), vec![1, 2, 3]);
}

#[test]
fn cap_fetch_limit_zero_is_empty() {
    let initial = run_fetch_test(
        vec![SourceSpec::new(cap_schema(), vec![r(1, 10), r(2, 20)])],
        cap_build(0),
    );
    assert!(initial.is_empty());
}

// --- Add -------------------------------------------------------------------

#[test]
fn add_with_room_forwards() {
    // limit 5, only 3 rows tracked: an add appends the pk and forwards.
    assert_eq!(
        push_one(5, vec![r(1, 10), r(2, 20), r(3, 30)], add_c(4, 40)),
        vec![add(cleaf(r(4, 40)))]
    );
}

#[test]
fn add_at_limit_is_dropped() {
    // limit 3, already full (first 3 tracked): a further add is dropped.
    assert_eq!(
        push_one(
            3,
            vec![r(1, 10), r(2, 20), r(3, 30), r(4, 40)],
            add_c(6, 60)
        ),
        vec![]
    );
}

// --- Remove ----------------------------------------------------------------

#[test]
fn remove_untracked_is_dropped() {
    // limit 3 tracks {1,2,3}; removing the untracked id 4 emits nothing.
    assert_eq!(
        push_one(
            3,
            vec![r(1, 10), r(2, 20), r(3, 30), r(4, 40)],
            remove_c(4, 40)
        ),
        vec![]
    );
}

#[test]
fn remove_tracked_refills() {
    // limit 3 tracks {1,2,3}; removing id 2 refills with the first untracked
    // partition row (id 4): Remove then Add(refill).
    assert_eq!(
        push_one(
            3,
            vec![r(1, 10), r(2, 20), r(3, 30), r(4, 40)],
            remove_c(2, 20)
        ),
        vec![remove(cleaf(r(2, 20))), add(cleaf(r(4, 40)))]
    );
}

#[test]
fn remove_tracked_no_refill_shrinks() {
    // Only {1,2,3} exist (== limit 3); removing id 1 has no refill: just the Remove.
    assert_eq!(
        push_one(3, vec![r(1, 10), r(2, 20), r(3, 30)], remove_c(1, 10)),
        vec![remove(cleaf(r(1, 10)))]
    );
}

// --- Child -----------------------------------------------------------------

#[test]
fn child_gates_on_membership() {
    // A Child forwards iff its row's pk is tracked. (Child needs no overlay.)
    let (g, cap, catch) = manual(vec![r(1, 10), r(2, 20), r(3, 30), r(4, 40)], 3); // tracks {1,2,3}
    let child_change = |id: i64| Change::Child {
        node: Node::leaf(row(r(id, 0))),
        rel: rel(0),
        child: Box::new(Change::Add(Node::leaf(row(vec![V::Int(99), V::Int(0)])))),
    };
    g.push_at(cap, child_change(2), Port::Single); // tracked → kept
    g.push_at(cap, child_change(4), Port::Single); // untracked → dropped
    assert_eq!(
        g.catch_pushes(catch),
        vec![child(
            r(2, 0),
            rel(0),
            add(cleaf(vec![V::Int(99), V::Int(0)]))
        )]
    );
}

// --- Edit ------------------------------------------------------------------

#[test]
fn edit_pk_unchanged_forwards() {
    // A tracked row's non-PK edit (text 10 → 11) forwards unchanged.
    assert_eq!(
        push_one(
            3,
            vec![r(1, 10), r(2, 20), r(3, 30)],
            SourceChange::Edit {
                row: row(r(1, 11)),
                old: row(r(1, 10))
            },
        ),
        vec![edit(r(1, 10), r(1, 11))]
    );
}

#[test]
fn edit_pk_changed_swaps_membership() {
    // An edit that changes the tracked PK (id 1 → 10) swaps it in the set and
    // forwards the Edit; afterwards a Child for the NEW pk forwards and one for the
    // OLD pk drops. (Direct push_at: Cap's edit does no reentrant fetch.)
    let (g, cap, catch) = manual(vec![r(1, 10), r(2, 20), r(3, 30)], 3); // tracks {1,2,3}
    g.push_at(
        cap,
        Change::Edit {
            node: Node::leaf(row(r(10, 10))),
            old: Node::leaf(row(r(1, 10))),
        },
        Port::Single,
    );
    let child_change = |id: i64| Change::Child {
        node: Node::leaf(row(r(id, 0))),
        rel: rel(0),
        child: Box::new(Change::Add(Node::leaf(row(vec![V::Int(99), V::Int(0)])))),
    };
    g.push_at(cap, child_change(10), Port::Single); // new pk tracked → kept
    g.push_at(cap, child_change(1), Port::Single); // old pk gone → dropped
    assert_eq!(
        g.catch_pushes(catch),
        vec![
            edit(r(1, 10), r(10, 10)),
            child(r(10, 0), rel(0), add(cleaf(vec![V::Int(99), V::Int(0)]))),
        ]
    );
}

#[test]
fn edit_untracked_is_dropped() {
    // limit 3 tracks {1,2,3}; editing the untracked id 4 emits nothing.
    let (g, cap, catch) = manual(vec![r(1, 10), r(2, 20), r(3, 30), r(4, 40)], 3);
    g.push_at(
        cap,
        Change::Edit {
            node: Node::leaf(row(r(4, 41))),
            old: Node::leaf(row(r(4, 40))),
        },
        Port::Single,
    );
    assert_eq!(g.catch_pushes(catch), vec![]);
}
