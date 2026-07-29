#![cfg(feature = "testkit")]

//! Operator-level tests for `Skip` (`op/skip.rs` / `skip.ts`, spec `07` §5): the
//! start-bound fetch gate (forward + reverse) and the push gate / Edit split.
//! Wired by hand (conn → skip → catch) so the operator semantics are exercised in
//! isolation; the `build_pipeline` lowering of `start` is covered in `builder.rs`.

use rindle::change::{Basis, Change, FetchRequest, Node, OutEdge, Port, Start};
use rindle::graph::{Graph, NodeId};
use rindle::op::Skip;
use rindle::testkit::{add, cleaf, edit, remove, CaughtNode};
use rindle::value::{owned_row as row, OwnedValue as V, SourceSchema};

// issue(id, val); PK id; sort by id asc.
fn issues_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "val"], vec![0], vec![(0, true)])
}

fn bound(id: i64, basis: Basis) -> Start {
    Start {
        row: row(vec![V::Int(id), V::Null]),
        basis,
    }
}

fn rows_1_4() -> Vec<Vec<V>> {
    vec![
        vec![V::Int(1), V::Int(10)],
        vec![V::Int(2), V::Int(20)],
        vec![V::Int(3), V::Int(30)],
        vec![V::Int(4), V::Int(40)],
    ]
}

/// conn(issues) → skip(bound) → catch. Returns (graph, issues_src, skip, catch).
fn build(rows: Vec<Vec<V>>, bound: Start) -> (Graph, NodeId, NodeId, NodeId) {
    let mut g = Graph::new();
    let src = g.add_source(issues_schema(), rows.into_iter().map(row).collect());
    let conn = g.connect(src, Some(vec![(0, true)]), None, vec![]);
    let skip = g.add_skip(Skip::new(conn, bound));
    let catch = g.add_catch(skip, false);
    g.set_conn_output(
        conn,
        OutEdge {
            node: skip,
            port: Port::Single,
        },
    );
    g.set_output(skip, catch);
    (g, src, skip, catch)
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

// A leaf Add/Remove change for `(id, val)`. Returned as `Change<'_>` with an
// inferred (short) lifetime — a `Change<'static>` would force `push_at`'s `&'g
// self` borrow to `'static`. The `Edit` cases are built inline at the call site.
fn add_c<'g>(id: i64, val: i64) -> Change<'g> {
    Change::Add(Node::leaf(row(vec![V::Int(id), V::Int(val)])))
}
fn remove_c<'g>(id: i64, val: i64) -> Change<'g> {
    Change::Remove(Node::leaf(row(vec![V::Int(id), V::Int(val)])))
}
fn edit_c<'g>(old: (i64, i64), new: (i64, i64)) -> Change<'g> {
    Change::Edit {
        node: Node::leaf(row(vec![V::Int(new.0), V::Int(new.1)])),
        old: Node::leaf(row(vec![V::Int(old.0), V::Int(old.1)])),
    }
}

#[test]
fn skip_push_uses_connection_sort_not_table_sort() {
    // Table schema sort is id ASC, but this connection is score DESC, id ASC.
    // `start after score=40,id=4` should keep score 35 and drop score 45.
    let schema = SourceSchema::new(vec!["id", "owner", "score"], vec![0], vec![(0, true)]);
    let mut g = Graph::new();
    let src = g.add_source(schema, Vec::new());
    let conn = g.connect(src, Some(vec![(2, false), (0, true)]), None, vec![]);
    let skip = g.add_skip(Skip::new(
        conn,
        Start {
            row: row(vec![V::Int(4), V::Null, V::Int(40)]),
            basis: Basis::After,
        },
    ));
    let catch = g.add_catch(skip, false);
    g.set_conn_output(
        conn,
        OutEdge {
            node: skip,
            port: Port::Single,
        },
    );
    g.set_output(skip, catch);

    g.push_at(
        skip,
        Change::Add(Node::leaf(row(vec![V::Int(2), V::Int(1), V::Int(35)]))),
        Port::Single,
    );
    g.push_at(
        skip,
        Change::Add(Node::leaf(row(vec![V::Int(5), V::Int(1), V::Int(45)]))),
        Port::Single,
    );

    assert_eq!(
        g.catch_pushes(catch),
        vec![add(cleaf(vec![V::Int(2), V::Int(1), V::Int(35)]))]
    );
}

#[test]
fn skip_fetch_drops_before_bound_exclusive() {
    // After[2]: the bound row (id 2) and everything before it are dropped.
    let (g, _s, _skip, catch) = build(rows_1_4(), bound(2, Basis::After));
    assert_eq!(ids(&g.catch_fetch(catch, &FetchRequest::all())), vec![3, 4]);
}

#[test]
fn skip_fetch_keeps_bound_when_inclusive() {
    // At[2]: the bound row (id 2) survives, only ids before it are dropped.
    let (g, _s, _skip, catch) = build(rows_1_4(), bound(2, Basis::At));
    assert_eq!(
        ids(&g.catch_fetch(catch, &FetchRequest::all())),
        vec![2, 3, 4]
    );
}

#[test]
fn skip_fetch_reverse_trims_far_end() {
    // A reverse scan with no incoming start pushes no start to the source (it
    // returns [4,3,2,1]); Skip `take_while`s rows still at/after the bound and stops
    // at the first below it (id 2, excluded by After) → [4, 3].
    let (g, _s, _skip, catch) = build(rows_1_4(), bound(2, Basis::After));
    let rev = FetchRequest {
        reverse: true,
        ..FetchRequest::all()
    };
    assert_eq!(ids(&g.catch_fetch(catch, &rev)), vec![4, 3]);
}

#[test]
fn skip_push_gates_add_and_remove() {
    // After[2]: rows with id <= 2 are dropped, id > 2 pass through unchanged.
    let (g, _s, skip, catch) = build(vec![], bound(2, Basis::After));
    g.push_at(skip, add_c(1, 100), Port::Single); // dropped
    g.push_at(skip, add_c(3, 300), Port::Single); // kept
    g.push_at(skip, remove_c(2, 200), Port::Single); // dropped
    g.push_at(skip, remove_c(4, 400), Port::Single); // kept
    assert_eq!(
        g.catch_pushes(catch),
        vec![
            add(cleaf(vec![V::Int(3), V::Int(300)])),
            remove(cleaf(vec![V::Int(4), V::Int(400)])),
        ]
    );
}

#[test]
fn skip_push_splits_edit_across_the_bound() {
    // After[2]: shouldBePresent(id) = id > 2. The four straddle cases of
    // `maybe-split-and-push-edit-change`: (T,T)→Edit, (T,F)→Remove(old),
    // (F,T)→Add(new), (F,F)→dropped.
    let (g, _s, skip, catch) = build(vec![], bound(2, Basis::After));
    g.push_at(skip, edit_c((3, 30), (4, 40)), Port::Single); // (T,T) → Edit
    g.push_at(skip, edit_c((3, 30), (1, 10)), Port::Single); // (T,F) → Remove(old)
    g.push_at(skip, edit_c((1, 10), (5, 50)), Port::Single); // (F,T) → Add(new)
    g.push_at(skip, edit_c((1, 10), (2, 20)), Port::Single); // (F,F) → dropped
    assert_eq!(
        g.catch_pushes(catch),
        vec![
            edit(vec![V::Int(3), V::Int(30)], vec![V::Int(4), V::Int(40)]),
            remove(cleaf(vec![V::Int(3), V::Int(30)])),
            add(cleaf(vec![V::Int(5), V::Int(50)])),
        ]
    );
}
