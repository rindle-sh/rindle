//! The memory source is now backed by the real COW B+tree, fetched through the
//! backend-agnostic `source_common` seam. `self_join.rs` proves the reentrant
//! graph, but its fetches are constraint-only / forward / no-start. These tests
//! exercise the parts the BTree wiring added: the cursor **seek** (start basis),
//! **reverse** scans, and the **COW commit** (add/delete) + snapshot isolation.

use rindle::change::{Basis, FetchRequest, Start};
use rindle::graph::{Graph, NodeId};
use rindle::value::{owned_row as row, OwnedValue::Int, SourceSchema};
use rindle::SourceChange;

fn schema() -> SourceSchema {
    SourceSchema::new(vec!["id"], vec![0], vec![(0, true)]) // id asc
}

fn r(id: i64) -> Vec<rindle::value::OwnedValue> {
    vec![Int(id)]
}

/// Build a source `[1,2,3,4]` with one (unwired) connection; return its id.
fn source_1234() -> (Graph, NodeId) {
    let mut g = Graph::new();
    let src = g.add_source(
        schema(),
        vec![r(1), r(2), r(3), r(4)].into_iter().map(row).collect(),
    );
    let conn = g.connect(src, Some(schema().sort), None, vec![]);
    (g, conn)
}

fn ids_at(g: &Graph, conn: NodeId, req: FetchRequest) -> Vec<i64> {
    g.fetch(conn, &req)
        .map(|n| match &n.row.col(0).to_owned() {
            Int(i) => *i,
            other => panic!("expected Int id, got {other:?}"),
        })
        .collect()
}

fn start(id: i64, basis: Basis) -> Option<Start> {
    Some(Start {
        row: row(r(id)),
        basis,
    })
}

#[test]
fn forward_scan_in_sort_order() {
    let (g, conn) = source_1234();
    assert_eq!(ids_at(&g, conn, FetchRequest::all()), vec![1, 2, 3, 4]);
}

#[test]
fn reverse_scan_descends() {
    let (g, conn) = source_1234();
    let rev = FetchRequest {
        reverse: true,
        ..Default::default()
    };
    assert_eq!(ids_at(&g, conn, rev), vec![4, 3, 2, 1]);
}

#[test]
fn forward_seek_start_basis() {
    let (g, conn) = source_1234();
    // At ⇒ inclusive `>=`; After ⇒ exclusive `>`.
    let at = FetchRequest {
        start: start(2, Basis::At),
        ..Default::default()
    };
    assert_eq!(ids_at(&g, conn, at), vec![2, 3, 4]);

    let after = FetchRequest {
        start: start(2, Basis::After),
        ..Default::default()
    };
    assert_eq!(ids_at(&g, conn, after), vec![3, 4]);

    // A start beyond the end yields nothing.
    let past = FetchRequest {
        start: start(99, Basis::After),
        ..Default::default()
    };
    assert_eq!(ids_at(&g, conn, past), Vec::<i64>::new());
}

#[test]
fn reverse_seek_start_basis() {
    let (g, conn) = source_1234();
    // Reverse + At ⇒ `<=` descending; reverse + After ⇒ `<` descending.
    let at = FetchRequest {
        start: start(3, Basis::At),
        reverse: true,
        ..Default::default()
    };
    assert_eq!(ids_at(&g, conn, at), vec![3, 2, 1]);

    let after = FetchRequest {
        start: start(3, Basis::After),
        reverse: true,
        ..Default::default()
    };
    assert_eq!(ids_at(&g, conn, after), vec![2, 1]);
}

#[test]
fn commit_add_and_remove_via_cow() {
    let (g, conn) = source_1234();
    // Source-push with an unwired connection still sets the overlay, drains, and
    // commits to the BTree (BTree::add). The new row lands in sort order.
    let src = NodeId::new(0, 0); // add_source was the first node added
    g.source_push(src, SourceChange::Add(row(r(5))));
    assert_eq!(ids_at(&g, conn, FetchRequest::all()), vec![1, 2, 3, 4, 5]);

    // Insert in the middle, too (proves it's sorted, not appended).
    g.source_push(src, SourceChange::Add(row(vec![Int(0)])));
    assert_eq!(
        ids_at(&g, conn, FetchRequest::all()),
        vec![0, 1, 2, 3, 4, 5]
    );

    // Remove (BTree::delete).
    g.source_push(src, SourceChange::Remove(row(r(3))));
    assert_eq!(ids_at(&g, conn, FetchRequest::all()), vec![0, 1, 2, 4, 5]);
}

#[test]
fn fetch_snapshot_is_isolated_from_a_later_commit() {
    let (g, conn) = source_1234();
    let src = NodeId::new(0, 0);

    // Open a fetch stream and consume the first row only.
    let mut stream = g.fetch(conn, &FetchRequest::all());
    let first = stream.next().map(|n| match &n.row.col(0).to_owned() {
        Int(i) => *i,
        _ => panic!(),
    });
    assert_eq!(first, Some(1));

    // Commit a write *while the earlier stream is still alive*.
    g.source_push(src, SourceChange::Add(row(r(5))));

    // The held stream reflects the snapshot it was opened against — no `5`.
    let rest: Vec<i64> = stream
        .map(|n| match &n.row.col(0).to_owned() {
            Int(i) => *i,
            _ => panic!(),
        })
        .collect();
    assert_eq!(rest, vec![2, 3, 4]);

    // A fresh fetch sees the committed write.
    assert_eq!(ids_at(&g, conn, FetchRequest::all()), vec![1, 2, 3, 4, 5]);

    // And the RAII cursor count is balanced back to zero.
    assert_eq!(g.cursors_open(src), 0);
}
