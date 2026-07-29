//! End-to-end example: **loss recovery** in the flat-change subscription protocol
//! (`FLAT-CHANGES-DESIGN.md` §2.3/§5.4).
//!
//! Run it:
//! ```text
//! cargo run --example flat_resync
//! ```
//!
//! Flat changes are deltas against receiver-held state, and the sender is stateless —
//! it cannot re-derive a delta the transport dropped. `rc` add/remove are also not
//! idempotent. So the protocol's contract is: the transport must be ordered + reliable
//! **within an epoch**; if a batch is lost (a `seq` gap), the receiver cannot patch it,
//! and the only repair is a **full re-hydrate under a new epoch**. Stale in-flight
//! batches from the old epoch are then rejected.
//!
//! This example simulates a dropped batch and shows the whole recovery dance.

use rindle::value::{owned_row as row, OwnedValue as V, Schema, SourceSchema};
use rindle::{Graph, NodeId, ProtocolError, Publisher, RecvNode, SourceChange, Subscriber};

// A flat list query: issue(id, title), ordered by id. (No relationships — recovery is
// orthogonal to nesting.) This is the view/wire `Schema` (consumed by `Publisher`).
fn issue_schema() -> Schema {
    Schema::new(vec!["id", "title"], vec![0], vec![(0, true)])
}
// The same table as a SOURCE schema (fed to `add_source`).
fn issue_source_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "title"], vec![0], vec![(0, true)])
}
fn issue(id: i64, title: &str) -> Vec<V> {
    vec![V::Int(id), V::str(title)]
}

/// `source → conn → change_sink`. Returns `(graph, source, sink)`.
fn build_sender(initial: &[Vec<V>]) -> (Graph, NodeId, NodeId) {
    let mut g = Graph::new();
    let src = g.add_source(
        issue_source_schema(),
        initial.iter().cloned().map(row).collect(),
    );
    let conn = g.connect(src, Some(issue_schema().sort), None, vec![]);
    let sink = g.add_change_sink(conn);
    g.set_sink_edge(conn, sink);
    (g, src, sink)
}

fn titles(top: &[RecvNode]) -> Vec<String> {
    top.iter()
        .map(|n| match (n.row.col(0), n.row.col(1)) {
            (rindle::value::Value::Int(id), rindle::value::Value::Str(t)) => {
                format!("{id}:{}", String::from_utf8_lossy(t))
            }
            _ => "?".to_string(),
        })
        .collect()
}

fn main() {
    let (sender, src, sink) = build_sender(&[issue(1, "alpha"), issue(2, "beta")]);

    // ---- Epoch 1 subscription: handshake + snapshot + a couple of good batches ----
    let mut pub1 = Publisher::new(1, &issue_schema());
    let mut sub = Subscriber::open(&pub1.hello()).expect("open epoch 1");
    sub.apply(&pub1.snapshot(&sender.try_hydrate_change_sink(sink).unwrap()))
        .expect("snapshot");
    println!("epoch 1, after snapshot: {:?}", titles(sub.top()));

    // A delivered transaction (seq 1).
    sender.source_push(src, SourceChange::Add(row(issue(3, "gamma"))));
    let b1 = pub1.commit(&sender.take_sink_changes(sink)).unwrap();
    sub.apply(&b1).expect("apply seq 1");
    println!("epoch 1, after seq {}: {:?}", b1.seq, titles(sub.top()));

    // ---- A batch is produced but LOST in transit (never delivered to the receiver) ----
    sender.source_push(src, SourceChange::Add(row(issue(4, "delta"))));
    let _lost = pub1.commit(&sender.take_sink_changes(sink)).unwrap(); // seq 2 — dropped!
    println!(
        "\n‼ batch seq {} was produced but dropped by the transport",
        _lost.seq
    );

    // ---- The NEXT batch arrives, but its seq is beyond what the receiver expects ----
    sender.source_push(
        src,
        SourceChange::Edit {
            row: row(issue(1, "alpha!")),
            old: row(issue(1, "alpha")),
        },
    );
    let after_gap = pub1.commit(&sender.take_sink_changes(sink)).unwrap(); // seq 3
    match sub.apply(&after_gap) {
        Err(ProtocolError::Gap { expected, got }) => {
            println!("✗ receiver detected a GAP (expected seq {expected}, got {got}) — its tree is now stale and CANNOT be patched from deltas");
        }
        other => panic!("expected a gap, got {other:?}"),
    }

    // ---- Recovery: re-subscribe at a NEW epoch with a fresh snapshot of CURRENT state ----
    println!("\n↻ recovering: re-subscribe at a new epoch with a fresh hydrate");
    let mut pub2 = Publisher::new(2, &issue_schema()); // bump the epoch
    let mut sub2 = Subscriber::open(&pub2.hello()).expect("open epoch 2");
    sub2.apply(&pub2.snapshot(&sender.try_hydrate_change_sink(sink).unwrap()))
        .expect("re-snapshot");
    // The fresh snapshot reflects ALL prior changes (gamma, delta, and alpha!).
    println!("epoch 2, after re-hydrate: {:?}", titles(sub2.top()));

    // ---- A stale epoch-1 batch is rejected by the epoch-2 subscriber ----
    match sub2.apply(&after_gap) {
        Err(ProtocolError::EpochMismatch { expected, got }) => {
            println!("✓ stale batch from epoch {got} rejected by the epoch-{expected} subscriber");
        }
        other => panic!("expected an epoch mismatch, got {other:?}"),
    }

    println!("\nRecovered: the receiver is consistent again under epoch 2.");
}
