//! End-to-end example: reconstruct a live query's `ArrayView` across a (simulated)
//! network hop using **flat changes** (`FLAT-CHANGES-DESIGN.md`).
//!
//! Run it:
//! ```text
//! cargo run --example flat_subscription
//! # ...and to ALSO print the literal JSON wire bytes for each message:
//! cargo run --example flat_subscription --features serde
//! ```
//!
//! The story: a **sender** runs the query `issue { comments }` over an in-memory engine
//! plus a `change_sink`, and a **receiver** — which could be a different process, or a
//! different language entirely — rebuilds the exact nested view tree from a `Hello`
//! handshake plus a stream of `Batch`es. The receiver here is the reference
//! [`rindle::Subscriber`] (a port of the View's `apply_change`); a JS/host receiver would
//! reimplement the same small contract.
//!
//! What it demonstrates:
//!   1. the handshake (`Hello`: epoch + comparator version + the wire schema),
//!   2. a **chunked** hydrate snapshot (paged one top-level entry per frame),
//!   3. incremental transactions (add a comment, edit an issue, remove a comment),
//!   4. the at-most-once safety net: a re-delivered batch is a harmless no-op.

use rindle::change::{OutEdge, Port};
use rindle::value::{owned_row as row, OwnedValue as V, RelDef, RelId, Schema, SourceSchema};
use rindle::{Applied, Graph, NodeId, Publisher, RecvNode, SourceChange, Subscriber};

// --- the query's hierarchical view schema: issue(id, title) { comments(id, issue, body) }
fn comment_schema() -> Schema {
    Schema::new(vec!["id", "issue", "body"], vec![0], vec![(0, true)])
}
fn issue_schema() -> Schema {
    Schema::new(vec!["id", "title"], vec![0], vec![(0, true)])
        .with_relationships(vec![RelDef::related("comments", comment_schema())])
}
// --- the SOURCE schemas (fed to `add_source`); "comments" is slot 0 (RelId(0)).
fn comment_source_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "issue", "body"], vec![0], vec![(0, true)])
}
fn issue_source_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "title"], vec![0], vec![(0, true)])
}
fn issue(id: i64, title: &str) -> Vec<V> {
    vec![V::Int(id), V::str(title)]
}
fn comment(id: i64, issue: i64, body: &str) -> Vec<V> {
    vec![V::Int(id), V::Int(issue), V::str(body)]
}

/// Build the sender pipeline `issue ⟕ comments` ending in a `change_sink`.
/// Returns `(graph, issue_src, comment_src, sink)`.
fn build_sender(issues: &[Vec<V>], comments: &[Vec<V>]) -> (Graph, NodeId, NodeId, NodeId) {
    let mut g = Graph::new();
    let isrc = g.add_source(
        issue_source_schema(),
        issues.iter().cloned().map(row).collect(),
    );
    let csrc = g.add_source(
        comment_source_schema(),
        comments.iter().cloned().map(row).collect(),
    );
    let pconn = g.connect(isrc, Some(vec![(0, true)]), None, vec![]);
    let cconn = g.connect(csrc, Some(vec![(0, true)]), None, vec![]);
    // issue.id (col 0) == comment.issue (col 1), relationship "comments" (slot 0).
    let join = g.add_join_slot(pconn, cconn, vec![0], vec![1], RelId(0));
    g.set_conn_output(
        pconn,
        OutEdge {
            node: join,
            port: Port::JoinParent,
        },
    );
    g.set_conn_output(
        cconn,
        OutEdge {
            node: join,
            port: Port::JoinChild,
        },
    );
    let sink = g.add_change_sink(join);
    g.set_sink_edge(join, sink);
    (g, isrc, csrc, sink)
}

// --- pretty-print the receiver's reconstructed tree, resolving names from the schema ---
fn fmt_val(v: &V) -> String {
    match v {
        V::Absent => "absent".to_string(),
        V::Null => "null".to_string(),
        V::Bool(b) => b.to_string(),
        V::Int(i) => i.to_string(),
        V::Float(f) => f.to_string(),
        V::Str(s) | V::Json(s) => format!("{s:?}"),
    }
}
fn print_node(n: &RecvNode, schema: &Schema, indent: usize) {
    let pad = "  ".repeat(indent);
    let cols: Vec<String> = schema
        .columns
        .iter()
        .enumerate()
        .map(|(i, name)| format!("{name}={}", fmt_val(&n.row.col(i).to_owned())))
        .collect();
    // rc > 1 means a row reached via multiple query paths (annotated for clarity).
    let rc = if n.rc > 1 {
        format!("  (rc={})", n.rc)
    } else {
        String::new()
    };
    println!("{pad}• {}{rc}", cols.join(", "));
    for (slot, kids) in n.rels.iter().enumerate() {
        if let Some(child_schema) = schema.rel_child(rindle::RelId(slot as u32)) {
            if !kids.is_empty() {
                println!("{pad}    {}:", schema.relationships[slot].name);
                for k in kids {
                    print_node(k, child_schema, indent + 3);
                }
            }
        }
    }
}
fn render(label: &str, top: &[RecvNode]) {
    println!("\n┌─ receiver view: {label}");
    for n in top {
        print_node(n, &issue_schema(), 1);
    }
    println!("└─");
}

// Print the JSON wire bytes for a message — only when built `--features serde`
// (the `Publisher`/`Subscriber` API itself is in-process and needs no serde).
#[cfg(feature = "serde")]
fn show_wire<T: serde::Serialize>(what: &str, v: &T) {
    println!("   ↪ wire[{what}] = {}", serde_json::to_string(v).unwrap());
}
#[cfg(not(feature = "serde"))]
fn show_wire<T>(_what: &str, _v: &T) {}

fn main() {
    // ===== SENDER: build the pipeline + change-sink, seed initial data =====
    let issues = [
        issue(1, "Login fails on Safari"),
        issue(2, "Dashboard is slow"),
    ];
    let comments = [
        comment(10, 1, "Can you repro?"),
        comment(11, 1, "Yes, every time"),
        comment(20, 2, "Likely the N+1 query"),
    ];
    let (sender, isrc, csrc, sink) = build_sender(&issues, &comments);
    let mut publisher = Publisher::new(1, &issue_schema()); // epoch 1

    // ===== HANDSHAKE: Hello carries epoch + comparator version + the wire schema =====
    println!("== handshake ==");
    let hello = publisher.hello();
    show_wire("hello", &hello);
    let mut subscriber = Subscriber::open(&hello).expect("comparator + schema accepted");

    // ===== SNAPSHOT: chunked hydrate, one top-level issue per frame =====
    println!("\n== hydrate snapshot (chunked) ==");
    for chunk in publisher.snapshot_chunks(&sender.try_hydrate_change_sink(sink).unwrap(), 1) {
        println!(
            "   chunk #{} (last={}) — {} entr{}",
            chunk.index,
            chunk.last,
            chunk.adds.len(),
            if chunk.adds.len() == 1 { "y" } else { "ies" }
        );
        show_wire("chunk", &chunk);
        subscriber
            .apply_snapshot_chunk(&chunk)
            .expect("chunk in order");
    }
    assert!(subscriber.snapshot_complete()); // render only after the last chunk
    render("after snapshot", subscriber.top());

    // ===== INCREMENTAL TRANSACTIONS =====
    // A helper: push one source change, then ship the resulting batch to the receiver.
    let mut step = |label: &str, src: NodeId, change: SourceChange| {
        sender.source_push(src, change);
        match publisher.commit(&sender.take_sink_changes(sink)) {
            Some(batch) => {
                println!("\n== {label} (batch seq {}) ==", batch.seq);
                show_wire("batch", &batch);
                subscriber.apply(&batch).expect("in-order apply");
                render(label, subscriber.top());
                Some(batch)
            }
            None => {
                println!("\n== {label}: produced no change (empty batch, skipped) ==");
                None
            }
        }
    };

    // 1. A new comment lands on issue 1 → a nested Child(add) under that issue.
    step(
        "add comment 12 to issue 1",
        csrc,
        SourceChange::Add(row(comment(12, 1, "Fixed in #42"))),
    );
    // 2. Edit issue 2's title (sort key unchanged → in-place edit).
    step(
        "edit issue 2 title",
        isrc,
        SourceChange::Edit {
            row: row(issue(2, "Dashboard is slow (P1)")),
            old: row(issue(2, "Dashboard is slow")),
        },
    );
    // 3. Remove a comment from issue 1.
    let last = step(
        "remove comment 10",
        csrc,
        SourceChange::Remove(row(comment(10, 1, "Can you repro?"))),
    );

    // ===== SAFETY: a re-delivered batch is an idempotent no-op =====
    // (rc add/remove are not idempotent, so the protocol discards an already-applied
    // seq rather than double-applying — see FLAT-CHANGES-DESIGN.md §5.4.)
    if let Some(batch) = last {
        println!("\n== re-deliver the last batch (seq {}) ==", batch.seq);
        let outcome = subscriber.apply(&batch).expect("no error");
        assert_eq!(outcome, Applied::Duplicate);
        println!("   → {outcome:?}: discarded, tree unchanged ✓");
    }

    println!(
        "\nDone. The receiver reconstructed the exact view from the wire alone.\n\
         (`tests/flat_oracle.rs` proves this tree matches the engine's own ArrayView.)"
    );
}
