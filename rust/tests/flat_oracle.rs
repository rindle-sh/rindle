//! Differential conformance oracle for the **flat-change → reference-receiver**
//! round-trip (`FLAT-CHANGES-DESIGN.md`).
//!
//! For each scenario we stand up TWO identical pipelines:
//!   - graph **A** ends in the production `ArrayView` (the ground truth);
//!   - graph **B** ends in a `change_sink` (the wire source).
//!
//! Both are driven with the same source changes in lockstep. After hydrate and after
//! every transaction we `flatten` graph B's emitted `CaughtChange`s, feed them to the
//! [`Receiver`], and assert the receiver's reconstructed tree equals graph A's real
//! `ArrayView` tree — **rows, multi-path `rc`, and nested relationship slots**. Any
//! divergence between "what the sender materialized" and "what a remote receiver
//! rebuilds from the flat stream" fails the test.

use std::cmp::Ordering;
use std::sync::Arc;

use rindle::change::{OutEdge, Port};
use rindle::graph::{Graph, NodeId};
use rindle::value::{
    compare_values, owned_row as orow, OwnedRow, OwnedValue as V, RelDef, RelId, Schema,
    SourceSchema,
};
use rindle::view::Entry;
use rindle::{
    flatten_all, Applied, ProtocolError, Publisher, Receiver, RecvNode, SnapStatus, SourceChange,
    Subscriber,
};

// ---------------------------------------------------------------------------
// Tree comparison: receiver `RecvNode` tree vs the View's `Entry` tree
// ---------------------------------------------------------------------------

fn row_eq(a: &OwnedRow, b: &OwnedRow) -> bool {
    a.len() == b.len()
        && a.cells()
            .zip(b.cells())
            .all(|(x, y)| compare_values(x, y) == Ordering::Equal)
}

/// Round-trip a schema through the wire form (`Schema -> WireSchema -> Schema`). The
/// oracle builds its `Receiver` from this — so the receiver only ever sees what the
/// `WireSchema` carries, proving (against the live View) that the wire schema is a
/// sufficient reconstruction input (`FLAT-CHANGES-DESIGN.md` §5.2).
fn via_wire(schema: &Schema) -> Schema {
    rindle::to_schema(&rindle::to_wire(schema))
}

/// Assert the receiver list equals the View list at this level, recursing through
/// in-view relationship slots. Gating slots (no child schema) must be empty on both.
fn assert_eq_tree(recv: &[RecvNode], view: &[Arc<Entry>], schema: &Schema, ctx: &str) {
    assert_eq!(recv.len(), view.len(), "{ctx}: list length");
    for (i, (r, v)) in recv.iter().zip(view.iter()).enumerate() {
        assert!(
            row_eq(&r.row, &v.row),
            "{ctx}[{i}]: row mismatch ({:?} vs {:?})",
            r.row,
            v.row
        );
        assert_eq!(r.rc, v.rc, "{ctx}[{i}]: rc mismatch");
        assert_eq!(r.rels.len(), v.rels.len(), "{ctx}[{i}]: slot count");
        for (slot, (rl, vl)) in r.rels.iter().zip(v.rels.iter()).enumerate() {
            match schema.rel_child(RelId(slot as u32)) {
                Some(cs) => {
                    assert_eq_tree(rl, &vl.items, cs, &format!("{ctx}[{i}].rel{slot}"));
                }
                None => {
                    assert!(
                        rl.is_empty(),
                        "{ctx}[{i}].rel{slot}: receiver gating slot non-empty"
                    );
                    assert!(
                        vl.items.is_empty(),
                        "{ctx}[{i}].rel{slot}: view gating slot non-empty"
                    );
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Scenario 1 — flat root list ordered by a NON-PK column (exercises edit-moves)
// ---------------------------------------------------------------------------

// issue(id, val); PK id; sort by (val asc, id asc) so editing `val` MOVES a row.
// The VIEW/wire schema (flat, no relationships).
fn root_schema() -> Schema {
    Schema::new(vec!["id", "val"], vec![0], vec![(1, true), (0, true)])
}
// The same table as a SOURCE schema (fed to `add_source`).
fn root_source_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "val"], vec![0], vec![(1, true), (0, true)])
}
fn irow(id: i64, val: i64) -> Vec<V> {
    vec![V::Int(id), V::Int(val)]
}

/// Build a root-only pipeline `source → conn`; returns `(graph, source, top)`.
fn build_root(initial: &[Vec<V>]) -> (Graph, NodeId, NodeId) {
    let mut g = Graph::new();
    let src = g.add_source(
        root_source_schema(),
        initial.iter().cloned().map(orow).collect(),
    );
    let conn = g.connect(src, Some(root_schema().sort), None, vec![]);
    (g, src, conn)
}

#[test]
fn oracle_root_list_add_move_remove() {
    let initial = vec![irow(1, 30), irow(2, 10), irow(3, 20)];

    // A: View. B: change_sink.
    let (mut gv, src_v, top_v) = build_root(&initial);
    let view = gv.add_view(top_v, root_schema());
    gv.set_sink_edge(top_v, view);
    gv.hydrate(view);

    let (mut gs, src_s, top_s) = build_root(&initial);
    let sink = gs.add_change_sink(top_s);
    gs.set_sink_edge(top_s, sink);

    // Receiver driven by the WIRE-derived schema (proves the WireSchema is sufficient).
    let mut recv = Receiver::new(via_wire(&root_schema()));
    recv.apply_all(&flatten_all(&gs.try_hydrate_change_sink(sink).unwrap()));
    assert_eq_tree(
        recv.top(),
        &gv.view_data(view).items,
        &root_schema(),
        "hydrate",
    );

    // Each closure is one transaction applied to BOTH graphs, then compared.
    let mut txn = |label: &str, changes: Vec<SourceChange>| {
        for c in &changes {
            gv.source_push(src_v, c.clone());
            gs.source_push(src_s, c.clone());
        }
        gv.flush_view(view);
        recv.apply_all(&flatten_all(&gs.take_sink_changes(sink)));
        assert_eq_tree(recv.top(), &gv.view_data(view).items, &root_schema(), label);
    };

    // Add a row that sorts to the front.
    txn("add-front", vec![SourceChange::Add(orow(irow(4, 5)))]);
    // Edit-move: id=1 val 30 → 15 (jumps from last to the middle).
    txn(
        "edit-move",
        vec![SourceChange::Edit {
            row: orow(irow(1, 15)),
            old: orow(irow(1, 30)),
        }],
    );
    // Remove a middle row.
    txn("remove", vec![SourceChange::Remove(orow(irow(3, 20)))]);
    // A multi-change transaction: add + edit-move + remove together.
    txn(
        "multi",
        vec![
            SourceChange::Add(orow(irow(5, 25))),
            SourceChange::Edit {
                row: orow(irow(2, 40)),
                old: orow(irow(2, 10)),
            },
            SourceChange::Remove(orow(irow(4, 5))),
        ],
    );
}

// ---------------------------------------------------------------------------
// Scenario 2 — nested `issue { comments }` (exercises Child add/remove/edit)
// ---------------------------------------------------------------------------

fn comment_schema() -> Schema {
    Schema::new(vec!["id", "issue", "val"], vec![0], vec![(0, true)])
}
fn issue_schema() -> Schema {
    Schema::new(vec!["id", "val"], vec![0], vec![(0, true)])
        .with_relationships(vec![RelDef::related("comments", comment_schema())])
}
// Source-schema counterparts (fed to `add_source`); the "comments" relationship is
// query-derived as slot 0 (RelId(0)).
fn comment_source_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "issue", "val"], vec![0], vec![(0, true)])
}
fn issue_source_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "val"], vec![0], vec![(0, true)])
}
fn issue(id: i64, val: i64) -> Vec<V> {
    vec![V::Int(id), V::Int(val)]
}
fn comment(id: i64, issue: i64, val: i64) -> Vec<V> {
    vec![V::Int(id), V::Int(issue), V::Int(val)]
}

/// Build `issue ⟕ comments` (rel "comments", issue.id == comment.issue). Returns
/// `(graph, issue_src, comment_src, top_join)`.
fn build_nested(issues: &[Vec<V>], comments: &[Vec<V>]) -> (Graph, NodeId, NodeId, NodeId) {
    let mut g = Graph::new();
    let isrc = g.add_source(
        issue_source_schema(),
        issues.iter().cloned().map(orow).collect(),
    );
    let csrc = g.add_source(
        comment_source_schema(),
        comments.iter().cloned().map(orow).collect(),
    );
    let pconn = g.connect(isrc, Some(vec![(0, true)]), None, vec![]);
    let cconn = g.connect(csrc, Some(vec![(0, true)]), None, vec![]);
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
    (g, isrc, csrc, join)
}

#[test]
fn oracle_nested_child_add_remove_edit() {
    let issues = vec![issue(1, 100), issue(2, 200)];
    let comments = vec![comment(10, 1, 5), comment(11, 1, 9), comment(20, 2, 1)];

    let (mut gv, iv, cv, top_v) = build_nested(&issues, &comments);
    let view = gv.add_view(top_v, issue_schema());
    gv.set_sink_edge(top_v, view);
    gv.hydrate(view);

    let (mut gs, is, cs, top_s) = build_nested(&issues, &comments);
    let sink = gs.add_change_sink(top_s);
    gs.set_sink_edge(top_s, sink);

    // Receiver driven by the WIRE-derived schema (proves the WireSchema is sufficient).
    let mut recv = Receiver::new(via_wire(&issue_schema()));
    recv.apply_all(&flatten_all(&gs.try_hydrate_change_sink(sink).unwrap()));
    assert_eq_tree(
        recv.top(),
        &gv.view_data(view).items,
        &issue_schema(),
        "hydrate",
    );

    // One transaction = a list of (source, change) applied to both graphs.
    let mut txn = |label: &str, changes: Vec<(bool, SourceChange)>| {
        // `bool` = is-issue-source (else comment-source).
        for (is_issue, c) in &changes {
            let (sv, ss) = if *is_issue { (iv, is) } else { (cv, cs) };
            gv.source_push(sv, c.clone());
            gs.source_push(ss, c.clone());
        }
        gv.flush_view(view);
        recv.apply_all(&flatten_all(&gs.take_sink_changes(sink)));
        assert_eq_tree(
            recv.top(),
            &gv.view_data(view).items,
            &issue_schema(),
            label,
        );
    };

    // Child Add: a new comment on issue 1.
    txn(
        "child-add",
        vec![(false, SourceChange::Add(orow(comment(12, 1, 7))))],
    );
    // Child Edit (in place — comments sort by id, so val change does not move).
    txn(
        "child-edit",
        vec![(
            false,
            SourceChange::Edit {
                row: orow(comment(10, 1, 50)),
                old: orow(comment(10, 1, 5)),
            },
        )],
    );
    // Child Remove: drop a comment from issue 1.
    txn(
        "child-remove",
        vec![(false, SourceChange::Remove(orow(comment(11, 1, 9))))],
    );
    // Root Add: a new issue with no comments yet.
    txn(
        "root-add",
        vec![(true, SourceChange::Add(orow(issue(3, 300))))],
    );
    // Then a comment for the new issue (child add under a freshly-added parent).
    txn(
        "child-add-new-parent",
        vec![(false, SourceChange::Add(orow(comment(30, 3, 2))))],
    );
    // Root Remove: drop issue 2 (and its comment leaves with it).
    txn(
        "root-remove",
        vec![(true, SourceChange::Remove(orow(issue(2, 200))))],
    );
    // Mixed transaction: add a comment to issue 1 AND add a new issue, together.
    txn(
        "mixed",
        vec![
            (false, SourceChange::Add(orow(comment(13, 1, 3)))),
            (true, SourceChange::Add(orow(issue(4, 400)))),
        ],
    );
}

// ---------------------------------------------------------------------------
// Scenario 3 — the full subscription PROTOCOL end-to-end (Publisher → Subscriber)
// ---------------------------------------------------------------------------

/// Push a list of `(is_issue, change)` to both graphs (drives the nested topology).
fn push_both(
    gv: &Graph,
    iv: NodeId,
    cv: NodeId,
    gs: &Graph,
    is: NodeId,
    cs: NodeId,
    changes: &[(bool, SourceChange)],
) {
    for (is_issue, c) in changes {
        let (sv, ss) = if *is_issue { (iv, is) } else { (cv, cs) };
        gv.source_push(sv, c.clone());
        gs.source_push(ss, c.clone());
    }
}

#[test]
fn oracle_protocol_e2e() {
    let issues = vec![issue(1, 100), issue(2, 200)];
    let comments = vec![comment(10, 1, 5), comment(20, 2, 1)];

    // A: the live ArrayView (ground truth). B: the change_sink driving the protocol.
    let (mut gv, iv, cv, top_v) = build_nested(&issues, &comments);
    let view = gv.add_view(top_v, issue_schema());
    gv.set_sink_edge(top_v, view);
    gv.hydrate(view);

    let (mut gs, is, cs, top_s) = build_nested(&issues, &comments);
    let sink = gs.add_change_sink(top_s);
    gs.set_sink_edge(top_s, sink);

    // Open the subscription (Hello carries the WireSchema) and apply the seq-0 snapshot.
    let mut pubr = Publisher::new(1, &issue_schema());
    let mut sub = Subscriber::open(&pubr.hello()).expect("open");
    sub.apply(&pubr.snapshot(&gs.try_hydrate_change_sink(sink).unwrap()))
        .expect("snapshot");
    assert_eq_tree(
        sub.top(),
        &gv.view_data(view).items,
        &issue_schema(),
        "snapshot",
    );

    // A normal transaction: a child add → a non-empty batch.
    push_both(
        &gv,
        iv,
        cv,
        &gs,
        is,
        cs,
        &[(false, SourceChange::Add(orow(comment(11, 1, 9))))],
    );
    gv.flush_view(view);
    let b = pubr.commit(&gs.take_sink_changes(sink)).expect("non-empty");
    assert_eq!(sub.apply(&b), Ok(Applied::Applied));
    assert_eq_tree(
        sub.top(),
        &gv.view_data(view).items,
        &issue_schema(),
        "child-add",
    );

    // An EMPTY transaction: a comment on a non-existent issue produces no change at the
    // top → no batch, no seq consumed; both trees stay equal.
    push_both(
        &gv,
        iv,
        cv,
        &gs,
        is,
        cs,
        &[(false, SourceChange::Add(orow(comment(50, 99, 1))))],
    );
    gv.flush_view(view);
    assert!(
        pubr.commit(&gs.take_sink_changes(sink)).is_none(),
        "orphan comment → empty batch"
    );
    assert_eq_tree(
        sub.top(),
        &gv.view_data(view).items,
        &issue_schema(),
        "empty-txn",
    );

    // A LOST batch: produce a real batch but never deliver it (simulate transport drop).
    push_both(
        &gv,
        iv,
        cv,
        &gs,
        is,
        cs,
        &[(true, SourceChange::Add(orow(issue(3, 300))))],
    );
    gv.flush_view(view);
    let _lost = pubr.commit(&gs.take_sink_changes(sink)).expect("non-empty"); // dropped

    // The NEXT batch then arrives with a seq beyond what the subscriber expects → Gap.
    push_both(
        &gv,
        iv,
        cv,
        &gs,
        is,
        cs,
        &[(false, SourceChange::Add(orow(comment(12, 1, 7))))],
    );
    gv.flush_view(view);
    let after_gap = pubr.commit(&gs.take_sink_changes(sink)).expect("non-empty");
    assert!(
        matches!(sub.apply(&after_gap), Err(ProtocolError::Gap { .. })),
        "a lost batch must surface as a gap (the subscriber's tree is now stale)"
    );

    // Recovery: re-subscribe at a NEW epoch with a fresh snapshot of the CURRENT state
    // (the sender cannot re-derive the missed delta — full re-hydrate is the only repair).
    let mut pub2 = Publisher::new(2, &issue_schema());
    let mut sub2 = Subscriber::open(&pub2.hello()).expect("reopen");
    sub2.apply(&pub2.snapshot(&gs.try_hydrate_change_sink(sink).unwrap()))
        .expect("re-snapshot");
    assert_eq_tree(
        sub2.top(),
        &gv.view_data(view).items,
        &issue_schema(),
        "rehydrated",
    );

    // The stale epoch-1 batch is rejected by the epoch-2 subscriber.
    assert!(matches!(
        sub2.apply(&after_gap),
        Err(ProtocolError::EpochMismatch { .. })
    ));
}

#[test]
fn oracle_chunked_snapshot_e2e() {
    // Several top-level issues so the snapshot pages into multiple chunks.
    let issues = vec![issue(1, 100), issue(2, 200), issue(3, 300)];
    let comments = vec![comment(10, 1, 5), comment(11, 1, 9), comment(20, 2, 1)];

    let (mut gv, iv, cv, top_v) = build_nested(&issues, &comments);
    let view = gv.add_view(top_v, issue_schema());
    gv.set_sink_edge(top_v, view);
    gv.hydrate(view);

    let (mut gs, is, cs, top_s) = build_nested(&issues, &comments);
    let sink = gs.add_change_sink(top_s);
    gs.set_sink_edge(top_s, sink);

    let mut pubr = Publisher::new(1, &issue_schema());
    let mut sub = Subscriber::open(&pubr.hello()).expect("open");

    // Chunk the hydrate snapshot ONE top-level issue per frame.
    let chunks = pubr.snapshot_chunks(&gs.try_hydrate_change_sink(sink).unwrap(), 1);
    assert!(chunks.len() >= 3, "expected multiple snapshot chunks");
    let n = chunks.len();
    for (i, ch) in chunks.iter().enumerate() {
        let st = sub.apply_snapshot_chunk(ch).expect("chunk");
        if i + 1 == n {
            assert_eq!(st, SnapStatus::Complete);
        } else {
            assert_eq!(st, SnapStatus::Accepted);
            // Mid-snapshot the tree is partial — must not be treated as complete.
            assert!(!sub.snapshot_complete());
        }
    }
    assert!(sub.snapshot_complete());
    assert_eq_tree(
        sub.top(),
        &gv.view_data(view).items,
        &issue_schema(),
        "chunked-snapshot",
    );

    // Incremental commits resume at seq 1 over the chunk-assembled baseline.
    push_both(
        &gv,
        iv,
        cv,
        &gs,
        is,
        cs,
        &[(false, SourceChange::Add(orow(comment(12, 1, 7))))],
    );
    gv.flush_view(view);
    let b = pubr.commit(&gs.take_sink_changes(sink)).expect("non-empty");
    assert_eq!(b.seq, 1);
    assert_eq!(sub.apply(&b), Ok(Applied::Applied));
    assert_eq_tree(
        sub.top(),
        &gv.view_data(view).items,
        &issue_schema(),
        "post-chunk-commit",
    );

    push_both(
        &gv,
        iv,
        cv,
        &gs,
        is,
        cs,
        &[(true, SourceChange::Add(orow(issue(4, 400))))],
    );
    gv.flush_view(view);
    let b2 = pubr.commit(&gs.take_sink_changes(sink)).expect("non-empty");
    assert_eq!(b2.seq, 2);
    assert_eq!(sub.apply(&b2), Ok(Applied::Applied));
    assert_eq_tree(
        sub.top(),
        &gv.view_data(view).items,
        &issue_schema(),
        "post-chunk-commit-2",
    );
}

// ---------------------------------------------------------------------------
// Scenario 4 — multi-path rc>1 WITHOUT hidden junctions (HYDRATE).
//
// "The same track added to a playlist twice": a junction `entry(playlist, track)`
// (PK = both columns) with a duplicate (1,100) row. The memory source keeps the
// duplicate as a multiset on bulk load, so the View rc-counts it (the entry reaches
// rc=2). This drives the receiver's `apply_add` Ok-path (rc += 1) end-to-end during
// snapshot reconstruction and asserts it matches the live ArrayView's rc.
//
// NOTE (coverage scope): incremental mutation of a *duplicated key* is unsupported by
// the memory source (a dup ADD trips a debug_assert), and the only incrementally-
// correct rc>1 source would have been the hidden junction, now removed from the
// engine. So the *incremental* rc paths (remove-at-rc>1, the edit-move
// ghost, the merge-into-existing) are exercised by the receiver UNIT tests
// (`src/flat_receiver.rs`), not here. This scenario covers the reachable e2e case:
// rc>1 established at hydrate.
// ---------------------------------------------------------------------------

fn entry_schema() -> Schema {
    Schema::new(
        vec!["playlist", "track"],
        vec![0, 1],
        vec![(0, true), (1, true)],
    )
}
fn playlist_schema() -> Schema {
    Schema::new(vec!["id"], vec![0], vec![(0, true)])
        .with_relationships(vec![RelDef::related("entries", entry_schema())])
}
// Source-schema counterparts (fed to `add_source`); "entries" is slot 0 (RelId(0)).
fn entry_source_schema() -> SourceSchema {
    SourceSchema::new(
        vec!["playlist", "track"],
        vec![0, 1],
        vec![(0, true), (1, true)],
    )
}
fn playlist_source_schema() -> SourceSchema {
    SourceSchema::new(vec!["id"], vec![0], vec![(0, true)])
}
fn pl(id: i64) -> Vec<V> {
    vec![V::Int(id)]
}
fn entry(playlist: i64, track: i64) -> Vec<V> {
    vec![V::Int(playlist), V::Int(track)]
}

/// Build `playlist ⟕ entries` (rel "entries", playlist.id == entry.playlist).
fn build_playlist(playlists: &[Vec<V>], entries: &[Vec<V>]) -> (Graph, NodeId) {
    let mut g = Graph::new();
    let psrc = g.add_source(
        playlist_source_schema(),
        playlists.iter().cloned().map(orow).collect(),
    );
    let esrc = g.add_source(
        entry_source_schema(),
        entries.iter().cloned().map(orow).collect(),
    );
    let pconn = g.connect(psrc, Some(vec![(0, true)]), None, vec![]);
    let econn = g.connect(esrc, Some(vec![(0, true), (1, true)]), None, vec![]);
    let join = g.add_join_slot(pconn, econn, vec![0], vec![0], RelId(0));
    g.set_conn_output(
        pconn,
        OutEdge {
            node: join,
            port: Port::JoinParent,
        },
    );
    g.set_conn_output(
        econn,
        OutEdge {
            node: join,
            port: Port::JoinChild,
        },
    );
    (g, join)
}

/// The maximum `rc` anywhere in a View tree (to assert rc>1 is genuinely exercised).
fn max_rc(view: &[Arc<Entry>]) -> u32 {
    let mut m = 0;
    for e in view {
        m = m.max(e.rc);
        for rel in e.rels.iter() {
            m = m.max(max_rc(&rel.items));
        }
    }
    m
}

#[test]
fn oracle_rc_multipath_hydrate() {
    let playlists = vec![pl(1)];
    // Track 100 added to the playlist TWICE → the (1,100) entry is rc=2.
    let entries = vec![entry(1, 100), entry(1, 100), entry(1, 200)];

    let (mut gv, top_v) = build_playlist(&playlists, &entries);
    let view = gv.add_view(top_v, playlist_schema());
    gv.set_sink_edge(top_v, view);
    gv.hydrate(view);

    let (mut gs, top_s) = build_playlist(&playlists, &entries);
    let sink = gs.add_change_sink(top_s);
    gs.set_sink_edge(top_s, sink);

    // Reconstruct via the wire-derived schema; must match the View INCLUDING rc.
    let mut recv = Receiver::new(via_wire(&playlist_schema()));
    recv.apply_all(&flatten_all(&gs.try_hydrate_change_sink(sink).unwrap()));

    let v = gv.view_data(view);
    assert!(
        max_rc(&v.items) >= 2,
        "the duplicated junction entry must reach rc>=2 (got {})",
        max_rc(&v.items)
    );
    assert_eq_tree(
        recv.top(),
        &v.items,
        &playlist_schema(),
        "rc-multipath-hydrate",
    );

    // Also assert the duplicate is the (1,100) entry specifically, at rc 2.
    let entries_list = &recv.top()[0].rels[0];
    assert_eq!(entries_list.len(), 2, "two distinct tracks (100 dup, 200)");
    assert_eq!(entries_list[0].rc, 2, "(1,100) appears twice → rc 2");
    assert_eq!(entries_list[1].rc, 1, "(1,200) once → rc 1");
}
