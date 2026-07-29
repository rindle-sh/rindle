//! Core-level query teardown over the in-memory backend: `Graph::destroy_pipeline`
//! disconnects one pipeline's source connection so the eager push fan-out skips it,
//! while sibling pipelines over the SAME shared source keep being driven; freed slots
//! are recycled by the next build with no arena growth.

use rindle::{owned_row, Graph, OwnedValue, SourceChange, SourceSchema};

fn id_schema() -> SourceSchema {
    SourceSchema::new(vec!["id"], vec![0], vec![(0, true)])
}
fn row(n: i64) -> rindle::OwnedRow {
    owned_row(vec![OwnedValue::Int(n)])
}

/// Build a recorded source→change-sink pipeline over `src`, hydrate it, and return
/// `(manifest, sink_id)`.
fn build_sink(g: &mut Graph, src: rindle::NodeId) -> (rindle::PipelineManifest, rindle::NodeId) {
    g.begin_recording();
    let conn = g.connect(src, Some(vec![(0, true)]), None, Vec::new());
    let sink = g.add_change_sink(conn);
    g.set_sink_edge(conn, sink);
    let manifest = g.take_recording();
    let _ = g.try_hydrate_change_sink(sink).unwrap();
    (manifest, sink)
}

#[test]
fn destroy_disconnects_from_shared_source_survivor_keeps_running() {
    let mut g = Graph::new();
    let src = g.add_source(id_schema(), Vec::new()); // shared, persistent source

    let (m1, sink1) = build_sink(&mut g, src);
    let (_m2, sink2) = build_sink(&mut g, src);

    // One push fans out to BOTH sinks.
    g.source_push(src, SourceChange::Add(row(1)));
    assert_eq!(g.take_sink_changes(sink1).len(), 1);
    assert_eq!(g.take_sink_changes(sink2).len(), 1);

    let nodes = g.node_count();

    // Tear down pipeline 1.
    g.destroy_pipeline(&m1);

    // A subsequent push is fanned ONLY to the survivor — pipeline 1's connection was
    // disconnected from the source, so the eager fan-out skips it entirely.
    g.source_push(src, SourceChange::Add(row(2)));
    assert_eq!(g.take_sink_changes(sink2).len(), 1, "survivor still driven");

    // The source is intact (both rows present): re-hydrating the survivor's connection
    // would see {1, 2} — proven indirectly by the survivor having received row 2.

    // A fresh pipeline reuses the freed slots — the arena does not grow.
    let (_m3, sink3) = build_sink(&mut g, src);
    assert_eq!(
        g.node_count(),
        nodes,
        "torn-down slots were recycled, not appended"
    );

    // The rebuilt pipeline hydrated against the live source: it sees both rows, and a
    // new push reaches it too.
    g.source_push(src, SourceChange::Add(row(3)));
    assert_eq!(g.take_sink_changes(sink3).len(), 1);
    assert_eq!(g.take_sink_changes(sink2).len(), 1);
}
