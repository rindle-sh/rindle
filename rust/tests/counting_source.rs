//! `CountingSource` — the `analyze query` emit-counter decorator
//! (`designs-implemented/ANALYZE-QUERY-DESIGN.md` §3.4).
//!
//! Proves the decorator counts exactly the rows a source vends into the pipeline,
//! over the object-safe `Source` seam, on the in-memory backend (the one backend the
//! core crate can exercise without SQLite). The daemon-side differential (emitted vs.
//! d2s oracle) lives in `rindle-replica`/`rindle-sqlite`; this pins the mechanism.

use std::cell::Cell;
use std::rc::Rc;

use rindle::value::OwnedValue as V;
use rindle::{
    build_pipeline, owned_row, table, CountingSource, EmitCounter, Graph, MemorySource,
    SourceSchema,
};

fn issue_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "title"], vec![0], vec![(0, true)])
}

/// Build a `CountingSource(MemorySource)` over `n` rows, hydrate a plain
/// `table("issue")` query, and hand back (rows hydrated, emit count).
fn hydrate_counted(n: i64) -> (usize, u64) {
    let schema = issue_schema();
    let rows = (1..=n)
        .map(|i| owned_row(vec![V::Int(i), V::str(&format!("t{i}"))]))
        .collect();
    let mem = MemorySource::new(schema.clone().into_schema(), rows);

    let emitted: EmitCounter = Rc::new(Cell::new(0));
    let counting = CountingSource::new(Box::new(mem), emitted.clone());

    let mut graph = Graph::new();
    let src_id = graph.add_dyn_source(Box::new(counting));

    let ast = table("issue").build();
    let resolve = |t: &str| (t == "issue").then(|| (src_id, schema.clone()));
    let top = build_pipeline(&mut graph, &ast, &resolve).expect("pipeline builds");
    let sink = graph.add_change_sink(top);
    graph.set_sink_edge(top, sink);
    let initial = graph.try_hydrate_change_sink(sink).unwrap();
    (initial.len(), emitted.get())
}

#[test]
fn counts_every_emitted_row() {
    let (hydrated, emitted) = hydrate_counted(5);
    assert_eq!(hydrated, 5, "5 rows materialize");
    assert_eq!(
        emitted, 5,
        "the decorator tallies exactly the 5 vended rows"
    );
}

#[test]
fn empty_source_emits_nothing() {
    let (hydrated, emitted) = hydrate_counted(0);
    assert_eq!(hydrated, 0);
    assert_eq!(emitted, 0, "no rows vended ⇒ counter stays zero");
}
