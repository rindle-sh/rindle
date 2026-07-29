//! The build-gated engine metric counters (the `metrics` feature) actually increment
//! at the `try_source_push` seam. Only compiled/run with `--features metrics`
//! (`cargo test --features metrics`); a no-op otherwise.

#![cfg(feature = "metrics")]

use rindle::{owned_row, Graph, OwnedValue, SourceChange, SourceSchema};

fn id_schema() -> SourceSchema {
    SourceSchema::new(vec!["id"], vec![0], vec![(0, true)])
}
fn row(n: i64) -> rindle::OwnedRow {
    owned_row(vec![OwnedValue::Int(n)])
}

#[test]
fn changes_processed_counts_by_kind() {
    // Deltas off the process-global snapshot (this is the only test in the binary, so
    // nothing else races the registry).
    let before = rindle::metrics::snapshot();

    let mut g = Graph::new();
    let src = g.add_source(id_schema(), Vec::new());
    g.source_push(src, SourceChange::Add(row(1)));
    g.source_push(src, SourceChange::Add(row(2)));
    g.source_push(src, SourceChange::Remove(row(1)));

    let after = rindle::metrics::snapshot();
    assert_eq!(
        after.changes_add - before.changes_add,
        2,
        "two adds counted under kind=add"
    );
    assert_eq!(
        after.changes_remove - before.changes_remove,
        1,
        "one remove counted under kind=remove"
    );
    assert_eq!(
        after.changes_edit - before.changes_edit,
        0,
        "no edits in this run"
    );
}
