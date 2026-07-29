#![cfg(feature = "testkit")]

//! Projection support (`PROJECTION-SUPPORT-DESIGN.md`), phase 2: the connection
//! **presence predicate**. A projected query (`.select(..)`) drops a shared row that is
//! missing any column it structurally reads (§3.3), and `filter_push`'s Edit split turns
//! a column **widening** (`Absent → value`) into an `Add` and a **narrowing**
//! (`value → Absent`) into a `Remove` (D-3). A `'*'` query is unaffected (§7).
//!
//! These exercise the engine within one local store (no sync yet); partial rows are
//! simulated with `OwnedValue::Absent` cells in the source.

use rindle::change::SourceChange;
use rindle::table;
use rindle::testkit::{run_fetch_test_ast_view, run_push_walk_ast_view, TableSpec};
use rindle::value::{owned_row, OwnedValue as V, SourceSchema};

fn issue_schema() -> SourceSchema {
    // issue(id, priority); PK id, sorted by id.
    SourceSchema::new(vec!["id", "priority"], vec![0], vec![(0, true)])
}

/// Top-level ids in a materialized view snapshot.
fn ids(nodes: &[rindle::testkit::CaughtNode]) -> Vec<i64> {
    nodes
        .iter()
        .map(|n| match &n.row.col(0).to_owned() {
            V::Int(i) => *i,
            other => panic!("expected Int id, got {other:?}"),
        })
        .collect()
}

#[test]
fn projected_query_drops_rows_missing_a_required_column() {
    // id=2 has an Absent `priority`; selecting `priority` hides it. The `'*'` query keeps
    // every full-width row.
    let rows = vec![
        vec![V::Int(1), V::Int(5)],
        vec![V::Int(2), V::Absent],
        vec![V::Int(3), V::Int(9)],
    ];
    let sources = vec![TableSpec::new("issue", issue_schema(), rows)];

    let projected = table("issue")
        .select("priority")
        .order_by("id", "asc")
        .build();
    let nodes = run_fetch_test_ast_view(sources.clone(), &projected).expect("builds");
    assert_eq!(ids(&nodes), vec![1, 3], "Absent-priority row is dropped");

    let star = table("issue").order_by("id", "asc").build();
    let nodes = run_fetch_test_ast_view(sources, &star).expect("builds");
    assert_eq!(ids(&nodes), vec![1, 2, 3], "'*' keeps all rows");
}

#[test]
fn widening_an_edit_admits_the_row() {
    // Seed id=1 with an Absent `priority` (hidden), then edit it to a real value: the
    // presence predicate's old/new split emits an Add, surfacing the row.
    let sources = vec![TableSpec::new(
        "issue",
        issue_schema(),
        vec![vec![V::Int(1), V::Absent]],
    )];
    let ast = table("issue")
        .select("priority")
        .order_by("id", "asc")
        .build();
    let edit = SourceChange::Edit {
        old: owned_row(vec![V::Int(1), V::Absent]),
        row: owned_row(vec![V::Int(1), V::Int(7)]),
    };
    let (initial, steps) =
        run_push_walk_ast_view(sources, &ast, vec![("issue", edit)]).expect("builds");
    assert_eq!(ids(&initial), Vec::<i64>::new(), "hidden while Absent");
    assert_eq!(ids(&steps[0]), vec![1], "widened in once priority is set");
}

#[test]
fn narrowing_an_edit_removes_the_row() {
    // Seed id=1 with a present `priority` (visible), then edit it to Absent: the split
    // emits a Remove, dropping the row from the projected view.
    let sources = vec![TableSpec::new(
        "issue",
        issue_schema(),
        vec![vec![V::Int(1), V::Int(7)]],
    )];
    let ast = table("issue")
        .select("priority")
        .order_by("id", "asc")
        .build();
    let edit = SourceChange::Edit {
        old: owned_row(vec![V::Int(1), V::Int(7)]),
        row: owned_row(vec![V::Int(1), V::Absent]),
    };
    let (initial, steps) =
        run_push_walk_ast_view(sources, &ast, vec![("issue", edit)]).expect("builds");
    assert_eq!(ids(&initial), vec![1], "visible while present");
    assert_eq!(
        ids(&steps[0]),
        Vec::<i64>::new(),
        "narrowed out once Absent"
    );
}
