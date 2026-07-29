#![cfg(feature = "testkit")]

//! Builder lowering of a **precomputed** relationship aggregate
//! (`AGGREGATE-SYNC-DESIGN.md` §3.3) — the normalized-client display path.
//!
//! A reduce-backed `issue { commentCount: count(comments) }` folds the child rows in a
//! `reduce` (`reduce_aggregate_builder.rs`). The normalized client never holds those
//! rows: the server ships the reduce's `(group_key, count)` output as a **synthetic base
//! table**, and the client rewrites the relationship to read it with a plain singular
//! join — `aggregate: Count` **+** `aggregate_precomputed: true`. The view schema (and so
//! the presented scalar) is *identical* to the reduce path; only the dataflow source
//! differs (a base table, not a `reduce`). These tests pin that: the same scalar
//! projection, and the same materialized tree, sourced from a precomputed table.

use rindle::testkit::{ast_view_schema, cleaf, cnode, rel, run_push_test_ast_view, TableSpec};
use rindle::value::{owned_row as row, OwnedValue as V, SourceSchema};
use rindle::{Aggregate, Ast, CorrelatedSubquery, Correlation, SourceChange};

fn issues_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "val"], vec![0], vec![(0, true)])
}
/// The synthetic aggregate table: `[group_key, count]`, keyed/sorted by the group key —
/// exactly the reduce's output shape (`Reduce::count_by` / `agg_relationship_reldef`).
fn agg_schema() -> SourceSchema {
    SourceSchema::new(vec!["issue", "count"], vec![0], vec![(0, true)])
}

/// `issue { commentCount: <precomputed count(comments)> }` — the relationship reads the
/// synthetic `__agg_comments` table (group `issue` ↔ `issue.id`) instead of reducing.
fn issue_precomputed_count_ast() -> Ast {
    Ast {
        table: "issue".into(),
        related: vec![CorrelatedSubquery {
            correlation: Correlation {
                parent_field: vec!["id".into()],
                child_field: vec!["issue".into()],
            },
            subquery: Box::new(Ast {
                table: "__agg_comments".into(),
                alias: Some("commentCount".into()),
                aggregate: Some(Aggregate::Count),
                aggregate_precomputed: true,
                ..Default::default()
            }),
            system: None,
        }],
        ..Default::default()
    }
}

/// issues 1, 2, 3 with the server-computed counts pre-shipped: issue 1 → 2, issue 2 → 1,
/// issue 3 → **absent** (a childless group has no synthetic row → empty → identity 0).
fn sources() -> Vec<TableSpec> {
    vec![
        TableSpec::new(
            "issue",
            issues_schema(),
            vec![
                vec![V::Int(1), V::Int(100)],
                vec![V::Int(2), V::Int(200)],
                vec![V::Int(3), V::Int(300)],
            ],
        ),
        TableSpec::new(
            "__agg_comments",
            agg_schema(),
            vec![vec![V::Int(1), V::Int(2)], vec![V::Int(2), V::Int(1)]],
        ),
    ]
}

// --- view schema (identical projection to the reduce path) -------------------

#[test]
fn precomputed_view_schema_is_the_same_scalar_projection() {
    let schema =
        ast_view_schema(&sources(), &issue_precomputed_count_ast()).expect("view schema derives");
    assert_eq!(schema.relationships.len(), 1);
    let rd = &schema.relationships[0];
    assert_eq!(rd.name.as_ref(), "commentCount");

    // Same synthetic child `[issue, count]` — singular, keyed/sorted by the group column.
    let child = rd
        .child
        .as_deref()
        .expect("aggregate relationship is in-view");
    assert_eq!(
        child.columns.iter().map(|c| &**c).collect::<Vec<_>>(),
        vec!["issue", "count"]
    );
    assert_eq!(child.primary_key, vec![0]);
    assert_eq!(child.sort, vec![(0, true)]);
    assert!(child.singular, "the aggregate child is singular");

    // Same scalar projection: surface `count` (index 1), empty group → identity 0.
    let proj = rd
        .project
        .as_ref()
        .expect("relationship is scalar-projected");
    assert_eq!(proj.col, 1);
    assert!(
        matches!(proj.identity, V::Int(0)),
        "count identity is 0, got {:?}",
        proj.identity
    );
}

// --- dataflow (the server count, read from the synthetic table) --------------

#[test]
fn materializes_the_server_count_per_issue() {
    // No pushes: hydrate and read. Each issue carries the precomputed `[issue, count]`
    // row as a one-row child; the childless issue 3 is empty (→ identity 0 at projection).
    let r =
        run_push_test_ast_view(sources(), &issue_precomputed_count_ast(), vec![], false).unwrap();
    assert_eq!(
        r.initial,
        vec![
            cnode(
                vec![V::Int(1), V::Int(100)],
                vec![(rel(0), vec![cleaf(vec![V::Int(1), V::Int(2)])])]
            ),
            cnode(
                vec![V::Int(2), V::Int(200)],
                vec![(rel(0), vec![cleaf(vec![V::Int(2), V::Int(1)])])]
            ),
            cnode(vec![V::Int(3), V::Int(300)], vec![(rel(0), vec![])]),
        ]
    );
}

#[test]
fn a_server_count_edit_updates_the_projected_value() {
    // The server re-counts issue 1 (2 → 3) and ships an Edit of the synthetic row; the
    // aggregate child edits in place (the group key is the PK, so this is a value-only edit).
    let r = run_push_test_ast_view(
        sources(),
        &issue_precomputed_count_ast(),
        vec![(
            "__agg_comments",
            SourceChange::Edit {
                old: row(vec![V::Int(1), V::Int(2)]),
                row: row(vec![V::Int(1), V::Int(3)]),
            },
        )],
        false,
    )
    .unwrap();
    assert_eq!(
        r.data[0],
        cnode(
            vec![V::Int(1), V::Int(100)],
            vec![(rel(0), vec![cleaf(vec![V::Int(1), V::Int(3)])])]
        )
    );
}

#[test]
fn a_new_group_births_the_count_on_a_previously_childless_issue() {
    // Issue 3 had no synthetic row (empty → 0). The server's first count for it arrives as
    // an Add of `[3, 1]` (the reduce's 0 → 1 birth, shipped as a base-table add).
    let r = run_push_test_ast_view(
        sources(),
        &issue_precomputed_count_ast(),
        vec![(
            "__agg_comments",
            SourceChange::Add(row(vec![V::Int(3), V::Int(1)])),
        )],
        false,
    )
    .unwrap();
    assert_eq!(
        r.data[2],
        cnode(
            vec![V::Int(3), V::Int(300)],
            vec![(rel(0), vec![cleaf(vec![V::Int(3), V::Int(1)])])]
        )
    );
}

#[test]
fn a_group_death_returns_the_issue_to_empty() {
    // The last comment leaves issue 2: the server ships a Remove of the synthetic row, and
    // the relationship goes empty again (→ identity 0 at the projection boundary).
    let r = run_push_test_ast_view(
        sources(),
        &issue_precomputed_count_ast(),
        vec![(
            "__agg_comments",
            SourceChange::Remove(row(vec![V::Int(2), V::Int(1)])),
        )],
        false,
    )
    .unwrap();
    assert_eq!(
        r.data[1],
        cnode(vec![V::Int(2), V::Int(200)], vec![(rel(0), vec![])])
    );
}
