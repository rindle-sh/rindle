#![cfg(feature = "testkit")]

//! `.one()` (singular query) — Stage 2 of the `Format` removal.
//!
//! `.one()` is recorded as query *intent* on the AST ([`rindle::Ast::one`]), lowered onto
//! the view [`Schema`]'s `singular` flag by `view_schema`, and presented as a single
//! object / `null` at the result boundary (see `tests/diff_fixtures.rs`). The engine
//! tree stays plural at every level. Root-level `.one()` is covered here; relationship-
//! level `.one()` (which needs a correlated per-parent `limit 1`) is a follow-up.

use rindle::table;
use rindle::testkit::{ast_view_schema, run_fetch_test_ast_view, TableSpec};
use rindle::value::{OwnedValue as V, SourceSchema};

fn issue_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "title"], vec![0], vec![(0, true)])
}

fn issues(n: i64) -> TableSpec {
    let rows = (1..=n)
        .map(|i| vec![V::Int(i), V::str(&format!("t{i}"))])
        .collect();
    TableSpec::new("issue", issue_schema(), rows)
}

#[test]
fn one_sets_ast_flag_and_limit() {
    let ast = table("issue").one().build();
    assert!(ast.one, "`.one()` records singular intent on the AST");
    assert_eq!(ast.limit, Some(1), "`.one()` caps the query to one row");
}

#[test]
fn one_lowers_to_singular_view_schema() {
    let ast = table("issue").one().build();
    let schema = ast_view_schema(&[issues(3)], &ast).expect("view schema derives");
    assert!(schema.singular, "root view schema is singular for `.one()`");
}

#[test]
fn plain_query_view_schema_is_not_singular() {
    let ast = table("issue").build();
    let schema = ast_view_schema(&[issues(3)], &ast).expect("view schema derives");
    assert!(!schema.singular, "a plain query stays plural");
}

#[test]
fn one_materializes_a_single_row() {
    // The `limit = 1` that `.one()` sets caps the materialized tree to the first row by
    // sort (PK-completed) — the engine tree is plural, just bounded to one element.
    let ast = table("issue").one().build();
    let nodes = run_fetch_test_ast_view(vec![issues(3)], &ast).expect("fetch pipeline builds");
    assert_eq!(nodes.len(), 1, "`.one()` materializes exactly one row");
    match &nodes[0].row.col(0).to_owned() {
        V::Int(i) => assert_eq!(*i, 1, "the first row by id"),
        other => panic!("expected an Int id, got {other:?}"),
    }
}
