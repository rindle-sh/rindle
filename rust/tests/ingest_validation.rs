//! WS02.4 — ingest width validation + predicate cross-type non-panic.
//!
//! These guard two data-reachable panics: a wrong-width ingest row (which would
//! later trap at the engine's unchecked column index) and an ordering predicate over
//! a column/literal type mismatch (which used to `panic!` in `compare_values`).
//! Both must surface/skip cleanly — never abort — and must hold in `--release`.

use rindle::{
    create_predicate, owned_row, Graph, Lit, Op, OwnedValue, RindleError, Schema, SimpleCondition,
    SourceSchema, ValuePosition,
};

/// `id` (col 0, PK) + `name` (col 1, a string column) — the pipeline `Schema`
/// (consumed by `create_predicate`).
fn schema() -> Schema {
    Schema::new(vec!["id", "name"], vec![0], vec![(0, true)])
}

/// The same table as a SOURCE schema (fed to `try_add_source`).
fn source_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "name"], vec![0], vec![(0, true)])
}

#[test]
fn try_add_source_rejects_wrong_width_row() {
    let mut g = Graph::new();
    // The schema declares 2 columns; this row carries 1 cell.
    let bad = vec![owned_row(vec![OwnedValue::Int(1)])];
    let err = g.try_add_source(source_schema(), bad).unwrap_err();
    assert!(
        matches!(err, RindleError::SchemaViolation(_)),
        "wrong-width ingest row should be a SchemaViolation, got {err:?}"
    );
}

#[test]
fn try_add_source_accepts_correct_width_rows() {
    let mut g = Graph::new();
    let ok = vec![
        owned_row(vec![OwnedValue::Int(1), OwnedValue::str("a")]),
        owned_row(vec![OwnedValue::Int(2), OwnedValue::str("b")]),
    ];
    assert!(g.try_add_source(source_schema(), ok).is_ok());
}

#[test]
fn predicate_cross_type_ordering_does_not_panic() {
    // `name > 5` — a string column compared against a number literal. A `where`
    // filter must never crash the pipeline on a type mismatch (07 §8.1); before
    // WS02.4 this panicked in `compare_values`.
    let s = schema();
    let cond = SimpleCondition {
        op: Op::Gt,
        left: ValuePosition::Column {
            name: "name".into(),
        },
        right: ValuePosition::Literal {
            value: Lit::Number(5.0),
        },
    };
    let pred = create_predicate(&cond, &s).expect("predicate compiles");
    // A row whose `name` cell is a string — exercises the cross-type compare path.
    let row = owned_row(vec![OwnedValue::Int(1), OwnedValue::str("hello")]);
    // The contract under test is simply: this returns a bool without panicking.
    let _ = pred.eval(&row);

    // And the comparison is deterministic/total: the same operands compare equal to
    // themselves and the evaluation is stable across calls.
    assert_eq!(pred.eval(&row), pred.eval(&row));
}
