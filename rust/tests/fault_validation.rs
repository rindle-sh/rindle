//! WS02.2 — strict change-consistency validation returns a typed `RindleError` in
//! **release** (where the default `debug_assert!` is stripped), so a malformed change
//! producer surfaces an error instead of silently corrupting state. Default-off keeps
//! the hot path unchanged.

use rindle::{
    owned_row, Graph, OutEdge, OwnedRow, OwnedValue, Port, RelDef, RelId, RindleError, Schema,
    SourceChange, SourceSchema,
};

fn schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "v"], vec![0], vec![(0, true)])
}

fn row(id: i64, v: i64) -> OwnedRow {
    owned_row(vec![OwnedValue::Int(id), OwnedValue::Int(v)])
}

fn crow(id: i64, issue: i64, v: i64) -> OwnedRow {
    owned_row(vec![
        OwnedValue::Int(id),
        OwnedValue::Int(issue),
        OwnedValue::Int(v),
    ])
}

#[test]
fn strict_rejects_inconsistent_changes() {
    let mut g = Graph::new();
    let src = g.add_source(schema(), vec![row(1, 10)]);
    g.set_validate_changes(true);
    assert!(g.validate_changes());

    // ADD of an already-present PK.
    let e = g
        .try_source_push(src, SourceChange::Add(row(1, 99)))
        .unwrap_err();
    assert!(
        matches!(e, RindleError::ConsistencyViolation { kind } if kind.contains("ADD")),
        "expected ADD ConsistencyViolation, got {e:?}"
    );

    // REMOVE of an absent PK.
    let e = g
        .try_source_push(src, SourceChange::Remove(row(2, 0)))
        .unwrap_err();
    assert!(
        matches!(e, RindleError::ConsistencyViolation { kind } if kind.contains("REMOVE")),
        "expected REMOVE ConsistencyViolation, got {e:?}"
    );

    // EDIT whose `old` row is absent.
    let e = g
        .try_source_push(
            src,
            SourceChange::Edit {
                row: row(3, 1),
                old: row(3, 0),
            },
        )
        .unwrap_err();
    assert!(
        matches!(e, RindleError::ConsistencyViolation { kind } if kind.contains("EDIT")),
        "expected EDIT ConsistencyViolation, got {e:?}"
    );
}

#[test]
fn strict_allows_a_valid_change() {
    let mut g = Graph::new();
    let src = g.add_source(schema(), vec![row(1, 10)]);
    g.set_validate_changes(true);
    // A consistent ADD (new PK) still succeeds under strict validation.
    assert!(g
        .try_source_push(src, SourceChange::Add(row(2, 20)))
        .is_ok());
}

#[test]
fn strict_rejects_join_key_mutating_child_edit() {
    // issue(id, val) ⟕ comment(id, issue, val) on issue.id (col 0) == comment.issue
    // (col 1). The connections have no split-edit keys, so a child edit that mutates
    // the join key reaches the Join as an Edit (rather than being split at the source)
    // and trips the child-edit join-key consistency check.
    // The hierarchical VIEW schema (slot 0 = "comments"); the SOURCE schema below
    // carries no relationships.
    let issue_view_schema = Schema::new(vec!["id", "val"], vec![0], vec![(0, true)])
        .with_relationships(vec![RelDef::related(
            "comments",
            Schema::new(vec!["id", "issue", "val"], vec![0], vec![(0, true)]),
        )]);
    let issue_schema = SourceSchema::new(vec!["id", "val"], vec![0], vec![(0, true)]);
    let comment_schema = SourceSchema::new(vec!["id", "issue", "val"], vec![0], vec![(0, true)]);

    let mut g = Graph::new();
    let issues = g.add_source(issue_schema, vec![row(1, 100)]);
    let comments = g.add_source(comment_schema, vec![crow(10, 1, 500)]);
    let pconn = g.connect(issues, Some(vec![(0, true)]), None, vec![]);
    let cconn = g.connect(comments, Some(vec![(0, true)]), None, vec![]);
    let join = g.add_join_slot(pconn, cconn, vec![0], vec![1], RelId(0));
    let view = g.add_view(join, issue_view_schema);
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
    g.set_output(join, view);
    g.hydrate(view);

    g.set_validate_changes(true);
    // Edit comment 10's `issue` (col 1 — the join key) from 1 to 2.
    let r = g.try_source_push(
        comments,
        SourceChange::Edit {
            row: crow(10, 2, 500),
            old: crow(10, 1, 500),
        },
    );
    assert!(
        matches!(&r, Err(RindleError::ConsistencyViolation { kind }) if kind.contains("child edit")),
        "expected a child-edit join-key ConsistencyViolation, got {r:?}"
    );
}

#[test]
fn validation_off_by_default() {
    let g = Graph::new();
    assert!(
        !g.validate_changes(),
        "change validation must be off by default (hot-path parity)"
    );
    let mut g = g;
    let src = g.add_source(schema(), vec![row(1, 10)]);
    // A valid change works with validation off (the unchanged hot path).
    assert!(g
        .try_source_push(src, SourceChange::Add(row(2, 20)))
        .is_ok());
}
