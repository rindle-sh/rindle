#![cfg(feature = "testkit")]

//! **Production-shaped multi-EXISTS** — the regression suite for the query-local
//! relationship slot tree (productionization/query-local-relationship-slots.md).
//!
//! Two-or-more EXISTS under a top-level `AND`/`OR` triggers the builder's alias
//! uniquifier (`comments` → `comments_0`, `labels` → `labels_1`, …). Before the
//! slot-tree refactor, the source [`Schema`] had to pre-declare those synthesized
//! names or the build failed with `BuildError::UnknownRelationship("comments_0")`.
//! No real client (e.g. the wasm `schema_from_js`) declares them, so multi-EXISTS was
//! unreachable in production.
//!
//! These tests pin the fix: a **production-shaped** schema — declaring the table's
//! REAL relationship names, or none at all — builds, fetches, and pushes a multi-EXISTS
//! query correctly. The slot layout is derived from the query AST, not the schema.

use rindle::change::SourceChange;
use rindle::testkit::{
    add, cleaf, cnode, rel, run_fetch_test_ast, run_fetch_test_ast_view, run_push_test_ast,
    CaughtNode, TableSpec,
};
use rindle::value::{owned_row as row, OwnedValue as V, SourceSchema};
use rindle::{
    Ast, Condition, CorrelatedSubquery, CorrelatedSubqueryCondition, Correlation, Dir, ExistsOp,
    OrderPart,
};

fn ir(id: i64, x: i64) -> Vec<V> {
    vec![V::Int(id), V::Int(x)]
}
fn chr(id: i64, issue: i64) -> Vec<V> {
    vec![V::Int(id), V::Int(issue)]
}
fn child_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "issueID"], vec![0], vec![(0, true)])
}

/// `EXISTS(alias)` over `table`, correlated on `issue.id == <table>.issueID`.
fn exists(alias: &str, table: &str) -> Condition {
    Condition::CorrelatedSubquery(CorrelatedSubqueryCondition {
        related: CorrelatedSubquery {
            correlation: Correlation {
                parent_field: vec!["id".into()],
                child_field: vec!["issueID".into()],
            },
            subquery: Box::new(Ast {
                table: table.into(),
                alias: Some(alias.into()),
                order_by: vec![OrderPart("id".into(), Dir::Asc)],
                ..Default::default()
            }),
            system: None,
        },
        op: ExistsOp::Exists,
        flip: None,
        scalar: None,
        plan_id: None,
    })
}

fn issue_ast(where_: Condition) -> Ast {
    Ast {
        table: "issue".into(),
        order_by: vec![OrderPart("id".into(), Dir::Asc)],
        r#where: Some(where_),
        ..Default::default()
    }
}

fn sources(
    issue_schema: SourceSchema,
    issues: Vec<Vec<V>>,
    comments: Vec<Vec<V>>,
    labels: Vec<Vec<V>>,
) -> Vec<TableSpec> {
    vec![
        TableSpec::new("issue", issue_schema, issues),
        TableSpec::new("comment", child_schema(), comments),
        TableSpec::new("label", child_schema(), labels),
    ]
}

/// The production-shaped issue schema: the source declares no relationships, so the
/// query-local slot tree mints the `comments_0` / `labels_1` gate slots itself.
fn issue_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "x"], vec![0], vec![(0, true)])
}

/// issue carrying both gating slots (query-local order: EXISTS pre-order → comments=0,
/// labels=1).
fn issue_with(id: i64, x: i64, comments: Vec<Vec<V>>, labels: Vec<Vec<V>>) -> CaughtNode {
    cnode(
        ir(id, x),
        vec![
            (rel(0), comments.into_iter().map(cleaf).collect()),
            (rel(1), labels.into_iter().map(cleaf).collect()),
        ],
    )
}

#[test]
fn and_of_two_exists_builds_and_fetches_against_real_relationship_names() {
    // `where EXISTS(comments) AND EXISTS(labels)` — kept iff the issue has ≥1 comment AND
    // ≥1 label. Pre-refactor this was `UnknownRelationship("comments_0")`.
    let ast = issue_ast(Condition::And {
        conditions: vec![exists("comments", "comment"), exists("labels", "label")],
    });
    let tree = run_fetch_test_ast(
        sources(
            issue_schema(),
            vec![ir(1, 10), ir(2, 20), ir(3, 30)],
            vec![chr(100, 1), chr(101, 2)], // issue 1 + 2 have comments
            vec![chr(200, 1), chr(201, 3)], // issue 1 + 3 have labels
        ),
        &ast,
    )
    .unwrap();
    // Only issue 1 has BOTH; it carries comments@0 + labels@1.
    assert_eq!(
        tree,
        vec![issue_with(1, 10, vec![chr(100, 1)], vec![chr(200, 1)])]
    );
}

#[test]
fn and_of_two_exists_builds_with_no_declared_relationships() {
    // The strongest production-shaped proof: the source schema declares ZERO
    // relationships (a leaf table). The slot tree is derived purely from the query, so
    // the build/fetch still works.
    let bare_issue = SourceSchema::new(vec!["id", "x"], vec![0], vec![(0, true)]);
    let ast = issue_ast(Condition::And {
        conditions: vec![exists("comments", "comment"), exists("labels", "label")],
    });
    let tree = run_fetch_test_ast(
        sources(
            bare_issue,
            vec![ir(1, 10), ir(2, 20)],
            vec![chr(100, 1)],
            vec![chr(200, 1)],
        ),
        &ast,
    )
    .unwrap();
    assert_eq!(
        tree,
        vec![issue_with(1, 10, vec![chr(100, 1)], vec![chr(200, 1)])]
    );
}

#[test]
fn and_of_two_exists_push_flips_membership_on_real_names() {
    // issue 2 has a comment but no label → absent. Adding its first label completes the
    // AND → Add(issue 2) carrying both gating relationships. Exercises the EXISTS gate
    // flip + the query-local slot tagging on the push path.
    let ast = issue_ast(Condition::And {
        conditions: vec![exists("comments", "comment"), exists("labels", "label")],
    });
    let res = run_push_test_ast(
        sources(
            issue_schema(),
            vec![ir(1, 10), ir(2, 20)],
            vec![chr(100, 1), chr(101, 2)], // both issues have comments
            vec![chr(200, 1)],              // only issue 1 has a label initially
        ),
        &ast,
        vec![("label", SourceChange::Add(row(chr(201, 2))))],
        false,
    )
    .unwrap();
    // Initially only issue 1 matches.
    assert_eq!(
        res.initial,
        vec![issue_with(1, 10, vec![chr(100, 1)], vec![chr(200, 1)])]
    );
    // The new label flips issue 2 into the result, carrying comments@0 + labels@1.
    assert_eq!(
        res.pushes,
        vec![add(issue_with(2, 20, vec![chr(101, 2)], vec![chr(201, 2)]))]
    );
}

/// A materialized `related(comments)` on a query whose `where` is `EXISTS(labels)` — a
/// MIXED frame: one in-view materialized slot + one out-of-view gating slot. Query-local
/// order is materialized-first, so comments=slot 0 (in view), labels=slot 1 (gating,
/// excluded from the View tree). Drives the production `ArrayView`, so this pins the
/// View-path slot alignment for a mixed frame (the dataflow Join, the `Exists` gate, and
/// the View materialization must agree on `RelId`s).
fn related_comments() -> CorrelatedSubquery {
    CorrelatedSubquery {
        correlation: Correlation {
            parent_field: vec!["id".into()],
            child_field: vec!["issueID".into()],
        },
        subquery: Box::new(Ast {
            table: "comment".into(),
            alias: Some("comments".into()),
            order_by: vec![OrderPart("id".into(), Dir::Asc)],
            ..Default::default()
        }),
        system: None,
    }
}

#[test]
fn mixed_related_and_exists_frame_through_the_view() {
    let mut ast = issue_ast(exists("labels", "label"));
    ast.related = vec![related_comments()];
    // issue 1: comment + label → in view, carries its comment at slot 0.
    // issue 2: comment, NO label → fails EXISTS(labels) → absent.
    // issue 3: NO comment, has a label → in view, empty comments at slot 0.
    let data = run_fetch_test_ast_view(
        sources(
            issue_schema(),
            vec![ir(1, 10), ir(2, 20), ir(3, 30)],
            vec![chr(100, 1), chr(101, 2)],
            vec![chr(200, 1), chr(202, 3)],
        ),
        &ast,
    )
    .unwrap();
    // The View shows ONLY the in-view materialized `comments` (slot 0); the gating
    // `labels` (slot 1) is out of view.
    assert_eq!(
        data,
        vec![
            cnode(ir(1, 10), vec![(rel(0), vec![cleaf(chr(100, 1))])]),
            cnode(ir(3, 30), vec![(rel(0), vec![])]),
        ]
    );
}

#[test]
fn or_of_two_exists_builds_against_real_relationship_names() {
    // The `OR` shape also uniquifies (`comments_0` / `labels_1`). Kept iff the issue has
    // a comment OR a label.
    let ast = issue_ast(Condition::Or {
        conditions: vec![exists("comments", "comment"), exists("labels", "label")],
    });
    let tree = run_fetch_test_ast(
        sources(
            issue_schema(),
            vec![ir(1, 10), ir(2, 20), ir(3, 30)],
            vec![chr(100, 1)], // issue 1 has a comment
            vec![chr(201, 2)], // issue 2 has a label
        ),
        &ast,
    )
    .unwrap();
    // issue 1 (via comments) and issue 2 (via labels) kept; issue 3 dropped.
    assert_eq!(
        tree,
        vec![
            issue_with(1, 10, vec![chr(100, 1)], vec![]),
            issue_with(2, 20, vec![], vec![chr(201, 2)]),
        ]
    );
}
