#![cfg(feature = "testkit")]

//! Builder / AST lowering of a **relationship aggregate** — `issue { commentCount:
//! count(comments) }` (`REDUCE-DESIGN.md` §9, §10 checklist). The fluent
//! [`Query::count_as`](rindle::query::Query) records `aggregate: Count` on the
//! `related` subquery; the builder lowers it to a grouped **lazy** `reduce`
//! partitioned by the correlation child key, attached to the parent join as a
//! **scalar-projected singular** relationship. These tests pin the three lowering
//! consumers: the AST, the view schema (projection annotation), and the dataflow
//! (the materialized aggregate row per parent, through the production `View`).

use rindle::testkit::{ast_view_schema, cleaf, cnode, rel, run_push_test_ast_view, TableSpec};
use rindle::value::{owned_row as row, OwnedValue as V, SourceSchema};
use rindle::{table, Aggregate, Ast, SourceChange};

fn issues_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "val"], vec![0], vec![(0, true)])
}
fn comments_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "issue", "val"], vec![0], vec![(0, true)])
}

/// `issue { commentCount: count(comments) }` — comment.issue ↔ issue.id.
fn issue_count_ast() -> Ast {
    table("issue")
        .count_as("commentCount", |r| {
            table("comment").r#where("issue", r.col("id"))
        })
        .build()
}

/// issues 1, 2, 3 — group 1 has 2 comments, 2 has 1, 3 has none (empty → identity).
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
            "comment",
            comments_schema(),
            vec![
                vec![V::Int(10), V::Int(1), V::Int(500)],
                vec![V::Int(11), V::Int(1), V::Int(501)],
                vec![V::Int(20), V::Int(2), V::Int(900)],
            ],
        ),
    ]
}

// --- AST ---------------------------------------------------------------------

#[test]
fn count_as_marks_the_subquery_aggregate() {
    let ast = issue_count_ast();
    assert_eq!(ast.related.len(), 1);
    let csq = &ast.related[0];
    assert_eq!(csq.subquery.alias.as_deref(), Some("commentCount"));
    assert_eq!(csq.subquery.aggregate, Some(Aggregate::Count));
    assert_eq!(csq.subquery.table.as_ref(), "comment");
    // The correlation siphoned from `r#where("issue", r.col("id"))`.
    assert_eq!(csq.correlation.parent_field, vec!["id".into()]);
    assert_eq!(csq.correlation.child_field, vec!["issue".into()]);
}

// --- view schema (the scalar-projection annotation) --------------------------

#[test]
fn view_schema_lowers_to_a_scalar_projected_relationship() {
    let schema = ast_view_schema(&sources(), &issue_count_ast()).expect("view schema derives");
    assert_eq!(schema.relationships.len(), 1);
    let rd = &schema.relationships[0];
    assert_eq!(rd.name.as_ref(), "commentCount");

    // The child is the synthetic aggregate row `[issue, count]`, keyed/sorted by the
    // group column and singular (the one-row `.one()` shape the projection unwraps).
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

    // Scalar-projected: surface the `count` column (index 1), empty group → identity 0.
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

// --- dataflow (one aggregate row per parent, through the production View) -----

#[test]
fn materializes_one_count_per_issue() {
    // No pushes: just hydrate and read the materialized tree. Each issue carries its
    // count as a one-row `[issue, count]` child; the childless issue 3 is empty (the
    // empty→identity substitution is the projection's job, not the tree's).
    let r = run_push_test_ast_view(sources(), &issue_count_ast(), vec![], false).unwrap();
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
fn comment_add_edits_the_count_under_its_issue() {
    // Add a comment to issue 1 (2 → 3): the aggregate child edits in place.
    let r = run_push_test_ast_view(
        sources(),
        &issue_count_ast(),
        vec![(
            "comment",
            SourceChange::Add(row(vec![V::Int(12), V::Int(1), V::Int(502)])),
        )],
        false,
    )
    .unwrap();
    assert_eq!(
        r.data,
        vec![
            cnode(
                vec![V::Int(1), V::Int(100)],
                vec![(rel(0), vec![cleaf(vec![V::Int(1), V::Int(3)])])]
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
fn first_comment_births_the_count_on_an_empty_issue() {
    // Issue 3 starts childless; its first comment is a 0 → 1 birth.
    let r = run_push_test_ast_view(
        sources(),
        &issue_count_ast(),
        vec![(
            "comment",
            SourceChange::Add(row(vec![V::Int(30), V::Int(3), V::Int(700)])),
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
fn count_applies_the_child_where_before_reducing() {
    // count(comments WHERE val >= 900): the child filter runs in the reduce's input
    // pipeline, so only comment 20 (issue 2, val 900) is counted. Issues 1 and 3 have
    // no qualifying comment → empty relationship; issue 2 → 1.
    let ast = table("issue")
        .count_as("bigCount", |r| {
            table("comment")
                .r#where("issue", r.col("id"))
                .where_op("val", ">=", 900)
        })
        .build();
    let r = run_push_test_ast_view(sources(), &ast, vec![], false).unwrap();
    assert_eq!(
        r.initial,
        vec![
            cnode(vec![V::Int(1), V::Int(100)], vec![(rel(0), vec![])]),
            cnode(
                vec![V::Int(2), V::Int(200)],
                vec![(rel(0), vec![cleaf(vec![V::Int(2), V::Int(1)])])]
            ),
            cnode(vec![V::Int(3), V::Int(300)], vec![(rel(0), vec![])]),
        ]
    );
}

// --- top-level aggregate: a bare global count now lowers (was rejected) ------

#[test]
fn top_level_global_count_lowers() {
    // Previously `BuildError::Unsupported`; a top-level `count(*)` over the root table
    // now lowers to a global `reduce` feeding the View — one row, value 3 (issues
    // 1,2,3). Full top-level-aggregate coverage is in `tests/top_level_aggregate.rs`.
    let mut ast = table("issue").build();
    ast.aggregate = Some(Aggregate::Count);
    let r = run_push_test_ast_view(sources(), &ast, vec![], false).unwrap();
    assert_eq!(r.initial, vec![cleaf(vec![V::Int(3)])]);
}

// ===========================================================================
// Relationship aggregates: sum(child.col) / avg(child.col)
// ===========================================================================

/// `issue { estTotal: sum(comments.val) }`.
fn issue_sum_ast() -> Ast {
    table("issue")
        .sum_as("estTotal", "val", |r| {
            table("comment").r#where("issue", r.col("id"))
        })
        .build()
}
/// `issue { estAvg: avg(comments.val) }`.
fn issue_avg_ast() -> Ast {
    table("issue")
        .avg_as("estAvg", "val", |r| {
            table("comment").r#where("issue", r.col("id"))
        })
        .build()
}

#[test]
fn sum_as_marks_the_subquery_aggregate() {
    let ast = issue_sum_ast();
    let csq = &ast.related[0];
    assert_eq!(csq.subquery.alias.as_deref(), Some("estTotal"));
    assert_eq!(csq.subquery.aggregate, Some(Aggregate::Sum("val".into())));
}

#[test]
fn sum_view_schema_projects_the_sum_column_with_null_identity() {
    let schema = ast_view_schema(&sources(), &issue_sum_ast()).expect("view schema derives");
    let rd = &schema.relationships[0];
    let child = rd.child.as_deref().expect("in-view");
    // The synthetic aggregate row is `[issue, sum]` (the aggregate column named `sum`).
    assert_eq!(
        child.columns.iter().map(|c| &**c).collect::<Vec<_>>(),
        vec!["issue", "sum"]
    );
    assert!(child.singular);
    let proj = rd.project.as_ref().expect("scalar-projected");
    assert_eq!(proj.col, 1);
    // sum's empty-group identity is NULL (SQL `sum` of no rows), not 0.
    assert!(
        matches!(proj.identity, V::Null),
        "sum identity is NULL, got {:?}",
        proj.identity
    );
}

#[test]
fn avg_view_schema_projects_the_avg_column_with_null_identity() {
    let schema = ast_view_schema(&sources(), &issue_avg_ast()).expect("view schema derives");
    let rd = &schema.relationships[0];
    let child = rd.child.as_deref().expect("in-view");
    assert_eq!(
        child.columns.iter().map(|c| &**c).collect::<Vec<_>>(),
        vec!["issue", "avg"]
    );
    let proj = rd.project.as_ref().expect("scalar-projected");
    assert!(matches!(proj.identity, V::Null));
}

#[test]
fn materializes_one_sum_per_issue() {
    // The synthetic child is `[issue, sum]`: issue 1 → 500+501 = 1001, issue 2 → 900,
    // issue 3 → empty (the empty→NULL identity is the projection's job, not the tree's).
    let r = run_push_test_ast_view(sources(), &issue_sum_ast(), vec![], false).unwrap();
    assert_eq!(
        r.initial,
        vec![
            cnode(
                vec![V::Int(1), V::Int(100)],
                vec![(rel(0), vec![cleaf(vec![V::Int(1), V::Int(1001)])])]
            ),
            cnode(
                vec![V::Int(2), V::Int(200)],
                vec![(rel(0), vec![cleaf(vec![V::Int(2), V::Int(900)])])]
            ),
            cnode(vec![V::Int(3), V::Int(300)], vec![(rel(0), vec![])]),
        ]
    );
}

#[test]
fn comment_add_moves_the_sum_under_its_issue() {
    // Add a comment (val 502) to issue 1: sum 1001 → 1503.
    let r = run_push_test_ast_view(
        sources(),
        &issue_sum_ast(),
        vec![(
            "comment",
            SourceChange::Add(row(vec![V::Int(12), V::Int(1), V::Int(502)])),
        )],
        false,
    )
    .unwrap();
    assert_eq!(
        r.data[0],
        cnode(
            vec![V::Int(1), V::Int(100)],
            vec![(rel(0), vec![cleaf(vec![V::Int(1), V::Int(1503)])])]
        )
    );
}

#[test]
fn comment_value_edit_moves_the_sum() {
    // Editing comment 10's val 500 → 600 within issue 1 keeps the child count but moves
    // the sum 1001 → 1101 (the case a `count` aggregate would not observe).
    let r = run_push_test_ast_view(
        sources(),
        &issue_sum_ast(),
        vec![(
            "comment",
            SourceChange::Edit {
                old: row(vec![V::Int(10), V::Int(1), V::Int(500)]),
                row: row(vec![V::Int(10), V::Int(1), V::Int(600)]),
            },
        )],
        false,
    )
    .unwrap();
    assert_eq!(
        r.data[0],
        cnode(
            vec![V::Int(1), V::Int(100)],
            vec![(rel(0), vec![cleaf(vec![V::Int(1), V::Int(1101)])])]
        )
    );
}

#[test]
fn materializes_one_avg_per_issue() {
    // issue 1 → (500+501)/2 = 500.5; issue 2 → 900.0; issue 3 → empty.
    let r = run_push_test_ast_view(sources(), &issue_avg_ast(), vec![], false).unwrap();
    assert_eq!(
        r.initial,
        vec![
            cnode(
                vec![V::Int(1), V::Int(100)],
                vec![(rel(0), vec![cleaf(vec![V::Int(1), V::Float(500.5)])])]
            ),
            cnode(
                vec![V::Int(2), V::Int(200)],
                vec![(rel(0), vec![cleaf(vec![V::Int(2), V::Float(900.0)])])]
            ),
            cnode(vec![V::Int(3), V::Int(300)], vec![(rel(0), vec![])]),
        ]
    );
}

#[test]
fn unknown_summed_column_is_a_build_error() {
    // A `sum` over a column the child source does not have fails to build.
    let ast = table("issue")
        .sum_as("bogus", "nope", |r| {
            table("comment").r#where("issue", r.col("id"))
        })
        .build();
    assert!(ast_view_schema(&sources(), &ast).is_err());
}
