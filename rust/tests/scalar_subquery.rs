#![cfg(feature = "testkit")]

//! Build-time scalar-subquery resolution (`SCALAR-SUBQUERY-DESIGN.md`, §9 slice):
//! a `scalar`-flagged correlated `EXISTS`/`NOT EXISTS` whose child binds its full
//! primary key to a literal is folded — before lowering — into a plain parent
//! condition, and the join is deleted. These tests pin the resolver's AST rewrite,
//! the end-to-end result (built with **only the parent source seeded**, proving the
//! child join is gone), and the snapshot contract (§3).

use rindle::scalar::{resolve_scalars, ScalarCatalog, ScalarSource};
use rindle::testkit::{cleaf, run_fetch_test_ast, TableSpec};
use rindle::value::{owned_row as row, OwnedValue as V, Schema, SourceSchema};
use rindle::{
    Ast, BuildError, Condition, CorrelatedSubquery, CorrelatedSubqueryCondition, Correlation,
    ExistsOp, Lit, MemorySource, Op, SimpleCondition, ValuePosition,
};

// issue(id, x) PK id — the parent.
fn issue_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "x"], vec![0], vec![(0, true)])
}
// assignee(id, issueID) PK id — the scalar child (read by the resolver).
fn assignee_schema() -> Schema {
    Schema::new(vec!["id", "issueID"], vec![0], vec![(0, true)])
}
fn ir(id: i64, x: i64) -> Vec<V> {
    vec![V::Int(id), V::Int(x)]
}

/// A catalog backed by in-memory child sources (PK-only `unique_keys`).
struct Cat(Vec<(String, MemorySource)>);
impl ScalarCatalog for Cat {
    fn source(&self, table: &str) -> Option<&dyn ScalarSource> {
        self.0
            .iter()
            .find(|(n, _)| n == table)
            .map(|(_, s)| s as &dyn ScalarSource)
    }
}
fn assignee_catalog(rows: Vec<Vec<V>>) -> Cat {
    let src = MemorySource::new(assignee_schema(), rows.into_iter().map(row).collect());
    Cat(vec![("assignee".to_string(), src)])
}

/// `issue WHERE <op> EXISTS(assignee WHERE assignee.id = key)`, correlated
/// `assignee.issueID = issue.id`, flagged `scalar`.
fn scalar_exists_ast(op: ExistsOp, key: i64) -> Ast {
    child_eq_ast(op, "id", key)
}
/// Same shape, but the child binds `child_col = key` (so a test can bind a non-PK
/// column to exercise the precondition failure).
fn child_eq_ast(op: ExistsOp, child_col: &str, key: i64) -> Ast {
    Ast {
        table: "issue".into(),
        r#where: Some(Condition::CorrelatedSubquery(CorrelatedSubqueryCondition {
            related: CorrelatedSubquery {
                correlation: Correlation {
                    parent_field: vec!["id".into()],
                    child_field: vec!["issueID".into()],
                },
                subquery: Box::new(Ast {
                    table: "assignee".into(),
                    r#where: Some(Condition::Simple(SimpleCondition {
                        op: Op::Eq,
                        left: ValuePosition::Column {
                            name: child_col.into(),
                        },
                        right: ValuePosition::Literal {
                            value: Lit::Number(key as f64),
                        },
                    })),
                    ..Default::default()
                }),
                system: None,
            },
            op,
            flip: None,
            scalar: Some(true),
            plan_id: None,
        })),
        ..Default::default()
    }
}

fn parent_only(rows: Vec<Vec<V>>) -> Vec<TableSpec> {
    vec![TableSpec::new("issue", issue_schema(), rows)]
}
fn simple_eq(op: Op, lit: i64) -> Condition {
    Condition::Simple(SimpleCondition {
        op,
        left: ValuePosition::Column { name: "id".into() },
        right: ValuePosition::Literal {
            value: Lit::Int(lit),
        },
    })
}

// --- the fold (AST rewrite) ------------------------------------------------

#[test]
fn exists_present_folds_to_parent_equality() {
    // assignee 7 → issueID 2 ; EXISTS(assignee.id = 7) ⇒ issue.id = 2.
    let cat = assignee_catalog(vec![vec![V::Int(7), V::Int(2)]]);
    let resolved = resolve_scalars(&scalar_exists_ast(ExistsOp::Exists, 7), &cat).unwrap();
    assert_eq!(resolved.r#where, Some(simple_eq(Op::Eq, 2)));
}

#[test]
fn not_exists_present_folds_to_is_not() {
    // NOT EXISTS over a present unique row ⇒ `field IS NOT literal` (design §5).
    let cat = assignee_catalog(vec![vec![V::Int(7), V::Int(2)]]);
    let resolved = resolve_scalars(&scalar_exists_ast(ExistsOp::NotExists, 7), &cat).unwrap();
    assert_eq!(resolved.r#where, Some(simple_eq(Op::IsNot, 2)));
}

// --- end-to-end (the join is gone) -----------------------------------------

#[test]
fn exists_present_runs_with_only_the_parent_source() {
    // Only `issue` is seeded — if the fold didn't delete the assignee join, the
    // build would fail to resolve the `assignee` table. It runs ⇒ join eliminated.
    let cat = assignee_catalog(vec![vec![V::Int(7), V::Int(2)]]);
    let resolved = resolve_scalars(&scalar_exists_ast(ExistsOp::Exists, 7), &cat).unwrap();
    let out = run_fetch_test_ast(
        parent_only(vec![ir(1, 10), ir(2, 20), ir(3, 30)]),
        &resolved,
    )
    .unwrap();
    assert_eq!(out, vec![cleaf(ir(2, 20))]);
}

#[test]
fn exists_absent_drops_all_parents() {
    // No assignee with id=7 ⇒ EXISTS folds to constant-false ⇒ no issues.
    let cat = assignee_catalog(vec![]);
    let resolved = resolve_scalars(&scalar_exists_ast(ExistsOp::Exists, 7), &cat).unwrap();
    let out = run_fetch_test_ast(parent_only(vec![ir(1, 10), ir(2, 20)]), &resolved).unwrap();
    assert!(out.is_empty());
}

#[test]
fn not_exists_absent_keeps_all_parents() {
    // No assignee with id=7 ⇒ NOT EXISTS folds to constant-true ⇒ every issue.
    let cat = assignee_catalog(vec![]);
    let resolved = resolve_scalars(&scalar_exists_ast(ExistsOp::NotExists, 7), &cat).unwrap();
    let out = run_fetch_test_ast(parent_only(vec![ir(1, 10), ir(2, 20)]), &resolved).unwrap();
    assert_eq!(out, vec![cleaf(ir(1, 10)), cleaf(ir(2, 20))]);
}

#[test]
fn not_exists_present_excludes_the_matched_parent() {
    // assignee 7 → issueID 2 ; NOT EXISTS ⇒ issue.id IS NOT 2 ⇒ keeps 1, 3.
    let cat = assignee_catalog(vec![vec![V::Int(7), V::Int(2)]]);
    let resolved = resolve_scalars(&scalar_exists_ast(ExistsOp::NotExists, 7), &cat).unwrap();
    let out = run_fetch_test_ast(
        parent_only(vec![ir(1, 10), ir(2, 20), ir(3, 30)]),
        &resolved,
    )
    .unwrap();
    assert_eq!(out, vec![cleaf(ir(1, 10)), cleaf(ir(3, 30))]);
}

// --- contract & safety -----------------------------------------------------

#[test]
fn inlined_value_is_a_snapshot() {
    // Resolved against assignee 7 → issueID 2. The child later changing (→ 3) does
    // NOT affect the already-resolved AST: the value is frozen (design §3).
    let cat = assignee_catalog(vec![vec![V::Int(7), V::Int(2)]]);
    let resolved = resolve_scalars(&scalar_exists_ast(ExistsOp::Exists, 7), &cat).unwrap();
    let _later = assignee_catalog(vec![vec![V::Int(7), V::Int(3)]]); // a later world
    let out = run_fetch_test_ast(parent_only(vec![ir(2, 20), ir(3, 30)]), &resolved).unwrap();
    assert_eq!(out, vec![cleaf(ir(2, 20))]); // still 2, not 3 — snapshot, not live
}

#[test]
fn unflagged_subquery_is_left_for_normal_lowering() {
    // No `scalar` flag ⇒ the resolver is a no-op (existing corpus is untouched).
    let mut ast = scalar_exists_ast(ExistsOp::Exists, 7);
    if let Some(Condition::CorrelatedSubquery(c)) = ast.r#where.as_mut() {
        c.scalar = None;
    }
    let cat = assignee_catalog(vec![vec![V::Int(7), V::Int(2)]]);
    assert_eq!(resolve_scalars(&ast, &cat).unwrap(), ast);
}

#[test]
fn binding_a_non_unique_column_is_unsupported() {
    // Bind `issueID` (not a unique key) ⇒ no full unique key bound ⇒ loud error,
    // never a silent live fallback (design §7).
    let ast = child_eq_ast(ExistsOp::Exists, "issueID", 2);
    let cat = assignee_catalog(vec![vec![V::Int(7), V::Int(2)]]);
    assert!(matches!(
        resolve_scalars(&ast, &cat),
        Err(BuildError::Unsupported(_))
    ));
}

#[test]
fn unknown_child_table_is_an_error() {
    let ast = scalar_exists_ast(ExistsOp::Exists, 7);
    let empty = Cat(Vec::new());
    assert!(matches!(
        resolve_scalars(&ast, &empty),
        Err(BuildError::UnknownTable(_))
    ));
}

#[test]
fn extra_filter_beyond_the_unique_key_is_unsupported() {
    // child WHERE `id = 7 AND issueID = 2`: binds the PK *plus* an extra filter.
    // Folding to the correlation value would silently drop `issueID = 2`, so the
    // exact-cover rule rejects it (design §4).
    let mut ast = scalar_exists_ast(ExistsOp::Exists, 7);
    if let Some(Condition::CorrelatedSubquery(c)) = ast.r#where.as_mut() {
        c.related.subquery.r#where = Some(Condition::And {
            conditions: vec![
                Condition::Simple(SimpleCondition {
                    op: Op::Eq,
                    left: ValuePosition::Column { name: "id".into() },
                    right: ValuePosition::Literal {
                        value: Lit::Number(7.0),
                    },
                }),
                Condition::Simple(SimpleCondition {
                    op: Op::Eq,
                    left: ValuePosition::Column {
                        name: "issueID".into(),
                    },
                    right: ValuePosition::Literal {
                        value: Lit::Number(2.0),
                    },
                }),
            ],
        });
    }
    let cat = assignee_catalog(vec![vec![V::Int(7), V::Int(2)]]);
    assert!(matches!(
        resolve_scalars(&ast, &cat),
        Err(BuildError::Unsupported(_))
    ));
}

// --- cross-level recursion (§6) --------------------------------------------
//
// project ← issue(projectId) ← comment(issueId). The inner `EXISTS(comment id = K)`
// folds to `issue.id = <comment.issueId>`, which binds issue's PK and *unlocks* the
// outer fold to `project.id = <issue.projectId>`. Both joins vanish.

fn project_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "x"], vec![0], vec![(0, true)])
}

/// Catalog holding the two *child* tables the nested scalars read.
fn nested_catalog(issues: Vec<Vec<V>>, comments: Vec<Vec<V>>) -> Cat {
    Cat(vec![
        (
            "issue".into(),
            MemorySource::new(
                Schema::new(vec!["id", "projectId"], vec![0], vec![(0, true)]),
                issues.into_iter().map(row).collect(),
            ),
        ),
        (
            "comment".into(),
            MemorySource::new(
                Schema::new(vec!["id", "issueId"], vec![0], vec![(0, true)]),
                comments.into_iter().map(row).collect(),
            ),
        ),
    ])
}

/// `project WHERE EXISTS(issue WHERE EXISTS(comment WHERE comment.id = key))`, both
/// flagged `scalar`; correlations `comment.issueId = issue.id`,
/// `issue.projectId = project.id`.
fn nested_scalar_ast(key: i64) -> Ast {
    let scalar_csq = |child_field: &str, subquery: Ast| CorrelatedSubqueryCondition {
        related: CorrelatedSubquery {
            correlation: Correlation {
                parent_field: vec!["id".into()],
                child_field: vec![child_field.into()],
            },
            subquery: Box::new(subquery),
            system: None,
        },
        op: ExistsOp::Exists,
        flip: None,
        scalar: Some(true),
        plan_id: None,
    };
    let comment = Ast {
        table: "comment".into(),
        r#where: Some(Condition::Simple(SimpleCondition {
            op: Op::Eq,
            left: ValuePosition::Column { name: "id".into() },
            right: ValuePosition::Literal {
                value: Lit::Number(key as f64),
            },
        })),
        ..Default::default()
    };
    let issue = Ast {
        table: "issue".into(),
        r#where: Some(Condition::CorrelatedSubquery(scalar_csq(
            "issueId", comment,
        ))),
        ..Default::default()
    };
    Ast {
        table: "project".into(),
        r#where: Some(Condition::CorrelatedSubquery(scalar_csq(
            "projectId",
            issue,
        ))),
        ..Default::default()
    }
}

#[test]
fn cross_level_nested_scalar_collapses_to_one_equality() {
    // comment 5 → issueId 10 ; issue 10 → projectId 3. Whole nest ⇒ project.id = 3.
    let cat = nested_catalog(
        vec![vec![V::Int(10), V::Int(3)]],
        vec![vec![V::Int(5), V::Int(10)]],
    );
    let resolved = resolve_scalars(&nested_scalar_ast(5), &cat).unwrap();
    assert_eq!(resolved.r#where, Some(simple_eq(Op::Eq, 3)));

    // End-to-end with ONLY the project parent seeded — both joins are gone.
    let out = run_fetch_test_ast(
        vec![TableSpec::new(
            "project",
            project_schema(),
            vec![ir(1, 0), ir(3, 0)],
        )],
        &resolved,
    )
    .unwrap();
    assert_eq!(out, vec![cleaf(ir(3, 0))]);
}

#[test]
fn cross_level_inner_absent_cascades_to_constant_false() {
    // No comment 999 ⇒ inner folds to constant-false ⇒ the issue subquery is empty ⇒
    // the outer EXISTS folds to constant-false too (no projects), not an error.
    let cat = nested_catalog(vec![vec![V::Int(10), V::Int(3)]], vec![]);
    let resolved = resolve_scalars(&nested_scalar_ast(999), &cat).unwrap();
    let out = run_fetch_test_ast(
        vec![TableSpec::new(
            "project",
            project_schema(),
            vec![ir(1, 0), ir(3, 0)],
        )],
        &resolved,
    )
    .unwrap();
    assert!(out.is_empty());
}

#[test]
fn cross_level_stops_at_an_unflagged_level() {
    // Outer flagged, inner NOT flagged: the inner EXISTS stays a join, so the outer
    // child WHERE is a correlated subquery (not a unique-key binding) ⇒ Unsupported.
    let mut ast = nested_scalar_ast(5);
    if let Some(Condition::CorrelatedSubquery(outer)) = ast.r#where.as_mut() {
        if let Some(Condition::CorrelatedSubquery(inner)) = outer.related.subquery.r#where.as_mut()
        {
            inner.scalar = None;
        }
    }
    let cat = nested_catalog(
        vec![vec![V::Int(10), V::Int(3)]],
        vec![vec![V::Int(5), V::Int(10)]],
    );
    assert!(matches!(
        resolve_scalars(&ast, &cat),
        Err(BuildError::Unsupported(_))
    ));
}
