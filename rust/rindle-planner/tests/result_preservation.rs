//! **Result-preservation: planned == unplanned.**
//!
//! Flipping an EXISTS join changes the *execution strategy* (parent-driven semi-join →
//! child-driven `FlippedJoin`), never the *result*. We prove it end-to-end: take an
//! `issue WHERE EXISTS(comments)` AST, run it unplanned (semi) through the real IVM
//! `View`, then run `plan_ast` (with a cost model that flips it) and run the planned
//! AST through the same `View`, and assert the materialized trees — and the
//! incremental push stream — are identical. Each test also asserts the planner *did*
//! flip, so the `FlippedJoin` path is actually exercised (not a trivial no-op compare).

use std::rc::Rc;

use rindle::change::SourceChange;
use rindle::testkit::{run_fetch_test_ast, run_push_test_ast, TableSpec};
use rindle::value::{owned_row as row, OwnedValue as V, SourceSchema};
use rindle::{
    Ast, Condition, CorrelatedSubquery, CorrelatedSubqueryCondition, Correlation, Dir, ExistsOp,
    OrderPart,
};
use rindle_planner::{
    plan_ast, ConnectionCostModel, CostModelCost, FanoutConfidence, FanoutEst, PlannerConstraint,
};

// issue(id, x); PK id; declares the "comments" relationship (the EXISTS subquery).
fn issue_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "x"], vec![0], vec![(0, true)])
}
fn comment_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "issueID"], vec![0], vec![(0, true)])
}

fn ir(id: i64, x: i64) -> Vec<V> {
    vec![V::Int(id), V::Int(x)]
}
fn cr(id: i64, issue: i64) -> Vec<V> {
    vec![V::Int(id), V::Int(issue)]
}

fn ast_sources(issues: Vec<Vec<V>>, comments: Vec<Vec<V>>) -> Vec<TableSpec> {
    vec![
        TableSpec::new("issue", issue_schema(), issues),
        TableSpec::new("comment", comment_schema(), comments),
    ]
}

/// `issue WHERE EXISTS(comments)`, unplanned (`flip: None` — the planner decides).
fn exists_ast() -> Ast {
    Ast {
        table: "issue".into(),
        order_by: vec![OrderPart("id".into(), Dir::Asc)],
        r#where: Some(Condition::CorrelatedSubquery(CorrelatedSubqueryCondition {
            related: CorrelatedSubquery {
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
            },
            op: ExistsOp::Exists,
            flip: None,
            scalar: None,
            plan_id: None,
        })),
        ..Default::default()
    }
}

/// Cost model that makes flipping `issue WHERE EXISTS(comments)` win: a large parent
/// (`issue`) against a tiny child (`comment`) with a per-scan startup cost, so the
/// per-parent semi-probe (1000 × 50) loses to scanning the child once.
struct FlipModel;
impl ConnectionCostModel for FlipModel {
    fn estimate(
        &self,
        table: &str,
        _sort: &[OrderPart],
        _filters: Option<&Condition>,
        _constraint: Option<&PlannerConstraint>,
    ) -> CostModelCost {
        let (startup, rows) = if table == "issue" {
            (0.0, 1000.0)
        } else {
            (50.0, 1.0)
        };
        CostModelCost {
            startup_cost: startup,
            rows,
            fanout: Rc::new(|_| FanoutEst {
                fanout: 1.0,
                confidence: FanoutConfidence::None,
            }),
        }
    }
}

fn flip_of(ast: &Ast) -> Option<bool> {
    match ast.r#where.as_ref().unwrap() {
        Condition::CorrelatedSubquery(c) => c.flip,
        _ => panic!("expected an EXISTS where"),
    }
}

#[test]
fn planned_flip_preserves_fetch_result() {
    let unplanned = exists_ast();
    let planned = plan_ast(&unplanned, Rc::new(FlipModel));
    assert_eq!(
        flip_of(&planned),
        Some(true),
        "the cost model should flip this EXISTS (else the test is vacuous)"
    );

    // issue 1 has two comments; issues 2 and 3 have none → only issue 1 survives.
    let issues = vec![ir(1, 100), ir(2, 200), ir(3, 300)];
    let comments = vec![cr(10, 1), cr(11, 1)];

    let semi =
        run_fetch_test_ast(ast_sources(issues.clone(), comments.clone()), &unplanned).unwrap();
    let flipped = run_fetch_test_ast(ast_sources(issues, comments), &planned).unwrap();

    assert!(!semi.is_empty(), "expected issue 1 to be in the result");
    assert_eq!(semi, flipped, "flipping must preserve the fetch result");
}

#[test]
fn planned_flip_preserves_push_result() {
    let unplanned = exists_ast();
    let planned = plan_ast(&unplanned, Rc::new(FlipModel));
    assert_eq!(flip_of(&planned), Some(true));

    // No comments at hydrate (no issue passes EXISTS); adding issue 1's first comment
    // must flip it into the result identically under both lowerings.
    let pushes = || vec![("comment", SourceChange::Add(row(cr(10, 1))))];
    let sources = || ast_sources(vec![ir(1, 100), ir(2, 200)], vec![]);

    let semi = run_push_test_ast(sources(), &unplanned, pushes(), false).unwrap();
    let flipped = run_push_test_ast(sources(), &planned, pushes(), false).unwrap();

    assert!(semi.initial.is_empty(), "no issue should exist at hydrate");
    assert!(
        !semi.pushes.is_empty(),
        "the comment add should surface issue 1"
    );
    assert_eq!(semi.initial, flipped.initial, "flip must preserve hydrate");
    assert_eq!(
        semi.pushes, flipped.pushes,
        "flip must preserve the push stream"
    );
}
