#![cfg(feature = "testkit")]

//! End-to-end tests for a **non-flipped EXISTS under `OR`** (and nested AND/OR),
//! the Phase-2 builder lowering: `build_pipeline` lowers `where (x = v OR
//! exists(rel))` to the EXISTS-child `Join` (Cap-bounded) + a Filter sub-graph whose
//! `OR` is a `FanOut` of a leaf `Filter` branch and an `Exists` gate branch,
//! k-way-collapsed by a `FanIn`. Drives the real source so the Join's child port +
//! the in-flight overlay + the rel-preserving fan broadcast are all exercised.
//!
//! The load-bearing cases are the membership flips through the fan: a child Add/Remove
//! that an `Exists` branch converts to a parent Add/Remove, vs. the same change on a
//! row the **leaf** branch already keeps (CHILD precedence in `collapse_accumulated`
//! suppresses the spurious Add/Remove).

use rindle::change::SourceChange;
use rindle::testkit::{
    add, child, cleaf, cnode, rel, remove, run_fetch_test_ast, run_push_test_ast, CaughtChange,
    CaughtNode, TableSpec,
};
use rindle::value::{owned_row as row, OwnedValue as V, SourceSchema};
use rindle::{
    Ast, Condition, CorrelatedSubquery, CorrelatedSubqueryCondition, Correlation, Dir, ExistsOp,
    Lit, Op, OrderPart, SimpleCondition, ValuePosition,
};

// issue(id, x); PK id. Production-shaped real relationship name; the query-local slot
// tree puts the single EXISTS(comments) at slot 0.
fn issue_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "x"], vec![0], vec![(0, true)])
}
// comment(id, issueID); PK id; joins to issue on comment.issueID == issue.id.
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
// An issue node with its comments relationship (slot 0).
fn issue_with(id: i64, x: i64, comments: Vec<Vec<V>>) -> CaughtNode {
    cnode(
        ir(id, x),
        vec![(rel(0), comments.into_iter().map(cleaf).collect())],
    )
}

/// `x = v` leaf condition.
fn x_eq(v: i64) -> Condition {
    Condition::Simple(SimpleCondition {
        op: Op::Eq,
        left: ValuePosition::Column { name: "x".into() },
        right: ValuePosition::Literal {
            value: Lit::Number(v as f64),
        },
    })
}
/// `x >= v` leaf condition.
fn x_ge(v: i64) -> Condition {
    Condition::Simple(SimpleCondition {
        op: Op::Ge,
        left: ValuePosition::Column { name: "x".into() },
        right: ValuePosition::Literal {
            value: Lit::Number(v as f64),
        },
    })
}
/// `EXISTS(comments)` condition.
fn exists_comments() -> Condition {
    Condition::CorrelatedSubquery(CorrelatedSubqueryCondition {
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
    })
}

/// `issue WHERE <cond> ORDER BY id`.
fn issue_where(cond: Condition) -> Ast {
    Ast {
        table: "issue".into(),
        order_by: vec![OrderPart("id".into(), Dir::Asc)],
        r#where: Some(cond),
        ..Default::default()
    }
}

/// `issue WHERE (x = 100 OR EXISTS(comments))`.
fn or_ast() -> Ast {
    issue_where(Condition::Or {
        conditions: vec![x_eq(100), exists_comments()],
    })
}

// --- fetch (the OR fan: leaf Filter branch + Exists gate branch) -----------

#[test]
fn fetch_keeps_rows_matching_either_branch() {
    // issue 1 matches x=100 (no comments); issue 2 matches neither (dropped);
    // issue 3 matches EXISTS (has comment 10). The kept rows carry their comments
    // relationship (empty for the leaf-matched issue 1).
    let tree = run_fetch_test_ast(
        ast_sources(vec![ir(1, 100), ir(2, 200), ir(3, 300)], vec![cr(10, 3)]),
        &or_ast(),
    )
    .unwrap();
    assert_eq!(
        tree,
        vec![
            issue_with(1, 100, vec![]),
            issue_with(3, 300, vec![cr(10, 3)]),
        ]
    );
}

#[test]
fn fetch_a_row_matching_both_branches_appears_once() {
    // issue 1 matches BOTH x=100 and EXISTS — the FanIn PK-dedup yields it once.
    let tree = run_fetch_test_ast(
        ast_sources(vec![ir(1, 100), ir(2, 200)], vec![cr(10, 1)]),
        &or_ast(),
    )
    .unwrap();
    assert_eq!(tree, vec![issue_with(1, 100, vec![cr(10, 1)])]);
}

// --- push: child Add/Remove flips through the fan ---------------------------

#[test]
fn child_add_flips_exists_on_a_non_leaf_row_to_add() {
    // issue 2 (x=200) is absent at hydrate (fails both branches). Adding its first
    // comment flips EXISTS 0→1; the leaf branch still drops it, so the Exists branch's
    // conversion to Add(issue 2) survives the collapse. Exercises the rel-preserving
    // fan broadcast (the Exists branch must count the just-added comment off the node).
    let res = run_push_test_ast(
        ast_sources(vec![ir(1, 100), ir(2, 200)], vec![]),
        &or_ast(),
        vec![("comment", SourceChange::Add(row(cr(10, 2))))],
        false,
    )
    .unwrap();
    assert_eq!(res.initial, vec![issue_with(1, 100, vec![])]);
    assert_eq!(res.pushes, vec![add(issue_with(2, 200, vec![cr(10, 2)]))]);
}

#[test]
fn child_add_on_a_leaf_matched_row_stays_a_child() {
    // issue 1 (x=100) is ALREADY in the result via the leaf branch. Adding its first
    // comment flips EXISTS 0→1 (the Exists branch wants Add) but the leaf branch keeps
    // the Child — CHILD precedence suppresses the spurious Add, so the row stays put
    // and only its relationship updates.
    let res = run_push_test_ast(
        ast_sources(vec![ir(1, 100), ir(2, 200)], vec![]),
        &or_ast(),
        vec![("comment", SourceChange::Add(row(cr(11, 1))))],
        false,
    )
    .unwrap();
    assert_eq!(res.initial, vec![issue_with(1, 100, vec![])]);
    assert_eq!(
        res.pushes,
        vec![child(ir(1, 100), rel(0), add(cleaf(cr(11, 1))))]
    );
}

#[test]
fn child_remove_last_comment_on_a_non_leaf_row_to_remove() {
    // issue 2 (x=200) is in ONLY via EXISTS. Removing its last comment flips EXISTS
    // 1→0; the leaf branch drops it, so the Exists branch's conversion to Remove(issue
    // 2) survives — carrying the removed comment (the view's last copy of it).
    let res = run_push_test_ast(
        ast_sources(vec![ir(1, 100), ir(2, 200)], vec![cr(10, 2)]),
        &or_ast(),
        vec![("comment", SourceChange::Remove(row(cr(10, 2))))],
        false,
    )
    .unwrap();
    assert_eq!(
        res.initial,
        vec![
            issue_with(1, 100, vec![]),
            issue_with(2, 200, vec![cr(10, 2)]),
        ]
    );
    assert_eq!(
        res.pushes,
        vec![remove(issue_with(2, 200, vec![cr(10, 2)]))]
    );
}

#[test]
fn child_remove_on_a_leaf_matched_row_stays_a_child() {
    // issue 1 (x=100) stays in via the leaf branch even after losing its last comment;
    // CHILD precedence suppresses the Exists branch's spurious Remove.
    let res = run_push_test_ast(
        ast_sources(vec![ir(1, 100)], vec![cr(10, 1)]),
        &or_ast(),
        vec![("comment", SourceChange::Remove(row(cr(10, 1))))],
        false,
    )
    .unwrap();
    assert_eq!(res.initial, vec![issue_with(1, 100, vec![cr(10, 1)])]);
    assert_eq!(
        res.pushes,
        vec![child(ir(1, 100), rel(0), remove(cleaf(cr(10, 1))))]
    );
}

// --- push: parent Add/Remove through the fan --------------------------------

#[test]
fn parent_add_matching_the_leaf_branch_is_added() {
    // A fresh issue with x=100 and no comments: the leaf branch keeps it (Add), the
    // Exists branch drops it (size 0). The Add survives with an empty comments rel —
    // the parent-side broadcast must NOT panic the Exists gate on the missing-but-empty
    // relationship.
    let res = run_push_test_ast(
        ast_sources(vec![ir(1, 100)], vec![]),
        &or_ast(),
        vec![("issue", SourceChange::Add(row(ir(5, 100))))],
        false,
    )
    .unwrap();
    assert_eq!(res.pushes, vec![add(issue_with(5, 100, vec![]))]);
}

#[test]
fn parent_add_matching_neither_branch_is_dropped() {
    // x=600 (fails the leaf) and no comments (fails EXISTS) → both branches drop it,
    // the collapse yields nothing.
    let res = run_push_test_ast(
        ast_sources(vec![ir(1, 100)], vec![]),
        &or_ast(),
        vec![("issue", SourceChange::Add(row(ir(6, 600))))],
        false,
    )
    .unwrap();
    assert_eq!(res.pushes, Vec::<CaughtChange>::new());
}

// --- nested boolean trees ---------------------------------------------------

#[test]
fn nested_or_under_or() {
    // `x = 100 OR (x = 200 OR EXISTS(comments))` — a nested FanOut branch. All three
    // issues qualify (1 via outer leaf, 2 via inner leaf, 3 via EXISTS).
    let ast = issue_where(Condition::Or {
        conditions: vec![
            x_eq(100),
            Condition::Or {
                conditions: vec![x_eq(200), exists_comments()],
            },
        ],
    });
    let tree = run_fetch_test_ast(
        ast_sources(vec![ir(1, 100), ir(2, 200), ir(3, 300)], vec![cr(10, 3)]),
        &ast,
    )
    .unwrap();
    assert_eq!(
        tree,
        vec![
            issue_with(1, 100, vec![]),
            issue_with(2, 200, vec![]),
            issue_with(3, 300, vec![cr(10, 3)]),
        ]
    );
}

#[test]
fn and_over_a_subquery_or() {
    // `x >= 200 AND (x = 300 OR EXISTS(comments))` — apply_and over a leaf Filter and
    // an OR fan. issue 1 (x=100) fails the AND; issue 2 (x=200) passes via EXISTS;
    // issue 3 (x=300) passes via the inner leaf.
    let ast = issue_where(Condition::And {
        conditions: vec![
            x_ge(200),
            Condition::Or {
                conditions: vec![x_eq(300), exists_comments()],
            },
        ],
    });
    let tree = run_fetch_test_ast(
        ast_sources(
            vec![ir(1, 100), ir(2, 200), ir(3, 300)],
            vec![cr(10, 2), cr(11, 1)],
        ),
        &ast,
    )
    .unwrap();
    assert_eq!(
        tree,
        vec![
            issue_with(2, 200, vec![cr(10, 2)]),
            issue_with(3, 300, vec![]),
        ]
    );
}

#[test]
fn and_over_subquery_or_push_flips_to_add() {
    // Same shape as above; a comment added to issue 2 (x=200, passes the AND's first
    // conjunct, currently failing the inner OR) flips EXISTS and adds it.
    let ast = issue_where(Condition::And {
        conditions: vec![
            x_ge(200),
            Condition::Or {
                conditions: vec![x_eq(300), exists_comments()],
            },
        ],
    });
    let res = run_push_test_ast(
        ast_sources(vec![ir(1, 100), ir(2, 200), ir(3, 300)], vec![]),
        &ast,
        vec![("comment", SourceChange::Add(row(cr(20, 2))))],
        false,
    )
    .unwrap();
    // Hydrate: only issue 3 (x=300 via the inner leaf) qualifies.
    assert_eq!(res.initial, vec![issue_with(3, 300, vec![])]);
    assert_eq!(res.pushes, vec![add(issue_with(2, 200, vec![cr(20, 2)]))]);
}

// --- parent Edit through the fan --------------------------------------------

#[test]
fn parent_edit_out_of_the_leaf_branch_removes() {
    // issue 1 is in via the leaf (x=100); issue 2 via EXISTS. A non-key Edit of issue 1
    // to x=300 (it has no comments) fails BOTH branches: the leaf branch splits the Edit
    // on (pred(old)=true, pred(new)=false) -> Remove(old), the Exists branch drops it,
    // and the collapse keeps the Remove. Exercises the Edit materialize/rebuild + the
    // collapse `Edit` arm through the fan.
    let res = run_push_test_ast(
        ast_sources(vec![ir(1, 100), ir(2, 200)], vec![cr(10, 2)]),
        &or_ast(),
        vec![(
            "issue",
            SourceChange::Edit {
                row: row(ir(1, 300)),
                old: row(ir(1, 100)),
            },
        )],
        false,
    )
    .unwrap();
    assert_eq!(
        res.initial,
        vec![
            issue_with(1, 100, vec![]),
            issue_with(2, 200, vec![cr(10, 2)]),
        ]
    );
    assert_eq!(res.pushes, vec![remove(issue_with(1, 100, vec![]))]);
}

// --- limit-0 EXISTS under OR (a constant-false branch) ----------------------

#[test]
fn limit_zero_exists_under_or_is_a_const_false_branch() {
    // EXISTS over a `limit 0` (empty) subquery is constant-false (`apply_csq_condition`
    // builds a `Const(false)` Filter, and the join loop builds NO relationship Join), so
    // that OR branch never matches and the node carries no comments relationship. Only
    // the leaf `x=100` keeps rows; issue 2 (has a comment but x=200) is dropped.
    let mut ex = exists_comments();
    if let Condition::CorrelatedSubquery(c) = &mut ex {
        c.related.subquery.limit = Some(0);
    }
    let ast = issue_where(Condition::Or {
        conditions: vec![x_eq(100), ex],
    });
    let tree = run_fetch_test_ast(
        ast_sources(vec![ir(1, 100), ir(2, 200)], vec![cr(10, 2)]),
        &ast,
    )
    .unwrap();
    assert_eq!(tree, vec![cleaf(ir(1, 100))]); // no comments rel (no Join was built)
}

// --- NOT EXISTS under OR ----------------------------------------------------

#[test]
fn not_exists_under_or_keeps_leaf_matches_and_childless_rows() {
    // `x=100 OR NOT EXISTS(comments)`: issue 1 passes via the leaf even with a
    // comment, issue 2 passes via NOT EXISTS, issue 3 is dropped.
    let mut ex = exists_comments();
    if let Condition::CorrelatedSubquery(c) = &mut ex {
        c.op = ExistsOp::NotExists;
    }
    let ast = issue_where(Condition::Or {
        conditions: vec![x_eq(100), ex],
    });
    let tree = run_fetch_test_ast(
        ast_sources(
            vec![ir(1, 100), ir(2, 200), ir(3, 200)],
            vec![cr(10, 1), cr(11, 3)],
        ),
        &ast,
    )
    .unwrap();
    assert_eq!(
        tree,
        vec![
            issue_with(1, 100, vec![cr(10, 1)]),
            issue_with(2, 200, vec![])
        ]
    );
}

// --- two DISTINCT EXISTS under OR (one Exists gate per relationship slot) ----

// issue(id, x). The query-local slot tree (EXISTS pre-order) puts comments=slot 0,
// reactions=slot 1.
fn issue_schema_2rel() -> SourceSchema {
    SourceSchema::new(vec!["id", "x"], vec![0], vec![(0, true)])
}
fn reaction_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "issueID"], vec![0], vec![(0, true)])
}
fn rr(id: i64, issue: i64) -> Vec<V> {
    vec![V::Int(id), V::Int(issue)]
}
fn sources_2rel(
    issues: Vec<Vec<V>>,
    comments: Vec<Vec<V>>,
    reactions: Vec<Vec<V>>,
) -> Vec<TableSpec> {
    vec![
        TableSpec::new("issue", issue_schema_2rel(), issues),
        TableSpec::new("comment", comment_schema(), comments),
        TableSpec::new("reaction", reaction_schema(), reactions),
    ]
}
/// `EXISTS(reactions)` on slot 1 (reaction.issueID == issue.id).
fn exists_reactions() -> Condition {
    Condition::CorrelatedSubquery(CorrelatedSubqueryCondition {
        related: CorrelatedSubquery {
            correlation: Correlation {
                parent_field: vec!["id".into()],
                child_field: vec!["issueID".into()],
            },
            subquery: Box::new(Ast {
                table: "reaction".into(),
                alias: Some("reactions".into()),
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
// An issue carrying BOTH relationship slots (comments=0, reactions=1).
fn issue_cr(id: i64, x: i64, comments: Vec<Vec<V>>, reactions: Vec<Vec<V>>) -> CaughtNode {
    cnode(
        ir(id, x),
        vec![
            (rel(0), comments.into_iter().map(cleaf).collect()),
            (rel(1), reactions.into_iter().map(cleaf).collect()),
        ],
    )
}

#[test]
fn two_distinct_exists_under_or_fetch() {
    // `WHERE EXISTS(comments) OR EXISTS(reactions)` — a FanOut with two independent
    // `Exists` gates, one per relationship slot. issue 1 qualifies via comments, issue 2
    // via reactions, issue 3 via neither (dropped). Each kept node carries BOTH gated
    // relationships (the join loop attaches both, one possibly empty).
    let ast = issue_where(Condition::Or {
        conditions: vec![exists_comments(), exists_reactions()],
    });
    let tree = run_fetch_test_ast(
        sources_2rel(
            vec![ir(1, 0), ir(2, 0), ir(3, 0)],
            vec![cr(10, 1)],
            vec![rr(20, 2)],
        ),
        &ast,
    )
    .unwrap();
    assert_eq!(
        tree,
        vec![
            issue_cr(1, 0, vec![cr(10, 1)], vec![]),
            issue_cr(2, 0, vec![], vec![rr(20, 2)]),
        ]
    );
}

#[test]
fn two_distinct_exists_under_or_push_flips_the_right_gate() {
    // Adding a reaction to issue 3 (in neither branch) flips ONLY the reactions gate
    // (slot 1) 0->1 -> Add. The comments gate (slot 0) sees a Child to a different slot
    // and drops it; the rel-preserving broadcast keeps BOTH slots so each gate counts
    // its own. The collapse yields a single Add.
    let ast = issue_where(Condition::Or {
        conditions: vec![exists_comments(), exists_reactions()],
    });
    let res = run_push_test_ast(
        sources_2rel(
            vec![ir(1, 0), ir(2, 0), ir(3, 0)],
            vec![cr(10, 1)],
            vec![rr(20, 2)],
        ),
        &ast,
        vec![("reaction", SourceChange::Add(row(rr(21, 3))))],
        false,
    )
    .unwrap();
    assert_eq!(
        res.initial,
        vec![
            issue_cr(1, 0, vec![cr(10, 1)], vec![]),
            issue_cr(2, 0, vec![], vec![rr(20, 2)]),
        ]
    );
    assert_eq!(
        res.pushes,
        vec![add(issue_cr(3, 0, vec![], vec![rr(21, 3)]))]
    );
}
