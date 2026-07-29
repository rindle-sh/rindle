#![cfg(feature = "testkit")]

//! The IVM **consistency invariant** for the union fan: an incremental push must leave
//! the result equal to a fresh query against the post-push source state
//! (`view-after-push == fresh-fetch-of-post-push-state`). This is the ground-truth
//! correctness check that settles the documented JS divergence.
//!
//! For `issue WHERE (x=1 OR EXISTS_flipped(comments) OR EXISTS(labels))` — the union-fan
//! query — the **JS engine VIOLATES this invariant** for non-flipped-EXISTS pushes (its
//! production `{change, position}` source overlay makes the `Exists` size re-count miss
//! the in-flight child, so it drops label-adds / parent-adds / removes / edits). The
//! **Rust upholds it** (its re-materialize overlay counts the in-flight child), so the
//! Rust is *more correct*, not merely different. Each case below would be `INCONSISTENT`
//! under JS (verified out-of-band against `mono` `runPushTest` vs `runFetchTest`).

use std::collections::BTreeSet;

use rindle::change::SourceChange;
use rindle::testkit::{run_fetch_test_ast, run_push_test_ast, CaughtChange, CaughtNode, TableSpec};
use rindle::value::{owned_row as row, OwnedRow, OwnedValue as V, SourceSchema};
use rindle::{
    Ast, Condition, CorrelatedSubquery, CorrelatedSubqueryCondition, Correlation, Dir, ExistsOp,
    Lit, Op, OrderPart, SimpleCondition, ValuePosition,
};

fn issue_schema() -> SourceSchema {
    // The source declares no relationships; slots are query-derived (EXISTS
    // pre-order → comments=0, labels=1).
    SourceSchema::new(vec!["id", "x"], vec![0], vec![(0, true)])
}
fn child_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "issueID"], vec![0], vec![(0, true)])
}
fn ir(id: i64, x: i64) -> Vec<V> {
    vec![V::Int(id), V::Int(x)]
}
fn cr(id: i64, issue: i64) -> Vec<V> {
    vec![V::Int(id), V::Int(issue)]
}
fn sources(issues: Vec<Vec<V>>, comments: Vec<Vec<V>>, labels: Vec<Vec<V>>) -> Vec<TableSpec> {
    vec![
        TableSpec::new("issue", issue_schema(), issues),
        TableSpec::new("comment", child_schema(), comments),
        TableSpec::new("label", child_schema(), labels),
    ]
}
fn exists(alias: &str, table: &str, flip: bool) -> Condition {
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
        flip: if flip { Some(true) } else { None },
        scalar: None,
        plan_id: None,
    })
}
/// The UNION-FAN query: `x = 1 OR EXISTS_flipped(comments) OR EXISTS(labels)`.
fn union_ast() -> Ast {
    Ast {
        table: "issue".into(),
        order_by: vec![OrderPart("id".into(), Dir::Asc)],
        r#where: Some(Condition::Or {
            conditions: vec![
                Condition::Simple(SimpleCondition {
                    op: Op::Eq,
                    left: ValuePosition::Column { name: "x".into() },
                    right: ValuePosition::Literal {
                        value: Lit::Number(1.0),
                    },
                }),
                exists("comments", "comment", true),
                exists("labels", "label", false),
            ],
        }),
        ..Default::default()
    }
}

fn id_of_node(n: &CaughtNode) -> i64 {
    match n.row.col(0).to_owned() {
        V::Int(i) => i,
        _ => panic!("non-int id"),
    }
}
fn id_of_row(r: &OwnedRow) -> i64 {
    match r.col(0).to_owned() {
        V::Int(i) => i,
        _ => panic!("non-int id"),
    }
}

/// Apply the Catch push stream to the initial row set → the maintained top-level row ids.
/// (A `Child` updates a relationship on a still-present row, so it never changes the set.)
fn maintained_ids(initial: &[CaughtNode], pushes: &[CaughtChange]) -> Vec<i64> {
    let mut set: BTreeSet<i64> = initial.iter().map(id_of_node).collect();
    for ch in pushes {
        match ch {
            CaughtChange::Add(n) => {
                set.insert(id_of_node(n));
            }
            CaughtChange::Remove(n) => {
                set.remove(&id_of_node(n));
            }
            CaughtChange::Edit { old, row } => {
                set.remove(&id_of_row(old));
                set.insert(id_of_row(row));
            }
            CaughtChange::Child { .. } => {}
        }
    }
    set.into_iter().collect()
}

fn fresh_ids(s1: Vec<TableSpec>) -> Vec<i64> {
    let tree = run_fetch_test_ast(s1, &union_ast()).unwrap();
    tree.iter().map(id_of_node).collect()
}

/// Drive one push from `s0`, then assert `maintained == fresh-fetch(s1)`.
fn assert_consistent(
    label: &str,
    s0: Vec<TableSpec>,
    push: (&str, SourceChange),
    s1: Vec<TableSpec>,
) {
    let res = run_push_test_ast(s0, &union_ast(), vec![(push.0, push.1)], false).unwrap();
    let maintained = maintained_ids(&res.initial, &res.pushes);
    let fresh = fresh_ids(s1);
    assert_eq!(
        maintained, fresh,
        "{label}: maintained {maintained:?} != fresh {fresh:?} (IVM consistency violated)"
    );
}

#[test]
fn u1_add_label_to_bare_row_stays_consistent() {
    // JS drops this (maintained=[1], fresh=[1,4]); the Rust adds issue 4 → consistent.
    assert_consistent(
        "U1 add label to bare issue4",
        sources(vec![ir(1, 1), ir(4, 5)], vec![], vec![]),
        ("label", SourceChange::Add(row(cr(21, 4)))),
        sources(vec![ir(1, 1), ir(4, 5)], vec![], vec![cr(21, 4)]),
    );
}

#[test]
fn u2_add_parent_with_preexisting_label_stays_consistent() {
    // JS drops this (maintained=[1], fresh=[1,5]); the Rust adds issue 5 → consistent.
    assert_consistent(
        "U2 add parent issue5 w/ pre-existing label",
        sources(vec![ir(1, 1)], vec![], vec![cr(40, 5)]),
        ("issue", SourceChange::Add(row(ir(5, 5)))),
        sources(vec![ir(1, 1), ir(5, 5)], vec![], vec![cr(40, 5)]),
    );
}

#[test]
fn u3_remove_last_label_stays_consistent() {
    // JS keeps issue 3 (maintained=[1,3], fresh=[1]); the Rust removes it → consistent.
    assert_consistent(
        "U3 remove issue3 last label",
        sources(vec![ir(1, 1), ir(3, 5)], vec![], vec![cr(20, 3)]),
        ("label", SourceChange::Remove(row(cr(20, 3)))),
        sources(vec![ir(1, 1), ir(3, 5)], vec![], vec![]),
    );
}

#[test]
fn u4_edit_out_of_leaf_stays_consistent() {
    // JS keeps issue 1 (maintained=[1], fresh=[]); the Rust removes it → consistent.
    assert_consistent(
        "U4 edit issue1 x=1->5 out of the leaf",
        sources(vec![ir(1, 1)], vec![], vec![]),
        (
            "issue",
            SourceChange::Edit {
                row: row(ir(1, 5)),
                old: row(ir(1, 1)),
            },
        ),
        sources(vec![ir(1, 5)], vec![], vec![]),
    );
}

#[test]
fn c1_flipped_comment_add_is_consistent_in_both_engines() {
    // Control: the FLIPPED path is push-correct in JS too (maintained=[1,4]=fresh).
    assert_consistent(
        "C1 add comment to bare issue4 (flipped)",
        sources(vec![ir(1, 1), ir(4, 5)], vec![], vec![]),
        ("comment", SourceChange::Add(row(cr(11, 4)))),
        sources(vec![ir(1, 1), ir(4, 5)], vec![cr(11, 4)], vec![]),
    );
}
