#![cfg(feature = "testkit")]

//! Adversarial guard for the Zero-faithful in-flight backfill fix: a relationship that is
//! BOTH materialized (`related`) AND the EXISTS gate, on the SAME source, with a backfill
//! refill. The refill node's child-relationship thunk is built mid child-push under the
//! `Join`'s live in-flight overlay (which re-adds the in-flight child only for the GATE
//! count, gated to parents after the current fan-out position), so this checks the
//! refilled node's MATERIALIZED relation is still correct (drawn from unchanged data),
//! i.e. the overlay does not leak a stale/removed child into the persisted view. Ground
//! truth = a fresh hydrate of the post-push source state.

use rindle::testkit::{run_push_test_ast_view, TableSpec};
use rindle::value::{owned_row, OwnedValue as V, SourceSchema};
use rindle::{
    Ast, Condition, CorrelatedSubquery, CorrelatedSubqueryCondition, Correlation, Dir, ExistsOp,
    OrderPart, SourceChange,
};

fn parent_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "sort", "mtid"], vec![0], vec![(0, true)])
}
fn mt_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "name"], vec![0], vec![(0, true)])
}

fn csq(alias: &str) -> CorrelatedSubquery {
    CorrelatedSubquery {
        correlation: Correlation {
            parent_field: vec!["mtid".into()],
            child_field: vec!["id".into()],
        },
        subquery: Box::new(Ast {
            table: "mt".into(),
            alias: Some(alias.into()),
            order_by: vec![OrderPart("id".into(), Dir::Asc)],
            ..Default::default()
        }),
        system: None,
    }
}

/// `parent.related('m').whereExists('m').orderBy(sort).limit(2)` — materialize AND gate
/// on the SAME mt source.
fn query() -> Ast {
    Ast {
        table: "parent".into(),
        order_by: vec![OrderPart("sort".into(), Dir::Asc)],
        limit: Some(2),
        related: vec![csq("m")],
        r#where: Some(Condition::CorrelatedSubquery(CorrelatedSubqueryCondition {
            related: csq("mx"),
            op: ExistsOp::Exists,
            flip: None,
            scalar: None,
            plan_id: None,
        })),
        ..Default::default()
    }
}

fn parents() -> Vec<Vec<V>> {
    vec![
        vec![V::Int(1), V::Int(10), V::Int(1)], // p1 -> mt1 (in window)
        vec![V::Int(2), V::Int(20), V::Int(1)], // p2 -> mt1 (in window)
        vec![V::Int(3), V::Int(30), V::Int(2)], // p3 -> mt2 (refill)
        vec![V::Int(4), V::Int(40), V::Int(2)], // p4 -> mt2 (refill)
    ]
}

#[test]
fn refilled_node_materialized_relation_matches_fresh_hydrate() {
    // Remove mt1 → p1,p2 lose EXISTS; backfill refills p3,p4 (mt2). Their materialized
    // `m` relation must read mt2 (unchanged), never a stale mt1.
    let post = run_push_test_ast_view(
        vec![
            TableSpec::new("parent", parent_schema(), parents()),
            TableSpec::new(
                "mt",
                mt_schema(),
                vec![vec![V::Int(1), V::str("a")], vec![V::Int(2), V::str("b")]],
            ),
        ],
        &query(),
        vec![(
            "mt",
            SourceChange::Remove(owned_row(vec![V::Int(1), V::str("a")])),
        )],
        false,
    )
    .expect("push run");

    // Fresh hydrate of the POST-push state (mt has only mt2), no push.
    let fresh = run_push_test_ast_view(
        vec![
            TableSpec::new("parent", parent_schema(), parents()),
            TableSpec::new("mt", mt_schema(), vec![vec![V::Int(2), V::str("b")]]),
        ],
        &query(),
        vec![],
        false,
    )
    .expect("fresh hydrate");

    assert_eq!(
        post.data, fresh.initial,
        "post-push view (incl. materialized `m` of refilled p3,p4) must equal a fresh hydrate"
    );
    // Sanity: window is p3,p4 each carrying exactly mt2.
    let ids: Vec<i64> = post
        .data
        .iter()
        .map(|n| match &n.row.col(0).to_owned() {
            V::Int(i) => *i,
            _ => -1,
        })
        .collect();
    assert_eq!(ids, vec![3, 4]);
}
