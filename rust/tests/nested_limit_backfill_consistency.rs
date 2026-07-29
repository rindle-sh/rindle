#![cfg(feature = "testkit")]

//! Adversarial: a MATERIALIZED limited relationship (inner `Take`) re-fetched while an
//! OUTER `whereExists(...).limit()` backfill refills its parent — the reentrant path the
//! Zero-faithful in-flight child overlay must keep consistent (the outer `Join`'s live
//! overlay re-adds the in-flight child so the refilled parent is counted, while the inner
//! `Take` re-materializes its own top-N). Verifies the refilled parents' nested top-N
//! relationship matches a fresh hydrate of the post-push state.

use rindle::testkit::{run_push_test_ast_view, TableSpec};
use rindle::value::{owned_row, OwnedValue as V, SourceSchema};
use rindle::{
    Ast, Condition, CorrelatedSubquery, CorrelatedSubqueryCondition, Correlation, Dir, ExistsOp,
    OrderPart, SourceChange,
};

fn item_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "sort", "grp"], vec![0], vec![(0, true)])
}
fn tag_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "grp"], vec![0], vec![(0, true)])
}

fn corr() -> Correlation {
    Correlation {
        parent_field: vec!["grp".into()],
        child_field: vec!["grp".into()],
    }
}
fn tag_sub(alias: &str, limit: Option<u32>) -> CorrelatedSubquery {
    CorrelatedSubquery {
        correlation: corr(),
        subquery: Box::new(Ast {
            table: "tag".into(),
            alias: Some(alias.into()),
            order_by: vec![OrderPart("id".into(), Dir::Asc)],
            limit,
            ..Default::default()
        }),
        system: None,
    }
}

/// `item.whereExists('g': tag).orderBy(sort).limit(2).related('tg': tag.orderBy(id).limit(2))`
fn query() -> Ast {
    Ast {
        table: "item".into(),
        order_by: vec![OrderPart("sort".into(), Dir::Asc)],
        limit: Some(2),
        related: vec![tag_sub("tg", Some(2))],
        r#where: Some(Condition::CorrelatedSubquery(CorrelatedSubqueryCondition {
            related: tag_sub("g", None),
            op: ExistsOp::Exists,
            flip: None,
            scalar: None,
            plan_id: None,
        })),
        ..Default::default()
    }
}

fn items() -> Vec<Vec<V>> {
    vec![
        vec![V::Int(1), V::Int(10), V::Int(1)], // g1, in window
        vec![V::Int(2), V::Int(20), V::Int(1)], // g1, in window
        vec![V::Int(3), V::Int(30), V::Int(2)], // g2, refill
        vec![V::Int(4), V::Int(40), V::Int(2)], // g2, refill
    ]
}

#[test]
fn nested_topn_relation_of_refilled_parent_matches_fresh_hydrate() {
    // tag t1 is the ONLY tag in g1; removing it empties g1 -> i1,i2 lose EXISTS ->
    // outer backfill refills i3,i4 (g2), each materializing g2's top-2 tags via the
    // inner Take (re-fetched reentrantly under the outer Join's live in-flight overlay).
    let tags_before = vec![
        vec![V::Int(1), V::Int(1)], // t1 -> g1 (sole)
        vec![V::Int(2), V::Int(2)], // t2 -> g2
        vec![V::Int(3), V::Int(2)], // t3 -> g2
        vec![V::Int(4), V::Int(2)], // t4 -> g2 (beyond inner limit 2)
    ];
    let post = run_push_test_ast_view(
        vec![
            TableSpec::new("item", item_schema(), items()),
            TableSpec::new("tag", tag_schema(), tags_before),
        ],
        &query(),
        vec![(
            "tag",
            SourceChange::Remove(owned_row(vec![V::Int(1), V::Int(1)])),
        )],
        false,
    )
    .expect("push run");

    let fresh = run_push_test_ast_view(
        vec![
            TableSpec::new("item", item_schema(), items()),
            TableSpec::new(
                "tag",
                tag_schema(),
                vec![
                    vec![V::Int(2), V::Int(2)],
                    vec![V::Int(3), V::Int(2)],
                    vec![V::Int(4), V::Int(2)],
                ],
            ),
        ],
        &query(),
        vec![],
        false,
    )
    .expect("fresh hydrate");

    let ids: Vec<i64> = post
        .data
        .iter()
        .map(|n| match &n.row.col(0).to_owned() {
            V::Int(i) => *i,
            _ => -1,
        })
        .collect();
    assert_eq!(ids, vec![3, 4], "window refilled to i3,i4 (g2)");
    assert_eq!(
        post.data, fresh.initial,
        "refilled parents' nested top-2 `tg` relation must equal a fresh hydrate (no stale tag)"
    );
}
