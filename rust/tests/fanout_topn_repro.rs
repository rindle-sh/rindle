#![cfg(feature = "testkit")]

//! Regression (found by the decorated-push fuzz sweep, `RUNAWAY-PUSH-FINDINGS.md` bug B):
//! partially-masked fan-out + top-N push. `parent.whereExists(mt).orderBy(sort).limit(2)`
//! over 4 parents on mt1 (2 in-window, 2 below the window) + 2 parents on mt2 (refill,
//! *beyond* the masked mt1 parents). On `Remove(mt1)` all four mt1 parents fail EXISTS in
//! one push.
//!
//! It USED to mask: `Take` refilled the window from p5,p6 (skipping the transiently-masked
//! p3,p4), sliding its bound to p6/sort60; the late removes for p3,p4 (below the new bound
//! but NEVER in the materialized window) were forwarded as phantom `Remove`s →
//! `Remove(1),Add(5),Remove(2),Add(6),Remove(3),Remove(4)` → View panic "node does not
//! exist".
//!
//! Now the Zero-faithful in-flight child overlay holds it: during the `Remove(mt1)`
//! fan-out, `Join` publishes the change LIVE (`inprogress_overlay` + `inprogress_position`,
//! `graph.rs`), and `Take`'s reentrant refill — refetching through the `Exists` gate —
//! sees PRE-change membership for not-yet-processed parents (the overlay re-adds the
//! in-flight-removed child for parents sorting after the current one, gated by the
//! parent's effective order via `input_sort`). So p3,p4 still count and the removes
//! **cascade** — `Remove(1),Add(3),Remove(2),Add(4),Remove(3),Add(5),Remove(4),Add(6)` —
//! landing on the correct window `[5,6]` with no phantom.

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
    SourceSchema::new(vec!["id"], vec![0], vec![(0, true)])
}

fn query() -> Ast {
    Ast {
        table: "parent".into(),
        order_by: vec![OrderPart("sort".into(), Dir::Asc)],
        limit: Some(2),
        r#where: Some(Condition::CorrelatedSubquery(CorrelatedSubqueryCondition {
            related: CorrelatedSubquery {
                correlation: Correlation {
                    parent_field: vec!["mtid".into()],
                    child_field: vec!["id".into()],
                },
                subquery: Box::new(Ast {
                    table: "mt".into(),
                    alias: Some("mt".into()),
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

fn sources() -> Vec<TableSpec> {
    vec![
        TableSpec::new(
            "parent",
            parent_schema(),
            vec![
                vec![V::Int(1), V::Int(10), V::Int(1)], // p1: mt1, in window
                vec![V::Int(2), V::Int(20), V::Int(1)], // p2: mt1, in window
                vec![V::Int(3), V::Int(30), V::Int(1)], // p3: mt1, below window (masked)
                vec![V::Int(4), V::Int(40), V::Int(1)], // p4: mt1, below window (masked)
                vec![V::Int(5), V::Int(50), V::Int(2)], // p5: mt2, refill
                vec![V::Int(6), V::Int(60), V::Int(2)], // p6: mt2, refill
            ],
        ),
        TableSpec::new("mt", mt_schema(), vec![vec![V::Int(1)], vec![V::Int(2)]]),
    ]
}

#[test]
fn partially_masked_fanout_topn_push_is_consistent() {
    let res = run_push_test_ast_view(
        sources(),
        &query(),
        vec![("mt", SourceChange::Remove(owned_row(vec![V::Int(1)])))],
        false,
    )
    .expect("build+run");
    let ids: Vec<i64> = res
        .data
        .iter()
        .map(|n| match &n.row.col(0).to_owned() {
            V::Int(i) => *i,
            _ => -1,
        })
        .collect();
    assert_eq!(
        ids,
        vec![5, 6],
        "after Remove(mt1) the only qualifying parents are p5,p6 (mt2)"
    );
}
