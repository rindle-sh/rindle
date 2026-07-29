#![cfg(feature = "testkit")]

//! Regression: a **partitioned `Take` leaks a storage slot per partition that ever
//! drained to empty**. The wiki demo (`apps/wiki-demo`) pins two boards, each a
//! top-N parent with a nested correlated `limit` relationship — e.g.
//! `page.orderBy(last_ts desc).limit(N) { edits_recent: edit(page_id).orderBy(ts desc).limit(M) }`.
//! The inner `limit(M)` lowers to a `Take` PARTITIONED by the correlation child key
//! (`page_id`). Every page that floats into the parent window hydrates its own inner
//! partition; when that page later ages out of the retention window and its edits are
//! pruned, the partition drains to size 0 — but `Take` wrote a zombie
//! `Take{size:0, bound:None}` slot instead of deleting it (unlike `Reduce`, which
//! deletes a group on death). Under the demo's continuous Wikimedia churn the set of
//! distinct pages/editors grows without bound, so operator scratch state grows a few
//! MB/hour even with **zero** subscribers (the queries are pinned) — independent of the
//! windowed source data, which IS bounded by the prune.
//!
//! This drives the same shape through many rounds of "new page edited (enters the
//! window) → oldest page pruned (its edit removed, draining its inner partition)". The
//! live partition set is bounded by the parent limit, so operator state must reach a
//! bounded steady state. Pre-fix it grows ~linearly with rounds (one leaked slot per
//! distinct page); post-fix it stays bounded.

use rindle::testkit::{run_push_walk_ast_view_state_entries, TableSpec};
use rindle::value::{owned_row as row, OwnedValue as V, SourceSchema};
use rindle::{Ast, CorrelatedSubquery, Correlation, Dir, OrderPart, SourceChange};

// page(id, last_ts); ranked by last_ts desc (newest-edited floats up), PK id.
fn page_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "last_ts"], vec![0], vec![(1, false), (0, true)])
}
// edit(id, page_id, ts); the nested relationship, ordered ts desc, PK id.
fn edit_schema() -> SourceSchema {
    SourceSchema::new(
        vec!["id", "page_id", "ts"],
        vec![0],
        vec![(2, false), (0, true)],
    )
}

/// `page.orderBy(last_ts desc).limit(3) { edits_recent: edit(page_id).orderBy(ts desc).limit(2) }`
/// — the `topEdited("latest")` shape, shrunk to a tiny window so the bounded steady
/// state is small and obvious.
fn board() -> Ast {
    Ast {
        table: "page".into(),
        order_by: vec![OrderPart("last_ts".into(), Dir::Desc)],
        limit: Some(3),
        related: vec![CorrelatedSubquery {
            correlation: Correlation {
                parent_field: vec!["id".into()],
                child_field: vec!["page_id".into()],
            },
            subquery: Box::new(Ast {
                table: "edit".into(),
                alias: Some("edits_recent".into()),
                order_by: vec![OrderPart("ts".into(), Dir::Desc)],
                limit: Some(2),
                ..Default::default()
            }),
            system: None,
        }],
        ..Default::default()
    }
}

#[test]
fn partitioned_take_does_not_leak_a_slot_per_pruned_partition() {
    const ROUNDS: i64 = 60;

    // Seed three pages (ids 0,1,2) each with one edit, so the parent window starts full.
    let pages: Vec<Vec<V>> = (0..3).map(|i| vec![V::Int(i), V::Int(i)]).collect();
    let edits: Vec<Vec<V>> = (0..3)
        .map(|i| vec![V::Int(i), V::Int(i), V::Int(i)])
        .collect();

    // Each round: a brand-new page is edited (enters the top of the window, hydrating its
    // inner edit-partition), which bumps the oldest of the previous top-3 out; then the
    // page that fell out `ROUNDS_BACK` ago is pruned — its edit removed (draining that
    // page's inner partition to empty) and the page row deleted. The distinct page count
    // climbs every round while the LIVE set stays ~3, so a per-partition leak shows as
    // operator state climbing with `ROUNDS`.
    let mut pushes: Vec<(&str, SourceChange)> = Vec::new();
    for k in 3..(3 + ROUNDS) {
        // New page k edited at time k (newest), with its single edit.
        pushes.push(("page", SourceChange::Add(row(vec![V::Int(k), V::Int(k)]))));
        pushes.push((
            "edit",
            SourceChange::Add(row(vec![V::Int(k), V::Int(k), V::Int(k)])),
        ));
        // Prune the page that aged out (id k-3): remove its edit (drains partition k-3 to
        // 0), then delete the page row.
        let old = k - 3;
        pushes.push((
            "edit",
            SourceChange::Remove(row(vec![V::Int(old), V::Int(old), V::Int(old)])),
        ));
        pushes.push((
            "page",
            SourceChange::Remove(row(vec![V::Int(old), V::Int(old)])),
        ));
    }

    let entries = run_push_walk_ast_view_state_entries(
        vec![
            TableSpec::new("page", page_schema(), pages),
            TableSpec::new("edit", edit_schema(), edits),
        ],
        &board(),
        pushes,
    )
    .expect("build+walk");

    // Live partitions are bounded by the parent limit (3) + a handful of in-flight slots
    // and the per-Take `maxBound` keys. A per-pruned-partition leak makes this grow with
    // ROUNDS (≈ 60+). A generous bound that still fails hard pre-fix:
    assert!(
        entries <= 16,
        "operator scratch state did not reach a bounded steady state under churn: \
         {entries} entries after {ROUNDS} rounds (a partitioned Take is leaking one \
         slot per pruned partition)"
    );
}
