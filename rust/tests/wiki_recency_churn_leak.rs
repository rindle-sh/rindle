#![cfg(feature = "testkit")]

//! Reproduction probe for the *recency-churn* leak the lockstep
//! `wiki_boards_state_bounded.rs` test misses. There, each round removes the old
//! page AND its edits in lockstep, so every retired partition drains to empty (the
//! already-fixed `Take` drain path). Production ranks the parent by `last_ts`, so a
//! page leaves the top-N **by recency** while its edits are still inside the retention
//! window (not yet pruned). Its child `Take` partition was hydrated when it entered
//! the view; nothing tore it down when it left — so operator state grew one zombie
//! slot per distinct page that ever floated through the board (op-state ∝ window, not
//! ∝ view). The join-side eviction in `graph.rs` (`evict_child_partitions`, driven by
//! a parent `Remove` on a primary-keyed correlation) fixes it. This drives exactly the
//! missed shape: many distinct pages, each edited once with an increasing `last_ts`,
//! churning through a parent `limit(3)` — with NO removals at all — and asserts total
//! operator scratch entries stay bounded by the (limit-3) view, not the page count.

use rindle::value::{owned_row as row, OwnedValue as V, SourceSchema};
use rindle::{
    build_pipeline, Ast, CorrelatedSubquery, Correlation, Dir, Graph, OrderPart, SourceChange,
};

fn page_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "last_ts"], vec![0], vec![(1, false), (0, true)])
}
fn edit_schema_pages() -> SourceSchema {
    SourceSchema::new(
        vec!["id", "page_id", "ts"],
        vec![0],
        vec![(2, false), (0, true)],
    )
}

/// `page.orderBy(last_ts desc).limit(3) { edits_recent: edit(page_id).orderBy(ts desc).limit(2) }`
fn pages_board() -> Ast {
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
fn recency_churn_state_bounded() {
    let mut g = Graph::new();
    let page_id = g.add_source(page_schema(), vec![]);
    let edit_id = g.add_source(edit_schema_pages(), vec![]);

    let ast = pages_board();
    let top = {
        let resolve = |t: &str| match t {
            "page" => Some((page_id, page_schema())),
            "edit" => Some((edit_id, edit_schema_pages())),
            _ => None,
        };
        build_pipeline(&mut g, &ast, &resolve).expect("build")
    };
    let sink = g.add_change_sink(top);
    g.set_sink_edge(top, sink);
    let _ = g.try_hydrate_change_sink(sink).unwrap();

    let report = |g: &Graph| -> usize { g.storage_snapshot().iter().map(Vec::len).sum() };

    let mut after_warm = 0usize;
    const PAGES: i64 = 60;
    for k in 0..PAGES {
        // Each page is edited once, with a strictly increasing last_ts/ts, so it enters
        // the top-3 by recency and pushes an older page out — but nothing is ever removed.
        let ts = (k + 1) * 1000;
        g.source_push_isolated(page_id, SourceChange::Add(row(vec![V::Int(k), V::Int(ts)])))
            .expect("page add");
        let _ = g.take_sink_changes(sink);
        g.source_push_isolated(
            edit_id,
            SourceChange::Add(row(vec![V::Int(k), V::Int(k), V::Int(ts)])),
        )
        .expect("edit add");
        let _ = g.take_sink_changes(sink);
        if k == 9 {
            after_warm = report(&g);
        }
    }
    let after_all = report(&g);
    println!("op-state entries: after 10 pages = {after_warm}, after {PAGES} pages = {after_all}");
    assert!(
        after_all <= after_warm + 2,
        "operator state grew with distinct-page count (recency churn): \
         {after_warm} -> {after_all} over {PAGES} pages (expected ~bounded by the limit-3 view)"
    );
}
