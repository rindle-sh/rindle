#![cfg(feature = "testkit")]

//! Probe for a leak the existing `wiki_recency_churn_leak.rs` does NOT cover: the
//! demo's SECOND board, `recentEditors`, which puts a `where edits > 1` **Filter**
//! between the source and the recency `Take`:
//!
//!   `editor.where(edits > 1).orderBy(last_ts desc).limit(N)
//!        { edits_recent: edit(user).orderBy(ts desc).limit(M) }`
//!
//! Same churn as the pages board — many distinct editors, each passing the filter and
//! floating through the top-N by recency, nothing ever removed — and asserts total
//! operator scratch entries stay bounded by the view, not by the editor count. If the
//! Filter sitting under the Take breaks the join-side partition eviction, op-state grows
//! ~1 slot per distinct editor that ever floated through the board.

use rindle::value::{owned_row as row, OwnedValue as V, SourceSchema};
use rindle::{
    build_pipeline, Ast, Condition, CorrelatedSubquery, Correlation, Dir, Graph, Lit, Op,
    OrderPart, SimpleCondition, SourceChange, ValuePosition,
};

fn editor_schema() -> SourceSchema {
    // name(pk), edits, last_ts ; ranked by last_ts desc, name asc tiebreak
    SourceSchema::new(
        vec!["name", "edits", "last_ts"],
        vec![0],
        vec![(2, false), (0, true)],
    )
}
fn edit_schema_users() -> SourceSchema {
    // id(pk), user, ts ; newest first
    SourceSchema::new(
        vec!["id", "user", "ts"],
        vec![0],
        vec![(2, false), (0, true)],
    )
}

fn edits_gt_1() -> Condition {
    Condition::Simple(SimpleCondition {
        op: Op::Gt,
        left: ValuePosition::Column {
            name: "edits".into(),
        },
        right: ValuePosition::Literal {
            value: Lit::Number(1.0),
        },
    })
}

/// `editor.where(edits > 1).orderBy(last_ts desc).limit(3) { edits_recent: edit(user).orderBy(ts desc).limit(2) }`
fn editors_board() -> Ast {
    Ast {
        table: "editor".into(),
        r#where: Some(edits_gt_1()),
        order_by: vec![OrderPart("last_ts".into(), Dir::Desc)],
        limit: Some(3),
        related: vec![CorrelatedSubquery {
            correlation: Correlation {
                parent_field: vec!["name".into()],
                child_field: vec!["user".into()],
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
fn editors_recency_churn_state_bounded() {
    let mut g = Graph::new();
    let editor_id = g.add_source(editor_schema(), vec![]);
    let edit_id = g.add_source(edit_schema_users(), vec![]);

    let ast = editors_board();
    let top = {
        let resolve = |t: &str| match t {
            "editor" => Some((editor_id, editor_schema())),
            "edit" => Some((edit_id, edit_schema_users())),
            _ => None,
        };
        build_pipeline(&mut g, &ast, &resolve).expect("build")
    };
    let sink = g.add_change_sink(top);
    g.set_sink_edge(top, sink);
    let _ = g.try_hydrate_change_sink(sink).unwrap();

    let report = |g: &Graph| -> usize { g.storage_snapshot().iter().map(Vec::len).sum() };

    let mut after_warm = 0usize;
    const EDITORS: i64 = 60;
    for k in 0..EDITORS {
        let ts = (k + 1) * 1000;
        // Editor k passes the filter (edits = 2) and enters the top-3 by recency, pushing an
        // older editor out — nothing is ever removed.
        g.source_push_isolated(
            editor_id,
            SourceChange::Add(row(vec![V::Int(k), V::Int(2), V::Int(ts)])),
        )
        .expect("editor add");
        let _ = g.take_sink_changes(sink);
        // Two edits for this editor (its newest-edits sub-board), distinct ids.
        for j in 0..2 {
            g.source_push_isolated(
                edit_id,
                SourceChange::Add(row(vec![V::Int(k * 10 + j), V::Int(k), V::Int(ts)])),
            )
            .expect("edit add");
            let _ = g.take_sink_changes(sink);
        }
        if k == 9 {
            after_warm = report(&g);
        }
    }
    let after_all = report(&g);
    println!("editors op-state entries: after 10 = {after_warm}, after {EDITORS} = {after_all}");
    assert!(
        after_all <= after_warm + 4,
        "operator state grew with distinct-editor count (recency churn under a Filter): \
         {after_warm} -> {after_all} over {EDITORS} editors (expected ~bounded by the limit-3 view)"
    );
}
