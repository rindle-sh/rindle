#![cfg(feature = "testkit")]

//! Companion to `take_partition_leak.rs`, closing its blind spots for the wiki demo
//! (`apps/wiki-demo`). That test drives the `topEdited` shape through the **View**
//! build with a single edit per page; production (the `rindle-server` worker engine)
//! runs the **Catch** build — `add_change_sink`, whose fetch-forcing populates *more*
//! operator scratch state than the View — and pages carry *many* edits pruned together,
//! while the second board (`topRecentEditors`) adds a `where edits > 1` Filter under the
//! Take. This drives BOTH boards through the Catch build with multi-edit partitions over
//! many rounds of churn and asserts operator scratch state reaches a bounded steady
//! state (the partitioned-`Take` drained-slot leak, fixed in `op/take.rs`, would make it
//! grow ~linearly with the distinct-key count).

use std::collections::HashMap;

use rindle::value::{owned_row as row, OwnedValue as V, SourceSchema};
use rindle::{
    build_pipeline, Ast, Condition, CorrelatedSubquery, Correlation, Dir, Graph, Lit, NodeId, Op,
    OrderPart, SimpleCondition, SourceChange, ValuePosition,
};

fn page_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "last_ts"], vec![0], vec![(1, false), (0, true)])
}
fn editor_schema() -> SourceSchema {
    SourceSchema::new(
        vec!["name", "edits", "last_ts"],
        vec![0],
        vec![(2, false), (0, true)],
    )
}
fn edit_schema_editors() -> SourceSchema {
    SourceSchema::new(
        vec!["id", "page_id", "user", "ts"],
        vec![0],
        vec![(3, false), (0, true)],
    )
}
fn edit_schema_pages() -> SourceSchema {
    SourceSchema::new(
        vec!["id", "page_id", "ts"],
        vec![0],
        vec![(2, false), (0, true)],
    )
}

/// Build the production-style **Catch** pipeline (`add_change_sink`, what the
/// `rindle-server` worker engine uses), hydrate, replay `pushes` draining the sink each
/// step (one commit = one drain), and return the total operator scratch-state entry
/// count — a partitioned-limiter slot leak shows up here as growth under churn.
fn catch_entries(
    sources: Vec<(&'static str, SourceSchema, Vec<Vec<V>>)>,
    ast: &Ast,
    pushes: Vec<(&'static str, SourceChange)>,
) -> usize {
    let mut g = Graph::new();
    let mut ids: HashMap<&'static str, (NodeId, SourceSchema)> = HashMap::new();
    for (name, schema, rows) in sources {
        let initial = rows.into_iter().map(row).collect();
        let id = g.add_source(schema.clone(), initial);
        ids.insert(name, (id, schema));
    }
    let top = {
        let resolve = |t: &str| ids.get(t).map(|(id, sc)| (*id, sc.clone()));
        build_pipeline(&mut g, ast, &resolve).expect("build")
    };
    let sink = g.add_change_sink(top);
    g.set_sink_edge(top, sink);
    let _ = g.try_hydrate_change_sink(sink).unwrap();
    for (name, change) in pushes {
        let (id, _) = ids[name];
        g.source_push_isolated(id, change).expect("push");
        let _ = g.take_sink_changes(sink);
    }
    g.storage_snapshot().iter().map(Vec::len).sum()
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

/// `editor.where(edits>1).orderBy(last_ts desc).limit(3) { edits_recent: edit(user).orderBy(ts desc).limit(2) }`
fn editors_board() -> Ast {
    Ast {
        table: "editor".into(),
        r#where: Some(Condition::Simple(SimpleCondition {
            left: ValuePosition::Column {
                name: "edits".into(),
            },
            op: Op::Gt,
            right: ValuePosition::Literal {
                value: Lit::Number(1.0),
            },
        })),
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

/// Edits per page/editor — exercises a partition that holds several rows (the limit is 2,
/// so most are beyond the window) and is then pruned as a batch.
const EPP: i64 = 6;
const ROUNDS: i64 = 60;

/// edit ids for the round-`old` page/editor (3 seeds use ids 0..3*EPP; round k uses the
/// next EPP-block). Returned with each id's deterministic `ts`.
fn aged_out_edit_ids(old: i64) -> Vec<(i64, i64)> {
    let (start, ts_base) = if old < 3 {
        (old * EPP, old * 100)
    } else {
        (3 * EPP + (old - 3) * EPP, old * 1000)
    };
    (0..EPP).map(|j| (start + j, ts_base + j)).collect()
}

#[test]
fn pages_board_catch_state_bounded() {
    let pages: Vec<Vec<V>> = (0..3).map(|i| vec![V::Int(i), V::Int(i)]).collect();
    let mut edits: Vec<Vec<V>> = Vec::new();
    let mut eid = 0i64;
    for p in 0..3 {
        for j in 0..EPP {
            edits.push(vec![V::Int(eid), V::Int(p), V::Int(p * 100 + j)]);
            eid += 1;
        }
    }

    let mut pushes: Vec<(&str, SourceChange)> = Vec::new();
    for k in 3..(3 + ROUNDS) {
        pushes.push((
            "page",
            SourceChange::Add(row(vec![V::Int(k), V::Int(k * 1000)])),
        ));
        for j in 0..EPP {
            pushes.push((
                "edit",
                SourceChange::Add(row(vec![V::Int(eid), V::Int(k), V::Int(k * 1000 + j)])),
            ));
            eid += 1;
        }
        let old = k - 3;
        for (id, ts) in aged_out_edit_ids(old) {
            pushes.push((
                "edit",
                SourceChange::Remove(row(vec![V::Int(id), V::Int(old), V::Int(ts)])),
            ));
        }
        let last_ts = if old < 3 { old } else { old * 1000 };
        pushes.push((
            "page",
            SourceChange::Remove(row(vec![V::Int(old), V::Int(last_ts)])),
        ));
    }

    let entries = catch_entries(
        vec![
            ("page", page_schema(), pages),
            ("edit", edit_schema_pages(), edits),
        ],
        &pages_board(),
        pushes,
    );
    assert!(
        entries <= 40,
        "pages board (Catch build) operator state did not stay bounded under churn: \
         {entries} entries after {ROUNDS} rounds"
    );
}

#[test]
fn editors_board_catch_state_bounded() {
    let editors: Vec<Vec<V>> = (0..3)
        .map(|i| vec![V::Int(i), V::Int(EPP), V::Int(i)])
        .collect();
    let mut edits: Vec<Vec<V>> = Vec::new();
    let mut eid = 0i64;
    for p in 0..3 {
        for j in 0..EPP {
            edits.push(vec![V::Int(eid), V::Int(p), V::Int(p), V::Int(p * 100 + j)]);
            eid += 1;
        }
    }

    let mut pushes: Vec<(&str, SourceChange)> = Vec::new();
    for k in 3..(3 + ROUNDS) {
        pushes.push((
            "editor",
            SourceChange::Add(row(vec![V::Int(k), V::Int(EPP), V::Int(k * 1000)])),
        ));
        for j in 0..EPP {
            pushes.push((
                "edit",
                SourceChange::Add(row(vec![
                    V::Int(eid),
                    V::Int(k),
                    V::Int(k),
                    V::Int(k * 1000 + j),
                ])),
            ));
            eid += 1;
        }
        let old = k - 3;
        for (id, ts) in aged_out_edit_ids(old) {
            pushes.push((
                "edit",
                SourceChange::Remove(row(vec![V::Int(id), V::Int(old), V::Int(old), V::Int(ts)])),
            ));
        }
        let last_ts = if old < 3 { old } else { old * 1000 };
        pushes.push((
            "editor",
            SourceChange::Remove(row(vec![V::Int(old), V::Int(EPP), V::Int(last_ts)])),
        ));
    }

    let entries = catch_entries(
        vec![
            ("editor", editor_schema(), editors),
            ("edit", edit_schema_editors(), edits),
        ],
        &editors_board(),
        pushes,
    );
    assert!(
        entries <= 40,
        "editors board (Catch build) operator state did not stay bounded under churn: \
         {entries} entries after {ROUNDS} rounds"
    );
}
