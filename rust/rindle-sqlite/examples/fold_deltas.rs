//! Fold the raw delta stream yourself, then prove the folded view equals a fresh
//! query — using only the open engine (`rindle` + `rindle-sqlite`). Attach a change
//! sink, fold each `CaughtChange` into your own map, and assert it matches a fresh
//! SELECT. Backs the `product-page` `example-rust` doc.
//!
//!     cargo run -p rindle-sqlite --example fold_deltas

use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;

use rindle::{
    build_pipeline, owned_row, table, CaughtChange, Graph, OwnedRow, OwnedValue, SourceChange,
    SourceSchema, Value, ValueType,
};
use rindle_sqlite::{ColumnDef, GraphTableSourceExt, TableSource};
use rusqlite::Connection;
use serde_json::json;

const COLS: [&str; 4] = ["id", "title", "priority", "open"];

struct Consumer {
    view: BTreeMap<i64, serde_json::Value>, // id -> the row, as you'd render it
}

impl Consumer {
    fn apply(&mut self, changes: &[CaughtChange]) {
        for ch in changes {
            match ch {
                // A row entered the result set.
                CaughtChange::Add(n) => {
                    self.view.insert(pk(&n.row), row_to_json(&n.row));
                }
                // A row left it (deleted, or no longer matches the filter).
                CaughtChange::Remove(n) => {
                    self.view.remove(&pk(&n.row));
                }
                // A row that stayed but changed. The key can move, so drop old then insert new.
                CaughtChange::Edit { old, row } => {
                    self.view.remove(&pk(old));
                    self.view.insert(pk(row), row_to_json(row));
                }
                // Nested-relationship diffs — only a `sub_as(..)` query emits these.
                CaughtChange::Child { .. } => {}
            }
        }
    }
}

// Cells are read positionally with `row.col(c)`. The engine carries every number as
// `f64`, so an integer primary key can arrive as `Float(1.0)` — normalize it.
fn pk(row: &OwnedRow) -> i64 {
    match row.col(0) {
        Value::Int(i) => i,
        Value::Float(f) => f as i64,
        other => panic!("non-numeric primary key: {other:?}"),
    }
}

fn row_to_json(row: &OwnedRow) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    for (name, cell) in COLS.iter().zip(row.cells()) {
        let v = match cell {
            Value::Int(i) => json!(i),
            Value::Float(f) if f.fract() == 0.0 => json!(f as i64),
            Value::Float(f) => json!(f),
            Value::Bool(b) => json!(b),
            Value::Str(s) => json!(std::str::from_utf8(s).unwrap()),
            _ => serde_json::Value::Null,
        };
        obj.insert((*name).to_string(), v);
    }
    serde_json::Value::Object(obj)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Rc::new(Connection::open_in_memory()?);
    db.execute_batch(
        "CREATE TABLE issues (id INTEGER NOT NULL, title TEXT NOT NULL,
                              priority INTEGER NOT NULL, open BOOLEAN NOT NULL);
         CREATE UNIQUE INDEX issues_pk ON issues (id);",
    )?;

    let columns = vec![
        ColumnDef {
            name: "id".into(),
            ty: ValueType::Number,
            optional: false,
        },
        ColumnDef {
            name: "title".into(),
            ty: ValueType::String,
            optional: false,
        },
        ColumnDef {
            name: "priority".into(),
            ty: ValueType::Number,
            optional: false,
        },
        ColumnDef {
            name: "open".into(),
            ty: ValueType::Boolean,
            optional: false,
        },
    ];
    let schema = SourceSchema::new(
        vec!["id", "title", "priority", "open"],
        vec![0],
        vec![(0, true)],
    );

    let mut graph = Graph::new();
    let issues_src = graph.add_table_source(TableSource::new_with_schema(
        db.clone(),
        "issues",
        columns,
        vec![0],
        schema.clone(),
    ));
    let sources = HashMap::from([("issues", (issues_src, schema))]);
    let resolve = |t: &str| sources.get(t).cloned();

    // The live query: the OPEN issues, maintained incrementally.
    let ast = table("issues").r#where("open", true).build();
    let top = build_pipeline(&mut graph, &ast, &resolve).expect("build");

    // A change sink hands you the raw delta stream (instead of a materialized View).
    let sink = graph.add_change_sink(top);
    graph.set_sink_edge(top, sink);

    let mut consumer = Consumer {
        view: BTreeMap::new(),
    };
    consumer.apply(&graph.try_hydrate_change_sink(sink).unwrap()); // fires once: the cold-start snapshot

    // Insert three issues (one already closed). Each push is write-through.
    graph.try_source_push(
        issues_src,
        SourceChange::Add(owned_row(vec![
            OwnedValue::Int(1),
            OwnedValue::str("login button misaligned"),
            OwnedValue::Int(2),
            OwnedValue::Bool(true),
        ])),
    )?;
    graph.try_source_push(
        issues_src,
        SourceChange::Add(owned_row(vec![
            OwnedValue::Int(2),
            OwnedValue::str("slow dashboard query"),
            OwnedValue::Int(1),
            OwnedValue::Bool(true),
        ])),
    )?;
    graph.try_source_push(
        issues_src,
        SourceChange::Add(owned_row(vec![
            OwnedValue::Int(3),
            OwnedValue::str("typo in footer"),
            OwnedValue::Int(3),
            OwnedValue::Bool(false), // closed → never enters
        ])),
    )?;
    consumer.apply(&graph.take_sink_changes(sink));

    // Close issue 1 — the consumer sees a Remove for #1, no rescan.
    graph.try_source_push(
        issues_src,
        SourceChange::Edit {
            old: owned_row(vec![
                OwnedValue::Int(1),
                OwnedValue::str("login button misaligned"),
                OwnedValue::Int(2),
                OwnedValue::Bool(true),
            ]),
            row: owned_row(vec![
                OwnedValue::Int(1),
                OwnedValue::str("login button misaligned"),
                OwnedValue::Int(2),
                OwnedValue::Bool(false),
            ]),
        },
    )?;
    consumer.apply(&graph.take_sink_changes(sink));

    // The payoff: the delta-folded view must equal a fresh query over the same
    // SQLite database the write-through pushes populated.
    let mut stmt =
        db.prepare("SELECT id, title, priority, open FROM issues WHERE open = 1 ORDER BY id")?;
    let fresh: Vec<serde_json::Value> = stmt
        .query_map([], |r| {
            Ok(json!({
                "id": r.get::<_, i64>(0)?,
                "title": r.get::<_, String>(1)?,
                "priority": r.get::<_, i64>(2)?,
                "open": r.get::<_, bool>(3)?,
            }))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    let folded: Vec<serde_json::Value> = consumer.view.values().cloned().collect();
    assert_eq!(
        folded, fresh,
        "the delta-folded view must equal a fresh query — the IVM guarantee"
    );

    println!("folded view == fresh query ({} row(s)):", folded.len());
    for row in &folded {
        println!("  {row}");
    }
    Ok(())
}
