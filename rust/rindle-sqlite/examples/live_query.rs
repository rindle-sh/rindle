//! Quickstart: a live, incrementally-maintained SQLite query using only the open
//! engine (`rindle` + `rindle-sqlite`). Register a table as a source, hydrate a
//! materialized view, and push write-through changes that update the view by the
//! delta — never re-running the query. Backs the `product-page` `quickstart` doc.
//!
//!     cargo run -p rindle-sqlite --example live_query

use std::collections::HashMap;
use std::rc::Rc;

use rindle::{
    build_pipeline, owned_row, table, view_schema, Graph, OwnedValue, SourceChange, SourceSchema,
    Value, ValueType,
};
use rindle_sqlite::{ColumnDef, GraphTableSourceExt, TableSource};
use rusqlite::Connection;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. An ordinary SQLite database + schema, with a UNIQUE index on the primary key.
    let db = Rc::new(Connection::open_in_memory()?);
    db.execute_batch(
        "CREATE TABLE issues (id INTEGER NOT NULL, title TEXT, open BOOLEAN);
         CREATE UNIQUE INDEX issues_pk ON issues (id);",
    )?;

    // 2. Describe the columns (name, engine type, nullable) and the primary key.
    let columns = vec![
        ColumnDef {
            name: "id".into(),
            ty: ValueType::Number,
            optional: false,
        },
        ColumnDef {
            name: "title".into(),
            ty: ValueType::String,
            optional: true,
        },
        ColumnDef {
            name: "open".into(),
            ty: ValueType::Boolean,
            optional: true,
        },
    ];
    // The SourceSchema (columns, primary-key indices, default sort) is what queries
    // resolve against — reused below for both lowering and the view.
    let schema = SourceSchema::new(vec!["id", "title", "open"], vec![0], vec![(0, true)]);

    // 3. Register the table as a source on the graph.
    let mut graph = Graph::new();
    let issues_src = graph.add_table_source(TableSource::new_with_schema(
        db.clone(),
        "issues",
        columns,
        vec![0],
        schema.clone(),
    ));

    // `resolve` maps each table name to (source NodeId, SourceSchema).
    let mut sources = HashMap::new();
    sources.insert("issues", (issues_src, schema));
    let resolve = |name: &str| sources.get(name).cloned();

    // Define the query: all OPEN issues.
    let ast = table("issues").r#where("open", true).build();

    // Build the pipeline and hydrate a view.
    let top = build_pipeline(&mut graph, &ast, &resolve).expect("build the pipeline");
    let vschema = view_schema(&ast, &resolve).expect("derive the view schema");
    let view = graph.add_view(top, vschema);
    graph.set_sink_edge(top, view);
    graph.try_hydrate(view)?; // materialize the initial result set

    // Push write-through changes: each writes SQLite AND folds the delta into the view.
    graph.try_source_push(
        issues_src,
        SourceChange::Add(owned_row(vec![
            OwnedValue::Int(1),
            OwnedValue::str("first"),
            OwnedValue::Bool(true),
        ])),
    )?;
    graph.try_source_push(
        issues_src,
        SourceChange::Add(owned_row(vec![
            OwnedValue::Int(2),
            OwnedValue::str("second"),
            OwnedValue::Bool(true),
        ])),
    )?;
    // Close issue 2 — an Edit that no longer matches `open = true`, so the view drops it.
    graph.try_source_push(
        issues_src,
        SourceChange::Edit {
            old: owned_row(vec![
                OwnedValue::Int(2),
                OwnedValue::str("second"),
                OwnedValue::Bool(true),
            ]),
            row: owned_row(vec![
                OwnedValue::Int(2),
                OwnedValue::str("second"),
                OwnedValue::Bool(false),
            ]),
        },
    )?;

    graph.flush_view(view); // close the transaction and fire the view's listeners
    let data = graph.view_data(view);

    println!("open issues ({}):", data.items.len());
    for entry in &data.items {
        println!("  {:?}", entry.row.to_value_vec());
    }

    // The view now holds exactly issue 1 — reached by the delta alone, no rescan.
    assert_eq!(data.items.len(), 1, "only issue 1 stays open");
    let pk = match data.items[0].row.col(0) {
        Value::Int(i) => i,
        Value::Float(f) => f as i64,
        other => panic!("non-numeric primary key: {other:?}"),
    };
    assert_eq!(pk, 1, "the surviving open issue is #1");
    Ok(())
}
