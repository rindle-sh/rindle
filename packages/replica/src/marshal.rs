//! Rust ↔ bare JS marshalling for the napi boundary. The output shape is **byte-for-byte
//! the wasm `Db`'s** (`src/wasm/marshal.rs`): bare cells (`number | string | boolean |
//! null`), camelCase keys, `{ tag }`-tagged ops — so the SAME `@rindle/client` `ArrayView`
//! folds either backend's stream. Built as `serde_json::Value` (napi's `serde-json` feature
//! converts it to a JS object on return).

use rindle::{FlatChange, FlatOp, OwnedValue, WireNode, WireRel, WireSchema};
use serde_json::{json, Map, Value};

// The normalized-protocol JSON rendering (cells, ops, batches, hellos) lives in
// `rindle_replica::wire_json` now — shared with the Rust daemon's ws front so both emit
// the identical wire. Re-exported here so the napi glue keeps one import site.
pub use rindle_replica::wire_json::{
    json_to_owned, normalized_batch_to_json, normalized_hello_to_json, owned_to_json,
};

fn wire_row_to_json(row: &[OwnedValue]) -> Value {
    Value::Array(row.iter().map(owned_to_json).collect())
}

fn wire_node_to_json(node: &WireNode) -> Value {
    let rels: Vec<Value> = node
        .rels
        .iter()
        .map(|(rel, children)| {
            json!({
                "rel": rel.0 as f64,
                "children": children.iter().map(wire_node_to_json).collect::<Vec<_>>(),
            })
        })
        .collect();
    json!({ "row": wire_row_to_json(&node.row), "rels": rels })
}

fn flat_change_to_json(c: &FlatChange) -> Value {
    let path: Vec<Value> = c
        .path
        .iter()
        .map(|seg| json!({ "rel": seg.rel.0 as f64, "parentRow": wire_row_to_json(&seg.parent_row) }))
        .collect();
    let op = match &c.op {
        FlatOp::Add(node) => json!({ "tag": "add", "node": wire_node_to_json(node) }),
        FlatOp::Remove { row } => json!({ "tag": "remove", "row": wire_row_to_json(row) }),
        FlatOp::Edit { old, new } => {
            json!({ "tag": "edit", "old": wire_row_to_json(old), "new": wire_row_to_json(new) })
        }
    };
    json!({ "path": path, "op": op })
}

/// A batch of flat changes → a JS array (the hydrate snapshot, or one commit's events).
pub fn flat_changes_to_json(changes: &[FlatChange]) -> Value {
    Value::Array(changes.iter().map(flat_change_to_json).collect())
}

fn wire_rel_to_json(r: &WireRel) -> Value {
    let child = match &r.child {
        Some(cs) => wire_schema_to_json(cs),
        None => Value::Null,
    };
    // A scalar-projected relationship aggregate (`REDUCE-DESIGN.md` §9) carries
    // `project: { col, identity }` (identity as a bare cell); `null` otherwise.
    let project = match &r.project {
        Some(p) => json!({ "col": p.col as f64, "identity": owned_to_json(&p.identity) }),
        None => Value::Null,
    };
    json!({ "name": r.name.to_string(), "slot": r.slot as f64, "child": child, "project": project })
}

/// A `WireSchema` → the camelCase JS object the `ArrayView` builds from (handed once in
/// `hello`): `{ columns, primaryKey, sort: [col, asc][], singular, relationships }`.
pub fn wire_schema_to_json(ws: &WireSchema) -> Value {
    let sort: Vec<Value> = ws
        .sort
        .iter()
        .map(|&(c, asc)| json!([c as f64, asc]))
        .collect();
    let mut obj = Map::new();
    obj.insert(
        "columns".into(),
        Value::Array(
            ws.columns
                .iter()
                .map(|c| Value::String(c.to_string()))
                .collect(),
        ),
    );
    obj.insert(
        "primaryKey".into(),
        Value::Array(ws.primary_key.iter().map(|&c| json!(c as f64)).collect()),
    );
    obj.insert("sort".into(), Value::Array(sort));
    obj.insert("singular".into(), Value::Bool(ws.singular));
    obj.insert(
        "relationships".into(),
        Value::Array(ws.relationships.iter().map(wire_rel_to_json).collect()),
    );
    Value::Object(obj)
}
