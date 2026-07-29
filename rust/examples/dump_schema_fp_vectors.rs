//! Dump `schema_fp` conformance vectors from the real engine (`src/wire_schema.rs`) for the
//! TS `schemaFp` port in `@rindle/remote` (`protocol.ts`). Each vector is the **JS-shaped**
//! (camelCase, bare) wire schema the TS side consumes plus the engine's fingerprint as hex —
//! so the TS port is checked against ground truth, proving cross-language parity (and future
//! Rust-server interop).
//!
//!   cargo run --example dump_schema_fp_vectors > packages/remote/test/fixtures/schema-fp-vectors.json

use rindle::{schema_fp, to_wire, OwnedValue, RelDef, Schema, WireSchema};
use serde_json::{json, Value};

/// An `OwnedValue` → the **bare** JS cell the TS side stores (the lossy marshal collapse:
/// `Int`/`Float`→number, `Str`/`Json`→string), used for a projection's empty-identity.
fn value_to_bare_js(v: &OwnedValue) -> Value {
    match v {
        OwnedValue::Null => Value::Null,
        OwnedValue::Bool(b) => Value::Bool(*b),
        OwnedValue::Int(i) => json!(i),
        OwnedValue::Float(f) => json!(f),
        OwnedValue::Str(s) | OwnedValue::Json(s) => Value::String(s.to_string()),
        // The projection sentinel never occurs as a scalar-projection identity; in this lossy
        // bare-JS form it collapses to `null` (JS `undefined` has no JSON form).
        OwnedValue::Absent => Value::Null,
    }
}

/// A `WireSchema` → the camelCase JS object the TS `schemaFp` consumes (matches the napi /
/// wasm marshallers): `{ columns, primaryKey, sort: [col, asc][], singular, relationships }`.
/// A scalar-projected relationship (`REDUCE-DESIGN.md` §9) also carries
/// `project: { col, identity }` (identity as a bare cell).
fn wire_to_js(ws: &WireSchema) -> Value {
    json!({
        "columns": ws.columns.iter().map(|c| c.to_string()).collect::<Vec<_>>(),
        "primaryKey": ws.primary_key.clone(),
        "sort": ws.sort.iter().map(|&(c, asc)| json!([c, asc])).collect::<Vec<_>>(),
        "singular": ws.singular,
        "relationships": ws
            .relationships
            .iter()
            .map(|r| {
                json!({
                    "name": r.name.to_string(),
                    "slot": r.slot,
                    "child": r.child.as_ref().map(wire_to_js).unwrap_or(Value::Null),
                    "project": r.project.as_ref().map(|p| json!({
                        "col": p.col,
                        "identity": value_to_bare_js(&p.identity),
                    })).unwrap_or(Value::Null),
                })
            })
            .collect::<Vec<_>>(),
    })
}

fn vector(schema: &Schema) -> Value {
    let ws = to_wire(schema);
    json!({ "schema": wire_to_js(&ws), "fp": schema_fp(&ws).to_string() })
}

fn comment_schema() -> Schema {
    Schema::new(vec!["id", "issue", "val"], vec![0], vec![(0, true)])
}

fn nested() -> Schema {
    Schema::new(vec!["id", "val"], vec![0], vec![(1, true), (0, true)]).with_relationships(vec![
        RelDef::related("comments", comment_schema()),
        RelDef::new("gate"), // out-of-view / gating slot (child: None)
    ])
}

fn main() {
    let mut singular = nested();
    singular.singular = true;

    // A scalar-projected relationship aggregate (`REDUCE-DESIGN.md` §9): issue
    // { commentCount: count(comments) } — the singular [issueID, count] child surfaced
    // as a scalar (project col 1 = count, empty-identity 0).
    let mut agg_child = Schema::new(vec!["issueID", "count"], vec![0], vec![(0, true)]);
    agg_child.singular = true;
    let projected =
        Schema::new(vec!["id", "val"], vec![0], vec![(0, true)]).with_relationships(vec![
            RelDef::related("commentCount", agg_child).project_scalar(1, OwnedValue::Int(0)),
        ]);

    let vectors = vec![
        // Flat, single PK sort.
        vector(&Schema::new(vec!["id", "val"], vec![0], vec![(0, true)])),
        // Multi-column resolved sort (val asc, id asc), composite PK.
        vector(&Schema::new(
            vec!["a", "b", "c"],
            vec![0, 1],
            vec![(2, false), (0, true), (1, true)],
        )),
        // Nested in-view relationship + a gating slot.
        vector(&nested()),
        // Same, singular (the `.one()` flag flips the fingerprint).
        vector(&singular),
        // A scalar-projected relationship aggregate (REDUCE-DESIGN.md §9).
        vector(&projected),
    ];

    println!(
        "{}",
        serde_json::to_string_pretty(&Value::Array(vectors)).unwrap()
    );
}
