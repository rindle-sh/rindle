//! Value / error / schema / view-tree marshalling across the wasm boundary
//! (productionization WS01.2).
//!
//! Every conversion is **panic-free at the boundary**: a malformed JS input yields a
//! thrown JS `Error` (a `JsValue`), never a Rust panic / `panic = "abort"` wasm trap.
//! The crate-internal `RindleError`/`BuildError` are surfaced as JS `Error`s carrying a
//! stable `.kind` string (spec 00 §7.1: structured errors across the boundary, not
//! panics).

use std::sync::Arc;

use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

use crate::builder::BuildError;
use crate::change::SourceChange;
use crate::changes::CaughtNode;
use crate::error::RindleError;
use crate::value::{OwnedRow, OwnedValue, RelDef, RelId, Schema, Sort, SourceSchema};
use crate::view::{Entry, ResultType, ViewData};

// ---------------------------------------------------------------------------
// Errors → JS
// ---------------------------------------------------------------------------

/// Build a JS `Error` with a `.kind` string tag and a message. Returned as the `Err`
/// of every fallible boundary fn so wasm-bindgen *throws* it (never aborts).
pub fn make_error(kind: &str, message: &str) -> JsValue {
    let err = js_sys::Error::new(message);
    let _ = js_sys::Reflect::set(&err, &JsValue::from_str("kind"), &JsValue::from_str(kind));
    err.into()
}

/// `RindleError` → a thrown JS `Error` `{ message, kind }`.
pub fn rindle_error_to_js(e: &RindleError) -> JsValue {
    let kind = match e {
        RindleError::UnsupportedValue(_) => "unsupported_value",
        RindleError::InvalidUtf8 { .. } => "invalid_utf8",
        RindleError::Storage(_) => "storage",
        RindleError::ConsistencyViolation { .. } => "consistency_violation",
        RindleError::SchemaViolation(_) => "schema_violation",
        RindleError::Sqlite { .. } => "sqlite",
        // Unreachable in practice on wasm (the client never arms a push deadline —
        // FOLLOWER-LAG-SHED §6.6), but the taxonomy stays total.
        RindleError::PushDeadlineExceeded { .. } => "push_deadline",
    };
    make_error(kind, &e.to_string())
}

/// `BuildError` → a thrown JS `Error` whose `.kind` is the rejected-shape taxonomy
/// (so a JS client can branch on `unsupported` vs `unknown_table`, etc.).
pub fn build_error_to_js(e: &BuildError) -> JsValue {
    let kind = match e {
        BuildError::UnknownColumn(_) => "unknown_column",
        // The 226 §8 int64 gate: unreachable from the wasm home today (a JS-supplied
        // schema carries no column types until Stage E), mapped for exhaustiveness.
        BuildError::Unsupported(_) | BuildError::Int64ColumnUnsupported { .. } => "unsupported",
        BuildError::Invalid(_) => "invalid",
        BuildError::UnknownTable(_) => "unknown_table",
        BuildError::UnknownRelationship(_) => "unknown_relationship",
    };
    make_error(kind, &e.to_string())
}

// ---------------------------------------------------------------------------
// Value: Rust → JS
// ---------------------------------------------------------------------------

/// `OwnedValue` → `JsValue`. `Int(i64)` crosses as a JS `number` (f64): a magnitude
/// `> 2^53` loses precision (the single-`number` model, spec R10) — which is why the
/// `strict_i64` walk (`js_safe`, MANDATORY since design 226 Stage C, §8) refuses
/// every frame/tree containing such an `Int` BEFORE it reaches this conversion.
pub fn owned_value_to_js(v: &OwnedValue) -> JsValue {
    match v {
        // `Absent` maps to JS `undefined` — the presence ABI (`PROJECTION-SUPPORT-DESIGN.md`
        // §5.3). On the **flat** path a projected query's union row carries `Absent` for a
        // column it did not sync; the JS `ArrayView` omits an `undefined` cell from the result
        // object (§6). `null` stays `null` (present-and-null). For a `'*'` row no cell is ever
        // `Absent`, so this arm is never taken there (§7).
        OwnedValue::Absent => JsValue::UNDEFINED,
        OwnedValue::Null => JsValue::NULL,
        OwnedValue::Bool(b) => JsValue::from_bool(*b),
        OwnedValue::Int(i) => JsValue::from_f64(*i as f64),
        OwnedValue::Float(f) => JsValue::from_f64(*f),
        OwnedValue::Str(s) => JsValue::from_str(s),
        // `Json` passes through as its source string (documented default); a consumer
        // can `JSON.parse` it. Parsing here would force a shape the engine doesn't own.
        OwnedValue::Json(s) => JsValue::from_str(s),
    }
}

/// [`owned_value_to_js`] over a flat row's borrowed cell — zero-copy into
/// `JsValue::from_str` (row text is UTF-8-validated at construction).
pub fn value_to_js(v: crate::value::Value<'_>) -> JsValue {
    use crate::value::Value as V;
    match v {
        V::Absent => JsValue::UNDEFINED,
        V::Null => JsValue::NULL,
        V::Bool(b) => JsValue::from_bool(b),
        V::Int(i) => JsValue::from_f64(i as f64),
        V::Float(f) => JsValue::from_f64(f),
        V::Str(b) | V::Json(b) => JsValue::from_str(&String::from_utf8_lossy(b)),
    }
}

// ---------------------------------------------------------------------------
// Value: JS → Rust
// ---------------------------------------------------------------------------

/// A single JS cell → `OwnedValue`, inferring the variant from the JS **runtime
/// type**: `undefined`→**Absent** (the presence ABI, §5.3), `null`→Null,
/// boolean→Bool, number→**Float** (R10: one number type), string→Str,
/// object/array→Json (stringified).
///
/// A **BigInt** cell is a typed `unsupported_value` error (design 226 Stage A): there
/// is no `int64` column type yet, so accepting one could only round or corrupt.
/// (Before Stage A it fell through to `JSON.stringify` — which throws on BigInt — and
/// the error was swallowed to `NULL`, a silent data loss.) A value `JSON.stringify`
/// cannot serialize for any other reason (symbol, function, cyclic object) is the
/// same typed error rather than a silent `NULL`.
///
/// **Why number→Float, always (not Int).** A [`Schema`] carries no per-column type,
/// so there is nothing to "key by"; and Float-always is safe given the exact
/// comparators (design 226 §5.1): every PK / join-key / sort-key value originates
/// from JS data → it is `Float` on *both* sides of any sort/join comparison, so
/// those axes stay type-homogeneous — and even a mixed Int/Float pair now compares
/// exactly (`compare_int_f64`, no panic and no widening). `Int` arises solely from
/// AST literals, and Int↔Float meet only in *predicate* evaluation. Initial rows and
/// pushed rows both flow through here, so a column's representation is consistent
/// across hydrate and incremental push. (This is a deliberate, documented deviation
/// from WS01.2's "key by ValueType" wording, which assumed a typed schema.)
pub fn js_to_owned_value(js: &JsValue) -> Result<OwnedValue, JsValue> {
    // The absence ABI (`PROJECTION-SUPPORT-DESIGN.md` §5.3): JS `undefined` in a cell
    // slot marshals to `OwnedValue::Absent` (the cell does not exist on this partial /
    // projected / optimistic-partial row), while `null` stays `OwnedValue::Null`
    // (present-and-null). For a `'*'` / full row no cell is ever `undefined`, so this is
    // inert there (§7).
    if js.is_undefined() {
        Ok(OwnedValue::Absent)
    } else if js.is_null() {
        Ok(OwnedValue::Null)
    } else if let Some(b) = js.as_bool() {
        Ok(OwnedValue::Bool(b))
    } else if let Some(n) = js.as_f64() {
        Ok(OwnedValue::Float(n))
    } else if let Some(s) = js.as_string() {
        Ok(OwnedValue::Str(Arc::from(s.as_str())))
    } else if js.is_bigint() {
        Err(make_error(
            "unsupported_value",
            "a BigInt cell cannot cross this boundary: there is no int64 column type yet \
             (design 226) — pass a number within Number.MAX_SAFE_INTEGER, or a string",
        ))
    } else {
        match js_sys::JSON::stringify(js) {
            Ok(s) => Ok(OwnedValue::Json(Arc::from(String::from(s).as_str()))),
            Err(_) => Err(make_error(
                "unsupported_value",
                "cell value cannot be marshalled: JSON.stringify failed \
                 (symbol, function, or cyclic object)",
            )),
        }
    }
}

/// A JS array of cells → an `OwnedRow`, validating the cell count against the
/// schema's column count. A wrong-width row is rejected **here** with a thrown JS
/// error, instead of slipping through to the engine's unchecked column index where
/// it would panic / abort the wasm instance (WS02.4's ingest guard, applied at the
/// boundary).
pub fn js_to_owned_row(js: &JsValue, expected_cols: usize) -> Result<OwnedRow, JsValue> {
    let arr = js
        .dyn_ref::<js_sys::Array>()
        .ok_or_else(|| make_error("invalid_row", "expected a row to be an array of cells"))?;
    let len = arr.length() as usize;
    if len != expected_cols {
        return Err(make_error(
            "invalid_row",
            &format!("row has {len} cells but the schema declares {expected_cols} columns"),
        ));
    }
    let cells: Vec<OwnedValue> = arr
        .iter()
        .map(|c| js_to_owned_value(&c))
        .collect::<Result<_, _>>()?;
    Ok(crate::value::owned_row(cells))
}

/// A JS change object `{ type, row, old? }` → a [`SourceChange`], with each row
/// width-checked against `expected_cols`.
pub fn js_to_source_change(js: &JsValue, expected_cols: usize) -> Result<SourceChange, JsValue> {
    let ty = js_sys::Reflect::get(js, &JsValue::from_str("type"))
        .ok()
        .and_then(|t| t.as_string())
        .ok_or_else(|| make_error("invalid_change", "change is missing a string `type`"))?;
    let row_js = js_sys::Reflect::get(js, &JsValue::from_str("row"))
        .map_err(|_| make_error("invalid_change", "change is missing `row`"))?;
    let row = js_to_owned_row(&row_js, expected_cols)?;
    match ty.as_str() {
        "add" => Ok(SourceChange::Add(row)),
        "remove" => Ok(SourceChange::Remove(row)),
        "edit" => {
            let old_js = js_sys::Reflect::get(js, &JsValue::from_str("old"))
                .map_err(|_| make_error("invalid_change", "an `edit` change is missing `old`"))?;
            let old = js_to_owned_row(&old_js, expected_cols)?;
            Ok(SourceChange::Edit { row, old })
        }
        other => Err(make_error(
            "invalid_change",
            &format!("unknown change type `{other}` (expected add | remove | edit)"),
        )),
    }
}

// ---------------------------------------------------------------------------
// View tree: Rust → JS
// ---------------------------------------------------------------------------

/// `ResultType` → a JS string (`"unknown" | "complete" | "error"`).
pub fn result_type_to_js(rt: ResultType) -> JsValue {
    JsValue::from_str(match rt {
        ResultType::Unknown => "unknown",
        ResultType::Complete => "complete",
        ResultType::Error => "error",
    })
}

/// Materialize the `ViewData` tree into a plain JS value. A `.one()` (singular) level
/// — `schema.singular` — becomes a single object or `null`; otherwise a JS array. Each
/// entry is `{ <col>: val, …, <relName>: child }` with column/relationship names
/// resolved from the hierarchical view [`Schema`], and only **in-view** relationship
/// slots emitted (`schema.rel_child(slot)` is `Some`) — gating EXISTS slots are
/// excluded, matching JS `view.data`.
pub fn view_data_to_js(data: &ViewData, schema: &Schema) -> JsValue {
    if schema.singular {
        return match data.items.first() {
            Some(e) => entry_to_js(e, schema),
            None => JsValue::NULL,
        };
    }
    let arr = js_sys::Array::new();
    for e in &data.items {
        arr.push(&entry_to_js(e, schema));
    }
    arr.into()
}

fn entry_to_js(e: &Entry, schema: &Schema) -> JsValue {
    // Slot alignment is structurally guaranteed: an `Entry` is created with
    // `rels.len() == schema.relationships.len()`, and `builder::view_schema` builds
    // `schema.relationships` by iterating the QUERY-LOCAL slot tree
    // (`builder::query_local_slot_names`) ONCE — the same tree the dataflow joins resolve
    // against — so both index spaces (`e.rels[slot]`, `schema.relationships[slot]`) line
    // up. The `.get(slot)` accessor below is defensive belt-and-suspenders.
    let obj = js_sys::Object::new();
    // Columns by name. A projected level (`schema.projection`, §6) reports only the
    // selected base columns, in select order; a `'*'` level reports every column. Either
    // way the row is full width, so we read it positionally by `ColId` (§2.1).
    match &schema.projection {
        Some(cols) => {
            for &c in cols {
                let cell = e.row.get(c).map(value_to_js).unwrap_or(JsValue::NULL);
                let name = schema.columns.get(c).map(|n| n.as_ref()).unwrap_or("");
                let set = js_sys::Reflect::set(&obj, &JsValue::from_str(name), &cell);
                debug_assert!(set.is_ok(), "Reflect::set on a fresh object cannot fail");
            }
        }
        None => {
            for (i, name) in schema.columns.iter().enumerate() {
                let cell = e.row.get(i).map(value_to_js).unwrap_or(JsValue::NULL);
                let set = js_sys::Reflect::set(&obj, &JsValue::from_str(name), &cell);
                debug_assert!(set.is_ok(), "Reflect::set on a fresh object cannot fail");
            }
        }
    }
    // In-view relationships by name (skip gating / out-of-view slots: a slot is in-view
    // iff its `RelDef` carries a child schema).
    for (slot, rel) in e.rels.iter().enumerate() {
        let rd: &RelDef = match schema.relationships.get(slot) {
            Some(rd) => rd,
            None => continue,
        };
        let child_schema = match rd.child.as_deref() {
            Some(cs) => cs,
            None => continue,
        };
        // A **scalar-projected** relationship aggregate (`REDUCE-DESIGN.md` §9): unwrap
        // the one-row child to a bare scalar (its `project.col` cell), substituting the
        // aggregate identity when the relationship is empty (a childless parent). This
        // takes precedence over the singular/plural object/array shape below.
        let val: JsValue = if let Some(proj) = &rd.project {
            match rel.items.first() {
                Some(c) => c
                    .row
                    .get(proj.col)
                    .map(value_to_js)
                    .unwrap_or(JsValue::NULL),
                None => owned_value_to_js(&proj.identity),
            }
        }
        // A singular (`.one()`) relationship is a single object or `null`; otherwise an
        // array. The engine stores it plural either way — we unwrap here.
        else if child_schema.singular {
            match rel.items.first() {
                Some(c) => entry_to_js(c, child_schema),
                None => JsValue::NULL,
            }
        } else {
            let arr = js_sys::Array::new();
            for c in &rel.items {
                arr.push(&entry_to_js(c, child_schema));
            }
            arr.into()
        };
        let set = js_sys::Reflect::set(&obj, &JsValue::from_str(&rd.name), &val);
        debug_assert!(set.is_ok(), "Reflect::set on a fresh object cannot fail");
    }
    obj.into()
}

/// Marshal a hydrated [`CaughtNode`] tree — a one-shot `tx.query` result
/// (`203-MUTATOR-READS-DESIGN.md` §5.2) — into a plain keyed JS object
/// `{ <col>: val, …, <relName>: child }`, recursing into every **in-view** relationship by
/// name. The `CaughtNode` sibling of [`entry_to_js`]: it presents a mutator's one-shot read
/// **identically to `view.data`** (same projection, `.one()` singular unwrap, and
/// scalar-projected relationship aggregate), the only difference being the children source —
/// a [`CaughtNode`]'s `relationships` map (the hydrate's `expand_node` already eagerly drained
/// the child thunks) rather than a materialized [`Entry`]'s slot vec. A gating EXISTS / join-only
/// slot (no child schema) is skipped, exactly as in the view.
pub fn caught_node_to_js(node: &CaughtNode, schema: &Schema) -> JsValue {
    let obj = js_sys::Object::new();
    // Columns by name (projection-aware; the row is full width, read positionally by ColId).
    match &schema.projection {
        Some(cols) => {
            for &c in cols {
                let cell = node.row.get(c).map(value_to_js).unwrap_or(JsValue::NULL);
                let name = schema.columns.get(c).map(|n| n.as_ref()).unwrap_or("");
                let set = js_sys::Reflect::set(&obj, &JsValue::from_str(name), &cell);
                debug_assert!(set.is_ok(), "Reflect::set on a fresh object cannot fail");
            }
        }
        None => {
            for (i, name) in schema.columns.iter().enumerate() {
                let cell = node.row.get(i).map(value_to_js).unwrap_or(JsValue::NULL);
                let set = js_sys::Reflect::set(&obj, &JsValue::from_str(name), &cell);
                debug_assert!(set.is_ok(), "Reflect::set on a fresh object cannot fail");
            }
        }
    }
    // In-view relationships by name (skip gating EXISTS / join-only slots — no child schema).
    for (slot, rd) in schema.relationships.iter().enumerate() {
        let Some(child_schema) = rd.child.as_deref() else {
            continue;
        };
        let children: &[CaughtNode] = node
            .relationships
            .get(&RelId(slot as u32))
            .map(|v| v.as_slice())
            .unwrap_or(&[]);
        let val: JsValue = if let Some(proj) = &rd.project {
            // Scalar-projected relationship aggregate (`REDUCE-DESIGN.md` §9): unwrap the
            // one-row child to its `project.col` scalar; the identity for a childless parent.
            match children.first() {
                Some(c) => c
                    .row
                    .get(proj.col)
                    .map(value_to_js)
                    .unwrap_or(JsValue::NULL),
                None => owned_value_to_js(&proj.identity),
            }
        } else if child_schema.singular {
            // A `.one()` relationship is a single object or `null`.
            match children.first() {
                Some(c) => caught_node_to_js(c, child_schema),
                None => JsValue::NULL,
            }
        } else {
            let arr = js_sys::Array::new();
            for c in children {
                arr.push(&caught_node_to_js(c, child_schema));
            }
            arr.into()
        };
        let set = js_sys::Reflect::set(&obj, &JsValue::from_str(&rd.name), &val);
        debug_assert!(set.is_ok(), "Reflect::set on a fresh object cannot fail");
    }
    obj.into()
}

// ---------------------------------------------------------------------------
// Schema spec: JS → Schema
// ---------------------------------------------------------------------------

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct SchemaSpec {
    columns: Vec<String>,
    primary_key: Vec<usize>,
    #[serde(default)]
    sort: Vec<(usize, bool)>,
}

/// Deserialize a JS schema spec into a [`SourceSchema`].
///
/// Shape: `{ columns: string[], primaryKey: number[], sort?: [number, boolean][] }`.
/// `sort` defaults to the primary key ascending. Relationships are **not** declared
/// on a source schema — a relationship is a property of a *query* (the AST's
/// `sub`/EXISTS correlation), and the engine derives the per-query relationship
/// slots at build time.
pub fn schema_from_js(spec: JsValue) -> Result<SourceSchema, JsValue> {
    let spec: SchemaSpec = serde_wasm_bindgen::from_value(spec)
        .map_err(|e| make_error("invalid_schema", &e.to_string()))?;
    schema_spec_to_source_schema(spec)
}

fn schema_spec_to_source_schema(spec: SchemaSpec) -> Result<SourceSchema, JsValue> {
    if spec.columns.is_empty() {
        return Err(make_error(
            "invalid_schema",
            "a schema must declare at least one column",
        ));
    }
    // The engine orders and identifies rows by the sort (PK-completed); a table with
    // neither a primary key nor a sort would materialize children in unstable
    // insertion order. Reject it loudly here rather than silently degrade.
    if spec.primary_key.is_empty() && spec.sort.is_empty() {
        return Err(make_error(
            "invalid_schema",
            "a schema must declare a `primaryKey` or a `sort`",
        ));
    }
    let cols: Vec<&str> = spec.columns.iter().map(|c| c.as_str()).collect();
    let sort: Sort = if spec.sort.is_empty() {
        spec.primary_key.iter().map(|&c| (c, true)).collect()
    } else {
        spec.sort.clone()
    };
    Ok(SourceSchema::new(cols, spec.primary_key.clone(), sort))
}

// ---------------------------------------------------------------------------
// Flat changes (BARE cells) → JS  (the wasm backend's wire; WASM-CLIENT-DESIGN.md §7)
// ---------------------------------------------------------------------------

use crate::flat::{FlatChange, FlatOp, WireNode};

/// Set an own property on a fresh JS object — a [`js_sys::Reflect::set`] that cannot fail
/// on a plain object (asserted in debug).
pub(crate) fn js_set(obj: &js_sys::Object, key: &str, val: &JsValue) {
    let r = js_sys::Reflect::set(obj, &JsValue::from_str(key), val);
    debug_assert!(r.is_ok(), "Reflect::set on a fresh object cannot fail");
}

/// A wire row (positional cells) → a JS array of **bare** values ([`owned_value_to_js`]).
fn wire_row_to_js(row: &[OwnedValue]) -> JsValue {
    let arr = js_sys::Array::new();
    for v in row {
        arr.push(&owned_value_to_js(v));
    }
    arr.into()
}

/// A [`WireNode`] → `{ row: Value[], rels: { rel: number, children: WireNode[] }[] }`
/// (children stay in the operator's sort order — never re-sorted by the receiver).
fn wire_node_to_js(node: &WireNode) -> JsValue {
    let obj = js_sys::Object::new();
    js_set(&obj, "row", &wire_row_to_js(&node.row));
    let rels = js_sys::Array::new();
    for (slot, children) in &node.rels {
        let r = js_sys::Object::new();
        js_set(&r, "rel", &JsValue::from_f64(slot.ix() as f64));
        let kids = js_sys::Array::new();
        for c in children {
            kids.push(&wire_node_to_js(c));
        }
        js_set(&r, "children", &kids.into());
        rels.push(&r.into());
    }
    js_set(&obj, "rels", &rels.into());
    obj.into()
}

/// A [`FlatChange`] → `{ path: { rel, parentRow }[], op }` with **bare** cells; `op` is
/// `{ tag:'add', node } | { tag:'remove', row } | { tag:'edit', old, new }`.
fn flat_change_to_js(c: &FlatChange) -> JsValue {
    let obj = js_sys::Object::new();
    let path = js_sys::Array::new();
    for seg in &c.path {
        let s = js_sys::Object::new();
        js_set(&s, "rel", &JsValue::from_f64(seg.rel.ix() as f64));
        js_set(&s, "parentRow", &wire_row_to_js(&seg.parent_row));
        path.push(&s.into());
    }
    js_set(&obj, "path", &path.into());

    let op = js_sys::Object::new();
    match &c.op {
        FlatOp::Add(node) => {
            js_set(&op, "tag", &JsValue::from_str("add"));
            js_set(&op, "node", &wire_node_to_js(node));
        }
        FlatOp::Remove { row } => {
            js_set(&op, "tag", &JsValue::from_str("remove"));
            js_set(&op, "row", &wire_row_to_js(row));
        }
        FlatOp::Edit { old, new } => {
            js_set(&op, "tag", &JsValue::from_str("edit"));
            js_set(&op, "old", &wire_row_to_js(old));
            js_set(&op, "new", &wire_row_to_js(new));
        }
    }
    js_set(&obj, "op", &op.into());
    obj.into()
}

/// A batch of [`FlatChange`]s → a JS array (the hydrate snapshot, or one commit's events).
pub fn flat_changes_to_js(changes: &[FlatChange]) -> JsValue {
    let arr = js_sys::Array::new();
    for c in changes {
        arr.push(&flat_change_to_js(c));
    }
    arr.into()
}

// ---------------------------------------------------------------------------
// Wire schema → JS  (camelCase; the per-query view schema, handed once in `hello`)
// ---------------------------------------------------------------------------

use crate::wire_schema::{WireRel, WireSchema};

/// A [`WireSchema`] → a camelCase JS object `{ columns, primaryKey, sort: [col, asc][],
/// singular, relationships: { name, slot, child }[] }`. Hand-marshalled (not the shared
/// protocol serde) so the JS-facing shape is camelCase and stays decoupled from the wire
/// format `WireSchema` serializes to over a network.
pub fn wire_schema_to_js(ws: &WireSchema) -> JsValue {
    let obj = js_sys::Object::new();

    let columns = js_sys::Array::new();
    for c in &ws.columns {
        columns.push(&JsValue::from_str(c));
    }
    js_set(&obj, "columns", &columns.into());

    let pk = js_sys::Array::new();
    for &c in &ws.primary_key {
        pk.push(&JsValue::from_f64(c as f64));
    }
    js_set(&obj, "primaryKey", &pk.into());

    let sort = js_sys::Array::new();
    for &(c, asc) in &ws.sort {
        let pair = js_sys::Array::new();
        pair.push(&JsValue::from_f64(c as f64));
        pair.push(&JsValue::from_bool(asc));
        sort.push(&pair.into());
    }
    js_set(&obj, "sort", &sort.into());

    js_set(&obj, "singular", &JsValue::from_bool(ws.singular));

    let rels = js_sys::Array::new();
    for r in &ws.relationships {
        rels.push(&wire_rel_to_js(r));
    }
    js_set(&obj, "relationships", &rels.into());

    obj.into()
}

/// A [`WireRel`] → `{ name, slot, child: WireSchema | null, project: { col, identity } |
/// null }` (`child: null` ⇒ an out-of-view / gating slot — the receiver's in-view gate
/// drops `Child`s addressed at it; `project` non-null ⇒ a scalar-projected relationship
/// aggregate the receiver unwraps to a scalar, `REDUCE-DESIGN.md` §9).
fn wire_rel_to_js(r: &WireRel) -> JsValue {
    let obj = js_sys::Object::new();
    js_set(&obj, "name", &JsValue::from_str(&r.name));
    js_set(&obj, "slot", &JsValue::from_f64(r.slot as f64));
    let child = match &r.child {
        Some(cs) => wire_schema_to_js(cs),
        None => JsValue::NULL,
    };
    js_set(&obj, "child", &child);
    let project = match &r.project {
        Some(p) => {
            let po = js_sys::Object::new();
            js_set(&po, "col", &JsValue::from_f64(p.col as f64));
            js_set(&po, "identity", &owned_value_to_js(&p.identity));
            po.into()
        }
        None => JsValue::NULL,
    };
    js_set(&obj, "project", &project);
    obj.into()
}
