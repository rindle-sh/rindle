//! The wasm/JS client boundary (productionization WS01.4 + 01.5).
//!
//! Exposes [`RindleView`] — a built, hydrated query view callable from JavaScript that
//! drives the engine's `build → hydrate → push → flush → subscribe → read` loop (the
//! exact sequence the `testkit` AST runners use). One `RindleView` owns one
//! single-threaded [`Graph`] (the `!Send` model, D3); concurrency is N independent
//! views, never a shared graph.
//!
//! **Reentrancy (R7).** Every method takes `&self`, so a JS listener callback firing
//! inside `flush` may re-enter `push`/`data` (two shared borrows of the JS-held
//! object) without a `BorrowMutError`. The engine's interior mutability does the
//! actual mutation under `&self`, and `View::flush` takes its listener vec out before
//! firing, so no engine borrow is held across the callback.

pub mod db;
pub mod marshal;

use std::collections::BTreeMap;
use std::rc::Rc;

use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;

use crate::ast::Ast;
use crate::builder::build_pipeline;
use crate::graph::{Graph, NodeId};
use crate::value::{OwnedRow, Schema, SourceSchema};
use crate::view::{Listener, ResultType, ViewData};

/// Install the panic hook so a residual wasm panic surfaces a legible JS `Error`
/// (with a Rust message) instead of an opaque `RuntimeError: unreachable` (WS09.6).
/// Runs once on module load.
#[wasm_bindgen(start)]
pub fn __rindle_wasm_start() {
    console_error_panic_hook::set_once();
}

/// A built, hydrated query view, callable from JavaScript.
#[wasm_bindgen]
pub struct RindleView {
    graph: Graph,
    view_id: NodeId,
    /// The hierarchical view schema (column + relationship names per level; a
    /// relationship is in-view iff its `RelDef` has a child schema) used to marshal the
    /// result tree. `Rc` so the listener closure shares it cheaply.
    view_schema: Rc<Schema>,
    /// Table name → (source `NodeId`, column count), for [`RindleView::push`]. The
    /// column count width-checks incoming change rows at the boundary.
    sources: BTreeMap<String, (NodeId, usize)>,
}

#[wasm_bindgen]
impl RindleView {
    /// Build + hydrate a view from an AST (JS JSON) and a table→schema map.
    ///
    /// - `ast_json`: the query AST — Zero's wire shape (see `src/ast.rs`).
    /// - `schemas`: `{ tableName: <SchemaSpec> }` (see [`marshal::schema_from_js`]).
    /// - `data`: optional `{ tableName: row[][] }` initial rows (each row a cell
    ///   array, positional with the schema's `columns`); a table absent here seeds
    ///   empty.
    ///
    /// A `BuildError`/`RindleError` is thrown as a JS `Error` with a `.kind` tag.
    pub fn build(
        ast_json: JsValue,
        schemas: JsValue,
        data: JsValue,
    ) -> Result<RindleView, JsValue> {
        let mut ast: Ast = serde_wasm_bindgen::from_value(ast_json)
            .map_err(|e| marshal::make_error("invalid_ast", &e.to_string()))?;
        // One literal identity per query text on every home (design 226 §6):
        // serde-wasm-bindgen delivers >2^53 integral JS numbers as their BINARY f64;
        // re-key them onto the wire-token rule every JSON home parses.
        crate::ast::canonicalize_wire_number_lits(&mut ast);
        validate_correlations(&ast)?;

        // 1. Seed one source per declared table.
        let mut graph = Graph::new();
        let mut by_name: BTreeMap<String, (NodeId, SourceSchema)> = BTreeMap::new();
        for (table, spec) in js_object_entries(&schemas)? {
            let schema = marshal::schema_from_js(spec)?;
            let rows = initial_rows(&data, &table, schema.columns.len())?;
            let sid = graph
                .try_add_source(schema.clone(), rows)
                .map_err(|e| marshal::rindle_error_to_js(&e))?;
            by_name.insert(table, (sid, schema));
        }

        // 2. Lower the AST and derive the view shape (the `resolve` borrow of
        //    `by_name` is scoped to this block so `by_name` is readable after).
        let (top, view_schema) = {
            let resolve = |t: &str| by_name.get(t).cloned();
            let top = build_pipeline(&mut graph, &ast, &resolve)
                .map_err(|e| marshal::build_error_to_js(&e))?;
            let vs = crate::builder::view_schema(&ast, &resolve)
                .map_err(|e| marshal::build_error_to_js(&e))?;
            (top, vs)
        };

        // 3. Sink into an `ArrayView` (with_ids = false) and hydrate.
        let view_id = graph.add_view_with(top, view_schema.clone(), false, ResultType::Unknown);
        graph.set_sink_edge(top, view_id);
        graph
            .try_hydrate(view_id)
            .map_err(|e| marshal::rindle_error_to_js(&e))?;

        let sources = by_name
            .into_iter()
            .map(|(k, (id, s))| (k, (id, s.columns.len())))
            .collect();
        Ok(RindleView {
            graph,
            view_id,
            view_schema: Rc::new(view_schema),
            sources,
        })
    }

    /// The current materialized view tree as a JS value (array / object / `null`).
    /// Throws a typed `unsupported_value` error when the tree holds an `Int` outside
    /// `Number.MAX_SAFE_INTEGER` instead of rounding it — the 09.8 `strict_i64`
    /// check, **mandatory** since design 226 Stage C (§8; Stage E adds the bigint
    /// lane and makes it column-aware). The `subscribe` listener path cannot throw —
    /// a violating tree is WITHHELD there (the subscriber keeps its last delivered
    /// snapshot; rounded cells never reach JS) and surfaces typed on the next
    /// `data()` read.
    pub fn data(&self) -> Result<JsValue, JsValue> {
        let data = self.graph.view_data(self.view_id);
        if let Some(i) = crate::js_safe::unsafe_int_in_view_data(&data) {
            return Err(marshal::make_error(
                "unsupported_value",
                &format!("i64 {i} exceeds the JS safe-integer range (strict_i64)"),
            ));
        }
        Ok(marshal::view_data_to_js(&data, &self.view_schema))
    }

    /// Push one source change `{ type: 'add'|'remove'|'edit', row, old? }` to `table`.
    /// Does **not** flush — call [`RindleView::flush`] to close the transaction (so a JS
    /// client can batch several pushes into one notification).
    pub fn push(&self, table: &str, change: JsValue) -> Result<(), JsValue> {
        let (src, cols) = *self
            .sources
            .get(table)
            .ok_or_else(|| marshal::make_error("unknown_table", &format!("no source `{table}`")))?;
        let change = marshal::js_to_source_change(&change, cols)?;
        self.graph
            .try_source_push(src, change)
            .map_err(|e| marshal::rindle_error_to_js(&e))
    }

    /// Close the current transaction: fire listeners with the new snapshot.
    pub fn flush(&self) {
        self.graph.flush_view(self.view_id);
    }

    /// Subscribe a JS callback `(data, resultType) => void`, fired **once
    /// immediately** and on every subsequent [`RindleView::flush`]. Returns the
    /// listener index.
    pub fn subscribe(&self, cb: js_sys::Function) -> usize {
        let schema = self.view_schema.clone();
        let listener: Listener = Box::new(move |data: &ViewData, rt: ResultType| {
            // strict_i64 (§8): this path cannot throw, so a violating tree is
            // WITHHELD — a rounded `Int` must never reach JS. The subscriber keeps
            // its last delivered snapshot and the violation surfaces as the typed
            // error on the next `data()` read.
            if crate::js_safe::unsafe_int_in_view_data(data).is_some() {
                return;
            }
            let data_js = marshal::view_data_to_js(data, &schema);
            let rt_js = marshal::result_type_to_js(rt);
            // Swallow a throwing JS callback — never let it unwind into `panic =
            // "abort"`. (A diagnostic could be logged here in a future revision.)
            let _ = cb.call2(&JsValue::NULL, &data_js, &rt_js);
        });
        self.graph.view_add_listener(self.view_id, listener)
    }

    /// The view's current result type (`"unknown" | "complete" | "error"`).
    #[wasm_bindgen(js_name = resultType)]
    pub fn result_type(&self) -> JsValue {
        marshal::result_type_to_js(self.graph.view_result_type(self.view_id))
    }

    /// Resolve the async result type (the `queryComplete` transition); fires
    /// listeners out of band. Accepts `"complete" | "error" | "unknown"`.
    #[wasm_bindgen(js_name = setResultType)]
    pub fn set_result_type(&self, rt: &str) {
        let rt = match rt {
            "complete" => ResultType::Complete,
            "error" => ResultType::Error,
            _ => ResultType::Unknown,
        };
        self.graph.set_view_result_type(self.view_id, rt);
    }
}

/// Read a JS object's own enumerable entries as `(key, value)` pairs (the schemas
/// map). An absent/`null` object yields no entries.
fn js_object_entries(obj: &JsValue) -> Result<Vec<(String, JsValue)>, JsValue> {
    if obj.is_undefined() || obj.is_null() {
        return Ok(Vec::new());
    }
    let obj: &js_sys::Object = obj
        .dyn_ref::<js_sys::Object>()
        .ok_or_else(|| marshal::make_error("invalid_schemas", "`schemas` must be an object"))?;
    let mut out = Vec::new();
    for entry in js_sys::Object::entries(obj).iter() {
        let pair = js_sys::Array::from(&entry);
        let key = pair
            .get(0)
            .as_string()
            .ok_or_else(|| marshal::make_error("invalid_schemas", "schema key must be a string"))?;
        out.push((key, pair.get(1)));
    }
    Ok(out)
}

/// Initial rows for `table` from the optional `{ tableName: row[][] }` data map,
/// each row width-checked against `expected_cols`.
fn initial_rows(
    data: &JsValue,
    table: &str,
    expected_cols: usize,
) -> Result<Vec<OwnedRow>, JsValue> {
    if data.is_undefined() || data.is_null() {
        return Ok(Vec::new());
    }
    let rows_js = js_sys::Reflect::get(data, &JsValue::from_str(table))
        .map_err(|_| marshal::make_error("invalid_data", "`data` must be an object"))?;
    if rows_js.is_undefined() || rows_js.is_null() {
        return Ok(Vec::new());
    }
    let arr = rows_js.dyn_ref::<js_sys::Array>().ok_or_else(|| {
        marshal::make_error("invalid_data", "table data must be an array of rows")
    })?;
    let mut rows = Vec::with_capacity(arr.length() as usize);
    for r in arr.iter() {
        rows.push(marshal::js_to_owned_row(&r, expected_cols)?);
    }
    Ok(rows)
}

/// Reject a malformed AST at the boundary: every correlated subquery's join keys
/// must be non-empty and equal length on both sides. Without this a bad AST defers
/// to a deep internal failure; here it throws a clear `invalid_ast` error. Recurses
/// through nested `related` subqueries.
pub(crate) fn validate_correlations(ast: &Ast) -> Result<(), JsValue> {
    for csq in &ast.related {
        let c = &csq.correlation;
        if c.parent_field.is_empty() || c.parent_field.len() != c.child_field.len() {
            return Err(marshal::make_error(
                "invalid_ast",
                &format!(
                    "relationship `{}` has an invalid correlation: parentField ({}) and \
                     childField ({}) must be non-empty and the same length",
                    csq.subquery.table,
                    c.parent_field.len(),
                    c.child_field.len()
                ),
            ));
        }
        validate_correlations(&csq.subquery)?;
    }
    Ok(())
}
