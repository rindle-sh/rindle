#![cfg(feature = "testkit")]

//! **Rust↔JS IVM differential corpus** (spec 11 §3.6, recorded-fixture mode).
//!
//! Each `tests/fixtures/diff/*.json` is `{kind, input, expected}` where the JS engine
//! (`mono` `runFetchTest`/`runPushTest`) is the ORACLE: `expected.data` is the
//! `ArrayView` tree and `expected.pushes` the `Catch` change stream. We replay the
//! SAME `input` through the Rust engine (`build_pipeline` → production `ArrayView` via
//! `run_*_test_ast_view`), serialize our result back to the JS-shaped **name-keyed**
//! JSON, and assert equality. Recorded (not a live bridge) → Rust CI stays Node-free.
//!
//! Regenerate the fixtures from the sibling mono checkout:
//!   cd /home/mlaw/workspace/mono
//!   GEN_FIXTURES=1 FIXTURE_OUT=/home/mlaw/workspace/rusty-ivm/tests/fixtures/diff \
//!     pnpm exec vitest run /home/mlaw/workspace/rusty-ivm/tools/gen_diff_fixtures.test.ts
//!
//! serde is `testkit`-gated, so this runs under `cargo test --features "sqlite testkit"`.
use std::collections::BTreeMap;
use std::sync::Arc;

use serde_json::{json, Map, Value};

use rindle::ast::Ast;
use rindle::change::SourceChange;
use rindle::normalize_pipeline_ast;
use rindle::testkit::{
    ast_view_schema, run_fetch_test_ast_view, run_push_test_ast_view, CaughtChange, CaughtNode,
    TableSpec,
};
use rindle::value::{ColId, OwnedValue, RelDef, RelId, Schema, SourceSchema};

// ---------------------------------------------------------------------------
// value <-> JSON boundary (names exist ONLY here; the engine is index-addressed)
// ---------------------------------------------------------------------------

/// Leak a fixture-supplied name to `&'static str` (the engine's `Schema`/`RelDef`
/// take static names). Fine for a short-lived test process.
fn leak(s: &str) -> &'static str {
    Box::leak(s.to_string().into_boxed_str())
}

/// The number-coercion rule of `builder::number_to_owned` (integral, in-range `f64` →
/// `Int`, else `Float`). Applied at load so a row cell and an AST literal (which the
/// builder collapses the same way) compare equal in joins/filters.
fn number_to_owned(n: f64) -> OwnedValue {
    let lo = i64::MIN as f64;
    let hi = -(i64::MIN as f64);
    if n.fract() == 0.0 && n >= lo && n < hi {
        OwnedValue::Int(n as i64)
    } else {
        OwnedValue::Float(n)
    }
}

fn json_to_owned(v: &Value) -> OwnedValue {
    match v {
        Value::Null => OwnedValue::Null,
        Value::Bool(b) => OwnedValue::Bool(*b),
        Value::Number(n) => number_to_owned(n.as_f64().expect("finite JSON number")),
        Value::String(s) => OwnedValue::str(s),
        // A nested array/object is a JSON-typed column; store its canonical text.
        Value::Array(_) | Value::Object(_) => OwnedValue::Json(Arc::from(v.to_string())),
    }
}

fn owned_to_json(v: &OwnedValue) -> Value {
    match v {
        OwnedValue::Absent | OwnedValue::Null => Value::Null, // fixtures use full rows
        OwnedValue::Bool(b) => Value::Bool(*b),
        OwnedValue::Int(i) => json!(*i),
        OwnedValue::Float(f) => json!(*f),
        OwnedValue::Str(s) => Value::String(s.to_string()),
        OwnedValue::Json(s) => serde_json::from_str(s).unwrap_or(Value::String(s.to_string())),
    }
}

/// Normalize numbers so the int/float distinction never causes a spurious mismatch:
/// JS `number` has no int/float split, so an integral value is canonicalized to an
/// integer on BOTH sides (`100.0` → `100`); genuine fractions are left alone. This is
/// the value-parity checkpoint (foundations §2.4) made explicit.
fn canon(v: &mut Value) {
    match v {
        Value::Number(n) => {
            if let Some(f) = n.as_f64() {
                if f.fract() == 0.0 && f.abs() < 9.007_199_254_740_992e15 {
                    *v = json!(f as i64);
                }
            }
        }
        Value::Array(a) => a.iter_mut().for_each(canon),
        Value::Object(o) => o.values_mut().for_each(canon),
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// loading the JS `input` (Sources/SourceContents/ast/pushes) into Rust
// ---------------------------------------------------------------------------

/// Per-table metadata the loader threads through row/push construction: the column
/// names in a fixed (parse) order, so a name-keyed JSON row maps to a positional
/// `OwnedRow` the same way everywhere.
struct TableInfo {
    columns: Vec<String>,
}

struct Loaded {
    specs: Vec<TableSpec>,
    info: BTreeMap<String, TableInfo>,
    ast: Ast,
    /// Per-table ordered-unique relationship aliases, derived from the AST. Sources
    /// (now [`SourceSchema`]) carry no relationships; this drives the **catch** oracle
    /// schema ([`catch_schema_for`]) — the in-view/gating child shape the `Catch` sink
    /// produces — which the builder otherwise derives query-locally.
    rel_aliases: BTreeMap<String, Vec<String>>,
}

/// Walk the AST collecting, per table, the ordered-unique relationship aliases
/// (`related` + `where`-EXISTS). Sources carry no relationships (the builder derives
/// the per-query slot tree from the AST's correlations); this list is what the
/// [`catch_schema_for`] oracle iterates to reconstruct the expected `Catch` shape.
fn collect_rel_aliases(ast: &Ast, out: &mut BTreeMap<String, Vec<String>>) {
    let ast = normalize_pipeline_ast(ast);
    let table = ast.table.to_string();
    let entry = out.entry(table.clone()).or_default();
    for csq in &ast.related {
        if let Some(a) = csq.subquery.alias.as_deref() {
            if !entry.iter().any(|x| x == a) {
                entry.push(a.to_string());
            }
        }
    }
    // EXISTS aliases live in `where`; gather them for this frame's table.
    let mut exists = Vec::new();
    gather_exists(ast.r#where.as_ref(), &mut exists);
    for csq in &exists {
        if let Some(a) = csq.related.subquery.alias.as_deref() {
            let e = out.entry(table.clone()).or_default();
            if !e.iter().any(|x| x == a) {
                e.push(a.to_string());
            }
        }
    }
    // Recurse into child subqueries (collect their tables' relationships too).
    for csq in &ast.related {
        collect_rel_aliases(&csq.subquery, out);
    }
    for csq in &exists {
        collect_rel_aliases(&csq.related.subquery, out);
    }
}

fn gather_exists<'a>(
    cond: Option<&'a rindle::ast::Condition>,
    out: &mut Vec<&'a rindle::ast::CorrelatedSubqueryCondition>,
) {
    use rindle::ast::Condition::*;
    match cond {
        Some(CorrelatedSubquery(c)) => out.push(c),
        Some(And { conditions }) | Some(Or { conditions }) => {
            for c in conditions {
                gather_exists(Some(c), out);
            }
        }
        _ => {}
    }
}

fn load(input: &Value) -> Loaded {
    let ast: Ast = serde_json::from_value(input["ast"].clone()).expect("input.ast parses as Ast");

    let mut rel_aliases: BTreeMap<String, Vec<String>> = BTreeMap::new();
    collect_rel_aliases(&ast, &mut rel_aliases);

    let sources = input["sources"].as_object().expect("input.sources object");
    let contents = input.get("sourceContents").and_then(|v| v.as_object());

    let mut specs = Vec::new();
    let mut info = BTreeMap::new();
    for (name, src) in sources {
        let columns: Vec<String> = src["columns"]
            .as_object()
            .expect("source.columns object")
            .keys()
            .cloned()
            .collect();
        let col_id = |c: &str| {
            columns
                .iter()
                .position(|x| x == c)
                .expect("pk column exists")
        };
        let pk: Vec<ColId> = src["primaryKeys"]
            .as_array()
            .expect("source.primaryKeys array")
            .iter()
            .map(|v| col_id(v.as_str().expect("pk name string")))
            .collect();

        let sort = pk.iter().map(|&c| (c, true)).collect::<Vec<_>>();
        let schema = SourceSchema::new(columns.iter().map(|c| leak(c)).collect(), pk.clone(), sort);

        let rows: Vec<Vec<OwnedValue>> = contents
            .and_then(|c| c.get(name))
            .and_then(|v| v.as_array())
            .map(|rows| rows.iter().map(|r| row_from_obj(r, &columns)).collect())
            .unwrap_or_default();

        specs.push(TableSpec::new(name.clone(), schema, rows));
        info.insert(name.clone(), TableInfo { columns });
    }

    Loaded {
        specs,
        info,
        ast,
        rel_aliases,
    }
}

fn row_from_obj(obj: &Value, columns: &[String]) -> Vec<OwnedValue> {
    let o = obj.as_object().expect("row object");
    columns
        .iter()
        .map(|c| o.get(c).map(json_to_owned).unwrap_or(OwnedValue::Null))
        .collect()
}

fn load_pushes(input: &Value, info: &BTreeMap<String, TableInfo>) -> Vec<(String, SourceChange)> {
    input
        .get("pushes")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .map(|p| {
                    let name = p[0].as_str().expect("push source name").to_string();
                    let cols = &info[&name].columns;
                    let ch = &p[1];
                    let row = |key| rindle::value::owned_row(row_from_obj(&ch[key], cols));
                    let change = match ch["type"].as_str().expect("push type") {
                        "add" => SourceChange::Add(row("row")),
                        "remove" => SourceChange::Remove(row("row")),
                        "edit" => SourceChange::Edit {
                            row: row("row"),
                            old: row("oldRow"),
                        },
                        other => panic!("unknown push type {other:?}"),
                    };
                    (name, change)
                })
                .collect()
        })
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// serializing the Rust result back to the JS-shaped name-keyed JSON
// ---------------------------------------------------------------------------

/// `ArrayView.data` shape: a flat object per node — columns by name, **plus** each
/// in-view relationship inlined under its alias. A relationship is in-view iff its
/// `RelDef` carries a child schema; a singular (`.one()`) child schema is inlined as a
/// single object / `null`, otherwise a `[children]` array. Mirrors the JS `data` getter.
fn data_node_to_json(node: &CaughtNode, schema: &Schema) -> Value {
    let mut obj = Map::new();
    for (i, col) in schema.columns.iter().enumerate() {
        obj.insert(
            (*col).to_string(),
            owned_to_json(&node.row.col(i).to_owned()),
        );
    }
    for (slot, rd) in schema.relationships.iter().enumerate() {
        let Some(child_schema) = rd.child.as_deref() else {
            continue; // out-of-view (gating) relationship — not in `data`
        };
        let children = node
            .relationships
            .get(&RelId(slot as u32))
            .map(|v| v.as_slice())
            .unwrap_or(&[]);
        let v = if child_schema.singular {
            children
                .first()
                .map(|c| data_node_to_json(c, child_schema))
                .unwrap_or(Value::Null)
        } else {
            Value::Array(
                children
                    .iter()
                    .map(|c| data_node_to_json(c, child_schema))
                    .collect(),
            )
        };
        obj.insert(rd.name.to_string(), v);
    }
    Value::Object(obj)
}

fn data_to_json(nodes: &[CaughtNode], schema: &Schema) -> Value {
    if schema.singular {
        return nodes
            .first()
            .map(|n| data_node_to_json(n, schema))
            .unwrap_or(Value::Null);
    }
    Value::Array(nodes.iter().map(|n| data_node_to_json(n, schema)).collect())
}

/// `Catch.pushes` shape: `{type,node}` / `{type:'child', row, child:{...}}`.
/// Unlike `ArrayView.data`, this includes every relationship thunk `Catch.expandNode`
/// drains, including out-of-view EXISTS-gating slots.
fn catch_schema(
    ast: &Ast,
    specs: &[TableSpec],
    rel_aliases: &BTreeMap<String, Vec<String>>,
) -> Schema {
    let by_table: BTreeMap<String, SourceSchema> = specs
        .iter()
        .map(|s| (s.name.clone(), s.schema.clone()))
        .collect();
    catch_schema_for(ast, &by_table, rel_aliases)
}

fn catch_schema_for(
    ast: &Ast,
    by_table: &BTreeMap<String, SourceSchema>,
    rel_aliases: &BTreeMap<String, Vec<String>>,
) -> Schema {
    let ast = normalize_pipeline_ast(ast);
    let base = by_table
        .get(ast.table.as_ref())
        .unwrap_or_else(|| panic!("unknown table in fixture AST: {:?}", ast.table));
    // The relationship NAMES come from the AST-derived alias list (sources declare
    // none); each is in-view (`RelDef::related` + child schema) iff this frame's AST
    // materializes it, else a gating `RelDef::new`.
    let empty = Vec::new();
    let names = rel_aliases.get(ast.table.as_ref()).unwrap_or(&empty);
    let relationships = names
        .iter()
        .map(|name| {
            catch_child_ast_for_alias(&ast, name)
                .map(|child| RelDef::related(name, catch_schema_for(child, by_table, rel_aliases)))
                .unwrap_or_else(|| RelDef::new(name))
        })
        .collect();
    Schema {
        columns: base.columns.clone(),
        column_types: base.column_types.clone(),
        primary_key: base.primary_key.clone(),
        sort: base.sort.clone(),
        relationships,
        singular: false,
        // The catch oracle materializes full rows; projection is a view-boundary concern.
        projection: None,
    }
}

fn catch_child_ast_for_alias<'a>(ast: &'a Ast, alias: &str) -> Option<&'a Ast> {
    // `related` aliases are deduped last-writer-wins by the builder.
    if let Some(csq) = ast
        .related
        .iter()
        .rev()
        .find(|c| c.subquery.alias.as_deref() == Some(alias))
    {
        return Some(&csq.subquery);
    }

    let mut exists = Vec::new();
    gather_exists(ast.r#where.as_ref(), &mut exists);
    exists
        .into_iter()
        .rev()
        .find(|c| c.related.subquery.alias.as_deref() == Some(alias))
        .map(|c| c.related.subquery.as_ref())
}

fn row_to_json(row: &rindle::OwnedRow, schema: &Schema) -> Value {
    let mut obj = Map::new();
    for (i, col) in schema.columns.iter().enumerate() {
        obj.insert((*col).to_string(), owned_to_json(&row.col(i).to_owned()));
    }
    Value::Object(obj)
}

fn catch_node_to_json(node: &CaughtNode, schema: &Schema) -> Value {
    let mut rels = Map::new();
    for (slot, children) in &node.relationships {
        let rd = schema
            .relationships
            .get(slot.ix())
            .unwrap_or_else(|| panic!("caught relationship slot {:?} is undeclared", slot));
        let child_schema = rd
            .child
            .as_deref()
            .unwrap_or_else(|| panic!("caught relationship {:?} has no child schema", rd.name));
        rels.insert(
            rd.name.to_string(),
            Value::Array(
                children
                    .iter()
                    .map(|c| catch_node_to_json(c, child_schema))
                    .collect(),
            ),
        );
    }
    json!({
        "row": row_to_json(&node.row, schema),
        "relationships": Value::Object(rels),
    })
}

fn catch_change_to_json(change: &CaughtChange, schema: &Schema) -> Value {
    match change {
        CaughtChange::Add(node) => json!({
            "type": "add",
            "node": catch_node_to_json(node, schema),
        }),
        CaughtChange::Remove(node) => json!({
            "type": "remove",
            "node": catch_node_to_json(node, schema),
        }),
        CaughtChange::Edit { old, row } => json!({
            "type": "edit",
            "oldRow": row_to_json(old, schema),
            "row": row_to_json(row, schema),
        }),
        CaughtChange::Child { row, rel, change } => {
            let rd = schema.relationships.get(rel.ix()).unwrap_or_else(|| {
                panic!("caught child relationship slot {:?} is undeclared", rel)
            });
            let child_schema = rd
                .child
                .as_deref()
                .unwrap_or_else(|| panic!("caught child relationship {:?} has no schema", rd.name));
            json!({
                "type": "child",
                "row": row_to_json(row, schema),
                "child": {
                    "relationshipName": rd.name,
                    "change": catch_change_to_json(change, child_schema),
                },
            })
        }
    }
}

fn catch_pushes_to_json(changes: &[CaughtChange], schema: &Schema) -> Value {
    Value::Array(
        changes
            .iter()
            .map(|c| catch_change_to_json(c, schema))
            .collect(),
    )
}

// ---------------------------------------------------------------------------
// the differential
// ---------------------------------------------------------------------------

fn fixtures_dir() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/diff")
}

fn assert_json_eq(name: &str, what: &str, mut actual: Value, mut expected: Value) {
    canon(&mut actual);
    canon(&mut expected);
    if actual != expected {
        panic!(
            "[{name}] {what} mismatch (Rust vs JS oracle)\n  rust:     {}\n  expected: {}",
            serde_json::to_string_pretty(&actual).unwrap(),
            serde_json::to_string_pretty(&expected).unwrap(),
        );
    }
}

fn run_one(name: &str, fixture: &Value) {
    let kind = fixture["kind"].as_str().expect("fixture.kind");
    let input = &fixture["input"];
    let expected = &fixture["expected"];
    let loaded = load(input);
    let vschema = ast_view_schema(&loaded.specs, &loaded.ast).expect("view schema derives");

    match kind {
        "fetch" => {
            let data =
                run_fetch_test_ast_view(loaded.specs, &loaded.ast).expect("fetch pipeline builds");
            assert_json_eq(
                name,
                "data",
                data_to_json(&data, &vschema),
                expected["data"].clone(),
            );
        }
        "push" => {
            let cschema = catch_schema(&loaded.ast, &loaded.specs, &loaded.rel_aliases);
            let pushes = load_pushes(input, &loaded.info);
            let names: Vec<&str> = pushes.iter().map(|(n, _)| n.as_str()).collect();
            let pushes_arg: Vec<(&str, SourceChange)> = names
                .iter()
                .zip(pushes.iter())
                .map(|(n, (_, c))| (*n, c.clone()))
                .collect();
            let res = run_push_test_ast_view(loaded.specs, &loaded.ast, pushes_arg, false)
                .expect("push pipeline builds");
            assert_json_eq(
                name,
                "data (post-push)",
                data_to_json(&res.data, &vschema),
                expected["data"].clone(),
            );
            assert_json_eq(
                name,
                "pushes",
                catch_pushes_to_json(&res.pushes, &cschema),
                expected["pushes"].clone(),
            );
        }
        other => panic!("[{name}] unknown fixture kind {other:?}"),
    }
}

#[test]
fn diff_corpus_matches_js_oracle() {
    let dir = fixtures_dir();
    let mut found = 0;
    let mut entries: Vec<_> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("read fixtures dir {dir:?}: {e}"))
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "json"))
        .collect();
    entries.sort();
    for path in entries {
        let name = path.file_stem().unwrap().to_string_lossy().to_string();
        let text = std::fs::read_to_string(&path).expect("read fixture");
        let fixture: Value = serde_json::from_str(&text).expect("fixture parses");
        run_one(&name, &fixture);
        found += 1;
    }
    assert!(found > 0, "no diff fixtures found in {dir:?}");
    eprintln!("diff corpus: {found} fixtures matched the JS oracle");
}

// ---------------------------------------------------------------------------
// `.one()` (singular) presentation — Stage 2. The view tree is plural; `.one()`
// is unwrapped to a single object / `null` at the serialization boundary, driven
// by `Schema::singular` (lowered from `Ast::one` by `view_schema`). Root-level
// only here; relationship-level `.one()` (per-parent `limit 1`) is a follow-up.
// ---------------------------------------------------------------------------

fn issue_specs(rows: Vec<Vec<OwnedValue>>) -> Vec<TableSpec> {
    vec![TableSpec::new(
        "issue",
        SourceSchema::new(vec!["id", "title"], vec![0], vec![(0, true)]),
        rows,
    )]
}

#[test]
fn one_root_serializes_as_object_not_array() {
    let specs = issue_specs(vec![
        vec![OwnedValue::Int(1), OwnedValue::str("a")],
        vec![OwnedValue::Int(2), OwnedValue::str("b")],
    ]);
    let ast = rindle::table("issue").one().build();
    let schema = ast_view_schema(&specs, &ast).expect("view schema derives");
    assert!(schema.singular, "`.one()` lowers to a singular view schema");
    let nodes = run_fetch_test_ast_view(specs, &ast).expect("fetch pipeline builds");
    let json = data_to_json(&nodes, &schema);
    assert!(
        json.is_object(),
        "`.one()` root serializes as a single object, got {json}"
    );
    assert_eq!(
        json.get("id").and_then(|v| v.as_i64()),
        Some(1),
        "the first row by sort"
    );
}

#[test]
fn one_root_over_empty_source_serializes_as_null() {
    let specs = issue_specs(vec![]);
    let ast = rindle::table("issue").one().build();
    let schema = ast_view_schema(&specs, &ast).expect("view schema derives");
    let nodes = run_fetch_test_ast_view(specs, &ast).expect("fetch pipeline builds");
    let json = data_to_json(&nodes, &schema);
    assert!(
        json.is_null(),
        "`.one()` over an empty source is null, got {json}"
    );
}
