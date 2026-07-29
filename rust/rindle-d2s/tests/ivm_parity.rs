//! IVM-parity smoke tests — the check this crate exists to serve.
//!
//! Each test drives the **same `Ast`** through two independent implementations — the
//! real IVM `View` (memory backend, via the `testkit` runners) and `rindle-d2s` (one
//! SQLite `SELECT` run against the same data) — then asserts the two materialized
//! JSON trees are equal. The IVM decomposes the query into a dataflow graph; SQLite
//! evaluates it declaratively, so agreement is strong evidence the compiler models
//! the engine's join/EXISTS/filter/order logic.
//!
//! The rusty-ivm `View` materializes **every** relationship as a plural array
//! (`view_schema` is plural at every level), so the `Catalog` here declares
//! relationships `Many` to match.

use rindle::testkit::{ast_view_schema, run_fetch_test_ast_view, TableSpec};
use rindle::{
    table, Ast, CaughtNode, Condition, Lit, Op, OwnedValue as V, RelId, Schema, SimpleCondition,
    SourceSchema, ValuePosition,
};
use rindle_d2s::{compile, Cardinality, Catalog};
use rusqlite::Connection;
use serde_json::{Map, Value};

// ── fixture: issue ─many(comments)→ comment ─────────────────────────────────────

fn issue_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "title"], vec![0], vec![(0, true)])
}
fn comment_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "issueId", "body"], vec![0], vec![(0, true)])
}

fn sources() -> Vec<TableSpec> {
    vec![
        TableSpec::new(
            "issue",
            issue_schema(),
            vec![
                vec![V::Int(1), V::str("a")],
                vec![V::Int(2), V::str("b")],
                vec![V::Int(3), V::str("c")],
            ],
        ),
        TableSpec::new(
            "comment",
            comment_schema(),
            vec![
                vec![V::Int(10), V::Int(1), V::str("x")],
                vec![V::Int(11), V::Int(1), V::str("y")],
                vec![V::Int(12), V::Int(2), V::str("z")],
            ],
        ),
    ]
}

fn catalog() -> Catalog {
    let mut c = Catalog::new();
    c.table("issue", &["id", "title"], &["id"]);
    c.table("comment", &["id", "issueId", "body"], &["id"]);
    c.relationship("issue", "comments", Cardinality::Many);
    c
}

// ── the parity assertion ────────────────────────────────────────────────────────

/// Run `ast` through the IVM `View` and through `rindle-d2s`+SQLite over the same data,
/// and assert the two JSON result trees are equal.
fn assert_parity(sources: Vec<TableSpec>, catalog: &Catalog, ast: &Ast) {
    // IVM side: materialize the View, serialize its tree to name-keyed JSON.
    let schema = ast_view_schema(&sources, ast).unwrap();
    let setup = seed_sql(&sources);
    let nodes = run_fetch_test_ast_view(sources, ast).unwrap();
    let ivm = Value::Array(nodes.iter().map(|n| node_to_json(n, &schema)).collect());

    // Oracle side: compile to SQLite, run over the identical data, parse the JSON.
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch("PRAGMA case_sensitive_like = ON;")
        .unwrap();
    conn.execute_batch(&setup).unwrap();
    let sql = compile(catalog, ast).unwrap_or_else(|e| panic!("compile: {e}"));
    let text: String = conn
        .query_row(&sql, [], |r| r.get(0))
        .unwrap_or_else(|e| panic!("run: {e}\nSQL:\n{sql}"));
    let oracle: Value = serde_json::from_str(&text).unwrap();

    assert_eq!(
        canon(&ivm),
        canon(&oracle),
        "IVM != oracle\n--- IVM ---\n{ivm:#}\n--- ORACLE ---\n{oracle:#}\n--- SQL ---\n{sql}"
    );
}

// ── tests ───────────────────────────────────────────────────────────────────────

#[test]
fn parity_projection_root() {
    // A projected root: the View reports only the selected columns; the oracle projects
    // identically (PROJECTION-SUPPORT-DESIGN.md §6).
    let ast = table("issue").select("title").order_by("id", "asc").build();
    assert_parity(sources(), &catalog(), &ast);
}

#[test]
fn parity_projection_with_filter_and_order() {
    // The output projects to `select` even when `where`/`order_by` reference other columns
    // (those drive presence/order but are not in the output).
    let ast = table("issue")
        .select("title")
        .where_op("id", ">", 1)
        .order_by("id", "asc")
        .build();
    assert_parity(sources(), &catalog(), &ast);
}

#[test]
fn parity_projection_keeps_relationships() {
    // A projected parent still emits its in-view relationship key; select restricts only the
    // parent's own columns.
    let ast = table("issue")
        .select("id")
        .sub_as("comments", |row| {
            table("comment")
                .r#where("issueId", row.col("id"))
                .order_by("id", "asc")
        })
        .order_by("id", "asc")
        .build();
    assert_parity(sources(), &catalog(), &ast);
}

#[test]
fn parity_sub_tree() {
    // `.sub` — the nested-children tree must match between the View and the oracle.
    let ast = table("issue")
        .sub_as("comments", |row| {
            table("comment")
                .r#where("issueId", row.col("id"))
                .order_by("id", "asc")
        })
        .order_by("id", "asc")
        .build();
    assert_parity(sources(), &catalog(), &ast);
}

#[test]
fn parity_sub_tree_with_child_filter() {
    // Tree + a filter on the child rows.
    let ast = table("issue")
        .sub_as("comments", |row| {
            table("comment")
                .r#where("issueId", row.col("id"))
                .where_op("body", "!=", "x")
                .order_by("id", "asc")
        })
        .order_by("id", "asc")
        .build();
    assert_parity(sources(), &catalog(), &ast);
}

#[test]
fn parity_exists() {
    // EXISTS filters the parent and is excluded from the output tree (gating slot).
    // The IVM builder needs the gating subquery aliased; the oracle ignores the alias.
    let ast = table("issue")
        .where_exists(|row| {
            table("comment")
                .alias("hasComment")
                .r#where("issueId", row.col("id"))
        })
        .order_by("id", "asc")
        .build();
    assert_parity(sources(), &catalog(), &ast);
}

#[test]
fn parity_not_exists() {
    let ast = table("issue")
        .where_not_exists(|row| {
            table("comment")
                .alias("hasComment")
                .r#where("issueId", row.col("id"))
        })
        .order_by("id", "asc")
        .build();
    assert_parity(sources(), &catalog(), &ast);
}

#[test]
fn parity_nested_and_or() {
    // (title = 'a' OR title = 'c') AND id > 1  ⇒  only issue 3.
    let cond = Condition::And {
        conditions: vec![
            Condition::Or {
                conditions: vec![
                    simple("title", Op::Eq, Lit::Str("a".into())),
                    simple("title", Op::Eq, Lit::Str("c".into())),
                ],
            },
            simple("id", Op::Gt, Lit::Number(1.0)),
        ],
    };
    let ast = Ast {
        table: "issue".into(),
        r#where: Some(cond),
        order_by: vec![rindle::OrderPart("id".into(), rindle::Dir::Asc)],
        ..Default::default()
    };
    assert_parity(sources(), &catalog(), &ast);
}

#[test]
fn parity_tree_plus_nested_and_or() {
    // Combine the tree (`.sub`) with a nested AND/OR filter on the parent.
    let mut ast = table("issue")
        .sub_as("comments", |row| {
            table("comment")
                .r#where("issueId", row.col("id"))
                .order_by("id", "desc")
        })
        .order_by("id", "asc")
        .build();
    ast.r#where = Some(Condition::Or {
        conditions: vec![
            simple("id", Op::Eq, Lit::Number(1.0)),
            simple("id", Op::Eq, Lit::Number(2.0)),
        ],
    });
    assert_parity(sources(), &catalog(), &ast);
}

// ── relationship aggregates (count(child), REDUCE-DESIGN.md §9) ──────────────────

#[test]
fn parity_count_aggregate() {
    // `issue { commentCount: count(comments) }`. The oracle emits a scalar `count(*)`
    // column (0 for the childless issue 3); the IVM unwraps its one-row aggregate
    // relationship to the same scalar.
    let ast = table("issue")
        .count_as("commentCount", |row| {
            table("comment").r#where("issueId", row.col("id"))
        })
        .order_by("id", "asc")
        .build();
    assert_parity(sources(), &catalog(), &ast);
}

#[test]
fn parity_count_aggregate_with_child_filter() {
    // count of only the comments passing a child filter — the filter runs in the
    // reduce's input pipeline (IVM) and the `count(*)`'s WHERE (oracle).
    let ast = table("issue")
        .count_as("bigComments", |row| {
            table("comment")
                .r#where("issueId", row.col("id"))
                .where_op("body", "!=", "x")
        })
        .order_by("id", "asc")
        .build();
    assert_parity(sources(), &catalog(), &ast);
}

#[test]
fn parity_count_aggregate_alongside_materialized_sub() {
    // A count aggregate and a materialized `.sub` on the same parent, different slots.
    let ast = table("issue")
        .sub_as("comments", |row| {
            table("comment")
                .r#where("issueId", row.col("id"))
                .order_by("id", "asc")
        })
        .count_as("commentCount", |row| {
            table("comment").r#where("issueId", row.col("id"))
        })
        .order_by("id", "asc")
        .build();
    assert_parity(sources(), &catalog(), &ast);
}

#[test]
fn parity_count_aggregate_with_parent_filter() {
    // The parent is also filtered; each surviving issue still carries its count.
    let ast = table("issue")
        .where_op("id", ">", 1)
        .count_as("commentCount", |row| {
            table("comment").r#where("issueId", row.col("id"))
        })
        .order_by("id", "asc")
        .build();
    assert_parity(sources(), &catalog(), &ast);
}

// ── relationship aggregates: sum(child.col) / avg(child.col) ─────────────────────
//
// A comment fixture with an **integer** `score` column (integer sums stay integer in
// both SQLite and the IVM — no floating Kahan-summation difference to reconcile).

fn scored_comment_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "issueId", "score"], vec![0], vec![(0, true)])
}
fn scored_sources() -> Vec<TableSpec> {
    vec![
        TableSpec::new(
            "issue",
            issue_schema(),
            vec![
                vec![V::Int(1), V::str("a")],
                vec![V::Int(2), V::str("b")],
                vec![V::Int(3), V::str("c")], // childless → sum/avg NULL
            ],
        ),
        TableSpec::new(
            "comment",
            scored_comment_schema(),
            vec![
                vec![V::Int(10), V::Int(1), V::Int(3)],
                vec![V::Int(11), V::Int(1), V::Int(4)], // issue 1: sum 7, avg 3.5 (fractional)
                vec![V::Int(12), V::Int(2), V::Int(10)], // issue 2: sum 10, avg 10.0 (whole)
            ],
        ),
    ]
}
fn scored_catalog() -> Catalog {
    let mut c = Catalog::new();
    c.table("issue", &["id", "title"], &["id"]);
    c.table("comment", &["id", "issueId", "score"], &["id"]);
    c.relationship("issue", "comments", Cardinality::Many);
    c
}

#[test]
fn parity_sum_aggregate() {
    // `issue { total: sum(comments.score) }`. The oracle emits `(SELECT sum(a.score) …)`
    // (NULL for the childless issue 3); the IVM unwraps its aggregate row to the same
    // scalar (empty → the projection's NULL identity).
    let ast = table("issue")
        .sum_as("total", "score", |row| {
            table("comment").r#where("issueId", row.col("id"))
        })
        .order_by("id", "asc")
        .build();
    assert_parity(scored_sources(), &scored_catalog(), &ast);
}

#[test]
fn parity_avg_aggregate() {
    // `issue { mean: avg(comments.score) }`. SQLite `avg` and the IVM both divide the
    // integer sum by the non-NULL count in double, so the reals agree exactly.
    let ast = table("issue")
        .avg_as("mean", "score", |row| {
            table("comment").r#where("issueId", row.col("id"))
        })
        .order_by("id", "asc")
        .build();
    assert_parity(scored_sources(), &scored_catalog(), &ast);
}

#[test]
fn parity_sum_aggregate_with_child_filter() {
    // sum over only the comments passing a child filter — the filter runs below the
    // reduce (IVM) and in the `sum(...)`'s WHERE (oracle).
    let ast = table("issue")
        .sum_as("bigTotal", "score", |row| {
            table("comment")
                .r#where("issueId", row.col("id"))
                .where_op("score", ">", 3)
        })
        .order_by("id", "asc")
        .build();
    assert_parity(scored_sources(), &scored_catalog(), &ast);
}

// ── helpers ─────────────────────────────────────────────────────────────────────

fn simple(field: &str, op: Op, v: Lit) -> Condition {
    Condition::Simple(SimpleCondition {
        op,
        left: ValuePosition::Column { name: field.into() },
        right: ValuePosition::Literal { value: v },
    })
}

/// Serialize a `View` `CaughtNode` to the same name-keyed JSON shape the oracle emits:
/// columns by name, then each **in-view** relationship by name as a plural array.
fn node_to_json(node: &CaughtNode, schema: &Schema) -> Value {
    let mut obj = Map::new();
    // Projection (PROJECTION-SUPPORT-DESIGN.md §6): a projected level reports only its selected
    // columns (the d2s oracle projects identically).
    match &schema.projection {
        Some(cols) => {
            for &i in cols {
                obj.insert(
                    schema.columns[i].to_string(),
                    owned_to_json(&node.row.col(i).to_owned()),
                );
            }
        }
        None => {
            for (i, col) in schema.columns.iter().enumerate() {
                obj.insert(col.to_string(), owned_to_json(&node.row.col(i).to_owned()));
            }
        }
    }
    for (slot, rd) in schema.relationships.iter().enumerate() {
        let Some(child_schema) = rd.child.as_deref() else {
            continue; // gating (EXISTS) relationship — not in the output tree
        };
        let children = node
            .relationships
            .get(&RelId(slot as u32))
            .map(|v| v.as_slice())
            .unwrap_or(&[]);
        // A scalar-projected relationship aggregate (`count(child)`) unwraps to a scalar
        // (the projected column of the one child, or the identity `0` when empty).
        let v = if let Some(proj) = &rd.project {
            match children.first() {
                Some(c) => owned_to_json(&c.row.col(proj.col).to_owned()),
                None => owned_to_json(&proj.identity),
            }
        } else {
            Value::Array(
                children
                    .iter()
                    .map(|c| node_to_json(c, child_schema))
                    .collect(),
            )
        };
        obj.insert(rd.name.to_string(), v);
    }
    Value::Object(obj)
}

fn owned_to_json(v: &V) -> Value {
    match v {
        V::Absent => Value::Null, // parity fixtures use full rows; never `Absent`
        V::Null => Value::Null,
        V::Bool(b) => Value::Bool(*b),
        V::Int(i) => Value::Number((*i).into()),
        V::Float(f) => serde_json::Number::from_f64(*f)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        V::Str(s) => Value::String(s.to_string()),
        V::Json(s) => serde_json::from_str(s).unwrap_or(Value::Null),
    }
}

/// Build `CREATE TABLE`/`INSERT` SQL from the SAME sources the IVM consumes, so both
/// sides see identical data (no separate fixture to drift).
fn seed_sql(sources: &[TableSpec]) -> String {
    let mut s = String::new();
    for t in sources {
        let cols: Vec<String> = t.schema.columns.iter().map(|c| quote(c)).collect();
        s.push_str(&format!(
            "CREATE TABLE {} ({});\n",
            quote(&t.name),
            cols.join(", ")
        ));
        for r in &t.rows {
            let vals: Vec<String> = r.cells().map(|v| v_to_sql(&v.to_owned())).collect();
            s.push_str(&format!(
                "INSERT INTO {} VALUES ({});\n",
                quote(&t.name),
                vals.join(", ")
            ));
        }
    }
    s
}

fn v_to_sql(v: &V) -> String {
    match v {
        V::Absent => "NULL".into(), // parity fixtures use full rows; never `Absent`
        V::Null => "NULL".into(),
        V::Bool(true) => "1".into(),
        V::Bool(false) => "0".into(),
        V::Int(i) => i.to_string(),
        V::Float(f) => format!("{f}"),
        V::Str(s) | V::Json(s) => format!("'{}'", s.replace('\'', "''")),
    }
}

fn quote(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

/// Collapse integral floats to integers on both sides (the JS `number` int/float
/// non-distinction) so e.g. a column read as `5.0` compares equal to SQLite's `5`.
fn canon(v: &Value) -> Value {
    match v {
        Value::Array(a) => Value::Array(a.iter().map(canon).collect()),
        Value::Object(m) => Value::Object(m.iter().map(|(k, x)| (k.clone(), canon(x))).collect()),
        Value::Number(n) => match n.as_f64() {
            Some(f) if f.is_finite() && f.fract() == 0.0 && f.abs() < 9e15 => {
                Value::Number((f as i64).into())
            }
            _ => v.clone(),
        },
        _ => v.clone(),
    }
}

/// Design 226 Stage D (§9): the collapse is **already exact-safe** — a boundary i64 rides
/// JSON as an integer whose f64 image is `≥ 9e15`, so `canon` passes it through untouched
/// (the exact differential lanes live in `exact_i64_parity.rs` and use no canon at all).
/// Pinned so a future "simplification" can't quietly widen the exact plane.
#[test]
fn canon_is_identity_on_exact_boundary_i64() {
    for i in [9_007_199_254_740_993_i64, i64::MAX, i64::MIN] {
        let v = Value::Number(i.into());
        assert_eq!(canon(&v), v);
    }
    assert_eq!(
        canon(&serde_json::json!(5.0)),
        serde_json::json!(5),
        "the collapse below 9e15 stays"
    );
}
