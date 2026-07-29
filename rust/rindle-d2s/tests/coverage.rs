//! Exhaustive branch coverage for the compiler: every operator, the full condition
//! tree (AND/OR/empty), every literal form, deep nesting, child filters/order/limit,
//! nullable ordering (`null < everything`), composite PKs, and the error paths.
//! Each test runs the emitted SQL against real SQLite and checks the parsed result.

use rindle::{
    table, Ast, Condition, CorrelatedSubquery, Correlation, Lit, Op, SimpleCondition, ValuePosition,
};
use rindle_d2s::{compile, Cardinality, Catalog, CompileError};
use rusqlite::Connection;
use serde_json::{json, Value};

// ── fixtures ──────────────────────────────────────────────────────────────────

/// issue ─many→ comment ─many→ reaction; comment ─one→ issue. `assignee` is nullable.
fn cat() -> Catalog {
    let mut c = Catalog::new();
    c.table("issue", &["id", "title", "closed", "assignee"], &["id"]);
    c.table("comment", &["id", "issueId", "body"], &["id"]);
    c.table("reaction", &["id", "commentId", "emoji"], &["id"]);
    c.relationship("issue", "comments", Cardinality::Many);
    c.relationship("comment", "reactions", Cardinality::Many);
    c.relationship("comment", "issue", Cardinality::One);
    c
}

const DATA: &str = "
    CREATE TABLE issue (id INTEGER PRIMARY KEY, title TEXT, closed INTEGER, assignee TEXT);
    CREATE TABLE comment (id INTEGER PRIMARY KEY, issueId INTEGER, body TEXT);
    CREATE TABLE reaction (id INTEGER PRIMARY KEY, commentId INTEGER, emoji TEXT);
    INSERT INTO issue VALUES (1,'a',0,'alice'),(2,'b',1,NULL),(3,'c',0,'bob');
    INSERT INTO comment VALUES (10,1,'x'),(11,1,'y'),(12,2,'z');
    INSERT INTO reaction VALUES (100,10,'p'),(101,10,'q'),(102,12,'r');
";

fn run_on(ast: &Ast, catalog: &Catalog, setup: &str) -> Value {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch("PRAGMA case_sensitive_like = ON;")
        .unwrap();
    conn.execute_batch(setup).unwrap();
    let sql = compile(catalog, ast).unwrap_or_else(|e| panic!("compile: {e}"));
    let text: String = conn
        .query_row(&sql, [], |r| r.get(0))
        .unwrap_or_else(|e| panic!("run: {e}\nSQL:\n{sql}"));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("json: {e}\n{text}\nSQL:\n{sql}"))
}

/// Compile + run against the default `cat()`/`DATA` fixture.
fn run(ast: &Ast) -> Value {
    run_on(ast, &cat(), DATA)
}

/// The `"id"` of each top-level result row, in result order.
fn ids(v: &Value) -> Vec<i64> {
    v.as_array()
        .unwrap()
        .iter()
        .map(|o| o["id"].as_i64().unwrap())
        .collect()
}

// builders for ASTs the fluent builder can't express (RHS columns, OR, raw literals)
fn col(name: &str) -> ValuePosition {
    ValuePosition::Column { name: name.into() }
}
fn lit(v: Lit) -> ValuePosition {
    ValuePosition::Literal { value: v }
}
fn simple(field: &str, op: Op, v: Lit) -> Condition {
    Condition::Simple(SimpleCondition {
        op,
        left: col(field),
        right: lit(v),
    })
}
fn issue_where(cond: Condition) -> Ast {
    Ast {
        table: "issue".into(),
        r#where: Some(cond),
        ..Default::default()
    }
}

// ── comparison operators ────────────────────────────────────────────────────────

#[test]
fn op_gt() {
    assert_eq!(
        ids(&run(&issue_where(simple("id", Op::Gt, Lit::Number(1.0))))),
        vec![2, 3]
    );
}
#[test]
fn op_ge() {
    assert_eq!(
        ids(&run(&issue_where(simple("id", Op::Ge, Lit::Number(2.0))))),
        vec![2, 3]
    );
}
#[test]
fn op_lt() {
    assert_eq!(
        ids(&run(&issue_where(simple("id", Op::Lt, Lit::Number(3.0))))),
        vec![1, 2]
    );
}
#[test]
fn op_le() {
    assert_eq!(
        ids(&run(&issue_where(simple("id", Op::Le, Lit::Number(2.0))))),
        vec![1, 2]
    );
}
#[test]
fn op_ne() {
    assert_eq!(
        ids(&run(&issue_where(simple("id", Op::Ne, Lit::Number(2.0))))),
        vec![1, 3]
    );
}

// ── IS / IS NOT (null-safe) ─────────────────────────────────────────────────────

#[test]
fn is_null() {
    assert_eq!(
        ids(&run(&issue_where(simple("assignee", Op::Is, Lit::Null)))),
        vec![2]
    );
}
#[test]
fn is_not_null() {
    assert_eq!(
        ids(&run(&issue_where(simple("assignee", Op::IsNot, Lit::Null)))),
        vec![1, 3]
    );
}
#[test]
fn eq_null_is_never_true() {
    // `= NULL` (not null-safe) matches nothing — distinct from `IS NULL`.
    assert_eq!(
        ids(&run(&issue_where(simple("assignee", Op::Eq, Lit::Null)))),
        Vec::<i64>::new()
    );
}

// ── LIKE family ─────────────────────────────────────────────────────────────────

#[test]
fn not_like() {
    // titles not matching 'a' ⇒ 'b','c'.
    assert_eq!(
        ids(&run(&issue_where(simple(
            "title",
            Op::NotLike,
            Lit::Str("a".into())
        )))),
        vec![2, 3]
    );
}
#[test]
fn not_ilike() {
    assert_eq!(
        ids(&run(&issue_where(simple(
            "title",
            Op::NotILike,
            Lit::Str("A".into())
        )))),
        vec![2, 3]
    );
}

// ── IN / NOT IN ─────────────────────────────────────────────────────────────────

fn arr(ns: &[f64]) -> Lit {
    Lit::Array(ns.iter().map(|n| Lit::Number(*n)).collect())
}

#[test]
fn op_in() {
    assert_eq!(
        ids(&run(&issue_where(simple("id", Op::In, arr(&[1.0, 3.0]))))),
        vec![1, 3]
    );
}
#[test]
fn op_not_in() {
    assert_eq!(
        ids(&run(&issue_where(simple(
            "id",
            Op::NotIn,
            arr(&[1.0, 3.0])
        )))),
        vec![2]
    );
}
#[test]
fn op_in_empty_matches_nothing() {
    assert_eq!(
        ids(&run(&issue_where(simple("id", Op::In, arr(&[]))))),
        Vec::<i64>::new()
    );
}
#[test]
fn op_not_in_empty_matches_everything() {
    assert_eq!(
        ids(&run(&issue_where(simple("id", Op::NotIn, arr(&[]))))),
        vec![1, 2, 3]
    );
}

// ── AND / OR / empty ────────────────────────────────────────────────────────────

#[test]
fn and_of_conditions() {
    let cond = Condition::And {
        conditions: vec![
            simple("closed", Op::Eq, Lit::Number(0.0)),
            simple("id", Op::Lt, Lit::Number(3.0)),
        ],
    };
    assert_eq!(ids(&run(&issue_where(cond))), vec![1]);
}
#[test]
fn or_of_conditions() {
    let cond = Condition::Or {
        conditions: vec![
            simple("closed", Op::Eq, Lit::Number(1.0)),
            simple("id", Op::Eq, Lit::Number(3.0)),
        ],
    };
    assert_eq!(ids(&run(&issue_where(cond))), vec![2, 3]);
}
#[test]
fn empty_and_is_true() {
    assert_eq!(
        ids(&run(&issue_where(Condition::And { conditions: vec![] }))),
        vec![1, 2, 3]
    );
}
#[test]
fn empty_or_is_false() {
    assert_eq!(
        ids(&run(&issue_where(Condition::Or { conditions: vec![] }))),
        Vec::<i64>::new()
    );
}
#[test]
fn nested_and_or() {
    // closed=0 AND (id=1 OR id=3) ⇒ {1,3}.
    let cond = Condition::And {
        conditions: vec![
            simple("closed", Op::Eq, Lit::Number(0.0)),
            Condition::Or {
                conditions: vec![
                    simple("id", Op::Eq, Lit::Number(1.0)),
                    simple("id", Op::Eq, Lit::Number(3.0)),
                ],
            },
        ],
    };
    assert_eq!(ids(&run(&issue_where(cond))), vec![1, 3]);
}

// ── literals: bool, float, string-escape, column-on-RHS ─────────────────────────

#[test]
fn bool_true_inlines_as_one() {
    assert_eq!(
        ids(&run(&issue_where(simple(
            "closed",
            Op::Eq,
            Lit::Bool(true)
        )))),
        vec![2]
    );
}
#[test]
fn bool_false_inlines_as_zero() {
    assert_eq!(
        ids(&run(&issue_where(simple(
            "closed",
            Op::Eq,
            Lit::Bool(false)
        )))),
        vec![1, 3]
    );
}
#[test]
fn float_literal_and_output() {
    let setup = "CREATE TABLE product (id INTEGER PRIMARY KEY, price REAL);
                 INSERT INTO product VALUES (1, 0.5),(2, 19.5);";
    let mut c = Catalog::new();
    c.table("product", &["id", "price"], &["id"]);
    // Non-integral literal compiles to a float; exact-binary values round-trip in output.
    let ast = Ast {
        table: "product".into(),
        r#where: Some(simple("price", Op::Gt, Lit::Number(1.0))),
        ..Default::default()
    };
    assert_eq!(run_on(&ast, &c, setup), json!([{"id": 2, "price": 19.5}]));
    let ast_eq = Ast {
        table: "product".into(),
        r#where: Some(simple("price", Op::Eq, Lit::Number(0.5))),
        ..Default::default()
    };
    assert_eq!(run_on(&ast_eq, &c, setup), json!([{"id": 1, "price": 0.5}]));
}
#[test]
fn string_literal_is_quote_escaped() {
    let setup =
        "CREATE TABLE issue (id INTEGER PRIMARY KEY, title TEXT, closed INTEGER, assignee TEXT);
                 INSERT INTO issue VALUES (4, 'o''brien', 0, NULL);";
    let ast = issue_where(simple("title", Op::Eq, Lit::Str("o'brien".into())));
    assert_eq!(
        run_on(&ast, &cat(), setup),
        json!([{"id": 4, "title": "o'brien", "closed": 0, "assignee": null}])
    );
}
#[test]
fn column_on_rhs() {
    // closed = id ⇒ only a row where the two columns are equal.
    let setup =
        "CREATE TABLE issue (id INTEGER PRIMARY KEY, title TEXT, closed INTEGER, assignee TEXT);
                 INSERT INTO issue VALUES (0,'z',0,NULL),(1,'a',5,NULL);";
    let ast = issue_where(Condition::Simple(SimpleCondition {
        op: Op::Eq,
        left: col("closed"),
        right: col("id"),
    }));
    assert_eq!(ids(&run_on(&ast, &cat(), setup)), vec![0]);
}

// ── ordering ────────────────────────────────────────────────────────────────────

#[test]
fn multi_column_order() {
    // closed ASC, id DESC ⇒ closed=0 {3,1}, then closed=1 {2}.
    let ast = table("issue")
        .order_by("closed", "asc")
        .order_by("id", "desc")
        .build();
    assert_eq!(ids(&run(&ast)), vec![3, 1, 2]);
}
#[test]
fn nullable_order_asc_nulls_first() {
    // assignee ASC: NULL (issue 2) < 'alice' (1) < 'bob' (3) — `null < everything`.
    let ast = table("issue").order_by("assignee", "asc").build();
    assert_eq!(ids(&run(&ast)), vec![2, 1, 3]);
}
#[test]
fn nullable_order_desc_nulls_last() {
    let ast = table("issue").order_by("assignee", "desc").build();
    assert_eq!(ids(&run(&ast)), vec![3, 1, 2]);
}

// ── limit ───────────────────────────────────────────────────────────────────────

#[test]
fn limit_zero() {
    let ast = table("issue").order_by("id", "asc").limit(0).build();
    assert_eq!(ids(&run(&ast)), Vec::<i64>::new());
}

// ── deep nesting & child clauses ────────────────────────────────────────────────

#[test]
fn nested_related_depth_two() {
    // issue → comments → reactions (subtype must survive two levels).
    let ast = table("issue")
        .sub_as("comments", |row| {
            table("comment")
                .r#where("issueId", row.col("id"))
                .sub_as("reactions", |r2| {
                    table("reaction").r#where("commentId", r2.col("id"))
                })
        })
        .order_by("id", "asc")
        .build();
    assert_eq!(
        run(&ast),
        json!([
            {"id": 1, "title": "a", "closed": 0, "assignee": "alice", "comments": [
                {"id": 10, "issueId": 1, "body": "x", "reactions": [
                    {"id": 100, "commentId": 10, "emoji": "p"},
                    {"id": 101, "commentId": 10, "emoji": "q"},
                ]},
                {"id": 11, "issueId": 1, "body": "y", "reactions": []},
            ]},
            {"id": 2, "title": "b", "closed": 1, "assignee": null, "comments": [
                {"id": 12, "issueId": 2, "body": "z", "reactions": [
                    {"id": 102, "commentId": 12, "emoji": "r"},
                ]},
            ]},
            {"id": 3, "title": "c", "closed": 0, "assignee": "bob", "comments": []},
        ])
    );
}

#[test]
fn related_child_with_where_order_limit() {
    // issue → comments (body != 'x', ORDER BY id DESC, LIMIT 1).
    let ast = table("issue")
        .sub_as("comments", |row| {
            table("comment")
                .r#where("issueId", row.col("id"))
                .where_op("body", "!=", "x")
                .order_by("id", "desc")
                .limit(1)
        })
        .order_by("id", "asc")
        .build();
    assert_eq!(
        run(&ast),
        json!([
            {"id": 1, "title": "a", "closed": 0, "assignee": "alice", "comments": [
                {"id": 11, "issueId": 1, "body": "y"},
            ]},
            {"id": 2, "title": "b", "closed": 1, "assignee": null, "comments": [
                {"id": 12, "issueId": 2, "body": "z"},
            ]},
            {"id": 3, "title": "c", "closed": 0, "assignee": "bob", "comments": []},
        ])
    );
}

#[test]
fn exists_with_inner_filter() {
    // issues with a comment whose body = 'y' ⇒ only issue 1.
    let ast = table("issue")
        .where_exists(|row| {
            table("comment")
                .r#where("issueId", row.col("id"))
                .where_op("body", "=", "y")
        })
        .order_by("id", "asc")
        .build();
    assert_eq!(ids(&run(&ast)), vec![1]);
}

#[test]
fn nested_exists() {
    // issues that have a comment that has a reaction ⇒ {1,2}.
    let ast = table("issue")
        .where_exists(|i| {
            table("comment")
                .r#where("issueId", i.col("id"))
                .where_exists(|c| table("reaction").r#where("commentId", c.col("id")))
        })
        .order_by("id", "asc")
        .build();
    assert_eq!(ids(&run(&ast)), vec![1, 2]);
}

// ── composite primary key ───────────────────────────────────────────────────────

#[test]
fn composite_pk_root_ordering() {
    let setup = "CREATE TABLE enrollment (studentId INTEGER, courseId INTEGER, PRIMARY KEY (studentId, courseId));
                 INSERT INTO enrollment VALUES (2,100),(1,101),(1,100);";
    let mut c = Catalog::new();
    c.table(
        "enrollment",
        &["studentId", "courseId"],
        &["studentId", "courseId"],
    );
    // No order_by ⇒ complete_ordering appends (studentId, courseId) ⇒ deterministic.
    let ast = Ast {
        table: "enrollment".into(),
        ..Default::default()
    };
    assert_eq!(
        run_on(&ast, &c, setup),
        json!([
            {"studentId": 1, "courseId": 100},
            {"studentId": 1, "courseId": 101},
            {"studentId": 2, "courseId": 100},
        ])
    );
}

// ── error paths ─────────────────────────────────────────────────────────────────

#[test]
fn err_unknown_table() {
    let ast = Ast {
        table: "nope".into(),
        ..Default::default()
    };
    assert_eq!(
        compile(&cat(), &ast),
        Err(CompileError::UnknownTable("nope".into()))
    );
}

#[test]
fn err_unknown_relationship() {
    // A `related` whose alias is not declared in the catalog.
    let ast = table("issue")
        .sub_as("ghosts", |row| {
            table("comment").r#where("issueId", row.col("id"))
        })
        .build();
    assert_eq!(
        compile(&cat(), &ast),
        Err(CompileError::UnknownRelationship {
            table: "issue".into(),
            rel: "ghosts".into(),
        })
    );
}

#[test]
fn err_missing_alias() {
    // A `related` subquery with no alias.
    let ast = Ast {
        table: "issue".into(),
        related: vec![CorrelatedSubquery {
            correlation: Correlation {
                parent_field: vec!["id".into()],
                child_field: vec!["issueId".into()],
            },
            system: None,
            subquery: Box::new(Ast {
                table: "comment".into(),
                alias: None,
                ..Default::default()
            }),
        }],
        ..Default::default()
    };
    assert_eq!(
        compile(&cat(), &ast),
        Err(CompileError::MissingAlias {
            table: "comment".into()
        })
    );
}

#[test]
fn start_paging_inclusive_and_exclusive() {
    // `start` lowers to a keyset predicate: inclusive keeps the bound row, exclusive drops it.
    let inc = table("issue")
        .start_at("id", 2)
        .order_by("id", "asc")
        .build();
    assert_eq!(ids(&run(&inc)), vec![2, 3]);
    let exc = table("issue")
        .start_after("id", 2)
        .order_by("id", "asc")
        .build();
    assert_eq!(ids(&run(&exc)), vec![3]);
}

#[test]
fn start_paging_descending_and_window() {
    // DESC: the rows "after" the bound have a smaller key.
    let desc = table("issue")
        .start_after("id", 3)
        .order_by("id", "desc")
        .build();
    assert_eq!(ids(&run(&desc)), vec![2, 1]);
    // start + LIMIT = a paging window.
    let window = table("issue")
        .start_at("id", 1)
        .order_by("id", "asc")
        .limit(2)
        .build();
    assert_eq!(ids(&run(&window)), vec![1, 2]);
}

#[test]
fn err_start_inside_exists_rejected() {
    // `start` is honored on materialized collections, but not inside an EXISTS (existence
    // ignores paging). It must error loudly there, never be silently dropped.
    let ast = table("issue")
        .where_exists(|i| {
            table("comment")
                .alias("c")
                .r#where("issueId", i.col("id"))
                .start_after("id", 10)
        })
        .order_by("id", "asc")
        .build();
    assert!(matches!(
        compile(&cat(), &ast),
        Err(CompileError::Unsupported(_))
    ));
}
