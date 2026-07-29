//! Execute the compiled SQL against real SQLite (the vendored bedrock amalgamation)
//! and assert the parsed JSON result tree matches the expected hierarchical shape —
//! the same `json_object`/`json_group_array` shape the IVM `View` materializes.

use rindle::{table, Ast};
use rindle_d2s::{compile, compile_one, Cardinality, Catalog, CompileError};
use rusqlite::Connection;
use serde_json::{json, Value};

/// issue(id, title, closed) ─many→ comment(id, issueId, body); comment ─one→ issue.
fn issue_comment_catalog() -> Catalog {
    let mut c = Catalog::new();
    c.table("issue", &["id", "title", "closed"], &["id"]);
    c.table("comment", &["id", "issueId", "body"], &["id"]);
    c.relationship("issue", "comments", Cardinality::Many);
    c.relationship("comment", "issue", Cardinality::One);
    c
}

const ISSUE_COMMENT_DATA: &str = "
    CREATE TABLE issue (id INTEGER PRIMARY KEY, title TEXT, closed INTEGER);
    CREATE TABLE comment (id INTEGER PRIMARY KEY, issueId INTEGER, body TEXT);
    INSERT INTO issue VALUES (1,'a',0),(2,'b',1),(3,'c',0);
    INSERT INTO comment VALUES (10,1,'x'),(11,1,'y'),(12,2,'z');
";

/// Run a *plural* compile and return the parsed JSON array.
fn run(catalog: &Catalog, ast: &Ast, setup: &str) -> Value {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch("PRAGMA case_sensitive_like = ON;")
        .unwrap();
    conn.execute_batch(setup).unwrap();
    let sql = compile(catalog, ast).unwrap_or_else(|e| panic!("compile failed: {e}"));
    let text: String = conn
        .query_row(&sql, [], |r| r.get(0))
        .unwrap_or_else(|e| panic!("run failed: {e}\nSQL:\n{sql}"));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("bad json: {e}\n{text}\nSQL:\n{sql}"))
}

/// Run a *singular* compile (`.one()`) and return the parsed JSON object/null.
fn run_one(catalog: &Catalog, ast: &Ast, setup: &str) -> Value {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(setup).unwrap();
    let sql = compile_one(catalog, ast).unwrap();
    let text: String = conn.query_row(&sql, [], |r| r.get(0)).unwrap();
    serde_json::from_str(&text).unwrap()
}

#[test]
fn single_table_filter_order_limit() {
    let ast = table("issue")
        .where_op("closed", "=", 0)
        .order_by("id", "desc")
        .limit(2)
        .build();
    // closed=0 ⇒ ids 1,3; order id desc ⇒ [3,1]; limit 2 ⇒ both.
    let got = run(&issue_comment_catalog(), &ast, ISSUE_COMMENT_DATA);
    assert_eq!(
        got,
        json!([
            {"id": 3, "title": "c", "closed": 0},
            {"id": 1, "title": "a", "closed": 0},
        ])
    );
}

#[test]
fn many_related_children() {
    let ast = table("issue")
        .sub_as("comments", |row| {
            table("comment").r#where("issueId", row.col("id"))
        })
        .order_by("id", "asc")
        .build();
    let got = run(&issue_comment_catalog(), &ast, ISSUE_COMMENT_DATA);
    assert_eq!(
        got,
        json!([
            {"id": 1, "title": "a", "closed": 0, "comments": [
                {"id": 10, "issueId": 1, "body": "x"},
                {"id": 11, "issueId": 1, "body": "y"},
            ]},
            {"id": 2, "title": "b", "closed": 1, "comments": [
                {"id": 12, "issueId": 2, "body": "z"},
            ]},
            {"id": 3, "title": "c", "closed": 0, "comments": []},
        ])
    );
}

#[test]
fn count_aggregate_scalar_column() {
    // `issue { commentCount: count(comments) }` — a scalar `count(*)` column (0 for the
    // childless issue 3), not a nested collection (REDUCE-DESIGN.md §9).
    let ast = table("issue")
        .count_as("commentCount", |row| {
            table("comment").r#where("issueId", row.col("id"))
        })
        .order_by("id", "asc")
        .build();
    let got = run(&issue_comment_catalog(), &ast, ISSUE_COMMENT_DATA);
    assert_eq!(
        got,
        json!([
            {"id": 1, "title": "a", "closed": 0, "commentCount": 2},
            {"id": 2, "title": "b", "closed": 1, "commentCount": 1},
            {"id": 3, "title": "c", "closed": 0, "commentCount": 0},
        ])
    );
}

#[test]
fn count_aggregate_with_child_filter() {
    // count of only the comments passing the child filter (body != 'x').
    let ast = table("issue")
        .count_as("bigComments", |row| {
            table("comment")
                .r#where("issueId", row.col("id"))
                .where_op("body", "!=", "x")
        })
        .order_by("id", "asc")
        .build();
    let got = run(&issue_comment_catalog(), &ast, ISSUE_COMMENT_DATA);
    assert_eq!(
        got,
        json!([
            {"id": 1, "title": "a", "closed": 0, "bigComments": 1}, // only comment 11 (y)
            {"id": 2, "title": "b", "closed": 1, "bigComments": 1}, // comment 12 (z)
            {"id": 3, "title": "c", "closed": 0, "bigComments": 0},
        ])
    );
}

#[test]
fn count_aggregate_rejects_limit_and_start() {
    // A count ignores paging/limit, but the IVM applies them in the reduce's input
    // pipeline — so reject rather than silently diverge.
    let cat = issue_comment_catalog();
    let limited = table("issue")
        .count_as("c", |row| {
            table("comment").r#where("issueId", row.col("id")).limit(1)
        })
        .build();
    assert!(matches!(
        compile(&cat, &limited),
        Err(CompileError::Unsupported(_))
    ));
    let paged = table("issue")
        .count_as("c", |row| {
            table("comment")
                .r#where("issueId", row.col("id"))
                .start_at("id", 11)
        })
        .build();
    assert!(matches!(
        compile(&cat, &paged),
        Err(CompileError::Unsupported(_))
    ));
}

#[test]
fn sum_aggregate_scalar_column() {
    // `issue { idSum: sum(comments.id) }` — a scalar `sum(a.id)` column, NULL for the
    // childless issue 3 (SQL `sum` of no rows), matching the projection's NULL identity.
    let ast = table("issue")
        .sum_as("idSum", "id", |row| {
            table("comment").r#where("issueId", row.col("id"))
        })
        .order_by("id", "asc")
        .build();
    let got = run(&issue_comment_catalog(), &ast, ISSUE_COMMENT_DATA);
    assert_eq!(
        got,
        json!([
            {"id": 1, "title": "a", "closed": 0, "idSum": 21},   // 10 + 11
            {"id": 2, "title": "b", "closed": 1, "idSum": 12},
            {"id": 3, "title": "c", "closed": 0, "idSum": null}, // no comments → NULL
        ])
    );
}

#[test]
fn avg_aggregate_scalar_column() {
    // `issue { idAvg: avg(comments.id) }` — SQLite `avg` is real; the childless issue is
    // NULL. issue 1: (10+11)/2 = 10.5; issue 2: 12.0.
    let ast = table("issue")
        .avg_as("idAvg", "id", |row| {
            table("comment").r#where("issueId", row.col("id"))
        })
        .order_by("id", "asc")
        .build();
    let got = run(&issue_comment_catalog(), &ast, ISSUE_COMMENT_DATA);
    assert_eq!(
        got,
        json!([
            {"id": 1, "title": "a", "closed": 0, "idAvg": 10.5},
            {"id": 2, "title": "b", "closed": 1, "idAvg": 12.0},
            {"id": 3, "title": "c", "closed": 0, "idAvg": null},
        ])
    );
}

#[test]
fn one_related_child() {
    let ast = table("comment")
        .sub_as("issue", |row| {
            table("issue").r#where("id", row.col("issueId"))
        })
        .order_by("id", "asc")
        .build();
    let got = run(&issue_comment_catalog(), &ast, ISSUE_COMMENT_DATA);
    assert_eq!(
        got,
        json!([
            {"id": 10, "issueId": 1, "body": "x", "issue": {"id": 1, "title": "a", "closed": 0}},
            {"id": 11, "issueId": 1, "body": "y", "issue": {"id": 1, "title": "a", "closed": 0}},
            {"id": 12, "issueId": 2, "body": "z", "issue": {"id": 2, "title": "b", "closed": 1}},
        ])
    );
}

#[test]
fn one_related_child_null_when_absent() {
    // A comment whose issueId points nowhere ⇒ singular relationship is JSON null.
    let setup = "
        CREATE TABLE issue (id INTEGER PRIMARY KEY, title TEXT, closed INTEGER);
        CREATE TABLE comment (id INTEGER PRIMARY KEY, issueId INTEGER, body TEXT);
        INSERT INTO comment VALUES (99, 555, 'orphan');
    ";
    let ast = table("comment")
        .sub_as("issue", |row| {
            table("issue").r#where("id", row.col("issueId"))
        })
        .build();
    let got = run(&issue_comment_catalog(), &ast, setup);
    assert_eq!(
        got,
        json!([{"id": 99, "issueId": 555, "body": "orphan", "issue": null}])
    );
}

#[test]
fn exists_filter() {
    let ast = table("issue")
        .where_exists(|row| table("comment").r#where("issueId", row.col("id")))
        .order_by("id", "asc")
        .build();
    // Only issues 1 and 2 have comments; EXISTS filters, does not project children.
    let got = run(&issue_comment_catalog(), &ast, ISSUE_COMMENT_DATA);
    assert_eq!(
        got,
        json!([
            {"id": 1, "title": "a", "closed": 0},
            {"id": 2, "title": "b", "closed": 1},
        ])
    );
}

#[test]
fn not_exists_filter() {
    let ast = table("issue")
        .where_not_exists(|row| table("comment").r#where("issueId", row.col("id")))
        .order_by("id", "asc")
        .build();
    // Only issue 3 has no comments.
    let got = run(&issue_comment_catalog(), &ast, ISSUE_COMMENT_DATA);
    assert_eq!(got, json!([{"id": 3, "title": "c", "closed": 0}]));
}

#[test]
fn like_is_case_sensitive() {
    let ast = table("issue").where_op("title", "LIKE", "A").build();
    // case_sensitive_like = ON ⇒ 'A' does NOT match 'a'.
    let got = run(&issue_comment_catalog(), &ast, ISSUE_COMMENT_DATA);
    assert_eq!(got, json!([]));
}

#[test]
fn ilike_folds_case() {
    let ast = table("issue").where_op("title", "ILIKE", "A").build();
    // ILIKE lowers both sides ⇒ matches 'a'.
    let got = run(&issue_comment_catalog(), &ast, ISSUE_COMMENT_DATA);
    assert_eq!(got, json!([{"id": 1, "title": "a", "closed": 0}]));
}

#[test]
fn single_object_root() {
    let ast = table("issue").where_op("id", "=", 2).build();
    let got = run_one(&issue_comment_catalog(), &ast, ISSUE_COMMENT_DATA);
    assert_eq!(got, json!({"id": 2, "title": "b", "closed": 1}));
}
