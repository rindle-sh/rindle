//! **Correctness oracle for index advice: the index `rindle-index-advice` derives actually turns
//! the engine's real fetch from a table SCAN into an index SEARCH.**
//!
//! The `rindle-index-advice` unit tests pin that the derivation emits the *expected columns*. This
//! test closes the loop against SQLite's own planner — the check that the advice is *correct*, not
//! just that the code emits the strings we expect. For each `docs/INDEXING.md` rule it:
//!   1. derives the index from the query AST via [`derive_query`] + [`subsume`] (the code under
//!      test), and asserts the derived columns,
//!   2. builds the **exact** SQL the `TableSource` leaf runs for that fetch via
//!      [`build_select_query`] (not a hand-transcription — the real lowering), and
//!   3. `EXPLAIN QUERY PLAN`s that SQL with and without the derived index, asserting the plan
//!      flips: SCAN → `SEARCH … USING INDEX` for the R1/R2 per-parent probe, and a full-sort
//!      `USE TEMP B-TREE` → an index-ordered scan for the R3 boundary re-fetch.
//!
//! So a derivation that named the wrong columns, wrong order, or wrong direction would fail here —
//! the index it produced would not serve the engine's own fetch. Mirrors the EXPLAIN pattern in
//! `tests/start_bound_seek.rs`.

use std::collections::BTreeMap;

use rusqlite::{params_from_iter, Connection};

use rindle::ast::Ast;
use rindle::value::{Sort, ValueType};
use rindle::{Constraint, Operand, OwnedValue, SqlCondition, SqlOp};
use rindle_index_advice::{derive_query, subsume};
use rindle_sqlite::{build_select_query, ColumnDef, CompiledQuery};

fn col(name: &str) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        ty: ValueType::Number,
        optional: false,
    }
}

/// Derive + subsume the advice for `ast`, returning the suggested index columns for `table` as
/// `(name, desc)` — the columns actually under test.
fn advice(
    query: &str,
    ast: serde_json::Value,
    pks: &[(&str, &[String])],
    table: &str,
) -> Vec<(String, bool)> {
    let pk_map: BTreeMap<&str, &[String]> = pks.iter().copied().collect();
    let ast: Ast = serde_json::from_value(ast).expect("valid wire ast");
    let mut out = Vec::new();
    derive_query(query, &ast, &pk_map, &mut out);
    subsume(out)
        .iter()
        .find(|s| s.table == table)
        .map(|s| s.cols.iter().map(|c| (c.name.clone(), c.desc)).collect())
        .unwrap_or_default()
}

/// `CREATE INDEX ixadv ON "<table>" ("<col>" [DESC], …)` from the derived columns.
fn create_advice_index(conn: &Connection, table: &str, cols: &[(String, bool)]) {
    let spec = cols
        .iter()
        .map(|(n, d)| {
            if *d {
                format!("\"{n}\" DESC")
            } else {
                format!("\"{n}\"")
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    conn.execute_batch(&format!("CREATE INDEX ixadv ON \"{table}\" ({spec});"))
        .expect("create advice index");
}

/// The `detail` column (index 3) of every EXPLAIN QUERY PLAN step, joined.
fn plan(conn: &Connection, q: &CompiledQuery) -> String {
    let mut stmt = conn
        .prepare(&format!("EXPLAIN QUERY PLAN {}", q.sql))
        .expect("prepare eqp");
    stmt.query_map(params_from_iter(q.params.iter()), |r| r.get::<_, String>(3))
        .expect("eqp")
        .collect::<Result<Vec<_>, _>>()
        .expect("eqp rows")
        .join(" | ")
}

/// `track(id pk, albumId, genreId)`, 2000 rows over 400 albums × 20 genres.
fn seed_track(conn: &Connection) {
    conn.execute_batch(
        "CREATE TABLE track(id INTEGER PRIMARY KEY, albumId INTEGER NOT NULL, genreId INTEGER NOT NULL);
         WITH RECURSIVE g(k) AS (SELECT 1 UNION ALL SELECT k+1 FROM g WHERE k < 2000)
         INSERT INTO track SELECT k, k % 400, k % 20 FROM g;",
    )
    .expect("seed track");
}

#[test]
fn r1_derived_fk_index_makes_the_per_parent_fetch_seek() {
    // album → its tracks: the child is fetched once per parent by the correlation column.
    let album_pk = vec!["id".to_string()];
    let track_pk = vec!["id".to_string()];
    let cols = advice(
        "albumTracks",
        serde_json::json!({ "table": "album", "related": [ {
            "correlation": { "parentField": ["id"], "childField": ["albumId"] },
            "subquery": { "table": "track" }
        } ] }),
        &[("album", &album_pk), ("track", &track_pk)],
        "track",
    );
    assert_eq!(
        cols,
        vec![("albumId".to_string(), false)],
        "R1 derives the child FK index"
    );

    // The engine's real per-parent fetch: SELECT … FROM track WHERE "albumId" = ?
    let columns = [col("id"), col("albumId"), col("genreId")]; // ColId: id=0, albumId=1, genreId=2
    let constraint: Constraint = vec![(1, OwnedValue::Float(3.0))]; // albumId = 3
    let q = build_select_query(
        "track",
        &columns,
        Some(&constraint),
        &[],
        None,
        None,
        false,
        None,
    );

    let conn = Connection::open_in_memory().expect("open");
    seed_track(&conn);
    let before = plan(&conn, &q);
    assert!(
        before.contains("SCAN") && !before.contains("USING INDEX"),
        "without the index the per-parent fetch scans the whole table: {before}"
    );

    create_advice_index(&conn, "track", &cols);
    let after = plan(&conn, &q);
    assert!(
        after.contains("INDEX ixadv"),
        "the derived FK index turns the fetch into a seek: {after}"
    );
}

#[test]
fn r2_derived_composite_makes_the_two_equality_gate_seek() {
    // Albums that have a genre-N track: the EXISTS gate correlates AND filters → two equalities.
    let album_pk = vec!["id".to_string()];
    let track_pk = vec!["id".to_string()];
    let cols = advice(
        "albumsWithGenre",
        serde_json::json!({ "table": "album", "where": {
            "type": "correlatedSubquery", "op": "EXISTS", "related": {
                "correlation": { "parentField": ["id"], "childField": ["albumId"] },
                "subquery": { "table": "track", "where": {
                    "type": "simple", "op": "=",
                    "left": { "type": "column", "name": "genreId" },
                    "right": { "type": "literal", "value": 1 } } }
            } }
        }),
        &[("album", &album_pk), ("track", &track_pk)],
        "track",
    );
    assert_eq!(
        cols,
        vec![
            ("albumId".to_string(), false),
            ("genreId".to_string(), false)
        ],
        "R2 derives the composite, correlation column FIRST"
    );

    // The engine's real gate probe: SELECT … FROM track WHERE "albumId" = ? AND "genreId" = ?
    let columns = [col("id"), col("albumId"), col("genreId")];
    let constraint: Constraint = vec![(1, OwnedValue::Float(3.0))]; // albumId = 3
    let filter = SqlCondition::Simple {
        left: Operand::Column(2), // genreId
        op: SqlOp::Eq,
        right: Operand::Literal(OwnedValue::Float(1.0)),
    };
    let q = build_select_query(
        "track",
        &columns,
        Some(&constraint),
        &[],
        Some(&filter),
        None,
        false,
        None,
    );

    let conn = Connection::open_in_memory().expect("open");
    seed_track(&conn);
    let before = plan(&conn, &q);
    assert!(
        before.contains("SCAN") && !before.contains("USING INDEX"),
        "without the composite the two-equality gate scans: {before}"
    );

    create_advice_index(&conn, "track", &cols);
    let after = plan(&conn, &q);
    assert!(
        after.contains("INDEX ixadv"),
        "the derived composite serves both equalities in one seek: {after}"
    );
}

#[test]
fn r3_derived_sort_index_removes_the_boundary_refetch_sort() {
    // A top-N feed's boundary re-fetch is an ORDER BY on every windowed write; without an index
    // it full-sorts the table (a temp b-tree), with the derived (sortCol, pk) index it is served
    // in index order.
    let issue_pk = vec!["id".to_string()];
    let cols = advice(
        "issueFeed",
        serde_json::json!({ "table": "issue", "orderBy": [["createdAt", "desc"]], "limit": 50 }),
        &[("issue", &issue_pk)],
        "issue",
    );
    assert_eq!(
        cols,
        vec![("createdAt".to_string(), true), ("id".to_string(), false)],
        "R3 derives (sortCol DESC, pk)"
    );

    // The engine's real ordered fetch: SELECT … FROM issue ORDER BY "createdAt" DESC, "id"
    let columns = [col("id"), col("createdAt")]; // ColId: id=0, createdAt=1
    let sort: Sort = vec![(1, true), (0, false)]; // createdAt DESC, id ASC
    let q = build_select_query("issue", &columns, None, &[], None, Some(&sort), false, None);

    let conn = Connection::open_in_memory().expect("open");
    conn.execute_batch(
        "CREATE TABLE issue(id INTEGER PRIMARY KEY, createdAt REAL NOT NULL);
         WITH RECURSIVE g(k) AS (SELECT 1 UNION ALL SELECT k+1 FROM g WHERE k < 2000)
         INSERT INTO issue SELECT k, CAST(k AS REAL) FROM g;",
    )
    .expect("seed issue");

    let before = plan(&conn, &q);
    assert!(
        before.contains("TEMP B-TREE"),
        "without the index the ordered fetch full-sorts the table: {before}"
    );

    create_advice_index(&conn, "issue", &cols);
    let after = plan(&conn, &q);
    assert!(
        after.contains("INDEX ixadv") && !after.contains("TEMP B-TREE"),
        "the derived (createdAt DESC, id) index serves the order natively — no sort: {after}"
    );
}
