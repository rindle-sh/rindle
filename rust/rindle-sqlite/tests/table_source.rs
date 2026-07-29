//! End-to-end tests for the production [`TableSource`] (spec `05` §10): the
//! zero-copy lazy fetch over a real `rusqlite` cursor, the overlay/start/edit-split
//! machinery shared with the memory leaf, the write path, `get_row`, the value
//! boundary (bool/null/json/lossless integer-to-f64), self-join reentrancy, and RAII cursor
//! release. Direct ports of the `table-source.test.ts` cases.
//!
//! **Number columns vend as f64** (the JS `number` is f64 — INTEGER widens through
//! `Number(bigint)`), so every number value in a row/constraint/start here is built
//! as `Float`, matching what the leaf vends (mixing `Int`/`Float` for one column
//! would trip the `compare_values` type-mismatch guard).

use std::cell::RefCell;
use std::rc::Rc;

use rusqlite::Connection;

use rindle::change::{Basis, FetchRequest, Start};
use rindle::graph::NodeId;
use rindle::value::{RelId, Value};
use rindle::{
    owned_row as orow, ColId, ConnId, Connection as Conn, ConnectionFilters, Constraint, Graph,
    MultiConstraint, Node, Operand, OutEdge, OwnedRow, OwnedValue, Port, RelDef, RindleError,
    RowPredicate, Schema, Source, SourceChange, SourceSchema, SqlCondition, SqlOp, ValueType,
};
use rindle_sqlite::{ColumnDef, GraphTableSourceExt, TableSource};

const ID: ColId = 0;
const OWNER: ColId = 1;

// --- helpers -------------------------------------------------------------

/// A number column's owned value (f64 — see the module note).
fn n(x: i64) -> OwnedValue {
    OwnedValue::Float(x as f64)
}

fn issue(id: i64, owner: Option<i64>, title: &str) -> OwnedRow {
    orow(vec![
        n(id),
        owner.map(n).unwrap_or(OwnedValue::Null),
        OwnedValue::str(title),
    ])
}

fn id_of(row: &OwnedRow) -> i64 {
    match row.col(ID) {
        Value::Float(f) => f as i64,
        Value::Int(i) => i,
        other => panic!("expected number id, got {other:?}"),
    }
}

fn fetch_ids(ts: &TableSource, conn: ConnId, req: &FetchRequest) -> Vec<i64> {
    ts.fetch(conn, req).map(|r| id_of(&r)).collect()
}

fn constraint(col: ColId, v: i64) -> Constraint {
    vec![(col, n(v))]
}

fn wire(ts: &TableSource, conn: ConnId) {
    ts.set_conn_output(
        conn,
        OutEdge {
            node: NodeId::new(0, 0),
            port: Port::Single,
        },
    );
}

/// issue(id:number!, owner:number?, title:string!); PK=id, with an explicit
/// UNIQUE index (the discoverable one the PK-UNIQUE assert requires — a bare
/// `INTEGER PRIMARY KEY` is the rowid and has no listed index, §5.1).
fn issues() -> (Rc<Connection>, TableSource) {
    let conn = Rc::new(Connection::open_in_memory().unwrap());
    conn.execute_batch(
        "CREATE TABLE issue (id INTEGER NOT NULL, owner INTEGER, title TEXT NOT NULL);
         CREATE UNIQUE INDEX issue_pk ON issue (id);",
    )
    .unwrap();
    let cols = vec![
        ColumnDef {
            name: "id".into(),
            ty: ValueType::Number,
            optional: false,
        },
        ColumnDef {
            name: "owner".into(),
            ty: ValueType::Number,
            optional: true,
        },
        ColumnDef {
            name: "title".into(),
            ty: ValueType::String,
            optional: false,
        },
    ];
    let ts = TableSource::new(conn.clone(), "issue", cols, vec![ID]);
    (conn, ts)
}

/// Seed rows straight into the table (bypassing the source write path).
fn seed(conn: &Connection, rows: &[(i64, Option<i64>, &str)]) {
    for (id, owner, title) in rows {
        conn.execute(
            "INSERT INTO issue (id, owner, title) VALUES (?1, ?2, ?3)",
            rusqlite::params![id, owner, title],
        )
        .unwrap();
    }
}

// =========================================================================
// Fetch: ordered, constraint, reverse, start, compound, multiConstraint
// =========================================================================

#[test]
fn fetch_all_in_sort_order() {
    let (conn, ts) = issues();
    seed(
        &conn,
        &[(3, Some(1), "c"), (1, Some(1), "a"), (2, Some(2), "b")],
    );
    let c = ts.connect(Some(vec![(ID, true)]), None, vec![]);
    assert_eq!(fetch_ids(&ts, c, &FetchRequest::all()), vec![1, 2, 3]);
    assert_eq!(ts.cursors_open(), 0, "cursor released after drain");
}

#[test]
fn connection_wraps_emitted_rows_into_nodes() {
    // The source emits ROWS; the connection boundary wraps each in a leaf Node.
    let (conn, ts) = issues();
    seed(&conn, &[(1, Some(1), "a"), (2, Some(2), "b")]);
    let c = ts.connect(Some(vec![(ID, true)]), None, vec![]);
    let nodes: Vec<Node> = ts.fetch(c, &FetchRequest::all()).map(Node::leaf).collect();
    assert_eq!(nodes.len(), 2);
    assert!(nodes.iter().all(|node| node.rels.is_empty()), "leaf nodes");
    assert_eq!(
        nodes
            .iter()
            .map(|node| id_of(&node.row))
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
}

#[test]
fn fetch_with_constraint() {
    let (conn, ts) = issues();
    seed(
        &conn,
        &[(1, Some(10), "a"), (2, Some(20), "b"), (3, Some(10), "c")],
    );
    let c = ts.connect(Some(vec![(ID, true)]), None, vec![]);
    let req = FetchRequest::with_constraint(constraint(OWNER, 10));
    assert_eq!(fetch_ids(&ts, c, &req), vec![1, 3]);
}

#[test]
fn fetch_reverse_is_reversed_forward() {
    let (conn, ts) = issues();
    seed(
        &conn,
        &[(1, Some(1), "a"), (2, Some(1), "b"), (3, Some(1), "c")],
    );
    let c = ts.connect(Some(vec![(ID, true)]), None, vec![]);
    let req = FetchRequest {
        reverse: true,
        ..Default::default()
    };
    assert_eq!(fetch_ids(&ts, c, &req), vec![3, 2, 1]);
}

#[test]
fn fetch_start_at_and_after() {
    let (conn, ts) = issues();
    seed(
        &conn,
        &[(1, Some(1), "a"), (2, Some(1), "b"), (3, Some(1), "c")],
    );
    let c = ts.connect(Some(vec![(ID, true)]), None, vec![]);

    let at = FetchRequest {
        start: Some(Start {
            row: issue(2, Some(1), "b"),
            basis: Basis::At,
        }),
        ..Default::default()
    };
    assert_eq!(fetch_ids(&ts, c, &at), vec![2, 3]);

    let after = FetchRequest {
        start: Some(Start {
            row: issue(2, Some(1), "b"),
            basis: Basis::After,
        }),
        ..Default::default()
    };
    assert_eq!(fetch_ids(&ts, c, &after), vec![3]);
}

#[test]
fn fetch_compound_mixed_direction_order() {
    let (conn, ts) = issues();
    // order owner asc, id desc.
    seed(
        &conn,
        &[
            (1, Some(10), "a"),
            (2, Some(10), "b"),
            (3, Some(20), "c"),
            (4, Some(20), "d"),
        ],
    );
    let c = ts.connect(Some(vec![(OWNER, true), (ID, false)]), None, vec![]);
    assert_eq!(fetch_ids(&ts, c, &FetchRequest::all()), vec![2, 1, 4, 3]);
}

#[test]
fn fetch_multi_constraint_in_batch() {
    let (conn, ts) = issues();
    seed(
        &conn,
        &[
            (1, Some(1), "a"),
            (2, Some(2), "b"),
            (3, Some(3), "c"),
            (4, Some(4), "d"),
        ],
    );
    let c = ts.connect(Some(vec![(ID, true)]), None, vec![]);
    let mc: MultiConstraint = vec![constraint(ID, 1), constraint(ID, 3), constraint(ID, 4)];
    let req = FetchRequest {
        multi_constraints: vec![mc],
        ..Default::default()
    };
    assert_eq!(fetch_ids(&ts, c, &req), vec![1, 3, 4]);
}

// =========================================================================
// Value boundary: bool / null / json / lossless integer-to-f64
// =========================================================================

fn value_types_db() -> (Rc<Connection>, TableSource) {
    let conn = Rc::new(Connection::open_in_memory().unwrap());
    conn.execute_batch(
        "CREATE TABLE vt (id INTEGER NOT NULL, num REAL, txt TEXT, flag INTEGER, js TEXT);
         CREATE UNIQUE INDEX vt_pk ON vt (id);",
    )
    .unwrap();
    let cols = vec![
        ColumnDef {
            name: "id".into(),
            ty: ValueType::Number,
            optional: false,
        },
        ColumnDef {
            name: "num".into(),
            ty: ValueType::Number,
            optional: true,
        },
        ColumnDef {
            name: "txt".into(),
            ty: ValueType::String,
            optional: true,
        },
        ColumnDef {
            name: "flag".into(),
            ty: ValueType::Boolean,
            optional: true,
        },
        ColumnDef {
            name: "js".into(),
            ty: ValueType::Json,
            optional: true,
        },
    ];
    let ts = TableSource::new(conn.clone(), "vt", cols, vec![0]);
    (conn, ts)
}

#[test]
fn value_types_roundtrip_via_get_row() {
    let (conn, ts) = value_types_db();
    conn.execute_batch(
        r#"INSERT INTO vt VALUES (1, 3.5, 'hi', 1, '{"a":1}');
           INSERT INTO vt VALUES (2, NULL, NULL, 0, NULL);"#,
    )
    .unwrap();

    let r1 = ts.get_row(&[(0, n(1))]).expect("row 1");
    assert!(matches!(r1.col(1), Value::Float(f) if (f - 3.5).abs() < 1e-9));
    assert!(matches!(r1.col(2), Value::Str(b"hi")));
    assert!(matches!(r1.col(3), Value::Bool(true)));
    assert!(matches!(r1.col(4), Value::Json(b) if b == br#"{"a":1}"#));

    let r2 = ts.get_row(&[(0, n(2))]).expect("row 2");
    assert!(matches!(r2.col(1), Value::Null), "null number");
    assert!(matches!(r2.col(2), Value::Null), "null string");
    assert!(matches!(r2.col(3), Value::Bool(false)), "flag 0 -> false");
    assert!(matches!(r2.col(4), Value::Null), "null json");

    assert!(ts.get_row(&[(0, n(999))]).is_none(), "absent -> None");
}

#[test]
fn inexact_f64_integer_parks_unsupported_value() {
    let conn = Rc::new(Connection::open_in_memory().unwrap());
    conn.execute_batch(
        "CREATE TABLE big (id INTEGER NOT NULL);
         CREATE UNIQUE INDEX big_pk ON big (id);
         INSERT INTO big VALUES (1), (9007199254740993);", // 2^53 + 1 loses precision as f64
    )
    .unwrap();
    let cols = vec![ColumnDef {
        name: "id".into(),
        ty: ValueType::Number,
        optional: false,
    }];
    let ts = TableSource::new(conn.clone(), "big", cols, vec![0]);

    let c = ts.connect(Some(vec![(0, true)]), None, vec![]);
    // id 1 vends; the inexact 2^53 + 1 row ends the stream with a value error.
    let ids = fetch_ids(&ts, c, &FetchRequest::all());
    assert_eq!(ids, vec![1], "the exact row vended before the error");
    assert!(
        matches!(
            ts.take_fetch_error(),
            Some(RindleError::UnsupportedValue(_))
        ),
        "the out-of-range integer parked an UnsupportedValue, not a silent end"
    );
    assert!(ts.take_fetch_error().is_none(), "error consumed once");
    assert_eq!(
        ts.cursors_open(),
        0,
        "cursor released even on the error path"
    );
}

#[test]
fn exactly_representable_large_integers_vend_as_numbers() {
    let conn = Rc::new(Connection::open_in_memory().unwrap());
    conn.execute_batch(
        "CREATE TABLE big (id INTEGER NOT NULL);
         CREATE UNIQUE INDEX big_pk ON big (id);
         INSERT INTO big VALUES
           (-9223372036854775808),
           (9007199254740992),
           (18014398509481984);",
    )
    .unwrap();
    let cols = vec![ColumnDef {
        name: "id".into(),
        ty: ValueType::Number,
        optional: false,
    }];
    let ts = TableSource::new(conn.clone(), "big", cols, vec![0]);

    let c = ts.connect(Some(vec![(0, true)]), None, vec![]);
    assert_eq!(
        fetch_ids(&ts, c, &FetchRequest::all()),
        vec![i64::MIN, 1_i64 << 53, 1_i64 << 54]
    );
    assert!(ts.take_fetch_error().is_none());
}

#[test]
fn invalid_text_bytes_surface_as_rindle_error() {
    let conn = Rc::new(Connection::open_in_memory().unwrap());
    conn.execute_batch(
        "CREATE TABLE bad (id INTEGER NOT NULL, title TEXT NOT NULL);
         CREATE UNIQUE INDEX bad_pk ON bad (id);
         INSERT INTO bad VALUES (1, CAST(x'FF' AS TEXT));",
    )
    .unwrap();
    let cols = vec![
        ColumnDef {
            name: "id".into(),
            ty: ValueType::Number,
            optional: false,
        },
        ColumnDef {
            name: "title".into(),
            ty: ValueType::String,
            optional: false,
        },
    ];
    let ts = TableSource::new(conn, "bad", cols, vec![0]);
    let c = ts.connect(Some(vec![(0, true)]), None, vec![]);

    let rows: Vec<_> = ts.fetch(c, &FetchRequest::all()).collect();
    assert!(rows.is_empty());
    assert!(matches!(
        ts.take_fetch_error(),
        Some(RindleError::InvalidUtf8 {
            value_type: "TEXT",
            ..
        })
    ));
}

#[test]
fn graph_try_fetch_all_surfaces_source_runtime_errors() {
    let conn = Rc::new(Connection::open_in_memory().unwrap());
    conn.execute_batch(
        "CREATE TABLE bad (id INTEGER NOT NULL, title TEXT NOT NULL);
         CREATE UNIQUE INDEX bad_pk ON bad (id);
         INSERT INTO bad VALUES (1, CAST(x'FF' AS TEXT));",
    )
    .unwrap();
    let cols = vec![
        ColumnDef {
            name: "id".into(),
            ty: ValueType::Number,
            optional: false,
        },
        ColumnDef {
            name: "title".into(),
            ty: ValueType::String,
            optional: false,
        },
    ];
    let ts = TableSource::new(conn, "bad", cols, vec![0]);
    let mut g = Graph::new();
    let src = g.add_table_source(ts);
    let top = g.connect(src, Some(vec![(0, true)]), None, vec![]);

    assert!(matches!(
        g.try_fetch_all(top, &FetchRequest::all()),
        Err(RindleError::InvalidUtf8 {
            value_type: "TEXT",
            ..
        })
    ));
}

#[test]
fn graph_try_hydrate_surfaces_nested_relationship_runtime_errors() {
    let conn = Rc::new(Connection::open_in_memory().unwrap());
    conn.execute_batch(
        "CREATE TABLE issue (id INTEGER NOT NULL);
         CREATE UNIQUE INDEX issue_pk ON issue (id);
         INSERT INTO issue VALUES (1);
         CREATE TABLE comment (id INTEGER NOT NULL, issue INTEGER NOT NULL, title TEXT NOT NULL);
         CREATE UNIQUE INDEX comment_pk ON comment (id);
         INSERT INTO comment VALUES (10, 1, CAST(x'FF' AS TEXT));",
    )
    .unwrap();
    let issue_cols = vec![ColumnDef {
        name: "id".into(),
        ty: ValueType::Number,
        optional: false,
    }];
    let comment_cols = vec![
        ColumnDef {
            name: "id".into(),
            ty: ValueType::Number,
            optional: false,
        },
        ColumnDef {
            name: "issue".into(),
            ty: ValueType::Number,
            optional: false,
        },
        ColumnDef {
            name: "title".into(),
            ty: ValueType::String,
            optional: false,
        },
    ];
    // The VIEW schema keeps the relationship ("comments" => RelId(0)); the SOURCE
    // schema the leaf is registered with carries none (relationships are query-local).
    let comment_schema = Schema::new(vec!["id", "issue", "title"], vec![0], vec![(0, true)]);
    let issue_view_schema = Schema::new(vec!["id"], vec![0], vec![(0, true)])
        .with_relationships(vec![RelDef::related("comments", comment_schema)]);
    let issue_source_schema = SourceSchema::new(vec!["id"], vec![0], vec![(0, true)]);

    let issue_ts = TableSource::new_with_schema(
        conn.clone(),
        "issue",
        issue_cols,
        vec![0],
        issue_source_schema,
    );
    let comment_ts = TableSource::new(conn, "comment", comment_cols, vec![0]);

    let mut g = Graph::new();
    let issues = g.add_table_source(issue_ts);
    let comments = g.add_table_source(comment_ts);
    let parent = g.connect(issues, Some(vec![(0, true)]), None, vec![]);
    let child = g.connect(comments, Some(vec![(0, true)]), None, vec![]);
    let join = g.add_join_slot(parent, child, vec![0], vec![1], RelId(0));
    let view = g.add_view(join, issue_view_schema);
    g.set_conn_output(
        parent,
        OutEdge {
            node: join,
            port: Port::JoinParent,
        },
    );
    g.set_conn_output(
        child,
        OutEdge {
            node: join,
            port: Port::JoinChild,
        },
    );
    g.set_output(join, view);

    assert!(matches!(
        g.try_hydrate(view),
        Err(RindleError::InvalidUtf8 {
            value_type: "TEXT",
            ..
        })
    ));
}

// =========================================================================
// Write path: add / remove / edit (update & delete+insert) (§3.11)
// =========================================================================

fn table_dump(conn: &Connection) -> Vec<(i64, Option<i64>, String)> {
    let mut s = conn
        .prepare("SELECT id, owner, title FROM issue ORDER BY id")
        .unwrap();
    s.query_map([], |r| {
        Ok((
            r.get::<_, i64>(0)?,
            r.get::<_, Option<i64>>(1)?,
            r.get::<_, String>(2)?,
        ))
    })
    .unwrap()
    .map(|r| r.unwrap())
    .collect()
}

#[test]
fn push_add_and_remove_writes_table_and_fans_out() {
    let (conn, ts) = issues();
    let c = ts.connect(Some(vec![(ID, true)]), None, vec![]);
    wire(&ts, c);
    let rec = RefCell::new(Vec::new());
    let record = |_c: &Conn, sc: SourceChange| {
        rec.borrow_mut().push(match sc {
            SourceChange::Add(_) => "add",
            SourceChange::Remove(_) => "remove",
            SourceChange::Edit { .. } => "edit",
        });
    };

    ts.push(SourceChange::Add(issue(7, Some(1), "g")), &record);
    assert_eq!(table_dump(&conn), vec![(7, Some(1), "g".to_string())]);

    ts.push(SourceChange::Remove(issue(7, Some(1), "g")), &record);
    assert_eq!(table_dump(&conn), vec![]);

    assert_eq!(rec.into_inner(), vec!["add", "remove"]);
    assert_eq!(ts.cursors_open(), 0);
}

#[test]
fn try_push_surfaces_sqlite_write_error() {
    let (_conn, ts) = issues();
    let bad = orow(vec![n(7), OwnedValue::Null, OwnedValue::Null]);

    let err = ts
        .try_push(SourceChange::Add(bad), &|_c, _sc| {}, false)
        .unwrap_err();

    assert!(matches!(
        err,
        RindleError::Sqlite {
            op: "run insert",
            ..
        }
    ));
    assert!(
        ts.take_fetch_error().is_none(),
        "write errors return directly instead of using the fetch-error side channel"
    );
}

#[test]
fn push_edit_same_pk_uses_update() {
    let (conn, ts) = issues();
    seed(&conn, &[(1, Some(10), "a")]);
    let c = ts.connect(Some(vec![(ID, true)]), None, vec![]);
    wire(&ts, c);
    ts.push(
        SourceChange::Edit {
            row: issue(1, Some(20), "a2"),
            old: issue(1, Some(10), "a"),
        },
        &|_c, _sc| {},
    );
    assert_eq!(table_dump(&conn), vec![(1, Some(20), "a2".to_string())]);
}

#[test]
fn push_edit_changed_pk_uses_delete_insert() {
    let (conn, ts) = issues();
    seed(&conn, &[(1, Some(10), "a")]);
    let c = ts.connect(Some(vec![(ID, true)]), None, vec![]);
    wire(&ts, c);
    ts.push(
        SourceChange::Edit {
            row: issue(2, Some(10), "a"),
            old: issue(1, Some(10), "a"),
        },
        &|_c, _sc| {},
    );
    // PK changed 1 -> 2: old deleted, new inserted.
    assert_eq!(table_dump(&conn), vec![(2, Some(10), "a".to_string())]);
}

#[test]
fn all_columns_pk_has_no_update_statement() {
    // A table whose only column is the PK -> update is None -> edit is delete+insert.
    let conn = Rc::new(Connection::open_in_memory().unwrap());
    conn.execute_batch(
        "CREATE TABLE kv (k INTEGER NOT NULL);
         CREATE UNIQUE INDEX kv_pk ON kv (k);
         INSERT INTO kv VALUES (1);",
    )
    .unwrap();
    let cols = vec![ColumnDef {
        name: "k".into(),
        ty: ValueType::Number,
        optional: false,
    }];
    let ts = TableSource::new(conn.clone(), "kv", cols, vec![0]);
    let c = ts.connect(Some(vec![(0, true)]), None, vec![]);
    wire(&ts, c);
    ts.push(
        SourceChange::Edit {
            row: orow(vec![n(2)]),
            old: orow(vec![n(1)]),
        },
        &|_c, _sc| {},
    );
    let ids: Vec<i64> = {
        let mut s = conn.prepare("SELECT k FROM kv ORDER BY k").unwrap();
        s.query_map([], |r| r.get::<_, i64>(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
    };
    assert_eq!(ids, vec![2], "delete 1 + insert 2");
}

// =========================================================================
// get_row by an arbitrary unique key (§3.12)
// =========================================================================

#[test]
fn get_row_by_pk_and_by_other_unique_key() {
    let conn = Rc::new(Connection::open_in_memory().unwrap());
    conn.execute_batch(
        "CREATE TABLE u (id INTEGER NOT NULL, email TEXT NOT NULL, name TEXT);
         CREATE UNIQUE INDEX u_pk ON u (id);
         CREATE UNIQUE INDEX u_email ON u (email);
         INSERT INTO u VALUES (1, 'a@x', 'Ann');",
    )
    .unwrap();
    let cols = vec![
        ColumnDef {
            name: "id".into(),
            ty: ValueType::Number,
            optional: false,
        },
        ColumnDef {
            name: "email".into(),
            ty: ValueType::String,
            optional: false,
        },
        ColumnDef {
            name: "name".into(),
            ty: ValueType::String,
            optional: true,
        },
    ];
    let ts = TableSource::new(conn.clone(), "u", cols, vec![0]);

    let by_pk = ts.get_row(&[(0, n(1))]).expect("by pk");
    assert!(matches!(by_pk.col(2), Value::Str(b"Ann")));

    let by_email = ts
        .get_row(&[(1, OwnedValue::str("a@x"))])
        .expect("by email (a non-PK unique key)");
    assert_eq!(id_of(&by_email), 1);

    assert!(ts.get_row(&[(1, OwnedValue::str("missing@x"))]).is_none());

    // The PK is a discovered unique index.
    assert!(ts.unique_indexes().iter().any(|ix| ix == &vec![0usize]));
}

// =========================================================================
// Overlay + epoch gate + self-join reentrancy (§3.5, §3.10)
// =========================================================================

#[test]
fn reentrant_fetch_during_push_sees_overlay_then_no_busy_cursor() {
    let (conn, ts) = issues();
    seed(&conn, &[(1, Some(1), "a"), (3, Some(1), "c")]);
    let c = ts.connect(Some(vec![(ID, true)]), None, vec![]);
    wire(&ts, c);

    // During the Add(2) push, a reentrant fetch of the same connection observes the
    // overlay (its epoch was bumped before its own push) — even though the row is
    // NOT yet written (write happens after the drain).
    let seen_mid_push: RefCell<Vec<i64>> = RefCell::new(Vec::new());
    ts.push(SourceChange::Add(issue(2, Some(1), "b")), &|_c, _sc| {
        let ids = fetch_ids(&ts, c, &FetchRequest::all());
        *seen_mid_push.borrow_mut() = ids;
    });

    assert_eq!(
        seen_mid_push.into_inner(),
        vec![1, 2, 3],
        "the overlay add spliced in at its sorted position, before it was written"
    );
    // And after the push, the row is committed and no cursor leaked.
    assert_eq!(fetch_ids(&ts, c, &FetchRequest::all()), vec![1, 2, 3]);
    assert_eq!(
        ts.cursors_open(),
        0,
        "no busy cursor after the reentrant fetch"
    );
}

// =========================================================================
// Two live cursors on one connection, stepped interleaved (the self-join shape
// the unsafe OwnedSqliteRows must survive — §3.10). A second `prepare_cached` of
// the same SQL while the first statement is checked out prepares a FRESH one, so
// the two step independently with no aliasing.
// =========================================================================

#[test]
fn two_live_cursors_step_interleaved_and_release() {
    let (conn, ts) = issues();
    seed(
        &conn,
        &[(1, Some(1), "a"), (2, Some(1), "b"), (3, Some(1), "c")],
    );
    let a = ts.connect(Some(vec![(ID, true)]), None, vec![]);
    let b = ts.connect(Some(vec![(ID, true)]), None, vec![]); // a second connection

    let mut sa = ts.fetch(a, &FetchRequest::all());
    let mut sb = ts.fetch(b, &FetchRequest::all());
    assert_eq!(ts.cursors_open(), 2, "two live cursors on one connection");

    // Pull them alternately; identical SQL, two independent statements.
    let mut order = Vec::new();
    loop {
        let x = sa.next().map(|r| id_of(&r));
        let y = sb.next().map(|r| id_of(&r));
        if x.is_none() && y.is_none() {
            break;
        }
        order.extend(x);
        order.extend(y);
    }
    assert_eq!(
        order,
        vec![1, 1, 2, 2, 3, 3],
        "neither cursor corrupted the other"
    );

    drop(sa);
    drop(sb);
    assert_eq!(ts.cursors_open(), 0, "both cursors released");
}

// =========================================================================
// RAII: early-break releases the cursor (Primitive #2, §3.10)
// =========================================================================

#[test]
fn early_break_releases_cursor() {
    let (conn, ts) = issues();
    seed(
        &conn,
        &[
            (1, Some(1), "a"),
            (2, Some(1), "b"),
            (3, Some(1), "c"),
            (4, Some(1), "d"),
        ],
    );
    let c = ts.connect(Some(vec![(ID, true)]), None, vec![]);

    {
        let mut stream = ts.fetch(c, &FetchRequest::all());
        assert_eq!(ts.cursors_open(), 1, "cursor open during scan");
        // Pull only the first row, then drop the stream (a Take-style early break).
        let first = stream.next().map(|r| id_of(&r));
        assert_eq!(first, Some(1));
        assert_eq!(ts.cursors_open(), 1, "still open mid-scan");
    } // stream drops here -> cursor + statement released

    assert_eq!(
        ts.cursors_open(),
        0,
        "RAII released the statement on early break"
    );
    // The connection is free for a write (no "database is busy").
    wire(&ts, c);
    ts.push(SourceChange::Add(issue(9, Some(1), "i")), &|_c, _sc| {});
    assert_eq!(table_dump(&conn).len(), 5);
}

// =========================================================================
// Edit-split polarity (§3.6) + SQL filter pushdown
// =========================================================================

fn split_kinds(old_owner: Option<i64>, new_owner: Option<i64>) -> Vec<&'static str> {
    let (conn, ts) = issues();
    // Seed the OLD row so the EDIT existence assert holds.
    seed(&conn, &[(1, old_owner, "x")]);
    let c = ts.connect(Some(vec![(ID, true)]), None, vec![OWNER]); // split on owner
    wire(&ts, c);
    let rec = RefCell::new(Vec::new());
    ts.push(
        SourceChange::Edit {
            row: issue(1, new_owner, "x"),
            old: issue(1, old_owner, "x"),
        },
        &|_c, sc: SourceChange| {
            rec.borrow_mut().push(match sc {
                SourceChange::Add(_) => "add",
                SourceChange::Remove(_) => "remove",
                SourceChange::Edit { .. } => "edit",
            });
        },
    );
    rec.into_inner()
}

#[test]
fn edit_split_polarity() {
    // Equal non-null split key -> no split (one Edit flows).
    assert_eq!(split_kinds(Some(10), Some(10)), vec!["edit"]);
    // null -> null on a split key DOES split (`!values_equal(null, null)` is true).
    assert_eq!(split_kinds(None, None), vec!["remove", "add"]);
    // Changed value -> split.
    assert_eq!(split_kinds(Some(10), Some(20)), vec!["remove", "add"]);
}

#[test]
fn sql_filter_pushdown_filters_committed_predicate_narrows_overlay() {
    let (conn, ts) = issues();
    seed(
        &conn,
        &[(1, Some(10), "a"), (2, Some(20), "b"), (3, Some(10), "c")],
    );

    // Filter owner == 10: lowered to SQL (committed rows) AND a predicate (overlay).
    let predicate: RowPredicate = Rc::new(|row: &OwnedRow| match row.col(OWNER) {
        Value::Float(f) => f as i64 == 10,
        _ => false,
    });
    let filters = ConnectionFilters {
        predicate,
        pk_constraint: None,
        fully_applied: true,
        sql_condition: Some(SqlCondition::Simple {
            left: Operand::Column(OWNER),
            op: SqlOp::Eq,
            right: Operand::Literal(n(10)),
        }),
        // Hand-built filter (not via the builder); leave it in the always-visited
        // scan list so this test exercises only the SQL-committed-row filtering.
        push_guard: None,
    };
    let c = ts.connect(Some(vec![(ID, true)]), Some(filters), vec![]);
    wire(&ts, c);

    // Committed rows: only owner==10 (ids 1, 3) — SQL filtered out id 2. The
    // predicate never ran on a committed row; SQL did the work (table-source.ts:297
    // — no committed-row `generate_with_filter`, unlike the memory leaf).
    assert_eq!(fetch_ids(&ts, c, &FetchRequest::all()), vec![1, 3]);

    // During an Add(owner==10) push (which passes the predicate, so `push_one`
    // fires), the reentrant fetch splices the overlay add in — even though it is
    // not yet committed — sorted into the SQL-filtered committed rows.
    let seen: RefCell<Vec<i64>> = RefCell::new(Vec::new());
    ts.push(SourceChange::Add(issue(4, Some(10), "d")), &|_c, _sc| {
        *seen.borrow_mut() = fetch_ids(&ts, c, &FetchRequest::all());
    });
    assert_eq!(
        seen.into_inner(),
        vec![1, 3, 4],
        "the matching overlay add spliced into the SQL-filtered committed rows"
    );
}

// =========================================================================
// The declared exact-i64 (`int64`) column plane (design 226 §4.2, Stage C2)
// =========================================================================

/// big(id: number PK, big: int64?) — the `big` column is the declared exact plane.
fn int64_db(rows_sql: &str) -> (Rc<Connection>, TableSource) {
    let conn = Rc::new(Connection::open_in_memory().unwrap());
    conn.execute_batch(&format!(
        "CREATE TABLE big (id INTEGER NOT NULL, big INTEGER);
         CREATE UNIQUE INDEX big_pk ON big (id);
         {rows_sql}"
    ))
    .unwrap();
    let cols = vec![
        ColumnDef {
            name: "id".into(),
            ty: ValueType::Number,
            optional: false,
        },
        ColumnDef {
            name: "big".into(),
            ty: ValueType::Int,
            optional: true,
        },
    ];
    let ts = TableSource::new(conn.clone(), "big", cols, vec![0]);
    (conn, ts)
}

#[test]
fn int64_cells_hydrate_exactly_with_no_roundtrip_bound() {
    // The exact values a `number` column MUST refuse (2^53+1 does not round-trip;
    // i64::MAX/MIN saturate) hydrate bit-exactly from an `int64` column — scan and
    // point-lookup alike — and NULL stays NULL.
    let (_conn, ts) = int64_db(
        "INSERT INTO big VALUES (1, 9007199254740993);
         INSERT INTO big VALUES (2, 9223372036854775807);
         INSERT INTO big VALUES (3, -9223372036854775808);
         INSERT INTO big VALUES (4, NULL);",
    );

    let c = ts.connect(Some(vec![(0, true)]), None, vec![]);
    let rows: Vec<OwnedRow> = ts.fetch(c, &FetchRequest::all()).collect();
    assert!(ts.take_fetch_error().is_none(), "no value error on scan");
    let bigs: Vec<Value> = rows.iter().map(|r| r.col(1)).collect();
    assert!(matches!(bigs[0], Value::Int(9_007_199_254_740_993)));
    assert!(matches!(bigs[1], Value::Int(i64::MAX)));
    assert!(matches!(bigs[2], Value::Int(i64::MIN)));
    assert!(matches!(bigs[3], Value::Null));

    let r = ts.get_row(&[(0, n(2))]).expect("row 2");
    assert!(
        matches!(r.col(1), Value::Int(i64::MAX)),
        "get_row is exact too"
    );
}

#[test]
fn non_integer_storage_in_an_int64_column_parks_unsupported_value() {
    // SQLite INTEGER affinity happily stores REAL/TEXT under a BIGINT decltype;
    // the int64 contract (NULL-or-INTEGER) refuses them at the read boundary —
    // a parked UnsupportedValue, never the generic mismatch pass-through.
    let (_conn, ts) = int64_db(
        "INSERT INTO big VALUES (1, 7);
         INSERT INTO big VALUES (2, 2.5);", // REAL storage class survives INTEGER affinity
    );

    let c = ts.connect(Some(vec![(0, true)]), None, vec![]);
    let ids = fetch_ids(&ts, c, &FetchRequest::all());
    assert_eq!(ids, vec![1], "the INTEGER row vended before the error");
    assert!(
        matches!(
            ts.take_fetch_error(),
            Some(RindleError::UnsupportedValue(_))
        ),
        "REAL in an int64 column parked an UnsupportedValue"
    );

    // TEXT storage class in the int64 position: same refusal, on the point path.
    let (_conn2, ts2) = int64_db("INSERT INTO big VALUES (1, 'not-a-number');");
    assert!(
        matches!(
            ts2.try_get_row(&[(0, n(1))]),
            Err(RindleError::UnsupportedValue(_))
        ),
        "TEXT in an int64 column errors on get_row"
    );
}
