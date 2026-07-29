//! SQLite test-support for the shared `rindle` testkit fixtures.
//!
//! The core `rindle::testkit` runners are backend-agnostic: the `*_with_backend`
//! variants take a [`SourceBackend`] that seeds each fixture into some leaf. This
//! module provides [`sqlite_backend`] — a [`SourceBackend::Custom`] that seeds a real
//! in-memory `TableSource` — so the SQLite leaf can be cross-checked against the
//! memory leaf using the exact same fixtures. Gated behind this crate's `testkit`
//! feature (which enables `rindle/testkit`).

use std::cell::Cell;
use std::rc::Rc;

use rindle::change::{FetchRequest, OutEdge, RowFlow};
use rindle::error::RindleError;
use rindle::graph::{ConnId, Source};
use rindle::source_common::{Connection as Conn, ConnectionFilters};
use rindle::testkit::SourceBackend;
use rindle::value::{ColId, Schema, Sort, Value};
use rindle::{Graph, MemorySource, NodeId, OwnedRow, SourceChange, SourceSchema, ValueType};

use crate::query_builder::{ident, ColumnDef};
use crate::table_source::TableSource;

#[cfg(feature = "scan-stats")]
pub use crate::table_source::{ScanSink, ScanStats};

/// A SQLite [`SourceBackend`] for the shared testkit fixtures: each source is seeded
/// into a real in-memory `TableSource`. Pass to the `*_with_backend` runners to
/// cross-check the SQLite leaf against the memory leaf.
pub fn sqlite_backend() -> SourceBackend {
    SourceBackend::Custom(Rc::new(add_sqlite_test_source))
}

/// Like [`sqlite_backend`] but attaches the shared `sink` to **every** `TableSource`
/// it builds, so a single read of the sink after a run yields the query's total SQLite
/// work (`sqlite3_stmt_status`). The sink is attached *after* seeding, so only
/// post-build fetches (hydrate + EXISTS probes) are counted — not the seed writes.
/// `scan-stats` only; used by `tests/plan_quality.rs`.
#[cfg(feature = "scan-stats")]
pub fn sqlite_backend_with_scan_sink(sink: ScanSink) -> SourceBackend {
    SourceBackend::Custom(Rc::new(move |g, table_name, schema, rows, extra_rows| {
        let source = build_sqlite_source(table_name, schema, rows, extra_rows);
        source.set_scan_sink(sink.clone());
        g.add_dyn_source(Box::new(source))
    }))
}

/// A shared row-pull counter for [`CountingSource`]. Cloned into the wrapped leaf's
/// `fetch` streams; read after a run to see how many leaf rows the pipeline drew.
pub type PullCounter = Rc<Cell<usize>>;

/// A [`Source`] decorator that counts how many rows the pipeline **pulls** from the
/// wrapped leaf's `fetch` stream. Because every backend's `fetch` returns a *lazy*
/// [`RowFlow`], the count reflects exactly how far a consumer drew the cursor: a
/// `LIMIT N` hydrate that short-circuits once the window is full pulls ~`N` rows,
/// whereas a non-lazy graph would drain the whole table first. Backend-agnostic —
/// it wraps both the in-memory `MemorySource` and the SQLite `TableSource`, so the
/// same laziness assertion runs over both leaves (see `tests/dataflow_laziness.rs`).
pub struct CountingSource<S: Source> {
    inner: S,
    pulled: PullCounter,
}

impl<S: Source> CountingSource<S> {
    pub fn new(inner: S, pulled: PullCounter) -> CountingSource<S> {
        CountingSource { inner, pulled }
    }
}

impl<S: Source> Source for CountingSource<S> {
    fn connect(
        &self,
        sort: Option<Sort>,
        filters: Option<ConnectionFilters>,
        split_edit_keys: Vec<ColId>,
    ) -> ConnId {
        self.inner.connect(sort, filters, split_edit_keys)
    }

    fn fetch<'g>(&'g self, conn: ConnId, req: &FetchRequest) -> RowFlow<'g> {
        let counter = self.pulled.clone();
        Box::new(
            self.inner
                .fetch(conn, req)
                .inspect(move |_row| counter.set(counter.get() + 1)),
        )
    }

    fn conn_sort(&self, conn: ConnId) -> Sort {
        self.inner.conn_sort(conn)
    }

    fn schema(&self) -> &Schema {
        self.inner.schema()
    }

    fn destroy(&self, conn: ConnId) {
        self.inner.destroy(conn)
    }

    fn cursors_open(&self) -> i64 {
        self.inner.cursors_open()
    }

    fn try_push(
        &self,
        change: SourceChange,
        push_one: &dyn Fn(&Conn, SourceChange),
        strict: bool,
    ) -> Result<(), RindleError> {
        self.inner.try_push(change, push_one, strict)
    }

    fn take_error(&self) -> Option<RindleError> {
        self.inner.take_error()
    }

    fn set_conn_output(&self, conn: ConnId, edge: OutEdge) {
        self.inner.set_conn_output(conn, edge)
    }
}

/// A **memory** [`SourceBackend`] whose leaf rows are counted as the pipeline pulls
/// them ([`CountingSource`] over a `MemorySource`). Read `pulled` after a run to
/// assert laziness — e.g. that a `LIMIT` hydrate did not exhaust the leaf.
pub fn counting_memory_backend(pulled: PullCounter) -> SourceBackend {
    counting_memory_backend_for(None, pulled)
}

/// A **SQLite** [`SourceBackend`] whose leaf rows are counted as the pipeline pulls
/// them ([`CountingSource`] over a real in-memory `TableSource`). The cross-backend
/// twin of [`counting_memory_backend`], so one laziness test runs over both leaves.
pub fn counting_sqlite_backend(pulled: PullCounter) -> SourceBackend {
    counting_sqlite_backend_for(None, pulled)
}

/// Like [`counting_memory_backend`] but, when `table` is `Some`, only the named
/// table's pulls are counted (other tables use a plain leaf). Lets a subquery /
/// EXISTS test isolate the **child** leaf's pulls from the parent's.
pub fn counting_memory_backend_for(table: Option<&str>, pulled: PullCounter) -> SourceBackend {
    let table = table.map(str::to_owned);
    SourceBackend::Custom(Rc::new(move |g, table_name, schema, rows, _extra_rows| {
        if counts(&table, &table_name) {
            let source = MemorySource::new(schema.into_schema(), rows);
            g.add_dyn_source(Box::new(CountingSource::new(source, pulled.clone())))
        } else {
            g.add_source(schema, rows)
        }
    }))
}

/// Like [`counting_sqlite_backend`] but counts only the named `table` (see
/// [`counting_memory_backend_for`]).
pub fn counting_sqlite_backend_for(table: Option<&str>, pulled: PullCounter) -> SourceBackend {
    let table = table.map(str::to_owned);
    SourceBackend::Custom(Rc::new(move |g, table_name, schema, rows, extra_rows| {
        let source = build_sqlite_source(table_name.clone(), schema, rows, extra_rows);
        if counts(&table, &table_name) {
            g.add_dyn_source(Box::new(CountingSource::new(source, pulled.clone())))
        } else {
            g.add_dyn_source(Box::new(source))
        }
    }))
}

/// Whether `table_name` should be counted: an unset filter counts every table.
fn counts(filter: &Option<String>, table_name: &str) -> bool {
    match filter {
        None => true,
        Some(t) => t == table_name,
    }
}

/// Build + seed an in-memory `TableSource` (the leaf the SQLite backends share). The
/// caller decides whether to attach a scan sink and add it to a graph.
fn build_sqlite_source(
    table_name: String,
    schema: SourceSchema,
    rows: Vec<OwnedRow>,
    extra_rows: &[OwnedRow],
) -> TableSource {
    let conn = Rc::new(rusqlite::Connection::open_in_memory().expect("open sqlite test db"));
    let columns = infer_sqlite_columns(&schema, &rows, extra_rows);
    create_sqlite_table(&conn, &table_name, &columns, &schema.primary_key);
    let source = TableSource::new_with_schema(
        conn,
        &table_name,
        columns,
        schema.primary_key.clone(),
        schema.clone(),
    );
    for row in rows {
        source.push(SourceChange::Add(row), &|_, _| {});
    }
    source
}

fn add_sqlite_test_source(
    g: &mut Graph,
    table_name: String,
    schema: SourceSchema,
    rows: Vec<OwnedRow>,
    extra_rows: &[OwnedRow],
) -> NodeId {
    g.add_dyn_source(Box::new(build_sqlite_source(
        table_name, schema, rows, extra_rows,
    )))
}

/// Materialize a set of [`TableSpec`](rindle::testkit::TableSpec) fixtures into **one**
/// ANALYZE'd in-memory connection for the planner's [`SqliteCostModel`](crate::SqliteCostModel)
/// to estimate against. The physical layout (a UNIQUE index on each PK, no secondary
/// indexes) matches the per-table DBs [`sqlite_backend_with_scan_sink`] runs the query
/// against, so the cost model's prediction and the measured execution see the same
/// indexes. `scan-stats` only.
#[cfg(feature = "scan-stats")]
pub fn cost_model_db(specs: &[rindle::testkit::TableSpec]) -> Rc<rusqlite::Connection> {
    let conn = Rc::new(rusqlite::Connection::open_in_memory().expect("open cost-model db"));
    for s in specs {
        let columns = infer_sqlite_columns(&s.schema, &s.rows, &[]);
        create_sqlite_table(&conn, &s.name, &columns, &s.schema.primary_key);
        // Seed via a `TableSource` on the shared conn (the tested write path); the
        // source is dropped at end of iteration, the rows persist in `conn`.
        let source = TableSource::new_with_schema(
            conn.clone(),
            &s.name,
            columns,
            s.schema.primary_key.clone(),
            s.schema.clone(),
        );
        for row in &s.rows {
            source.push(SourceChange::Add(row.clone()), &|_, _| {});
        }
    }
    conn.execute_batch("ANALYZE;")
        .expect("ANALYZE cost-model db");
    conn
}

fn infer_sqlite_columns(
    schema: &SourceSchema,
    rows: &[OwnedRow],
    extra_rows: &[OwnedRow],
) -> Vec<ColumnDef> {
    schema
        .columns
        .iter()
        .enumerate()
        .map(|(col, name)| {
            let mut ty = ValueType::Null;
            let mut optional = false;
            for row in rows.iter().chain(extra_rows.iter()) {
                match row.col(col) {
                    Value::Absent | Value::Null => optional = true,
                    Value::Bool(_) => ty = merge_type(ty, ValueType::Boolean),
                    Value::Int(_) => {}
                    Value::Float(_) => ty = merge_type(ty, ValueType::Number),
                    Value::Str(_) => ty = merge_type(ty, ValueType::String),
                    Value::Json(_) => ty = merge_type(ty, ValueType::Json),
                }
            }
            ColumnDef {
                name: name.as_ref().into(),
                ty,
                optional,
            }
        })
        .collect()
}

fn merge_type(current: ValueType, incoming: ValueType) -> ValueType {
    match (current, incoming) {
        // Value inference never produces `Int`: `infer_sqlite_columns` skips
        // `Value::Int` cells entirely (the blank-affinity trick below), and a
        // declared int64 fixture supplies its `ColumnDef`s directly, bypassing
        // inference. Loud here if that assumption ever changes.
        (ValueType::Int, _) | (_, ValueType::Int) => {
            unreachable!("value inference never produces ValueType::Int")
        }
        (ValueType::Null, ty) => ty,
        (ValueType::Number, _) | (_, ValueType::Number) => ValueType::Number,
        (ValueType::String, _) | (_, ValueType::String) => ValueType::String,
        (ValueType::Json, _) | (_, ValueType::Json) => ValueType::Json,
        (ValueType::Boolean, ValueType::Boolean) => ValueType::Boolean,
        (ty, ValueType::Null) => ty,
    }
}

fn create_sqlite_table(
    conn: &rusqlite::Connection,
    table_name: &str,
    columns: &[ColumnDef],
    primary_key: &[usize],
) {
    assert!(
        !primary_key.is_empty(),
        "sqlite test source requires a non-empty primary key"
    );
    let column_sql = columns
        .iter()
        .map(|c| match sqlite_affinity(c.ty) {
            "" => ident(&c.name),
            affinity => format!("{} {affinity}", ident(&c.name)),
        })
        .collect::<Vec<_>>()
        .join(", ");
    conn.execute_batch(&format!(
        "CREATE TABLE {} ({column_sql});",
        ident(table_name)
    ))
    .expect("create sqlite test source table");

    let pk_cols = primary_key
        .iter()
        .map(|&c| ident(&columns[c].name))
        .collect::<Vec<_>>()
        .join(", ");
    let index_name = format!("__zql_{}_pk", table_name.replace('"', "\"\""));
    conn.execute_batch(&format!(
        "CREATE UNIQUE INDEX {} ON {} ({pk_cols});",
        ident(&index_name),
        ident(table_name)
    ))
    .expect("create sqlite test source pk index");
}

fn sqlite_affinity(ty: ValueType) -> &'static str {
    match ty {
        ValueType::Boolean => "INTEGER",
        ValueType::Number => "REAL",
        ValueType::String | ValueType::Json => "TEXT",
        // A DECLARED exact-i64 column (design 226) is INTEGER affinity, matching
        // the production `BIGINT` shape. (Unreachable via inference — see
        // `merge_type` — but a fixture may declare it.)
        ValueType::Int => "INTEGER",
        // Test fixtures commonly use `Int` for JS-number-ish ids. Leaving the
        // column affinity blank preserves SQLite's INTEGER storage class, so the
        // test TableSource vends the same `OwnedValue::Int` rows as MemorySource.
        ValueType::Null => "",
    }
}
