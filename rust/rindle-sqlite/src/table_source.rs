//! The server-side leaf [`Source`]: a `Source` backed by a SQLite table (the
//! server-side replica of Postgres). Production port of
//! `packages/zqlite/src/table-source.ts` (`class TableSource`), the peer of the
//! client-side [`MemorySource`](rindle::memory_source::MemorySource). It owns three things
//! (`05` §1.1):
//!
//! 1. **The leaf scan** — a `FetchRequest` is compiled to a parameterized
//!    `SELECT` ([`build_select_query`]) and executed against `rusqlite`, vending
//!    rows **lazily and zero-copy**: `Str`/`Json` values borrow directly from the
//!    `sqlite3_column_text` buffers (valid only until the next step), and a row is
//!    materialized exactly once — at the connection boundary — via the borrow
//!    checker-forced [`RowRef::to_owned_row`]. The whole conn→stmt→cursor borrow
//!    chain is owned by one `OwnedSqliteRows` so the lazy stream can be boxed
//!    and returned (the `05` §13 Q1 self-reference, solved with one contained,
//!    drop-ordered `unsafe`).
//! 2. **Connections + the overlay/push/edit-split machinery** — identical to the
//!    memory leaf: it reuses the backend-agnostic [`source_common`](rindle::source_common) seam verbatim
//!    (`generate_with_overlay_checked`, [`generate_with_start`],
//!    `try_gen_push_and_write_with_split_edit`). The leaf only differs in the scan.
//! 3. **The write path** — `INSERT`/`DELETE`/`UPDATE`, committed **after** the
//!    vend (the self-join correctness invariant, §3.11).
//!
//! **A source emits rows, not nodes** (`04`/`05` §1.1): `fetch` returns a
//! [`RowFlow`]; the *connection boundary* (`Graph::fetch` on a `SourceConn`) wraps
//! each row in a leaf node. The Node concept lives entirely above this module —
//! exactly as for [`MemorySource`](rindle::memory_source::MemorySource).
//!
//! Build target: `feature = "sqlite"` only.

use std::cell::{Cell, RefCell};
use std::cmp::Ordering;
use std::rc::Rc;

use rusqlite::types::ValueRef;
use rusqlite::{params_from_iter, CachedStatement, Connection, Rows};

use crate::query_builder::{build_select_query, ident, to_sqlite_param, ColumnDef, SqliteParam};
use crate::sqlite::{is_exact_f64_integer, ColType, SqliteError, SqliteRow};
use crate::tiebreak::TiebreakStream;
use rindle::change::{Basis, FetchRequest, OutEdge, RowFlow, SourceChange, Start};
use rindle::error::RindleError;
use rindle::graph::{ConnId, Source};
use rindle::source_common::{
    generate_with_overlay_checked, generate_with_overlay_unordered_checked, generate_with_start,
    try_gen_push_and_write_with_split_edit, ConnTable, Connection as Conn, ConnectionFilters,
    Overlay,
};
use rindle::value::{
    compare_values, ColId, OwnedRow as Row, OwnedValue, RowRef, Schema, Sort, SourceSchema,
    ValueType,
};

/// SQLite's own per-statement work counters (`sqlite3_stmt_status`), accumulated
/// across every leaf `SELECT` a query runs. This is how the `plan_quality` test
/// measures how much work SQLite actually did for a planned query vs its unplanned
/// variant — the Rust analogue of the JS planner's "scanned rows" tracking.
///
/// Surfaced **only** under the `scan-stats` feature, so no instrumentation ships in
/// production. Read off the executed statement at the cursor's drop via
/// [`rusqlite::Statement::reset_status`], so early-termination (an EXISTS limit-1
/// probe that stops before the end) is honoured: the counters reflect the steps
/// SQLite actually took.
#[cfg(feature = "scan-stats")]
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub struct ScanStats {
    /// `SQLITE_STMTSTATUS_FULLSCAN_STEP`: rows visited by full (un-indexed) table
    /// scans — precisely the work a good plan avoids.
    pub fullscan_steps: u64,
    /// `SQLITE_STMTSTATUS_SORT`: number of sort operations performed.
    pub sorts: u64,
    /// `SQLITE_STMTSTATUS_VM_STEP`: total VM operations executed — the best single
    /// proxy for "how much work SQLite did" across the whole query.
    pub vm_steps: u64,
    /// `SQLITE_STMTSTATUS_RUN`: number of times a leaf statement was run (≈ the number
    /// of fetch/probe calls the dataflow issued).
    pub runs: u64,
}

/// A shared accumulator for [`ScanStats`]. One sink is attached to every
/// [`TableSource`] in a graph (via the test backend) so a single read yields the
/// query's total SQLite work. `scan-stats` only.
#[cfg(feature = "scan-stats")]
pub type ScanSink = Rc<Cell<ScanStats>>;

/// A shared slot for the **representative fetch SQL** a source ran — the exact leaf
/// `SELECT` (`?`-parameterized) that `analyze query` re-`EXPLAIN`s to report the chosen
/// access path per leaf. First-write-wins: a parent-driven scan runs the same SQL text
/// N times (only the bound values differ), so one capture is enough. `scan-stats` only;
/// never populated in production (no sink attached).
#[cfg(feature = "scan-stats")]
pub type PlanSink = Rc<RefCell<Option<String>>>;

/// Map a logical [`ValueType`] to the leaf's storage-class tag (`05` §4.7). A
/// `number` column is `Float` (it widens INTEGER→f64 after an exact round-trip
/// check at the boundary); a `null`-typed column
/// is treated as `Text` (any storage class passes through the leaf's fallback arms).
fn col_type_of(ty: ValueType) -> ColType {
    match ty {
        ValueType::Boolean => ColType::Bool,
        ValueType::Number => ColType::Float,
        // The declared exact-i64 plane (design 226 §4.2): INTEGER cells read as
        // exact `Value::Int` (the long-dormant `sqlite.rs` read arm); the scan
        // guard beside `check_number_bounds` enforces NULL-or-INTEGER storage.
        ValueType::Int => ColType::Int,
        ValueType::String => ColType::Text,
        ValueType::Json => ColType::Json,
        ValueType::Null => ColType::Text,
    }
}

/// Immutable, shared table metadata (one allocation, `Rc`-shared between the
/// source and every in-flight cursor so a `fetch` clones one `Rc` instead of
/// re-deriving names/types/SQL). The mutable source state (db handle, connections,
/// overlay, epoch) lives on [`TableSource`] directly.
struct TableMeta {
    table: Box<str>,
    columns: Vec<ColumnDef>,
    /// Per-column storage tag, parallel to `columns`; lent to each [`SqliteRow`].
    col_types: Vec<ColType>,
    /// The `Float`-tagged column indices — the only columns the per-row lossless-f64
    /// check ([`check_number_bounds`]) applies to, precomputed so the hot loop
    /// doesn't re-walk (and re-`get_ref`) every column on every row.
    float_cols: Vec<ColId>,
    /// The `Int`-tagged (declared `int64`, design 226 §4.2) column indices — the
    /// columns the per-row storage-class check ([`check_int_storage`]) applies to:
    /// a cell must be NULL or INTEGER, never REAL/TEXT/BLOB.
    int_cols: Vec<ColId>,
    primary_key: Vec<ColId>,
    /// The non-PK columns, in declared order — the `UPDATE … SET` list (§3.11).
    non_pk: Vec<ColId>,
    /// Reported to downstream operators (`Source::schema`). Per-connection sort is
    /// on each [`Conn`]; this carries the table's primary-index sort.
    schema: Schema,
    // -- write statements, precomputed once (text only; prepared via the
    //    per-connection statement cache on use, `05` §4.3) --
    insert_sql: String,
    delete_sql: String,
    /// `None` iff every column is part of the PK (no non-PK column to SET, §3.11).
    update_sql: Option<String>,
    check_exists_sql: String,
    // -- in-engine sort tiebreak metadata (design 204) --
    /// True when the PK is a single `INTEGER PRIMARY KEY` (a rowid alias): an `(ord)`
    /// index is then physically `(ord, rowid)` = `(ord, pk)`, so SQL already serves
    /// the appended-PK order and the tiebreak adapter bypasses.
    pk_is_rowid_alias: bool,
    /// True for a `WITHOUT ROWID` table: secondary indexes carry the PK tail, so an
    /// `(ord)` index is physically `(ord, pk)` and the tiebreak adapter bypasses.
    without_rowid: bool,
    /// Every index's `(name, leading resolvable key columns in index order)`. The
    /// structural basis for [`TableMeta::tiebreak_prefix_len`]'s bypass decision —
    /// answers "can SQL stream the prefix / the full `(prefix, pk)` order?" without a
    /// query planner. The name keys the `sqlite_stat1` run-size lookup
    /// ([`TableSource::prefix_run_within_cap`]) that gates a large-run bypass.
    all_index_cols: Vec<(String, Vec<ColId>)>,
}

impl TableMeta {
    /// Whether the in-engine sort tiebreak adapter (design 204) should engage for a
    /// connection ordered by `sort`, and if so how many leading (prefix) columns
    /// SQLite should `ORDER BY`. `None` ⇒ bypass (hand the full sort to SQL).
    ///
    /// Correctness holds on **either** path, so this is a pure performance heuristic
    /// (design §"When to bypass"): engage only when an index can stream the user prefix
    /// but no index serves the full `(prefix, pk)` order.
    fn tiebreak_prefix_len(&self, sort: &Sort) -> Option<usize> {
        let appended = appended_pk_len(sort, &self.primary_key);
        // Nothing to relocate (no appended PK), or a pure-PK order (an empty prefix
        // would collapse the whole table into one buffered run): bypass.
        if appended == 0 || appended >= sort.len() {
            return None;
        }
        let prefix_len = sort.len() - appended;

        // SQL already streams the full `(prefix, pk)` order for these shapes: a rowid
        // alias makes any `(ord)` index physically `(ord, rowid)` = `(ord, pk)`, and a
        // WITHOUT ROWID index carries the PK tail.
        if self.pk_is_rowid_alias || self.without_rowid {
            return None;
        }

        let prefix_cols: Vec<ColId> = sort[..prefix_len].iter().map(|&(c, _)| c).collect();
        let full_cols: Vec<ColId> = sort.iter().map(|&(c, _)| c).collect();

        // A user-declared `(prefix ++ pk)` index already serves the whole order.
        if self
            .all_index_cols
            .iter()
            .any(|(_, ix)| leads_with(ix, &full_cols))
        {
            return None;
        }
        // Engage only if some index can stream the prefix order; otherwise SQL
        // temp-b-trees either way and the adapter would only add buffering.
        if self
            .all_index_cols
            .iter()
            .any(|(_, ix)| leads_with(ix, &prefix_cols))
        {
            Some(prefix_len)
        } else {
            None
        }
    }

    /// The name of an index whose leading columns are exactly `sort[..prefix_len]` —
    /// the one whose `sqlite_stat1` row estimates the equal-prefix run size for the
    /// large-run bypass gate ([`TableSource::prefix_run_within_cap`]). Any such index
    /// gives the same estimate (run size is a property of the data, not the index), so
    /// the first is fine. `tiebreak_prefix_len` returning `Some` guarantees one exists.
    fn prefix_stream_index(&self, sort: &Sort, prefix_len: usize) -> Option<&str> {
        let prefix_cols: Vec<ColId> = sort[..prefix_len].iter().map(|&(c, _)| c).collect();
        self.all_index_cols
            .iter()
            .find(|(_, ix)| leads_with(ix, &prefix_cols))
            .map(|(name, _)| name.as_str())
    }
}

/// Length of the trailing run of ascending PK columns `resolve_sort` appended to
/// `sort` (design 204). A PK column the user placed is indistinguishable from an
/// appended one, so this over-counts when the user explicitly ordered by an ascending
/// PK column at the tail — harmless, since treating it as the tiebreak is exactly what
/// the engine would append anyway (perf-only, never a correctness issue).
fn appended_pk_len(sort: &Sort, pk: &[ColId]) -> usize {
    let mut n = 0;
    for &(col, asc) in sort.iter().rev() {
        if asc && n < pk.len() && pk.contains(&col) {
            n += 1;
        } else {
            break;
        }
    }
    n
}

/// Does index key-column list `ix` begin with exactly `cols` (order-sensitive)? Used
/// to decide, structurally, whether an index can stream a given ordering. Direction is
/// ignored — SQLite can scan an index in reverse — which is safe because the decision
/// is perf-only (design 204).
fn leads_with(ix: &[ColId], cols: &[ColId]) -> bool {
    ix.len() >= cols.len() && ix[..cols.len()] == *cols
}

/// Estimated equal-prefix run size above which the tiebreak adapter bypasses to SQL's
/// own sort (design 204 §"Costs / risks"). A buffered run is bounded to this many
/// [`OwnedRow`](rindle::value::OwnedRow)s, so a run key with fewer than
/// `rows / TIEBREAK_MAX_ENGAGE_RUN` distinct values falls back to the temp b-tree. A
/// heuristic ceiling, not a correctness bound — tune against first-page latency vs.
/// buffering cost.
const TIEBREAK_MAX_ENGAGE_RUN: u64 = 4096;

/// Parse the average number of rows sharing the first `prefix_len` index columns from an
/// `sqlite_stat1.stat` string (design 204). The stat is space-separated integers
/// `"<nRows> <k1> <k2> …"` where `kN` = avg rows sharing the first `N` index columns
/// (mirrors `stat_fanout.rs`'s `parts[depth]`), so the equal-prefix run estimate is
/// `parts[prefix_len]`. `None` if the stat is empty/short/malformed (caller engages).
fn stat1_prefix_run(stat: &str, prefix_len: usize) -> Option<u64> {
    stat.split(' ').nth(prefix_len)?.parse::<u64>().ok()
}

/// `TableSource` — the SQLite-backed leaf source. Mirrors [`MemorySource`](rindle::memory_source::MemorySource)'s shape:
/// the overlay/push/edit-split machinery is shared; only the leaf scan (a real
/// `rusqlite` cursor instead of a COW B+tree) and the write path differ.
pub struct TableSource {
    meta: Rc<TableMeta>,
    /// The active snapshot DB handle (swapped by [`Self::set_db`], §5.7). `Rc` so
    /// each in-flight cursor keeps its own snapshot alive (the SQLite analogue of
    /// the COW snapshot stability the memory leaf gets from `Arc`).
    db: RefCell<Rc<Connection>>,
    conns: ConnTable,
    /// The single in-flight change, epoch-tagged. Read LIVE via a tightly-scoped
    /// borrow during a reentrant fetch, cloned out, cleared after the push drains
    /// BEFORE the write (§3.11, foundations §6.3).
    overlay: RefCell<Option<Overlay>>,
    epoch: Cell<u32>,
    /// Primitive #2: open-cursor count. A fetch stream bumps it; the cursor's
    /// `Drop` (`CursorGuard`) decrements — even on an early `Take` break — so a
    /// test can prove no statement leaked (the "database is busy" guard, §3.10).
    cursors_open: Rc<Cell<i64>>,
    /// Where a cursor parks a value-conversion / step error to be resurfaced at
    /// the owning boundary (`05` §4.5, §13 Q9): `next_row` can only return
    /// `Option`, so the error lands here and the consumer drains then checks
    /// [`Self::take_fetch_error`] — NEVER silently mapped to end-of-stream.
    fetch_error: Rc<RefCell<Option<RindleError>>>,
    /// Unique-key column sets discovered from `pragma_index_list`/`index_info`
    /// (§5.1); used for the PK-UNIQUE construction assert.
    unique_indexes: Vec<Vec<ColId>>,
    /// Optional shared work-counter sink (`scan-stats` only). When set, every cursor
    /// this source vends accumulates its `sqlite3_stmt_status` counters here on drop.
    #[cfg(feature = "scan-stats")]
    scan_sink: RefCell<Option<ScanSink>>,
    /// Optional capture of the representative fetch SQL (`scan-stats` only). When set,
    /// the first leaf `SELECT` this source runs is recorded here so `analyze query` can
    /// re-`EXPLAIN` it for the per-leaf access-path text.
    #[cfg(feature = "scan-stats")]
    plan_sink: RefCell<Option<PlanSink>>,
}

impl TableSource {
    /// Build a source over an existing `table` in `db`. Sets the load-bearing
    /// `PRAGMA case_sensitive_like = ON` (db.ts:45 — makes bare `LIKE`
    /// case-sensitive, the contract the `ILIKE`→`lower()` lowering relies on),
    /// discovers unique indexes, asserts the PK has one (§5.1), and precomputes the
    /// write SQL + the reported `Schema`.
    pub fn new(
        db: Rc<Connection>,
        table: &str,
        columns: Vec<ColumnDef>,
        primary_key: Vec<ColId>,
    ) -> TableSource {
        Self::try_new(db, table, columns, primary_key).expect("create SQLite TableSource")
    }

    pub fn try_new(
        db: Rc<Connection>,
        table: &str,
        columns: Vec<ColumnDef>,
        primary_key: Vec<ColId>,
    ) -> Result<TableSource, RindleError> {
        Self::new_inner(db, table, columns, primary_key, None)
    }

    /// Build a source while preserving the caller's full reported [`Schema`]
    /// (notably relationship slots). The SQLite projection still comes from
    /// `columns` in `ColId` order; `schema` is what downstream operators see.
    pub fn new_with_schema(
        db: Rc<Connection>,
        table: &str,
        columns: Vec<ColumnDef>,
        primary_key: Vec<ColId>,
        schema: SourceSchema,
    ) -> TableSource {
        Self::try_new_with_schema(db, table, columns, primary_key, schema)
            .expect("create SQLite TableSource")
    }

    pub fn try_new_with_schema(
        db: Rc<Connection>,
        table: &str,
        columns: Vec<ColumnDef>,
        primary_key: Vec<ColId>,
        schema: SourceSchema,
    ) -> Result<TableSource, RindleError> {
        Self::new_inner(db, table, columns, primary_key, Some(schema.into_schema()))
    }

    fn new_inner(
        db: Rc<Connection>,
        table: &str,
        columns: Vec<ColumnDef>,
        primary_key: Vec<ColId>,
        reported_schema: Option<Schema>,
    ) -> Result<TableSource, RindleError> {
        db.execute_batch("PRAGMA case_sensitive_like = ON;")
            .map_err(|e| RindleError::sqlite("set case_sensitive_like", e))?;
        // Query Planner Stability Guarantee (see rindle-replica `parallel::open_wal2` for the full
        // story): the IVM leaf seeks are cached (`prepare_cached`) and re-bound with a fresh key on
        // every derive. QPSG makes their plans value-independent so a re-bound seek is never
        // re-parsed — on the bedrock-SQLite build that re-prepare cost ~2x the seek itself. The
        // cluster connections set this in `open_wal2`; stamping it here too covers every *embedded*
        // SQLite source (a caller's own `Connection`), the one guaranteed chokepoint for "an IVM
        // source runs on this connection." A whole-connection dbconfig, idempotent to re-set — so
        // it is harmless when several sources share one connection or the connection already had it.
        db.set_db_config(
            rusqlite::config::DbConfig::SQLITE_DBCONFIG_ENABLE_QPSG,
            true,
        )
        .map_err(|e| RindleError::sqlite("set QPSG", e))?;

        let unique_indexes = discover_unique_indexes(&db, table, &columns, &primary_key)?;
        debug_assert!(
            {
                let mut pk = primary_key.clone();
                pk.sort_unstable();
                unique_indexes.iter().any(|ix| {
                    let mut s = ix.clone();
                    s.sort_unstable();
                    s == pk
                })
            },
            "primary key {primary_key:?} does not have a UNIQUE index in table {table:?}"
        );

        // In-engine sort tiebreak metadata (design 204). All three are structural
        // (schema/index shape), computed once here; the per-fetch decision in
        // `TableMeta::tiebreak_prefix_len` is then allocation-light.
        let pk_is_rowid_alias = is_pk_rowid_alias(&db, table, &columns, &primary_key);
        let without_rowid = db
            .prepare(&format!("SELECT rowid FROM {} LIMIT 0", ident(table)))
            .is_err();
        let all_index_cols = discover_all_index_columns(&db, table, &columns)?;

        let col_types: Vec<ColType> = columns.iter().map(|c| col_type_of(c.ty)).collect();
        let float_cols: Vec<ColId> = col_types
            .iter()
            .enumerate()
            .filter(|(_, ty)| **ty == ColType::Float)
            .map(|(i, _)| i)
            .collect();
        let int_cols: Vec<ColId> = col_types
            .iter()
            .enumerate()
            .filter(|(_, ty)| **ty == ColType::Int)
            .map(|(i, _)| i)
            .collect();
        let non_pk: Vec<ColId> = (0..columns.len())
            .filter(|c| !primary_key.contains(c))
            .collect();

        // The reported schema. Column names are leaked once (the source lives for
        // the program's life); they are never read on the hot path — the IVM is
        // index-addressed — but kept correct for downstream/debug use.
        let leaked_cols: Vec<&'static str> = columns
            .iter()
            .map(|c| {
                let s: &'static str = Box::leak(c.name.clone());
                s
            })
            .collect();
        let primary_index_sort: Sort = primary_key.iter().map(|&c| (c, true)).collect();
        let schema = reported_schema.unwrap_or_else(|| {
            Schema::new(leaked_cols, primary_key.clone(), primary_index_sort)
                .with_column_types(columns.iter().map(|c| c.ty).collect())
        });

        let insert_sql = build_insert_sql(table, &columns);
        let delete_sql = build_delete_sql(table, &primary_key, &columns);
        let update_sql = build_update_sql(table, &primary_key, &non_pk, &columns);
        let check_exists_sql = build_check_exists_sql(table, &primary_key, &columns);

        let meta = TableMeta {
            table: table.into(),
            columns,
            col_types,
            float_cols,
            int_cols,
            primary_key,
            non_pk,
            schema,
            insert_sql,
            delete_sql,
            update_sql,
            check_exists_sql,
            pk_is_rowid_alias,
            without_rowid,
            all_index_cols,
        };

        Ok(TableSource {
            meta: Rc::new(meta),
            db: RefCell::new(db),
            conns: ConnTable::new(),
            overlay: RefCell::new(None),
            epoch: Cell::new(0),
            cursors_open: Rc::new(Cell::new(0)),
            fetch_error: Rc::new(RefCell::new(None)),
            unique_indexes,
            #[cfg(feature = "scan-stats")]
            scan_sink: RefCell::new(None),
            #[cfg(feature = "scan-stats")]
            plan_sink: RefCell::new(None),
        })
    }

    /// Cheaply build a new source handle over the same SQLite snapshot and table
    /// metadata, with fresh connections and overlay state. This is the SQLite
    /// peer of [`MemorySource::fork`](rindle::MemorySource::fork): useful for
    /// building a clean graph/pipeline around an already-seeded backing store
    /// without re-discovering schema metadata or re-inserting rows.
    pub fn fork(&self) -> TableSource {
        TableSource {
            meta: self.meta.clone(),
            db: RefCell::new(self.db.borrow().clone()),
            conns: ConnTable::new(),
            overlay: RefCell::new(None),
            epoch: Cell::new(0),
            cursors_open: Rc::new(Cell::new(0)),
            fetch_error: Rc::new(RefCell::new(None)),
            unique_indexes: self.unique_indexes.clone(),
            #[cfg(feature = "scan-stats")]
            scan_sink: RefCell::new(self.scan_sink.borrow().clone()),
            #[cfg(feature = "scan-stats")]
            plan_sink: RefCell::new(self.plan_sink.borrow().clone()),
        }
    }

    /// Open-cursor count (Primitive #2). `0` ⇒ no cursor is mid-iteration and the
    /// connection is free for a write.
    pub fn cursors_open(&self) -> i64 {
        self.cursors_open.get()
    }

    /// Attach a shared [`ScanSink`]: every leaf `SELECT` this source vends from now on
    /// accumulates its `sqlite3_stmt_status` work counters into `sink` when its cursor
    /// drops. Test-only (the `scan-stats` feature). Attach *after* seeding so the seed
    /// writes are not counted.
    #[cfg(feature = "scan-stats")]
    pub fn set_scan_sink(&self, sink: ScanSink) {
        *self.scan_sink.borrow_mut() = Some(sink);
    }

    /// Attach a shared [`PlanSink`]: the first leaf `SELECT` this source runs from now on
    /// records its (`?`-parameterized) SQL text into `sink`, for `analyze query` to
    /// re-`EXPLAIN`. `scan-stats` only. First-write-wins, so it names the representative
    /// access path even when the query drives the leaf many times.
    #[cfg(feature = "scan-stats")]
    pub fn set_plan_sink(&self, sink: PlanSink) {
        *self.plan_sink.borrow_mut() = Some(sink);
    }

    /// Take the error a cursor parked during the last drained fetch (`05` §4.5):
    /// a lossy integer-to-f64 conversion in a `number` column (`UnsupportedValue`) or a
    /// `sqlite3_step` failure. `None` if the fetch drained cleanly. The consumer
    /// drains the stream, THEN calls this — the error is resurfaced, not swallowed.
    pub fn take_fetch_error(&self) -> Option<RindleError> {
        self.fetch_error.borrow_mut().take()
    }

    /// The large-run bypass gate for the tiebreak adapter (design 204 §"Costs / risks").
    /// The structural [`TableMeta::tiebreak_prefix_len`] decides the adapter *can*
    /// engage; this decides it *should*, by estimating the equal-prefix run size from
    /// `sqlite_stat1`. A low-cardinality sort column (few distinct values ⇒ large runs)
    /// would have the adapter buffer a huge run in memory while holding the read cursor
    /// open, so bypass to SQL's own (temp-b-tree) sort instead. Correctness holds on
    /// either path, so an absent/stale estimate only costs performance.
    ///
    /// No cache: one `prepare_cached` lookup per engage-candidate fetch keeps the read
    /// path allocation-light while staying current with a later `ANALYZE` (revisit
    /// caching if this shows up in a profile). `true` when no stats exist yet, so the
    /// adapter still engages on un-`ANALYZE`d tables (today's behavior); `ANALYZE` (R5)
    /// is what makes the bypass fire for the tables where it matters.
    ///
    /// **Estimate is the average, not the max** (`sqlite_stat1` has no histogram): a
    /// skewed key with one giant value can still slip a large run past an in-range
    /// average — a bounded residual, not a hard cap.
    fn prefix_run_within_cap(&self, sort: &Sort, prefix_len: usize) -> bool {
        let Some(idx_name) = self.meta.prefix_stream_index(sort, prefix_len) else {
            return true; // structurally impossible after `tiebreak_prefix_len` → engage
        };
        let db = self.db.borrow();
        let stat: Option<String> = db
            .prepare_cached("SELECT stat FROM sqlite_stat1 WHERE tbl = ?1 AND idx = ?2")
            .ok()
            .and_then(|mut s| {
                s.query_row(rusqlite::params![&self.meta.table, idx_name], |r| r.get(0))
                    .ok()
            });
        match stat
            .as_deref()
            .and_then(|s| stat1_prefix_run(s, prefix_len))
        {
            Some(avg_run) => avg_run <= TIEBREAK_MAX_ENGAGE_RUN,
            None => true, // no ANALYZE stats (or malformed) → engage
        }
    }

    /// The full per-fetch tiebreak engage decision (design 204): the SQL `ORDER BY`
    /// prefix length to engage the adapter with, or `None` to bypass to the full-sort
    /// SQL plan. Engage only for an **ordered, unconstrained** fetch whose predicted
    /// equal-prefix run fits the cap.
    ///
    /// The **unconstrained** guard is the review fix for join children. A fetch carrying
    /// a constraint / multiConstraint (a join's FK equality, a pushed-down `where`) can
    /// steer SQLite to a *more selective* index than the prefix one; narrowing the
    /// `ORDER BY` to the prefix would then either layer redundant in-engine buffering on
    /// a temp b-tree SQLite still builds, or flip its plan to a full ordered scan.
    /// [`TableMeta::tiebreak_prefix_len`] is constraint-blind, so we can't judge that
    /// locally — bypass when constrained, keeping every unconstrained win with no
    /// regression (design 204 §"When to bypass": bias to bypass when unsure).
    fn tiebreak_engage(&self, req: &FetchRequest, sort: &Sort, unordered: bool) -> Option<usize> {
        if unordered || req.constraint.is_some() || req.has_multi() {
            return None;
        }
        self.meta
            .tiebreak_prefix_len(sort)
            .filter(|&prefix_len| self.prefix_run_within_cap(sort, prefix_len))
    }

    /// The unique-key column sets discovered at construction (§5.1). Each is a set
    /// of `ColId`s in `pragma_index_info` order.
    pub fn unique_indexes(&self) -> &[Vec<ColId>] {
        &self.unique_indexes
    }

    /// Swap the active snapshot DB handle (`setDB`, §5.7). In-flight cursors keep
    /// their own `Rc<Connection>`, so they are unaffected. Full Snapshotter
    /// integration is out of scope; only the handle rebind is wired here.
    pub fn set_db(&self, db: Rc<Connection>) {
        *self.db.borrow_mut() = db;
    }

    /// Wire a connection's downstream edge (mirrors `input.setOutput`).
    pub fn set_conn_output(&self, conn: ConnId, edge: OutEdge) {
        self.conns.set_output(conn, edge);
    }

    // -- push orchestration (identical shape to MemorySource::push) --

    /// Eager push: fan `change` to every wired connection (overlay live,
    /// epoch-gated), clear the overlay, then write — via the backend-agnostic
    /// [`try_gen_push_and_write_with_split_edit`]. The write is committed **after**
    /// the drain (§3.11), so a reentrant self-join fetch sees the overlay but not
    /// the written row. `push_one` is the graph's downstream driver.
    pub fn push(&self, change: SourceChange, push_one: &dyn Fn(&Conn, SourceChange)) {
        self.try_push(change, push_one, false)
            .expect("push SQLite change")
    }

    pub fn try_push(
        &self,
        change: SourceChange,
        push_one: &dyn Fn(&Conn, SourceChange),
        strict: bool,
    ) -> Result<(), RindleError> {
        try_gen_push_and_write_with_split_edit(
            &self.conns,
            change,
            &|row| self.exists(row),
            &|o| *self.overlay.borrow_mut() = o,
            &|c| self.write_change(c),
            &|| {
                let e = self.epoch.get() + 1;
                self.epoch.set(e);
                e
            },
            push_one,
            strict,
        )
    }

    /// `exists` — the PK-keyed `checkExists` (`SELECT 1 … WHERE pk=? LIMIT 1`,
    /// §3.11). Backs the ADD/REMOVE/EDIT existence asserts in `gen_push`.
    fn exists(&self, row: &Row) -> Result<bool, RindleError> {
        let db = self.db.borrow();
        let params = self.pk_params(row);
        // Bind to a local so the `CachedStatement` temporary drops at this `;`,
        // before the `db` borrow — not after it (E0597 on a tail expression).
        let found = db
            .prepare_cached(&self.meta.check_exists_sql)
            .map_err(|e| RindleError::sqlite("prepare checkExists", e))?
            .exists(params_from_iter(&params))
            .map_err(|e| RindleError::sqlite("run checkExists", e))?;
        Ok(found)
    }

    /// Apply ADD/REMOVE/EDIT to the backing table (`#writeChange`, §3.11). EDIT
    /// uses `UPDATE` when the PK is unchanged and a non-PK column exists, else
    /// `DELETE old` + `INSERT row`. Committed after the vend (caller ordering).
    fn write_change(&self, change: &SourceChange) -> Result<(), RindleError> {
        let db = self.db.borrow();
        match change {
            SourceChange::Add(row) => {
                let params = self.all_col_params(row);
                db.prepare_cached(&self.meta.insert_sql)
                    .map_err(|e| RindleError::sqlite("prepare insert", e))?
                    .execute(params_from_iter(&params))
                    .map_err(|e| RindleError::sqlite("run insert", e))?;
            }
            SourceChange::Remove(row) => {
                let params = self.pk_params(row);
                db.prepare_cached(&self.meta.delete_sql)
                    .map_err(|e| RindleError::sqlite("prepare delete", e))?
                    .execute(params_from_iter(&params))
                    .map_err(|e| RindleError::sqlite("run delete", e))?;
            }
            SourceChange::Edit { row, old } => {
                if self.can_use_update(old, row) {
                    // UPDATE binds the merged `{...old, ...row}` row; since a Rust
                    // Edit's `row` is always a FULL row, the merge collapses to
                    // `row` (§13 Q12). Params: non-PK new values, then PK values.
                    let mut params = self.non_pk_params(row);
                    params.extend(self.pk_params(row));
                    let update_sql = self
                        .meta
                        .update_sql
                        .as_ref()
                        .ok_or_else(|| RindleError::Storage("missing update SQL".into()))?;
                    db.prepare_cached(update_sql)
                        .map_err(|e| RindleError::sqlite("prepare update", e))?
                        .execute(params_from_iter(&params))
                        .map_err(|e| RindleError::sqlite("run update", e))?;
                } else {
                    let dparams = self.pk_params(old);
                    db.prepare_cached(&self.meta.delete_sql)
                        .map_err(|e| RindleError::sqlite("prepare delete", e))?
                        .execute(params_from_iter(&dparams))
                        .map_err(|e| RindleError::sqlite("run delete (edit)", e))?;
                    let iparams = self.all_col_params(row);
                    db.prepare_cached(&self.meta.insert_sql)
                        .map_err(|e| RindleError::sqlite("prepare insert", e))?
                        .execute(params_from_iter(&iparams))
                        .map_err(|e| RindleError::sqlite("run insert (edit)", e))?;
                }
            }
        }
        Ok(())
    }

    /// `canUseUpdate` (§3.11, table-source.ts:661): every PK column unchanged AND
    /// a non-PK column exists. The PK comparator is **identity** (`null == null` via
    /// [`compare_values`]) — the OPPOSITE polarity of the split-edit comparator
    /// (§3.6 / §13 Q13), so a null PK counts as "unchanged" and permits UPDATE.
    fn can_use_update(&self, old: &Row, row: &Row) -> bool {
        if self.meta.update_sql.is_none() {
            return false; // every column is PK → no SET list
        }
        self.meta
            .primary_key
            .iter()
            .all(|&pk| compare_values(old.col(pk), row.col(pk)) == Ordering::Equal)
    }

    /// Bind every column (the INSERT value list), in declared order.
    fn all_col_params(&self, row: &Row) -> Vec<SqliteParam> {
        (0..self.meta.columns.len())
            .map(|c| to_sqlite_param(row.col(c), self.meta.columns[c].ty))
            .collect()
    }

    /// Bind the PK columns (the DELETE/UPDATE/checkExists WHERE), in PK order.
    fn pk_params(&self, row: &Row) -> Vec<SqliteParam> {
        self.meta
            .primary_key
            .iter()
            .map(|&c| to_sqlite_param(row.col(c), self.meta.columns[c].ty))
            .collect()
    }

    /// Bind the non-PK columns (the UPDATE SET list), in declared order.
    fn non_pk_params(&self, row: &Row) -> Vec<SqliteParam> {
        self.meta
            .non_pk
            .iter()
            .map(|&c| to_sqlite_param(row.col(c), self.meta.columns[c].ty))
            .collect()
    }

    /// Retrieve a single row by an **arbitrary unique key** (`getRow`, §3.12) — not
    /// used in the IVM pipeline but useful for consistency reads. Builds `SELECT
    /// <all declared cols> … WHERE keyCols = ?` (bare `=`, like the constraint
    /// path), runs it, and returns an **owned** row (it is meant to escape) or
    /// `None`. Use [`Self::try_get_row`] to surface SQLite and value-conversion
    /// failures as [`RindleError`]; this compatibility wrapper panics on those errors.
    pub fn get_row(&self, key: &[(ColId, OwnedValue)]) -> Option<Row> {
        self.try_get_row(key).expect("get SQLite row")
    }

    pub fn try_get_row(&self, key: &[(ColId, OwnedValue)]) -> Result<Option<Row>, RindleError> {
        let sql = self.get_row_sql(key);
        let params: Vec<SqliteParam> = key
            .iter()
            .map(|(c, v)| to_sqlite_param(v.as_ref(), self.meta.columns[*c].ty))
            .collect();
        let db = self.db.borrow();
        let mut stmt = db
            .prepare_cached(&sql)
            .map_err(|e| RindleError::sqlite("prepare getRow", e))?;
        let mut rows = stmt
            .query(params_from_iter(&params))
            .map_err(|e| RindleError::sqlite("run getRow", e))?;
        match rows
            .next()
            .map_err(|e| RindleError::sqlite("step getRow", e))?
        {
            Some(row) => {
                if let Err(e) = check_row_cells(row, &self.meta) {
                    return Err(e.into());
                }
                Ok(Some(
                    SqliteRow::new(row, &self.meta.col_types).try_to_owned_row()?,
                ))
            }
            None => Ok(None),
        }
    }

    /// `SELECT <all declared cols> FROM t WHERE k0=? AND k1=?` for a key set
    /// (`#getRowStmt`, §3.12). Built per call; `prepare_cached` caches the prepared
    /// statement by text, so the repeated-key-shape cost is just the string build.
    fn get_row_sql(&self, key: &[(ColId, OwnedValue)]) -> String {
        let mut sql = String::from("SELECT ");
        for (i, c) in self.meta.columns.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            sql.push_str(&ident(&c.name));
        }
        sql.push_str(" FROM ");
        sql.push_str(&ident(&self.meta.table));
        sql.push_str(" WHERE ");
        for (i, (col, _)) in key.iter().enumerate() {
            if i > 0 {
                sql.push_str(" AND ");
            }
            sql.push_str(&ident(&self.meta.columns[*col].name));
            sql.push_str(" = ?");
        }
        sql
    }
}

/// `assertOrderingIncludesPK` (`complete-ordering.ts`): an ordered connection's
/// sort must include every PK column (so sort-equality ⇒ identity — the overlay
/// remove-suppression depends on it, §3.5). Mirrors the memory leaf's private check.
fn assert_ordering_includes_pk(sort: &Sort, pk: &[ColId]) {
    for &p in pk {
        assert!(
            sort.iter().any(|(c, _)| *c == p),
            "connection ordering must include the primary key column {p}",
        );
    }
}

impl Source for TableSource {
    fn connect(
        &self,
        sort: Option<Sort>,
        filters: Option<ConnectionFilters>,
        split_edit_keys: Vec<ColId>,
    ) -> ConnId {
        let unordered = sort.is_none();
        let internal_sort =
            sort.unwrap_or_else(|| self.meta.primary_key.iter().map(|&c| (c, true)).collect());
        if !unordered {
            assert_ordering_includes_pk(&internal_sort, &self.meta.primary_key);
        }
        self.conns.connect(Conn {
            sort: internal_sort,
            unordered,
            split_edit_keys,
            filters,
            last_pushed_epoch: Cell::new(0),
            output: Cell::new(None),
        })
    }

    fn schema(&self) -> &Schema {
        &self.meta.schema
    }

    fn conn_sort(&self, conn: ConnId) -> Sort {
        self.conns.sort(conn)
    }

    fn destroy(&self, conn: ConnId) {
        // Null the output edge AND recycle the slot (the shared `ConnTable` free-list);
        // backing SQLite indexes are kept (§3.10), like the memory leaf.
        self.conns.destroy(conn);
    }

    fn cursors_open(&self) -> i64 {
        TableSource::cursors_open(self)
    }

    fn try_push(
        &self,
        change: SourceChange,
        push_one: &dyn Fn(&Conn, SourceChange),
        strict: bool,
    ) -> Result<(), RindleError> {
        TableSource::try_push(self, change, push_one, strict)
    }

    fn take_error(&self) -> Option<RindleError> {
        TableSource::take_fetch_error(self)
    }

    fn set_conn_output(&self, conn: ConnId, edge: OutEdge) {
        TableSource::set_conn_output(self, conn, edge)
    }

    /// `#fetch` (the hot read path, §5.3). Compile the request to a `SELECT`
    /// (constraint + multiConstraints + start-prefilter + filter + ORDER BY all
    /// lowered into SQL), open the lazy zero-copy cursor, then drive it through the
    /// shared overlay/start seam. **Unlike the memory leaf there is NO committed-row
    /// constraint-trim or filter pass** — SQL already did them; the connection's
    /// `predicate` narrows only the (not-in-SQL) overlay rows (table-source.ts:297).
    fn fetch<'g>(&'g self, conn: ConnId, req: &FetchRequest) -> RowFlow<'g> {
        let idx = self.conns.live_ix(conn);

        // Snapshot the connection's needed bits (short borrow, cloned out — no
        // RefCell borrow held across the vend, foundations §6.2).
        let (sort, unordered, predicate, sql_condition, last_epoch) = {
            let conns = self.conns.borrow();
            let c = &conns[idx];
            (
                c.sort.clone(),
                c.unordered,
                c.filters.as_ref().map(|f| f.predicate.clone()),
                c.filters.as_ref().and_then(|f| f.sql_condition.clone()),
                c.last_pushed_epoch.get(),
            )
        };

        // In-engine sort tiebreak (design 204). `Some(n)` narrows the SQL `ORDER BY` to
        // the first `n` (prefix) columns and re-applies the appended-PK tiebreak in
        // engine; `None` keeps today's full-sort SQL plan. `tiebreak_engage` holds the
        // ordered / unconstrained / run-size gates.
        let tiebreak = self.tiebreak_engage(req, &sort, unordered);

        // What the SELECT orders by, and the start bound it prefilters on. When the
        // adapter is engaged the SQL order narrows to the prefix and the start bound is
        // relaxed to be inclusive on the prefix boundary (`Basis::At` over the prefix),
        // so the whole boundary run is returned; the exact At/After cut is re-applied in
        // engine by `generate_with_start` over the full sort. The full `sort`/`reverse`
        // still flow to the overlay seam below.
        let prefix_sort: Option<Sort> = tiebreak.map(|n| sort[..n].to_vec());
        let sql_order: Option<&Sort> = if unordered {
            None
        } else if let Some(p) = prefix_sort.as_ref() {
            Some(p)
        } else {
            Some(&sort)
        };
        let relaxed_start: Option<Start> = if tiebreak.is_some() {
            req.start.as_ref().map(|s| Start {
                row: s.row.clone(),
                basis: Basis::At,
            })
        } else {
            req.start.clone()
        };
        let compiled = build_select_query(
            &self.meta.table,
            &self.meta.columns,
            req.constraint.as_ref(),
            &req.multi_constraints,
            sql_condition.as_ref(),
            sql_order,
            req.reverse,
            relaxed_start.as_ref(),
        );

        // Open the cursor: it OWNS the conn (Rc snapshot) + prepared statement +
        // rows, so the lazy stream can be boxed and returned (the §13 Q1 self-ref).
        let db = self.db.borrow().clone();
        let guard = CursorGuard::new(self.cursors_open.clone());
        let leaf = match OwnedSqliteRows::new(
            db,
            &compiled.sql,
            &compiled.params,
            self.meta.clone(),
            self.fetch_error.clone(),
            guard,
        ) {
            Ok(leaf) => leaf,
            Err(e) => {
                *self.fetch_error.borrow_mut() =
                    Some(RindleError::sqlite("prepare/query fetch", e));
                return Box::new(std::iter::empty());
            }
        };

        // `scan-stats`: hand this cursor the source's shared work-counter sink so it
        // accumulates the statement's `sqlite3_stmt_status` counters when it drops; and
        // capture the representative fetch SQL (first-write-wins) for `analyze`'s per-leaf
        // `EXPLAIN`. Both are no-ops unless a sink was attached (only during analyze).
        #[cfg(feature = "scan-stats")]
        let leaf = {
            if let Some(sink) = self.plan_sink.borrow().as_ref() {
                let mut slot = sink.borrow_mut();
                if slot.is_none() {
                    *slot = Some(compiled.sql.clone());
                }
            }
            leaf.with_scan_sink(self.scan_sink.borrow().clone())
        };

        // Snapshot the epoch-gated overlay (tight borrow, cloned out). The shared
        // seam computes the narrowed overlays eagerly, then yields the lazy splice.
        let overlay = self.overlay.borrow().clone();

        if unordered {
            generate_with_overlay_unordered_checked(
                leaf,
                req.constraint.as_ref(),
                overlay.as_ref(),
                last_epoch,
                &self.meta.primary_key,
                predicate.as_ref(),
                &req.multi_constraints,
                Some(self.fetch_error.clone()),
            )
        } else {
            let start_at = req.start.as_ref().map(|s| s.row.clone());
            // When engaged, restore the full `(prefix, pk)` order in-engine *before* the
            // overlay splice sees the stream (design 204). The seam still receives the
            // full `sort`/`reverse`.
            let stream = match tiebreak {
                Some(prefix_len) => {
                    let tb = TiebreakStream::new(
                        leaf,
                        sort.clone(),
                        prefix_len,
                        req.reverse,
                        self.fetch_error.clone(),
                    );
                    generate_with_overlay_checked(
                        start_at.as_ref(),
                        tb,
                        req.constraint.as_ref(),
                        overlay.as_ref(),
                        last_epoch,
                        &sort,
                        req.reverse,
                        predicate.as_ref(),
                        &req.multi_constraints,
                        Some(self.fetch_error.clone()),
                    )
                }
                None => generate_with_overlay_checked(
                    start_at.as_ref(),
                    leaf,
                    req.constraint.as_ref(),
                    overlay.as_ref(),
                    last_epoch,
                    &sort,
                    req.reverse,
                    predicate.as_ref(),
                    &req.multi_constraints,
                    Some(self.fetch_error.clone()),
                ),
            };
            generate_with_start(stream, req.start.clone(), sort, req.reverse)
        }
    }
}

/// The build-time read seam for scalar-subquery resolution
/// (`SCALAR-SUBQUERY-DESIGN.md` §8): uniqueness metadata + a point lookup. Distinct
/// from the runtime [`Source`] trait — no connect/fetch/push, just a read.
impl rindle::scalar::ScalarSource for TableSource {
    fn schema(&self) -> &Schema {
        &self.meta.schema
    }

    /// PK first — always available, even for a rowid-alias PK with no separate index
    /// row — then each *discovered* non-PK unique key (the hardened
    /// `discover_unique_indexes` set, design §4.1).
    fn unique_keys(&self) -> Vec<Vec<ColId>> {
        let pk = &self.meta.primary_key;
        let mut keys = vec![pk.clone()];
        for ix in &self.unique_indexes {
            if !same_col_set(ix, pk) {
                keys.push(ix.clone());
            }
        }
        keys
    }

    /// The single row whose `bound` key columns match — a real `SELECT … WHERE k = ?`
    /// point read against the snapshot ([`TableSource::get_row`]).
    fn lookup_unique(&self, bound: &[(ColId, OwnedValue)]) -> Option<Row> {
        self.get_row(bound)
    }
}

// ---------------------------------------------------------------------------
// The owning zero-copy cursor (the §13 Q1 self-reference, solved)
// ---------------------------------------------------------------------------

/// Owns the whole conn→stmt→cursor borrow chain as one movable, boxable unit so a
/// lazy [`RowFlow`] can return it. This is the spec's #1 implementation risk
/// (`05` §13 Q1): a `rusqlite::Rows` borrows a `Statement` which borrows the
/// `Connection`, and safe Rust cannot store a value alongside what it borrows. We
/// solve it with one contained `unsafe`: the statement is **`Box`-pinned** (stable
/// heap address) and the `Connection` is kept alive by an owned `Rc`, so the two
/// borrows are real and the lifetimes are erased to `'static` only internally —
/// never exposed. The field **drop order is load-bearing** (declaration order):
/// `rows` (resets the cursor) → `_stmt` (returns the raw statement to the
/// connection's cache) → `_conn` (last `Rc` release). Reordering would return the
/// statement to a freed connection.
struct OwnedSqliteRows {
    rows: Rows<'static>,
    _stmt: Box<CachedStatement<'static>>,
    _conn: Rc<Connection>,
    meta: Rc<TableMeta>,
    /// Shared with the source: a parked error is written here and resurfaced via
    /// [`TableSource::take_fetch_error`] after the drain.
    error_sink: Rc<RefCell<Option<RindleError>>>,
    _guard: CursorGuard,
    /// `scan-stats` only: where this cursor reports its `sqlite3_stmt_status` work
    /// counters on drop. `None` ⇒ uninstrumented (the production default).
    #[cfg(feature = "scan-stats")]
    scan_sink: Option<ScanSink>,
}

impl OwnedSqliteRows {
    fn new(
        conn: Rc<Connection>,
        sql: &str,
        params: &[SqliteParam],
        meta: Rc<TableMeta>,
        error_sink: Rc<RefCell<Option<RindleError>>>,
        guard: CursorGuard,
    ) -> rusqlite::Result<OwnedSqliteRows> {
        let stmt_borrowed: CachedStatement<'_> = conn.prepare_cached(sql)?;
        // SAFETY: erase the connection-borrow lifetime to 'static. `_conn` (an Rc
        // clone) is stored in this struct and dropped AFTER `_stmt`, so the
        // Connection strictly outlives the statement. The 'static reference is
        // never exposed outside this struct.
        let mut stmt: Box<CachedStatement<'static>> = Box::new(unsafe {
            std::mem::transmute::<CachedStatement<'_>, CachedStatement<'static>>(stmt_borrowed)
        });
        let rows_borrowed = stmt.query(params_from_iter(params))?;
        // SAFETY: `rows` borrows `*stmt`. `stmt` is `Box`-pinned, so moving the Box
        // into the struct leaves the heap `CachedStatement` (and thus the pointer
        // inside `rows`) at a stable address. `_stmt` is dropped AFTER `rows`
        // (declaration order). We only ever touch the statement through `rows`
        // after this point, so the aliasing is never observed.
        let rows: Rows<'static> =
            unsafe { std::mem::transmute::<Rows<'_>, Rows<'static>>(rows_borrowed) };
        Ok(OwnedSqliteRows {
            rows,
            _stmt: stmt,
            _conn: conn,
            meta,
            error_sink,
            _guard: guard,
            #[cfg(feature = "scan-stats")]
            scan_sink: None,
        })
    }
}

#[cfg(feature = "scan-stats")]
impl OwnedSqliteRows {
    /// Attach the source's shared work-counter sink to this cursor.
    ///
    /// When a sink is attached, the statement's counters are **zeroed first**: the
    /// prepared statement comes from the connection's statement cache, and a *live*
    /// cursor (no sink) accumulates `sqlite3_stmt_status` counts it never resets — so
    /// without this reset an instrumented run over previously-live leaf SQL would read
    /// that history into its own totals.
    fn with_scan_sink(mut self, sink: Option<ScanSink>) -> Self {
        if sink.is_some() {
            use rusqlite::StatementStatus::{FullscanStep, Run, Sort, VmStep};
            for s in [FullscanStep, Run, Sort, VmStep] {
                let _ = self._stmt.reset_status(s);
            }
        }
        self.scan_sink = sink;
        self
    }
}

/// On drop, read the executed statement's `sqlite3_stmt_status` counters into the
/// shared sink. `reset_status` returns the prior value AND zeroes the counter — which
/// is required for correctness: these counters persist across `sqlite3_reset`, so a
/// cached statement reused on the next fetch must start from zero or the totals would
/// double-count. Drop runs before the fields, so `_stmt` is still live here; the read
/// is a counter query (no execution-state mutation), so it does not disturb the
/// (already drop-ordered) `rows`→`_stmt`→`_conn` teardown.
#[cfg(feature = "scan-stats")]
impl Drop for OwnedSqliteRows {
    fn drop(&mut self) {
        let Some(sink) = &self.scan_sink else { return };
        use rusqlite::StatementStatus::{FullscanStep, Run, Sort, VmStep};
        let read = |s| self._stmt.reset_status(s).max(0) as u64;
        let mut acc = sink.get();
        acc.fullscan_steps += read(FullscanStep);
        acc.sorts += read(Sort);
        acc.vm_steps += read(VmStep);
        acc.runs += read(Run);
        sink.set(acc);
    }
}

impl rindle::value::RowStream for OwnedSqliteRows {
    type Row<'a>
        = SqliteRow<'a>
    where
        Self: 'a;

    fn next_row(&mut self) -> Option<SqliteRow<'_>> {
        match self.rows.next() {
            Ok(Some(row)) => {
                // Eager per-cell contract validation — lossless f64 for `number`
                // columns, NULL-or-INTEGER storage class for `int64` columns —
                // matching the capture and SQL-client value boundary. A violation
                // parks `UnsupportedValue` and ends the stream — NOT a silent end.
                // (json/UTF-8 stay deferred to `to_owned`.)
                if let Err(e) = check_row_cells(row, &self.meta) {
                    *self.error_sink.borrow_mut() = Some(e.into());
                    return None;
                }
                Some(SqliteRow::new(row, &self.meta.col_types))
            }
            Ok(None) => None,
            Err(e) => {
                // A `sqlite3_step` failure (I/O, corruption, expression error) —
                // parked, never mapped to a clean end-of-stream (`05` §4.5).
                *self.error_sink.borrow_mut() = Some(SqliteError::Step(e).into());
                None
            }
        }
    }
}

/// Eager lossless-f64 check: a `number`-column (`ColType::Float`) cell stored as
/// INTEGER must widen to `f64` and narrow back without precision loss, or it is an
/// [`SqliteError::UnsupportedValue`]. Only `number` columns are checked because
/// their canonical engine representation is `f64`.
fn check_number_bounds(row: &rusqlite::Row, meta: &TableMeta) -> Result<(), SqliteError> {
    for &i in &meta.float_cols {
        if let ValueRef::Integer(v) = row.get_ref_unwrap(i) {
            if !is_exact_f64_integer(v) {
                return Err(SqliteError::UnsupportedValue(format!(
                    "value {v} (in {}.{}) cannot round-trip through f64",
                    meta.table, meta.columns[i].name
                )));
            }
        }
    }
    Ok(())
}

/// Eager storage-class check for declared `int64` (`ColType::Int`) columns
/// (design 226 §4.2): a cell must be NULL or INTEGER. SQLite affinity would
/// happily hold REAL/TEXT/BLOB under a `BIGINT` declaration; refusing it here —
/// before [`SqliteRow::col`] can hit its deterministic mismatch pass-through —
/// is what makes an `int64` position provably `Int`, mirroring the CDC capture
/// guard so cold hydrate and live capture agree.
fn check_int_storage(row: &rusqlite::Row, meta: &TableMeta) -> Result<(), SqliteError> {
    for &i in &meta.int_cols {
        match row.get_ref_unwrap(i) {
            ValueRef::Null | ValueRef::Integer(_) => {}
            other => {
                return Err(SqliteError::UnsupportedValue(format!(
                    "storage class {} (in {}.{}) in an int64 column (int64 cells are NULL or INTEGER)",
                    other.data_type(),
                    meta.table,
                    meta.columns[i].name
                )));
            }
        }
    }
    Ok(())
}

/// The per-row cell contract at the read boundary: the `number` lossless-f64
/// check plus the `int64` storage-class check. One call site per cursor shape.
fn check_row_cells(row: &rusqlite::Row, meta: &TableMeta) -> Result<(), SqliteError> {
    check_number_bounds(row, meta)?;
    check_int_storage(row, meta)
}

// ---------------------------------------------------------------------------
// RAII cursor guard (Primitive #2): Drop == JS finally/.return() + cache return
// ---------------------------------------------------------------------------

/// Bumps the open-cursor count on creation, decrements on `Drop`. Rides *inside*
/// [`OwnedSqliteRows`], so it fires when the boxed stream drops — on normal end,
/// an early `Take` break, `?`, or early return — for free, no `finally`. NOTE:
/// this leaf is `sqlite`-only (a native server target). Run the server with the
/// `release-server` profile (`panic = "unwind"`), where `Drop` also runs on panic;
/// under a plain `release` build (`panic = "abort"`) a panic aborts the process and
/// NO destructor runs, so panic is covered only in unwinding builds (`release-server`,
/// `cargo test`). See WS02.
struct CursorGuard(Rc<Cell<i64>>);
impl CursorGuard {
    fn new(c: Rc<Cell<i64>>) -> CursorGuard {
        c.set(c.get() + 1);
        CursorGuard(c)
    }
}
impl Drop for CursorGuard {
    fn drop(&mut self) {
        self.0.set(self.0.get() - 1);
    }
}

// ---------------------------------------------------------------------------
// Construction helpers: unique-index discovery + write-statement SQL
// ---------------------------------------------------------------------------

/// Discover the table's **statically-unique, soundly-inlineable** key column sets
/// (`getUniqueIndexes`, §5.1; scalar-subquery design §4.1–4.2) via `pragma_index_list`
/// / `pragma_index_xinfo` (bound params, no interpolation). Each result is a
/// `Vec<ColId>` of one unique index's key columns. Finds the auto-created PK/UNIQUE
/// indexes too, so the PK-UNIQUE construction assert holds for an ordinary
/// `PRIMARY KEY(...)` table, and the scalar resolver can prove single-row.
///
/// Two hardening rules make this sound for *inlining* (not just the assert):
/// - **PARTIAL unique indexes are skipped** (`partial = 1`): they enforce uniqueness
///   only within their predicate, so a bound match could still hit >1 row globally.
/// - **An expression key column discards the whole index.** `index_xinfo` reports a
///   NULL `name` for an expression key (e.g. `lower(x)`); silently dropping it would
///   make the index look *narrower* than it is — the unsound direction. If any key
///   column is not a plain declared column, the index is unusable and dropped whole.
///
/// (Collation §4.5 — only `BINARY` keys are strictly safe to inline against — is a
/// follow-up; the slice's real use is the PK, `BINARY` by default.)
fn discover_unique_indexes(
    db: &Connection,
    table: &str,
    columns: &[ColumnDef],
    primary_key: &[ColId],
) -> Result<Vec<Vec<ColId>>, RindleError> {
    let name_to_id = |n: &str| -> Option<ColId> { columns.iter().position(|c| &*c.name == n) };

    // (index name, unique flag, partial flag) for every index on the table.
    let idx_list: Vec<(String, i64, i64)> = {
        let mut stmt = db
            .prepare("SELECT name, \"unique\", partial FROM pragma_index_list(?1)")
            .map_err(|e| RindleError::sqlite("prepare pragma_index_list", e))?;
        let out = stmt
            .query_map([table], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, i64>(2)?,
                ))
            })
            .map_err(|e| RindleError::sqlite("run pragma_index_list", e))?
            .filter_map(|r| r.ok())
            .collect();
        out
    };

    let mut result = Vec::new();
    for (idx_name, unique, partial) in idx_list {
        if unique == 0 || partial != 0 {
            continue; // non-unique, or partial (predicate-scoped) → not inlineable
        }
        // Key columns of this index (`key = 1`); a NULL name ⇒ expression column.
        let mut info = db
            .prepare("SELECT name, key FROM pragma_index_xinfo(?1)")
            .map_err(|e| RindleError::sqlite("prepare pragma_index_xinfo", e))?;
        let key_cols: Vec<Option<String>> = info
            .query_map([&idx_name], |r| {
                Ok((r.get::<_, Option<String>>(0)?, r.get::<_, i64>(1)?))
            })
            .map_err(|e| RindleError::sqlite("run pragma_index_xinfo", e))?
            .filter_map(|r| r.ok())
            .filter(|(_, key)| *key == 1)
            .map(|(name, _)| name)
            .collect();

        // Resolve every key column to a declared `ColId`; an expression (NULL) or
        // unknown column makes the whole index unusable (`collect` to `Option`
        // short-circuits to `None`).
        let resolved: Option<Vec<ColId>> = key_cols
            .into_iter()
            .map(|n| n.as_deref().and_then(name_to_id))
            .collect();
        if let Some(cols) = resolved {
            if !cols.is_empty() {
                result.push(cols);
            }
        }
    }

    // A rowid-alias `INTEGER PRIMARY KEY` (a single column whose DECLARED type is exactly
    // `INTEGER`) has no entry in `pragma_index_list`, yet it IS a unique key: the rowid enforces
    // uniqueness and SQLite point-looks-it-up natively (`SEARCH … USING INTEGER PRIMARY KEY`).
    // Recognize it so the source needs no redundant synthetic PK index (GitHub #68) and the
    // PK-has-a-unique-key invariant still holds.
    if primary_key.len() == 1 && !result.iter().any(|ix| same_col_set(ix, primary_key)) {
        let pk_name: &str = &columns[primary_key[0]].name;
        let decl: rusqlite::Result<String> = db.query_row(
            "SELECT type FROM pragma_table_info(?1) WHERE name = ?2",
            rusqlite::params![table, pk_name],
            |r| r.get(0),
        );
        if matches!(decl, Ok(t) if t.trim().eq_ignore_ascii_case("INTEGER")) {
            result.push(primary_key.to_vec());
        }
    }
    Ok(result)
}

/// True when `primary_key` is a single `INTEGER PRIMARY KEY` — SQLite's rowid alias
/// (design 204 / GitHub #68). Runs the same declared-type probe
/// [`discover_unique_indexes`] uses to recognize the alias.
fn is_pk_rowid_alias(
    db: &Connection,
    table: &str,
    columns: &[ColumnDef],
    primary_key: &[ColId],
) -> bool {
    if primary_key.len() != 1 {
        return false;
    }
    let pk_name: &str = &columns[primary_key[0]].name;
    let decl: rusqlite::Result<String> = db.query_row(
        "SELECT type FROM pragma_table_info(?1) WHERE name = ?2",
        rusqlite::params![table, pk_name],
        |r| r.get(0),
    );
    matches!(decl, Ok(t) if t.trim().eq_ignore_ascii_case("INTEGER"))
}

/// Every index's leading resolvable key columns (in index order), unique or not — the
/// structural basis for the tiebreak bypass decision (design 204,
/// [`TableMeta::tiebreak_prefix_len`]). Unlike [`discover_unique_indexes`] this keeps
/// non-unique indexes (a user's `CREATE INDEX ix ON t(ord)`) and stops each index at
/// its first expression/unknown key column (the leading resolvable run is all the
/// ordering decision needs).
fn discover_all_index_columns(
    db: &Connection,
    table: &str,
    columns: &[ColumnDef],
) -> Result<Vec<(String, Vec<ColId>)>, RindleError> {
    let name_to_id = |n: &str| -> Option<ColId> { columns.iter().position(|c| &*c.name == n) };

    let idx_names: Vec<String> = {
        let mut stmt = db
            .prepare("SELECT name FROM pragma_index_list(?1)")
            .map_err(|e| RindleError::sqlite("prepare pragma_index_list (all)", e))?;
        let out = stmt
            .query_map([table], |r| r.get::<_, String>(0))
            .map_err(|e| RindleError::sqlite("run pragma_index_list (all)", e))?
            .filter_map(|r| r.ok())
            .collect();
        out
    };

    let mut result = Vec::new();
    for idx_name in idx_names {
        let mut info = db
            .prepare("SELECT name, key FROM pragma_index_xinfo(?1)")
            .map_err(|e| RindleError::sqlite("prepare pragma_index_xinfo (all)", e))?;
        // Key columns only (`key = 1`), in index order (the pragma yields seqno order).
        let key_cols: Vec<Option<String>> = info
            .query_map([&idx_name], |r| {
                Ok((r.get::<_, Option<String>>(0)?, r.get::<_, i64>(1)?))
            })
            .map_err(|e| RindleError::sqlite("run pragma_index_xinfo (all)", e))?
            .filter_map(|r| r.ok())
            .filter(|(_, key)| *key == 1)
            .map(|(name, _)| name)
            .collect();

        // Leading resolvable columns only: stop at the first expression/unknown key.
        let mut cols = Vec::new();
        for n in key_cols {
            match n.as_deref().and_then(name_to_id) {
                Some(id) => cols.push(id),
                None => break,
            }
        }
        if !cols.is_empty() {
            result.push((idx_name, cols));
        }
    }
    Ok(result)
}

/// Set-equality over two `ColId` lists (order-insensitive) — used to dedup the PK
/// against the discovered unique indexes in [`TableSource`]'s `unique_keys`.
fn same_col_set(a: &[ColId], b: &[ColId]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut a = a.to_vec();
    let mut b = b.to_vec();
    a.sort_unstable();
    b.sort_unstable();
    a == b
}

fn build_insert_sql(table: &str, columns: &[ColumnDef]) -> String {
    let cols = columns
        .iter()
        .map(|c| ident(&c.name))
        .collect::<Vec<_>>()
        .join(", ");
    let placeholders = vec!["?"; columns.len()].join(", ");
    format!(
        "INSERT INTO {} ({cols}) VALUES ({placeholders})",
        ident(table)
    )
}

fn build_delete_sql(table: &str, pk: &[ColId], columns: &[ColumnDef]) -> String {
    format!(
        "DELETE FROM {} WHERE {}",
        ident(table),
        pk_eq_clause(pk, columns)
    )
}

fn build_update_sql(
    table: &str,
    pk: &[ColId],
    non_pk: &[ColId],
    columns: &[ColumnDef],
) -> Option<String> {
    if non_pk.is_empty() {
        return None; // every column is PK — cannot UPDATE (§3.11)
    }
    let set = non_pk
        .iter()
        .map(|&c| format!("{} = ?", ident(&columns[c].name)))
        .collect::<Vec<_>>()
        .join(", ");
    Some(format!(
        "UPDATE {} SET {set} WHERE {}",
        ident(table),
        pk_eq_clause(pk, columns)
    ))
}

fn build_check_exists_sql(table: &str, pk: &[ColId], columns: &[ColumnDef]) -> String {
    format!(
        "SELECT 1 FROM {} WHERE {} LIMIT 1",
        ident(table),
        pk_eq_clause(pk, columns)
    )
}

/// `pk0 = ? AND pk1 = ? …` — the shared WHERE for delete/update/checkExists.
fn pk_eq_clause(pk: &[ColId], columns: &[ColumnDef]) -> String {
    pk.iter()
        .map(|&c| format!("{} = ?", ident(&columns[c].name)))
        .collect::<Vec<_>>()
        .join(" AND ")
}

/// The in-engine sort tiebreak *bypass decision* (design 204). Correctness holds on
/// both paths, so these assert the perf heuristic itself — that the adapter engages
/// for the rowid+non-rowid-PK shape and bypasses everywhere SQL can already stream the
/// full `(prefix, pk)` order. Without these, a silently-bypassing regression would
/// still pass every differential correctness test.
#[cfg(test)]
mod tiebreak_decision {
    use super::*;

    fn cols_k_ord() -> Vec<ColumnDef> {
        vec![
            ColumnDef {
                name: "k".into(),
                ty: ValueType::String,
                optional: false,
            },
            ColumnDef {
                name: "ord".into(),
                ty: ValueType::Number,
                optional: false,
            },
        ]
    }

    /// Build a `TableSource` over table `t` from `ddl`.
    fn source(ddl: &str, cols: Vec<ColumnDef>, pk: Vec<ColId>) -> TableSource {
        let conn = Rc::new(rusqlite::Connection::open_in_memory().unwrap());
        conn.execute_batch(ddl).unwrap();
        TableSource::new(conn, "t", cols, pk)
    }

    /// The engine's resolved order for `ORDER BY ord`: `(ord=1 asc, appended pk=0 asc)`.
    fn ord_then_pk() -> Sort {
        vec![(1, true), (0, true)]
    }

    #[test]
    fn engages_for_rowid_text_pk_with_prefix_index() {
        let ts = source(
            "CREATE TABLE t (k TEXT NOT NULL PRIMARY KEY, ord INTEGER NOT NULL);
             CREATE INDEX ix ON t (ord);",
            cols_k_ord(),
            vec![0],
        );
        assert_eq!(ts.meta.tiebreak_prefix_len(&ord_then_pk()), Some(1));
    }

    #[test]
    fn bypasses_rowid_alias_integer_pk() {
        // `id INTEGER PRIMARY KEY` is the rowid; an (ord) index is (ord, rowid) = (ord, pk).
        let cols = vec![
            ColumnDef {
                name: "id".into(),
                ty: ValueType::Number,
                optional: false,
            },
            ColumnDef {
                name: "ord".into(),
                ty: ValueType::Number,
                optional: false,
            },
        ];
        let ts = source(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, ord INTEGER NOT NULL);
             CREATE INDEX ix ON t (ord);",
            cols,
            vec![0],
        );
        assert_eq!(ts.meta.tiebreak_prefix_len(&ord_then_pk()), None);
    }

    #[test]
    fn bypasses_without_rowid() {
        let ts = source(
            "CREATE TABLE t (k TEXT NOT NULL PRIMARY KEY, ord INTEGER NOT NULL) WITHOUT ROWID;
             CREATE INDEX ix ON t (ord);",
            cols_k_ord(),
            vec![0],
        );
        assert_eq!(ts.meta.tiebreak_prefix_len(&ord_then_pk()), None);
    }

    #[test]
    fn bypasses_when_covering_index_exists() {
        // The good-citizen user who followed R3 and added the suffixed (ord, k) index.
        let ts = source(
            "CREATE TABLE t (k TEXT NOT NULL PRIMARY KEY, ord INTEGER NOT NULL);
             CREATE INDEX ix ON t (ord, k);",
            cols_k_ord(),
            vec![0],
        );
        assert_eq!(ts.meta.tiebreak_prefix_len(&ord_then_pk()), None);
    }

    #[test]
    fn bypasses_without_prefix_index() {
        // No (ord) index: SQL temp-b-trees either way, so the adapter would only buffer.
        let ts = source(
            "CREATE TABLE t (k TEXT NOT NULL PRIMARY KEY, ord INTEGER NOT NULL);",
            cols_k_ord(),
            vec![0],
        );
        assert_eq!(ts.meta.tiebreak_prefix_len(&ord_then_pk()), None);
    }

    #[test]
    fn bypasses_pure_pk_order() {
        // `ORDER BY k` (the pk): nothing is appended, so there is no tiebreak to relocate.
        let ts = source(
            "CREATE TABLE t (k TEXT NOT NULL PRIMARY KEY, ord INTEGER NOT NULL);
             CREATE INDEX ix ON t (ord);",
            cols_k_ord(),
            vec![0],
        );
        let pk_only: Sort = vec![(0, true)];
        assert_eq!(ts.meta.tiebreak_prefix_len(&pk_only), None);
    }

    // -- the sqlite_stat1 large-run bypass gate (design 204 §"Costs / risks") --

    /// `stat1.stat` is `"<nRows> <k1> <k2> …"`; the run estimate for a length-`L`
    /// prefix is `parts[L]` (avg rows sharing the first `L` index columns).
    #[test]
    fn stat1_prefix_run_parsing() {
        assert_eq!(stat1_prefix_run("1000 500", 1), Some(500));
        assert_eq!(stat1_prefix_run("1000 500 2", 1), Some(500));
        assert_eq!(stat1_prefix_run("1000 500 2", 2), Some(2)); // composite prefix
        assert_eq!(stat1_prefix_run("1000 500", 2), None); // stat too short
        assert_eq!(stat1_prefix_run("", 1), None);
        assert_eq!(stat1_prefix_run("1000 x", 1), None); // malformed
    }

    /// Build a rowid+TEXT-pk table with an unsuffixed `(ord)` index and bulk-load
    /// `rows` rows whose `ord` is `ord_expr(n)` (a SQL expression over the row number).
    fn analyzed_kv(rows: u32, ord_expr: &str) -> TableSource {
        let conn = Rc::new(rusqlite::Connection::open_in_memory().unwrap());
        conn.execute_batch(
            "CREATE TABLE t (k TEXT NOT NULL PRIMARY KEY, ord INTEGER NOT NULL);
             CREATE INDEX ix ON t (ord);",
        )
        .unwrap();
        let ts = TableSource::new(conn.clone(), "t", cols_k_ord(), vec![0]);
        conn.execute_batch(&format!(
            "INSERT INTO t(k, ord)
             WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c WHERE n < {rows})
             SELECT 'k' || n, {ord_expr} FROM c;
             ANALYZE;"
        ))
        .unwrap();
        ts
    }

    #[test]
    fn gate_bypasses_low_cardinality_after_analyze() {
        // One distinct `ord` over 5000 rows ⇒ a single ~5000-row run, above the cap:
        // the structural verdict engages, but the stat gate bypasses.
        let ts = analyzed_kv(5000, "1");
        assert_eq!(ts.meta.tiebreak_prefix_len(&ord_then_pk()), Some(1));
        assert!(
            !ts.prefix_run_within_cap(&ord_then_pk(), 1),
            "a low-cardinality run above the cap must bypass"
        );
    }

    #[test]
    fn gate_engages_high_cardinality_after_analyze() {
        // Distinct `ord` per row ⇒ runs of ~1: engage.
        let ts = analyzed_kv(5000, "n");
        assert!(ts.prefix_run_within_cap(&ord_then_pk(), 1));
    }

    #[test]
    fn gate_engages_when_unanalyzed() {
        // No ANALYZE ⇒ no sqlite_stat1 row ⇒ engage (today's behavior; ANALYZE is what
        // makes the bypass fire for the tables where it matters).
        let ts = source(
            "CREATE TABLE t (k TEXT NOT NULL PRIMARY KEY, ord INTEGER NOT NULL);
             CREATE INDEX ix ON t (ord);",
            cols_k_ord(),
            vec![0],
        );
        assert!(ts.prefix_run_within_cap(&ord_then_pk(), 1));
    }

    /// The unconstrained guard (design 204 review): a fetch carrying a constraint /
    /// multiConstraint bypasses even where the structural + stat verdict would engage,
    /// so a join child's FK-equality fetch can't be steered onto a bad plan.
    #[test]
    fn bypasses_constrained_fetch() {
        let ts = source(
            "CREATE TABLE t (k TEXT NOT NULL PRIMARY KEY, ord INTEGER NOT NULL);
             CREATE INDEX ix ON t (ord);",
            cols_k_ord(),
            vec![0],
        );
        let sort = ord_then_pk();

        // Unconstrained ordered scan (un-ANALYZE'd ⇒ stat gate engages): the win.
        assert_eq!(
            ts.tiebreak_engage(&FetchRequest::all(), &sort, false),
            Some(1)
        );

        // A single constraint (a join child's FK equality): bypass.
        let constrained = FetchRequest::with_constraint(vec![(1, OwnedValue::Float(1.0))]);
        assert_eq!(ts.tiebreak_engage(&constrained, &sort, false), None);

        // A non-empty multiConstraint (FlippedJoin IN-batch): bypass.
        let multi = FetchRequest {
            multi_constraints: vec![vec![vec![(1, OwnedValue::Float(1.0))]]],
            ..Default::default()
        };
        assert_eq!(ts.tiebreak_engage(&multi, &sort, false), None);

        // An *empty* multiConstraint entry is not a constraint (`has_multi` false): engage.
        let empty_multi = FetchRequest {
            multi_constraints: vec![vec![]],
            ..Default::default()
        };
        assert_eq!(ts.tiebreak_engage(&empty_multi, &sort, false), Some(1));

        // Unordered bypasses regardless.
        assert_eq!(ts.tiebreak_engage(&FetchRequest::all(), &sort, true), None);
    }
}

/// The leaf's reported schema carries the declared column types (design 226 §4.1,
/// Stage C1): `TableSource`'s default reported [`Schema`] derives `column_types`
/// from its `ColumnDef`s, so the builder's `resolve` seam sees the same types the
/// SQLite value boundary enforces. (The `new_with_schema` path instead trusts the
/// caller's `SourceSchema` — the replica installs its discovered types there.)
#[cfg(test)]
mod reported_schema_types {
    use super::*;

    #[test]
    fn default_reported_schema_derives_types_from_column_defs() {
        let conn = Rc::new(rusqlite::Connection::open_in_memory().unwrap());
        conn.execute_batch(
            "CREATE TABLE t (id INTEGER NOT NULL PRIMARY KEY, title TEXT, open BOOLEAN, meta JSON);",
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
                optional: true,
            },
            ColumnDef {
                name: "open".into(),
                ty: ValueType::Boolean,
                optional: true,
            },
            ColumnDef {
                name: "meta".into(),
                ty: ValueType::Json,
                optional: true,
            },
        ];
        let ts = TableSource::new(conn, "t", cols, vec![0]);
        assert_eq!(
            ts.meta.schema.column_types,
            vec![
                ValueType::Number,
                ValueType::String,
                ValueType::Boolean,
                ValueType::Json,
            ]
        );
    }
}
