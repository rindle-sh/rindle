//! Phase 2 — the **SQLite leaf**: a zero-copy lending [`RowStream`] over a real
//! `rusqlite` cursor. This is the hardest backend for the canonical value model
//! (`value.rs`) to satisfy, and the reason the model is designed *here* rather
//! than in the abstract: a SQLite statement has **no row** — only a prepared
//! statement, a `step`, and `column_*(i)` accessors whose text/blob pointers are
//! valid *only until the next `step`/`reset`*. The borrow checker turns a
//! use-after-step into a **compile error**, and forces an owned copy at exactly
//! the points the JS already copies.
//!
//! What this proves (the spike's exit criteria for the SQLite side):
//!
//! 1. **Zero copy on the transient path, including strings.** `RowRef::col(i)`
//!    is a `rusqlite` `get_ref` — a `ValueRef` borrowing the step buffer — mapped
//!    to a `Value<'_>` with **no allocation** (`Str`/`Json` are `&[u8]` straight
//!    into SQLite's buffer). A filtered/pass-through scan that reads, compares,
//!    and drops rows allocates **nothing per row** (proven by the counting
//!    allocator in `tests/sqlite_zero_copy.rs`).
//! 2. **`to_owned_row()` is the one forced per-row copy**, at the Node boundary —
//!    the single point where a value must outlive its step. Same trait method
//!    the memory backend implements as an `Arc` bump (`btree.rs`); here it copies.
//! 3. **Step fallibility resolved (handoff decision 1 / `05` OQ-9):** `next_row`
//!    stays infallible (uniform with the never-erroring memory backend); a
//!    `sqlite3_step` error is **parked** on the stream and **re-raised at the
//!    first owning boundary** via [`SqliteRowStream::take_error`] — NEVER mapped
//!    to silent end-of-stream.
//! 4. **RAII statement cleanup (Primitive #2):** dropping the stream drops the
//!    `Rows` cursor (rusqlite resets the statement), so the next write does not
//!    hit "database is busy". [`StmtGuard`] makes that release observable — and
//!    is where a real prepared-statement pool returns its `PooledStmt`.

use std::cell::Cell;
use std::rc::Rc;

use rusqlite::types::ValueRef;

use rindle::error::RindleError;
use rindle::value::{ColId, OwnedRow, RowRef, RowStream, Value};

/// Per-column type tag, resolved once from the schema at build time
/// (foundations §4). Makes value conversion ty-directed: an INTEGER storage
/// class becomes `Int` or `Bool` depending on the column, TEXT becomes `Str` or
/// (unparsed) `Json`. The hot path never inspects a column *name*.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ColType {
    Int,
    Float,
    Bool,
    Text,
    Json,
}

/// A parked error, surfaced at the first owning/dyn boundary. Carries either a
/// `sqlite3_step` failure or a value-conversion failure; the production
/// `RindleError` (foundations §10) wraps the same set.
#[derive(Debug)]
pub enum SqliteError {
    /// A `sqlite3_step` error (I/O, corruption, an expression-evaluation error
    /// such as integer overflow) raised while advancing the cursor.
    Step(rusqlite::Error),
    /// A vended value cannot be represented (`UnsupportedValueError`): currently
    /// a `number`-column integer that cannot round-trip through `f64` without
    /// precision loss. UTF-8 failures are surfaced directly as
    /// [`RindleError::InvalidUtf8`] at the row-materialization boundary.
    UnsupportedValue(String),
}

impl From<SqliteError> for RindleError {
    fn from(value: SqliteError) -> RindleError {
        match value {
            SqliteError::Step(source) => RindleError::sqlite("step", source),
            SqliteError::UnsupportedValue(message) => RindleError::unsupported_value(message),
        }
    }
}

/// True when widening `i` to `f64` and narrowing it back preserves the exact
/// integer. This deliberately accepts sparse, exactly representable integers above
/// 2^53 (such as 2^54) while rejecting adjacent values that would lose precision.
///
/// The explicit upper-bound check handles `i64::MAX`: it rounds up to 2^63 as an
/// `f64`, then Rust's saturating float-to-integer cast would otherwise appear to
/// round-trip back to `i64::MAX`. `i64::MIN` is exactly -2^63 and is accepted.
#[inline]
pub fn is_exact_f64_integer(i: i64) -> bool {
    let widened = i as f64;
    widened < i64::MAX as f64 && widened as i64 == i
}

/// Build `SELECT <c0>, <c1>, … FROM <table>` projecting the declared columns in
/// `ColId` order. Result-row order == `columns` order == `ColId`, which is what
/// makes `RowRef::col(i)` an O(1) array index (`05` §8.1). (The full
/// constraint/start/filter `WHERE` lowering is `05` §4.4; out of scope here —
/// the spike threads its own predicates.)
pub fn select_sql(table: &str, columns: &[&str]) -> String {
    let mut sql = String::from("SELECT ");
    for (i, c) in columns.iter().enumerate() {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push_str(c);
    }
    sql.push_str(" FROM ");
    sql.push_str(table);
    sql
}

/// RAII cursor accounting (Primitive #2). Mirrors the JS `finally`/`.return()`
/// that resets+returns the prepared statement so the **next write** doesn't hit
/// "database is busy". `rusqlite::Rows` already resets the statement on `Drop`;
/// this guard makes the release *observable* (and is where a real pool would
/// return its `PooledStmt`). Decrements its counter on `Drop` — on normal end,
/// early `break`, `?`, or early return — for free, no `finally` needed. This is a
/// `sqlite`-only (native server) path: run the server with the `release-server`
/// profile (`panic = "unwind"`), where `Drop` also runs on panic; under a plain
/// `release` build (`panic = "abort"`) a panic aborts the process and no destructor
/// runs, so panic balance holds only in unwinding builds (`release-server`,
/// `cargo test`). See WS02.
pub struct StmtGuard(Rc<Cell<i64>>);

impl StmtGuard {
    pub fn new(open: Rc<Cell<i64>>) -> StmtGuard {
        open.set(open.get() + 1);
        StmtGuard(open)
    }
}

impl Drop for StmtGuard {
    fn drop(&mut self) {
        self.0.set(self.0.get() - 1);
    }
}

/// The leaf row stream: a live rusqlite cursor (`Rows`) plus the per-column type
/// tags. Implements the foundations lending [`RowStream`]; each `next_row`
/// reborrows `self`, so the returned [`SqliteRow`] is invalidated by the next
/// call — the SQLite cursor contract, now a **compile-time** invariant.
///
/// `'s` is the lifetime of the prepared statement the cursor borrows. The
/// statement is owned **elsewhere** (the caller / a pool), which sidesteps the
/// self-referential-struct problem (`05` §13 Q1): a field borrowing another
/// field of the same struct is not expressible in safe Rust, so the statement
/// lives outside and the stream borrows it for `'s`.
///
/// Field order is load-bearing: `rows` (the cursor) is declared before `_guard`
/// so it drops first — the cursor is released *before* the "returned to pool"
/// guard fires.
pub struct SqliteRowStream<'s> {
    rows: rusqlite::Rows<'s>,
    types: &'s [ColType],
    /// A step error parked here by `next_row` (which must return `Option`),
    /// re-raised at the owning boundary via [`Self::take_error`]. NEVER swallowed.
    parked_err: Option<SqliteError>,
    /// RAII release accounting (Primitive #2); `None` when the caller doesn't
    /// track open cursors.
    _guard: Option<StmtGuard>,
}

impl<'s> SqliteRowStream<'s> {
    /// Wrap a `rusqlite::Rows` cursor (from `stmt.query(params)`) and its column
    /// type tags.
    pub fn new(rows: rusqlite::Rows<'s>, types: &'s [ColType]) -> SqliteRowStream<'s> {
        SqliteRowStream {
            rows,
            types,
            parked_err: None,
            _guard: None,
        }
    }

    /// Same, but with a Primitive-#2 release guard (the open-cursor counter is
    /// decremented when this stream drops, including on early termination).
    pub fn guarded(
        rows: rusqlite::Rows<'s>,
        types: &'s [ColType],
        guard: StmtGuard,
    ) -> SqliteRowStream<'s> {
        SqliteRowStream {
            rows,
            types,
            parked_err: None,
            _guard: Some(guard),
        }
    }

    /// Re-raise the parked step error at the owning boundary. The node-stream /
    /// `fetch` layer calls this after the scan drains (where the result type is
    /// already fallible — `09`); a leftover `Some` here means the scan hit a
    /// `sqlite3_step` error that must propagate, NOT a clean end-of-stream.
    pub fn take_error(&mut self) -> Option<SqliteError> {
        self.parked_err.take()
    }
}

impl<'s> RowStream for SqliteRowStream<'s> {
    type Row<'a>
        = SqliteRow<'a>
    where
        Self: 'a;

    fn next_row(&mut self) -> Option<Self::Row<'_>> {
        // `Rows::next` advances the cursor and returns `Result<Option<&Row>>`;
        // the `&Row` borrows the step buffers (and so cannot outlive the next
        // `next_row`). Conversion is LAZY (per-cell, in `col`), so a pass-through
        // scan converts/copies nothing.
        //
        // A `sqlite3_step` error must NOT be silently mapped to end-of-stream
        // (the JS `iterate()` generator throws, and `#fetch`'s `finally` still
        // closes the cursor). Since `RowStream::next_row` returns `Option`, the
        // error is PARKED on `self` and re-raised at the owning boundary
        // (`take_error`). DO NOT `.ok()?` it.
        match self.rows.next() {
            Ok(Some(row)) => Some(SqliteRow {
                row,
                types: self.types,
            }),
            Ok(None) => None,
            Err(e) => {
                self.parked_err = Some(SqliteError::Step(e));
                None
            }
        }
    }
}

/// One borrowed SQLite row. `col(i)` reads column `i` lazily and **zero-copy**:
/// a `Str`/`Json` cell is a `&[u8]` pointing straight into SQLite's step buffer
/// (no allocation, no UTF-8 validation — bytewise `BINARY` compare on the hot
/// path; validate at the `to_owned` escape).
pub struct SqliteRow<'a> {
    row: &'a rusqlite::Row<'a>,
    types: &'a [ColType],
}

impl<'a> SqliteRow<'a> {
    /// Wrap a borrowed rusqlite row + its column type tags. Used by the
    /// production owning cursor (`table_source::OwnedSqliteRows`), which — unlike
    /// the borrow-the-caller's-statement [`SqliteRowStream`] — owns the whole
    /// conn→stmt→cursor chain and constructs each `SqliteRow` itself.
    #[inline]
    pub fn new(row: &'a rusqlite::Row<'a>, types: &'a [ColType]) -> SqliteRow<'a> {
        SqliteRow { row, types }
    }
}

impl RowRef for SqliteRow<'_> {
    fn col(&self, c: ColId) -> Value<'_> {
        // `get_ref_unwrap` is INFALLIBLE by construction: the SELECT projects
        // exactly `types.len()` columns in ColId order, and `c < types.len()` is
        // a build-time invariant (foundations §3.1), so the only `get_ref` error
        // (column-index-out-of-range) cannot occur. A bad `c` is a builder bug,
        // not a runtime error path.
        //
        // NB: a returned `Str`/`Json` borrows the step buffer — valid only until
        // the next `next_row` (the lending contract, enforced by the lifetime).
        match (self.types[c], self.row.get_ref_unwrap(c)) {
            (_, ValueRef::Null) => Value::Null,
            (ColType::Bool, ValueRef::Integer(i)) => Value::Bool(i != 0),
            (ColType::Int, ValueRef::Integer(i)) => Value::Int(i),
            (ColType::Float, ValueRef::Real(f)) => Value::Float(f),
            // A `number`-typed column stored as INTEGER widens to f64. The owning
            // production cursor checks lossless round-tripping before constructing
            // this row; this primitive's borrowed test cursor cannot park an error
            // from its infallible `col(&self)` method.
            (ColType::Float, ValueRef::Integer(i)) => Value::Float(i as f64),
            (ColType::Text, ValueRef::Text(b)) => Value::Str(b), // zero-copy bytes
            (ColType::Json, ValueRef::Text(b)) => Value::Json(b), // unparsed, borrowed
            // A (ty, storage-class) pair the schema forbids is a builder bug; we
            // pass it through deterministically rather than silently coerce.
            (_, ValueRef::Text(b)) | (_, ValueRef::Blob(b)) => Value::Str(b),
            (_, ValueRef::Integer(i)) => Value::Int(i),
            (_, ValueRef::Real(f)) => Value::Float(f),
        }
    }

    fn len(&self) -> usize {
        self.types.len()
    }

    /// Fallible forced per-row copy — **one flat allocation** for the whole row
    /// (`205-FLAT-ROW-SINGLE-BUFFER-DESIGN.md`): the two-pass builder sizes the
    /// buffer, validates TEXT/JSON UTF-8 (same eager timing as the old per-cell
    /// `try_to_owned`), then writes cells in place. Invalid bytes become a runtime
    /// error instead of a panic; lossy integer-to-f64 conversions are parked before
    /// this point by the owning SQLite cursor.
    fn try_to_owned_row(&self) -> Result<OwnedRow, RindleError> {
        OwnedRow::try_from_row_ref(self)
    }

    /// Infallible compatibility wrapper for direct tests and memory-equivalent
    /// trait users. Production SQLite source code uses [`Self::try_to_owned_row`].
    fn to_owned_row(&self) -> OwnedRow {
        self.try_to_owned_row()
            .expect("SQLite row could not be materialized")
    }
}

#[cfg(test)]
mod exact_f64_integer_tests {
    use super::is_exact_f64_integer;

    #[test]
    fn accepts_exact_sparse_values_and_rejects_precision_loss() {
        for value in [
            i64::MIN,
            -(1_i64 << 54),
            -(1_i64 << 53),
            0,
            1_i64 << 53,
            1_i64 << 54,
            i64::MAX - 1023,
        ] {
            assert!(
                is_exact_f64_integer(value),
                "expected {value} to round-trip"
            );
        }

        for value in [
            i64::MIN + 1,
            -((1_i64 << 53) + 1),
            (1_i64 << 53) + 1,
            i64::MAX,
        ] {
            assert!(
                !is_exact_f64_integer(value),
                "expected {value} to lose precision"
            );
        }
    }
}
