//! The prepared-statement cache (spec `05` §4.3) — **a thin wrapper over
//! rusqlite's built-in per-connection statement cache**, not a from-scratch port
//! of `internal/statement-cache.ts`.
//!
//! ## Why wrap instead of port
//!
//! The JS `StatementCache` exists to solve one problem: a SQLite prepared
//! statement **is not reentrant** — a single statement cannot be iterated by two
//! callers at once (`statement-cache.ts:16-20`). The JS solves it by *removing* a
//! statement from the cache on `get` and pushing it back on `return`; a
//! concurrent `get` for the same SQL (the self-join / reentrant-fetch case, `05`
//! §3.10) prepares a *fresh* statement so both callers iterate independently.
//!
//! rusqlite ships **exactly this discipline already** (`rusqlite::cache`):
//! - [`Connection::prepare_cached`] does `cache.remove(sql)` — the statement is
//!   *out of the cache while in use*, so a concurrent same-SQL `get` cannot alias
//!   it and prepares a fresh one. This is the JS "remove on get" semantics.
//! - The returned [`rusqlite::CachedStatement`] returns itself to the cache on
//!   `Drop` — RAII return, **strictly better** than the JS manual
//!   `finally → cache.return()` discipline (`table-source.ts:340-374`): an early
//!   `break`, a `?`, or early return all return the statement for free (foundations
//!   §7, README Primitive #2). This cache is `sqlite`-only (native server): a panic
//!   returns the statement only in unwinding builds; the `release-server` profile
//!   (`panic = "unwind"`) covers panic, but a plain `release` build (`panic = "abort"`)
//!   aborts the process and no destructor runs. See WS02.
//!
//! Critically, the built-in cache **dissolves the spec's #1 implementation risk**
//! (`05` §13 Q1, the `rusqlite::Statement<'static>` self-referential-struct
//! problem): it stores a lifetime-free `RawStatement` (a raw FFI pointer) and
//! re-attaches the connection borrow only for the checkout window. That raw
//! mechanism is `pub(crate)` in rusqlite, so a faithful multi-slot port could
//! *not* reuse it — it would have to reach for an `unsafe` `'static` transmute or
//! a self-referencing dependency (`ouroboros`), i.e. take on the exact risk the
//! spec flagged. Standing on `prepare_cached` is the lower-risk path.
//!
//! ## Deviations from the JS cache (noted, per `05` §13 Q7)
//!
//! The spec explicitly says *do not silently change the eviction policy and then
//! assume the same eviction order in tests*. The deviations here are deliberate:
//!
//! 1. **Single statement per SQL key, LRU-evicted** — not the JS
//!    `Map<sql, Statement[]>` (multiple cached copies per SQL for concurrent
//!    reuse). Under a reentrant self-join where the outer and inner fetch emit
//!    *identical* SQL (constraint values are bound params, `05` §4.4, so the text
//!    is identical), the single slot re-prepares one extra statement per reentrant
//!    cycle and finalizes the surplus on return. A real but modest hot-path cost,
//!    measurable at the §11 benchmarking milestone; if it bites, the upgrade to a
//!    multi-slot front is local to this module. We do **not** pre-optimize into
//!    the §13 Q1 unsafe territory before a benchmark justifies it.
//! 2. **LRU eviction (bounded), not manual `drop(n)`** — the JS cache is unbounded
//!    with an externally-driven `drop(n)` (`statement-cache.ts:53-69`). rusqlite's
//!    cache is a bounded LRU; [`StatementCache::set_capacity`] tunes the bound.
//!    This resolves §13 Q7 (the memory-footprint risk) without a pipeline-driver
//!    hook. Eviction *order* differs from the JS — any differential test that
//!    asserted the JS FIFO-ish trim order must be rewritten against LRU.
//! 3. **No internal whitespace normalization on the hot path** — the JS
//!    `normalizeWhitespace` (`statement-cache.ts:129`) collapses internal
//!    whitespace runs so hand-written SQL hits the same key. *All* fetch SQL here
//!    is emitted by `build_select_query` (`05` §4.4) in canonical single-spaced
//!    form, so normalization would be a wasted per-`get` allocation on the hot
//!    path. The cache assumes canonical SQL; rusqlite still trims leading/trailing
//!    whitespace for the key. (If a future caller feeds non-canonical SQL,
//!    normalize once when *building* it, not on every cache hit.)

use std::cell::Cell;
use std::ops::{Deref, DerefMut};
use std::rc::Rc;

use rusqlite::{CachedStatement, Connection, Statement};

/// rusqlite's default per-connection statement-cache capacity (LRU slots). We
/// adopt it unless [`StatementCache::with_capacity`] overrides — a server with
/// many distinct fetch shapes may want a larger bound (`05` §13 Q7).
pub const DEFAULT_CAPACITY: usize = 16;

/// The prepared-statement cache for a SQLite source (`05` §4.3). Owns the
/// connection (`Rc<Connection>`, mirroring the spec's `Rc<Db>` so the cache can
/// live alongside the per-snapshot write statements and be rebound on a
/// Snapshotter leapfrog, `05` §5.7) and lends out [`PooledStmt`] RAII guards.
///
/// The *actual* statement storage is rusqlite's per-connection cache, reached via
/// [`Connection::prepare_cached`]; this type adds (1) the `PooledStmt` checkout
/// handle — the single place to hang the `scanStatus` capture-on-drop hook (`05`
/// §13 Q8, deferred) — and (2) open-cursor accounting (README Primitive #2), so a
/// test can prove the statement was released even on an early `break`.
pub struct StatementCache {
    conn: Rc<Connection>,
    /// Cursors currently checked out. Incremented by [`Self::get`], decremented by
    /// [`PooledStmt`]'s `Drop`. The Rust analogue of the JS `LoggingIterableIterator`
    /// open-iterator tracking (`db.ts:304-308`); makes RAII release *observable*.
    open: Cell<i64>,
}

impl StatementCache {
    /// Wrap `conn` with the default LRU capacity ([`DEFAULT_CAPACITY`]).
    pub fn new(conn: Rc<Connection>) -> StatementCache {
        StatementCache {
            conn,
            open: Cell::new(0),
        }
    }

    /// Wrap `conn` and set the LRU capacity (number of distinct cached SQL
    /// statements retained). `0` disables caching (every `get` prepares fresh).
    pub fn with_capacity(conn: Rc<Connection>, capacity: usize) -> StatementCache {
        conn.set_prepared_statement_cache_capacity(capacity);
        StatementCache {
            conn,
            open: Cell::new(0),
        }
    }

    /// Re-bound the LRU capacity. Resolves `05` §13 Q7 (the server-side
    /// memory-footprint bound) without the JS external `drop(n)`.
    pub fn set_capacity(&self, capacity: usize) {
        self.conn.set_prepared_statement_cache_capacity(capacity);
    }

    /// Finalize every cached statement (drops the LRU contents). The bounded
    /// analogue of the JS `drop(size)`; e.g. on schema change before a re-prepare.
    pub fn flush(&self) {
        self.conn.flush_prepared_statement_cache();
    }

    /// The wrapped connection — needed by the source to prepare the *write*
    /// statements (insert/delete/update/checkExists/getExisting, `05` §4.3) that
    /// are held for a snapshot's lifetime rather than cache-checked-out per fetch.
    pub fn conn(&self) -> &Rc<Connection> {
        &self.conn
    }

    /// Cursors currently checked out (Primitive #2 accounting). `0` when every
    /// `PooledStmt` has been dropped — i.e. no statement is mid-iteration and the
    /// connection is free for a write.
    pub fn open_cursors(&self) -> i64 {
        self.open.get()
    }

    /// Check out a prepared statement for `sql`, **removed from the cache while in
    /// use** (a SQLite statement is not reentrant — `05` §3.10). Returns a
    /// [`PooledStmt`] RAII guard that returns the statement to the cache on `Drop`.
    ///
    /// A concurrent `get` for the same `sql` while this one is checked out finds
    /// the slot empty and prepares a *fresh* statement (rusqlite `cache.rs:147`),
    /// so the two iterate independently — the self-join / reentrant-fetch case.
    ///
    /// Fails only if `sql` is not valid SQL on this connection — a builder bug in
    /// practice, since fetch SQL is emitted by `build_select_query` (`05` §4.4).
    /// Surfaced as a `Result` rather than hidden (foundations §10).
    pub fn get(&self, sql: &str) -> rusqlite::Result<PooledStmt<'_>> {
        let stmt = self.conn.prepare_cached(sql)?;
        self.open.set(self.open.get() + 1);
        Ok(PooledStmt {
            stmt: Some(stmt),
            open: &self.open,
        })
    }

    /// Check out a statement, run `f` against it, and return it to the cache —
    /// the port of the JS `StatementCache.use` (`statement-cache.ts:103-110`) and
    /// the `StatementRunner` one-liners (`zero-cache/db/statements.ts`). The
    /// statement is released when `f` returns (or unwinds), automatically.
    pub fn with<T>(
        &self,
        sql: &str,
        f: impl FnOnce(&mut PooledStmt<'_>) -> T,
    ) -> rusqlite::Result<T> {
        let mut stmt = self.get(sql)?;
        Ok(f(&mut stmt))
    }
}

/// RAII handle owning a checked-out prepared statement (`05` §4.3, the
/// `PooledStmt`). On `Drop` it returns the statement to the per-connection cache
/// — which also **resets the cursor**, so the next write does not hit "database
/// is busy" — and decrements the open-cursor count. This replaces *both* the JS
/// `get`/`finally → cache.return()` and the `rowIterator.return?.()` cursor-reset
/// (`table-source.ts:340-374`); early `break`, `?`, and early return all trigger
/// it. A panic triggers it only in unwinding builds (this is `sqlite`-only server
/// code): the `release-server` profile (`panic = "unwind"`) covers panic, but a plain
/// `release` build (`panic = "abort"`) aborts and the `Drop` does not run. See WS02.
///
/// Deref/DerefMut expose the inner [`rusqlite::Statement`], so a caller does
/// `pooled.query(params)` exactly as on a bare statement (mirrors rusqlite's own
/// `CachedStatement`).
pub struct PooledStmt<'c> {
    /// `Option` so [`Self::discard`] can take the statement out *without*
    /// returning it to the cache. `None` after discard; `Drop` then returns
    /// nothing (the statement is finalized) but still decrements `open`.
    stmt: Option<CachedStatement<'c>>,
    open: &'c Cell<i64>,
}

impl<'c> PooledStmt<'c> {
    /// Drop the statement **without** returning it to the cache (rusqlite
    /// `CachedStatement::discard`). Use when a statement should not be reused —
    /// e.g. it errored, or the schema changed under it. The JS analogue is simply
    /// *not* calling `return` ("It is not an error to fail to call return",
    /// `statement-cache.ts:30-32`). The open-cursor count is still decremented
    /// (by `Drop`), so accounting stays balanced.
    pub fn discard(mut self) {
        if let Some(stmt) = self.stmt.take() {
            stmt.discard();
        }
        // `self` drops here → `Drop` decrements `open`; `stmt` is already `None`.
    }
}

impl<'c> Deref for PooledStmt<'c> {
    type Target = Statement<'c>;

    fn deref(&self) -> &Statement<'c> {
        // Deref-coerces &CachedStatement → &Statement. `expect` cannot fire on the
        // data path: `stmt` is `None` only after `discard`, which consumes `self`.
        self.stmt.as_ref().expect("PooledStmt used after discard")
    }
}

impl<'c> DerefMut for PooledStmt<'c> {
    fn deref_mut(&mut self) -> &mut Statement<'c> {
        self.stmt.as_mut().expect("PooledStmt used after discard")
    }
}

impl Drop for PooledStmt<'_> {
    fn drop(&mut self) {
        // scanStatus capture hook point (`05` §7 / §13 Q8): when a `DebugDelegate`
        // is active, read `sqlite3_stmt_scanstatus_v2` off the statement here,
        // before it returns to the cache. Observability-only, so deferred — the
        // data path does no work.
        //
        // Assigning `None` drops the inner `CachedStatement` *now*, which returns
        // it to the per-connection cache (and resets the cursor). On the `discard`
        // path `stmt` is already `None` — nothing to return.
        //
        // REENTRANCY SAFETY: this return goes through rusqlite's
        // `StatementCache::cache_stmt`, which takes a `RefCell::borrow_mut` — but
        // only *transiently* (around a single `insert`), never held across a vend.
        // Likewise checkout (`get` → `prepare_cached`) borrows the RefCell only to
        // `remove`, then releases. So a `PooledStmt` dropping while ANOTHER is
        // mid-iteration (the self-join / nested-checkout case, `05` §3.10) never
        // double-borrows: the live statement was already *removed* from the cache,
        // so it isn't borrowing the RefCell at all. This holds only as long as the
        // foundations §6.2 rule is upheld — never hold a cache borrow across a row
        // vend. A future refactor that did so would turn this Drop into an
        // `already borrowed` panic; keep checkout/return windows transient.
        self.stmt = None;
        self.open.set(self.open.get() - 1);
    }
}
