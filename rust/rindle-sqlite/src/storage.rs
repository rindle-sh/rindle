//! Spec `10` — **SQLite-backed operator scratch state** (the server spill path).
//!
//! This is the `#[cfg(feature = "sqlite")]` half of the former `rindle::storage` module,
//! lifted into `rindle-sqlite`: `DatabaseStorage` / `OpStorage` over rusqlite, plus the
//! `StorageValue` blob codec. The backend-agnostic `Storage` / `StorageValue` /
//! `StorageProvider` / `MemoryStorage` / `StorageFactory` types stay in `rindle::storage`.
//!
//! Each engine gets its own [`DatabaseStorage`] (a private temp-file DB), so a single
//! database hosts exactly one set of operators. Every operator is handed a unique,
//! auto-incrementing `op_id` and all of them share one `storage(op, key) -> val` table —
//! no per-tenant/client-group column is needed. `OpStorage` implements
//! [`rindle::storage::Storage`]; `DatabaseStorage` implements
//! [`rindle::storage::StorageProvider`], so a graph built with
//! `StorageFactory::custom(Rc::new(database_storage))` spills operator state to SQLite.

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::Arc;

use rusqlite::{params, OptionalExtension};

use rindle::error::RindleError;
use rindle::storage::{ReduceAcc, Storage, StorageProvider, StorageValue};
use rindle::value::{owned_row, OwnedRow, OwnedValue};

// ---------------------------------------------------------------------------
// DatabaseStorage (server) — spec 10 §4.4 / §5.2
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
pub struct DatabaseStorageOptions {
    /// Number of point operations (`set`/`get`/`del`, matching the JS behavior)
    /// before checkpointing the current scratch-state transaction.
    pub commit_interval: u32,
    /// Reserved for the production incremental-vacuum threshold. The current
    /// implementation clears rows correctly but does not run compaction yet.
    pub compaction_threshold_bytes: u64,
}

impl Default for DatabaseStorageOptions {
    fn default() -> DatabaseStorageOptions {
        DatabaseStorageOptions {
            commit_interval: 5_000,
            compaction_threshold_bytes: 50 * 1024 * 1024,
        }
    }
}

#[derive(Clone)]
pub struct DatabaseStorage {
    inner: Rc<DatabaseStorageInner>,
}

struct DatabaseStorageInner {
    conn: rusqlite::Connection,
    opts: DatabaseStorageOptions,
    num_writes: Cell<u32>,
    /// Monotonic operator-id allocator. Each operator in this database gets a unique
    /// `op_id` (its keyspace inside the shared `storage` table); ids are never reused,
    /// so a torn-down-then-rebuilt operator can never collide with its former rows.
    next_op_id: Cell<i64>,
}

impl DatabaseStorage {
    pub fn new_in_memory() -> rusqlite::Result<DatabaseStorage> {
        DatabaseStorage::new(rusqlite::Connection::open_in_memory()?)
    }

    /// Open operator storage over a **private, on-disk temporary database** — SQLite's
    /// `""` filename: a per-connection scratch DB created in the temp directory and
    /// deleted automatically when the connection closes. This is the server **spill
    /// path**: operator state can grow past RAM and overflow to disk, but it keeps the
    /// durability-free scratch pragmas (`journal_mode = OFF`, `synchronous = OFF`,
    /// `locking_mode = EXCLUSIVE`), so there are no journal writes or `fsync`s on the
    /// hot path — pages only reach disk when the OS page cache evicts them.
    pub fn new_temp_file() -> rusqlite::Result<DatabaseStorage> {
        DatabaseStorage::new(rusqlite::Connection::open("")?)
    }

    pub fn new(conn: rusqlite::Connection) -> rusqlite::Result<DatabaseStorage> {
        DatabaseStorage::with_options(conn, DatabaseStorageOptions::default())
    }

    pub fn with_options(
        conn: rusqlite::Connection,
        opts: DatabaseStorageOptions,
    ) -> rusqlite::Result<DatabaseStorage> {
        // Query Planner Stability Guarantee (see rindle-replica `parallel::open_wal2`): the shared
        // `storage(op, key) -> val` table is hit by cached point ops re-bound with a fresh key on
        // every set/get/del. QPSG keeps their plans value-independent so a re-bound seek is never
        // re-parsed by this bedrock-SQLite build. Set at open, before `init` prepares anything or
        // opens its `BEGIN`.
        conn.set_db_config(
            rusqlite::config::DbConfig::SQLITE_DBCONFIG_ENABLE_QPSG,
            true,
        )?;
        let db = DatabaseStorage {
            inner: Rc::new(DatabaseStorageInner {
                conn,
                opts,
                num_writes: Cell::new(0),
                next_op_id: Cell::new(1),
            }),
        };
        db.init()?;
        Ok(db)
    }

    /// Allocate one operator's isolated keyspace (a fresh `op_id`) over this database.
    /// The store gets its own (undrained) error sink — for direct use outside a graph
    /// (tests, benchmarks). Inside a graph the [`StorageProvider`] seam threads the
    /// graph's `runtime_error` sink in via [`create_storage_with_sink`].
    ///
    /// [`create_storage_with_sink`]: rindle::storage::StorageProvider::create_storage_with_sink
    pub fn create_storage(&self) -> OpStorage {
        self.op_storage_with_sink(Rc::default())
    }

    fn op_storage_with_sink(&self, error_sink: Rc<RefCell<Option<RindleError>>>) -> OpStorage {
        let op_id = self.inner.next_op_id.get();
        self.inner.next_op_id.set(op_id + 1);
        OpStorage {
            db: self.clone(),
            op_id,
            error_sink,
        }
    }

    pub fn checkpoint(&self) -> rusqlite::Result<()> {
        self.conn().execute_batch("COMMIT; BEGIN;")?;
        self.inner.num_writes.set(0);
        Ok(())
    }

    fn conn(&self) -> &rusqlite::Connection {
        &self.inner.conn
    }

    fn init(&self) -> rusqlite::Result<()> {
        self.conn().execute_batch(
            r#"
            PRAGMA journal_mode = OFF;
            PRAGMA synchronous = OFF;
            PRAGMA temp_store = MEMORY;
            PRAGMA locking_mode = EXCLUSIVE;
            CREATE TABLE IF NOT EXISTS storage (
                "op"  INTEGER NOT NULL,
                "key" TEXT NOT NULL,
                "val" BLOB NOT NULL,
                PRIMARY KEY("op", "key")
            ) WITHOUT ROWID;
            BEGIN;
            "#,
        )
    }

    fn maybe_checkpoint(&self) -> rusqlite::Result<()> {
        let interval = self.inner.opts.commit_interval;
        if interval == 0 {
            return Ok(());
        }
        let next = self.inner.num_writes.get().saturating_add(1);
        if next >= interval {
            self.checkpoint()?;
        } else {
            self.inner.num_writes.set(next);
        }
        Ok(())
    }
}

impl Drop for DatabaseStorageInner {
    fn drop(&mut self) {
        let _ = self.conn.execute_batch("COMMIT;");
    }
}

/// The graph's `StorageFactory` vends one [`OpStorage`] per stateful operator straight
/// off the database, threading the graph's `runtime_error` sink in (WS02.3) so storage
/// failures surface via `take_runtime_error` at the mutation boundary instead of aborting.
impl StorageProvider for DatabaseStorage {
    fn create_storage_with_sink(
        &self,
        error_sink: Rc<RefCell<Option<RindleError>>>,
    ) -> Box<dyn Storage> {
        Box::new(self.op_storage_with_sink(error_sink))
    }
}

/// One operator's namespaced scratch map inside the shared SQLite `storage` table,
/// keyed by this operator's unique `op_id`.
pub struct OpStorage {
    db: DatabaseStorage,
    op_id: i64,
    /// Where transient SQLite / decode errors are parked (WS02.3, spec 10 E14): the
    /// `Storage` trait is infallible by design, so an op returns a safe sentinel
    /// (None / empty / no-op) and parks the typed error here. The graph drains it via
    /// `take_runtime_error` at the mutation boundary, so the process never aborts.
    error_sink: Rc<RefCell<Option<RindleError>>>,
}

impl OpStorage {
    /// Park a storage error (set-if-empty, first failure wins). The infallible
    /// `Storage` method that called this then returns its safe sentinel.
    fn park(&self, err: RindleError) {
        let mut slot = self.error_sink.borrow_mut();
        if slot.is_none() {
            *slot = Some(err);
        }
    }
}

impl Storage for OpStorage {
    fn set(&self, key: &str, value: StorageValue) {
        if let Err(e) = self.db.maybe_checkpoint() {
            self.park(RindleError::sqlite("checkpoint operator storage", e));
            return;
        }
        let bytes = encode_storage_value(&value);
        // `prepare_cached` so the steady-state hot path (one `set` per operator per push)
        // reuses a compiled statement instead of re-parsing this SQL every call.
        let set = || -> rusqlite::Result<()> {
            self.db
                .conn()
                .prepare_cached(
                    r#"
                    INSERT INTO storage ("op", "key", "val")
                    VALUES (?1, ?2, ?3)
                    ON CONFLICT("op", "key")
                    DO UPDATE SET "val" = excluded."val"
                    "#,
                )?
                .execute(params![self.op_id, key, bytes])?;
            Ok(())
        };
        if let Err(e) = set() {
            self.park(RindleError::sqlite("set operator storage value", e));
        }
    }

    fn get(&self, key: &str) -> Option<StorageValue> {
        if let Err(e) = self.db.maybe_checkpoint() {
            self.park(RindleError::sqlite("checkpoint operator storage", e));
            return None;
        }
        let query = || -> rusqlite::Result<Option<Vec<u8>>> {
            self.db
                .conn()
                .prepare_cached(
                    r#"
                    SELECT "val"
                    FROM storage
                    WHERE "op" = ?1 AND "key" = ?2
                    "#,
                )?
                .query_row(params![self.op_id, key], |r| r.get(0))
                .optional()
        };
        let bytes = match query() {
            Ok(b) => b,
            Err(e) => {
                self.park(RindleError::sqlite("get operator storage value", e));
                return None;
            }
        };
        match bytes {
            Some(b) => match decode_storage_value(&b) {
                Ok(v) => Some(v),
                Err(msg) => {
                    self.park(RindleError::Storage(format!(
                        "decode operator storage value: {msg}"
                    )));
                    None
                }
            },
            None => None,
        }
    }

    fn del(&self, key: &str) {
        if let Err(e) = self.db.maybe_checkpoint() {
            self.park(RindleError::sqlite("checkpoint operator storage", e));
            return;
        }
        let del = || -> rusqlite::Result<()> {
            self.db
                .conn()
                .prepare_cached(
                    r#"
                    DELETE FROM storage
                    WHERE "op" = ?1 AND "key" = ?2
                    "#,
                )?
                .execute(params![self.op_id, key])?;
            Ok(())
        };
        if let Err(e) = del() {
            self.park(RindleError::sqlite("delete operator storage value", e));
        }
    }

    fn scan<'s>(&'s self, prefix: &str) -> Box<dyn Iterator<Item = (Box<str>, StorageValue)> + 's> {
        // Checkpoint once up front — a scan does no writes, so there is nothing to
        // re-check between pages. A failure parks and yields an empty scan, as before.
        // The actual rows are then pulled lazily, a bounded page at a time, by
        // `ScanCursor` rather than slurped into one `Vec` here.
        if let Err(e) = self.db.maybe_checkpoint() {
            self.park(RindleError::sqlite("checkpoint operator storage", e));
            return Box::new(std::iter::empty());
        }
        Box::new(ScanCursor::new(self, prefix))
    }

    /// Drop this operator's whole namespace in one `DELETE` (the
    /// [`Storage::clear`] fast path used by pipeline teardown), rather than the
    /// default per-key scan+del. `op_id`s are never reused, so this only reclaims disk
    /// — a rebuilt operator gets a fresh `op_id` and so an already-empty keyspace.
    ///
    /// Unlike the point ops, `clear` does NOT gate on `maybe_checkpoint`: a bulk delete
    /// only shrinks the open transaction (it runs in the standing `BEGIN`), so there is
    /// no checkpoint-failure path that could leave rows behind. Only the `DELETE` itself
    /// can fail, and it parks like the others.
    fn clear(&self) {
        if let Err(e) = self.db.conn().execute(
            r#"DELETE FROM storage WHERE "op" = ?1"#,
            params![self.op_id],
        ) {
            self.park(RindleError::sqlite("clear operator storage", e));
        }
    }
}

/// How many `(key, value)` pairs a [`ScanCursor`] pulls from SQLite per round trip.
/// `scan` used to materialize the *entire* matching range into one `Vec`; an operator over
/// a large keyspace (a `reduce`/`cap` enumerating every group) could pin that whole range in
/// RAM at once. The cursor pages the statement instead, buffering at most this many rows —
/// bounded memory however big the operator's namespace has grown. Scratch scans are usually
/// tiny (a handful of partition keys, spec 10 §5.1), so the common case is still a single
/// round trip; only genuinely large scans pay for the extra seeks.
const SCAN_PAGE: usize = 1024;

/// Lazy, bounded-memory [`Storage::scan`] over `OpStorage`: an ascending prefix scan that
/// fetches `SCAN_PAGE` rows per round trip and re-seeks with `"key" > last` for the next
/// page, instead of reading the whole range into a `Vec` up front.
///
/// Keyset (not `OFFSET`) pagination is what lets the cursor honor the `scan` contract that
/// the caller may `set`/`del` *while draining* (spec 10 §5.1, the reason `MemoryStorage`
/// snapshots into a `Vec`): each page is a self-contained statement, dropped before a pair
/// is yielded, so no read is pinned across the caller's loop. A concurrent write — which can
/// `COMMIT; BEGIN` the standing txn via `maybe_checkpoint` — just lands the next page in a
/// fresh snapshot that resumes strictly after `seek`; there is no live cursor to invalidate.
struct ScanCursor<'s> {
    store: &'s OpStorage,
    /// Owned because the `prefix: &str` argument does not outlive the returned iterator.
    /// Bounds the scan: paging stops at the first key that does not start with it.
    prefix: Box<str>,
    /// Exclusive lower bound for the *next* page (the last key already yielded). `None`
    /// before the first page, whose bound is instead the inclusive `"key" >= prefix` start.
    seek: Option<Box<str>>,
    /// The current page, drained one pair per `next()` before the next page is fetched.
    page: std::vec::IntoIter<(Box<str>, StorageValue)>,
    /// Set once the range is exhausted (a short page), the prefix is passed, or an error is
    /// parked — after which no further page is fetched.
    done: bool,
}

impl<'s> ScanCursor<'s> {
    fn new(store: &'s OpStorage, prefix: &str) -> ScanCursor<'s> {
        ScanCursor {
            store,
            prefix: prefix.into(),
            seek: None,
            page: Vec::new().into_iter(),
            done: false,
        }
    }

    /// Fetch the next page into `self.page`, advancing `self.seek` (to the last key read) and
    /// `self.done` (once the scan is finished). The SQLite work is delegated to `query_page`
    /// so no borrow of `self` is held across these field updates.
    fn fetch_page(&mut self) {
        let (page, finished) =
            Self::query_page(self.store, self.prefix.as_ref(), self.seek.as_deref());
        if finished {
            self.done = true;
        }
        // Resume the next page strictly after the last key we accepted.
        if let Some((last_key, _)) = page.last() {
            self.seek = Some(last_key.clone());
        }
        self.page = page.into_iter();
    }

    /// Read up to `SCAN_PAGE` `(key, value)` pairs, ordered ascending, with `"key"` bounded
    /// below by `seek` (exclusive) or — on the first page (`seek == None`) — by `prefix`
    /// (inclusive). Returns the decoded pairs and whether the scan is now finished: a short
    /// page (range exhausted), the first key past the prefix, or a parked error all end it.
    /// Any pairs decoded before an error are still returned (matching the old eager scan,
    /// which yielded everything decoded before the failing row).
    fn query_page(
        store: &OpStorage,
        prefix: &str,
        seek: Option<&str>,
    ) -> (Vec<(Box<str>, StorageValue)>, bool) {
        let conn = store.db.conn();
        // The first page seeks inclusively from the prefix; later pages strictly past the
        // last key already yielded. Two cached statements (one per bound), reused per page.
        let (sql, lower) = match seek {
            None => (
                r#"
                SELECT "key", "val"
                FROM storage
                WHERE "op" = ?1 AND "key" >= ?2
                ORDER BY "key"
                LIMIT ?3
                "#,
                prefix,
            ),
            Some(last) => (
                r#"
                SELECT "key", "val"
                FROM storage
                WHERE "op" = ?1 AND "key" > ?2
                ORDER BY "key"
                LIMIT ?3
                "#,
                last,
            ),
        };
        let mut stmt = match conn.prepare_cached(sql) {
            Ok(s) => s,
            Err(e) => {
                store.park(RindleError::sqlite("prepare scan operator storage", e));
                return (Vec::new(), true);
            }
        };
        let rows = match stmt.query_map(params![store.op_id, lower, SCAN_PAGE as i64], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, Vec<u8>>(1)?))
        }) {
            Ok(r) => r,
            Err(e) => {
                store.park(RindleError::sqlite("scan operator storage", e));
                return (Vec::new(), true);
            }
        };
        let mut out = Vec::new();
        let mut fetched = 0usize;
        for row in rows {
            fetched += 1;
            let (key, bytes) = match row {
                Ok(kv) => kv,
                Err(e) => {
                    store.park(RindleError::sqlite("read scan row", e));
                    return (out, true);
                }
            };
            if !key.starts_with(prefix) {
                return (out, true);
            }
            match decode_storage_value(&bytes) {
                Ok(value) => out.push((key.into_boxed_str(), value)),
                Err(msg) => {
                    store.park(RindleError::Storage(format!(
                        "decode operator storage scan value: {msg}"
                    )));
                    return (out, true);
                }
            }
        }
        // A short page (fewer rows than the limit) means the range is exhausted.
        (out, fetched < SCAN_PAGE)
    }
}

impl Iterator for ScanCursor<'_> {
    type Item = (Box<str>, StorageValue);

    fn next(&mut self) -> Option<(Box<str>, StorageValue)> {
        loop {
            if let Some(pair) = self.page.next() {
                return Some(pair);
            }
            if self.done {
                return None;
            }
            // `fetch_page` either refills `self.page` or sets `self.done`; loop to drain the
            // refilled page, or to return `None` now that the scan is finished.
            self.fetch_page();
        }
    }
}

/// On-disk operator-state codec version (WS09.3). Prepended to every blob *before*
/// the variant tag. The spill is a derived cache (never the source of truth), so the
/// migration policy on a version mismatch is **discard and re-hydrate from source**,
/// not in-place upgrade — bumping this is cheap precisely because recovery is rebuild.
/// v3: `ReduceAcc.int_sum` widened i64 → i128 (design 226 §5.3).
const STORAGE_FORMAT_VERSION: u8 = 3;

fn encode_storage_value(value: &StorageValue) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(STORAGE_FORMAT_VERSION);
    match value {
        StorageValue::Take { size, bound } => {
            out.push(1);
            put_u32(&mut out, *size);
            match bound {
                None => out.push(0),
                Some(row) => {
                    out.push(1);
                    put_row(&mut out, row);
                }
            }
        }
        StorageValue::Bound(row) => {
            out.push(2);
            put_row(&mut out, row);
        }
        StorageValue::Cap { size, pks } => {
            out.push(3);
            put_u32(&mut out, *size);
            put_len(&mut out, pks.len());
            for pk in pks {
                put_bytes(&mut out, pk.as_bytes());
            }
        }
        StorageValue::Reduce { count, accs } => {
            out.push(4);
            put_i64(&mut out, *count);
            put_len(&mut out, accs.len());
            for acc in accs {
                put_i128(&mut out, acc.int_sum);
                put_f64(&mut out, acc.float_sum);
                put_i64(&mut out, acc.non_null);
                put_i64(&mut out, acc.float_count);
            }
        }
    }
    out
}

fn decode_storage_value(bytes: &[u8]) -> Result<StorageValue, String> {
    let mut d = Decoder::new(bytes);
    let version = d.u8()?;
    if version != STORAGE_FORMAT_VERSION {
        return Err(format!(
            "operator state format v{version} unsupported (engine expects v{STORAGE_FORMAT_VERSION}); rebuild from source"
        ));
    }
    let value = match d.u8()? {
        1 => {
            let size = d.u32()?;
            let bound = match d.u8()? {
                0 => None,
                1 => Some(d.row()?),
                _ => return Err("invalid Take bound tag".into()),
            };
            StorageValue::Take { size, bound }
        }
        2 => StorageValue::Bound(d.row()?),
        3 => {
            let size = d.u32()?;
            let n = d.u32()? as usize;
            let mut pks = Vec::with_capacity(n);
            for _ in 0..n {
                pks.push(d.string()?.into_boxed_str());
            }
            StorageValue::Cap { size, pks }
        }
        4 => {
            let count = d.i64()?;
            let n = d.u32()? as usize;
            let mut accs = Vec::with_capacity(n);
            for _ in 0..n {
                accs.push(ReduceAcc {
                    int_sum: d.i128()?,
                    float_sum: d.f64()?,
                    non_null: d.i64()?,
                    float_count: d.i64()?,
                });
            }
            StorageValue::Reduce { count, accs }
        }
        _ => return Err("invalid storage value tag".into()),
    };
    d.finish()?;
    Ok(value)
}

fn put_row(out: &mut Vec<u8>, row: &OwnedRow) {
    put_len(out, row.len());
    for value in row.cells() {
        put_value(out, &value.to_owned());
    }
}

fn put_value(out: &mut Vec<u8>, value: &OwnedValue) {
    match value {
        // Operator-state rows are full server rows and never carry `Absent`; tag 6 keeps
        // the codec total and round-trippable should one ever be persisted.
        OwnedValue::Absent => out.push(6),
        OwnedValue::Null => out.push(0),
        OwnedValue::Bool(b) => {
            out.push(1);
            out.push(u8::from(*b));
        }
        OwnedValue::Int(i) => {
            out.push(2);
            out.extend_from_slice(&i.to_le_bytes());
        }
        OwnedValue::Float(f) => {
            out.push(3);
            out.extend_from_slice(&f.to_bits().to_le_bytes());
        }
        OwnedValue::Str(s) => {
            out.push(4);
            put_bytes(out, s.as_bytes());
        }
        OwnedValue::Json(s) => {
            out.push(5);
            put_bytes(out, s.as_bytes());
        }
    }
}

fn put_len(out: &mut Vec<u8>, len: usize) {
    let len = u32::try_from(len).expect("operator storage value length fits in u32");
    put_u32(out, len);
}

fn put_u32(out: &mut Vec<u8>, n: u32) {
    out.extend_from_slice(&n.to_le_bytes());
}

fn put_i64(out: &mut Vec<u8>, n: i64) {
    out.extend_from_slice(&n.to_le_bytes());
}

fn put_i128(out: &mut Vec<u8>, n: i128) {
    out.extend_from_slice(&n.to_le_bytes());
}

fn put_f64(out: &mut Vec<u8>, n: f64) {
    out.extend_from_slice(&n.to_bits().to_le_bytes());
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    put_len(out, bytes.len());
    out.extend_from_slice(bytes);
}

struct Decoder<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Decoder<'a> {
    fn new(bytes: &'a [u8]) -> Decoder<'a> {
        Decoder { bytes, pos: 0 }
    }

    fn finish(&self) -> Result<(), String> {
        if self.pos == self.bytes.len() {
            Ok(())
        } else {
            Err("trailing storage codec bytes".into())
        }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], String> {
        let end = self
            .pos
            .checked_add(len)
            .ok_or_else(|| "storage codec length overflow".to_string())?;
        let out = self
            .bytes
            .get(self.pos..end)
            .ok_or_else(|| "truncated storage codec value".to_string())?;
        self.pos = end;
        Ok(out)
    }

    fn u8(&mut self) -> Result<u8, String> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, String> {
        let mut buf = [0; 4];
        buf.copy_from_slice(self.take(4)?);
        Ok(u32::from_le_bytes(buf))
    }

    fn i64(&mut self) -> Result<i64, String> {
        let mut buf = [0; 8];
        buf.copy_from_slice(self.take(8)?);
        Ok(i64::from_le_bytes(buf))
    }

    fn i128(&mut self) -> Result<i128, String> {
        let mut buf = [0; 16];
        buf.copy_from_slice(self.take(16)?);
        Ok(i128::from_le_bytes(buf))
    }

    fn u64(&mut self) -> Result<u64, String> {
        let mut buf = [0; 8];
        buf.copy_from_slice(self.take(8)?);
        Ok(u64::from_le_bytes(buf))
    }

    fn f64(&mut self) -> Result<f64, String> {
        Ok(f64::from_bits(self.u64()?))
    }

    fn bytes(&mut self) -> Result<&'a [u8], String> {
        let len = self.u32()? as usize;
        self.take(len)
    }

    fn string(&mut self) -> Result<String, String> {
        String::from_utf8(self.bytes()?.to_vec())
            .map_err(|_| "storage codec string is not UTF-8".to_string())
    }

    fn row(&mut self) -> Result<OwnedRow, String> {
        let len = self.u32()? as usize;
        let mut values = Vec::with_capacity(len);
        for _ in 0..len {
            values.push(self.value()?);
        }
        Ok(owned_row(values))
    }

    fn value(&mut self) -> Result<OwnedValue, String> {
        match self.u8()? {
            0 => Ok(OwnedValue::Null),
            1 => match self.u8()? {
                0 => Ok(OwnedValue::Bool(false)),
                1 => Ok(OwnedValue::Bool(true)),
                _ => Err("invalid bool tag".into()),
            },
            2 => Ok(OwnedValue::Int(self.i64()?)),
            3 => Ok(OwnedValue::Float(f64::from_bits(self.u64()?))),
            4 => Ok(OwnedValue::Str(Arc::from(self.string()?))),
            5 => Ok(OwnedValue::Json(Arc::from(self.string()?))),
            6 => Ok(OwnedValue::Absent),
            _ => Err("invalid row value tag".into()),
        }
    }
}

// ---------------------------------------------------------------------------
// WS02.3 / WS09.3 — operator-storage error sink + format versioning
// ---------------------------------------------------------------------------

/// A SQLite operator-storage failure must surface as a parked `RindleError` (drained by
/// the graph at the mutation boundary) and return a safe sentinel — never `.expect`
/// / abort. Here we force the WS09.3 format-version mismatch (a bumped/corrupt blob)
/// and assert `get` returns `None` while parking a `RindleError::Storage`.
#[cfg(test)]
mod fault_tests {
    use super::*;

    #[test]
    fn decode_version_mismatch_parks_error_not_panic() {
        let db = DatabaseStorage::new_in_memory().expect("open in-memory db");
        let sink: Rc<RefCell<Option<RindleError>>> = Rc::default();
        let store = db.op_storage_with_sink(sink.clone());

        // A valid round-trip parks nothing.
        store.set(
            "ok",
            StorageValue::Bound(owned_row(vec![OwnedValue::Int(7)])),
        );
        assert!(matches!(store.get("ok"), Some(StorageValue::Bound(_))));
        assert!(
            sink.borrow().is_none(),
            "a valid round-trip must not park an error"
        );

        // Inject a blob whose leading version byte is wrong (a future codec / corrupt
        // store), into the store's own `op` namespace.
        let bad_blob: Vec<u8> = vec![
            STORAGE_FORMAT_VERSION.wrapping_add(1),
            2, /* Bound tag */
        ];
        store
            .db
            .conn()
            .execute(
                r#"INSERT INTO storage ("op","key","val") VALUES (?1,?2,?3)"#,
                params![store.op_id, "corrupt", bad_blob],
            )
            .expect("inject blob");

        // `get` must NOT panic: it returns the safe sentinel (None) and parks a typed
        // error for the graph to re-raise.
        let got = store.get("corrupt");
        assert!(
            got.is_none(),
            "a decode failure returns None, never a value"
        );
        let parked = sink.borrow();
        assert!(
            matches!(&*parked, Some(RindleError::Storage(msg)) if msg.contains("format v")),
            "expected a parked RindleError::Storage about a format version, got {parked:?}"
        );
    }
}

/// Spec 10 §3.2 — `scan` pages through SQLite a bounded chunk at a time rather than
/// materializing the whole range. These guard the keyset-paginated cursor across result
/// sets larger than one page: a bad inter-page seek would drop, duplicate, mis-order, or
/// bleed past the prefix. Data is sized off `SCAN_PAGE` so the test keeps spanning several
/// pages if that constant ever changes.
#[cfg(test)]
mod scan_paging_tests {
    use super::*;

    fn reduce_count(v: Option<StorageValue>) -> Option<i64> {
        match v {
            Some(StorageValue::Reduce { count, .. }) => Some(count),
            _ => None,
        }
    }

    #[test]
    fn scan_spans_many_pages_without_dropping_or_reordering_keys() {
        let db = DatabaseStorage::new_in_memory().expect("open in-memory db");
        let s = db.create_storage();

        // Several pages of "a:" keys, then a small "b:" block immediately after so a
        // prefix scan has to stop at a boundary that falls mid-page. Zero-padded to a fixed
        // width so byte (BINARY) order matches numeric order — and matches `MemoryStorage`'s
        // `BTreeMap<Box<str>>` order, the parity the two backends must keep.
        let n = SCAN_PAGE * 2 + 7;
        for i in 0..n {
            s.set(
                &format!("a:{i:06}"),
                StorageValue::Reduce {
                    count: i as i64,
                    accs: Vec::new(),
                },
            );
        }
        for i in 0..5 {
            s.set(
                &format!("b:{i:06}"),
                StorageValue::Reduce {
                    count: -1,
                    accs: Vec::new(),
                },
            );
        }

        // Whole-keyspace scan: every key exactly once, strictly ascending across pages.
        let all: Vec<Box<str>> = s.scan("").map(|(k, _)| k).collect();
        assert_eq!(all.len(), n + 5, "scan must yield every key across pages");
        assert!(
            all.windows(2).all(|w| w[0] < w[1]),
            "paged scan keys must stay strictly ascending with no duplicates"
        );

        // Prefix scan that crosses page boundaries: exactly the `n` "a:" keys, in order,
        // stopping before the "b:" block instead of running into the next page.
        let a_keys: Vec<String> = s.scan("a:").map(|(k, _)| k.to_string()).collect();
        let want: Vec<String> = (0..n).map(|i| format!("a:{i:06}")).collect();
        assert_eq!(
            a_keys, want,
            "prefix scan dropped/added a key or crossed the prefix"
        );

        // Values round-trip on both sides of a page boundary (paging must not corrupt them).
        for i in [0, SCAN_PAGE - 1, SCAN_PAGE, SCAN_PAGE + 1, n - 1] {
            assert_eq!(
                reduce_count(s.get(&format!("a:{i:06}"))),
                Some(i as i64),
                "value at index {i} (near a page boundary) failed to round-trip"
            );
        }
    }

    #[test]
    fn empty_and_unmatched_prefix_scans_are_empty() {
        let db = DatabaseStorage::new_in_memory().expect("open in-memory db");
        let s = db.create_storage();

        // Empty store: the cursor's first page is empty and the scan ends immediately.
        assert_eq!(
            s.scan("").count(),
            0,
            "scan of an empty store yields nothing"
        );

        s.set(
            "a:0",
            StorageValue::Reduce {
                count: 0,
                accs: Vec::new(),
            },
        );
        // A prefix with no matches must yield nothing (the first key found is past it).
        assert_eq!(
            s.scan("z:").count(),
            0,
            "non-matching prefix yields nothing"
        );
    }
}
