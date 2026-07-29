//! `@rindle/replica` — the SQLite-backed `rindle-replica` live-query engine exposed to Node via
//! napi-rs. The `#[napi] Db` class mirrors the wasm `Db` surface (register/query/commit/
//! destroy) and emits the SAME bare, camelCase, JS-shaped flat changes (see `marshal`), so
//! one `@rindle/client` `ArrayView` folds either backend's stream.

mod cluster_core;
mod marshal;
mod replica_core;

use napi::bindgen_prelude::*;
use napi::threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode};
use napi_derive::napi;
use serde_json::Value;

use cluster_core::{ClusterMutationWrite, ClusterReplica};
use replica_core::{ColType, Mutation, MutationWrite, Replica};
use rindle::{Ast, OwnedValue};

fn map_err<E: std::fmt::Display>(e: E) -> napi::Error {
    napi::Error::from_reason(e.to_string())
}

/// The typed error a tripped `strict_i64` check raises (productionization 09.8).
/// **Mandatory** since design 226 Stage C (§8): the 222 SQL plane can write
/// full-range i64, and the napi cell encode is `as f64` — an `Int` that cannot
/// round-trip through f64 must fail the crossing call, never silently round.
/// Stage E adds the bigint lane and makes the check column-aware.
fn strict_i64_error(i: i64) -> napi::Error {
    napi::Error::from_reason(format!(
        "unsupported_value: i64 {i} exceeds the JS safe-integer range (strict_i64)"
    ))
}

/// The 09.8 `strict_i64` gate on an outgoing flat batch.
fn check_flat_js_safe(changes: &[rindle::FlatChange]) -> Result<()> {
    if let Some(i) = rindle::js_safe::unsafe_int_in_flat_changes(changes) {
        return Err(strict_i64_error(i));
    }
    Ok(())
}

/// The 09.8 `strict_i64` gate on an outgoing normalized batch.
fn check_batch_js_safe(batch: &rindle_replica::NormalizedBatch) -> Result<()> {
    if let Some(i) = rindle_replica::unsafe_int_in_normalized_batch(batch) {
        return Err(strict_i64_error(i));
    }
    Ok(())
}

/// The 09.8 `strict_i64` gate on raw result rows (`queryRows`).
fn check_rows_js_safe(rows: &[Vec<OwnedValue>]) -> Result<()> {
    if let Some(i) = rows
        .iter()
        .find_map(|r| rindle::js_safe::unsafe_int_in_cells(r))
    {
        return Err(strict_i64_error(i));
    }
    Ok(())
}

fn col_type(s: &str) -> ColType {
    match s {
        "number" => ColType::Number,
        "boolean" => ColType::Boolean,
        "json" => ColType::Json,
        // The exact-i64 column plane (design 226): follower DDL renders BIGINT (the
        // self-describing decltype that survives rediscovery — INTEGER would
        // collapse to f64),
        // cells replicate exactly; IVM queries over it are refused until Stage E
        // (§8's gate), and the SQL plane reads/writes it exactly.
        "int64" => ColType::Int,
        _ => ColType::String,
    }
}

/// Parse the Store's positional `Mutation[]` (`{ op, table, row|old|new }`) into the core's
/// mutations, converting each bare cell to an `OwnedValue`.
fn parse_mutations(v: &Value) -> Result<Vec<Mutation>> {
    let arr = v
        .as_array()
        .ok_or_else(|| Error::from_reason("mutations must be an array"))?;
    let mut out = Vec::with_capacity(arr.len());
    for m in arr {
        let table = m
            .get("table")
            .and_then(|x| x.as_str())
            .ok_or_else(|| Error::from_reason("mutation missing `table`"))?
            .to_string();
        let op = m
            .get("op")
            .and_then(|x| x.as_str())
            .ok_or_else(|| Error::from_reason("mutation missing `op`"))?;
        let cells = |key: &str| -> Result<Vec<OwnedValue>> {
            let a = m
                .get(key)
                .and_then(|x| x.as_array())
                .ok_or_else(|| Error::from_reason(format!("mutation missing `{key}`")))?;
            Ok(a.iter().map(marshal::json_to_owned).collect())
        };
        out.push(match op {
            "add" => Mutation::Add {
                table,
                row: cells("row")?,
            },
            "remove" => Mutation::Remove {
                table,
                row: cells("row")?,
            },
            "edit" => Mutation::Edit {
                table,
                old: cells("old")?,
                new: cells("new")?,
            },
            other => return Err(Error::from_reason(format!("unknown mutation op: {other}"))),
        });
    }
    Ok(out)
}

/// The in-process, SQLite-backed live-query engine, callable from Node. One `Db` owns one
/// replica (a temp wal2 database, removed on GC). Single-threaded — use it on one thread.
/// The `strict_i64` boundary check (productionization 09.8) is **mandatory** on
/// every read/delivery surface of this addon (design 226 Stage C, §8): an `Int`
/// cell outside `Number.MAX_SAFE_INTEGER` crossing to JS fails the crossing call
/// with a typed error instead of silently rounding through the `as f64` cell
/// encode. Note the check guards the *boundary*, not the store: a `commit*` call
/// failing it means the write DID land durably — its derived frames were refused
/// delivery.
#[napi(js_name = "Db")]
pub struct ReplicaDb {
    inner: Replica,
}

#[napi]
impl ReplicaDb {
    /// Open a fresh replica over a unique temp wal2 file.
    #[napi(constructor)]
    pub fn new() -> Result<Self> {
        Ok(ReplicaDb {
            inner: Replica::open().map_err(map_err)?,
        })
    }

    /// Define + register a base table from the typed schema: column names (in order), the
    /// primary-key column indices, and each column's declared type (`"string" | "number" |
    /// "boolean" | "json"`, driving SQLite affinity). Idempotent per table.
    #[napi(js_name = "registerTable")]
    pub fn register_table(
        &self,
        name: String,
        columns: Vec<String>,
        primary_key: Vec<u32>,
        column_types: Vec<String>,
    ) -> Result<()> {
        let pk: Vec<usize> = primary_key.iter().map(|&i| i as usize).collect();
        let types: Vec<ColType> = column_types.iter().map(|t| col_type(t)).collect();
        self.inner
            .register_table(&name, &columns, &pk, &types)
            .map_err(map_err)
    }

    /// Register a live query from a Zero-wire AST (passed as JSON). Returns the `hello` +
    /// hydrate snapshot: `{ queryId, comparatorVersion, schema, snapshot }` — `schema` the
    /// camelCase wire view-schema, `snapshot` the bare flat changes.
    #[napi]
    pub fn query(&self, query_id: f64, ast_json: String) -> Result<Value> {
        let ast: Ast = serde_json::from_str(&ast_json)
            .map_err(|e| Error::from_reason(format!("invalid AST JSON: {e}")))?;
        let init = self.inner.query(query_id as u64, ast).map_err(map_err)?;
        check_flat_js_safe(&init.snapshot)?;
        Ok(serde_json::json!({
            "queryId": query_id,
            "comparatorVersion": init.comparator_version,
            "schema": marshal::wire_schema_to_json(&init.schema),
            "snapshot": marshal::flat_changes_to_json(&init.snapshot),
        }))
    }

    /// Register a NORMALIZED live query (NORMALIZED-CHANGES-DESIGN.md §3): the path-free twin
    /// of [`query`](ReplicaDb::query). Returns `{ queryId, hello, snapshot }` — `hello` the
    /// slim per-table-schema handshake (`{ epoch, comparatorVersion, tables, normalizedFp }`),
    /// `snapshot` the seq-0 batch of table-tagged `add` ops.
    #[napi(js_name = "queryNormalized")]
    pub fn query_normalized(&self, query_id: f64, ast_json: String, epoch: f64) -> Result<Value> {
        let ast: Ast = serde_json::from_str(&ast_json)
            .map_err(|e| Error::from_reason(format!("invalid AST JSON: {e}")))?;
        let init = self
            .inner
            .query_normalized(query_id as u64, ast, epoch as u64)
            .map_err(map_err)?;
        check_batch_js_safe(&init.snapshot)?;
        Ok(serde_json::json!({
            "queryId": query_id,
            "hello": marshal::normalized_hello_to_json(&init.hello),
            "snapshot": marshal::normalized_batch_to_json(&init.snapshot),
        }))
    }

    /// Tear down a registered query (reclaims its pipeline + stops delivery). No-op for an
    /// unknown id. Works for both flat and normalized queries.
    #[napi(js_name = "destroyQuery")]
    pub fn destroy_query(&self, query_id: f64) {
        self.inner.destroy_query(query_id as u64);
    }

    /// Apply a batch of positional mutations as one transaction and return each affected
    /// query's derived flat changes: `[{ queryId, events }]` (bare). An empty batch returns
    /// an empty array.
    #[napi]
    pub fn commit(&self, mutations: Value) -> Result<Value> {
        let muts = parse_mutations(&mutations)?;
        let batches = self.inner.commit(&muts).map_err(map_err)?;
        for (_, events) in &batches {
            check_flat_js_safe(events)?;
        }
        let out: Vec<Value> = batches
            .iter()
            .map(|(qid, events)| {
                serde_json::json!({ "queryId": *qid as f64, "events": marshal::flat_changes_to_json(events) })
            })
            .collect();
        Ok(Value::Array(out))
    }

    /// The normalized twin of [`commit`](ReplicaDb::commit): apply the txn and return each
    /// affected NORMALIZED query's batch: `[{ queryId, batch }]` (`batch` = `{ epoch, seq,
    /// cv, normalizedFp, ops }`). An empty batch returns an empty array.
    #[napi(js_name = "commitNormalized")]
    pub fn commit_normalized(&self, mutations: Value) -> Result<Value> {
        let muts = parse_mutations(&mutations)?;
        let batches = self.inner.commit_normalized(&muts).map_err(map_err)?;
        for (_, batch) in &batches {
            check_batch_js_safe(batch)?;
        }
        let out: Vec<Value> = batches
            .iter()
            .map(|(qid, batch)| {
                serde_json::json!({ "queryId": *qid as f64, "batch": marshal::normalized_batch_to_json(batch) })
            })
            .collect();
        Ok(Value::Array(out))
    }

    // --- the optimistic-writes upstream (OPTIMISTIC-WRITES-DESIGN.md §8.1–§8.2) -----

    /// One-time (idempotent) setup for client mutations: the `_rindle_client_mutations`
    /// table + its CDC capture, so `lmid` lands co-transactionally with each mutation.
    #[napi(js_name = "enableClientMutations")]
    pub fn enable_client_mutations(&self) -> Result<()> {
        self.inner.enable_client_mutations().map_err(map_err)
    }

    /// The durably stored high-water mutation id for a client (0 if new) — what the
    /// server's dup-skip / gap checks and a reconnect handshake read.
    #[napi(js_name = "clientLmid")]
    pub fn client_lmid(&self, client_id: String) -> Result<f64> {
        Ok(self.inner.client_lmid(&client_id).map_err(map_err)? as f64)
    }

    /// Open one mutation's write transaction. The JS server registry runs its mutator
    /// against the returned handle (`exec`/`queryRows` — reads see the txn's own
    /// writes), then `commitWithLmid` lands effects + lmid atomically.
    #[napi(js_name = "beginMutation")]
    pub fn begin_mutation(&self) -> Result<NativeMutationTxn> {
        Ok(NativeMutationTxn {
            inner: self.inner.begin_mutation().map_err(map_err)?,
        })
    }
}

/// One server-side mutation's open transaction (see `Db.beginMutation`). Dropping
/// without committing rolls back.
#[napi(js_name = "MutationTxn")]
pub struct NativeMutationTxn {
    inner: MutationWrite,
}

fn parse_params(params: &Value) -> Result<Vec<OwnedValue>> {
    let arr = params
        .as_array()
        .ok_or_else(|| Error::from_reason("params must be an array"))?;
    Ok(arr.iter().map(marshal::json_to_owned).collect())
}

#[napi]
impl NativeMutationTxn {
    /// Run one statement with positional (bare-cell) parameters; returns rows changed.
    #[napi]
    pub fn exec(&mut self, sql: String, params: Value) -> Result<f64> {
        let p = parse_params(&params)?;
        Ok(self.inner.exec(&sql, &p).map_err(map_err)? as f64)
    }

    /// Run a read returning all rows as bare-cell arrays. Reads through the open
    /// transaction — a read-dependent mutator sees its own writes (§4.1). An INTEGER
    /// result outside `Number.MAX_SAFE_INTEGER` is a typed error instead of a silently
    /// rounded `number` (09.8, mandatory since 226 Stage C) — reachable today via a
    /// SQL integer literal or expression, since SQL text can express any i64.
    #[napi(js_name = "queryRows")]
    pub fn query_rows(&mut self, sql: String, params: Value) -> Result<Value> {
        let p = parse_params(&params)?;
        let rows = self.inner.query(&sql, &p).map_err(map_err)?;
        check_rows_js_safe(&rows)?;
        Ok(Value::Array(
            rows.iter()
                .map(|r| Value::Array(r.iter().map(marshal::owned_to_json).collect()))
                .collect(),
        ))
    }

    /// Upsert the client's `last_mutation_id = mid` in THIS transaction, commit, and
    /// return `{ cv, batches: [{ queryId, batch }] }` — the commit version plus every
    /// affected normalized query's (cv-stamped) batch.
    #[napi(js_name = "commitWithLmid")]
    pub fn commit_with_lmid(&mut self, client_id: String, mid: f64) -> Result<Value> {
        let (cv, batches) = self
            .inner
            .commit_with_lmid(&client_id, mid as u64)
            .map_err(map_err)?;
        for (_, batch) in &batches {
            check_batch_js_safe(batch)?;
        }
        let out: Vec<Value> = batches
            .iter()
            .map(|(qid, batch)| {
                serde_json::json!({ "queryId": *qid as f64, "batch": marshal::normalized_batch_to_json(batch) })
            })
            .collect();
        Ok(serde_json::json!({ "cv": cv as f64, "batches": out }))
    }

    /// Roll back: effects discarded, nothing delivered.
    #[napi]
    pub fn rollback(&mut self) {
        self.inner.rollback();
    }
}

// ============================================================================
// The CLUSTER-backed, async-push surface (CLUSTER-FOLD-IN-DESIGN.md WS6) ------
//
// `ClusterDb` mirrors the optimistic `Db` surface but derives queries in parallel across
// IVM worker threads and pushes per-query batches / per-connection progress frames /
// faults **asynchronously** through a `ThreadsafeFunction` (`onEvent`). `commit*` returns
// `{cv}` immediately (after the durable COMMIT, pre-derive); the data + progress arrive on
// the callback. The single-thread `Db` above is untouched (still the simple in-process
// backend + the non-optimistic servers).
// ============================================================================

/// The cluster-backed, async-push live-query engine, callable from Node. One `ClusterDb`
/// owns one parallel replica (a temp wal2 database + a worker pool + a drain thread).
/// The 09.8 `strict_i64` check is mandatory here too (226 Stage C); on this async
/// surface a tripped batch check is delivered as a per-query `faulted` event
/// rather than a thrown error.
#[napi(js_name = "ClusterDb")]
pub struct ClusterReplicaDb {
    inner: ClusterReplica,
}

#[napi]
impl ClusterReplicaDb {
    /// Open a fresh cluster-backed replica. Call [`onEvent`](ClusterReplicaDb::on_event)
    /// before subscribing/committing so async batches/frames have somewhere to go.
    #[napi(constructor)]
    pub fn new() -> Result<Self> {
        Ok(ClusterReplicaDb {
            inner: ClusterReplica::open().map_err(map_err)?,
        })
    }

    /// Register the async-event callback. It is invoked `(err, event)` per delivered item;
    /// `event` is a tagged object: `{ t: "batch", conn, queryId, batch }`,
    /// `{ t: "progress", conn, frame }`, or `{ t: "faulted", conn, queryId, reason }`.
    ///
    /// The drain thread holds this `ThreadsafeFunction`, which keeps the Node event loop
    /// alive — call [`close`](ClusterReplicaDb::close) on shutdown to release it (the process
    /// can then exit even though the worker/drain threads are joined on `Drop`).
    #[napi(js_name = "onEvent")]
    pub fn on_event(&self, callback: ThreadsafeFunction<Value>) {
        self.inner.set_sink(Box::new(move |v: Value| {
            let _ = callback.call(Ok(v), ThreadsafeFunctionCallMode::NonBlocking);
        }));
    }

    /// Release the async-event callback (drops the `ThreadsafeFunction` ref so the Node
    /// process can exit) and let the worker/drain threads wind down. Idempotent; call on
    /// server shutdown. After this no further events are delivered.
    #[napi]
    pub fn close(&self) {
        self.inner.close();
    }

    /// Define + register a base table (see `Db.registerTable`). Idempotent.
    #[napi(js_name = "registerTable")]
    pub fn register_table(
        &self,
        name: String,
        columns: Vec<String>,
        primary_key: Vec<u32>,
        column_types: Vec<String>,
    ) -> Result<()> {
        let pk: Vec<usize> = primary_key.iter().map(|&i| i as usize).collect();
        let types: Vec<ColType> = column_types.iter().map(|t| col_type(t)).collect();
        self.inner
            .register_table(&name, &columns, &pk, &types)
            .map_err(map_err)
    }

    /// One-time (idempotent) setup for client mutations (`_rindle_client_mutations` + CDC).
    #[napi(js_name = "enableClientMutations")]
    pub fn enable_client_mutations(&self) -> Result<()> {
        self.inner.enable_client_mutations().map_err(map_err)
    }

    /// The durably stored high-water mutation id for a client (0 if new).
    #[napi(js_name = "clientLmid")]
    pub fn client_lmid(&self, client_id: String) -> Result<f64> {
        Ok(self.inner.client_lmid(&client_id).map_err(map_err)? as f64)
    }

    /// Register a connection (drain-side progress bookkeeping) under a consumer-assigned id.
    #[napi]
    pub fn connect(&self, conn: f64) {
        self.inner.connect(conn as u64);
    }

    /// Drop a connection's progress bookkeeping (destroy its queries separately).
    #[napi]
    pub fn disconnect(&self, conn: f64) {
        self.inner.disconnect(conn as u64);
    }

    /// Register a NORMALIZED live query for `conn` under `queryId`. Returns the slim `hello`
    /// **synchronously**; the seq-0 snapshot + every later batch arrive via `onEvent`.
    #[napi(js_name = "queryNormalized")]
    pub fn query_normalized(
        &self,
        conn: f64,
        query_id: f64,
        ast_json: String,
        epoch: f64,
    ) -> Result<Value> {
        let ast: Ast = serde_json::from_str(&ast_json)
            .map_err(|e| Error::from_reason(format!("invalid AST JSON: {e}")))?;
        let hello = self
            .inner
            .query_normalized(conn as u64, query_id as u64, ast, epoch as u64)
            .map_err(map_err)?;
        Ok(serde_json::json!({
            "queryId": query_id,
            "hello": marshal::normalized_hello_to_json(&hello),
        }))
    }

    /// Tear down a registered query (cluster pipeline + drain bookkeeping). No-op if unknown.
    #[napi(js_name = "destroyQuery")]
    pub fn destroy_query(&self, query_id: f64) {
        self.inner.destroy_query(query_id as u64);
    }

    /// Apply positional mutations as one **raw foreign write** (no lmid); returns the commit
    /// version `cv`. Affected queries' batches + touched connections' frames flow via `onEvent`.
    #[napi(js_name = "commitNormalized")]
    pub fn commit_normalized(&self, mutations: Value) -> Result<f64> {
        let muts = parse_mutations(&mutations)?;
        Ok(self.inner.commit_normalized(&muts).map_err(map_err)? as f64)
    }

    /// Open one mutation's write transaction (the JS mutator runs against it).
    #[napi(js_name = "beginMutation")]
    pub fn begin_mutation(&self) -> Result<ClusterMutationTxn> {
        Ok(ClusterMutationTxn {
            inner: self.inner.begin_mutation().map_err(map_err)?,
        })
    }
}

/// One server-side mutation's open cluster transaction (see `ClusterDb.beginMutation`).
/// Dropping without committing rolls back.
#[napi(js_name = "ClusterMutationTxn")]
pub struct ClusterMutationTxn {
    inner: ClusterMutationWrite,
}

#[napi]
impl ClusterMutationTxn {
    /// Run one statement with positional (bare-cell) parameters; returns rows changed.
    #[napi]
    pub fn exec(&mut self, sql: String, params: Value) -> Result<f64> {
        let p = parse_params(&params)?;
        Ok(self.inner.exec(&sql, &p).map_err(map_err)? as f64)
    }

    /// Run a read returning all rows as bare-cell arrays (sees the txn's own writes, §4.1).
    /// An INTEGER result outside `Number.MAX_SAFE_INTEGER` is a typed error instead of
    /// a silently rounded `number` (09.8, mandatory since 226 Stage C).
    #[napi(js_name = "queryRows")]
    pub fn query_rows(&mut self, sql: String, params: Value) -> Result<Value> {
        let p = parse_params(&params)?;
        let rows = self.inner.query(&sql, &p).map_err(map_err)?;
        check_rows_js_safe(&rows)?;
        Ok(Value::Array(
            rows.iter()
                .map(|r| Value::Array(r.iter().map(marshal::owned_to_json).collect()))
                .collect(),
        ))
    }

    /// Upsert the client's `last_mutation_id = mid` in THIS transaction, commit, and return
    /// the commit version `cv`. The lmid row is ordinary data: it derives through the
    /// client's own system query and is released by the same `cv_min` as the commit's
    /// effects. The per-query batches + the progress frame flow asynchronously via `onEvent`.
    #[napi(js_name = "commitWithLmid")]
    pub fn commit_with_lmid(&mut self, client_id: String, mid: f64) -> Result<f64> {
        Ok(self
            .inner
            .commit_with_lmid(&client_id, mid as u64)
            .map_err(map_err)? as f64)
    }

    /// Commit WITHOUT an lmid advance (a foreign/system write: confirms no client mutation)
    /// and return the commit version `cv`. Batches/progress flow asynchronously via `onEvent`.
    #[napi]
    pub fn commit(&mut self) -> Result<f64> {
        Ok(self.inner.commit().map_err(map_err)? as f64)
    }

    /// Roll back: effects discarded, nothing delivered.
    #[napi]
    pub fn rollback(&mut self) {
        self.inner.rollback();
    }
}
