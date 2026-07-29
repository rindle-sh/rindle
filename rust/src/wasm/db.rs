//! The wasm `Db` — a multi-query, in-memory live-query engine callable from JS, emitting
//! **bare-valued flat changes** (the WASM backend of the JS client; `WASM-CLIENT-DESIGN.md`
//! §5). A port of `rindle_replica::Engine` over in-memory sources, with **no SQLite/CDC**:
//! rows are pushed directly through a staging [`WriteTxn`].
//!
//! ## Model
//!
//! One `Db` owns one single-threaded [`Graph`] with one shared in-memory source per
//! registered table and one change-sink per registered query. A committed write pushes
//! the staged row changes through the shared sources — fanning each out to every
//! dependent query — then drains each query's sink into a flat-change batch.
//!
//! ## Delivery: pull at the FFI (no wasm→JS callback)
//!
//! [`WriteTxn::commit`] **returns** the per-query batches; the wasm side never calls into
//! JS. The JS `WasmBackend` adapter turns the returned array into its push-based
//! `ChangeEvent` stream. This keeps the ABI one-directional and sidesteps the reentrancy
//! hazard the materialize-in-wasm [`RindleView`](super::RindleView) has to guard.
//!
//! ## Wire shape
//!
//! Cells cross **bare** (`number | string | boolean | null`), not tagged: the JS host has
//! a typed schema, so per-column types are known there (`WASM-CLIENT-DESIGN.md` §7). The
//! per-query view schema (names + resolved sort + nesting) crosses once via
//! [`to_wire`](crate::wire_schema::to_wire).

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;

use wasm_bindgen::prelude::*;

use crate::ast::{Ast, Condition};
use crate::builder::{build_pipeline, view_schema};
use crate::change::SourceChange;
use crate::changes::CaughtChange;
use crate::flat::{flatten_all, FlatChange, FlatOp, WireNode};
use crate::graph::{Graph, NodeId, PipelineManifest};
use crate::memory_source::MemorySource;
use crate::optimistic::OptimisticTables;
use crate::value::{compare_values, owned_row, OwnedRow, OwnedValue, Schema, SourceSchema};
use crate::wire_schema::{to_wire, COMPARATOR_VERSION};

use super::marshal;

/// Per-registered-query bookkeeping: the change-sink to drain each commit, and the
/// [`PipelineManifest`] recorded at build time so [`Db::destroy_query`] can tear the
/// pipeline out precisely (mirrors `rindle_replica::engine::QueryReg`).
struct QueryReg {
    sink: NodeId,
    manifest: PipelineManifest,
    /// The hierarchical view schema, kept so each drained flat batch can be **projected to
    /// this query's columns** before marshalling (`PROJECTION-SUPPORT-DESIGN.md` §5.2 / §6):
    /// the client analogue of project-at-emit. `None`-projection levels pass through full.
    schema: Schema,
}

/// Per-query buffered sink events while a server-batch cycle is open.
type CycleBuf = BTreeMap<u64, Vec<CaughtChange>>;

/// Shared inner state behind a [`Db`] handle and every [`WriteTxn`] it issues. Held via
/// `Rc` so handles are cheap to clone and share one engine; the `RefCell`s give the
/// interior mutability wasm-bindgen's `&self` methods need (register/query/destroy borrow
/// mutably; a commit's push + drain are `&self` on the graph, so an immutable borrow).
struct DbInner {
    graph: RefCell<Graph>,
    /// table name → (shared source `NodeId`, base `SourceSchema`, column count).
    sources: RefCell<BTreeMap<String, (NodeId, SourceSchema, usize)>>,
    /// caller `query_id` → registration (keyed by the opaque id the JS layer supplies).
    queries: RefCell<BTreeMap<u64, QueryReg>>,
    /// The optimistic fork/rebase state: per-table `sync` baselines
    /// (`OPTIMISTIC-WRITES-DESIGN.md` §1). Tracked for every registered SYNCED table — one
    /// O(1) tree fork each; costs nothing unless a server batch runs. A local-only table is
    /// never tracked (`201-LOCAL-ONLY-TABLES-DESIGN.md` §3.1), which is exactly what keeps
    /// `rewind` from reverting it.
    optimistic: RefCell<OptimisticTables>,
    /// `Some` while a server-batch cycle is open ([`Db::server_batch_begin`] …
    /// [`Db::server_batch_end`]): every sink event buffers here — including those of
    /// in-cycle commits (the JS-driven pending re-invocations) — for the one raw
    /// per-query delivery at cycle end (§3; the stream is forwarded un-netted).
    cycle: RefCell<Option<CycleBuf>>,
}

impl DbInner {
    /// Drain every query's sink into the open cycle buffer (order-preserving).
    fn drain_into_cycle(&self, graph: &Graph, buf: &mut CycleBuf) {
        for (qid, reg) in self.queries.borrow().iter() {
            let changes = graph.take_sink_changes(reg.sink);
            if !changes.is_empty() {
                buf.entry(*qid).or_default().extend(changes);
            }
        }
    }

    /// The 09.8 `strict_i64` gate on an outgoing flat batch: a typed
    /// `unsupported_value` error naming the offending value. **Mandatory** since
    /// design 226 Stage C (§8): with the 222 SQL plane able to write full-range
    /// i64, an `Int` that cannot round-trip through f64 must never silently round
    /// on a JS boundary without a bigint lane. Stage E adds the bigint lane and
    /// makes this column-aware.
    fn check_js_safe(&self, changes: &[FlatChange]) -> Result<(), JsValue> {
        if let Some(i) = crate::js_safe::unsafe_int_in_flat_changes(changes) {
            return Err(strict_i64_error(i));
        }
        Ok(())
    }
}

/// The typed error a tripped `strict_i64` check throws (09.8's `ZqlError` analogue).
fn strict_i64_error(i: i64) -> JsValue {
    marshal::make_error(
        "unsupported_value",
        &format!("i64 {i} exceeds the JS safe-integer range (strict_i64)"),
    )
}

/// The in-memory live-query engine, callable from JavaScript. See the module docs.
#[wasm_bindgen]
pub struct Db {
    inner: Rc<DbInner>,
}

#[wasm_bindgen]
impl Db {
    #[wasm_bindgen(constructor)]
    #[allow(clippy::new_without_default)]
    pub fn new() -> Db {
        Db {
            inner: Rc::new(DbInner {
                graph: RefCell::new(Graph::new()),
                sources: RefCell::new(BTreeMap::new()),
                queries: RefCell::new(BTreeMap::new()),
                optimistic: RefCell::new(OptimisticTables::new()),
                cycle: RefCell::new(None),
            }),
        }
    }

    /// Register a base table from a `SchemaSpec` (`{ columns, primaryKey, sort? }` —
    /// [`marshal::schema_from_js`]; any `relationships` in the spec are ignored — nesting
    /// is query-local via `sub`). Seeds an empty shared source.
    ///
    /// `local` marks a **local-only** table (`201-LOCAL-ONLY-TABLES-DESIGN.md` §3.1): it is
    /// registered as a source but **NOT optimistically tracked** — so the §1.3 `rewind` never
    /// iterates it (it has no `sync` baseline to revert to), and a direct write to it persists
    /// across every server cycle (C1). A synced table tracks as before.
    ///
    /// Idempotent ONLY when the re-registration is truly identical — same schema **and** same
    /// locality (N1a). A genuine collision (one name claiming two different shapes/localities)
    /// errors loudly rather than silently no-op'ing, so the `__agg_*`/`_rindle_*` prefix ban
    /// (`createSchema`, N1) has a loud backstop instead of a silent one.
    #[wasm_bindgen(js_name = registerTable)]
    pub fn register_table(&self, table: &str, schema: JsValue, local: bool) -> Result<(), JsValue> {
        let sc = marshal::schema_from_js(schema)?;
        // N1a: a re-registration is idempotent only when truly identical. The existing locality
        // is read off the tracked set (a source is local iff it is NOT tracked).
        {
            let sources = self.inner.sources.borrow();
            if let Some((_, existing, _)) = sources.get(table) {
                let same_schema = existing.columns == sc.columns
                    && existing.primary_key == sc.primary_key
                    && existing.sort == sc.sort;
                let same_locality = self.inner.optimistic.borrow().is_tracked(table) != local;
                return if same_schema && same_locality {
                    Ok(())
                } else {
                    Err(marshal::make_error(
                        "table_conflict",
                        &format!(
                            "table {table:?} is already registered with a different schema or locality"
                        ),
                    ))
                };
            }
        }
        let cols = sc.columns.len();
        let sid = self
            .inner
            .graph
            .borrow_mut()
            .try_add_source(sc.clone(), Vec::new())
            .map_err(|e| marshal::rindle_error_to_js(&e))?;
        // A local table skips tracking: no `sync` fork, so `rewind` never reverts it (C1). A
        // synced table tracks one O(1) tree fork, as before.
        if !local {
            self.inner
                .optimistic
                .borrow_mut()
                .track(&self.inner.graph.borrow(), table, sid)
                .map_err(|e| marshal::rindle_error_to_js(&e))?;
        }
        self.inner
            .sources
            .borrow_mut()
            .insert(table.to_string(), (sid, sc, cols));
        Ok(())
    }

    /// Remove a base table previously added by [`register_table`](Self::register_table): free
    /// its source node, drop its optimistic baseline, and forget it from the source map — the
    /// inverse of `register_table`. For a SYNTHETIC aggregate table (`__agg_*`) whose last
    /// reading query has been destroyed (`AGGREGATE-SYNC-DESIGN.md` §4): aggregate state is
    /// reclaimed, not permanent. Idempotent for an unknown/already-removed table. Throws a JS
    /// `Error` if the table still has a live query connected to it — the caller must
    /// `destroy_query` every reader first (the optimistic/normalized backend refcounts so this
    /// holds).
    #[wasm_bindgen(js_name = unregisterTable)]
    pub fn unregister_table(&self, table: &str) -> Result<(), JsValue> {
        let sid = match self.inner.sources.borrow().get(table) {
            Some((sid, _, _)) => *sid,
            None => return Ok(()),
        };
        self.inner
            .graph
            .borrow_mut()
            .remove_source(sid)
            .map_err(|e| marshal::rindle_error_to_js(&e))?;
        self.inner.optimistic.borrow_mut().forget(table);
        self.inner.sources.borrow_mut().remove(table);
        Ok(())
    }

    /// Register a live query from a Zero-wire AST under the caller's opaque `query_id`.
    /// Lowers it into the shared graph, sinks it in a change-sink, and hydrates. Returns
    /// `{ queryId, comparatorVersion, schema, snapshot }` — the `hello` + hydrate
    /// snapshot for the JS `ArrayView` (snapshot cells **bare**). A malformed AST /
    /// unknown table / unknown column throws a JS `Error` carrying a `.kind`.
    pub fn query(&self, query_id: f64, ast_json: JsValue) -> Result<JsValue, JsValue> {
        let qid = query_id as u64;
        let mut ast: Ast = serde_wasm_bindgen::from_value(ast_json)
            .map_err(|e| marshal::make_error("invalid_ast", &e.to_string()))?;
        // One literal identity per query text on every home (design 226 §6):
        // serde-wasm-bindgen delivers >2^53 integral JS numbers as their BINARY f64;
        // re-key them onto the wire-token rule every JSON home parses.
        crate::ast::canonicalize_wire_number_lits(&mut ast);
        super::validate_correlations(&ast)?;

        let mut graph = self.inner.graph.borrow_mut();
        let sources = self.inner.sources.borrow();
        let resolve = |t: &str| sources.get(t).map(|(id, sc, _)| (*id, sc.clone()));

        // Record the slots this build creates so a failed build is cleaned up (not leaked)
        // and `destroy_query` can tear it down later. Mirrors `Engine::register_query`.
        graph.begin_recording();
        let top = match build_pipeline(&mut graph, &ast, &resolve) {
            Ok(t) => t,
            Err(e) => {
                let m = graph.take_recording();
                graph.destroy_pipeline(&m);
                return Err(marshal::build_error_to_js(&e));
            }
        };
        let vs = match view_schema(&ast, &resolve) {
            Ok(v) => v,
            Err(e) => {
                let m = graph.take_recording();
                graph.destroy_pipeline(&m);
                return Err(marshal::build_error_to_js(&e));
            }
        };
        let sink = graph.add_change_sink(top);
        graph.set_sink_edge(top, sink);
        // The cold drain is a `try_*` boundary: a parked leaf error or an
        // already-out-of-range §5.3 sum total surfaces here as the typed error, with
        // the partial pipeline reclaimed (which also clears the overflow state the
        // drain armed) — never a success whose aggregate cell is `NULL`.
        let initial = match graph.try_hydrate_change_sink(sink) {
            Ok(i) => i,
            Err(e) => {
                let m = graph.take_recording();
                graph.destroy_pipeline(&m);
                return Err(marshal::rindle_error_to_js(&e));
            }
        };
        let manifest = graph.take_recording();
        let wire = to_wire(&vs);
        drop(sources);
        drop(graph);

        let mut snapshot = flatten_all(&initial);
        project_flat_changes(&mut snapshot, &vs);
        if let Err(e) = self.inner.check_js_safe(&snapshot) {
            // The strict_i64 refusal must be as atomic as every build error above:
            // reclaim the pipeline, or the orphaned sink — registered in no
            // `QueryReg`, so `destroy_query` can never find it — buffers every
            // future commit's deltas unbounded.
            self.inner.graph.borrow_mut().destroy_pipeline(&manifest);
            return Err(e);
        }

        self.inner.queries.borrow_mut().insert(
            qid,
            QueryReg {
                sink,
                manifest,
                schema: vs,
            },
        );

        let out = js_sys::Object::new();
        marshal::js_set(&out, "queryId", &JsValue::from_f64(qid as f64));
        marshal::js_set(
            &out,
            "comparatorVersion",
            &JsValue::from_f64(COMPARATOR_VERSION as f64),
        );
        marshal::js_set(&out, "schema", &marshal::wire_schema_to_js(&wire));
        marshal::js_set(&out, "snapshot", &marshal::flat_changes_to_js(&snapshot));
        Ok(out.into())
    }

    /// Tear down a registered query: drop its sink + reclaim its pipeline (disconnect from
    /// the shared sources, free its slots — [`Graph::destroy_pipeline`]). Other queries
    /// and the shared sources are untouched. A no-op for an unknown id.
    #[wasm_bindgen(js_name = destroyQuery)]
    pub fn destroy_query(&self, query_id: f64) {
        if let Some(reg) = self.inner.queries.borrow_mut().remove(&(query_id as u64)) {
            self.inner
                .graph
                .borrow_mut()
                .destroy_pipeline(&reg.manifest);
        }
    }

    /// Open a write transaction. Stage row ops with [`WriteTxn::add`]/`remove`/`edit`,
    /// then [`WriteTxn::commit`] to apply them as one batch and get the per-query flat
    /// changes back. Dropping (or `rollback`) without committing applies nothing.
    pub fn write(&self) -> WriteTxn {
        WriteTxn {
            inner: self.inner.clone(),
            staged: RefCell::new(Vec::new()),
            forks: RefCell::new(HashMap::new()),
        }
    }

    /// Open a §1.3 reconcile cycle against the coherent server delta `deltas` —
    /// an array of base-table row ops `{ table, type: "add"|"remove"|"edit", row,
    /// old? }` (the cv-released normalized batch, bare cells). Runs the rewind
    /// (steps 1–4): the engine's input becomes the authoritative state `S'`, with
    /// every optimistic write un-applied and the server delta folded in; `sync`
    /// re-forks. Nothing is delivered yet — the JS side now **re-invokes each
    /// still-pending client mutator** (ordinary `write()`/`commit()` calls, whose
    /// events buffer into the cycle), then calls
    /// [`server_batch_end`](Db::server_batch_end) for the one coalesced delivery.
    ///
    /// Errors if a cycle is already open, on an unknown table, or on a push failure
    /// — after which the optimistic state is poisoned (a partial rewind may have
    /// applied): the only safe recovery is to discard this `Db` and re-hydrate.
    #[wasm_bindgen(js_name = serverBatchBegin)]
    pub fn server_batch_begin(&self, deltas: JsValue) -> Result<(), JsValue> {
        if self.inner.cycle.borrow().is_some() {
            return Err(marshal::make_error(
                "cycle_open",
                "a server batch is already open (call serverBatchEnd first)",
            ));
        }
        let arr = deltas.dyn_ref::<js_sys::Array>().ok_or_else(|| {
            marshal::make_error("invalid_batch", "expected an array of table row ops")
        })?;
        let mut map: BTreeMap<String, Vec<SourceChange>> = BTreeMap::new();
        {
            let sources = self.inner.sources.borrow();
            for item in arr.iter() {
                let table = js_sys::Reflect::get(&item, &JsValue::from_str("table"))
                    .ok()
                    .and_then(|t| t.as_string())
                    .ok_or_else(|| {
                        marshal::make_error("invalid_batch", "op is missing a string `table`")
                    })?;
                let cols = sources.get(&table).map(|(_, _, c)| *c).ok_or_else(|| {
                    marshal::make_error("unknown_table", &format!("no source `{table}`"))
                })?;
                map.entry(table)
                    .or_default()
                    .push(marshal::js_to_source_change(&item, cols)?);
            }
        }

        let graph = self.inner.graph.borrow();
        let mut buf = CycleBuf::new();
        if let Err(e) = self.inner.optimistic.borrow_mut().rewind(&graph, &map) {
            return Err(marshal::rindle_error_to_js(&e));
        }
        self.inner.drain_into_cycle(&graph, &mut buf);
        drop(graph);
        *self.inner.cycle.borrow_mut() = Some(buf);
        Ok(())
    }

    /// Close the open cycle: deliver the whole buffered event stream (rewind +
    /// re-invocations) per query, **in order**, as `[{ queryId, events: FlatChange[] }]`
    /// — one batch per affected query. The stream is forwarded RAW: the IVM sink emits
    /// changes in a valid incremental order (`FLAT-CHANGES-DESIGN.md` §5.4), so the
    /// receiver folds them in order to reach `view(head_new)`. The §3 "notify once"
    /// boundary is the view's own — one `applyChanges` per batch, settling before it
    /// notifies. Queries the cycle never touched are omitted.
    ///
    /// Note: a still-pending write whose re-invocation reproduced its prediction emits a
    /// balanced `remove`+`add` here. That nets to no observable change once the view
    /// settles, but it is no longer suppressed *at this boundary* (the netting/reordering
    /// that did so was the source of the coalescer correctness bugs). Restoring
    /// no-op-cycle ⇒ no-notify is a view-level optimization (Step 2 of the removal).
    #[wasm_bindgen(js_name = serverBatchEnd)]
    pub fn server_batch_end(&self) -> Result<JsValue, JsValue> {
        let Some(mut buf) = self.inner.cycle.borrow_mut().take() else {
            return Err(marshal::make_error(
                "no_cycle",
                "no server batch is open (call serverBatchBegin first)",
            ));
        };
        let graph = self.inner.graph.borrow();
        // Defensive final drain: a well-behaved caller committed every re-invocation,
        // but any straggler sink content still belongs to this cycle.
        self.inner.drain_into_cycle(&graph, &mut buf);

        let queries = self.inner.queries.borrow();
        // Two-phase, as in `WriteTxn::commit`: project + check the WHOLE cycle before
        // marshaling anything. A mid-loop strict_i64 trip would deliver the cycle to a
        // prefix of queries and silently drop it for the rest — after the §1.3 rewind
        // has already been applied, that desyncs every undelivered view. On a trip the
        // entire cycle is withheld uniformly and the caller re-hydrates.
        let mut projected: Vec<(u64, Vec<crate::flat::FlatChange>)> = Vec::new();
        let mut trip: Option<JsValue> = None;
        for (qid, events) in buf {
            let Some(reg) = queries.get(&qid) else {
                continue; // destroyed mid-cycle
            };
            let mut flat = flatten_all(&events);
            if flat.is_empty() {
                continue;
            }
            project_flat_changes(&mut flat, &reg.schema);
            if trip.is_none() {
                if let Err(e) = self.inner.check_js_safe(&flat) {
                    trip = Some(e);
                }
            }
            projected.push((qid, flat));
        }
        if let Some(e) = trip {
            return Err(e);
        }
        let out = js_sys::Array::new();
        for (qid, flat) in projected {
            let entry = js_sys::Object::new();
            marshal::js_set(&entry, "queryId", &JsValue::from_f64(qid as f64));
            marshal::js_set(&entry, "events", &marshal::flat_changes_to_js(&flat));
            out.push(&entry.into());
        }
        Ok(out.into())
    }
}

/// An open write transaction: a staging buffer of row ops applied atomically at
/// [`commit`](WriteTxn::commit). Nothing touches the engine until commit, so a dropped or
/// rolled-back txn is a clean no-op (no `Drop` undo needed).
#[wasm_bindgen]
pub struct WriteTxn {
    inner: Rc<DbInner>,
    staged: RefCell<Vec<(String, SourceChange)>>,
    /// `203-MUTATOR-READS-DESIGN.md` §4: lazy per-table read-cache forks, materialized
    /// **only** for `tx.query` (§5.2) and never for a write-only mutator. Each is an
    /// off-graph [`MemorySource`] equal to `live ⊕ this txn's buffer` for its table —
    /// seeded on the first `query` that reads the table (live COW fork + replay of that
    /// table's buffered ops, [`WriteTxn::ensure_fork`]) and kept current as later writes
    /// forward into it ([`WriteTxn::forward_to_fork`]). Held off-graph (not as graph
    /// nodes) so it just drops at `rollback`, and is released up front at `commit` (before
    /// the replay, so the live writes don't path-copy off these dead COW forks). The write
    /// path stays byte-for-byte unchanged and commit stays event-for-event unchanged
    /// (§6 / invariant 5).
    forks: RefCell<HashMap<String, MemorySource>>,
}

#[wasm_bindgen]
impl WriteTxn {
    /// Stage an add of `row` (a positional cell array, width-checked against the table's
    /// schema) to `table`.
    pub fn add(&self, table: &str, row: JsValue) -> Result<(), JsValue> {
        let cols = self.cols(table)?;
        let r = marshal::js_to_owned_row(&row, cols)?;
        let change = SourceChange::Add(r);
        self.forward_to_fork(table, &change);
        self.staged.borrow_mut().push((table.to_string(), change));
        Ok(())
    }

    /// Stage a remove of `row` from `table`.
    pub fn remove(&self, table: &str, row: JsValue) -> Result<(), JsValue> {
        let cols = self.cols(table)?;
        let r = marshal::js_to_owned_row(&row, cols)?;
        let change = SourceChange::Remove(r);
        self.forward_to_fork(table, &change);
        self.staged.borrow_mut().push((table.to_string(), change));
        Ok(())
    }

    /// Stage an edit of `old` → `new_row` in `table` (both width-checked).
    ///
    /// **Partial (optimistic) edit (`PROJECTION-SUPPORT-DESIGN.md` §8 / OQ-3).** An
    /// [`OwnedValue::Absent`] cell in `new_row` means *"not touching this column — leave it
    /// unchanged"*: it falls through to the current **effective** row (the live engine under
    /// this txn's staged overlay, i.e. what [`WriteTxn::get`] would return). The merge is
    /// resolved **here, at the staging boundary**, into a concrete full-width
    /// [`SourceChange::Edit`] whose `old` is that effective row — so operators, indexes, and
    /// the overlay never learn about merge semantics (they keep seeing whole rows), and a
    /// re-invocation after a server rebase re-reads the *new* effective row and re-merges.
    /// Presence is **monotonic**: a partial edit only *sets* columns (or leaves them), never
    /// narrows — narrowing is sync-only (§4.2). A full `new_row` (no `Absent`) stages verbatim,
    /// byte-identical to before (§7).
    pub fn edit(&self, table: &str, old: JsValue, new_row: JsValue) -> Result<(), JsValue> {
        let cols = self.cols(table)?;
        let old = marshal::js_to_owned_row(&old, cols)?;
        let row = marshal::js_to_owned_row(&new_row, cols)?;
        let change = if row.cells().any(|v| v.is_absent()) {
            // Compile `Absent`-as-unchanged away against the current effective row (the row's
            // PK cells are present — a mutator always supplies the key).
            match self.lookup_effective(table, &row) {
                Some(base) => {
                    let merged: Vec<OwnedValue> = (0..cols)
                        .map(|i| {
                            if row.col(i).is_absent() {
                                base.col(i).to_owned()
                            } else {
                                row.col(i).to_owned()
                            }
                        })
                        .collect();
                    SourceChange::Edit {
                        row: owned_row(merged),
                        old: base,
                    }
                }
                // No effective baseline to fall through to (editing a row that does not exist):
                // stage the caller's rows as-is — a degenerate case the source will reject.
                None => SourceChange::Edit { row, old },
            }
        } else {
            SourceChange::Edit { row, old }
        };
        self.forward_to_fork(table, &change);
        self.staged.borrow_mut().push((table.to_string(), change));
        Ok(())
    }

    /// The current **effective** full row for `probe`'s primary key — this txn's staged
    /// overlay (newest wins) over the live engine — or `None` if absent/removed. The shared
    /// read used by [`WriteTxn::get`]'s contract and the partial-edit merge in
    /// [`WriteTxn::edit`]. `probe` need only carry the PK cells at their columns.
    fn lookup_effective(&self, table: &str, probe: &OwnedRow) -> Option<OwnedRow> {
        let (sid, sc) = {
            let sources = self.inner.sources.borrow();
            let (s, sc, _) = sources.get(table)?;
            (*s, sc.clone())
        };
        let pk_eq = |row: &OwnedRow| {
            sc.primary_key
                .iter()
                .all(|&c| compare_values(probe.col(c), row.col(c)) == std::cmp::Ordering::Equal)
        };
        for (t, ch) in self.staged.borrow().iter().rev() {
            if t != table {
                continue;
            }
            match ch {
                SourceChange::Add(r) | SourceChange::Edit { row: r, .. } if pk_eq(r) => {
                    return Some(r.clone())
                }
                SourceChange::Remove(r) if pk_eq(r) => return None,
                _ => {}
            }
        }
        let graph = self.inner.graph.borrow();
        graph
            .memory_source(sid)
            .expect("registered tables are memory sources")
            .get_by_pk(probe)
    }

    /// The current row of `table` whose primary key equals `pk` (an array of the PK
    /// column cells, in `primaryKey` order), or `undefined`. Reads the live engine
    /// state **plus this transaction's own staged ops** (later staged ops win), so a
    /// read-dependent client mutator sees its prior writes — the §4.1 contract. During
    /// a server-batch cycle the live state is the rebased base (`S'` + the pending
    /// mutations re-invoked so far), which is exactly what re-invocation must read.
    pub fn get(&self, table: &str, pk: JsValue) -> Result<JsValue, JsValue> {
        let sources = self.inner.sources.borrow();
        let Some((sid, sc, cols)) = sources.get(table).map(|(s, sc, c)| (*s, sc.clone(), *c))
        else {
            return Err(marshal::make_error(
                "unknown_table",
                &format!("no source `{table}`"),
            ));
        };
        drop(sources);
        let pk_arr = pk.dyn_ref::<js_sys::Array>().ok_or_else(|| {
            marshal::make_error("invalid_row", "expected `pk` to be an array of key cells")
        })?;
        if pk_arr.length() as usize != sc.primary_key.len() {
            return Err(marshal::make_error(
                "invalid_row",
                &format!(
                    "pk has {} cells but the table key has {} columns",
                    pk_arr.length(),
                    sc.primary_key.len()
                ),
            ));
        }
        let _ = sid; // resolved again (with the schema) inside `lookup_effective`
                     // A full-width probe with the key cells in place (non-key cells ignored by
                     // the pk-only primary sort).
        let mut probe = vec![OwnedValue::Null; cols];
        for (i, &c) in sc.primary_key.iter().enumerate() {
            probe[c] = marshal::js_to_owned_value(&pk_arr.get(i as u32))?;
        }
        // The effective row = this txn's staged overlay (newest wins) over the live engine —
        // the same read the partial-edit merge uses (`lookup_effective`).
        Ok(match self.lookup_effective(table, &owned_row(probe)) {
            Some(r) => {
                if let Some(i) = crate::js_safe::unsafe_int_in_row(&r) {
                    return Err(strict_i64_error(i));
                }
                row_to_js(&r)
            }
            None => JsValue::UNDEFINED,
        })
    }

    /// Run a one-shot query (a `where`/`orderBy`/`limit`/join AST) over the state this
    /// transaction is mutating — the live base **plus** this txn's own staged writes-so-far
    /// (`203-MUTATOR-READS-DESIGN.md` §5.2; the same read-your-writes contract `get`
    /// provides, §4.1). Returns the query's rows as keyed JS objects with their materialized
    /// `related` children nested by name — presented identically to a `view.data` row of the
    /// same query ([`marshal::caught_node_to_js`]) — in the query's order.
    ///
    /// Mechanically this is [`Db::query`] minus the persisted `QueryReg`, pointed at the
    /// per-table read-cache forks (§4): for every table the AST reads it seeds a cache fork
    /// ([`ensure_fork`](Self::ensure_fork)) and registers a throwaway COW **fork-of-fork** of
    /// it as a *scratch* source node — `add_memory_source` takes the source by value, so the
    /// cache fork stays in `forks` for later reads/writes. It then builds a transient
    /// pipeline over those scratch nodes, hydrates the snapshot, and tears **everything**
    /// down — the pipeline (incl. its sink) and every scratch source — on **every** exit
    /// path, including a malformed-AST build failure (invariant 4). No durable sink is
    /// registered and nothing is delivered to any live query.
    pub fn query(&self, ast_json: JsValue) -> Result<JsValue, JsValue> {
        let mut ast: Ast = serde_wasm_bindgen::from_value(ast_json)
            .map_err(|e| marshal::make_error("invalid_ast", &e.to_string()))?;
        // One literal identity per query text on every home (design 226 §6):
        // serde-wasm-bindgen delivers >2^53 integral JS numbers as their BINARY f64;
        // re-key them onto the wire-token rule every JSON home parses.
        crate::ast::canonicalize_wire_number_lits(&mut ast);
        super::validate_correlations(&ast)?;

        // The tables the query reads — seed a read-cache fork for each (§4.1). Done before
        // the mutable graph borrow below: `ensure_fork` borrows the graph immutably.
        let mut tables: Vec<String> = Vec::new();
        collect_ast_tables(&ast, &mut tables);
        for t in &tables {
            self.ensure_fork(t)?;
        }

        let mut graph = self.inner.graph.borrow_mut();
        let sources = self.inner.sources.borrow();
        let forks = self.forks.borrow();

        // Register a throwaway COW fork-of-fork of each table's read-cache as a scratch
        // source node (§5.2 step 1). `scratch_ids` drives teardown on every exit path.
        let mut scratch: BTreeMap<String, (NodeId, SourceSchema)> = BTreeMap::new();
        let mut scratch_ids: Vec<NodeId> = Vec::new();
        for t in &tables {
            let Some((_, sc, _)) = sources.get(t) else {
                for id in &scratch_ids {
                    let _ = graph.remove_source(*id);
                }
                return Err(marshal::make_error(
                    "unknown_table",
                    &format!("no source `{t}`"),
                ));
            };
            let cache = forks.get(t).expect("ensure_fork seeded every read table");
            // Fork-of-fork carries the cache's indexes (incl. any §7.3 (A) secondary it holds)
            // via COW, so the scratch query reuses them rather than rebuilding on the throwaway.
            let sid = graph.add_memory_source(cache.fork_with_indexes());
            scratch.insert(t.clone(), (sid, sc.clone()));
            scratch_ids.push(sid);
        }
        let resolve = |t: &str| scratch.get(t).map(|(id, sc)| (*id, sc.clone()));

        // Build + hydrate the transient pipeline. On ANY failure, reclaim the partial
        // pipeline AND every scratch source before returning (invariant 4 / §5.2 step 5).
        graph.begin_recording();
        let top = match build_pipeline(&mut graph, &ast, &resolve) {
            Ok(t) => t,
            Err(e) => {
                let m = graph.take_recording();
                graph.destroy_pipeline(&m);
                for id in &scratch_ids {
                    let _ = graph.remove_source(*id);
                }
                return Err(marshal::build_error_to_js(&e));
            }
        };
        let vs = match view_schema(&ast, &resolve) {
            Ok(v) => v,
            Err(e) => {
                let m = graph.take_recording();
                graph.destroy_pipeline(&m);
                for id in &scratch_ids {
                    let _ = graph.remove_source(*id);
                }
                return Err(marshal::build_error_to_js(&e));
            }
        };
        let sink = graph.add_change_sink(top);
        graph.set_sink_edge(top, sink);
        // Fallible drain (§5.3): an out-of-range sum over the scratch rows must be the
        // typed error, not a `NULL` aggregate cell. Teardown below is unconditional
        // either way, so run it before surfacing the result.
        let hydrated = graph.try_hydrate_change_sink(sink);
        let manifest = graph.take_recording();
        graph.destroy_pipeline(&manifest);
        for id in &scratch_ids {
            graph
                .remove_source(*id)
                .map_err(|e| marshal::rindle_error_to_js(&e))?;
        }
        let initial = hydrated.map_err(|e| marshal::rindle_error_to_js(&e))?;
        drop(forks);
        drop(sources);
        drop(graph);

        // Marshal each hydrated node into a keyed JS object, recursing into its materialized
        // relationships by name (§5.2 step 4) — presenting identically to a `view.data` row of
        // the same query (projection, `.one()` unwrap, and scalar relationship aggregates all
        // honored per level by `caught_node_to_js`).
        let out = js_sys::Array::new();
        for change in initial {
            if let CaughtChange::Add(node) = change {
                if let Some(i) = crate::js_safe::unsafe_int_in_caught_node(&node) {
                    return Err(strict_i64_error(i));
                }
                out.push(&marshal::caught_node_to_js(&node, &vs));
            }
        }
        Ok(out.into())
    }

    /// Apply the staged ops (push through the shared sources, fanning to every dependent
    /// query), drain each affected query's sink, and return `[{ queryId, events:
    /// FlatChange[] }]` for the queries that changed (empties omitted, like
    /// `Publisher::commit`). Consumes the transaction.
    ///
    /// **Inside a server-batch cycle** (a pending mutator re-invocation between
    /// [`Db::server_batch_begin`] and [`Db::server_batch_end`]) the drained events
    /// buffer into the cycle instead and an **empty** array returns — the §3
    /// notify-once boundary: the whole cycle delivers exactly once, coalesced, at
    /// `serverBatchEnd`.
    pub fn commit(self) -> Result<JsValue, JsValue> {
        let staged = self.staged.take();
        // Drop the read-cache forks BEFORE writing the live sources (203 §4). They are COW forks
        // of the live trees, so leaving them alive across the replay would force every committed
        // write to path-copy its full B+tree path — even nodes that already diverged from the
        // optimistic `sync` baseline (which would otherwise mutate in place). Releasing them here
        // un-shares those nodes, so the replay pays only the bare `sync` copy-on-write minimum. A
        // no-op for a write-only mutator (the map is empty); the emitted events are unchanged.
        self.forks.borrow_mut().clear();
        let graph = self.inner.graph.borrow();
        let sources = self.inner.sources.borrow();
        let queries = self.inner.queries.borrow();

        for (table, change) in staged {
            let Some(src) = sources.get(&table).map(|(id, _, _)| *id) else {
                continue; // unknown table — validated at stage time; defensive skip
            };
            if let Err(e) = graph.try_source_push(src, change) {
                // Discard any partial sink buffers so a failed commit can't leak into the
                // next one (mirrors `Engine::apply_and_drain`'s discard-on-error).
                for reg in queries.values() {
                    let _ = graph.take_sink_changes(reg.sink);
                }
                return Err(marshal::rindle_error_to_js(&e));
            }
        }

        if let Some(buf) = self.inner.cycle.borrow_mut().as_mut() {
            drop(queries);
            self.inner.drain_into_cycle(&graph, buf);
            return Ok(js_sys::Array::new().into());
        }

        // Two-phase delivery: drain + project + check EVERY query, then marshal. A
        // strict_i64 trip must be atomic — erroring mid-loop would discard the
        // already-drained queries' deltas (view-after-write broken until re-hydrate)
        // while leaving undrained sinks to leak into the NEXT commit's delivery,
        // exactly what the push-failure discard above exists to prevent. On a trip
        // every sink is drained and NOTHING is delivered: the write is committed in
        // the engine, the caller re-hydrates its views.
        let mut drained: Vec<(u64, Vec<crate::flat::FlatChange>)> = Vec::new();
        let mut trip: Option<JsValue> = None;
        for (qid, reg) in queries.iter() {
            let changes = graph.take_sink_changes(reg.sink);
            if changes.is_empty() {
                continue;
            }
            let mut flat = flatten_all(&changes);
            project_flat_changes(&mut flat, &reg.schema);
            if trip.is_none() {
                if let Err(e) = self.inner.check_js_safe(&flat) {
                    trip = Some(e);
                }
            }
            drained.push((*qid, flat));
        }
        if let Some(e) = trip {
            return Err(e);
        }
        let out = js_sys::Array::new();
        for (qid, flat) in drained {
            let entry = js_sys::Object::new();
            marshal::js_set(&entry, "queryId", &JsValue::from_f64(qid as f64));
            marshal::js_set(&entry, "events", &marshal::flat_changes_to_js(&flat));
            out.push(&entry.into());
        }
        Ok(out.into())
    }

    /// Discard the staged ops without applying anything. (Nothing was applied, so this
    /// just consumes the handle; it exists for a symmetric, explicit API.)
    pub fn rollback(self) {}
}

impl WriteTxn {
    /// The column count for `table`, or a thrown `unknown_table` error.
    fn cols(&self, table: &str) -> Result<usize, JsValue> {
        self.inner
            .sources
            .borrow()
            .get(table)
            .map(|(_, _, c)| *c)
            .ok_or_else(|| marshal::make_error("unknown_table", &format!("no source `{table}`")))
    }

    /// `203` §4: keep an existing read-cache fork for `table` in step with a new staged op.
    /// A **no-op for a write-only mutator** (no fork was ever seeded) — the guarded
    /// one-liner the design's pay-for-use rests on (§7.1). Once a `tx.query` has seeded the
    /// fork, every subsequent `add`/`remove`/`edit` forwards into it so a later read stays
    /// consistent (invariant 5).
    fn forward_to_fork(&self, table: &str, change: &SourceChange) {
        if let Some(fork) = self.forks.borrow().get(table) {
            fork.apply_change(change);
        }
    }

    /// `203` §4.1: ensure a per-table read-cache fork exists for `table`, seeding it on the
    /// first `tx.query` that reads the table. Seeding is the O(1) COW fork of the live
    /// source plus a replay of *this table's* buffered ops in stage order — so the fork
    /// equals `live ⊕ this txn's buffer` for that table by construction (read-your-writes).
    /// Idempotent: a fork that already exists (kept current by `forward_to_fork`) is left
    /// untouched — never re-forked or re-seeded within an invocation (§7.2).
    fn ensure_fork(&self, table: &str) -> Result<(), JsValue> {
        if self.forks.borrow().contains_key(table) {
            return Ok(());
        }
        let sid = self
            .inner
            .sources
            .borrow()
            .get(table)
            .map(|(s, _, _)| *s)
            .ok_or_else(|| marshal::make_error("unknown_table", &format!("no source `{table}`")))?;
        let fork = {
            let graph = self.inner.graph.borrow();
            // §7.3 (A): carry the live source's secondary indexes (COW), so a later `tx.query`
            // sorting in an order a live query already built reuses that index instead of
            // rebuilding it. Kept current by `apply_change` during the replay below.
            graph
                .memory_source(sid)
                .expect("registered tables are memory sources")
                .fork_with_indexes()
        };
        for (t, ch) in self.staged.borrow().iter() {
            if t == table {
                fork.apply_change(ch);
            }
        }
        self.forks.borrow_mut().insert(table.to_string(), fork);
        Ok(())
    }
}

/// Every base table an `ast` reads — its root `table` plus every `related` subquery and
/// every `where`-EXISTS correlated subquery, recursively (`203` §5.2: the tables a
/// one-shot mutator query needs a read-cache fork for). Deduped, first-seen order; mirrors
/// the TS `collectTables` (`packages/optimistic/src/backend.ts`).
fn collect_ast_tables(ast: &Ast, out: &mut Vec<String>) {
    let name = ast.table.to_string();
    if !out.contains(&name) {
        out.push(name);
    }
    for rel in &ast.related {
        collect_ast_tables(&rel.subquery, out);
    }
    if let Some(cond) = &ast.r#where {
        collect_condition_tables(cond, out);
    }
}

/// Walk a filter tree collecting the tables of every EXISTS correlated subquery (recursing
/// through `and`/`or`). Helper for [`collect_ast_tables`].
fn collect_condition_tables(cond: &Condition, out: &mut Vec<String>) {
    match cond {
        Condition::And { conditions } | Condition::Or { conditions } => {
            for c in conditions {
                collect_condition_tables(c, out);
            }
        }
        Condition::CorrelatedSubquery(csc) => {
            collect_ast_tables(&csc.related.subquery, out);
        }
        Condition::Simple(_) => {}
    }
}

/// Per-level keep-mask for a projected view schema (`PROJECTION-SUPPORT-DESIGN.md` §6):
/// the columns this level reports = `select` ∪ the columns reconstruction needs (PK + sort).
/// `None` ⇒ a `'*'` level (no projection): keep everything, byte-identical to before (§7).
///
/// The PK/sort floor is unavoidable on the flat path — the JS `ArrayView` keys + binary-
/// searches rows by them, so they must survive even when `select` omits them; a consumer that
/// asked for only `select` still sees `select` plus that floor.
fn keep_mask(schema: &Schema) -> Option<Vec<bool>> {
    let proj = schema.projection.as_ref()?;
    let mut keep = vec![false; schema.columns.len()];
    let mut set = |c: usize| {
        if c < keep.len() {
            keep[c] = true;
        }
    };
    for &c in proj {
        set(c);
    }
    for &c in &schema.primary_key {
        set(c);
    }
    for &(c, _) in &schema.sort {
        set(c);
    }
    Some(keep)
}

/// Blank every non-kept cell to [`OwnedValue::Absent`] (→ JS `undefined`, omitted by the
/// `ArrayView`). Width-invariant — only cell *values* change.
fn project_row(row: &mut [OwnedValue], keep: &[bool]) {
    for (i, cell) in row.iter_mut().enumerate() {
        if !keep.get(i).copied().unwrap_or(true) {
            *cell = OwnedValue::Absent;
        }
    }
}

/// Project a [`WireNode`] subtree against its (hierarchical) view schema, recursing each
/// in-view relationship into its child schema.
fn project_node(node: &mut WireNode, schema: &Schema) {
    if let Some(keep) = keep_mask(schema) {
        project_row(&mut node.row, &keep);
    }
    for (rel, children) in node.rels.iter_mut() {
        if let Some(child_schema) = schema.rel_child(*rel) {
            for child in children.iter_mut() {
                project_node(child, child_schema);
            }
        }
    }
}

/// Project a batch of [`FlatChange`]s to each query level's reported columns (§5.2 / §6) —
/// the client analogue of the server's project-at-emit, applied to the local engine's flat
/// sink so a projected query never resolves a row from a column it did not select (even when
/// the shared base row carries that column for another query). A no-op for a `'*'` query.
fn project_flat_changes(changes: &mut [FlatChange], schema: &Schema) {
    for c in changes.iter_mut() {
        // Each `path` segment's parent row is at the level reached so far; descend by `rel`.
        let mut cur = schema;
        for seg in c.path.iter_mut() {
            if let Some(keep) = keep_mask(cur) {
                project_row(&mut seg.parent_row, &keep);
            }
            match cur.rel_child(seg.rel) {
                Some(child) => cur = child,
                None => break,
            }
        }
        // The op applies at the reached level `cur`.
        match &mut c.op {
            FlatOp::Add(node) => project_node(node, cur),
            FlatOp::Remove { row } => {
                if let Some(keep) = keep_mask(cur) {
                    project_row(row, &keep);
                }
            }
            FlatOp::Edit { old, new } => {
                if let Some(keep) = keep_mask(cur) {
                    project_row(old, &keep);
                    project_row(new, &keep);
                }
            }
        }
    }
}

/// A bare positional row → a JS array of bare cells.
fn row_to_js(row: &OwnedRow) -> JsValue {
    let arr = js_sys::Array::new();
    for v in row.cells() {
        arr.push(&marshal::value_to_js(v));
    }
    arr.into()
}
