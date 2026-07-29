//! # rindle — an incremental view maintenance (IVM) engine
//!
//! A Rust port of Zero's ZQL query/dataflow engine: you build a query, hydrate a
//! materialized view, push source changes, and the view updates incrementally — with
//! a wasm/JS client (`wasm` feature) and an optional SQLite server leaf (`sqlite`).
//! See the **Fault model** and **Client (wasm/JS)** sections below, and
//! `productionization/` for the road to production.
//!
//! ## Public entry points
//!
//! - **Build a query** — [`Query`] / [`table`] (fluent) or an [`Ast`] (Zero's wire
//!   JSON, via `serde`), then [`build_pipeline`] to lower it into a wired arena
//!   [`Graph`].
//! - **Run it** — hydrate the [`Graph`] over a [`MemorySource`] (default) or a SQLite
//!   `TableSource` (`sqlite` feature), push [`SourceChange`]s, and read the
//!   incrementally-maintained [`View`] / [`ViewData`].
//! - **Errors** — fallible paths return [`RindleError`] (see *Fault model* below).
//!
//! ## Feature flags
//!
//! `memory` (default, client) and `sqlite` (server) are the two source backends; at
//! least one must be enabled (the `compile_error!` below). They are not mutually
//! exclusive — enabling both compiles. `wasm` adds the JS client, `observe` the
//! tracing facade, `testkit` the test oracle.
//!
//! ## Design
//!
//! The arena + `NodeId` operator graph, RAII (`Drop`) cursor cleanup, the COW
//! `Arc<Node>` B+tree, and the reentrant-fetch-during-push thesis that motivates them
//! are written up in `docs/DESIGN.md`.
//!
//! # Fault model (productionization WS02)
//!
//! The engine is built to be **survivable in a server**: no data-reachable input or
//! transient backend error should abort the process or silently corrupt view state.
//!
//! - **Use the `try_*` entry points in production** — [`Graph::try_source_push`],
//!   [`Graph::try_hydrate`], [`Graph::try_add_source`] — which return [`RindleError`].
//!   Their infallible peers ([`Graph::source_push`] / [`Graph::hydrate`] /
//!   [`Graph::add_source`]) `.expect` the result and are for tests/prototyping.
//! - **Strict change validation** ([`Graph::set_validate_changes`], WS02.2): off by
//!   default (hot-path parity). When on, a malformed change stream (ADD of an
//!   existing row, REMOVE/EDIT of an absent row) returns
//!   [`RindleError::ConsistencyViolation`] — in release, not a stripped `debug_assert!`.
//! - **Ingest validation** (WS02.4): [`Graph::try_add_source`] rejects a wrong-width
//!   row with [`RindleError::SchemaViolation`] before it reaches an unchecked index.
//! - **Predicates never crash** on a column/literal type mismatch (WS02.4).
//! - **SQLite operator-storage errors** (the spill backend) park a [`RindleError`] on
//!   the graph's runtime-error sink and surface via `take_runtime_error` instead of
//!   `.expect`-aborting; spilled blobs carry a format-version byte and a mismatch is
//!   a typed error (rebuild-from-source), not a panic (WS02.3 / WS09.3).
//! - **Panic taxonomy** (the [`invariant`] module, WS02.1): every remaining panic is
//!   a build-time / internal invariant; data-reachable failures are [`RindleError`].
//! - **Server fault isolation** (WS02.5): build the server with
//!   `--profile release-server` (`panic = "unwind"`) and drive mutations through
//!   [`Graph::source_push_isolated`] to contain a residual panic to one mutation
//!   (then discard + rebuild the view — never reuse a torn one). The wasm client
//!   keeps `panic = "abort"` plus a console panic hook.

//! # Client (wasm/JS)
//!
//! With the `wasm` feature, `wasm::RindleView` drives the engine from JavaScript over a
//! 5-call lifecycle (the `wasm` module has the full rustdoc):
//!
//! 1. `RindleView.build(astJson, schemas, data)` — lower an AST (Zero's wire JSON) + a
//!    `{ table: SchemaSpec }` map (+ optional `{ table: row[][] }` initial data),
//!    hydrate, return a handle. `BuildError`/`RindleError` throw as a JS `Error` with a
//!    `.kind` tag — never a wasm abort.
//! 2. `view.data()` — the materialized tree as a JS value: `{ <col>: v, …, <relName>:
//!    [child…] | child | null }`, in-view relationships only.
//! 3. `view.push(table, { type: 'add'|'remove'|'edit', row, old? })` — one source
//!    change (does **not** flush).
//! 4. `view.flush()` — close the transaction; fire subscribers.
//! 5. `view.subscribe((data, resultType) => …)` — fired once immediately, then on
//!    every flush.
//!
//! Cells cross the boundary by JS runtime type (number→`f64`, with an i64 > 2^53
//! precision caveat; string→string; object/array→JSON string). Build the artifact
//! with `clients/wasm/build.sh`. **Concurrency:** one `Graph`/`RindleView` per thread
//! (it is `!Send`); scale with N independent graphs and message passing, never a
//! shared `Arc<Mutex<Graph>>`.

// Broken intra-doc links fail the build (WS08.6): the published rustdoc must not ship
// dangling references. CI also runs `RUSTDOCFLAGS="-D warnings" cargo doc` on both
// feature axes to catch the rest (e.g. redundant link targets).
#![deny(rustdoc::broken_intra_doc_links)]

#[cfg(not(feature = "memory"))]
compile_error!("enable the `memory` feature (the only built-in source backend; the SQLite backend lives in the `rindle-sqlite` crate)");

pub mod ast;
pub mod btree;
pub mod builder;
pub mod change;
/// Owned, fully-materialized change events off the dataflow pipeline
/// ([`CaughtChange`](changes::CaughtChange) / [`CaughtNode`](changes::CaughtNode) +
/// [`expand_change`](changes::expand_change)). Graduated out of the test oracle so the
/// production change-stream sink ([`Graph::add_change_sink`](graph::Graph::add_change_sink))
/// and out-of-crate consumers can use them. See `src/changes.rs`.
pub mod changes;
pub mod error;
/// Flat change events for cross-process / cross-language view reconstruction
/// ([`FlatChange`](flat::FlatChange) + [`flatten`](flat::flatten)): the nested
/// [`CaughtChange`](changes::CaughtChange) tree linearized into root→leaf paths so a
/// remote receiver can rebuild the exact `ArrayView`. See `src/flat.rs` and
/// `FLAT-CHANGES-DESIGN.md` (the wire format + receiver `apply_change` contract).
pub mod flat;
/// The flat-change **subscription protocol** ([`Hello`](flat_protocol::Hello) /
/// [`Batch`](flat_protocol::Batch) / [`Publisher`](flat_protocol::Publisher) /
/// [`Subscriber`](flat_protocol::Subscriber)): the batch envelope, epoch/seq framing,
/// and the receiver-side safety rules (in-order apply, gap → re-hydrate, at-most-once,
/// epoch/schema/comparator validation). See `src/flat_protocol.rs` and
/// `FLAT-CHANGES-DESIGN.md` §5.4/§5.5/§2.3.
pub mod flat_protocol;
/// A reference [`Receiver`](flat_receiver::Receiver): a host-agnostic port of the
/// View's `apply_change`, fed by [`FlatChange`](flat::FlatChange)s, that reconstructs
/// the exact `ArrayView` tree (rc multi-path + edit-move, no COW). The executable form
/// of `FLAT-CHANGES-DESIGN.md` §6 and the conformance oracle. See `src/flat_receiver.rs`.
pub mod flat_receiver;
pub mod graph;
/// Panic taxonomy + the `invariant!` macro (WS02.1): the one rule separating
/// build-time/internal invariants (keep as panic) from data-reachable failures
/// (convert to [`error::RindleError`]). See `src/invariant.rs`.
pub mod invariant;
/// The cross-process **journal frame envelope**
/// ([`FrameHeader`](journal_frame::FrameHeader) + [`FrameKind`](journal_frame::FrameKind)):
/// the typed header (kind / committed_at / run identity + totals) in front of an opaque
/// change payload wherever journal entries live — the hctree master's `hct_journal`
/// payload column and the S3 archival segments (`rindle-backup`). Pure bytes, no deps.
/// See `src/journal_frame.rs`, `designs-implemented/211-HCTREE-LEADER-CDC-DESIGN.md` §4.1, and
/// `designs-implemented/212-JOURNAL-S3-SHIPPING-DESIGN.md` §2.2.
pub mod journal_frame;
/// JS-boundary safe-integer walkers (productionization 09.8, design 226 Stage A): the
/// opt-in `strict_i64` check that refuses an out-of-`Number.MAX_SAFE_INTEGER` `Int`
/// crossing to JS with a typed error instead of silently rounding it.
pub mod js_safe;
pub mod memory_source;
/// Build-gated process metrics (WS03, the `metrics` feature): `metric_inc!`/… fold
/// each seam into a relaxed atomic add on a process-global registry the daemon's
/// Prometheus endpoint reads, and to argument-consuming no-ops (no global linked)
/// otherwise. The scrape-path sibling of `observe`; see `src/metrics.rs`.
pub mod metrics;
/// Feature-gated observability shim (WS03.1): `obs_span!`/`obs_event!`/… expand to
/// `tracing` when `--features observe`, and to argument-consuming no-ops (no
/// `tracing` dependency) otherwise — keeping the default/wasm build lean.
pub mod observe;
pub mod op;
/// The optimistic-writes fork/rebase loop ([`OptimisticTables`](optimistic::OptimisticTables)
/// — per-table `sync` forks + the §1.3 rewind over the one live pipeline). The cycle's
/// buffered event stream is forwarded raw to the consumer (the §3 `Coalescer` was removed —
/// see the module docs). See `src/optimistic.rs` and `OPTIMISTIC-WRITES-DESIGN.md`.
pub mod optimistic;
pub mod predicate;
/// The guarded push fan-out reverse index (`designs/205-GUARDED-PUSH-FANOUT-DESIGN.md`):
/// prunes a source write's per-connection fan-out to the connections whose
/// equality-shaped `where` guard could match. A conservative superset index —
/// never under-approximates — so `filter_push` stays the exact gate.
pub mod push_index;
pub mod query;
pub mod scalar;
pub mod source_common;
pub mod storage;
/// The spec-`11` operator test harness (`Catch`/`Snitch`/runners/fingerprint).
/// Exported only for crate tests or the `testkit` cargo feature so production
/// library builds do not expose harness helpers. Use via `rindle::testkit::*`.
#[cfg(any(test, feature = "testkit"))]
pub mod testkit;
pub mod value;
/// The production materialization sink (`09`): the `Arc`-shared, reference-stable
/// `ArrayView` + immutable tree differ `apply_change`. See `src/view.rs`.
pub mod view;
/// The wasm/JS client boundary (WS01): `RindleView` + value/error/schema/view-tree
/// marshalling. Only compiled for the `wasm` feature. See `src/wasm/`.
#[cfg(feature = "wasm")]
pub mod wasm;
/// The view schema on the wire + content fingerprint
/// ([`WireSchema`](wire_schema::WireSchema) / [`schema_fp`](wire_schema::schema_fp) +
/// [`COMPARATOR_VERSION`](wire_schema::COMPARATOR_VERSION)): the hierarchical view
/// [`Schema`](value::Schema) reduced to what a receiver needs (names + resolved sort +
/// nested relationships), shipped once. See `src/wire_schema.rs` and
/// `FLAT-CHANGES-DESIGN.md` §5.2/§5.5.
pub mod wire_schema;

// Crate-root re-export of the observability shim macros (WS03.1 step) so the
// instrumented seams (WS03.2–03.5) can call `obs_span!`/… without a module path.
// Kept `pub(crate)` — these are internal instrumentation, not public API (WS01.7
// audits the public surface). Unreferenced until the seams land.
#[allow(unused_imports)]
pub(crate) use observe::{obs_counter_inc, obs_event, obs_gauge_set, obs_span};

// Crate-root re-export of the metrics instrumentation macros so the seams can call
// `metric_inc!`/… unqualified. `pub(crate)` — internal instrumentation, not public API.
#[allow(unused_imports)]
pub(crate) use metrics::{
    metric_add, metric_build_err, metric_change_kind, metric_changes_inc, metric_inc, metric_timer,
};

// ===========================================================================
// Public API — the deliberately-reviewed surface (WS01.7).
//
// The query you build (`Ast`/`Query`/`table`), the engine you drive (`Graph` +
// `try_*`), the values you push (`OwnedValue`/`SourceChange`/`Schema`), the view
// you read (`View`/`ViewData`/`Entry`), and the error you handle (`RindleError`).
// Engine-internal plumbing that must remain `pub` (it is a return type, or the
// manual-wiring tests use it) but is NOT part of the supported API is grouped
// under `#[doc(hidden)]` below, so the rendered docs / `cargo public-api` show only
// the surface above.
// ===========================================================================

pub use ast::{
    Aggregate, Ast, Bound, Condition, CorrelatedSubquery, CorrelatedSubqueryCondition, Correlation,
    Dir, ExistsOp, Lit, Op, OrderPart, SimpleCondition, System, ValuePosition,
};
pub use builder::{build_pipeline, view_schema, BuildError};
pub use change::{Change, SourceChange};
pub use changes::{expand_change, expand_node, CaughtChange, CaughtNode};
pub use error::RindleError;
pub use flat::{flatten, flatten_all, FlatChange, FlatOp, PathSeg, WireNode, WireRow};
pub use flat_protocol::{
    Applied, Batch, Hello, ProtocolError, Publisher, SnapStatus, SnapshotChunk, Subscriber,
};
pub use flat_receiver::{Receiver, RecvNode};
pub use graph::{Graph, NodeId, PipelineManifest};
pub use memory_source::MemorySource;
pub use predicate::LikeMatcher;
pub use query::{
    table, Cond, ExistsOpts, IntoDir, IntoLit, IntoOp, IntoRhs, Parent, ParentRow, Query, Rhs,
};
pub use scalar::{has_scalar_subquery, resolve_scalars, ScalarCatalog, ScalarSource};
pub use storage::{
    MemoryStorage, ReduceAcc, Storage, StorageFactory, StorageProvider, StorageValue,
};
pub use value::{
    owned_row, ColId, OwnedRow, OwnedValue, RelDef, RelId, ScalarProjection, Schema, Sort,
    SourceSchema, Value, ValueType,
};
pub use view::{Entry, EntryList, Listener, ResultType, View, ViewChange, ViewData};
pub use wire_schema::{
    schema_fp, to_schema, to_wire, SchemaFp, WireRel, WireSchema, COMPARATOR_VERSION,
};

// --- internal plumbing: `pub` (return types / manual-wiring tests depend on it)
//     but excluded from the documented / `cargo public-api` surface (WS01.7). ---
#[doc(hidden)]
pub use builder::{
    complete_ordering, create_predicate, normalize_pipeline_ast, schema_primary_key_names,
    transform_filters,
};
#[doc(hidden)]
pub use change::{ChangeType, Constraint, FetchRequest, MultiConstraint, Node, OutEdge, Port};
#[doc(hidden)]
pub use graph::{
    CollectedChange, ConnId, CountingSource, EmitCounter, Source, SourceLeaf, StorageId,
};
#[doc(hidden)]
pub use predicate::{CmpOp, CompiledPredicate, ValueSet};
#[doc(hidden)]
pub use source_common::{
    ConnTable, Connection, ConnectionFilters, Operand, Overlay, Overlays, RowPredicate,
    SqlCondition, SqlOp,
};
#[doc(hidden)]
pub use storage::{create_storage, OwnedStorageValue};
#[doc(hidden)]
pub use value::{
    compare_rows, compare_values, same_pk, values_equal, values_identical, RowRef, RowStream,
};
#[doc(hidden)]
pub use view::{apply_change, EntryId, EntryListInner, Mutate, TxnDirty, TxnGen, REL_ROOT};
