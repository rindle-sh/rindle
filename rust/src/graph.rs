//! Primitive #1: the operator graph as an **arena + `NodeId`**, NOT
//! `Rc<RefCell<dyn Operator>>`.
//!
//! Why this is the make-or-break decision (verified against `join.ts`):
//! `#pushChildChange` sets in-progress state then re-enters `parent.fetch()`
//! *while a push is in flight*, and self-joins put the same source in the graph
//! twice. The "obvious" `Rc<RefCell<dyn Operator>>` translation would hit a
//! runtime `BorrowMutError` on exactly that pattern. The arena fixes it: the
//! whole pipeline is one `Vec<Operator>` addressed by `NodeId`, every
//! fetch/push takes `&self` (a *shared* borrow of the graph), and per-operator
//! mutable state lives behind `Cell`/`RefCell` scoped to a single statement —
//! never held across a vend. Reentrant fetch-during-push is then just another
//! shared borrow. No cycles-of-owners, no borrow conflict.
//!
//! The leaf [`MemorySource`] (the connection/read/COW machinery) lives in
//! `memory_source.rs`; this module hosts the operator graph that wires it up
//! (`SourceConn`, `Join`, `View`, `Collector`) and drives the eager push fan-out
//! ([`Graph::source_push`]), which is inherently graph-level (it must reach
//! downstream operators by `NodeId`).

use std::cell::{Cell, RefCell};
use std::rc::Rc;

use crate::change::{
    build_join_constraint, materialize_change_edit_old_row_only,
    materialize_change_preserving_node, rebuild_change, Change, ChangeType, Constraint,
    FetchRequest, JoinOverlay, Node, NodeStream, OutEdge, Port, Relationship, RowFlow,
    SourceChange,
};
use crate::error::RindleError;
use crate::memory_source::MemorySource;
use crate::op::{is_join_match, row_equals_for_compound_key};
use crate::predicate::CompiledPredicate;
use crate::source_common::{Connection, ConnectionFilters};
use crate::storage::{Storage, StorageFactory};
use crate::{metric_change_kind, metric_changes_inc, metric_inc, metric_timer};
// `Row` aliases the canonical owned row; the operator graph is all owned values
// (it buffers, overlays, and materializes rows), so no `Value<'a>` appears here.
use crate::value::{
    compare_rows, compare_values, same_pk, ColId, OwnedRow as Row, RelId, Schema, Sort,
    SourceSchema,
};

/// How many fan-out loop iterations pass between wall-clock reads at a push-deadline
/// checkpoint ([`Graph::push_deadline_exceeded`], FOLLOWER-LAG-SHED §6.6): amortizes the
/// ~tens-of-ns clock read to nothing while still bailing within O(1024 · per-iteration cost)
/// of the deadline.
const DEADLINE_CHECK_EVERY: u32 = 1024;

/// A handle to an operator slot in the [`Graph`] arena. `idx` is the slot index;
/// `gen` is the slot's **generation**, bumped every time the slot is freed (a
/// pipeline teardown — [`Graph::destroy_pipeline`]). The generation is what makes
/// slot REUSE safe: a stale handle to a torn-down pipeline carries the old `gen`, so
/// it fails the generation check in `Graph::node` instead of silently aliasing the
/// new tenant of a recycled slot. Distinct from physical removal (which would shift
/// indices and is impossible here) — the slot keeps its index across reuse.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct NodeId {
    pub idx: u32,
    pub gen: u32,
}

impl NodeId {
    /// Construct a raw handle from a slot index and generation. The graph vends
    /// `NodeId`s from `add`; this is for the few call sites that wire a known slot
    /// directly (the builder/testkit and integration tests that reference a
    /// freshly-added node at generation 0).
    pub fn new(idx: u32, gen: u32) -> NodeId {
        NodeId { idx, gen }
    }
    fn ix(self) -> usize {
        self.idx as usize
    }
}

/// Index into the graph's **parallel storage arena** (foundations §6.4). A
/// stateful operator (`Take`/`Cap`/`Exists`, spec `07`) is handed one of these at
/// build time and reads/writes its scratch state via `Graph::storage`. The
/// operator never owns its store: the indirection is what lets the **client** keep
/// it in RAM ([`MemoryStorage`](crate::storage::MemoryStorage)) while the
/// **server** later spills it to SQLite (`10` §1.1) without touching operator code.
/// Distinct newtype from [`NodeId`] so the two index spaces can't be confused.
/// Generational for the same reason (storage slots are freed + reused on teardown).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct StorageId {
    pub idx: u32,
    pub gen: u32,
}

impl StorageId {
    fn ix(self) -> usize {
        self.idx as usize
    }
}

// ---------------------------------------------------------------------------
// The Source trait — the connection/read seam both backends implement
// ---------------------------------------------------------------------------

/// A connection handle into a source's connection table. Generational `{idx, gen}`
/// (like [`NodeId`]/[`StorageId`]) so a source can RECYCLE a torn-down connection's
/// slot — the [`ConnTable`](crate::source_common::ConnTable) bumps the generation on
/// free and checks it on every access, so a stale handle to a recycled slot fail-fasts.
/// Distinct from [`NodeId`]. Same role as `05`'s `ConnId`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ConnId {
    pub idx: u32,
    pub gen: u32,
}

impl ConnId {
    pub fn new(idx: u32, gen: u32) -> ConnId {
        ConnId { idx, gen }
    }
}

/// The connection + read contract a leaf backend owns locally (`04` §4.7 /
/// `05` §4.8). [`MemorySource`] implements it; the SQLite `TableSource` implements
/// the *same* trait, so `connect`/`fetch`/`schema`/`destroy` — and everything
/// downstream of the `NodeStream` they vend — are backend-identical.
///
/// The *eager push* fan-out is inherently graph-level (it drives downstream
/// operators by `NodeId`), so it lives in [`Graph::source_push`] + the source's
/// [`MemorySource::push`]; the trait scopes to what a source answers from `&self`.
pub trait Source {
    /// Register a new connection (one downstream output). `sort = None` ⇒
    /// unordered. Self-joins call this twice. Builds the [`Connection`] from the
    /// (07-compiled) filter spec + split-edit keys, and asserts the ordering
    /// includes the PK when ordered. Mirrors `connect` (`memory-source.ts:162`).
    fn connect(
        &self,
        sort: Option<Sort>,
        filters: Option<ConnectionFilters>,
        split_edit_keys: Vec<ColId>,
    ) -> ConnId;

    /// Lazy pull for `conn`: a stream of **rows** in (reverse-aware) sort order,
    /// overlay-spliced, start-gated, constraint-trimmed, filtered. A source emits
    /// rows; the `SourceConn` operator wraps them in leaf nodes (`Graph::fetch`).
    fn fetch<'g>(&'g self, conn: ConnId, req: &FetchRequest) -> RowFlow<'g>;

    /// The effective sort for a connection. This can differ from the table schema's
    /// default/primary sort, and ordered downstream operators must use this.
    fn conn_sort(&self, conn: ConnId) -> Sort;

    /// The schema of rows this source vends.
    fn schema(&self) -> &Schema;

    /// Drop a connection's downstream edge so it stops receiving pushes. Does NOT
    /// delete the backing indexes (§3.10).
    fn destroy(&self, conn: ConnId);

    /// Open-cursor count — `0` ⇒ no cursor is mid-iteration and the connection is
    /// free for a write.
    fn cursors_open(&self) -> i64;

    /// Eager fallible push: fan `change` to every connection (overlay live,
    /// epoch-gated), clear the overlay, then write. `push_one` is the graph's
    /// downstream driver. When `strict`, a malformed change returns a typed
    /// [`RindleError`] instead of a `debug_assert`. (A backend may keep a faster
    /// infallible `push` as an inherent method; this is the object-safe seam the
    /// graph drives.)
    fn try_push(
        &self,
        change: SourceChange,
        push_one: &dyn Fn(&Connection, SourceChange),
        strict: bool,
    ) -> Result<(), RindleError>;

    /// Take any error a cursor parked during the last drained fetch (`05` §4.5);
    /// `None` if it drained cleanly. Infallible backends always return `None`.
    fn take_error(&self) -> Option<RindleError>;

    /// Wire a connection's downstream output edge (mirrors `input.setOutput`).
    fn set_conn_output(&self, conn: ConnId, edge: OutEdge);
}

/// The source leaf stored in the arena.
///
/// The in-memory backend keeps its own concrete [`Memory`](SourceLeaf::Memory) variant
/// so the client/wasm hot path stays statically dispatched. Any other backend — e.g. the
/// SQLite `TableSource` from the `rindle-sqlite` crate — is erased behind the object-safe
/// [`Source`] trait via [`Dyn`](SourceLeaf::Dyn). The vtable hop is per connect/fetch,
/// never per row (`fetch` already returns a boxed [`RowFlow`]).
///
// The `Memory` variant is deliberately unboxed (the static client/wasm hot path);
// `Dyn` is a fat pointer. The size asymmetry is intentional and harmless — there is
// one `SourceLeaf` per source node (a handful per graph), so boxing `Memory` would
// only add a pointless heap indirection on the hot path.
#[allow(clippy::large_enum_variant)]
pub enum SourceLeaf {
    Memory(MemorySource),
    Dyn(Box<dyn Source>),
}

impl Source for SourceLeaf {
    fn connect(
        &self,
        sort: Option<Sort>,
        filters: Option<ConnectionFilters>,
        split_edit_keys: Vec<ColId>,
    ) -> ConnId {
        match self {
            SourceLeaf::Memory(s) => s.connect(sort, filters, split_edit_keys),
            SourceLeaf::Dyn(s) => s.connect(sort, filters, split_edit_keys),
        }
    }

    fn fetch<'g>(&'g self, conn: ConnId, req: &FetchRequest) -> RowFlow<'g> {
        match self {
            SourceLeaf::Memory(s) => s.fetch(conn, req),
            SourceLeaf::Dyn(s) => s.fetch(conn, req),
        }
    }

    fn conn_sort(&self, conn: ConnId) -> Sort {
        match self {
            SourceLeaf::Memory(s) => s.conn_sort(conn),
            SourceLeaf::Dyn(s) => s.conn_sort(conn),
        }
    }

    fn schema(&self) -> &Schema {
        match self {
            SourceLeaf::Memory(s) => s.schema(),
            SourceLeaf::Dyn(s) => s.schema(),
        }
    }

    fn destroy(&self, conn: ConnId) {
        match self {
            SourceLeaf::Memory(s) => s.destroy(conn),
            SourceLeaf::Dyn(s) => s.destroy(conn),
        }
    }

    fn cursors_open(&self) -> i64 {
        match self {
            SourceLeaf::Memory(s) => s.cursors_open(),
            SourceLeaf::Dyn(s) => s.cursors_open(),
        }
    }

    fn try_push(
        &self,
        change: SourceChange,
        push_one: &dyn Fn(&Connection, SourceChange),
        strict: bool,
    ) -> Result<(), RindleError> {
        match self {
            // Non-strict memory keeps the infallible hot-path `push` verbatim; strict
            // routes through the fallible sibling so a bad change returns a typed error.
            SourceLeaf::Memory(s) => {
                if strict {
                    s.try_push(change, push_one, true)
                } else {
                    s.push(change, push_one);
                    Ok(())
                }
            }
            SourceLeaf::Dyn(s) => s.try_push(change, push_one, strict),
        }
    }

    fn take_error(&self) -> Option<RindleError> {
        match self {
            SourceLeaf::Memory(_) => None,
            SourceLeaf::Dyn(s) => s.take_error(),
        }
    }

    fn set_conn_output(&self, conn: ConnId, edge: OutEdge) {
        match self {
            SourceLeaf::Memory(s) => s.set_conn_output(conn, edge),
            SourceLeaf::Dyn(s) => s.set_conn_output(conn, edge),
        }
    }
}

// ---------------------------------------------------------------------------
// CountingSource — a diagnostic decorator (the `analyze query` emit counter)
// ---------------------------------------------------------------------------

/// A shared per-source counter of rows a source emitted into the pipeline. One cell
/// per wrapped source node (so the count is already keyed per `NodeId`); a
/// child-driven join that re-fetches the same leaf under many constraints accumulates
/// across those fetches into the same cell. `Rc<Cell<..>>` (not atomic) — a `Graph`
/// is `!Send`, single-threaded by construction.
pub type EmitCounter = Rc<Cell<u64>>;

/// A [`Source`] decorator that counts the rows crossing the connection boundary — the
/// **one new measurement** the `analyze query` diagnostic needs
/// (`designs-implemented/ANALYZE-QUERY-DESIGN.md` §3.4). It wraps any object-safe [`Source`] and
/// delegates every method to `inner` unchanged, except [`fetch`](Source::fetch), whose
/// returned `RowFlow` is wrapped in a lazy `.inspect` that bumps `emitted` once per row
/// **as the row is pulled**. Because it counts `inner.fetch()`'s *final* stream — after
/// the backend's overlay/start/constraint/filter chain and across both the ordered and
/// unordered fetch paths — it sees exactly what the pipeline sees, on either backend
/// (SQLite or memory), and a downstream `Take` that abandons the stream early counts
/// only what it consumed (the correct "emitted into the pipeline" semantics).
///
/// This is installed **only** on the throwaway pipeline `analyze_query` builds; the
/// live `Graph::fetch` and every live source stay byte-for-byte unchanged — there is no
/// branch or wrapper on any steady-state fetch or push. The only per-analyze cost is
/// one vtable hop plus one `Cell` increment per emitted row.
pub struct CountingSource {
    inner: Box<dyn Source>,
    emitted: EmitCounter,
}

impl CountingSource {
    /// Wrap `inner`; rows it emits are tallied into `emitted` (a cell the caller keeps a
    /// clone of, to read the total back after hydration).
    pub fn new(inner: Box<dyn Source>, emitted: EmitCounter) -> CountingSource {
        CountingSource { inner, emitted }
    }
}

impl Source for CountingSource {
    fn connect(
        &self,
        sort: Option<Sort>,
        filters: Option<ConnectionFilters>,
        split_edit_keys: Vec<ColId>,
    ) -> ConnId {
        self.inner.connect(sort, filters, split_edit_keys)
    }

    fn fetch<'g>(&'g self, conn: ConnId, req: &FetchRequest) -> RowFlow<'g> {
        let n = self.emitted.clone();
        Box::new(
            self.inner
                .fetch(conn, req)
                .inspect(move |_| n.set(n.get() + 1)),
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
        push_one: &dyn Fn(&Connection, SourceChange),
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

// ---------------------------------------------------------------------------
// Operators
// ---------------------------------------------------------------------------

/// Hierarchical join (parent rows get a lazily-streamed `rel_name` relationship
/// of matching child rows). Mirrors `join.ts`.
pub struct Join {
    pub parent: NodeId,
    pub child: NodeId,
    pub parent_key: Vec<ColId>,
    pub child_key: Vec<ColId>,
    /// The relationship slot this join attaches (resolved from its name against the
    /// parent schema at build time — `08`). Index-addressed on the hot path
    /// (foundations §3.4); the name is recoverable via the parent
    /// `Schema::relationships[rel_slot]`.
    pub rel_slot: RelId,
    /// The downstream edge as a **port-carrying** [`OutEdge`] (like a
    /// [`SourceConn`]'s output), so a join can feed another join: `Port::Single`
    /// to a terminal sink, `Port::JoinParent` when this join is the *parent* of the
    /// next (sibling relationships stacked on one row), or `Port::JoinChild` when
    /// this join is a *nested child* feeding the parent join's child port. Both
    /// `join_push` and `push_child_change` forward on `out.port`.
    pub output: Cell<Option<OutEdge>>,
    /// The in-flight child change being fanned out by `Graph::push_child_change`
    /// — the LIVE port of Zero's `Join.#inprogressChildChange` (`join.ts:61`). Read
    /// by the child-relationship thunk on EVERY fetch (including the reentrant
    /// refetch a downstream `Take`/`Exists` triggers mid-push), so a not-yet-
    /// processed parent's relationship reflects PRE-change (stale) membership — the
    /// cascade the depth-2 EXISTS+top-N+push shapes need. Owned [`JoinOverlay`]
    /// (a row-level `SourceChange`, no `'g`) so it lives in the arena. `None` outside
    /// a child-push. See `Graph::join_overlay_for`.
    pub inprogress_overlay: RefCell<Option<JoinOverlay>>,
    /// The row of the parent currently being processed in the child-push fan-out —
    /// Zero's `#inprogressChildChangePosition` (`join.ts:62`). The overlay applies
    /// only to parents sorting strictly AFTER this (`compareRows(parent, pos) > 0`):
    /// parents at-or-before it have already had the change delivered. `None` until
    /// the first parent of a fan-out is reached.
    pub inprogress_position: RefCell<Option<Row>>,
}

/// A connection (one of a source's outputs). Self-joins create two of these over
/// the same source. Holds the full generational [`ConnId`] (not a bare index) so a
/// teardown disconnects the exact connection slot and a recycled slot can never be
/// mistaken for it.
pub struct SourceConn {
    pub source: NodeId,
    pub conn: ConnId,
}

/// A change recorded by a [`Collector`] — the spike's analogue of `Catch`'s
/// `pushes`. Lets a test assert the exact change sequence a source fans out.
/// Equality compares rows cell-by-cell via [`compare_values`] (null == null) —
/// `OwnedValue` deliberately has no derived `PartialEq` (it would invite the
/// wrong comparator), so we spell the row equality out.
#[derive(Clone, Debug)]
pub enum CollectedChange {
    Add(Row),
    Remove(Row),
    Edit { row: Row, old: Row },
}

/// Wrap a source `SourceChange` (rows) into a node-bearing downstream `Change`
/// (`makeAddChange`/`makeRemoveChange`/`makeEditChange`). Leaf nodes (no rels) —
/// this IS the row→node conversion, localized to the `SourceConn` write boundary.
/// Takes the change by value: the rows move into the nodes, no clone.
/// `pub(crate)`: the out-of-file [`FlippedJoin`](crate::op::FlippedJoin) builds the
/// nested `child` node of a forwarded `Change::Child` the same way.
pub(crate) fn source_change_to_node<'g>(change: SourceChange) -> Change<'g> {
    match change {
        SourceChange::Add(r) => Change::Add(Node::leaf(r)),
        SourceChange::Remove(r) => Change::Remove(Node::leaf(r)),
        SourceChange::Edit { row, old } => Change::Edit {
            node: Node::leaf(row),
            old: Node::leaf(old),
        },
    }
}

/// Cell-by-cell row equality for test assertions (`compare_values`, null == null).
fn full_row_eq(a: &Row, b: &Row) -> bool {
    a.len() == b.len()
        && (0..a.len()).all(|i| compare_values(a.col(i), b.col(i)) == std::cmp::Ordering::Equal)
}

impl PartialEq for CollectedChange {
    fn eq(&self, other: &Self) -> bool {
        use CollectedChange::*;
        match (self, other) {
            (Add(a), Add(b)) | (Remove(a), Remove(b)) => full_row_eq(a, b),
            (Edit { row: r1, old: o1 }, Edit { row: r2, old: o2 }) => {
                full_row_eq(r1, r2) && full_row_eq(o1, o2)
            }
            _ => false,
        }
    }
}

/// A leaf sink that records every change pushed to it (and, optionally, the rows
/// of a reentrant fetch of its input taken *during* the push — to observe the
/// epoch-gated overlay, as in the JS `fetch during push` tests). Stands in for
/// `Catch` (`catch.ts`).
pub struct Collector {
    pub input: NodeId,
    changes: RefCell<Vec<CollectedChange>>,
    fetch_on_push: Cell<bool>,
    fetched: RefCell<Vec<Vec<Row>>>,
    /// When set (via [`Graph::add_change_sink`]), every push also records the
    /// fully-materialized [`CaughtChange`](crate::changes::CaughtChange) tree (nested
    /// relationships drained) into `caught` — the production change-stream sink the
    /// `rindle-replica` wrapper consumes. Off for a plain testkit `Collector`, so its
    /// flat `changes` recording is byte-for-byte unchanged.
    capture_caught: Cell<bool>,
    caught: RefCell<Vec<crate::changes::CaughtChange>>,
}

// ---------------------------------------------------------------------------
// The multi-port chassis: the Filter sub-graph (`filter-operators.ts`) + the OR
// fan (`fan-out.ts`/`fan-in.ts`). This is the skeleton **five** operators share
// — `FilterStart`, `Filter`, `FanOut`, `FanIn`, `FilterEnd` (and, later, `Exists`
// and `FilterProbe`) — proven once here so spec `07`'s Filter/Exists and spec
// `06`'s fan land on a known-good frame.
//
// Two structural patterns are at stake, neither of which the single-output `Join`
// exercised:
//
//  1. **The `begin_filter`/`filter`/`end_filter` lifecycle** (a `FilterOperator`
//     *gates* — `filter(node) -> bool` — it does not vend its own rows; the one
//     row scan happens in `FilterStart::fetch`). The lifecycle brackets that scan
//     so a stateful link (Exists's per-loop cache) can cache for the loop and
//     clear after — and `end_filter` MUST run on early stream drop / early
//     return, which is the RAII (`Drop`) contract (Primitive #2; `07` §6.4). It
//     also runs on panic ONLY in unwinding builds (`release-server`, tests);
//     under the shipping `panic = "abort"` client profile a panic aborts and
//     `Drop` is skipped. See WS02. Dispatched
//     by `NodeId` through the arena (`07` §4.3 "not a `dyn FilterChain` chain"),
//     exactly like `fetch`/`push`.
//
//  2. **Multi-port wiring**: `FanOut` has one input and **N outputs** (branches);
//     `FanIn` has **N inputs** and one output. The OR push fans one change to
//     every branch, then collapses the branches' forwarded changes back to
//     exactly one (`push_accumulated_changes` — the dedup that stops an OR from
//     double-counting, `06` §3.5).
//
// **Why the accumulation lives on the stack, not in a `FanIn` field.** The JS
// `FanIn` buffers branch pushes in a `#accumulatedPushes: Change[]` *instance
// field*. A `Change<'g>` borrows the graph (its relationship thunks are
// `+ 'g`), so it cannot live in a `'static` `Graph` field without a
// self-reference. Instead [`Graph::chain_push`] threads the accumulation through
// the call stack (lifetime `'g`) and `FanIn` is just the branch terminator that
// hands its change up to the owning `FanOut`. This mirrors Primitive #5's move
// from JS's live `#inprogressChildChange` field to a by-value capture: the Rust
// port makes the accumulation lifetime explicit instead of relying on GC.

/// `FilterStart` (`filter-operators.ts:61-104`): adapts a normal `Input` → the
/// filter chain. Its `fetch` is a NORMAL operator fetch that brackets the input
/// scan with `begin_filter`/`end_filter` and yields each node iff the chain's
/// `filter(node)` is true. `chain_head` is the single `FilterOutput` edge — used
/// for both `filter` (fetch) and `push`; do not split it (`07` §4.3).
pub struct FilterStart {
    pub input: NodeId,
    pub chain_head: Cell<Option<NodeId>>,
}

/// `FilterEnd` (`filter-operators.ts:106-146`): adapts the chain → a normal
/// `Input`. Its `filter()` is always `true` (the chain terminator); its `fetch`
/// delegates to `start.fetch`; its `push` forwards to the normal downstream
/// `output`. `start` is the matched [`FilterStart`].
pub struct FilterEnd {
    pub start: NodeId,
    /// The post-chain downstream **edge** — port-bearing (the `Skip`/`Take`/`Cap`
    /// template), so a `FilterEnd` can feed a relationship join's `JoinParent` port
    /// (a `related`/sibling join over a `where` sub-graph) as well as a plain `Single`
    /// sink. Wired `Single` via [`Graph::set_output`], port-bearing via
    /// [`Graph::set_out_edge`].
    pub output: Cell<Option<OutEdge>>,
}

/// `FanOut` (`fan-out.ts`): one input, **N outputs** (the OR branches). On
/// `filter` it is OR (true on the first branch that passes, short-circuit). On
/// `push` it replays the change to every branch then collapses what they forward
/// (via the paired `FanIn`'s continuation). A `FilterOperator`, so it has no
/// `fetch` of its own.
pub struct FanOut {
    pub input: NodeId,
    /// The branch heads, wired late (two-phase) — interior-mutable.
    outputs: RefCell<Vec<NodeId>>,
    /// The paired [`FanIn`] (set with the branches via [`Graph::set_fan`]).
    fan_in: Cell<Option<NodeId>>,
}

/// `FanIn` (`fan-in.ts`): **N inputs** (the branch tails all point here), one
/// output. In the JS it buffers branch pushes; here it is the branch *terminator*
/// (its `chain_push` hands the change up to the owning `FanOut`, which owns the
/// accumulation — see the chassis note above). `output` is the post-fan
/// continuation (the next `FilterOutput`, e.g. `FilterEnd`).
pub struct FanIn {
    pub fan_out: NodeId,
    output: Cell<Option<NodeId>>,
}

/// A `Filter` link (`filter.ts`): a stateless predicate gate. `filter(node)` =
/// `pred(row) && downstream.filter(node)`; `push` is the predicate-gated
/// `filter_push` with the Edit split (`07` §3.4). The single `output` edge serves
/// both paths.
pub struct Filter {
    pub input: NodeId,
    pub pred: CompiledPredicate,
    output: Cell<Option<NodeId>>,
}

/// Proof instrumentation: a `FilterChain` link that records its `begin_filter` /
/// `filter` / `end_filter` calls and passes every node/change through unchanged.
/// It stands in for a *stateful* filter (the role `Exists` will fill — `Exists`
/// caches on `begin`, clears on `end`) so the chassis can prove the lifecycle
/// fires in order AND that `end_filter` runs even when the fetch stream is
/// abandoned early (the `Drop` guard, `07` §6.4). Not a real query operator.
pub struct FilterProbe {
    pub input: NodeId,
    output: Cell<Option<NodeId>>,
    begin_count: Cell<usize>,
    filter_count: Cell<usize>,
    end_count: Cell<usize>,
}

pub enum Operator {
    /// A **freed** slot (a torn-down pipeline's operator, [`Graph::destroy_pipeline`]).
    /// The slot keeps its arena index (so no other id shifts) but its generation has
    /// been bumped, so any live access goes through the generation check in
    /// `Graph::node` and fail-fasts before reaching here; the dispatch panic arms are
    /// a belt-and-suspenders net. Replacing the old `Operator` with this variant is what
    /// drops the torn-down operator's heavy state (View tree, Collector buffers,
    /// compiled predicates, …).
    Tombstone,
    Source(SourceLeaf),
    SourceConn(SourceConn),
    Join(Join),
    /// The production materialization sink (`09`): the `Arc`-shared, reference-stable
    /// `ArrayView` + immutable tree differ. Logic lives in `crate::view`.
    View(crate::view::View),
    Collector(Collector),
    FilterStart(FilterStart),
    FilterEnd(FilterEnd),
    FanOut(FanOut),
    FanIn(FanIn),
    Filter(Filter),
    FilterProbe(FilterProbe),
    // --- spec 06/07 operators, implemented out-of-file (the fan-out seam,
    // `crate::op`). Each variant's logic lives in its own `op/<name>.rs`; only the
    // thin wiring (this variant + delegating match arms + an `add_<name>` builder)
    // is here, so isolated operator agents don't collide in graph.rs. ---
    Skip(crate::op::Skip),
    Take(crate::op::Take),
    Cap(crate::op::Cap),
    /// A `where`-EXISTS gate (a `FilterChain` link; logic in `op/exists.rs`).
    Exists(crate::op::Exists),
    /// The flipped (child-driven) inner join (logic in `op/flipped_join.rs`).
    FlippedJoin(crate::op::FlippedJoin),
    /// The node-level OR fan over the source (`op/union.rs`): `UnionFanOut`
    /// broadcasts a source change to the OR branches; `UnionFanIn` k-way-merges their
    /// fetches (with PK dedup) and collapses their pushes back to one change.
    UnionFanOut(crate::op::UnionFanOut),
    UnionFanIn(crate::op::UnionFanIn),
    /// A global, invertible aggregate (`op/reduce.rs`, `REDUCE-DESIGN.md`): collapses
    /// its input into one synthetic aggregate row (v1: `count(*)`).
    Reduce(crate::op::Reduce),
    // --- spec 11 test harness sinks/taps, logic out-of-file in `crate::testkit`
    // (same seam shape as `crate::op`). `Catch` is the output oracle (generalizes
    // `Collector` to a full nested tree + Child); `Snitch` is the message-log tap. ---
    #[cfg(any(test, feature = "testkit"))]
    Catch(crate::testkit::Catch),
    #[cfg(any(test, feature = "testkit"))]
    Snitch(crate::testkit::Snitch),
}

// ---------------------------------------------------------------------------
// Graph
// ---------------------------------------------------------------------------

/// The exact arena ids one `build_pipeline` (+ its sink) created — captured by the
/// graph's recording mode ([`Graph::begin_recording`]/[`Graph::take_recording`]) so
/// the pipeline can be torn down precisely with [`Graph::destroy_pipeline`].
///
/// It is a *recorded id list*, not a contiguous range: with slot reuse a query's
/// nodes can land on scattered recycled slots. The `(source, ConnId)` pairs to
/// disconnect are recovered from the recorded [`SourceConn`] nodes, so they need not
/// be stored separately. Shared `Source` nodes are pre-registered (outside any build),
/// so they never appear here and are never torn down.
#[derive(Clone, Debug, Default)]
pub struct PipelineManifest {
    nodes: Vec<NodeId>,
    storage: Vec<StorageId>,
}

impl PipelineManifest {
    /// The node ids this pipeline owns (test/inspection access).
    pub fn nodes(&self) -> &[NodeId] {
        &self.nodes
    }
    /// The storage ids this pipeline owns.
    pub fn storage(&self) -> &[StorageId] {
        &self.storage
    }
}

#[derive(Default)]
pub struct Graph {
    nodes: Vec<Operator>,
    /// Per-slot **generation**, parallel to `nodes`. Bumped when a slot is freed
    /// ([`Graph::destroy_pipeline`]); [`Graph::node`] checks it on every access so a
    /// stale [`NodeId`] to a recycled slot fail-fasts instead of aliasing the new
    /// tenant.
    node_gens: Vec<u32>,
    /// Freed node-slot indices available for reuse (a LIFO free-list). `add` pops one
    /// before growing `nodes`, so a long-lived graph with query churn recycles slots
    /// rather than growing without bound.
    free_nodes: Vec<u32>,
    /// Parallel arena of per-operator scratch state (foundations §6.4), addressed
    /// by [`StorageId`]. Held here — not inside the operators — so a stateful
    /// operator reads its state through a *shared* `&Graph` borrow (like fetch/
    /// push), and so the backend (memory vs spilled-SQLite) is a graph-level
    /// choice. Empty for a graph with no stateful operators.
    storage: Vec<Box<dyn Storage>>,
    /// Per-storage-slot generation (the `node_gens` analogue for [`StorageId`]).
    storage_gens: Vec<u32>,
    /// Freed storage-slot indices available for reuse (the `free_nodes` analogue).
    free_storage: Vec<u32>,
    /// When `Some`, every [`Graph::add`]/[`Graph::alloc_storage`] appends the id it
    /// minted here — capturing the exact node/storage ids one `build_pipeline` created
    /// into a [`PipelineManifest`], so the pipeline can later be torn down precisely.
    /// Bracketed by [`Graph::begin_recording`]/[`Graph::take_recording`]; `None`
    /// (off) on the hot path.
    recording: Option<PipelineManifest>,
    storage_factory: StorageFactory,
    /// Opt-in strict change-consistency validation (WS02.2). When `true`, a malformed
    /// change (ADD of an existing row, REMOVE/EDIT of an absent row) returns a
    /// [`RindleError::ConsistencyViolation`] from [`Graph::try_source_push`] — runnable
    /// in **release**, unlike the default `debug_assert!`. Off by default to preserve
    /// hot-path parity; a server that doesn't fully trust its change producer turns
    /// it on. `Cell` so it is settable on a built `&Graph`.
    validate_changes: Cell<bool>,
    /// Graph-level runtime-error sink (WS02.2 / the WS02.3 prelude). A failure raised
    /// deep in the push fan-out — where the call stack cannot thread a `Result` back
    /// up — parks here (e.g. a strict join-key consistency violation); the existing
    /// [`take_runtime_error`](Self::take_runtime_error) drains it at the mutation
    /// boundary (`try_source_push`/`try_hydrate` already call it). `Rc<RefCell<…>>` so
    /// a future `OpStorage` (WS02.3) can hold a clone of the same sink.
    runtime_error: Rc<RefCell<Option<RindleError>>>,
    /// Host-armed wall-clock deadline for the CURRENT push (FOLLOWER-LAG-SHED §6.6 —
    /// the runaway-push bail). The unbounded fan-out loops (join child fan-out, take
    /// backfill/refill) test it every [`DEADLINE_CHECK_EVERY`] iterations and park a
    /// [`RindleError::PushDeadlineExceeded`] on expiry — converting today's
    /// stuck-worker detach-and-leak into a clean, prompt, single-push fault. `None`
    /// (never armed) on the wasm client, whose checkpoints therefore never call
    /// `Instant::now()` (which traps on wasm32-unknown-unknown) — no feature gate
    /// needed, the root crate stays std-only. `Cell` so the host arms it per push on
    /// a built `&Graph` (mirrors `validate_changes`).
    push_deadline: Cell<Option<std::time::Instant>>,
    /// Shared amortization counter for the armed deadline (§6.6). The fan-out loops (join child
    /// fan-out, take backfill/refill) increment THIS on every iteration and consult the clock only
    /// every [`DEADLINE_CHECK_EVERY`]-th tick — so the deadline bounds the WHOLE armed derive (a
    /// batch's many changes, each with its own small fan-out loop), not just one loop in isolation.
    /// Reset to 0 by [`set_push_deadline`](Self::set_push_deadline) each time the host (re)arms.
    /// A prior revision counted in a fan-out-loop-LOCAL `u32` reset per call, so the checkpoint
    /// never accumulated across the many small changes of a bulk write — the clock was never
    /// consulted and the deadline silently never fired for that shape. `Cell` (single-thread
    /// engine) so it is bumped through a shared `&Graph` borrow deep in the reentrant fan-out.
    push_deadline_ticks: Cell<u32>,
}

impl Graph {
    pub fn new() -> Graph {
        Graph::default()
    }

    pub fn with_storage_factory(storage_factory: StorageFactory) -> Graph {
        Graph {
            storage_factory,
            ..Graph::default()
        }
    }

    /// Enable/disable strict change-consistency validation (WS02.2). Off by default.
    /// When on, [`try_source_push`](Self::try_source_push) returns a
    /// [`RindleError::ConsistencyViolation`] (instead of a release-stripped
    /// `debug_assert!`) on a malformed change stream.
    pub fn set_validate_changes(&self, on: bool) {
        self.validate_changes.set(on);
    }

    /// Whether strict change validation is currently enabled.
    pub fn validate_changes(&self) -> bool {
        self.validate_changes.get()
    }

    /// Arm (or clear) the wall-clock deadline for the CURRENT push (FOLLOWER-LAG-SHED §6.6).
    /// A host that bounds per-push derive time arms `now + P` around each push and clears it
    /// after; on expiry the fan-out checkpoints park a
    /// [`RindleError::PushDeadlineExceeded`] and stop iterating — the push surfaces `Err`
    /// from [`try_source_push`](Self::try_source_push) with torn operator state, which the
    /// host discards (the same abort≙epoch-rehydrate contract a panic uses). Never arm this
    /// on wasm: the expiry check calls `Instant::now()`, which traps there.
    pub fn set_push_deadline(&self, deadline: Option<std::time::Instant>) {
        self.push_deadline.set(deadline);
        // Reset the amortization counter so the checkpoint interval is measured from THIS arm and
        // ticks accumulate across the whole armed derive — not per fan-out loop (see the field doc).
        self.push_deadline_ticks.set(0);
    }

    /// Amortized deadline checkpoint for the unbounded fan-out loops (§6.6): counts every call in
    /// the graph-level [`push_deadline_ticks`](Self::push_deadline_ticks) — SHARED across the whole
    /// armed window, so a batch of many small-fan-out changes accumulates toward the checkpoint
    /// instead of resetting per loop — and consults the clock only every [`DEADLINE_CHECK_EVERY`]-th
    /// call, and only when a deadline is armed, so the wasm client (which never arms one) never
    /// reaches `Instant::now()`.
    #[inline]
    pub(crate) fn push_deadline_exceeded(&self) -> bool {
        let ticks = self.push_deadline_ticks.get().wrapping_add(1);
        self.push_deadline_ticks.set(ticks);
        if !ticks.is_multiple_of(DEADLINE_CHECK_EVERY) {
            return false;
        }
        match self.push_deadline.get() {
            None => false,
            Some(deadline) => std::time::Instant::now() >= deadline,
        }
    }

    fn add(&mut self, op: Operator) -> NodeId {
        // Reuse a freed slot if one is available (its generation was already bumped at
        // free time, so the returned id is distinct from the slot's prior tenant);
        // otherwise grow the arena with a fresh generation-0 slot.
        let id = if let Some(idx) = self.free_nodes.pop() {
            let i = idx as usize;
            self.nodes[i] = op;
            NodeId::new(idx, self.node_gens[i])
        } else {
            let idx = self.nodes.len() as u32;
            self.nodes.push(op);
            self.node_gens.push(0);
            NodeId::new(idx, 0)
        };
        if let Some(rec) = self.recording.as_mut() {
            rec.nodes.push(id);
        }
        id
    }

    /// Begin capturing a [`PipelineManifest`]: subsequent `Graph::add` /
    /// [`Graph::alloc_storage`] record the ids they mint. Bracket one `build_pipeline`
    /// (+ its sink) with this and [`Graph::take_recording`] to get the exact id set to
    /// hand [`Graph::destroy_pipeline`]. # Panics (debug) if recording is already on.
    pub fn begin_recording(&mut self) {
        debug_assert!(self.recording.is_none(), "recording already in progress");
        self.recording = Some(PipelineManifest::default());
    }

    /// Stop recording and return the captured [`PipelineManifest`] (empty if recording
    /// was not active).
    pub fn take_recording(&mut self) -> PipelineManifest {
        self.recording.take().unwrap_or_default()
    }

    /// Allocate a fresh scratch-state slot for a stateful operator and return its
    /// [`StorageId`] (the builder, `08` §5, calls this and embeds the id in the
    /// `Take`/`Cap`/`Exists` struct). On the client/test path a fresh
    /// [`MemoryStorage`](crate::storage::MemoryStorage) *is* the namespace (`10`
    /// §3.3 / §4.5): each slot is a disjoint keyspace because it is a distinct
    /// store. On the SQLite server path, a graph constructed with
    /// `StorageFactory::sqlite` vends an `op_id`-namespaced `OpStorage` instead
    /// (`10` §4.4).
    pub fn alloc_storage(&mut self) -> StorageId {
        // Reuse a freed storage slot if one is available — its store was `clear`ed at
        // free time, so it is already an empty namespace ready for the new operator;
        // otherwise grow the arena. Thread the graph's runtime-error sink into a
        // freshly-created store so a SQLite operator-storage failure parks a `RindleError`
        // (drained by `take_runtime_error`) instead of `.expect`-aborting (WS02.3).
        let id = if let Some(idx) = self.free_storage.pop() {
            let i = idx as usize;
            StorageId {
                idx,
                gen: self.storage_gens[i],
            }
        } else {
            let idx = self.storage.len() as u32;
            self.storage.push(
                self.storage_factory
                    .create_storage_with_sink(self.runtime_error.clone()),
            );
            self.storage_gens.push(0);
            StorageId { idx, gen: 0 }
        };
        if let Some(rec) = self.recording.as_mut() {
            rec.storage.push(id);
        }
        id
    }

    /// Borrow a stateful operator's scratch store (the read side of
    /// [`Graph::alloc_storage`]). `&dyn Storage` so operator code is
    /// backend-agnostic; the store's own methods take `&self` (interior
    /// mutability), so this composes with the reentrant shared-borrow push.
    // `allow(dead_code)`: consumed by the stateful operators (`Take`/`Cap`/
    // `Exists`, spec `07`) once they land; exercised today by the arena test.
    #[allow(dead_code)]
    pub(crate) fn storage(&self, id: StorageId) -> &dyn Storage {
        let i = id.ix();
        assert!(
            self.storage_gens[i] == id.gen,
            "stale StorageId {id:?} (slot generation is {})",
            self.storage_gens[i]
        );
        &*self.storage[i]
    }

    /// The generation-checked operator lookup — the single read path into the node
    /// arena. Every dispatch/accessor goes through here, so a stale [`NodeId`] (an old
    /// generation for a slot a torn-down pipeline freed and a newer one recycled) is a
    /// loud fail-fast, never a silent alias. Checked in release too (the stale-handle
    /// safety net is the whole point of the generation).
    #[inline]
    fn node(&self, id: NodeId) -> &Operator {
        let i = id.ix();
        assert!(
            self.node_gens[i] == id.gen,
            "stale NodeId {id:?} (slot generation is {})",
            self.node_gens[i]
        );
        &self.nodes[i]
    }

    /// Snapshot **all** operator scratch storage, one entry per [`StorageId`] in
    /// allocation order, each a full ascending `scan("")` of `(key, value)` pairs.
    /// The test-only sink-independence probe (spec `11` §3.1.1): two graphs built
    /// from the *same* `Ast` allocate storage in the same order, so snapshot index
    /// `i` denotes the same operator's store in both — letting
    /// `run_push_test_ast_view` assert
    /// `Catch`-sink storage equals `View`-sink storage (operator state is identical
    /// regardless of sink). `StorageValue` has no `PartialEq`, so the comparison is
    /// spelled out at the testkit boundary.
    pub fn storage_snapshot(&self) -> Vec<Vec<(Box<str>, crate::storage::StorageValue)>> {
        self.storage.iter().map(|s| s.scan("").collect()).collect()
    }

    /// Total node slots in the arena, **including tombstoned (freed) slots**. A
    /// teardown does not shrink this; a subsequent build reuses freed slots, so this
    /// staying flat across a destroy+rebuild is the observable proof of slot reuse.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Total storage slots in the arena, including freed (cleared) ones (the
    /// [`node_count`](Self::node_count) analogue for [`StorageId`]).
    pub fn storage_count(&self) -> usize {
        self.storage.len()
    }

    /// Tear down one pipeline, identified by the [`PipelineManifest`] captured when it
    /// was built (recording mode). Two passes:
    ///
    /// 1. **Disconnect** every [`SourceConn`] the pipeline owns via
    ///    [`Source::destroy`], which nulls that connection's output edge. Because the
    ///    push fan-out only drives connections whose output is `Some`
    ///    ([`Graph::try_source_push`]), this orphans the whole subgraph from every
    ///    future source push. The shared [`Source`] itself — pre-registered, outside
    ///    the manifest — is untouched: its indexes and connection slots persist (§3.10).
    /// 2. **Reclaim** each storage slot (`clear` its contents) and each node slot
    ///    (replace the operator with [`Operator::Tombstone`], dropping its heavy state:
    ///    View tree, Collector buffers, compiled predicates, …). Both bump the slot's
    ///    generation and push it onto the free-list, so the slot is recycled by the
    ///    next `add`/`alloc_storage` while every *other* pipeline's ids stay valid.
    ///
    /// Idempotency / safety: each slot's current generation is asserted to match the
    /// manifest before freeing, so a double teardown (or a corrupted manifest) is a
    /// loud fail-fast rather than silent corruption. Runs under `&mut self`, so it can
    /// only be called between transactions, never mid-push.
    ///
    /// **Known limitation (deferred):** the shared source's per-connection slot is
    /// *nulled* (step 1) but not freed — `ConnId`s are not yet generational, so a
    /// source's `conns` Vec grows by one small record per teardown. Under sustained
    /// build/destroy churn against a long-lived source this is a slow, bounded-per-query
    /// leak (no correctness impact: the fan-out skips nulled connections). Reclaiming it
    /// needs a generational `ConnId` + per-source free-list — a planned fast-follow.
    pub fn destroy_pipeline(&mut self, manifest: &PipelineManifest) {
        // Pass 1: collect the (source, connection) pairs first — pass 2 tombstones the
        // SourceConn nodes, which would lose this information.
        let conns: Vec<(NodeId, ConnId)> = manifest
            .nodes
            .iter()
            .filter_map(|&nid| match self.node(nid) {
                Operator::SourceConn(sc) => Some((sc.source, sc.conn)),
                _ => None,
            })
            .collect();
        for (source, conn) in conns {
            self.source(source).destroy(conn);
        }

        // Pass 2a: clear + free each storage slot. The generation guard is a release
        // `assert!` (not `debug_assert!`): freeing a slot pushes it onto the free-list,
        // so a stale/double-freed id here would silently recycle a live store into two
        // owners. This runs once per slot at teardown (off any per-row path), so the
        // check is free; it must hold in release to match the docstring's guarantee.
        for &sid in &manifest.storage {
            let i = sid.ix();
            assert!(
                self.storage_gens[i] == sid.gen,
                "destroy_pipeline: stale StorageId {sid:?} in manifest (double free?)"
            );
            self.storage[i].clear();
            self.storage_gens[i] = self.storage_gens[i].wrapping_add(1);
            self.free_storage.push(i as u32);
        }

        // Pass 2b: tombstone + free each node slot (same release-checked guard).
        for &nid in &manifest.nodes {
            let i = nid.ix();
            assert!(
                self.node_gens[i] == nid.gen,
                "destroy_pipeline: stale NodeId {nid:?} in manifest (double free?)"
            );
            self.nodes[i] = Operator::Tombstone;
            self.node_gens[i] = self.node_gens[i].wrapping_add(1);
            self.free_nodes.push(i as u32);
        }
    }

    // --- construction helpers (mirror builder.ts wiring: two-phase, by id) ---

    /// Add an in-memory source. # Panics if a row's width does not match the schema;
    /// prefer [`try_add_source`](Self::try_add_source) in production (WS02.6).
    pub fn add_source(&mut self, schema: SourceSchema, initial: Vec<Row>) -> NodeId {
        self.add(Operator::Source(SourceLeaf::Memory(MemorySource::new(
            schema.into_schema(),
            initial,
        ))))
    }

    /// Fallible [`add_source`](Self::add_source): validates ingest row widths and
    /// returns a [`RindleError::SchemaViolation`] instead of panicking (WS02.4).
    pub fn try_add_source(
        &mut self,
        schema: SourceSchema,
        initial: Vec<Row>,
    ) -> Result<NodeId, RindleError> {
        let source = MemorySource::try_new(schema.into_schema(), initial)?;
        Ok(self.add(Operator::Source(SourceLeaf::Memory(source))))
    }

    pub fn add_memory_source(&mut self, source: MemorySource) -> NodeId {
        self.add(Operator::Source(SourceLeaf::Memory(source)))
    }

    /// Add an external [`Source`] erased behind a trait object — e.g. the SQLite
    /// `TableSource` from the `rindle-sqlite` crate. The in-memory backend uses the
    /// concrete [`add_source`](Self::add_source)/[`add_memory_source`](Self::add_memory_source).
    pub fn add_dyn_source(&mut self, source: Box<dyn Source>) -> NodeId {
        self.add(Operator::Source(SourceLeaf::Dyn(source)))
    }

    /// Remove an in-memory source added by [`add_source`](Self::add_source) /
    /// [`try_add_source`](Self::try_add_source): tombstone its node and free the slot (bump
    /// generation + free-list) — the single-node analogue of [`destroy_pipeline`](Self::destroy_pipeline).
    /// The inverse of `add_source`, for a synthetic table whose last reading query is gone
    /// (`AGGREGATE-SYNC-DESIGN.md` §4). Errors with a [`RindleError`] if `source` is not an
    /// in-memory source node, or if it still has a live connection — every pipeline reading it
    /// must be torn down first (`destroy_pipeline`), which the JS backend guarantees by
    /// refcounting readers.
    pub fn remove_source(&mut self, source: NodeId) -> Result<(), RindleError> {
        let live = match self.node(source) {
            Operator::Source(SourceLeaf::Memory(ms)) => ms.live_conn_count(),
            Operator::Source(SourceLeaf::Dyn(_)) => {
                return Err(RindleError::schema_violation(
                    "remove_source: external (dyn) sources are not removable".to_string(),
                ))
            }
            _ => {
                return Err(RindleError::schema_violation(format!(
                    "remove_source: {source:?} is not a source node"
                )))
            }
        };
        if live != 0 {
            return Err(RindleError::schema_violation(format!(
                "remove_source: {source:?} still has {live} live connection(s) — destroy readers first"
            )));
        }
        let i = source.ix();
        assert!(
            self.node_gens[i] == source.gen,
            "remove_source: stale NodeId {source:?} (slot generation is {})",
            self.node_gens[i]
        );
        self.nodes[i] = Operator::Tombstone;
        self.node_gens[i] = self.node_gens[i].wrapping_add(1);
        self.free_nodes.push(i as u32);
        Ok(())
    }

    /// Create a new connection (output) on a source and return its `SourceConn`
    /// node id. Self-joins call this twice on the same source.
    pub fn connect(
        &mut self,
        source: NodeId,
        sort: Option<Sort>,
        filters: Option<ConnectionFilters>,
        split_edit_keys: Vec<ColId>,
    ) -> NodeId {
        let conn = self.source(source).connect(sort, filters, split_edit_keys);
        self.add(Operator::SourceConn(SourceConn { source, conn }))
    }

    /// Wire a hierarchical join with an **already-resolved** [`RelId`] slot. The builder
    /// (`08`) resolves the relationship name against its *query-local* slot tree —
    /// computed from the query AST, not the shared source schema's declared
    /// relationships (which a query can't pre-declare for synthesized EXISTS-gate
    /// aliases like `comments_0`) — and passes the slot directly, so the join is not
    /// re-resolved against `input_schema(parent)`.
    pub fn add_join_slot(
        &mut self,
        parent: NodeId,
        child: NodeId,
        parent_key: Vec<ColId>,
        child_key: Vec<ColId>,
        rel_slot: RelId,
    ) -> NodeId {
        self.add(Operator::Join(Join {
            parent,
            child,
            parent_key,
            child_key,
            rel_slot,
            output: Cell::new(None),
            inprogress_overlay: RefCell::new(None),
            inprogress_position: RefCell::new(None),
        }))
    }

    /// The flipped, child-driven inner join with an **already-resolved** [`RelId`]
    /// slot — the flipped analogue of [`Graph::add_join_slot`], for the builder's
    /// query-local slot resolution. The flipped join outputs parent rows with the
    /// child relationship attached; wire its parent/child inputs with
    /// [`Graph::set_out_edge`] (`JoinParent`/`JoinChild`).
    pub fn add_flipped_join_slot(
        &mut self,
        parent: NodeId,
        child: NodeId,
        parent_key: Vec<ColId>,
        child_key: Vec<ColId>,
        rel_slot: RelId,
    ) -> NodeId {
        self.add(Operator::FlippedJoin(crate::op::FlippedJoin::new(
            parent, child, parent_key, child_key, rel_slot,
        )))
    }

    /// [`Graph::add_flipped_join_slot`] with an explicit IN-batch chunk size — the
    /// test seam analogous to the JS `setMultiConstraintChunkSizeForTest`
    /// (`flipped-join.ts:57`). A small size forces the chunked fetch path (per-window
    /// fetch + node-level k-way merge) so it can be diffed against the unchunked path.
    pub fn add_flipped_join_slot_with_chunk_size(
        &mut self,
        parent: NodeId,
        child: NodeId,
        parent_key: Vec<ColId>,
        child_key: Vec<ColId>,
        rel_slot: RelId,
        chunk_size: usize,
    ) -> NodeId {
        self.add(Operator::FlippedJoin(
            crate::op::FlippedJoin::new(parent, child, parent_key, child_key, rel_slot)
                .with_chunk_size(chunk_size),
        ))
    }

    /// Add a production [`View`](crate::view::View) over `input` with the default
    /// `ResultType::Complete` (the server/test default). For explicit `with_ids` or a
    /// pending result type use [`Graph::add_view_with`]. The view shape is carried by
    /// `schema` (a relationship is in-view iff its `RelDef` has a child schema).
    pub fn add_view(&mut self, input: NodeId, schema: Schema) -> NodeId {
        self.add(Operator::View(crate::view::View::new(
            input,
            schema,
            false,
            crate::view::ResultType::Complete,
        )))
    }

    /// Add a production [`View`](crate::view::View) with explicit `with_ids` and initial
    /// [`ResultType`](crate::view::ResultType). The view shape is carried by `schema`.
    pub fn add_view_with(
        &mut self,
        input: NodeId,
        schema: Schema,
        with_ids: bool,
        result_type: crate::view::ResultType,
    ) -> NodeId {
        self.add(Operator::View(crate::view::View::new(
            input,
            schema,
            with_ids,
            result_type,
        )))
    }

    /// Add an out-of-file operator (the fan-out seam). The builder takes a
    /// fully-constructed [`Skip`](crate::op::Skip), so growing `Skip`'s fields
    /// never touches this method. Wire its downstream with [`Graph::set_output`].
    /// (`Take`/`Cap`/`Exists`/`FlippedJoin`/`Union*` get an identical `add_*`.)
    pub fn add_skip(&mut self, skip: crate::op::Skip) -> NodeId {
        self.add(Operator::Skip(skip))
    }

    /// Add a [`Take`](crate::op::Take) (the `LIMIT` operator). Like
    /// [`Graph::add_skip`], takes a fully-constructed value (the builder hands it a
    /// [`StorageId`] from [`Graph::alloc_storage`]); wire its downstream with
    /// [`Graph::set_output`] (terminal sink) or [`Graph::set_out_edge`] (feeding a
    /// relationship join's parent port).
    pub fn add_take(&mut self, take: crate::op::Take) -> NodeId {
        self.add(Operator::Take(take))
    }

    /// Add a [`Cap`](crate::op::Cap) (the unordered EXISTS-child limiter). Same
    /// shape as [`Graph::add_take`]: a fully-constructed value carrying its
    /// [`StorageId`]; wire its downstream with [`Graph::set_output`] /
    /// [`Graph::set_out_edge`].
    pub fn add_cap(&mut self, cap: crate::op::Cap) -> NodeId {
        self.add(Operator::Cap(cap))
    }

    /// Add a [`Reduce`](crate::op::Reduce) (an invertible aggregate, `REDUCE-DESIGN.md`).
    /// Same shape as [`Graph::add_take`]: a fully-constructed value carrying its
    /// [`StorageId`]. Unlike `Take`, `Reduce` reshapes its row (output is a synthetic
    /// aggregate row), so it carries its own output schema. Wire its downstream with
    /// [`Graph::set_output`].
    pub fn add_reduce(&mut self, reduce: crate::op::Reduce) -> NodeId {
        self.add(Operator::Reduce(reduce))
    }

    /// Add an [`Exists`](crate::op::Exists) gate (a `FilterChain` link). Wire its
    /// upstream `FilterStart`'s chain head to it ([`Graph::set_chain_head`]) and its
    /// downstream `FilterOutput` with [`Graph::set_output`].
    pub fn add_exists(&mut self, exists: crate::op::Exists) -> NodeId {
        self.add(Operator::Exists(exists))
    }

    pub fn add_collector(&mut self, input: NodeId) -> NodeId {
        self.add(Operator::Collector(Collector {
            input,
            changes: RefCell::new(Vec::new()),
            fetch_on_push: Cell::new(false),
            fetched: RefCell::new(Vec::new()),
            capture_caught: Cell::new(false),
            caught: RefCell::new(Vec::new()),
        }))
    }

    /// Add a **change-stream sink**: a terminal sink that records the
    /// fully-materialized [`CaughtChange`](crate::changes::CaughtChange) tree of every
    /// change pushed to it (nested relationships drained eagerly). Wire it with
    /// [`set_sink_edge`](Self::set_sink_edge); drain per-transaction events with
    /// [`take_sink_changes`](Self::take_sink_changes) and get the initial hydration set
    /// with [`try_hydrate_change_sink`](Self::try_hydrate_change_sink). Implemented as a
    /// `Collector` with caught-capture enabled, so no new operator variant (and no
    /// change to the dispatch/fetch/schema match arms) is required.
    pub fn add_change_sink(&mut self, input: NodeId) -> NodeId {
        self.add(Operator::Collector(Collector {
            input,
            changes: RefCell::new(Vec::new()),
            fetch_on_push: Cell::new(false),
            fetched: RefCell::new(Vec::new()),
            capture_caught: Cell::new(true),
            caught: RefCell::new(Vec::new()),
        }))
    }

    // --- spec 11 testkit sinks/taps (the seam, `crate::testkit`) ---

    /// The output oracle [`Catch`](crate::testkit::Catch). Sink it on the operator
    /// under test, then wire the push edge with [`Graph::set_sink_edge`] (the
    /// runner does both). `fetch_on_push` records a re-fetch alongside each push.
    #[cfg(any(test, feature = "testkit"))]
    pub fn add_catch(&mut self, input: NodeId, fetch_on_push: bool) -> NodeId {
        self.add(Operator::Catch(crate::testkit::Catch::new(
            input,
            fetch_on_push,
        )))
    }

    /// The message-log tap [`Snitch`](crate::testkit::Snitch) over `input`. Wire
    /// its single downstream with [`Graph::set_output`]; read the log with
    /// [`Graph::snitch_log`].
    #[cfg(any(test, feature = "testkit"))]
    pub fn add_snitch(&mut self, input: NodeId, name: impl Into<String>) -> NodeId {
        self.add(Operator::Snitch(crate::testkit::Snitch::new(input, name)))
    }

    // --- the multi-port chassis: filter sub-graph + OR fan ---

    /// A `FilterStart` over `input` (the upstream normal `Input`). Wire its single
    /// chain edge with [`Graph::set_chain_head`].
    pub fn add_filter_start(&mut self, input: NodeId) -> NodeId {
        self.add(Operator::FilterStart(FilterStart {
            input,
            chain_head: Cell::new(None),
        }))
    }

    /// A `FilterEnd` paired with `start`. Wire its downstream with
    /// [`Graph::set_output`].
    pub fn add_filter_end(&mut self, start: NodeId) -> NodeId {
        self.add(Operator::FilterEnd(FilterEnd {
            start,
            output: Cell::new(None),
        }))
    }

    /// A `FanOut` over `input`. Wire its branches + paired `FanIn` with
    /// [`Graph::set_fan`].
    pub fn add_fan_out(&mut self, input: NodeId) -> NodeId {
        self.add(Operator::FanOut(FanOut {
            input,
            outputs: RefCell::new(Vec::new()),
            fan_in: Cell::new(None),
        }))
    }

    /// A `FanIn` paired with `fan_out`. Wire its post-fan continuation with
    /// [`Graph::set_output`].
    pub fn add_fan_in(&mut self, fan_out: NodeId) -> NodeId {
        self.add(Operator::FanIn(FanIn {
            fan_out,
            output: Cell::new(None),
        }))
    }

    /// A `UnionFanOut` over `input` (the node-level OR fan-out). Wire its branch
    /// broadcast edges + paired `UnionFanIn` with [`Graph::set_union_fan`].
    pub fn add_union_fan_out(&mut self, input: NodeId) -> NodeId {
        self.add(Operator::UnionFanOut(crate::op::UnionFanOut::new(input)))
    }

    /// A `UnionFanIn` paired with `fan_out` over the branch tails `inputs`, carrying
    /// the merged branch `schema` (it owns the output schema; the `sort` must be
    /// defined) and the per-branch **pushable constraints** (parallel to `inputs`) the
    /// fan-in merges into each branch fetch. Wire its post-fan continuation with
    /// [`Graph::set_output`].
    pub fn add_union_fan_in(
        &mut self,
        fan_out: NodeId,
        inputs: Vec<NodeId>,
        branch_constraints: Vec<crate::change::Constraint>,
        schema: Schema,
    ) -> NodeId {
        self.add(Operator::UnionFanIn(crate::op::UnionFanIn::new(
            fan_out,
            inputs,
            branch_constraints,
            schema,
        )))
    }

    /// A `Filter` link over `input` with predicate `pred`. Wire its single
    /// `FilterOutput` with [`Graph::set_output`].
    pub fn add_filter(&mut self, input: NodeId, pred: CompiledPredicate) -> NodeId {
        self.add(Operator::Filter(Filter {
            input,
            pred,
            output: Cell::new(None),
        }))
    }

    /// A `FilterProbe` link over `input` (proof instrumentation; see the struct).
    pub fn add_filter_probe(&mut self, input: NodeId) -> NodeId {
        self.add(Operator::FilterProbe(FilterProbe {
            input,
            output: Cell::new(None),
            begin_count: Cell::new(0),
            filter_count: Cell::new(0),
            end_count: Cell::new(0),
        }))
    }

    /// Wire a `FilterStart`'s single chain edge (`#output`, the chain head).
    pub fn set_chain_head(&self, filter_start: NodeId, head: NodeId) {
        self.filter_start_op(filter_start)
            .chain_head
            .set(Some(head));
    }

    /// Wire a `FanOut`'s branch outputs and its paired `FanIn`.
    pub fn set_fan(&self, fan_out: NodeId, branches: Vec<NodeId>, fan_in: NodeId) {
        let fo = self.fan_out_op(fan_out);
        *fo.outputs.borrow_mut() = branches;
        fo.fan_in.set(Some(fan_in));
    }

    /// Wire a `UnionFanOut`'s branch broadcast edges (each branch head + the port to
    /// push it on — `JoinParent` for a flipped branch, `Single` for a filter branch)
    /// and its paired `UnionFanIn`.
    pub fn set_union_fan(&self, fan_out: NodeId, branches: Vec<OutEdge>, fan_in: NodeId) {
        self.union_fan_out_op(fan_out).set_fan(branches, fan_in);
    }

    /// Wire a source connection's output edge (mirrors `input.setOutput`).
    pub fn set_conn_output(&self, conn: NodeId, edge: OutEdge) {
        let sc = self.source_conn(conn);
        self.source(sc.source).set_conn_output(sc.conn, edge);
    }

    /// Wire a single-output operator's downstream. Covers the join plus every
    /// single-`FilterOutput` chassis link (`FilterEnd`, `FanIn`, `Filter`,
    /// `FilterProbe`). `FilterStart` uses [`Graph::set_chain_head`] and `FanOut`
    /// uses [`Graph::set_fan`] — those are not single-output.
    pub fn set_output(&self, op: NodeId, downstream: NodeId) {
        match self.node(op) {
            // A join's single downstream lands on the sink's sole input port. A
            // join feeding *another join* (sibling/nested relationships) needs an
            // explicit port and is wired by the builder via [`Graph::set_out_edge`].
            Operator::Join(j) => j.output.set(Some(OutEdge {
                node: downstream,
                port: Port::Single,
            })),
            // Like a Join, a FlippedJoin's terminal-sink downstream lands on the
            // sink's sole input port; feeding another join's port uses `set_out_edge`.
            Operator::FlippedJoin(fj) => fj.output.set(Some(OutEdge {
                node: downstream,
                port: Port::Single,
            })),
            Operator::FilterEnd(fe) => fe.output.set(Some(OutEdge {
                node: downstream,
                port: Port::Single,
            })),
            Operator::FanIn(fi) => fi.output.set(Some(downstream)),
            // UnionFanIn has a single post-fan output (UnionFanOut is wired via
            // `set_union_fan`, not here).
            Operator::UnionFanIn(u) => u.set_output(OutEdge {
                node: downstream,
                port: Port::Single,
            }),
            Operator::Filter(f) => f.output.set(Some(downstream)),
            Operator::FilterProbe(p) => p.output.set(Some(downstream)),
            Operator::Exists(e) => e.output.set(Some(downstream)),
            // Like a Join, a Skip's downstream on a terminal sink lands on the
            // sink's sole input port; a Skip feeding a relationship join's parent
            // port is wired by the builder via [`Graph::set_out_edge`].
            Operator::Skip(s) => s.output.set(Some(OutEdge {
                node: downstream,
                port: Port::Single,
            })),
            // Like a Skip, a Take's terminal-sink downstream lands on the sink's
            // sole input port; feeding a relationship join's parent port uses
            // [`Graph::set_out_edge`].
            Operator::Take(t) => t.output.set(Some(OutEdge {
                node: downstream,
                port: Port::Single,
            })),
            Operator::Cap(c) => c.output.set(Some(OutEdge {
                node: downstream,
                port: Port::Single,
            })),
            // Reduce has a single downstream landing on the sink's sole input port
            // (its synthetic aggregate row); it is not a relationship parent.
            Operator::Reduce(r) => r.output.set(Some(OutEdge {
                node: downstream,
                port: Port::Single,
            })),
            #[cfg(any(test, feature = "testkit"))]
            Operator::Snitch(s) => s.set_output(downstream),
            #[cfg(any(test, feature = "testkit"))]
            Operator::Catch(_) => panic!(
                "set_output: Catch is a terminal sink (no downstream); attach it with \
                 add_catch + set_sink_edge (or route its upstream to it)"
            ),
            _ => panic!("set_output: not a single-output operator"),
        }
    }

    /// Wire the final edge from a built pipeline's last operator into a terminal
    /// sink ([`Catch`](crate::testkit::Catch)/[`Collector`]) **or** a tap
    /// ([`Snitch`](crate::testkit::Snitch), which forwards onward). Routes a
    /// `SourceConn` through [`Graph::set_conn_output`] (a bare `source → sink`
    /// pipeline) and any single-output operator — including a transparent tap —
    /// through [`Graph::set_output`]. Used by the testkit runners after the build
    /// closure returns the op feeding the sink.
    pub fn set_sink_edge(&self, upstream: NodeId, sink: NodeId) {
        match self.node(upstream) {
            Operator::SourceConn(_) => self.set_conn_output(
                upstream,
                OutEdge {
                    node: sink,
                    port: Port::Single,
                },
            ),
            _ => self.set_output(upstream, sink),
        }
    }

    /// Wire an upstream operator's output edge with an **explicit port** — the
    /// port-aware generalization of [`Graph::set_output`] (which always wires
    /// `Port::Single`). The builder (`08`) uses this to chain joins: a
    /// [`SourceConn`] routes through the source's [`Graph::set_conn_output`]; a
    /// [`Join`] sets its [`OutEdge`] cell directly. The port says how the
    /// *downstream* receives — `JoinParent` when `upstream` is the next join's
    /// parent (sibling relationships), `JoinChild` when it is a nested child top.
    pub fn set_out_edge(&self, upstream: NodeId, edge: OutEdge) {
        match self.node(upstream) {
            Operator::SourceConn(_) => self.set_conn_output(upstream, edge),
            Operator::Join(j) => j.output.set(Some(edge)),
            Operator::FlippedJoin(fj) => fj.output.set(Some(edge)),
            Operator::Skip(s) => s.output.set(Some(edge)),
            Operator::Take(t) => t.output.set(Some(edge)),
            Operator::Cap(c) => c.output.set(Some(edge)),
            // A grouped `Reduce` is the top of a relationship-aggregate child subtree:
            // it feeds a parent join's `JoinChild` port, which lifts each per-group
            // `Add`/`Edit`/`Remove` into a `Change::Child` on the parent (the §9 Tier-1
            // singular-relationship attachment, `REDUCE-DESIGN.md`). It already forwards
            // on `out.port`, so it carries a port-bearing edge like any join child.
            Operator::Reduce(r) => r.output.set(Some(edge)),
            // The filter-chain / union-fan tails now carry a port-bearing edge, so a
            // relationship join (`related`/sibling) can sit over a `where` sub-graph.
            Operator::FilterEnd(fe) => fe.output.set(Some(edge)),
            Operator::UnionFanIn(u) => u.set_output(edge),
            _ => panic!(
                "set_out_edge: not a source-conn, join, flipped-join, skip, take, cap, \
                 reduce, filter-end, or union-fan-in ({upstream:?})"
            ),
        }
    }

    /// True iff `node` can carry a port-bearing [`OutEdge`] via [`Graph::set_out_edge`].
    /// Covers the port-aware ops (`SourceConn`/`Join`/`FlippedJoin`/`Skip`/`Take`/`Cap`/
    /// `Reduce`) **and** the filter-chain / union-fan tails `FilterEnd`/`UnionFanIn`, which
    /// now carry a port-bearing `OutEdge` (Gap B) — so a `related`/sibling relationship join
    /// can sit over a `where` sub-graph (an OR, a nested AND-OR, or a flipped EXISTS).
    /// `Reduce` is here as the top of a relationship-aggregate child subtree (§9). `FanIn`
    /// is excluded: it returns up the filter-chain stack, it is not a downstream-pushing tail.
    pub fn output_port_capable(&self, node: NodeId) -> bool {
        matches!(
            self.node(node),
            Operator::SourceConn(_)
                | Operator::Join(_)
                | Operator::FlippedJoin(_)
                | Operator::Skip(_)
                | Operator::Take(_)
                | Operator::Cap(_)
                | Operator::Reduce(_)
                | Operator::FilterEnd(_)
                | Operator::UnionFanIn(_)
        )
    }

    /// Wire a `Port::Single` forward edge from `upstream` to `downstream`, tolerant of a
    /// non-port-aware tail. A `SourceConn` routes through [`Graph::set_conn_output`];
    /// every other single-output operator — port-aware ops (via their `Single` fallback)
    /// AND the filter-chain / union-fan tails `FilterEnd`/`FanIn`/`UnionFanIn` — routes
    /// through [`Graph::set_output`]. Unlike [`Graph::set_out_edge`], this accepts a
    /// `FilterEnd`/`UnionFanIn` tail, so a `Take`/`Cap` (a root or EXISTS-child `limit`)
    /// can sit above a `where`-subgraph / union-fan end without panicking.
    pub fn wire_single(&self, upstream: NodeId, downstream: NodeId) {
        match self.node(upstream) {
            Operator::SourceConn(_) => self.set_conn_output(
                upstream,
                OutEdge {
                    node: downstream,
                    port: Port::Single,
                },
            ),
            _ => self.set_output(upstream, downstream),
        }
    }

    // --- typed accessors ---
    //
    // PANIC-CLASS: internal — every `_ => panic!("expected X at {id:?}")` arm below
    // fires only if the *builder* hands an accessor a NodeId of the wrong operator
    // kind. That is an engine wiring bug, never reachable from data/push/AST, so it
    // is a correct fail-fast panic (foundations §10; WS02.1). Do NOT convert to
    // `RindleError` — these are unconditional catch-alls, not conditional invariants.

    fn source(&self, id: NodeId) -> &SourceLeaf {
        match self.node(id) {
            Operator::Source(s) => s,
            _ => panic!("expected Source at {id:?}"),
        }
    }
    /// The in-memory source behind `id`, or `None` if the node is a [`SourceLeaf::Dyn`]
    /// backend (or not a source). The optimistic fork/rebase loop reaches through this
    /// to fork the live primary tree (`OPTIMISTIC-WRITES-DESIGN.md` §1) — memory
    /// sources only, by design: the loop is the wasm client's, not a SQL backend's.
    pub fn memory_source(&self, id: NodeId) -> Option<&MemorySource> {
        match self.node(id) {
            Operator::Source(SourceLeaf::Memory(s)) => Some(s),
            _ => None,
        }
    }
    fn source_conn(&self, id: NodeId) -> &SourceConn {
        match self.node(id) {
            Operator::SourceConn(s) => s,
            _ => panic!("expected SourceConn at {id:?}"),
        }
    }
    fn join(&self, id: NodeId) -> &Join {
        match self.node(id) {
            Operator::Join(j) => j,
            _ => panic!("expected Join at {id:?}"),
        }
    }
    fn view(&self, id: NodeId) -> &crate::view::View {
        match self.node(id) {
            Operator::View(v) => v,
            _ => panic!("expected View at {id:?}"),
        }
    }
    fn collector(&self, id: NodeId) -> &Collector {
        match self.node(id) {
            Operator::Collector(c) => c,
            _ => panic!("expected Collector at {id:?}"),
        }
    }
    #[cfg(any(test, feature = "testkit"))]
    fn catch_op(&self, id: NodeId) -> &crate::testkit::Catch {
        match self.node(id) {
            Operator::Catch(c) => c,
            _ => panic!("expected Catch at {id:?}"),
        }
    }
    #[cfg(any(test, feature = "testkit"))]
    fn snitch_op(&self, id: NodeId) -> &crate::testkit::Snitch {
        match self.node(id) {
            Operator::Snitch(s) => s,
            _ => panic!("expected Snitch at {id:?}"),
        }
    }
    fn filter_start_op(&self, id: NodeId) -> &FilterStart {
        match self.node(id) {
            Operator::FilterStart(s) => s,
            _ => panic!("expected FilterStart at {id:?}"),
        }
    }
    fn fan_out_op(&self, id: NodeId) -> &FanOut {
        match self.node(id) {
            Operator::FanOut(f) => f,
            _ => panic!("expected FanOut at {id:?}"),
        }
    }
    fn fan_in_op(&self, id: NodeId) -> &FanIn {
        match self.node(id) {
            Operator::FanIn(f) => f,
            _ => panic!("expected FanIn at {id:?}"),
        }
    }
    fn union_fan_out_op(&self, id: NodeId) -> &crate::op::UnionFanOut {
        match self.node(id) {
            Operator::UnionFanOut(u) => u,
            _ => panic!("expected UnionFanOut at {id:?}"),
        }
    }
    /// `pub(crate)`: [`UnionFanOut::push`](crate::op::UnionFanOut) drives its paired
    /// fan-in's accumulate/drain through this (the fan-out seam).
    pub(crate) fn union_fan_in_op(&self, id: NodeId) -> &crate::op::UnionFanIn {
        match self.node(id) {
            Operator::UnionFanIn(u) => u,
            _ => panic!("expected UnionFanIn at {id:?}"),
        }
    }
    fn filter_op(&self, id: NodeId) -> &Filter {
        match self.node(id) {
            Operator::Filter(f) => f,
            _ => panic!("expected Filter at {id:?}"),
        }
    }
    fn filter_probe_op(&self, id: NodeId) -> &FilterProbe {
        match self.node(id) {
            Operator::FilterProbe(p) => p,
            _ => panic!("expected FilterProbe at {id:?}"),
        }
    }

    /// Schema of an input node's output rows (for resolving child PK/sort inside
    /// relationship thunks). Recurses through joins to the parent source.
    ///
    /// `pub(crate)`: operators in [`crate::op`] resolve their input's schema
    /// through this (the fan-out seam, `op/mod.rs`).
    pub(crate) fn input_schema(&self, id: NodeId) -> &Schema {
        match self.node(id) {
            Operator::SourceConn(sc) => self.source(sc.source).schema(),
            Operator::Join(j) => self.input_schema(j.parent),
            // FlippedJoin outputs parent rows (with the child relationship attached).
            Operator::FlippedJoin(fj) => self.input_schema(fj.parent),
            // UnionFanOut passes its input through; UnionFanIn owns the merged schema.
            Operator::UnionFanOut(u) => self.input_schema(u.input),
            Operator::UnionFanIn(u) => u.schema(),
            Operator::Source(s) => s.schema(),
            // `View::schema` is `Arc<Schema>`; hand back the inner `&Schema`.
            Operator::View(v) => v.schema.as_ref(),
            Operator::Collector(c) => self.input_schema(c.input),
            // The filter sub-graph never reshapes rows (`07` §3.1), so every link's
            // output schema is its input's. Recurse to the upstream source side.
            Operator::FilterStart(s) => self.input_schema(s.input),
            Operator::FilterEnd(e) => self.input_schema(e.start),
            Operator::FanOut(f) => self.input_schema(f.input),
            Operator::FanIn(f) => self.input_schema(self.fan_out_op(f.fan_out).input),
            Operator::Filter(f) => self.input_schema(f.input),
            Operator::FilterProbe(p) => self.input_schema(p.input),
            Operator::Exists(e) => self.input_schema(e.input),
            // Skip/Take/Cap/Exists never reshape rows → output schema = input's.
            Operator::Skip(s) => self.input_schema(s.input),
            Operator::Take(t) => self.input_schema(t.input),
            Operator::Cap(c) => self.input_schema(c.input),
            // Reduce DOES reshape: its output is the synthetic aggregate row, so it
            // carries (and returns) its own schema rather than delegating upstream.
            Operator::Reduce(r) => &r.schema,
            // Testkit sinks/taps never reshape rows → output schema = input's.
            #[cfg(any(test, feature = "testkit"))]
            Operator::Catch(c) => self.input_schema(c.input),
            #[cfg(any(test, feature = "testkit"))]
            Operator::Snitch(s) => self.input_schema(s.input),
            Operator::Tombstone => panic!("input_schema on a torn-down node {id:?}"),
        }
    }

    /// Effective output ordering for `id`. This is distinct from
    /// [`Self::input_schema`]: a source connection can be ordered by a per-query sort
    /// that is not the table schema's primary sort.
    pub(crate) fn input_sort(&self, id: NodeId) -> Sort {
        match self.node(id) {
            Operator::SourceConn(sc) => self.source(sc.source).conn_sort(sc.conn),
            Operator::Join(j) => self.input_sort(j.parent),
            Operator::FlippedJoin(fj) => self.input_sort(fj.parent),
            Operator::UnionFanOut(u) => self.input_sort(u.input),
            Operator::UnionFanIn(u) => u.schema().sort.clone(),
            Operator::Source(s) => s.schema().sort.clone(),
            Operator::View(v) => v.schema.sort.clone(),
            Operator::Collector(c) => self.input_sort(c.input),
            Operator::FilterStart(s) => self.input_sort(s.input),
            Operator::FilterEnd(e) => self.input_sort(e.start),
            Operator::FanOut(f) => self.input_sort(f.input),
            Operator::FanIn(f) => self.input_sort(self.fan_out_op(f.fan_out).input),
            Operator::Filter(f) => self.input_sort(f.input),
            Operator::FilterProbe(p) => self.input_sort(p.input),
            Operator::Exists(e) => self.input_sort(e.input),
            Operator::Skip(s) => self.input_sort(s.input),
            Operator::Take(t) => t.sort.clone(),
            Operator::Cap(c) => self.input_sort(c.input),
            // Reduce reshapes to its synthetic row: its order is its own (empty) sort
            // (the global aggregate is a single row), not the input's.
            Operator::Reduce(r) => r.schema.sort.clone(),
            #[cfg(any(test, feature = "testkit"))]
            Operator::Catch(c) => self.input_sort(c.input),
            #[cfg(any(test, feature = "testkit"))]
            Operator::Snitch(s) => self.input_sort(s.input),
            Operator::Tombstone => panic!("input_sort on a torn-down node {id:?}"),
        }
    }

    pub fn cursors_open(&self, source: NodeId) -> i64 {
        self.source(source).cursors_open()
    }

    // -------------------------------------------------------------------
    // FETCH (pull). Every fetch is a *shared* `&'g self` borrow -> reentrancy
    // composes for free.
    // -------------------------------------------------------------------

    pub fn fetch<'g>(&'g self, id: NodeId, req: &FetchRequest) -> NodeStream<'g> {
        match self.node(id) {
            // The connection boundary: a source emits rows; the SourceConn wraps
            // each in a leaf node. Downstream joins then attach relationships.
            Operator::SourceConn(sc) => {
                Box::new(self.source(sc.source).fetch(sc.conn, req).map(Node::leaf))
            }
            Operator::Join(_) => self.join_fetch(id, req),
            // Out-of-file operators delegate to their own module (the fan-out seam).
            Operator::FlippedJoin(fj) => fj.fetch(self, req),
            // The union fan IS fetchable (unlike the filter fan): UnionFanOut
            // delegates to its input; UnionFanIn k-way-merges the branch fetches.
            Operator::UnionFanOut(u) => u.fetch(self, req),
            Operator::UnionFanIn(u) => u.fetch(self, req),
            Operator::Skip(s) => s.fetch(self, req),
            Operator::Take(t) => t.fetch(self, req),
            Operator::Cap(c) => c.fetch(self, req),
            Operator::Reduce(r) => r.fetch(self, req),
            // Testkit tap: Snitch logs the fetch and vends the upstream stream
            // wrapped to log a fetchCount on Drop (`crate::testkit`).
            #[cfg(any(test, feature = "testkit"))]
            Operator::Snitch(s) => s.fetch(self, req),
            // The filter sub-graph's two fetch boundaries: `FilterStart` runs the
            // bracketed scan; `FilterEnd` delegates to it (`filter-operators.ts:118`).
            Operator::FilterStart(_) => self.filter_start_fetch(id, req),
            Operator::FilterEnd(e) => self.fetch(e.start, req),
            Operator::Source(_) => panic!("fetch a SourceConn, not the Source directly"),
            Operator::View(_) => panic!("Views are sinks, not fetched"),
            Operator::Collector(_) => panic!("Collectors are sinks, not fetched"),
            #[cfg(any(test, feature = "testkit"))]
            Operator::Catch(_) => panic!("Catch is a sink, not fetched; use catch_fetch ({id:?})"),
            // FilterOperators gate (`filter(node)`), they do not vend — the scan is
            // owned by `FilterStart::fetch` (`07` §3.2).
            Operator::FanOut(_)
            | Operator::FanIn(_)
            | Operator::Filter(_)
            | Operator::FilterProbe(_)
            | Operator::Exists(_) => {
                panic!("filter-chain links have no fetch; fetch the FilterStart/End ({id:?})")
            }
            Operator::Tombstone => panic!("fetch on a torn-down node {id:?}"),
        }
    }

    /// Eagerly drain `fetch` and surface any runtime error a backend parked during
    /// the scan. The lazy [`Graph::fetch`] API remains infallible for iterator
    /// composition; production callers that need error propagation should use this
    /// owned-drain boundary.
    pub fn try_fetch_all<'g>(
        &'g self,
        id: NodeId,
        req: &FetchRequest,
    ) -> Result<Vec<Node<'g>>, RindleError> {
        let out: Vec<Node<'g>> = self.fetch(id, req).collect();
        self.take_runtime_error()?;
        Ok(out)
    }

    fn join_fetch<'g>(&'g self, jid: NodeId, req: &FetchRequest) -> NodeStream<'g> {
        let j = self.join(jid);
        let parent_stream = self.fetch(j.parent, req);
        Box::new(parent_stream.map(move |pnode| {
            // No overlay during a plain fetch.
            self.process_parent_node(jid, pnode)
        }))
    }

    /// Attach this join's child relationship to a parent node as a thunk that reads
    /// the join's **LIVE** in-flight child overlay on every evaluation (`join.ts:252`
    /// `#processParentNode`), **preserving the parent node's existing relationships**
    /// (`join.ts:298` `{...parentNodeRelations, [relName]: childStream}`). Unlike the
    /// by-value [`Graph::attach_child_rel`] (kept for [`FlippedJoin`]), the thunk here
    /// re-reads [`Join::inprogress_overlay`]/[`Join::inprogress_position`] each time it
    /// is drained — so a reentrant refetch a downstream `Take`/`Exists` triggers
    /// mid-push sees PRE-change membership for not-yet-processed parents (the depth-2
    /// EXISTS+top-N+push cascade). Everything is captured BY VALUE except `self` (the
    /// shared `&'g Graph`), through which the live fields are re-read.
    fn process_parent_node<'g>(&'g self, jid: NodeId, mut parent_node: Node<'g>) -> Node<'g> {
        let j = self.join(jid);
        let child_id = j.child;
        let parent_key = j.parent_key.clone();
        let child_key = j.child_key.clone();
        let rel_slot = j.rel_slot;
        let child_schema = self.input_schema(child_id);
        let child_pk = child_schema.primary_key.clone();
        let child_sort: Sort = child_schema.sort.clone();
        let pr = parent_node.row.clone();

        let thunk: Box<dyn Fn() -> NodeStream<'g> + 'g> = Box::new(move || {
            let base: NodeStream<'g> = match build_join_constraint(&pr, &parent_key, &child_key) {
                Some(c) => self.fetch(child_id, &FetchRequest::with_constraint(c)),
                None => Box::new(std::iter::empty()),
            };
            // Live, gated read of the in-flight child overlay (lazy `#inprogress…`).
            match self.join_overlay_for(jid, &pr) {
                Some(ov) => splice_join_overlay_pre(base, &ov, &child_pk, &child_sort),
                None => base,
            }
        });

        parent_node.rels.push(Relationship {
            slot: rel_slot,
            thunk,
        });
        parent_node
    }

    /// Walk a relationship join's child subtree from `node`, deleting partitioned
    /// limiter (`Take`/`Cap`) slots that match `constraint` (the parent→child
    /// correlation key of the parent that just left a bounded parent view). Recurses
    /// through the row-preserving passthrough operators between the join and its
    /// limiter(s); stops at the source connection and at a nested `Join` (a grandchild
    /// relationship is keyed by a *different* correlation, so this constraint does not
    /// identify its partitions — a deeper-nesting residual, not a correctness issue).
    fn evict_child_partitions(&self, node: NodeId, constraint: &Constraint) {
        match self.node(node) {
            Operator::Take(t) => {
                t.evict_partition(self, constraint);
                // A Skip/Filter could sit between the Take and the source; recurse so a
                // limited-with-offset child still reaches its limiter chain.
                self.evict_child_partitions(t.input, constraint);
            }
            Operator::Cap(c) => {
                c.evict_partition(self, constraint);
                self.evict_child_partitions(c.input, constraint);
            }
            Operator::Skip(s) => self.evict_child_partitions(s.input, constraint),
            Operator::FilterStart(s) => self.evict_child_partitions(s.input, constraint),
            Operator::FilterEnd(e) => self.evict_child_partitions(e.start, constraint),
            // Anything else (SourceConn, a nested Join, a union/reduce reshape) is not a
            // partitioned child limiter keyed by THIS constraint — stop the walk.
            _ => {}
        }
    }

    /// The gated read of a [`Join`]'s LIVE in-flight child overlay for `parent_row`
    /// (Zero `join.ts:264-277`). Returns the overlay iff (a) the in-flight child joins
    /// to this parent (`isJoinMatch`) and (b) the parent sorts STRICTLY AFTER the
    /// parent currently being fanned out (`compareRows(parent, position) > 0`) — i.e.
    /// the change has not yet been delivered for this parent, so its relationship must
    /// still reflect the PRE-change child. `None` outside a child-push, for an
    /// unrelated parent, or for an at-or-before parent.
    fn join_overlay_for(&self, jid: NodeId, parent_row: &Row) -> Option<JoinOverlay> {
        let j = self.join(jid);
        let overlay = j.inprogress_overlay.borrow().clone()?;
        let position = j.inprogress_position.borrow().clone()?;
        if !is_join_match(
            parent_row,
            &j.parent_key,
            overlay.change.row(),
            &j.child_key,
        ) {
            return None;
        }
        // The fan-out delivers parents in the parent's EFFECTIVE output order (the
        // per-query sort, e.g. `orderBy`), NOT the table's primary-key sort — so the
        // "already delivered?" position gate must compare in that same order
        // (`input_sort`, not `input_schema(...).sort`). Zero uses the join schema's
        // `compareRows`, which for a memory connection is the resolved query order.
        let parent_sort = self.input_sort(j.parent);
        if compare_rows(&parent_sort, parent_row, &position) == std::cmp::Ordering::Greater {
            Some(overlay)
        } else {
            None
        }
    }

    /// The field-based core of [`Graph::process_parent_node`]: attach a join child
    /// relationship (`parent_key → child_key` fetch, optionally spliced with an
    /// in-progress child `overlay`) to `parent_node`, **appending** so the parent's
    /// existing relationships survive. Captures everything the thunk needs BY VALUE
    /// (Primitive #5), so the thunk owns its inputs and never reads a live operator
    /// field. `pub(crate)` so the out-of-file [`FlippedJoin`](crate::op::FlippedJoin)
    /// — which shares `Join`'s relationship + overlay model — builds the same thunk.
    pub(crate) fn attach_child_rel<'g>(
        &'g self,
        child_id: NodeId,
        parent_key: &[ColId],
        child_key: &[ColId],
        rel_slot: RelId,
        mut parent_node: Node<'g>,
        overlay: Option<JoinOverlay>,
    ) -> Node<'g> {
        let parent_key = parent_key.to_vec();
        let child_key = child_key.to_vec();
        let child_schema = self.input_schema(child_id);
        let child_pk = child_schema.primary_key.clone();
        let child_sort: Sort = child_schema.sort.clone();
        let pr = parent_node.row.clone();

        let thunk: Box<dyn Fn() -> NodeStream<'g> + 'g> = Box::new(move || {
            let base: NodeStream<'g> = match build_join_constraint(&pr, &parent_key, &child_key) {
                Some(c) => self.fetch(child_id, &FetchRequest::with_constraint(c)),
                None => Box::new(std::iter::empty()),
            };
            match &overlay {
                None => base,
                Some(ov) => splice_join_overlay(
                    base,
                    ov,
                    &pr,
                    &parent_key,
                    &child_key,
                    &child_pk,
                    &child_sort,
                ),
            }
        });

        // Append (don't replace) so the parent's pre-existing relationships survive.
        parent_node.rels.push(Relationship {
            slot: rel_slot,
            thunk,
        });
        parent_node
    }

    // -------------------------------------------------------------------
    // The Filter sub-graph: bracketed fetch + the begin/filter/end lifecycle.
    // -------------------------------------------------------------------

    /// `FilterStart::fetch` (`filter-operators.ts:86-103`). The single place the
    /// row scan happens for a `where`: pull each node from the upstream `Input`,
    /// gate it through the chain (`chain_filter`), yield iff it passes. The scan
    /// is bracketed by `begin_filter` (before) / `end_filter` (after) so a stateful
    /// link can cache for the loop. `end_filter` runs via an [`EndFilterGuard`]
    /// MOVED INTO the returned stream's closure — so dropping the stream early (a
    /// downstream `Take` break, `first()`) runs it. Unwinding also runs it, but
    /// only in unwinding builds; under the shipping `panic = "abort"` client
    /// profile a panic aborts the process and `Drop` does not run. This is the
    /// JS `try/finally` rendered as RAII (Primitive #2; `07` §6.4). See WS02.
    fn filter_start_fetch<'g>(&'g self, id: NodeId, req: &FetchRequest) -> NodeStream<'g> {
        // PANIC-CLASS: build-time — every `.expect("… not wired")` in this file
        // (FilterStart chain head, Filter/FilterProbe/FilterEnd/FanOut/join output)
        // asserts an edge the *builder* must have set before any fetch/push runs. An
        // unwired edge is an engine wiring bug, never data-reachable (WS02.1). Grep
        // the keep-bucket with `rg '"[^"]* not wired"' src/`.
        let chain = self
            .filter_start_op(id)
            .chain_head
            .get()
            .expect("FilterStart chain head not wired");
        let input = self.filter_start_op(id).input;

        // `begin_filter` runs before the guard exists — matching the JS, where
        // `beginFilter()` sits *outside* the `try/finally` (a begin-time throw
        // skips `endFilter` there too). The guard is built *before* the upstream
        // fetch and moved into the stream, so every panic on the actual scan path
        // (the data-driven one) is covered; `begin_filter` itself is infallible on
        // a correctly-wired graph (its only panic is the mis-wired-node arm).
        self.begin_filter(chain);
        let guard = EndFilterGuard { g: self, chain };
        let upstream = self.fetch(input, req);

        Box::new(upstream.filter_map(move |node| {
            // `guard` is owned by this closure → its Drop (end_filter) fires
            // exactly when the stream is dropped, however that happens.
            let _hold = &guard;
            if self.chain_filter(chain, &node) {
                Some(node)
            } else {
                None
            }
        }))
    }

    /// Begin a fetch loop for the chain rooted at `link` (`beginFilter()`,
    /// `filter-operators.ts:37`). Walks the same topology as [`chain_filter`]:
    /// every stateful link gets one `begin` per loop. No-op for stateless links.
    ///
    /// A `Filter`/`FilterProbe` link is never a chain terminator, so its `output`
    /// must be wired (`.expect`) — same strict contract as the push path
    /// ([`Graph::chain_push`]). Only a `FanOut`'s post-fan continuation is
    /// legitimately optional.
    pub(crate) fn begin_filter(&self, link: NodeId) {
        match self.node(link) {
            Operator::Filter(f) => {
                self.begin_filter(f.output.get().expect("Filter output not wired"))
            }
            Operator::FilterProbe(p) => {
                p.begin_count.set(p.begin_count.get() + 1);
                self.begin_filter(p.output.get().expect("FilterProbe output not wired"));
            }
            Operator::FanOut(fo) => {
                let branches = fo.outputs.borrow().clone();
                let cont = self.fan_continuation(link);
                for b in branches {
                    self.begin_filter(b);
                }
                if let Some(c) = cont {
                    self.begin_filter(c);
                }
            }
            // Branch terminator / chain terminator: the continuation past a FanIn
            // is begun by the owning FanOut; FilterEnd ends the chain.
            Operator::Exists(e) => e.begin(self),
            Operator::FanIn(_) | Operator::FilterEnd(_) => {}
            _ => panic!("begin_filter: not a filter-chain link ({link:?})"),
        }
    }

    /// `end_filter` (`endFilter()`, `filter-operators.ts:37`) — the mirror of
    /// [`Graph::begin_filter`]. Runs from the [`EndFilterGuard`]'s `Drop`.
    pub(crate) fn end_filter(&self, link: NodeId) {
        match self.node(link) {
            Operator::Filter(f) => {
                self.end_filter(f.output.get().expect("Filter output not wired"))
            }
            Operator::FilterProbe(p) => {
                p.end_count.set(p.end_count.get() + 1);
                self.end_filter(p.output.get().expect("FilterProbe output not wired"));
            }
            Operator::FanOut(fo) => {
                let branches = fo.outputs.borrow().clone();
                let cont = self.fan_continuation(link);
                for b in branches {
                    self.end_filter(b);
                }
                if let Some(c) = cont {
                    self.end_filter(c);
                }
            }
            Operator::Exists(e) => e.end(self),
            Operator::FanIn(_) | Operator::FilterEnd(_) => {}
            _ => panic!("end_filter: not a filter-chain link ({link:?})"),
        }
    }

    /// Walk the filter chain for `node`, returning whether it passes ALL links
    /// (`FilterOutput::filter`, `filter-operators.ts:37`). Each link is `pred &&
    /// downstream`; `FanOut` is OR-over-branches AND the post-fan continuation;
    /// `FanIn`/`FilterEnd` terminate the walk with `true`.
    ///
    /// `Filter`/`FilterProbe` outputs are `.expect`-wired (a link is never a
    /// terminator — same as the push path); only a `FanOut`'s continuation is
    /// optional. Never holds a `RefCell` borrow across a recursive call (the
    /// cardinal rule, `01` §6): `FanOut`'s branch list is cloned to a local first.
    pub(crate) fn chain_filter(&self, link: NodeId, node: &Node) -> bool {
        match self.node(link) {
            Operator::Filter(f) => {
                f.pred.eval(&node.row)
                    && self.chain_filter(f.output.get().expect("Filter output not wired"), node)
            }
            Operator::FilterProbe(p) => {
                p.filter_count.set(p.filter_count.get() + 1);
                self.chain_filter(p.output.get().expect("FilterProbe output not wired"), node)
            }
            Operator::FanOut(fo) => {
                let branches = fo.outputs.borrow().clone();
                // OR: short-circuit on the first branch that passes.
                let any = branches.iter().any(|&b| self.chain_filter(b, node));
                let cont = self.fan_continuation(link);
                any && cont.is_none_or(|c| self.chain_filter(c, node))
            }
            // Branch terminator (a branch's `filter` ends at its FanIn) and chain
            // terminator both return true.
            Operator::Exists(e) => e.filter(self, node),
            Operator::FanIn(_) | Operator::FilterEnd(_) => true,
            _ => panic!("chain_filter: not a filter-chain link ({link:?})"),
        }
    }

    /// The post-fan continuation of a `FanOut`: its paired `FanIn`'s downstream
    /// output (the link *after* the fan). `None` until wired.
    fn fan_continuation(&self, fan_out: NodeId) -> Option<NodeId> {
        let fin = self
            .fan_out_op(fan_out)
            .fan_in
            .get()
            .expect("FanOut fan_in not wired");
        self.fan_in_op(fin).output.get()
    }

    // -------------------------------------------------------------------
    // PUSH (eager). A whole push completes before the next (load-bearing).
    // `&self` shared borrow throughout -> reentrant fetch is legal.
    // -------------------------------------------------------------------

    /// `pub(crate)`: operators in [`crate::op`] drive their downstream edge
    /// through this (the fan-out seam). Single-output operators pass
    /// [`Port::Single`].
    pub(crate) fn push<'g>(&'g self, id: NodeId, change: Change<'g>, port: Port) {
        match self.node(id) {
            Operator::Join(_) => self.join_push(id, change, port),
            Operator::FlippedJoin(fj) => fj.push(self, change, port),
            // The union fan (reached on `Port::Single`): UnionFanOut broadcasts to
            // the branches; UnionFanIn accumulates/forwards.
            Operator::UnionFanOut(u) => u.push(self, change),
            Operator::UnionFanIn(u) => u.push(self, change),
            Operator::Skip(s) => s.push(self, change),
            Operator::Take(t) => t.push(self, change),
            Operator::Cap(c) => c.push(self, change),
            Operator::Reduce(r) => r.push(self, change),
            Operator::View(_) => self.view_push(id, change),
            Operator::Collector(_) => self.collector_push(id, change),
            // Testkit: Snitch logs + forwards on the arriving port (transparent);
            // Catch records the expanded change tree (the sink terminus).
            #[cfg(any(test, feature = "testkit"))]
            Operator::Snitch(s) => s.push(self, change, port),
            #[cfg(any(test, feature = "testkit"))]
            Operator::Catch(c) => {
                // Catch is a terminus: it only ever sits on a single-output edge
                // (a non-Single port means it was mis-wired before a join port).
                // Guard symmetrically with the fetch-path panic above.
                assert!(
                    matches!(port, Port::Single),
                    "Catch is a sink; expected Port::Single, got {port:?} ({id:?})"
                );
                c.record(self, &change)
            }
            // The filter sub-graph's push entry: push into the chain head. The
            // chain transforms/gates and side-effects the real downstream push at
            // its `FilterEnd`; nothing should bubble back up here.
            Operator::FilterStart(s) => {
                let head = s
                    .chain_head
                    .get()
                    .expect("FilterStart chain head not wired");
                let leftover = self.chain_push(head, change);
                debug_assert!(
                    leftover.is_empty(),
                    "filter sub-graph push did not terminate at a FilterEnd"
                );
            }
            _ => panic!("cannot push to {id:?}"),
        }
    }

    /// Push a change *through* a filter-chain link, returning the change(s) it
    /// hands back to its caller. The filter sub-graph is a **pure transform
    /// region**: links gate/split rows and never touch sink state (the
    /// side-effecting sink lives downstream of `FilterEnd`). So a link RETURNS its
    /// output rather than pushing it onward — which is what lets the OR fan
    /// accumulate branch results **on the call stack** (lifetime `'g`) instead of
    /// in a self-referential `FanIn` field (see the chassis note on the operators).
    ///
    /// Returns a non-empty vec ONLY at a `FanIn` (a branch terminator handing its
    /// change up to the owning `FanOut`). `FilterEnd` side-effects the real
    /// downstream push and returns empty; `Filter`/`FilterProbe`/`FanOut` recurse.
    pub(crate) fn chain_push<'g>(&'g self, link: NodeId, change: Change<'g>) -> Vec<Change<'g>> {
        match self.node(link) {
            Operator::Filter(_) => self.filter_chain_push(link, change),
            Operator::FilterProbe(p) => {
                // Pass-through gate (proof instrumentation has no predicate).
                let out = p.output.get().expect("FilterProbe output not wired");
                self.chain_push(out, change)
            }
            Operator::FanOut(_) => self.fan_out_push(link, change),
            Operator::Exists(e) => e.push_chain(self, change),
            // Branch terminator: hand the change up to the owning FanOut, which
            // owns the accumulation + collapse.
            Operator::FanIn(_) => vec![change],
            // Chain terminator: exit the sub-graph — the real downstream push.
            Operator::FilterEnd(e) => {
                let out = e.output.get().expect("FilterEnd output not wired");
                self.push(out.node, change, out.port);
                Vec::new()
            }
            _ => panic!("chain_push: not a filter-chain link ({link:?})"),
        }
    }

    /// `Filter` link push: `filterPush` + the Edit split (`07` §3.4,
    /// `maybe-split-and-push-edit-change.ts`). Add/Remove/Child pass through iff
    /// the predicate holds; an Edit splits on `(pred(old), pred(new))`.
    fn filter_chain_push<'g>(&'g self, link: NodeId, change: Change<'g>) -> Vec<Change<'g>> {
        let out = self
            .filter_op(link)
            .output
            .get()
            .expect("Filter output not wired");
        match change {
            Change::Add(n) | Change::Remove(n) if !self.filter_op(link).pred.eval(&n.row) => {
                let _ = n; // predicate failed → drop
                Vec::new()
            }
            Change::Add(n) => self.chain_push(out, Change::Add(n)),
            Change::Remove(n) => self.chain_push(out, Change::Remove(n)),
            Change::Edit { node, old } => {
                let pred = &self.filter_op(link).pred;
                match (pred.eval(&old.row), pred.eval(&node.row)) {
                    (true, true) => self.chain_push(out, Change::Edit { node, old }),
                    (true, false) => self.chain_push(out, Change::Remove(old)),
                    (false, true) => self.chain_push(out, Change::Add(node)),
                    (false, false) => Vec::new(),
                }
            }
            Change::Child { node, rel, child } => {
                // `filterPush` (`filter-push.ts:26`): a Child passes through unchanged
                // iff the predicate holds on the change's node row; else it is dropped.
                // (The nested sub-change rides along — a leaf Filter gates the parent
                // row, it never reshapes the child.)
                if self.filter_op(link).pred.eval(&node.row) {
                    self.chain_push(out, Change::Child { node, rel, child })
                } else {
                    Vec::new()
                }
            }
        }
    }

    /// `FanOut` push (`fan-out.ts:74`): replay the change to every branch, then
    /// collapse what the branches forwarded into exactly one change per type
    /// (`push_accumulated_changes`, `06` §3.5) and continue down the post-fan
    /// chain. The collapse is the dedup that stops an OR from double-counting a
    /// row that several branches keep.
    fn fan_out_push<'g>(&'g self, link: NodeId, change: Change<'g>) -> Vec<Change<'g>> {
        // Clone the branch list to a local — never hold the RefCell borrow across
        // the reentrant branch pushes (the cardinal rule, `01` §6).
        let branches = self.fan_out_op(link).outputs.borrow().clone();
        let fan_out_type = change.change_type();

        // Materialize the change once, rebuild a fresh copy per branch (the owned-change
        // model — a `Change<'g>` is neither `Clone` nor `'static`). The broadcast
        // **preserves relationships for every change type**: an `Exists` branch counts
        // its gated relationship off the change's node — on an Add/Remove/Edit (a
        // parent membership-test) as well as a Child (a relationship-size flip) — so
        // every branch's copy must carry it. A leaf-`Filter` branch reads only
        // `node.row` and ignores the rest; a leaf node (a pure-predicate fan with no
        // join above) materializes back to a leaf, so this stays cheap there.
        let owned = materialize_change_preserving_node(change);
        let mut accumulated: Vec<Change<'g>> = Vec::new();
        for b in &branches {
            accumulated.extend(self.chain_push(*b, rebuild_change(owned.clone())));
        }

        let collapsed = collapse_accumulated(accumulated, fan_out_type);

        let cont = self.fan_continuation(link);
        let mut leftover = Vec::new();
        for c in collapsed {
            match cont {
                Some(next) => leftover.extend(self.chain_push(next, c)),
                None => leftover.push(c),
            }
        }
        leftover
    }

    fn join_push<'g>(&'g self, jid: NodeId, change: Change<'g>, port: Port) {
        let j = self.join(jid);
        let out = j.output.get().expect("join output not wired");
        let parent_key = j.parent_key.clone();
        let child_key = j.child_key.clone();
        let parent = j.parent;
        let child = j.child;
        match port {
            // Parent side (`join.ts#pushParent`): re-process the parent node (which
            // re-attaches this join's child relationship) and forward, preserving
            // change type.
            Port::JoinParent => match change {
                Change::Add(node) => {
                    let pn = self.process_parent_node(jid, node);
                    self.push(out.node, Change::Add(pn), out.port);
                }
                Change::Remove(node) => {
                    // Keep the parent row to evict its child partition AFTER the
                    // downstream has consumed the removed subtree (which drains the
                    // child thunk against the still-present partition).
                    let key_row = node.row.clone();
                    let pn = self.process_parent_node(jid, node);
                    self.push(out.node, Change::Remove(pn), out.port);
                    // The parent left a (bounded) parent view. Evict the child
                    // limiter partition it hydrated so partitioned child Takes/Caps do
                    // not leak one slot per distinct parent that ever passed through the
                    // view — but ONLY when the parent correlation key is the parent's
                    // primary key, so the evicted partition belongs to exactly this one
                    // parent (a non-unique parent key could share a child partition with
                    // a still-live sibling parent). See `Take::evict_partition`.
                    if self.input_schema(parent).primary_key == parent_key {
                        if let Some(c) = build_join_constraint(&key_row, &parent_key, &child_key) {
                            self.evict_child_partitions(child, &c);
                        }
                    }
                }
                Change::Edit { node, old } => {
                    // A parent edit MUST NOT change the join key (`join.ts:167`);
                    // otherwise the relationship membership would change and this
                    // would not be a simple edit. Data-reachable (a malformed change
                    // stream): strict ⇒ park a `ConsistencyViolation` and skip the
                    // push (the boundary `take_runtime_error` re-raises it); non-strict
                    // ⇒ `debug_assert!` (WS02.2).
                    if !row_equals_for_compound_key(&old.row, &node.row, &parent_key) {
                        if self.validate_changes.get() {
                            self.park_runtime_error(RindleError::ConsistencyViolation {
                                kind: "parent edit changed the join key",
                            });
                            return;
                        }
                        debug_assert!(
                            false,
                            "parent edit must not change the join relationship key"
                        );
                    }
                    let pn = self.process_parent_node(jid, node);
                    let po = self.process_parent_node(jid, old);
                    self.push(out.node, Change::Edit { node: pn, old: po }, out.port);
                }
                Change::Child { node, rel, child } => {
                    // Passthrough (`join.ts:154`): a relationship *above* this join
                    // changed; re-process the parent (re-attaching our rel) and
                    // forward the original child change untouched.
                    let pn = self.process_parent_node(jid, node);
                    self.push(
                        out.node,
                        Change::Child {
                            node: pn,
                            rel,
                            child,
                        },
                        out.port,
                    );
                }
            },
            // Child side (`join.ts#pushChild`): re-enter the parent fetch with the
            // child's key and emit a Child change per matching parent. The full child
            // change is carried up — Add/Remove/Edit carry the (re-derivable) child
            // node, and a **nested `Change::Child`** (the *deepest* push, e.g. a
            // reaction under `issue{comments{reactions}}`) carries the inner change
            // verbatim so the View/Catch recurse into it. [`push_child_change`]
            // materializes the change once and rebuilds a fresh copy per parent.
            Port::JoinChild => {
                if let Change::Edit { node, old } = &change {
                    // Same join-key-immutability contract as the parent-edit check
                    // above (WS02.2): strict ⇒ park a `ConsistencyViolation` + skip;
                    // non-strict ⇒ `debug_assert!`.
                    if !row_equals_for_compound_key(&old.row, &node.row, &child_key) {
                        if self.validate_changes.get() {
                            self.park_runtime_error(RindleError::ConsistencyViolation {
                                kind: "child edit changed the join key",
                            });
                            return;
                        }
                        debug_assert!(
                            false,
                            "child edit must not change the join relationship key"
                        );
                    }
                }
                self.push_child_change(jid, change);
            }
            // PANIC-CLASS: internal — a Join is only ever dispatched on JoinParent /
            // JoinChild; a Single port here is an engine dispatch/wiring bug, not data.
            Port::Single => panic!("join received Single port"),
        }
    }

    /// The reentrant crux (`join.ts#pushChildChange`): build a parent constraint
    /// from the changed child's row, re-enter `parent.fetch` *mid-push*, and for each
    /// matching parent emit a `Change::Child` carrying the child change.
    ///
    /// `child_change` is the **full** downstream change on this join's child input —
    /// `Add`/`Remove`/`Edit` of a child row, or a nested `Change::Child` (the
    /// *deepest* push: a relationship under this child changed). A child can join to
    /// many parents, so the change is materialized **once** (its thunks drained now,
    /// under the active source overlay) and a fresh `Change<'g>` is rebuilt per
    /// parent. The row-level overlay (`Add`/`Remove`/`Edit` only) is published on the
    /// join as a LIVE field ([`Join::inprogress_overlay`]) so a reentrant refetch
    /// during the downstream push sees PRE-change membership for not-yet-processed
    /// parents (Zero `join.ts#pushChildChange`); a nested `Child` carries no overlay
    /// (see [`JoinOverlay::for_child_change`]).
    fn push_child_change<'g>(&'g self, jid: NodeId, child_change: Change<'g>) {
        let j = self.join(jid);
        let out = j.output.get().expect("join output not wired");
        let rel_slot = j.rel_slot;
        let parent = j.parent;
        // The key row is the changed-child's row (an Edit keeps the same join key,
        // asserted on entry; a nested Child carries the unchanged child row), so the
        // constraint and the per-parent overlay agree.
        let key_row = child_change.primary_row().clone();
        let constraint = match build_join_constraint(&key_row, &j.child_key, &j.parent_key) {
            Some(c) => c,
            None => return, // null child key cannot join
        };
        let overlay = JoinOverlay::for_child_change(&child_change);
        // An Edit's `old` subtree is consumed by no downstream (View reduces Edit→rows;
        // Exists gates only the new node), and the join-key-immutability contract makes
        // it identical to the already-materialized one anyway — so materialize it
        // row-only, skipping the reentrant leaf refetch of the whole old child subtree.
        let owned = materialize_change_edit_old_row_only(child_change);

        // Publish the in-flight child overlay LIVE; the guard clears it on every exit
        // (the JS `try/finally`, `join.ts:222-249`).
        *self.join(jid).inprogress_overlay.borrow_mut() = overlay;
        let _guard = InprogressGuard { g: self, jid };

        // Collect the reentrant parent fetch (with relationships preserved) so we
        // don't hold its `&self` stream borrow across the downstream push.
        let parents: Vec<Node> = self
            .fetch(parent, &FetchRequest::with_constraint(constraint))
            .collect();

        // The unbounded join fan-out (RUNAWAY-PUSH-FINDINGS §3): one child change pushes
        // one `Change::Child` per matching parent, O(N) with no downstream-limit awareness
        // — THE loop a runaway push spins in, so it carries a deadline checkpoint (§6.6). The
        // tick counter is graph-level (shared across the whole armed batch), so many small
        // changes accumulate toward the checkpoint too — not just one huge single-change fan-out.
        for pnode in parents {
            if self.push_deadline_exceeded() {
                self.park_runtime_error(RindleError::PushDeadlineExceeded {
                    site: "join child fan-out",
                });
                break; // torn is fine — the host discards the engine (abort≙epoch-rehydrate)
            }
            // Advance the in-flight position to the parent now being delivered; the
            // overlay applies only to parents sorting strictly after it.
            *self.join(jid).inprogress_position.borrow_mut() = Some(pnode.row.clone());
            let pn = self.process_parent_node(jid, pnode);
            let child = Box::new(rebuild_change(owned.clone()));
            self.push(
                out.node,
                Change::Child {
                    node: pn,
                    rel: rel_slot,
                    child,
                },
                out.port,
            );
        }
    }

    // -------------------------------------------------------------------
    // Source push: fan the change out to every connection (overlay active),
    // then clear the overlay, then commit the write. The orchestration
    // (edit-split, existence asserts, write-after-drain) lives in
    // `source_common`; the graph only supplies the downstream driver.
    // -------------------------------------------------------------------

    /// Infallible push (tests / prototyping).
    ///
    /// # Panics
    /// On any `RindleError` (SQLite I/O, a strict consistency violation, …). Prefer
    /// [`try_source_push`](Self::try_source_push) in production so the error is
    /// handled, not aborted (WS02.6 fault model).
    pub fn source_push(&self, src_id: NodeId, change: SourceChange) {
        self.try_source_push(src_id, change)
            .expect("source push failed")
    }

    pub fn try_source_push(&self, src_id: NodeId, change: SourceChange) -> Result<(), RindleError> {
        // Time the whole write-path push (the headline latency, 208.2) — one timer per
        // call, dropped on every return path; no-op with `metrics` off.
        let _timer = metric_timer!(source_push);
        let s = self.source(src_id);
        let strict = self.validate_changes.get();
        // Classify before `change` is moved into the push (compiled out when the
        // `metrics` feature is off); count `rindle.changes.processed` only after the
        // push succeeds below — a rejected change was not processed.
        let _change_kind = metric_change_kind!(&change);
        // The connection boundary on the write path: the source fans out a
        // `SourceChange` (rows); the `SourceConn` wraps it into a node-bearing
        // downstream `Change` (leaf nodes — joins attach relationships later).
        s.try_push(
            change,
            &|conn: &Connection, sc: SourceChange| {
                if let Some(edge) = conn.output.get() {
                    self.push(edge.node, source_change_to_node(sc), edge.port);
                }
            },
            strict,
        )?;
        metric_changes_inc!(_change_kind);
        self.take_runtime_error()
    }

    /// Server fault boundary (WS02.5): run one mutation under `catch_unwind` so a
    /// *residual* — unconverted, genuinely-unexpected — panic during this push does
    /// not abort the whole process (which, serving many connections, would take them
    /// all down). A caught panic becomes a `RindleError::Storage("internal panic: …")`.
    ///
    /// Effective only under `panic = "unwind"` (the `release-server` profile, D2);
    /// under `panic = "abort"` (the default/wasm client) a panic aborts regardless
    /// and this just forwards the result. It is the *last-resort net* **after** the
    /// WS02.1–02.4 conversions of the expected data-reachable panics — not a
    /// substitute for them.
    ///
    /// **Recovery contract: fail the connection, do not retry in place.** A panic
    /// mid-mutation can leave operator/view state logically torn (a `RefCell` is
    /// released on unwind — *not* poisoned, so the graph is still *usable*, just
    /// possibly inconsistent). The owning connection MUST discard and re-hydrate the
    /// view from source before serving further reads (WS09.6), never reuse it.
    pub fn source_push_isolated(
        &self,
        src_id: NodeId,
        change: SourceChange,
    ) -> Result<(), RindleError> {
        use std::panic::{catch_unwind, AssertUnwindSafe};
        match catch_unwind(AssertUnwindSafe(|| self.try_source_push(src_id, change))) {
            Ok(result) => result,
            Err(payload) => {
                // The containment fired: count it (208.5) so a rising `mutation_panics_total`
                // flags a bad mutation loose in the graph. No-op with `metrics` off.
                metric_inc!(mutation_panics);
                let msg = if let Some(s) = payload.downcast_ref::<&str>() {
                    (*s).to_string()
                } else if let Some(s) = payload.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "unknown panic".to_string()
                };
                Err(RindleError::Storage(format!(
                    "internal panic during push: {msg}"
                )))
            }
        }
    }

    // -------------------------------------------------------------------
    // View: hydrate / push / flush / listeners (the `array-view.ts` halves;
    // the pure folding lives in `crate::view::apply_change`).
    // -------------------------------------------------------------------

    /// Build the initial view by draining the input pipeline (`#hydrate`,
    /// `array-view.ts:140`). The fetch runs here (the `Graph` borrow stays with the
    /// caller); the [`View`](crate::view::View) folds each node `InPlace` and flushes
    /// once. No `RefCell` borrow is held across the fetch (the root is taken out
    /// inside `hydrate_from`, after this iterator is constructed).
    /// Infallible hydrate (tests / prototyping).
    ///
    /// # Panics
    /// On any `RindleError` from the initial fetch. Prefer
    /// [`try_hydrate`](Self::try_hydrate) in production (WS02.6 fault model).
    pub fn hydrate(&self, view_id: NodeId) {
        self.try_hydrate(view_id).expect("hydrate failed")
    }

    pub fn try_hydrate(&self, view_id: NodeId) -> Result<(), RindleError> {
        // Subscription cold-start latency (208.2): the one-time hydrate cost.
        let _timer = metric_timer!(hydrate);
        let input = self.view(view_id).input;
        let nodes: Vec<Node> = self.fetch(input, &FetchRequest::all()).collect();
        self.take_runtime_error()?;
        self.view(view_id).hydrate_from(nodes.into_iter());
        self.take_runtime_error()
    }

    /// Fold a downstream `Change` into the view (the pipeline's terminal `Output`).
    fn view_push<'g>(&'g self, view_id: NodeId, change: Change<'g>) {
        self.view(view_id).push_change(change);
    }

    /// Fire the view's listeners with the current snapshot and close the
    /// transaction (`flush`, `array-view.ts:173`).
    pub fn flush_view(&self, view_id: NodeId) {
        self.view(view_id).flush();
    }

    pub fn take_runtime_error(&self) -> Result<(), RindleError> {
        // The graph-level sink first (parked deep in the fan-out), then per-source
        // parked errors (the SQLite leaf fetch path).
        if let Some(err) = self.runtime_error.borrow_mut().take() {
            return Err(err);
        }
        for op in &self.nodes {
            match op {
                Operator::Source(source) => {
                    if let Some(err) = source.take_error() {
                        return Err(err);
                    }
                }
                // The §5.3 integer-sum overflow check (design 226) is state-based:
                // it raises while any stored group's all-integer set total is
                // outside i64 (SQLite's result contract — a fresh `SELECT sum(…)`
                // there errors every time too), and clears the moment the state
                // shrinks back into range. A delta-order transient corrected
                // before this boundary is consulted therefore never errors.
                Operator::Reduce(r) if r.overflow_count() > 0 => {
                    return Err(RindleError::UnsupportedValue(
                        "integer sum overflow: an aggregate's set total is outside \
                         the i64 range (design 226 §5.3)"
                            .into(),
                    ));
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// Park a runtime error on the graph-level sink (set-if-empty, so the first
    /// failure in a push wins). Drained by [`take_runtime_error`](Self::take_runtime_error)
    /// at the mutation boundary. Used where a failure arises deep in the push
    /// fan-out and cannot be returned up the stack (WS02.2 join-key check; WS02.3).
    pub(crate) fn park_runtime_error(&self, err: RindleError) {
        let mut slot = self.runtime_error.borrow_mut();
        if slot.is_none() {
            *slot = Some(err);
        }
    }

    /// Register a flush listener on the view; fires once immediately
    /// (`addListener`, `array-view.ts:115`). Returns its index.
    pub fn view_add_listener(&self, view_id: NodeId, l: crate::view::Listener) -> usize {
        self.view(view_id).add_listener(l)
    }

    /// The view's current [`ResultType`](crate::view::ResultType).
    pub fn view_result_type(&self, view_id: NodeId) -> crate::view::ResultType {
        self.view(view_id).result_type()
    }

    /// Resolve a pending query's result type (the async `queryComplete` path,
    /// `09` §3.8); fires listeners out of band.
    pub fn set_view_result_type(&self, view_id: NodeId, rt: crate::view::ResultType) {
        self.view(view_id).set_result_type(rt);
    }

    // -------------------------------------------------------------------
    // Collector: record pushes (+ optional fetch-during-push)
    // -------------------------------------------------------------------

    fn collector_push(&self, id: NodeId, change: Change) {
        // Change-stream sink capture (opt-in via `add_change_sink`): record the full
        // nested `CaughtChange` tree. Done first, because the flat path below early-
        // returns on `Change::Child` — but a change sink must see Child events too.
        if self.collector(id).capture_caught.get() {
            let caught = crate::changes::expand_change(&change);
            self.collector(id).caught.borrow_mut().push(caught);
            // A production change-sink (the `rindle-replica` seam) is drained ONLY through
            // `take_sink_changes` (the `caught` buffer); it never reads the flat testkit
            // `changes`/`fetched` recordings. Accumulating them here would grow `changes` by
            // one entry on every push, FOREVER — a per-commit leak invisible to
            // `storage_snapshot` (it scans operator storage, not collector buffers) that only
            // frees on teardown. So a change-sink stops here. (`capture_caught` is set only by
            // `add_change_sink`; a plain testkit `Collector` keeps the flat recording below.)
            return;
        }
        let (collected, input, fetch_on_push) = {
            let c = self.collector(id);
            let collected = match &change {
                Change::Add(n) => CollectedChange::Add(n.row.clone()),
                Change::Remove(n) => CollectedChange::Remove(n.row.clone()),
                Change::Edit { node, old } => CollectedChange::Edit {
                    row: node.row.clone(),
                    old: old.row.clone(),
                },
                Change::Child { .. } => return, // not exercised by source-level tests
            };
            c.changes.borrow_mut().push(collected.clone());
            (collected, c.input, c.fetch_on_push.get())
        };
        let _ = collected;
        if fetch_on_push {
            // Reentrant fetch of the source connection mid-push: observes the
            // epoch-gated overlay (this connection's epoch was bumped before its
            // own push, so it DOES see the in-flight change).
            let rows: Vec<Row> = self
                .fetch(input, &FetchRequest::all())
                .map(|n| n.row)
                .collect();
            self.collector(id).fetched.borrow_mut().push(rows);
        }
    }

    /// Enable reentrant fetch-during-push capture on a collector.
    pub fn set_collector_fetch_on_push(&self, id: NodeId, on: bool) {
        self.collector(id).fetch_on_push.set(on);
    }

    /// The changes a collector has received, in order.
    pub fn collector_changes(&self, id: NodeId) -> Vec<CollectedChange> {
        self.collector(id).changes.borrow().clone()
    }

    /// The row-sets captured by reentrant fetch-during-push (one per change).
    pub fn collector_fetched(&self, id: NodeId) -> Vec<Vec<Row>> {
        self.collector(id).fetched.borrow().clone()
    }

    // --- production change-stream sink (the `rindle-replica` seam) ---

    /// Drain (take) the change events a change-sink (see [`add_change_sink`](Self::add_change_sink))
    /// has accumulated since the last call, in arrival order. The buffer is left empty,
    /// so this is the per-transaction delta after a push batch + `flush`.
    pub fn take_sink_changes(&self, sink: NodeId) -> Vec<crate::changes::CaughtChange> {
        std::mem::take(&mut *self.collector(sink).caught.borrow_mut())
    }

    /// Materialize the pipeline feeding a change-sink as the initial set of
    /// [`CaughtChange::Add`](crate::changes::CaughtChange) events — the hydration
    /// snapshot (one `Add` per top-level node, relationships drained). Does not touch
    /// the push buffer. Mirrors `Catch.fetch` (`testkit.rs`) without the testkit gate.
    ///
    /// Fallible like [`try_hydrate`](Self::try_hydrate), and for the same reason: the
    /// cold drain is a `try_*` boundary. A leaf error parked mid-fetch (an unsafe
    /// integer, a failed `sqlite3_step`) ended the stream early, and folding reduce
    /// groups during the drain can arm the §5.3 integer-sum overflow state (design
    /// 226) — an already-out-of-range set total must surface HERE as the typed error,
    /// not register as a success whose aggregate cell is `NULL` while every later
    /// write on the shared graph fails the overflow check. Callers tear the partial
    /// pipeline down on `Err` (the registration error path), which also clears the
    /// overflow state the drain armed.
    pub fn try_hydrate_change_sink(
        &self,
        sink: NodeId,
    ) -> Result<Vec<crate::changes::CaughtChange>, RindleError> {
        let input = self.collector(sink).input;
        let initial: Vec<crate::changes::CaughtChange> = self
            .fetch(input, &FetchRequest::all())
            .map(|n| crate::changes::CaughtChange::Add(crate::changes::expand_node(&n)))
            .collect();
        self.take_runtime_error()?;
        Ok(initial)
    }

    // --- spec 11 testkit: Catch / Snitch readback ---

    /// `Catch.fetch` (`catch.ts:64-66`): materialize the pipeline feeding the
    /// catch into a comparable [`CaughtNode`](crate::testkit::CaughtNode) tree,
    /// eagerly draining every relationship thunk. Does not record into `pushes`.
    #[cfg(any(test, feature = "testkit"))]
    pub fn catch_fetch(
        &self,
        catch: NodeId,
        req: &FetchRequest,
    ) -> Vec<crate::testkit::CaughtNode> {
        let input = self.catch_op(catch).input;
        self.fetch(input, req)
            .map(|n| crate::testkit::expand_node(&n))
            .collect()
    }

    /// The downstream change stream a [`Catch`](crate::testkit::Catch) recorded.
    #[cfg(any(test, feature = "testkit"))]
    pub fn catch_pushes(&self, catch: NodeId) -> Vec<crate::testkit::CaughtChange> {
        self.catch_op(catch).pushes()
    }

    /// The `(change, fetch)` pairs recorded when the catch has `fetch_on_push` set.
    #[cfg(any(test, feature = "testkit"))]
    pub fn catch_pushes_with_fetch(
        &self,
        catch: NodeId,
    ) -> Vec<(
        crate::testkit::CaughtChange,
        Vec<crate::testkit::CaughtNode>,
    )> {
        self.catch_op(catch).pushes_with_fetch()
    }

    /// The message log a [`Snitch`](crate::testkit::Snitch) recorded, in order.
    #[cfg(any(test, feature = "testkit"))]
    pub fn snitch_log(&self, snitch: NodeId) -> Vec<crate::testkit::SnitchMessage> {
        self.snitch_op(snitch).log()
    }

    /// Clear every `Snitch`'s log (the `clearLog` of `fetch-and-push-tests.ts:81`
    /// — the runner calls this after hydrate so the log reflects only the pushes).
    #[cfg(any(test, feature = "testkit"))]
    pub fn clear_all_snitch_logs(&self) {
        for op in &self.nodes {
            if let Operator::Snitch(s) = op {
                s.clear_log();
            }
        }
    }

    /// Test/proof entry: inject a change at `id` (e.g. a `FilterStart`) exactly as
    /// a `SourceConn`'s output edge would deliver it. Drives the filter sub-graph
    /// in isolation; the full source push path is covered by the source tests.
    pub fn push_at<'g>(&'g self, id: NodeId, change: Change<'g>, port: Port) {
        self.push(id, change, port);
    }

    /// `(begin_filter, filter, end_filter)` call counts recorded by a
    /// `FilterProbe` — lets a test assert the lifecycle fired, and balanced.
    pub fn probe_counts(&self, id: NodeId) -> (usize, usize, usize) {
        let p = self.filter_probe_op(id);
        (p.begin_count.get(), p.filter_count.get(), p.end_count.get())
    }

    /// The view's current top-level result snapshot (the `data` getter). A cheap
    /// `Arc` clone of the materialized tree; the testkit's
    /// [`view_data_to_caught`](crate::testkit::view_data_to_caught) converts it to a
    /// comparable [`CaughtNode`](crate::testkit::CaughtNode) tree.
    pub fn view_data(&self, view_id: NodeId) -> crate::view::ViewData {
        self.view(view_id).data()
    }

    /// Test/inspection helper: `[(pk_id, [child_pk_id, ...]), ...]` using column 0.
    /// Children are flattened across all relationship slots in slot order (the
    /// production [`View`](crate::view::View)'s `dump_col0`).
    pub fn dump_view(&self, view_id: NodeId) -> Vec<(i64, Vec<i64>)> {
        self.view(view_id).dump_col0()
    }

    /// Recursive col-0 dump (the deep counterpart of [`Graph::dump_view`]): surfaces
    /// grandchildren so a nested relationship (`issue{comments{reactions}}`) is fully
    /// observable. See [`crate::view::View::dump_col0_deep`].
    pub fn dump_view_deep(&self, view_id: NodeId) -> Vec<crate::view::Col0Node> {
        self.view(view_id).dump_col0_deep()
    }

    /// Test helper: each top row's FULL columns (as `i64`) + its children's full
    /// columns. Unlike [`Graph::dump_view`] (column-0 ids only) this surfaces
    /// edited non-key columns, so an edit's *value* change is observable. Assumes
    /// all columns are `Int`.
    pub fn dump_view_rows(&self, view_id: NodeId) -> Vec<(Vec<i64>, Vec<Vec<i64>>)> {
        self.view(view_id).dump_rows()
    }
}

// ---------------------------------------------------------------------------
// Join overlay splice (node-level)
// ---------------------------------------------------------------------------

/// Splice the by-value join overlay into a parent's (committed) child stream
/// (Primitive #5). The spike's view re-materializes each parent's children, so the
/// overlay **adds** the in-progress child (`Add` / the new row of an `Edit`) and
/// **removes** the gone child (`Remove` / the old row of an `Edit`) — the opposite
/// polarity from `join.ts`'s suppress-and-deliver model, which is co-designed with
/// the production `ArrayView` (see [`JoinOverlay`](crate::change::JoinOverlay)).
/// Named distinctly from [`crate::source_common`]'s row-level
/// `generate_with_overlay` (the node-vs-row collision the audit flagged).
///
/// Collect + re-sort (rather than a streaming splice) is fine: child relationship
/// streams are small, and the output is identical to an in-order insert.
fn splice_join_overlay<'g>(
    base: NodeStream<'g>,
    overlay: &JoinOverlay,
    parent_row: &Row,
    parent_key: &[ColId],
    child_key: &[ColId],
    child_pk: &[ColId],
    child_sort: &Sort,
) -> NodeStream<'g> {
    let mut nodes: Vec<Node<'g>> = base.collect();

    // Add `add_row` iff it joins to this parent and isn't already present.
    let add_if_matches = |nodes: &mut Vec<Node<'g>>, add_row: &Row| {
        if is_join_match(parent_row, parent_key, add_row, child_key)
            && !nodes.iter().any(|n| same_pk(&n.row, add_row, child_pk))
        {
            nodes.push(Node::leaf(add_row.clone()));
        }
    };

    match &overlay.change {
        SourceChange::Add(row) => add_if_matches(&mut nodes, row),
        SourceChange::Remove(row) => nodes.retain(|n| !same_pk(&n.row, row, child_pk)),
        SourceChange::Edit { row, old } => {
            nodes.retain(|n| !same_pk(&n.row, old, child_pk));
            add_if_matches(&mut nodes, row);
        }
    }

    nodes.sort_by(|a, b| compare_rows(child_sort, &a.row, &b.row));
    Box::new(nodes.into_iter())
}

/// Splice the LIVE in-flight child overlay into a parent's child stream with Zero's
/// `join-utils.generateWithOverlay` **pull-to-pre** polarity — the production model
/// (`change.rs` [`JoinOverlay`] doc), used by the live-field Join path
/// ([`Graph::process_parent_node`]). The base child stream is already POST-change
/// (the cap commits its state synchronously; the source overlay is epoch-gated to
/// the post state), so this pulls it back to the PRE-change view a not-yet-processed
/// parent must see during a mid-push refetch: a **Remove** re-adds the gone child, an
/// **Add** suppresses the just-added child, an **Edit** re-adds `old` and suppresses
/// the new row. Identity is by PK (the sort always includes the full PK, so PK
/// equality matches Zero's `compareRows == 0`); the result is re-sorted so a
/// materialized relationship stays ordered (for an unordered EXISTS child only the
/// count matters). This is the EXACT OPPOSITE polarity of [`splice_join_overlay`]
/// (the spike push-to-post model, retained for [`FlippedJoin`](crate::op::FlippedJoin)).
fn splice_join_overlay_pre<'g>(
    base: NodeStream<'g>,
    overlay: &JoinOverlay,
    child_pk: &[ColId],
    child_sort: &Sort,
) -> NodeStream<'g> {
    let mut nodes: Vec<Node<'g>> = base.collect();

    // Re-add the in-flight gone/old child (it is absent from the post-change base),
    // unless a row with its PK is somehow still present (no double-add).
    let readd_if_absent = |nodes: &mut Vec<Node<'g>>, row: &Row| {
        if !nodes.iter().any(|n| same_pk(&n.row, row, child_pk)) {
            nodes.push(Node::leaf(row.clone()));
        }
    };
    // Suppress the in-flight added/new child (present in the post-change base).
    let suppress = |nodes: &mut Vec<Node<'g>>, row: &Row| {
        if let Some(i) = nodes.iter().position(|n| same_pk(&n.row, row, child_pk)) {
            nodes.remove(i);
        }
    };

    match &overlay.change {
        SourceChange::Add(row) => suppress(&mut nodes, row),
        SourceChange::Remove(row) => readd_if_absent(&mut nodes, row),
        SourceChange::Edit { row, old } => {
            suppress(&mut nodes, row);
            readd_if_absent(&mut nodes, old);
        }
    }

    nodes.sort_by(|a, b| compare_rows(child_sort, &a.row, &b.row));
    Box::new(nodes.into_iter())
}

// ---------------------------------------------------------------------------
// Filter sub-graph free helpers
// ---------------------------------------------------------------------------

/// RAII for the Filter sub-graph fetch lifecycle. Holds a *shared* graph borrow
/// and the chain head; its [`Drop`] runs `end_filter(chain)`. It is moved into
/// the fetch stream's closure, so it fires exactly when the stream is dropped —
/// early break, `?`-propagation, or early return — reproducing the JS
/// `try/finally` (`filter-operators.ts:98-102`; Primitive #2 / `07` §6.4). A
/// panic unwinding through it also fires it, but only in unwinding builds; under
/// the shipping `panic = "abort"` client profile a panic aborts and `Drop` is
/// skipped. See WS02.
struct EndFilterGuard<'g> {
    g: &'g Graph,
    chain: NodeId,
}

impl Drop for EndFilterGuard<'_> {
    fn drop(&mut self) {
        self.g.end_filter(self.chain);
    }
}

/// RAII for a [`Join`]'s in-flight child-change fan-out (`join.ts:222-249`
/// `try/finally`): clears [`Join::inprogress_overlay`]/[`Join::inprogress_position`]
/// when [`Graph::push_child_change`] returns — normally, via `?`, or unwinding (the
/// latter only under `panic = "unwind"`; under the shipping `panic = "abort"` profile
/// a panic aborts and `Drop` is skipped — see WS02). Leaving a stale overlay set
/// would corrupt every later fetch's membership, so the clear is not optional.
struct InprogressGuard<'g> {
    g: &'g Graph,
    jid: NodeId,
}

impl Drop for InprogressGuard<'_> {
    fn drop(&mut self) {
        let j = self.g.join(self.jid);
        *j.inprogress_overlay.borrow_mut() = None;
        *j.inprogress_position.borrow_mut() = None;
    }
}

/// `push_accumulated_changes` (`push-accumulated.ts:87`), the minimal dedup core.
/// Each branch of an OR forwarded 0..1 change; collapse the lot into **exactly
/// one** output change, keyed by the FAN-OUT's original change type. Dedup uses
/// the `Identity` relationship merge (`fan-in.ts:91`): keep the FIRST change of
/// each type and drop the later ones (the JS `identity(existing, _) = existing`).
///
/// The subtle case is `Edit`: a filter that *included* the old row but *excludes*
/// the new turns the edit into a `Remove(old)`; a sibling filter that did the
/// reverse turns it into an `Add(new)`. When both survive, they **reconstruct**
/// the `Edit{node: new, old}` (`push-accumulated.ts:202-213`) — otherwise the
/// view would see a bare Add or Remove and break its "remove-only-present /
/// add-only-absent" invariant.
fn collapse_accumulated<'g>(acc: Vec<Change<'g>>, fan_out_type: ChangeType) -> Vec<Change<'g>> {
    let mut add: Option<Node<'g>> = None;
    let mut remove: Option<Node<'g>> = None;
    let mut edit: Option<Change<'g>> = None;
    let mut child: Option<Change<'g>> = None;
    for c in acc {
        match c {
            Change::Add(n) => {
                add.get_or_insert(n);
            }
            Change::Remove(n) => {
                remove.get_or_insert(n);
            }
            Change::Edit { .. } => {
                edit.get_or_insert(c);
            }
            // A branch preserved the Child (a leaf Filter passes it through). Keep
            // the FIRST (the `Identity` merge — `fan-in.ts` passes `identity`).
            Change::Child { .. } => {
                child.get_or_insert(c);
            }
        }
    }
    match fan_out_type {
        // An Add entering the fan can only stay an Add or be dropped per branch —
        // a stateless Filter never manufactures a Remove/Edit from an Add.
        ChangeType::Add => {
            debug_assert!(
                remove.is_none() && edit.is_none() && child.is_none(),
                "Add fan-out collapsed a non-Add branch change"
            );
            add.map(Change::Add).into_iter().collect()
        }
        ChangeType::Remove => {
            debug_assert!(
                add.is_none() && edit.is_none() && child.is_none(),
                "Remove fan-out collapsed a non-Remove branch change"
            );
            remove.map(Change::Remove).into_iter().collect()
        }
        ChangeType::Edit => {
            debug_assert!(
                child.is_none(),
                "Edit fan-out produced a Child branch change"
            );
            if let Some(e) = edit {
                // An Edit survived a branch unsplit → it supersedes (the minimal
                // chassis has no relationships to merge in).
                vec![e]
            } else {
                match (add, remove) {
                    (Some(node), Some(old)) => vec![Change::Edit { node, old }], // reconstruct
                    (Some(node), None) => vec![Change::Add(node)],
                    (None, Some(old)) => vec![Change::Remove(old)],
                    (None, None) => Vec::new(),
                }
            }
        }
        // CHILD precedence (`push-accumulated.ts` CHILD case): a Child entering the
        // fan reaches each branch, which either **preserves** it (a leaf Filter) or
        // **converts** it to an Add/Remove (an `Exists` gate, when the relationship
        // change flips the parent's membership 0↔1). If any branch kept the Child,
        // it takes precedence over all else. Otherwise exactly one of Add/Remove
        // survives (the relationship is unique to one exists check, so the converters
        // can't disagree). `edit` is impossible here.
        ChangeType::Child => {
            debug_assert!(
                edit.is_none(),
                "Child fan-out produced an Edit branch change"
            );
            if let Some(c) = child {
                vec![c]
            } else {
                debug_assert!(
                    !(add.is_some() && remove.is_some()),
                    "Child fan-out: expected at most one of add/remove"
                );
                match (add, remove) {
                    (Some(node), _) => vec![Change::Add(node)],
                    (None, Some(old)) => vec![Change::Remove(old)],
                    (None, None) => Vec::new(),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::change::{Basis, Start};
    use crate::op::Skip;
    use crate::storage::StorageValue;
    use crate::value::{owned_row, OwnedValue};

    /// The operator **fan-out seam** (`op/mod.rs`): an operator whose struct +
    /// logic live in their own file (`op/skip.rs`) integrates into the arena and is
    /// reached by the graph's dispatch. We assert the *wiring* — the variant, the
    /// `add_skip` builder, `set_output`, and that `input_schema` resolves THROUGH
    /// the new variant to the source — without invoking the (stub) `fetch`/`push`.
    #[test]
    fn skip_operator_wires_into_arena() {
        let mut g = Graph::new();
        let src = g.add_source(
            SourceSchema::new(vec!["id"], vec![0], vec![(0, true)]),
            Vec::new(),
        );
        let conn = g.connect(src, Some(vec![(0, true)]), None, Vec::new());
        let skip = g.add_skip(Skip::new(
            conn,
            Start {
                row: owned_row(vec![OwnedValue::Int(0)]),
                basis: Basis::After,
            },
        ));
        let coll = g.add_collector(skip);
        g.set_output(skip, coll);

        // Dispatch reaches the out-of-file operator: schema resolves through it.
        assert_eq!(self_columns(&g, skip), vec!["id"]);
    }

    fn self_columns(g: &Graph, id: NodeId) -> Vec<String> {
        g.input_schema(id)
            .columns
            .iter()
            .map(|c| c.to_string())
            .collect()
    }

    /// [`Graph::remove_source`] (the inverse of `add_source`, for a synthetic aggregate table
    /// whose last query is gone): it must REFUSE while a reader is wired, SUCCEED once the
    /// pipeline is torn down, and recycle the freed slot (generation bumped). The
    /// optimistic/normalized backend leans on the refusal to keep `unregisterTable` safe.
    #[test]
    fn remove_source_refuses_a_live_reader_then_frees_the_slot() {
        let mut g = Graph::new();
        let src = g.add_source(
            SourceSchema::new(vec!["id"], vec![0], vec![(0, true)]),
            Vec::new(),
        );

        // A live reader: connect + wire its downstream. remove_source must refuse.
        g.begin_recording();
        let conn = g.connect(src, Some(vec![(0, true)]), None, Vec::new());
        let coll = g.add_collector(conn);
        g.set_sink_edge(conn, coll);
        let manifest = g.take_recording();
        assert!(
            g.remove_source(src).is_err(),
            "a source with a live reader must not be removed"
        );

        // Tear the reader down → 0 live connections → remove_source succeeds.
        g.destroy_pipeline(&manifest);
        assert!(g.remove_source(src).is_ok());

        // The freed source slot is recycled with a bumped generation (so a stale id is distinct).
        let src2 = g.add_source(
            SourceSchema::new(vec!["id"], vec![0], vec![(0, true)]),
            Vec::new(),
        );
        assert_eq!(src2.ix(), src.ix(), "freed source slot is reused");
        assert_ne!(src2.gen, src.gen, "generation bumped on free");

        // A non-source node is rejected (not silently freed).
        let conn2 = g.connect(src2, Some(vec![(0, true)]), None, Vec::new());
        assert!(g.remove_source(conn2).is_err());
    }

    // --- collapse_accumulated: the CHILD-precedence arm (spec-06 push-accumulated) ---
    //
    // Directly exercises the collapse for a `Child` fan-out — including the
    // `Exists`-branch conversion (Child → Add/Remove on a membership flip), which a
    // hand-wired leaf-only fan can't produce.

    fn leaf_node(id: i64) -> Node<'static> {
        Node::leaf(owned_row(vec![OwnedValue::Int(id)]))
    }
    fn child_chg(parent: i64) -> Change<'static> {
        Change::Child {
            node: leaf_node(parent),
            rel: crate::value::RelId(0),
            child: Box::new(Change::Add(leaf_node(parent * 10))),
        }
    }
    fn col0(c: &Change) -> i64 {
        let row = match c {
            Change::Add(n) | Change::Remove(n) => &n.row,
            Change::Edit { node, .. } | Change::Child { node, .. } => &node.row,
        };
        match row.col(0) {
            crate::value::Value::Int(i) => i,
            o => panic!("{o:?}"),
        }
    }

    #[test]
    fn collapse_child_precedence_keeps_the_child() {
        // One branch preserved the Child; another (an Exists gate) converted it to an
        // Add. The preserved Child takes precedence — the Add is dropped.
        let out = collapse_accumulated(
            vec![child_chg(1), Change::Add(leaf_node(1))],
            ChangeType::Child,
        );
        assert_eq!(out.len(), 1);
        assert!(matches!(out[0], Change::Child { .. }));
        assert_eq!(col0(&out[0]), 1);
    }

    #[test]
    fn collapse_two_preserved_children_dedup_to_one() {
        // Two branches preserved the same Child → keep-first dedups to one.
        let out = collapse_accumulated(vec![child_chg(1), child_chg(1)], ChangeType::Child);
        assert_eq!(out.len(), 1);
        assert!(matches!(out[0], Change::Child { .. }));
    }

    #[test]
    fn collapse_child_to_add_on_membership_flip() {
        // No branch preserved the Child; an Exists gate flipped it to an Add (the
        // parent gained its first matching child).
        let out = collapse_accumulated(vec![Change::Add(leaf_node(7))], ChangeType::Child);
        assert_eq!(out.len(), 1);
        assert!(matches!(out[0], Change::Add(_)));
        assert_eq!(col0(&out[0]), 7);
    }

    #[test]
    fn collapse_child_to_remove_on_membership_flip() {
        let out = collapse_accumulated(vec![Change::Remove(leaf_node(7))], ChangeType::Child);
        assert_eq!(out.len(), 1);
        assert!(matches!(out[0], Change::Remove(_)));
        assert_eq!(col0(&out[0]), 7);
    }

    #[test]
    fn collapse_child_dropped_when_no_branch_passes() {
        assert!(collapse_accumulated(Vec::new(), ChangeType::Child).is_empty());
    }

    /// The storage arena (foundations §6.4): each `alloc_storage` hands back a
    /// fresh, disjoint keyspace, and an operator reads/writes it through a *shared*
    /// `&Graph` borrow (the same borrow shape as a reentrant push).
    #[test]
    fn storage_arena_allocates_disjoint_namespaces() {
        let mut g = Graph::new();
        let a = g.alloc_storage();
        let b = g.alloc_storage();
        assert_ne!(a, b, "each slot is a distinct id");

        // Same key in two slots → two independent values (disjoint namespaces).
        g.storage(a).set(
            "k",
            StorageValue::Take {
                size: 1,
                bound: None,
            },
        );
        g.storage(b).set(
            "k",
            StorageValue::Take {
                size: 2,
                bound: None,
            },
        );
        let read_size = |id| match g.storage(id).get("k").unwrap() {
            StorageValue::Take { size, .. } => size,
            other => panic!("unexpected {other:?}"),
        };
        assert_eq!(read_size(a), 1);
        assert_eq!(read_size(b), 2);
        assert!(g.storage(a).get("absent").is_none());
    }

    fn id_schema() -> SourceSchema {
        SourceSchema::new(vec!["id"], vec![0], vec![(0, true)])
    }

    /// Build a minimal recorded "pipeline" over `src`: a connection, a scratch-storage
    /// slot, and a collector sink, wired source→sink. Returns the manifest + the ids.
    fn build_recorded(g: &mut Graph, src: NodeId) -> (PipelineManifest, NodeId, StorageId, NodeId) {
        g.begin_recording();
        let conn = g.connect(src, Some(vec![(0, true)]), None, Vec::new());
        let store = g.alloc_storage();
        let sink = g.add_collector(conn);
        g.set_sink_edge(conn, sink);
        (g.take_recording(), conn, store, sink)
    }

    /// `destroy_pipeline` reclaims a torn-down pipeline's storage (clears it) and node
    /// slots (tombstones them) WITHOUT growing the arena, and a later build REUSES those
    /// freed slots — same index, bumped generation, empty store.
    #[test]
    fn destroy_pipeline_reclaims_and_reuses_slots() {
        let mut g = Graph::new();
        let src = g.add_source(id_schema(), Vec::new()); // shared, NOT recorded

        let (m, _conn, store, _sink) = build_recorded(&mut g, src);
        // Put state in the operator-storage slot so we can prove it is cleared.
        g.storage(store).set(
            "k",
            StorageValue::Take {
                size: 9,
                bound: None,
            },
        );
        assert!(g.storage(store).get("k").is_some());

        let nodes = g.node_count();
        let stores = g.storage_count();

        g.destroy_pipeline(&m);
        // Tombstoned, not removed: the arena does not shrink.
        assert_eq!(g.node_count(), nodes);
        assert_eq!(g.storage_count(), stores);

        // A fresh build reuses the freed slots — no arena growth.
        let (_m2, conn2, store2, sink2) = build_recorded(&mut g, src);
        assert_eq!(g.node_count(), nodes, "node slots were recycled, not grown");
        assert_eq!(g.storage_count(), stores, "storage slot was recycled");

        // The reused storage slot: same index, bumped generation, and EMPTY (cleared).
        assert_eq!(store2.idx, store.idx);
        assert!(store2.gen > store.gen);
        assert!(
            g.storage(store2).get("k").is_none(),
            "a recycled storage slot must start empty"
        );

        // The reused node slots are the freed indices with bumped generations.
        let freed: std::collections::HashSet<u32> = m.nodes().iter().map(|n| n.idx).collect();
        assert!(freed.contains(&conn2.idx) && conn2.gen > 0);
        assert!(freed.contains(&sink2.idx) && sink2.gen > 0);

        // The SHARED source's connection slot was recycled too: the destroy+rebuild did
        // not grow its connection table (the conn-slot leak fix). One pipeline = one
        // connection, so the table holds exactly one slot before and after.
        let conn_slots = match &g.nodes[src.ix()] {
            Operator::Source(SourceLeaf::Memory(ms)) => ms.conn_count(),
            _ => unreachable!(),
        };
        assert_eq!(
            conn_slots, 1,
            "the source connection slot was recycled, not leaked"
        );
    }

    /// After teardown, a stale `NodeId` (the pre-free generation) is a loud fail-fast on
    /// any arena access — never a silent alias of whatever recycles the slot.
    #[test]
    #[should_panic(expected = "stale NodeId")]
    fn stale_node_id_after_teardown_fails_fast() {
        let mut g = Graph::new();
        let src = g.add_source(id_schema(), Vec::new());
        let (m, _conn, _store, sink) = build_recorded(&mut g, src);
        g.destroy_pipeline(&m);
        // `sink` carries the old generation; the slot's generation was bumped on free.
        let _ = g.input_schema(sink);
    }

    /// A stale `StorageId` likewise fails fast (the storage arena is generational too).
    #[test]
    #[should_panic(expected = "stale StorageId")]
    fn stale_storage_id_after_teardown_fails_fast() {
        let mut g = Graph::new();
        let src = g.add_source(id_schema(), Vec::new());
        let (m, _conn, store, _sink) = build_recorded(&mut g, src);
        g.destroy_pipeline(&m);
        let _ = g.storage(store).get("k");
    }
}
