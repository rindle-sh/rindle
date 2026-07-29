//! Backend-agnostic source machinery, shared by the memory (`BTree`) and SQLite
//! leaves. **This module references no `BTree`, no `rusqlite`, AND no `Node`** —
//! it is generic over `S: RowStream` and deals only in **rows** (`OwnedRow`) and
//! source changes (`SourceChange`). It is the seam the spec calls `source_common`
//! (`04` §4.6): the two leaves differ only in the concrete `S` they pass in.
//!
//! **A source emits rows, not nodes.** The overlay splice, the start/constraint/
//! filter chain, the k-way merge, and the push/edit-split orchestration here all
//! operate on rows. A row becomes a [`Node`](crate::change::Node) (row + lazily
//! attached relationships) only at the **connection boundary** — `Graph::fetch`
//! on a `SourceConn` wraps each emitted row in a leaf node on the read path, and
//! the `SourceConn`'s push driver wraps each `SourceChange` into a node-bearing
//! `Change` on the write path. Downstream **joins** then attach relationships. So
//! the Node concept lives entirely above this module.
//!
//! What lives here (faithful ports of `memory-source.ts`, which is the JS root
//! even though `05`/`TableSource` consume it verbatim):
//!
//! - the epoch-gated overlay splice — ordered ([`generate_with_overlay`]) and
//!   unordered ([`generate_with_overlay_unordered`]) — plus the pure overlay
//!   narrowers ([`compute_overlays`], [`overlays_for_constraint`], …);
//! - the post-scan row chain ([`generate_with_start`], [`generate_with_constraint`],
//!   [`generate_with_filter`]);
//! - the k-way [`merge_sorted_streams`] (binary min-heap, `Drop`-clean);
//! - the predicate-filtered push ([`filter_push`], edit-splitting by predicate);
//! - the eager push orchestration ([`gen_push_and_write_with_split_edit`]):
//!   edit-split detection, per-connection fan-out with the live overlay + epoch
//!   gate, and the deferred write-after-drain.
//!
//! The connection/overlay **types** ([`Connection`], [`ConnectionFilters`],
//! [`Overlay`], [`Overlays`], [`RowPredicate`]) live here too because their shape
//! is identical across backends (`04` §4.1, §4.2).

use std::cell::{Cell, Ref, RefCell};
use std::cmp::Ordering;
use std::marker::PhantomData;
use std::rc::Rc;

use crate::graph::ConnId;

use crate::change::{
    constraint_matches, Basis, Constraint, MultiConstraint, OutEdge, RowFlow, SourceChange, Start,
};
use crate::error::RindleError;
use crate::metric_add;
use crate::push_index::{PushGuard, PushIndex};
use crate::value::{
    compare_rows, same_pk, values_equal, ColId, OwnedRow as Row, OwnedValue, RowRef, RowStream,
    Sort,
};

// ---------------------------------------------------------------------------
// Shared connection / overlay types (identical shape across backends — §4.1/§4.2)
// ---------------------------------------------------------------------------

/// The in-memory predicate compiled from a connection's pushed-down filter
/// (`createPredicate`, owned by `07`). `Rc` so a fetch can clone it into the
/// returned lazy stream without holding a `RefCell` borrow on connection state
/// across the vend (the cardinal rule — foundations §6.2). The memory leaf reads
/// columns by [`ColId`] index; a bare `&Row` keeps this backend-agnostic.
pub type RowPredicate = Rc<dyn Fn(&Row) -> bool>;

/// The single in-flight source change, epoch-tagged. Rows are OWNED (overlays are
/// materialized — foundations §3.3). Mirrors `Overlay` (`memory-source.ts:59`);
/// IDENTICAL to `05`'s (it lives here).
#[derive(Clone, Debug)]
pub struct Overlay {
    pub epoch: u32,
    pub change: SourceChange,
}

/// The narrowed add/remove pair fed to the splice (`Overlays`,
/// `memory-source.ts:64`). Both owned.
#[derive(Clone, Default, Debug)]
pub struct Overlays {
    pub add: Option<Row>,
    pub remove: Option<Row>,
}

impl Overlays {
    /// No overlay rows survived narrowing — the splice would be an exact
    /// pass-through, so the stream builders skip it entirely.
    pub fn is_empty(&self) -> bool {
        self.add.is_none() && self.remove.is_none()
    }
}

/// A filter condition lowered to a **backend-neutral AST** — the input the SQLite
/// leaf's `build_select_query` walks to emit the SELECT `WHERE` (port of
/// query-builder.ts `NoSubqueryCondition`, subqueries already stripped). Pure
/// data, no `rusqlite`: the **memory** backend ignores it entirely (it filters via
/// [`RowPredicate`]); only the **SQLite** leaf lowers it to SQL text, leaving the
/// `predicate` to narrow only the (not-in-SQL) overlay rows. Column operands are
/// already-resolved [`ColId`]s; a literal operand's [`OwnedValue`] variant selects
/// its SQLite storage form (the JS `getJsType` → `toSQLiteType`).
#[derive(Clone, Debug)]
pub enum SqlCondition {
    Simple {
        left: Operand,
        op: SqlOp,
        right: Operand,
    },
    /// AND of zero-or-more conditions. Empty ⇒ the literal `TRUE`
    /// (query-builder.ts:174-181).
    And(Vec<SqlCondition>),
    /// OR of zero-or-more conditions. Empty ⇒ the literal `FALSE`
    /// (query-builder.ts:182-191).
    Or(Vec<SqlCondition>),
}

/// One side of a [`SqlCondition::Simple`]. A `Column` lowers to its quoted
/// identifier (resolved from the schema at build time); a `Literal` lowers to a
/// bound `?` param (query-builder.ts `valuePositionToSQL`).
#[derive(Clone, Debug)]
pub enum Operand {
    Column(ColId),
    Literal(OwnedValue),
}

/// Comparison / membership operators (query-builder.ts `simpleConditionToSQL`).
/// Most lower to their raw SQL spelling verbatim; `In`/`NotIn` lower to a
/// `json_each(?)` subquery over a JSON-array literal (query-builder.ts:196-205);
/// the `Like` family appends `ESCAPE '\'` and `Ilike` lowers both sides through
/// `lower(...)` (query-builder.ts `likeConditionToSQL`). `Is`/`IsNot` are the
/// NULL-aware equality operators in the JS `SimpleOperator` set (ast.ts:37): they
/// reach the `WHERE` verbatim (a user `.where(c, 'IS', null)`, and the engine's
/// `NOT EXISTS` → `field IS NOT literal` rewrite, resolve-scalar-subqueries.ts:176).
/// Distinct from the start-bound's nullable-aware `=`/`IS` choice (§3.7), which is
/// a SEPARATE code path keyed on column optionality, not a user operator.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SqlOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Is,
    IsNot,
    In,
    NotIn,
    Like,
    NotLike,
    Ilike,
    NotIlike,
}

/// The fully-pushed-down filter + the precomputed PK constraint. The memory leaf
/// only evaluates `predicate`; `pk_constraint` is hoisted from the condition at
/// connect time (the JS recomputes it per fetch via `primaryKeyConstraintFrom
/// Filters` — we hoist since the condition is fixed per connection). `fully_
/// applied` mirrors `SourceInput.fullyAppliedFilters` (`memory-source.ts:180`).
pub struct ConnectionFilters {
    pub predicate: RowPredicate,
    pub pk_constraint: Option<Constraint>,
    pub fully_applied: bool,
    /// SQLite-only: the filter lowered to a SQL `WHERE` condition. The memory
    /// backend leaves this `None` (it filters in-memory via `predicate`). When
    /// present, the SQLite leaf emits it into the `SELECT` so committed rows are
    /// SQL-filtered, and `predicate` narrows only the overlay (which SQL never
    /// sees) — faithful to the JS `connection.filters` carrying BOTH `condition`
    /// and `predicate` (`table-source.ts:256-261`).
    pub sql_condition: Option<SqlCondition>,
    /// Equality guard implied by `predicate` — feeds the [`ConnTable`] push index
    /// (`designs/205-GUARDED-PUSH-FANOUT-DESIGN.md`). Populated for **both** backends
    /// (unlike `sql_condition`), since the fan-out pruning it drives is backend-neutral.
    /// `None` ⇒ the connection is unconditionally visited on every push (today's
    /// behavior); `Some(g)` ⇒ visited only when a changed row's `g.col` cell hits one
    /// of `g.values`. The exact `predicate` still runs on every candidate, so the guard
    /// need only never *under*-approximate.
    pub push_guard: Option<PushGuard>,
}

/// One connection (one downstream output). Self-joins make two. Mirrors
/// `Connection` (`memory-source.ts:75`). IDENTICAL shape to `05`'s so this module
/// stays backend-agnostic. The mutable bits are `Cell` so they bump during a push
/// without a `&mut` borrow held across a vend (foundations §6.2).
pub struct Connection {
    /// Resolved order; always the internal sort (`sort ?? primaryIndexSort`).
    pub sort: Sort,
    /// Schema reports `sort: None` when true; internally `sort` is the primary
    /// index sort, used for overlay PK matching in the unordered path.
    pub unordered: bool,
    pub split_edit_keys: Vec<ColId>,
    pub filters: Option<ConnectionFilters>,
    /// Gates overlay visibility (the self-join epoch gate, §3.5).
    pub last_pushed_epoch: Cell<u32>,
    pub output: Cell<Option<OutEdge>>,
}

/// A source's connection registry: the [`Connection`]s plus a **generational
/// free-list**, so a torn-down connection's slot is RECYCLED rather than leaked under
/// query churn (the source-side analogue of the graph arena's node/storage reuse).
/// Shared by both [`Source`](crate::graph::Source) backends (the memory `BTree` leaf
/// and the SQLite `TableSource`) so the reuse logic — and its stale-`ConnId`
/// fail-fast — lives in exactly one place and the two backends cannot drift apart.
///
/// The push fan-out iterates [`borrow`](Self::borrow) directly (no `ConnId`), so it
/// is unaffected by the generation; a slot freed but not yet reused has
/// `output == None` and is skipped by the fan-out exactly as before.
#[derive(Default)]
pub struct ConnTable {
    conns: RefCell<Vec<Connection>>,
    /// Per-slot generation, bumped when the slot is freed; checked on every
    /// `ConnId`-addressed access so a stale handle to a recycled slot fail-fasts.
    gens: RefCell<Vec<u32>>,
    /// Freed slot indices available for reuse (LIFO).
    free: RefCell<Vec<u32>>,
    /// Reverse predicate index over the slots — prunes the push fan-out to the
    /// connections a change could match (`designs/205-GUARDED-PUSH-FANOUT-DESIGN.md`).
    /// A separate `RefCell` mutated only in [`connect`](Self::connect) /
    /// [`destroy`](Self::destroy); the push path takes a short immutable borrow to
    /// compute candidates *before* the drain begins, so it inherits the same
    /// no-reentrant-connect/destroy-during-push regime as `conns`.
    ///
    /// **Boxed** so the index's BTree headers live off the inline `ConnTable` — that
    /// keeps `SourceLeaf` (which holds a `ConnTable` by value) small enough to avoid
    /// bloating the arena `Operator` enum, and the box is touched only off the hot
    /// fetch path (connect/destroy and the once-per-push candidate lookup).
    index: Box<RefCell<PushIndex>>,
}

impl ConnTable {
    pub fn new() -> ConnTable {
        ConnTable::default()
    }

    /// Register `conn`, reusing a freed slot if one is available — its generation was
    /// bumped at free time, so the returned [`ConnId`] is distinct from the slot's prior
    /// tenant; otherwise grow the table with a fresh generation-0 slot.
    pub fn connect(&self, conn: Connection) -> ConnId {
        // Register the slot in the push index before moving `conn` into the vector.
        let index_slot = |table: &ConnTable, slot: u32, conn: &Connection| {
            let guard = conn.filters.as_ref().and_then(|f| f.push_guard.as_ref());
            table
                .index
                .borrow_mut()
                .insert(slot, guard, &conn.split_edit_keys);
        };
        if let Some(idx) = self.free.borrow_mut().pop() {
            let i = idx as usize;
            index_slot(self, idx, &conn);
            self.conns.borrow_mut()[i] = conn;
            ConnId::new(idx, self.gens.borrow()[i])
        } else {
            let idx = self.conns.borrow().len() as u32;
            index_slot(self, idx, &conn);
            self.conns.borrow_mut().push(conn);
            self.gens.borrow_mut().push(0);
            ConnId::new(idx, 0)
        }
    }

    /// Disconnect a connection — null its output edge so the push fan-out skips it —
    /// and free its slot for reuse (bump generation + add to the free-list). Idempotent
    /// and stale-safe: a non-matching generation (a double teardown / already-freed
    /// slot) is a no-op, never a panic, since teardown must be forgiving.
    pub fn destroy(&self, conn: ConnId) {
        let i = conn.idx as usize;
        let mut gens = self.gens.borrow_mut();
        if gens.get(i).copied() != Some(conn.gen) {
            return;
        }
        // Un-index before nulling the output — the connection is still in its slot,
        // so its own `push_guard` says exactly which buckets to remove it from. The
        // generation-match guard above keeps a double-destroy a no-op for the index too.
        if let Some(c) = self.conns.borrow().get(i) {
            let guard = c.filters.as_ref().and_then(|f| f.push_guard.as_ref());
            self.index
                .borrow_mut()
                .remove(conn.idx, guard, &c.split_edit_keys);
            c.output.set(None);
        }
        gens[i] = gens[i].wrapping_add(1);
        self.free.borrow_mut().push(i as u32);
    }

    /// The slot index for `conn`, fail-fasting on a stale generation (the connection
    /// analogue of [`Graph::node`](crate::graph::Graph)/`storage`). Release-checked.
    pub fn live_ix(&self, conn: ConnId) -> usize {
        let i = conn.idx as usize;
        let cur = self.gens.borrow().get(i).copied();
        assert!(
            cur == Some(conn.gen),
            "stale ConnId {conn:?} (slot generation is {cur:?})"
        );
        i
    }

    /// Set a connection's downstream output edge (gen-checked).
    pub fn set_output(&self, conn: ConnId, edge: OutEdge) {
        let i = self.live_ix(conn);
        self.conns.borrow()[i].output.set(Some(edge));
    }

    /// A connection's effective sort (gen-checked).
    pub fn sort(&self, conn: ConnId) -> Sort {
        let i = self.live_ix(conn);
        self.conns.borrow()[i].sort.clone()
    }

    /// Borrow the connection vector — for the push fan-out and per-connection reads
    /// (callers that already hold a [`live_ix`](Self::live_ix)).
    pub fn borrow(&self) -> Ref<'_, Vec<Connection>> {
        self.conns.borrow()
    }

    /// Total connection slots, **including freed (recyclable) ones**. With slot reuse
    /// this stays bounded by the peak live-connection count across a source's life — it
    /// does NOT grow per teardown. (The push fan-out's per-change cost is proportional
    /// to this, so keeping it bounded is the point of the free-list.)
    pub fn len(&self) -> usize {
        self.conns.borrow().len()
    }

    /// Whether the table has no slots yet (clippy's `len`-without-`is_empty` companion).
    pub fn is_empty(&self) -> bool {
        self.conns.borrow().is_empty()
    }

    /// Number of connections with a live downstream edge (current readers). A destroyed slot
    /// has its `output` nulled ([`destroy`](Self::destroy)), so this counts only live
    /// pipelines — used by [`Graph::remove_source`](crate::graph::Graph) to refuse removing a
    /// source a query still reads.
    pub fn live_conn_count(&self) -> usize {
        self.conns
            .borrow()
            .iter()
            .filter(|c| c.output.get().is_some())
            .count()
    }

    /// The slots that may satisfy `change` — a **superset** by construction
    /// (`designs/205-GUARDED-PUSH-FANOUT-DESIGN.md` §safety). Sorted ascending and
    /// deduped, so the push fan-out visits in slot order exactly as the full-scan
    /// loop did. Takes a short immutable borrow of the index that is released before
    /// it returns, so the drain can hold the `conns` borrow without index contention.
    pub fn push_candidates(&self, change: &SourceChange) -> Vec<u32> {
        self.index.borrow().push_candidates(change)
    }

    /// The distinct split-edit keys across all connections (refcounted union). Drives
    /// the [`gen_push_and_write_with_split_edit`] trigger check at O(distinct keys)
    /// instead of O(N·keys).
    pub fn split_edit_keys(&self) -> Vec<ColId> {
        self.index.borrow().split_keys().collect()
    }

    /// `true` iff some connection lists a split-edit key — lets the edit-split path
    /// skip the value-comparison check entirely when no connection needs it.
    pub fn has_split_edit_keys(&self) -> bool {
        self.index.borrow().has_split_keys()
    }
}

// ---------------------------------------------------------------------------
// The one reverse-aware comparator convention (§4.6)
// ---------------------------------------------------------------------------

/// `reverse=false` ⇒ `compare_rows(sort, a, b)`; `reverse=true` ⇒ its negation.
/// Defined ONCE so the cursor seek direction, the overlay-add splice position,
/// the remove-suppress equality, the start gate, and the k-way merge all agree.
#[inline]
pub fn compare_rows_rev(sort: &Sort, reverse: bool, a: &Row, b: &Row) -> Ordering {
    let c = compare_rows(sort, a, b);
    if reverse {
        c.reverse()
    } else {
        c
    }
}

// ---------------------------------------------------------------------------
// Leaf cursor -> owned-row adaptor (the single forced own at the dyn boundary)
// ---------------------------------------------------------------------------

/// Adapts a lending [`RowStream`] leaf into an owning `Iterator<Item = OwnedRow>`.
/// Each row is owned exactly here ([`RowRef::to_owned_row`]) — the one forced
/// copy on the SQLite leaf, an `Arc` bump on the memory leaf — so from this point
/// up every value is owned and the lending borrow never escapes. Still a row, not
/// a node: the node wrapper is added at the connection boundary, not here.
struct LeafRows<'g, S: RowStream> {
    rows: S,
    error_sink: Option<Rc<RefCell<Option<RindleError>>>>,
    _p: PhantomData<&'g ()>,
}

impl<S: RowStream> Iterator for LeafRows<'_, S> {
    type Item = Row;
    #[inline]
    fn next(&mut self) -> Option<Row> {
        self.rows
            .next_row()
            .and_then(|r| match r.try_to_owned_row() {
                Ok(row) => Some(row),
                Err(err) => {
                    if let Some(sink) = &self.error_sink {
                        *sink.borrow_mut() = Some(err);
                    }
                    None
                }
            })
    }
}

// ---------------------------------------------------------------------------
// Overlay narrowing (pure functions over Overlays) — computeOverlays + helpers
// ---------------------------------------------------------------------------

/// The initial {add, remove} from the (already epoch-gated) overlay change.
/// Add ⇒ {add}; Remove ⇒ {remove}; Edit ⇒ {add: new, remove: old}
/// (`computeOverlays`, `memory-source.ts:734-753`).
fn overlays_from_change(overlay: Option<&Overlay>) -> Overlays {
    match overlay.map(|o| &o.change) {
        Some(SourceChange::Add(r)) => Overlays {
            add: Some(r.clone()),
            remove: None,
        },
        Some(SourceChange::Remove(r)) => Overlays {
            add: None,
            remove: Some(r.clone()),
        },
        Some(SourceChange::Edit { row, old }) => Overlays {
            add: Some(row.clone()),
            remove: Some(old.clone()),
        },
        None => Overlays::default(),
    }
}

/// Drop an add/remove that sorts strictly **before** `start_at`
/// (`overlaysForStartAt`, `memory-source.ts:806`). Reverse-aware.
pub fn overlays_for_start_at(o: Overlays, start_at: &Row, sort: &Sort, reverse: bool) -> Overlays {
    let before = |r: &Row| compare_rows_rev(sort, reverse, r, start_at) == Ordering::Less;
    Overlays {
        add: o.add.filter(|r| !before(r)),
        remove: o.remove.filter(|r| !before(r)),
    }
}

/// Drop an add/remove that does not match the constraint (`overlaysForConstraint`,
/// `memory-source.ts:821`).
pub fn overlays_for_constraint(o: Overlays, constraint: &Constraint) -> Overlays {
    Overlays {
        add: o.add.filter(|r| constraint_matches(r, constraint)),
        remove: o.remove.filter(|r| constraint_matches(r, constraint)),
    }
}

/// Keep an add/remove iff it matches **some** entry of the multiConstraint
/// (`overlaysForMultiConstraint`, `memory-source.ts:787`).
pub fn overlays_for_multi_constraint(o: Overlays, mc: &MultiConstraint) -> Overlays {
    Overlays {
        add: o
            .add
            .filter(|r| mc.iter().any(|c| constraint_matches(r, c))),
        remove: o
            .remove
            .filter(|r| mc.iter().any(|c| constraint_matches(r, c))),
    }
}

/// Drop an add/remove that fails the filter predicate
/// (`overlaysForFilterPredicate`, `memory-source.ts:836`).
pub fn overlays_for_filter_predicate(o: Overlays, predicate: &RowPredicate) -> Overlays {
    Overlays {
        add: o.add.filter(|r| predicate(r)),
        remove: o.remove.filter(|r| predicate(r)),
    }
}

/// The full ordered narrowing pipeline: startAt → constraint → each non-empty
/// multiConstraint → filter (`computeOverlays`, `memory-source.ts:722`). Pure —
/// no leaf access. `sort`/`reverse` are the **index** comparator (the same one
/// the splice uses), per the `break`-safety dependency (§3.1).
pub fn compute_overlays(
    start_at: Option<&Row>,
    constraint: Option<&Constraint>,
    overlay: Option<&Overlay>,
    sort: &Sort,
    reverse: bool,
    predicate: Option<&RowPredicate>,
    multi_constraints: &[MultiConstraint],
) -> Overlays {
    let mut o = overlays_from_change(overlay);
    if let Some(s) = start_at {
        o = overlays_for_start_at(o, s, sort, reverse);
    }
    if let Some(c) = constraint {
        o = overlays_for_constraint(o, c);
    }
    for mc in multi_constraints {
        if !mc.is_empty() {
            o = overlays_for_multi_constraint(o, mc);
        }
    }
    if let Some(p) = predicate {
        o = overlays_for_filter_predicate(o, p);
    }
    o
}

// ---------------------------------------------------------------------------
// The ordered overlay splice (generateWithOverlay + generateWithOverlayInner)
// ---------------------------------------------------------------------------

/// Splice the pre-narrowed `add`/`remove` into an ORDERED row stream
/// (`generateWithOverlayInner`, `memory-source.ts:849`): emit `add` the first time
/// it sorts before the current row (and at end if never emitted); suppress
/// `remove` the first time it sort-equals a row (sort always includes the full
/// PK, so sort-equal ⇒ identity — §3.5). Lazy: no materialization.
struct OverlaySplice<I: Iterator<Item = Row>> {
    inner: I,
    add: Option<Row>,
    remove: Option<Row>,
    sort: Sort,
    reverse: bool,
    add_yielded: bool,
    remove_skipped: bool,
    /// A row pulled but deferred because we emitted `add` ahead of it.
    buffered: Option<Row>,
}

impl<I: Iterator<Item = Row>> Iterator for OverlaySplice<I> {
    type Item = Row;
    fn next(&mut self) -> Option<Row> {
        loop {
            let row = match self.buffered.take().or_else(|| self.inner.next()) {
                Some(r) => r,
                None => {
                    // End of stream: flush a never-emitted add (it sorts after all).
                    if !self.add_yielded {
                        if let Some(a) = self.add.take() {
                            self.add_yielded = true;
                            return Some(a);
                        }
                    }
                    return None;
                }
            };

            // Emit the add the first time it sorts before the current row, then
            // re-process the current row on the next pull.
            if !self.add_yielded {
                if let Some(a) = self.add.as_ref() {
                    if compare_rows_rev(&self.sort, self.reverse, a, &row) == Ordering::Less {
                        self.add_yielded = true;
                        let add_row = a.clone();
                        self.buffered = Some(row);
                        return Some(add_row);
                    }
                }
            }

            // Suppress the matching remove exactly once.
            if !self.remove_skipped {
                if let Some(rem) = self.remove.as_ref() {
                    if compare_rows_rev(&self.sort, self.reverse, rem, &row) == Ordering::Equal {
                        self.remove_skipped = true;
                        continue;
                    }
                }
            }

            return Some(row);
        }
    }
}

/// Splice already-computed `overlays` into an ordered row stream. Exposed for
/// direct testing (the JS `generateWithOverlayInner` suite). `sort`/`reverse` are
/// the index comparator.
pub fn generate_with_overlay_inner<'g, I: Iterator<Item = Row> + 'g>(
    inner: I,
    overlays: Overlays,
    sort: Sort,
    reverse: bool,
) -> RowFlow<'g> {
    // Empty overlays (every hydration — the overlay only exists mid-push, and
    // narrowing often clears it during a push too): the splice would pass every
    // row through unchanged, so don't pay its per-row checks.
    if overlays.is_empty() {
        return Box::new(inner);
    }
    Box::new(OverlaySplice {
        inner,
        add: overlays.add,
        remove: overlays.remove,
        sort,
        reverse,
        add_yielded: false,
        remove_skipped: false,
        buffered: None,
    })
}

/// Drive a leaf cursor through the epoch-gated, narrowed, ordered overlay splice
/// (`generateWithOverlay`, `memory-source.ts:697`). Generic over the leaf
/// `S: RowStream` — the seam both backends feed. `sort`/`reverse` are the
/// **index** comparator (the `break`-safety dependency, §3.1); `start_at`,
/// `constraint`, `predicate`, `multi_constraints` narrow the overlay only (the
/// committed rows are filtered later by [`generate_with_filter`]).
#[allow(clippy::too_many_arguments)]
pub fn generate_with_overlay<'g, S: RowStream + 'g>(
    start_at: Option<&Row>,
    rows: S,
    constraint: Option<&Constraint>,
    overlay: Option<&Overlay>,
    last_pushed_epoch: u32,
    sort: &Sort,
    reverse: bool,
    predicate: Option<&RowPredicate>,
    multi_constraints: &[MultiConstraint],
) -> RowFlow<'g> {
    generate_with_overlay_checked(
        start_at,
        rows,
        constraint,
        overlay,
        last_pushed_epoch,
        sort,
        reverse,
        predicate,
        multi_constraints,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn generate_with_overlay_checked<'g, S: RowStream + 'g>(
    start_at: Option<&Row>,
    rows: S,
    constraint: Option<&Constraint>,
    overlay: Option<&Overlay>,
    last_pushed_epoch: u32,
    sort: &Sort,
    reverse: bool,
    predicate: Option<&RowPredicate>,
    multi_constraints: &[MultiConstraint],
    error_sink: Option<Rc<RefCell<Option<RindleError>>>>,
) -> RowFlow<'g> {
    let gated = overlay.filter(|o| last_pushed_epoch >= o.epoch);
    let overlays = compute_overlays(
        start_at,
        constraint,
        gated,
        sort,
        reverse,
        predicate,
        multi_constraints,
    );
    let inner = LeafRows {
        rows,
        error_sink,
        _p: PhantomData,
    };
    generate_with_overlay_inner(inner, overlays, sort.clone(), reverse)
}

// ---------------------------------------------------------------------------
// The unordered overlay splice (generateWithOverlayUnordered + …InnerUnordered)
// ---------------------------------------------------------------------------

/// Unordered splice (`generateWithOverlayInnerUnordered`, `memory-source.ts:929`):
/// inject `add` eagerly at the head (row not yet in storage), suppress `remove`
/// inline by **PK match** (no comparator available, so explicit PK equality).
struct OverlayUnordered<I: Iterator<Item = Row>> {
    inner: I,
    add: Option<Row>,
    remove: Option<Row>,
    primary_key: Vec<ColId>,
    add_emitted: bool,
    remove_skipped: bool,
}

impl<I: Iterator<Item = Row>> Iterator for OverlayUnordered<I> {
    type Item = Row;
    fn next(&mut self) -> Option<Row> {
        if !self.add_emitted {
            self.add_emitted = true;
            if let Some(a) = self.add.take() {
                return Some(a);
            }
        }
        loop {
            let row = self.inner.next()?;
            if !self.remove_skipped {
                if let Some(rem) = self.remove.as_ref() {
                    if same_pk(&row, rem, &self.primary_key) {
                        self.remove_skipped = true;
                        continue;
                    }
                }
            }
            return Some(row);
        }
    }
}

/// Splice already-computed `overlays` into an unordered row stream. Exposed for
/// the JS `generateWithOverlayInnerUnordered` suite.
pub fn generate_with_overlay_inner_unordered<'g, I: Iterator<Item = Row> + 'g>(
    inner: I,
    overlays: Overlays,
    primary_key: Vec<ColId>,
) -> RowFlow<'g> {
    // Same pass-through fast path as the ordered splice above.
    if overlays.is_empty() {
        return Box::new(inner);
    }
    Box::new(OverlayUnordered {
        inner,
        add: overlays.add,
        remove: overlays.remove,
        primary_key,
        add_emitted: false,
        remove_skipped: false,
    })
}

/// Unordered variant of [`generate_with_overlay`] (`generateWithOverlayUnordered`,
/// `memory-source.ts:885`): NO start, add injected eagerly, remove suppressed by
/// PK. Narrowing is constraint → multiConstraint → predicate (no startAt).
pub fn generate_with_overlay_unordered<'g, S: RowStream + 'g>(
    rows: S,
    constraint: Option<&Constraint>,
    overlay: Option<&Overlay>,
    last_pushed_epoch: u32,
    primary_key: &[ColId],
    predicate: Option<&RowPredicate>,
    multi_constraints: &[MultiConstraint],
) -> RowFlow<'g> {
    generate_with_overlay_unordered_checked(
        rows,
        constraint,
        overlay,
        last_pushed_epoch,
        primary_key,
        predicate,
        multi_constraints,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn generate_with_overlay_unordered_checked<'g, S: RowStream + 'g>(
    rows: S,
    constraint: Option<&Constraint>,
    overlay: Option<&Overlay>,
    last_pushed_epoch: u32,
    primary_key: &[ColId],
    predicate: Option<&RowPredicate>,
    multi_constraints: &[MultiConstraint],
    error_sink: Option<Rc<RefCell<Option<RindleError>>>>,
) -> RowFlow<'g> {
    let gated = overlay.filter(|o| last_pushed_epoch >= o.epoch);
    let mut overlays = overlays_from_change(gated);
    if let Some(c) = constraint {
        overlays = overlays_for_constraint(overlays, c);
    }
    for mc in multi_constraints {
        if !mc.is_empty() {
            overlays = overlays_for_multi_constraint(overlays, mc);
        }
    }
    if let Some(p) = predicate {
        overlays = overlays_for_filter_predicate(overlays, p);
    }
    let inner = LeafRows {
        rows,
        error_sink,
        _p: PhantomData,
    };
    generate_with_overlay_inner_unordered(inner, overlays, primary_key.to_vec())
}

// ---------------------------------------------------------------------------
// The post-scan row chain (start / constraint / filter)
// ---------------------------------------------------------------------------

/// Skip rows until the start bound is reached, then pass the rest
/// (`generateWithStart`, `memory-source.ts:653`). Uses the **connection**
/// comparator (reverse-aware). `start = None` ⇒ pass-through.
pub fn generate_with_start<'g>(
    rows: RowFlow<'g>,
    start: Option<Start>,
    sort: Sort,
    reverse: bool,
) -> RowFlow<'g> {
    let Some(start) = start else {
        return rows;
    };
    let mut started = false;
    Box::new(rows.filter(move |row| {
        if !started {
            let c = compare_rows_rev(&sort, reverse, row, &start.row);
            started = match start.basis {
                Basis::At => c != Ordering::Less,
                Basis::After => c == Ordering::Greater,
            };
        }
        started
    }))
}

/// Trim trailing rows once the constraint stops matching — a `break`, NOT a
/// filter (`generateWithConstraint`, `memory-source.ts:505`). Valid because the
/// chosen index is sorted by the constraint columns first, so once the scan
/// passes the last matching row every subsequent row fails (§3.1).
pub fn generate_with_constraint<'g>(
    rows: RowFlow<'g>,
    constraint: Option<Constraint>,
) -> RowFlow<'g> {
    match constraint {
        None => rows,
        Some(c) => Box::new(rows.take_while(move |row| constraint_matches(row, &c))),
    }
}

/// Drop rows failing the predicate (`generateWithFilter`,
/// `memory-source.ts:517`).
pub fn generate_with_filter<'g>(rows: RowFlow<'g>, predicate: RowPredicate) -> RowFlow<'g> {
    Box::new(rows.filter(move |row| predicate(row)))
}

// ---------------------------------------------------------------------------
// K-way merge of pre-sorted row streams (mergeSortedStreams)
// ---------------------------------------------------------------------------

struct HeapEntry {
    row: Row,
    /// Which stream to refill from after this entry's row is emitted.
    idx: usize,
}

/// A binary min-heap merge of pre-sorted row streams (`mergeSortedStreams`,
/// `memory-source.ts:1051`). O(log K) per emit. Each heap entry holds an OWNED
/// row so the head survives a refill of its sub-stream (the lending leaf would
/// invalidate a borrow on the next pull — §4.6). `Drop`-clean for free: the
/// sub-streams live in `streams` and are dropped (releasing their B+tree
/// cursors + `Arc` snapshots) when this struct drops — the port of the JS
/// `finally` → `.return()` on each non-exhausted sub-iterator.
struct MergeSorted<'g> {
    streams: Vec<RowFlow<'g>>,
    heap: Vec<HeapEntry>,
    sort: Sort,
    reverse: bool,
    primed: bool,
}

impl MergeSorted<'_> {
    #[inline]
    fn less(&self, a: usize, b: usize) -> bool {
        compare_rows_rev(
            &self.sort,
            self.reverse,
            &self.heap[a].row,
            &self.heap[b].row,
        ) == Ordering::Less
    }

    fn sift_up(&mut self, mut i: usize) {
        while i > 0 {
            let p = (i - 1) >> 1;
            if !self.less(i, p) {
                return;
            }
            self.heap.swap(i, p);
            i = p;
        }
    }

    fn sift_down(&mut self, mut i: usize) {
        let n = self.heap.len();
        loop {
            let l = (i << 1) + 1;
            let r = l + 1;
            let mut smallest = i;
            if l < n && self.less(l, smallest) {
                smallest = l;
            }
            if r < n && self.less(r, smallest) {
                smallest = r;
            }
            if smallest == i {
                return;
            }
            self.heap.swap(i, smallest);
            i = smallest;
        }
    }

    fn prime(&mut self) {
        for i in 0..self.streams.len() {
            if let Some(row) = self.streams[i].next() {
                self.heap.push(HeapEntry { row, idx: i });
                self.sift_up(self.heap.len() - 1);
            }
        }
    }
}

impl Iterator for MergeSorted<'_> {
    type Item = Row;
    fn next(&mut self) -> Option<Row> {
        if !self.primed {
            self.primed = true;
            self.prime();
        }
        if self.heap.is_empty() {
            return None;
        }
        let idx = self.heap[0].idx;
        match self.streams[idx].next() {
            Some(new_row) => {
                // Refill the root in place and sift down; return the old root row.
                let old = std::mem::replace(&mut self.heap[0].row, new_row);
                self.sift_down(0);
                Some(old)
            }
            None => {
                // Stream exhausted. Move the tail into the root and shrink.
                let last = self.heap.pop().unwrap();
                if self.heap.is_empty() {
                    // `last` WAS the root (single entry); its row is the result.
                    Some(last.row)
                } else {
                    let old_root = std::mem::replace(&mut self.heap[0], last);
                    self.sift_down(0);
                    Some(old_root.row)
                }
            }
        }
    }
}

/// K-way min-heap merge of pre-sorted row streams under the reverse-aware
/// comparator. `Drop`-clean (early close drops every sub-stream — §5.5).
pub fn merge_sorted_streams<'g>(
    streams: Vec<RowFlow<'g>>,
    sort: Sort,
    reverse: bool,
) -> RowFlow<'g> {
    Box::new(MergeSorted {
        streams,
        heap: Vec::new(),
        sort,
        reverse,
        primed: false,
    })
}

// ---------------------------------------------------------------------------
// Predicate-filtered push (filter-push.ts + maybe-split-and-push-edit-change.ts)
// ---------------------------------------------------------------------------

/// Apply the connection's filter predicate to one source change, then hand the
/// (possibly transformed) change to `push` (`filterPush`, `filter-push.ts`).
/// Still a `SourceChange` (rows) — the node-bearing downstream `Change` is built
/// at the connection boundary, not here. With no predicate the change passes
/// verbatim. Add/Remove are dropped if the row fails the predicate. An Edit is
/// split by predicate (`maybeSplitAndPushEditChange`): old&new present ⇒ Edit;
/// only old ⇒ Remove(old); only new ⇒ Add(new); neither ⇒ dropped.
pub fn filter_push(
    change: SourceChange,
    predicate: Option<&RowPredicate>,
    push: &dyn Fn(SourceChange),
) {
    let Some(p) = predicate else {
        push(change);
        return;
    };
    match change {
        SourceChange::Add(r) => {
            if p(&r) {
                push(SourceChange::Add(r));
            }
        }
        SourceChange::Remove(r) => {
            if p(&r) {
                push(SourceChange::Remove(r));
            }
        }
        SourceChange::Edit { row, old } => {
            let old_present = p(&old);
            let new_present = p(&row);
            if old_present && new_present {
                push(SourceChange::Edit { row, old });
            } else if old_present {
                push(SourceChange::Remove(old));
            } else if new_present {
                push(SourceChange::Add(row));
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Eager push orchestration (genPushAndWriteWithSplitEdit / genPushAndWrite / genPush)
// ---------------------------------------------------------------------------

/// Existence asserts + per-connection fan-out with the live overlay
/// (`genPush`, `memory-source.ts:595`). For each connection with a wired output:
/// bump its epoch gate FIRST (so a reentrant fetch of a *later* connection won't
/// see the overlay yet — the self-join gate), set the overlay, then push the
/// (filter-applied) change. Clears the overlay after the drain. `push_one`
/// receives a `SourceChange` (rows); the connection boundary turns it into a
/// node-bearing downstream `Change`.
fn gen_push(
    conns: &ConnTable,
    change: &SourceChange,
    exists: &dyn Fn(&Row) -> bool,
    set_overlay: &dyn Fn(Option<Overlay>),
    push_epoch: u32,
    push_one: &dyn Fn(&Connection, SourceChange),
) {
    match change {
        SourceChange::Add(r) => debug_assert!(!exists(r), "MemorySource: ADD row already exists"),
        SourceChange::Remove(r) => debug_assert!(exists(r), "MemorySource: REMOVE row not found"),
        SourceChange::Edit { old, .. } => {
            debug_assert!(exists(old), "MemorySource: EDIT old row not found")
        }
    }

    let candidates = conns.push_candidates(change);
    let connections = conns.borrow();
    debug_assert_index_sound(&connections, &candidates, change);
    metric_add!(push_visited, candidates.len() as u64);
    metric_add!(
        push_skipped,
        (connections.len() as u64).saturating_sub(candidates.len() as u64)
    );

    for &slot in &candidates {
        let conn = &connections[slot as usize];
        if conn.output.get().is_some() {
            conn.last_pushed_epoch.set(push_epoch);
            set_overlay(Some(Overlay {
                epoch: push_epoch,
                change: change.clone(),
            }));
            let predicate = conn.filters.as_ref().map(|f| &f.predicate);
            filter_push(change.clone(), predicate, &|sc| push_one(conn, sc));
        }
    }

    set_overlay(None);
}

/// Debug-only soundness net for the push index (`designs/205` §4 "Debug-mode
/// exactness check"). The safety argument is "every skipped connection's predicate
/// rejects both rows", so assert exactly that: walk the wired **non**-candidates and
/// require both `predicate(old)` and `predicate(new)` to be false. O(N), compiled out
/// of release — it turns any guard-extraction or comparator-coarseness bug into an
/// immediate assert instead of a silently missing delta, and makes every differential
/// / fuzz lane a guard-soundness test.
#[inline]
fn debug_assert_index_sound(connections: &[Connection], candidates: &[u32], change: &SourceChange) {
    #[cfg(debug_assertions)]
    {
        // `candidates` is sorted ascending (push_candidates), so membership is a
        // binary search.
        let rejects = |conn: &Connection| -> bool {
            let Some(f) = conn.filters.as_ref() else {
                // A filterless connection sits in the always-visited scan list, so it
                // is ALWAYS a candidate — reaching here means the index dropped it.
                return false;
            };
            let p = &f.predicate;
            match change {
                SourceChange::Add(r) | SourceChange::Remove(r) => !p(r),
                SourceChange::Edit { row, old } => !p(row) && !p(old),
            }
        };
        for (slot, conn) in connections.iter().enumerate() {
            if conn.output.get().is_none() || candidates.binary_search(&(slot as u32)).is_ok() {
                continue;
            }
            debug_assert!(
                rejects(conn),
                "push index skipped connection slot {slot} whose predicate accepts the changed row(s) — a missing delta"
            );
        }
    }
    let _ = (connections, candidates, change);
}

/// Change-consistency check: ADD-not-already-present / REMOVE-present /
/// EDIT-old-present (WS02.2).
///
/// - **strict** (the per-graph `validate_changes` toggle): returns
///   `Err(RindleError::ConsistencyViolation)` on a malformed change. This runs in
///   **release**, so a misbehaving change producer surfaces a typed error instead of
///   silently corrupting index/overlay/view state.
/// - **non-strict** (default, hot path): `debug_assert!` only — stripped in release,
///   exactly as before. Zero overhead and identical behavior for valid streams.
fn assert_change_existence_checked(
    change: &SourceChange,
    exists: &dyn Fn(&Row) -> Result<bool, RindleError>,
    strict: bool,
) -> Result<(), RindleError> {
    let check = |ok: bool, kind: &'static str| -> Result<(), RindleError> {
        if ok {
            return Ok(());
        }
        if strict {
            Err(RindleError::ConsistencyViolation { kind })
        } else {
            debug_assert!(false, "Source: {kind}");
            Ok(())
        }
    };
    match change {
        SourceChange::Add(r) => check(!exists(r)?, "ADD row already exists"),
        SourceChange::Remove(r) => check(exists(r)?, "REMOVE row not found"),
        SourceChange::Edit { old, .. } => check(exists(old)?, "EDIT old row not found"),
    }
}

/// Fallible sibling of [`gen_push`]. It preserves the hot-path infallible helper
/// for memory while allowing external backends to surface existence/check errors
/// without side channels.
fn try_gen_push(
    conns: &ConnTable,
    change: &SourceChange,
    exists: &dyn Fn(&Row) -> Result<bool, RindleError>,
    set_overlay: &dyn Fn(Option<Overlay>),
    push_epoch: u32,
    push_one: &dyn Fn(&Connection, SourceChange),
    strict: bool,
) -> Result<(), RindleError> {
    assert_change_existence_checked(change, exists, strict)?;

    let candidates = conns.push_candidates(change);
    let connections = conns.borrow();
    debug_assert_index_sound(&connections, &candidates, change);
    metric_add!(push_visited, candidates.len() as u64);
    metric_add!(
        push_skipped,
        (connections.len() as u64).saturating_sub(candidates.len() as u64)
    );

    for &slot in &candidates {
        let conn = &connections[slot as usize];
        if conn.output.get().is_some() {
            conn.last_pushed_epoch.set(push_epoch);
            set_overlay(Some(Overlay {
                epoch: push_epoch,
                change: change.clone(),
            }));
            let predicate = conn.filters.as_ref().map(|f| &f.predicate);
            filter_push(change.clone(), predicate, &|sc| push_one(conn, sc));
        }
    }

    set_overlay(None);
    Ok(())
}

/// `genPush` then commit the write (`genPushAndWrite`, `memory-source.ts:581`).
/// The write happens AFTER the drain — a reentrant fetch during the drain sees
/// the overlay but not the committed row (the ordering invariant, §3.11).
#[allow(clippy::too_many_arguments)]
fn gen_push_and_write(
    conns: &ConnTable,
    change: SourceChange,
    exists: &dyn Fn(&Row) -> bool,
    set_overlay: &dyn Fn(Option<Overlay>),
    write: &dyn Fn(&SourceChange),
    push_epoch: u32,
    push_one: &dyn Fn(&Connection, SourceChange),
) {
    gen_push(conns, &change, exists, set_overlay, push_epoch, push_one);
    write(&change);
}

#[allow(clippy::too_many_arguments)]
fn try_gen_push_and_write(
    conns: &ConnTable,
    change: SourceChange,
    exists: &dyn Fn(&Row) -> Result<bool, RindleError>,
    set_overlay: &dyn Fn(Option<Overlay>),
    write: &dyn Fn(&SourceChange) -> Result<(), RindleError>,
    push_epoch: u32,
    push_one: &dyn Fn(&Connection, SourceChange),
    strict: bool,
) -> Result<(), RindleError> {
    try_gen_push(
        conns,
        &change,
        exists,
        set_overlay,
        push_epoch,
        push_one,
        strict,
    )?;
    write(&change)
}

/// Edit-split detection + per-connection fan-out + deferred write
/// (`genPushAndWriteWithSplitEdit`, `memory-source.ts:525`). If the change is an
/// Edit and any connection's split-edit key changes value
/// (`!values_equal` — null→null DOES split, §3.6 polarity), decompose into
/// Remove(old) then Add(row), each its own epoch + push + write.
///
/// The callbacks abstract the backend: `exists` is the existence check (memory:
/// primary-index `has`); `set_overlay`/`next_epoch` mutate source state; `write`
/// commits (memory: every index); `push_one` drives one source change to one
/// connection's output (→ connection boundary → downstream). No `'yield'`
/// (foundations §5).
#[allow(clippy::too_many_arguments)]
pub fn gen_push_and_write_with_split_edit(
    conns: &ConnTable,
    change: SourceChange,
    exists: &dyn Fn(&Row) -> bool,
    set_overlay: &dyn Fn(Option<Overlay>),
    write: &dyn Fn(&SourceChange),
    next_epoch: &dyn Fn() -> u32,
    push_one: &dyn Fn(&Connection, SourceChange),
) {
    if let SourceChange::Edit { row, old } = &change {
        if should_split_edit(conns, row, old) {
            gen_push_and_write(
                conns,
                SourceChange::Remove(old.clone()),
                exists,
                set_overlay,
                write,
                next_epoch(),
                push_one,
            );
            gen_push_and_write(
                conns,
                SourceChange::Add(row.clone()),
                exists,
                set_overlay,
                write,
                next_epoch(),
                push_one,
            );
            return;
        }
    }
    gen_push_and_write(
        conns,
        change,
        exists,
        set_overlay,
        write,
        next_epoch(),
        push_one,
    );
}

/// Whether an Edit must be split into Remove(old)+Add(row): true iff some live
/// connection's split-edit key changed value. Reads the [`ConnTable`]'s refcounted
/// **union** of split keys (`designs/205` §5) — exactly equivalent to today's
/// `any`-of-`any` over per-connection key lists (`k` is in the union iff some live
/// slot lists it), at O(distinct keys) instead of O(N·keys). Once triggered the split
/// is applied table-wide, as before; only the trigger check changed.
#[inline]
fn should_split_edit(conns: &ConnTable, row: &Row, old: &Row) -> bool {
    conns.has_split_edit_keys()
        && conns
            .split_edit_keys()
            .into_iter()
            .any(|k| !values_equal(row.col(k), old.col(k)))
}

#[allow(clippy::too_many_arguments)]
pub fn try_gen_push_and_write_with_split_edit(
    conns: &ConnTable,
    change: SourceChange,
    exists: &dyn Fn(&Row) -> Result<bool, RindleError>,
    set_overlay: &dyn Fn(Option<Overlay>),
    write: &dyn Fn(&SourceChange) -> Result<(), RindleError>,
    next_epoch: &dyn Fn() -> u32,
    push_one: &dyn Fn(&Connection, SourceChange),
    strict: bool,
) -> Result<(), RindleError> {
    if let SourceChange::Edit { row, old } = &change {
        if should_split_edit(conns, row, old) {
            try_gen_push_and_write(
                conns,
                SourceChange::Remove(old.clone()),
                exists,
                set_overlay,
                write,
                next_epoch(),
                push_one,
                strict,
            )?;
            try_gen_push_and_write(
                conns,
                SourceChange::Add(row.clone()),
                exists,
                set_overlay,
                write,
                next_epoch(),
                push_one,
                strict,
            )?;
            return Ok(());
        }
    }
    try_gen_push_and_write(
        conns,
        change,
        exists,
        set_overlay,
        write,
        next_epoch(),
        push_one,
        strict,
    )
}

#[cfg(test)]
mod conn_table_tests {
    use super::*;
    use crate::change::Port;
    use crate::graph::NodeId;
    use crate::value::owned_row;

    fn minimal_conn() -> Connection {
        Connection {
            sort: Vec::new(),
            unordered: true,
            split_edit_keys: Vec::new(),
            filters: None,
            last_pushed_epoch: Cell::new(0),
            output: Cell::new(None),
        }
    }

    /// A connection guarded on `col ∈ values` (with a trivial predicate — the index
    /// tests only exercise candidate selection, not `filter_push`).
    fn guarded_conn(col: ColId, values: Vec<OwnedValue>) -> Connection {
        Connection {
            filters: Some(ConnectionFilters {
                predicate: Rc::new(|_: &Row| true),
                pk_constraint: None,
                fully_applied: true,
                sql_condition: None,
                push_guard: Some(PushGuard { col, values }),
            }),
            ..minimal_conn()
        }
    }

    fn conn_with_split_keys(keys: Vec<ColId>) -> Connection {
        Connection {
            split_edit_keys: keys,
            ..minimal_conn()
        }
    }

    fn add(cells: Vec<OwnedValue>) -> SourceChange {
        SourceChange::Add(owned_row(cells))
    }

    #[test]
    fn connect_indexes_guard_so_push_candidates_prunes() {
        let t = ConnTable::new();
        let g = t.connect(guarded_conn(0, vec![OwnedValue::Int(42)])); // where col0 = 42
        let s = t.connect(minimal_conn()); // no filter -> scan list, always visited

        // A write to 42 visits both the guarded match and the scan connection.
        let mut hit = t.push_candidates(&add(vec![OwnedValue::Int(42)]));
        hit.sort_unstable();
        assert_eq!(hit, vec![g.idx, s.idx]);
        // A write to 99 visits only the scan connection.
        assert_eq!(
            t.push_candidates(&add(vec![OwnedValue::Int(99)])),
            vec![s.idx]
        );
    }

    #[test]
    fn destroy_unindexes_from_push_candidates() {
        let t = ConnTable::new();
        let g = t.connect(guarded_conn(0, vec![OwnedValue::Int(42)]));
        assert_eq!(
            t.push_candidates(&add(vec![OwnedValue::Int(42)])),
            vec![g.idx]
        );
        t.destroy(g);
        assert!(t
            .push_candidates(&add(vec![OwnedValue::Int(42)]))
            .is_empty());
    }

    #[test]
    fn recycled_slot_reindexes_under_new_guard() {
        let t = ConnTable::new();
        let a = t.connect(guarded_conn(0, vec![OwnedValue::Int(10)]));
        t.destroy(a);
        // Recycle a's slot with a different guard value.
        let b = t.connect(guarded_conn(0, vec![OwnedValue::Int(20)]));
        assert_eq!(b.idx, a.idx, "slot recycled");
        assert!(t
            .push_candidates(&add(vec![OwnedValue::Int(10)]))
            .is_empty());
        assert_eq!(
            t.push_candidates(&add(vec![OwnedValue::Int(20)])),
            vec![b.idx]
        );
    }

    #[test]
    fn split_edit_key_union_maintained_through_conntable() {
        let t = ConnTable::new();
        assert!(!t.has_split_edit_keys());
        let a = t.connect(conn_with_split_keys(vec![2, 5]));
        let b = t.connect(conn_with_split_keys(vec![5]));
        assert!(t.has_split_edit_keys());
        let mut keys = t.split_edit_keys();
        keys.sort_unstable();
        assert_eq!(keys, vec![2, 5]);
        t.destroy(a); // 2 -> 0 (drop), 5 -> 1 (b still lists it)
        assert_eq!(t.split_edit_keys(), vec![5]);
        t.destroy(b);
        assert!(!t.has_split_edit_keys());
    }

    /// A torn-down connection's slot is RECYCLED (same index, bumped generation), so the
    /// table does not grow per teardown — the leak fix.
    #[test]
    fn destroyed_connection_slot_is_recycled_not_leaked() {
        let t = ConnTable::new();
        let a = t.connect(minimal_conn());
        let b = t.connect(minimal_conn());
        assert_eq!(t.len(), 2);
        assert_eq!((a.idx, a.gen), (0, 0));
        assert_eq!((b.idx, b.gen), (1, 0));

        t.set_output(
            a,
            OutEdge {
                node: NodeId::new(9, 0),
                port: Port::Single,
            },
        );
        t.destroy(a);

        let c = t.connect(minimal_conn());
        assert_eq!(t.len(), 2, "slot reused, the table did not grow");
        assert_eq!(c.idx, a.idx, "recycled a's slot index");
        assert!(c.gen > a.gen, "recycled slot carries a bumped generation");
        // The recycled connection starts disconnected (a fresh `Connection`, output None).
        assert!(t.borrow()[c.idx as usize].output.get().is_none());
    }

    /// A stale `ConnId` (the pre-free generation) fail-fasts on any gen-checked access.
    #[test]
    #[should_panic(expected = "stale ConnId")]
    fn stale_conn_id_fails_fast() {
        let t = ConnTable::new();
        let a = t.connect(minimal_conn());
        t.destroy(a);
        let _ = t.sort(a);
    }

    /// `destroy` is idempotent / stale-safe: a second destroy of the same handle is a
    /// no-op (the slot is freed exactly once, not pushed onto the free-list twice).
    #[test]
    fn double_destroy_is_a_noop() {
        let t = ConnTable::new();
        let a = t.connect(minimal_conn());
        t.destroy(a);
        t.destroy(a); // stale generation → no-op

        let b = t.connect(minimal_conn()); // reuses a's single freed slot
        assert_eq!(b.idx, a.idx);
        let c = t.connect(minimal_conn()); // free-list now empty → must grow
        assert_ne!(c.idx, a.idx);
        assert_eq!(t.len(), 2);
    }
}
