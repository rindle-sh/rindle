//! The client-side leaf [`Source`]: a `Source` backed entirely by in-RAM COW
//! B+trees (`btree.rs`). Production port of `packages/zql/src/ivm/memory-source.ts`
//! (`class MemorySource`). It owns three things (`04` §1.1):
//!
//! 1. **Multiple COW B+tree indexes** — a `Sort → Index` map. The primary index
//!    is keyed by `(pk asc, …)`; others are built lazily from the primary when a
//!    connection requests a sort the source doesn't have, and are **kept for the
//!    source's lifetime** (never GC'd — §3.10).
//! 2. **Connections** — one per downstream, each with its own `sort`, filters,
//!    and split-edit keys ([`Connection`], in `source_common` since the shape is
//!    backend-identical).
//! 3. **The overlay / push / edit-split machinery** — fan a change out to every
//!    connection (overlay live, epoch-gated), clear the overlay, then write to
//!    every index. This machinery is the backend-agnostic
//!    [`source_common`] seam, shared verbatim with the
//!    SQLite `TableSource`.
//!
//! It is the zero-alloc-to-vend showcase: the B+tree stores `OwnedRow`s; the leaf
//! scan vends each as a borrowed `&OwnedRow` through the lending
//! [`RowStream`], and a row is only owned-cloned (an
//! `Arc` refcount bump) when it escapes into a `Node`, an overlay, or the view.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;

use crate::btree::{row_bound_of, BTree, Bound, RowBound};
use crate::change::{
    constraint_matches, constraint_matches_primary_key, merge_constraints, Constraint,
    FetchRequest, MultiConstraint, OutEdge, RowFlow, SourceChange,
};
use crate::error::RindleError;
use crate::graph::{ConnId, Source};
use crate::source_common::{
    self, generate_with_constraint, generate_with_filter, generate_with_overlay,
    generate_with_start, merge_sorted_streams, ConnTable, Connection, ConnectionFilters,
};
use crate::value::{
    compare_rows, owned_row, ColId, OwnedRow as Row, OwnedValue, RowRef, RowStream, Schema, Sort,
};

/// One index: a COW B+tree of `OwnedRow`s under a sort-specific comparator.
/// Mirrors JS `Index` (`memory-source.ts:69`) MINUS `usedBy` (the index-GC that
/// was removed — §4.1 DEVIATION: with no GC, `usedBy` is dead bookkeeping).
pub struct Index {
    pub sort: Sort,
    pub data: BTree,
}

/// The in-memory source. `fetch` seeks a [`BTreeCursor`](crate::btree::BTreeCursor)
/// over a COW snapshot and drives it through [`source_common`]; `write_change`
/// path-copies via `Rc::make_mut` so in-flight cursors keep their snapshot (the
/// load-bearing COW property — §6).
pub struct MemorySource {
    /// Static table metadata (columns + PK). The per-connection sort lives on each
    /// [`Connection`]; the schema's own `sort` is reported to downstream operators
    /// that need a child ordering (e.g. join relationship streams).
    pub schema: Schema,
    primary_key: Vec<ColId>,
    /// `(pk[0] asc, pk[1] asc, …)` — the primary index sort.
    primary_index_sort: Sort,
    /// `Sort → Index`. Keyed by the resolved `Sort` itself (not the JS
    /// `JSON.stringify(sort)` — `Sort: Hash + Eq`). KEPT for the source's lifetime.
    indexes: RefCell<HashMap<Sort, Index>>,
    conns: ConnTable,
    /// The single in-flight change, epoch-tagged. Read LIVE via a tightly-scoped
    /// borrow during a reentrant fetch, cloned out, cleared after the push drains
    /// BEFORE the write (§6.3).
    overlay: RefCell<Option<source_common::Overlay>>,
    epoch: Cell<u32>,
    /// Primitive #2 demo: RAII cursor count. Incremented when a fetch stream is
    /// created, decremented by its `Drop` (the analogue of a SQLite cursor).
    cursors_open: Rc<Cell<i64>>,
}

impl MemorySource {
    /// Validate that every ingest row matches the schema's column width (WS02.4): a
    /// wrong-width row is rejected here with a [`RindleError::SchemaViolation`] rather
    /// than reaching the engine's unchecked column index ([`crate::value::RowRef::col`])
    /// where it would panic / abort.
    pub fn validate_rows(schema: &Schema, rows: &[Row]) -> Result<(), RindleError> {
        let want = schema.columns.len();
        for (i, r) in rows.iter().enumerate() {
            if r.len() != want {
                return Err(RindleError::schema_violation(format!(
                    "ingest row {i} has {} cells but the schema declares {want} columns",
                    r.len()
                )));
            }
        }
        Ok(())
    }

    /// Build a source over `initial` rows, **validating** each row's width against
    /// the schema first (WS02.4). The primary index is keyed by `(pk asc)`; `initial`
    /// is sorted under it and bulk-loaded (`from_sorted`, O(N)). Callers guarantee
    /// `initial` has unique primary keys (a set). Prefer this over
    /// [`MemorySource::new`] / [`crate::graph::Graph::add_source`] in production.
    pub fn try_new(schema: Schema, initial: Vec<Row>) -> Result<MemorySource, RindleError> {
        Self::validate_rows(&schema, &initial)?;
        let primary_key = schema.primary_key.clone();
        let primary_index_sort: Sort = primary_key.iter().map(|&c| (c, true)).collect();
        let mut data = initial;
        data.sort_by(|a, b| compare_rows(&primary_index_sort, a, b));
        let tree = BTree::from_sorted(data.into_iter(), &primary_index_sort);
        let mut indexes = HashMap::new();
        indexes.insert(
            primary_index_sort.clone(),
            Index {
                sort: primary_index_sort.clone(),
                data: tree,
            },
        );
        Ok(MemorySource {
            schema,
            primary_key,
            primary_index_sort,
            indexes: RefCell::new(indexes),
            conns: ConnTable::new(),
            overlay: RefCell::new(None),
            epoch: Cell::new(0),
            cursors_open: Rc::new(Cell::new(0)),
        })
    }

    /// Panicking convenience wrapper over [`MemorySource::try_new`] (tests /
    /// prototyping).
    ///
    /// # Panics
    /// If a row's width does not match the schema. Prefer [`MemorySource::try_new`]
    /// or [`crate::graph::Graph::try_add_source`] in production (WS02.6).
    pub fn new(schema: Schema, initial: Vec<Row>) -> MemorySource {
        Self::try_new(schema, initial).expect("MemorySource::new: invalid ingest rows")
    }

    /// O(1) `fork` (`memory-source.ts:135`): a new source sharing the primary
    /// index's COW root (one `Rc` bump, no deep copy). Other indexes are NOT
    /// carried (they rebuild lazily on demand, as in the JS).
    pub fn fork(&self) -> MemorySource {
        let primary = self.indexes.borrow();
        let data = primary
            .get(&self.primary_index_sort)
            .expect("primary index")
            .data
            .fork();
        let mut indexes = HashMap::new();
        indexes.insert(
            self.primary_index_sort.clone(),
            Index {
                sort: self.primary_index_sort.clone(),
                data,
            },
        );
        MemorySource {
            schema: self.schema.clone(),
            primary_key: self.primary_key.clone(),
            primary_index_sort: self.primary_index_sort.clone(),
            indexes: RefCell::new(indexes),
            conns: ConnTable::new(),
            overlay: RefCell::new(None),
            epoch: Cell::new(0),
            cursors_open: Rc::new(Cell::new(0)),
        }
    }

    /// Like [`fork`](Self::fork), but COW-carries **every** index (each a one-`Rc` bump on its
    /// B+tree root), not just the primary — the `203-MUTATOR-READS-DESIGN.md` §7.3 (A) read-cache
    /// optimization. A mutator's one-shot `tx.query` that sorts in an order a live query already
    /// built then shares that secondary index by COW lineage, collapsing its O(n log n) lazy
    /// build to a bump. The carried indexes are kept current by
    /// [`apply_change`](Self::apply_change) (it writes every index), so seeding the cache fork
    /// (buffer replay) and write-forwarding maintain them. Used **only** for the off-graph
    /// read-cache fork and its per-query fork-of-fork; the plain [`fork`](Self::fork) stays
    /// primary-only for the optimistic `sync` baselines and the other COW snapshots that want the
    /// lighter, divergence-bounded fork (`OPTIMISTIC-WRITES-DESIGN.md` §1.2).
    pub fn fork_with_indexes(&self) -> MemorySource {
        let src = self.indexes.borrow();
        let mut indexes = HashMap::with_capacity(src.len());
        for (sort, index) in src.iter() {
            indexes.insert(
                sort.clone(),
                Index {
                    sort: index.sort.clone(),
                    data: index.data.fork(),
                },
            );
        }
        MemorySource {
            schema: self.schema.clone(),
            primary_key: self.primary_key.clone(),
            primary_index_sort: self.primary_index_sort.clone(),
            indexes: RefCell::new(indexes),
            conns: ConnTable::new(),
            overlay: RefCell::new(None),
            epoch: Cell::new(0),
            cursors_open: Rc::new(Cell::new(0)),
        }
    }

    /// Test hook: the set of index sorts currently held (indexes persist across
    /// `destroy` — §3.10). Mirrors `getIndexKeys` (`memory-source.ts:252`).
    pub fn get_index_keys(&self) -> Vec<Sort> {
        self.indexes.borrow().keys().cloned().collect()
    }

    /// An O(1) COW fork of the **primary** index tree (one `Rc` bump). The optimistic
    /// loop's `sync`/`S'` trees are forks of this — sharing the live tree's node
    /// lineage is what keeps `structural_diff` bounded by the divergence
    /// (`OPTIMISTIC-WRITES-DESIGN.md` §1.2).
    pub fn fork_primary(&self) -> BTree {
        self.indexes
            .borrow()
            .get(&self.primary_index_sort)
            .expect("primary index")
            .data
            .fork()
    }

    /// The primary-index sort (`(pk[0] asc, pk[1] asc, …)`) — the comparator every
    /// fork/diff over [`fork_primary`](Self::fork_primary) trees must use.
    pub fn primary_sort(&self) -> &Sort {
        &self.primary_index_sort
    }

    /// The current row whose primary key equals `probe`'s (other columns ignored by
    /// the pk-only primary sort), or `None`. The optimistic `MutationTx` read path.
    pub fn get_by_pk(&self, probe: &Row) -> Option<Row> {
        self.indexes
            .borrow()
            .get(&self.primary_index_sort)
            .expect("primary index")
            .data
            .get(probe, &self.primary_index_sort)
            .cloned()
    }

    pub fn cursors_open(&self) -> i64 {
        self.cursors_open.get()
    }

    /// Total connection slots, including freed (recyclable) ones. Stays bounded by the
    /// peak live-connection count under query churn (the slot is recycled on teardown —
    /// it does NOT grow per teardown). For metrics/tests.
    pub fn conn_count(&self) -> usize {
        self.conns.len()
    }

    /// Number of live downstream connections (readers) — 0 once every pipeline reading this
    /// source has been torn down. See [`ConnTable::live_conn_count`].
    pub fn live_conn_count(&self) -> usize {
        self.conns.live_conn_count()
    }

    /// Wire a connection's downstream edge (mirrors `input.setOutput`).
    pub fn set_conn_output(&self, conn: ConnId, edge: OutEdge) {
        self.conns.set_output(conn, edge);
    }

    // -- push orchestration (the source owns its state; the graph drives output) --

    /// Eager push: fan `change` to every connection (overlay live, epoch-gated),
    /// clear the overlay, then write to every index — via the backend-agnostic
    /// [`source_common::gen_push_and_write_with_split_edit`]. `push_one` is the
    /// graph's downstream driver (it reads the connection's output edge). Edit-
    /// splitting, existence asserts, and the write-after-drain ordering all live
    /// in `source_common`. `push_one` receives a `SourceChange` (rows); the graph
    /// turns it into a node-bearing downstream `Change` at the connection boundary.
    pub fn push(&self, change: SourceChange, push_one: &dyn Fn(&Connection, SourceChange)) {
        source_common::gen_push_and_write_with_split_edit(
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
        );
    }

    /// Fallible push used by strict change validation (WS02.2). Identical fan-out to
    /// [`MemorySource::push`] but routed through the fallible `source_common` sibling
    /// so a malformed change (when `strict`) returns a
    /// [`RindleError::ConsistencyViolation`]. The memory `exists`/`write` are infallible,
    /// so they wrap in `Ok`.
    pub fn try_push(
        &self,
        change: SourceChange,
        push_one: &dyn Fn(&Connection, SourceChange),
        strict: bool,
    ) -> Result<(), RindleError> {
        source_common::try_gen_push_and_write_with_split_edit(
            &self.conns,
            change,
            &|row| Ok(self.exists(row)),
            &|o| *self.overlay.borrow_mut() = o,
            &|c| {
                self.write_change(c);
                Ok(())
            },
            &|| {
                let e = self.epoch.get() + 1;
                self.epoch.set(e);
                e
            },
            push_one,
            strict,
        )
    }

    /// `exists` = primary-index `has` (`memory-source.ts:450`).
    fn exists(&self, row: &Row) -> bool {
        self.indexes
            .borrow()
            .get(&self.primary_index_sort)
            .expect("primary index")
            .data
            .has(row, &self.primary_index_sort)
    }

    /// Apply ADD/REMOVE/EDIT to **every** index (`#writeChange`,
    /// `memory-source.ts:463`). Each index is a separate COW B+tree; EDIT is
    /// delete-old + add-new (cannot `set` — old/row may differ in position). The
    /// same row `Arc` is stored in every index (only the tree structure differs).
    fn write_change(&self, change: &SourceChange) {
        let mut indexes = self.indexes.borrow_mut();
        for index in indexes.values_mut() {
            match change {
                SourceChange::Add(r) => {
                    let ok = index.data.add(r.clone(), &index.sort);
                    debug_assert!(ok, "MemorySource: ADD must be newly inserted");
                }
                SourceChange::Remove(r) => {
                    let ok = index.data.delete(r, &index.sort);
                    debug_assert!(ok, "MemorySource: REMOVE must have been present");
                }
                SourceChange::Edit { row, old } => {
                    let ok = index.data.delete(old, &index.sort);
                    debug_assert!(ok, "MemorySource: EDIT old must have been present");
                    index.data.add(row.clone(), &index.sort);
                }
            }
        }
    }

    /// Apply ADD/REMOVE/EDIT to every index **without** the live push path's existence
    /// asserts and **without** the connection fan-out — for the off-graph read-cache
    /// fork of `203-MUTATOR-READS-DESIGN.md` (mutator reads). A read-cache fork has no
    /// connections, so there is nothing to fan out to; this is purely the index write
    /// that keeps the fork equal to `live ⊕ this txn's buffer` for its table (§4 / §4.1):
    /// it seeds the fork (replay of the table's buffered ops) and write-forwards each
    /// later staged op. It is deliberately tolerant of the degenerate "edit a row that
    /// isn't there" case the staging path defers to commit (`WriteTxn::edit`), so a
    /// later `tx.query` cannot panic on it.
    pub fn apply_change(&self, change: &SourceChange) {
        let mut indexes = self.indexes.borrow_mut();
        for index in indexes.values_mut() {
            match change {
                SourceChange::Add(r) => {
                    index.data.add(r.clone(), &index.sort);
                }
                SourceChange::Remove(r) => {
                    index.data.delete(r, &index.sort);
                }
                SourceChange::Edit { row, old } => {
                    index.data.delete(old, &index.sort);
                    index.data.add(row.clone(), &index.sort);
                }
            }
        }
    }

    // -- lazy index management --

    /// Build the index for `index_sort` from the primary index's rows if it does
    /// not exist yet (`#getOrCreateIndex`, `memory-source.ts:225`). O(N) bulk load
    /// (`from_sorted`), never repeated `add`. Indexes are never removed (§3.10).
    fn ensure_index(&self, index_sort: &Sort) {
        if self.indexes.borrow().contains_key(index_sort) {
            return;
        }
        // Own each primary row (an `Arc` bump — the new index shares the primary's
        // row `Arc`s), re-sort under the new comparator, bulk-load.
        let mut rows: Vec<Row> = {
            let indexes = self.indexes.borrow();
            let primary = indexes
                .get(&self.primary_index_sort)
                .expect("primary index");
            let mut cur = primary
                .data
                .values_from(None, true, &self.primary_index_sort);
            let mut v = Vec::with_capacity(primary.data.len());
            while let Some(r) = cur.next_row() {
                v.push(r.to_owned_row());
            }
            v
        };
        rows.sort_by(|a, b| compare_rows(index_sort, a, b));
        let data = BTree::from_sorted(rows.into_iter(), index_sort);
        self.indexes.borrow_mut().insert(
            index_sort.clone(),
            Index {
                sort: index_sort.clone(),
                data,
            },
        );
    }

    // -- fetch --

    /// The multiConstraint path (`#fetchMulti`, `memory-source.ts:380`): drive a
    /// sub-fetch per entry of the FIRST multiConstraint (merging base ∧ entry),
    /// k-way-merge the sorted sub-streams, then post-filter against the rest
    /// (keep iff the row matches SOME entry of EVERY remaining multiConstraint).
    /// This is the one place the memory leaf diverges from `05` (which lowers to
    /// native SQL `IN`): it pays one index seek per primary entry.
    fn fetch_multi<'g>(&'g self, conn: ConnId, req: &FetchRequest) -> RowFlow<'g> {
        let multis: Vec<&MultiConstraint> = req
            .multi_constraints
            .iter()
            .filter(|mc| !mc.is_empty())
            .collect();
        // `has_multi` guarantees `multis` is non-empty.
        let primary = multis[0];
        let rest: Vec<MultiConstraint> = multis[1..].iter().map(|m| (*m).clone()).collect();
        let base = req.constraint.clone();

        let sub_streams: Vec<RowFlow<'g>> = primary
            .iter()
            .map(|c| {
                let merged = merge_constraints(base.as_ref(), c);
                let sub_req = FetchRequest {
                    constraint: Some(merged),
                    multi_constraints: Vec::new(),
                    start: req.start.clone(),
                    reverse: req.reverse,
                };
                self.fetch(conn, &sub_req)
            })
            .collect();

        let sort = self.conns.sort(conn);
        let merged = merge_sorted_streams(sub_streams, sort, req.reverse);

        if rest.is_empty() {
            return merged;
        }
        Box::new(merged.filter(move |row| {
            rest.iter()
                .all(|mc| mc.iter().any(|c| constraint_matches(row, c)))
        }))
    }
}

/// `assertOrderingIncludesPK` (`complete-ordering.ts`): an ordered connection's
/// sort must include every primary-key column (so sort-equality ⇒ identity — the
/// overlay remove-suppression and view diffing depend on it).
fn assert_ordering_includes_pk(sort: &Sort, pk: &[ColId]) {
    for &p in pk {
        assert!(
            sort.iter().any(|(c, _)| *c == p),
            "connection ordering must include the primary key column {p}",
        );
    }
}

/// Build the scan-start `RowBound` for a constraint (`memory-source.ts:326`):
/// each constrained column → its value; each unconstrained index column → the
/// type-extreme sentinel that sorts to the *near* end, reverse-aware
/// (forward: asc→Min/desc→Max; reverse: asc→Max/desc→Min).
fn build_scan_start(c: &Constraint, index_sort: &Sort, reverse: bool) -> RowBound {
    index_sort
        .iter()
        .map(|&(col, asc)| match c.iter().find(|(cc, _)| *cc == col) {
            Some((_, v)) => (col, Bound::Val(v.clone())),
            None => {
                let sentinel = if reverse {
                    if asc {
                        Bound::Max
                    } else {
                        Bound::Min
                    }
                } else if asc {
                    Bound::Min
                } else {
                    Bound::Max
                };
                (col, sentinel)
            }
        })
        .collect()
}

impl crate::scalar::ScalarSource for MemorySource {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    /// PK-only for the slice — the only statically-unique key a memory source has
    /// (its other indexes are query-derived ordering structures, not uniqueness
    /// constraints; `SCALAR-SUBQUERY-DESIGN.md` §4.3).
    fn unique_keys(&self) -> Vec<Vec<ColId>> {
        vec![self.primary_key.clone()]
    }

    fn lookup_unique(&self, bound: &[(ColId, OwnedValue)]) -> Option<Row> {
        // The caller guarantees `bound` covers the PK; build a full-width probe row
        // (non-PK cells are `Null` — `get_by_pk` only compares the PK columns).
        let mut cells = vec![OwnedValue::Null; self.schema.columns.len()];
        for (col, val) in bound {
            cells[*col] = val.clone();
        }
        self.get_by_pk(&owned_row(cells))
    }
}

impl Source for MemorySource {
    fn connect(
        &self,
        sort: Option<Sort>,
        filters: Option<ConnectionFilters>,
        split_edit_keys: Vec<ColId>,
    ) -> ConnId {
        let unordered = sort.is_none();
        let internal_sort = sort.unwrap_or_else(|| self.primary_index_sort.clone());
        if !unordered {
            assert_ordering_includes_pk(&internal_sort, &self.primary_key);
        }
        self.conns.connect(Connection {
            sort: internal_sort,
            unordered,
            split_edit_keys,
            filters,
            last_pushed_epoch: Cell::new(0),
            output: Cell::new(None),
        })
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn conn_sort(&self, conn: ConnId) -> Sort {
        self.conns.sort(conn)
    }

    fn destroy(&self, conn: ConnId) {
        // Drop the connection's output edge so it stops receiving pushes (and RECYCLE
        // its slot — the `ConnTable` free-list); KEEP all indexes (the deliberate JS
        // decision — §3.10).
        self.conns.destroy(conn);
    }

    fn cursors_open(&self) -> i64 {
        MemorySource::cursors_open(self)
    }

    fn try_push(
        &self,
        change: SourceChange,
        push_one: &dyn Fn(&Connection, SourceChange),
        strict: bool,
    ) -> Result<(), RindleError> {
        MemorySource::try_push(self, change, push_one, strict)
    }

    /// The in-memory backend is infallible — a fetch never parks an error.
    fn take_error(&self) -> Option<RindleError> {
        None
    }

    fn set_conn_output(&self, conn: ConnId, edge: OutEdge) {
        MemorySource::set_conn_output(self, conn, edge)
    }

    /// `#fetch` (the hot read path — `memory-source.ts:257`). Pick an index, seek
    /// a `scanStart` `RowBound`, then drive the lending cursor through the
    /// generator chain: overlay (index comparator) → start (connection comparator)
    /// → constraint (`break`) → filter. The two comparators differ by design when
    /// a constraint is present (§3.1). Emits **rows**, not nodes — the connection
    /// boundary (`Graph::fetch` on the `SourceConn`) wraps each row in a leaf node.
    fn fetch<'g>(&'g self, conn: ConnId, req: &FetchRequest) -> RowFlow<'g> {
        if req.has_multi() {
            return self.fetch_multi(conn, req);
        }
        let idx = self.conns.live_ix(conn);

        // Snapshot the connection's needed bits (short borrow, cloned out — no
        // RefCell borrow held across the vend).
        let (sort, predicate, pk_constraint, last_epoch) = {
            let conns = self.conns.borrow();
            let c = &conns[idx];
            (
                c.sort.clone(),
                c.filters.as_ref().map(|f| f.predicate.clone()),
                c.filters.as_ref().and_then(|f| f.pk_constraint.clone()),
                c.last_pushed_epoch.get(),
            )
        };

        // The PK constraint (from filters) is more limiting than req.constraint.
        let fetch_or_pk: Option<&Constraint> = pk_constraint.as_ref().or(req.constraint.as_ref());

        // 1. INDEX SORT: constraint cols (asc) then requested sort — unless a
        //    single-column PK is fully constrained (at most one result).
        let mut index_sort: Sort = Vec::new();
        if let Some(c) = fetch_or_pk {
            for (col, _) in c {
                index_sort.push((*col, true));
            }
        }
        if self.primary_key.len() > 1
            || fetch_or_pk.is_none()
            || !constraint_matches_primary_key(fetch_or_pk.unwrap(), &self.primary_key)
        {
            index_sort.extend(sort.iter().copied());
        }

        // 2. INDEX: fetch-or-build (lazy; kept for the source's lifetime).
        self.ensure_index(&index_sort);

        // 3. scanStart RowBound (constraint-seek; sentinels for unconstrained cols).
        //    With no constraint, seek to the plain start row (the basis is applied
        //    by generate_with_start, not the seek).
        let scan_start: Option<RowBound> = match fetch_or_pk {
            Some(c) => Some(build_scan_start(c, &index_sort, req.reverse)),
            None => req
                .start
                .as_ref()
                .map(|s| row_bound_of(&s.row, &index_sort)),
        };

        // 4. SCAN (lending; zero per-row alloc). Always inclusive.
        let cursor = {
            let indexes = self.indexes.borrow();
            let index = indexes.get(&index_sort).expect("ensure_index built it");
            if req.reverse {
                index
                    .data
                    .values_from_reversed(scan_start.as_ref(), true, &index_sort)
            } else {
                index
                    .data
                    .values_from(scan_start.as_ref(), true, &index_sort)
            }
        };

        // RAII guard (Primitive #2): bundled with the cursor — the innermost,
        // statically dispatched layer — so the count balances to zero on Drop
        // (even if a downstream `Take` abandons the stream early) without an
        // extra boxed wrapper on every fetch.
        let cursor = GuardedRows {
            rows: cursor,
            _guard: CursorGuard::new(self.cursors_open.clone()),
        };

        // 5. Snapshot the epoch-gated overlay (tight borrow, cloned out).
        let overlay = self.overlay.borrow().clone();

        // 6. The generator chain. The overlay splice uses the INDEX comparator
        //    (`break`-safety, §3.1); the start gate uses the CONNECTION comparator.
        //    Both use `req.constraint` (NOT fetch_or_pk — the PK constraint from
        //    filters acts as a fetch seek + a row filter, not the overlay
        //    constraint).
        let start_at = req.start.as_ref().map(|s| s.row.clone());
        let mut stream = generate_with_overlay(
            start_at.as_ref(),
            cursor,
            req.constraint.as_ref(),
            overlay.as_ref(),
            last_epoch,
            &index_sort,
            req.reverse,
            predicate.as_ref(),
            &[],
        );
        stream = generate_with_start(stream, req.start.clone(), sort, req.reverse);
        stream = generate_with_constraint(stream, req.constraint.clone());
        if let Some(p) = predicate {
            stream = generate_with_filter(stream, p);
        }
        stream
    }
}

// ---------------------------------------------------------------------------
// RAII cursor guard (Primitive #2): Drop == JS finally/.return()
// ---------------------------------------------------------------------------

struct CursorGuard(Rc<Cell<i64>>);
impl CursorGuard {
    fn new(c: Rc<Cell<i64>>) -> Self {
        c.set(c.get() + 1);
        CursorGuard(c)
    }
}
impl Drop for CursorGuard {
    fn drop(&mut self) {
        self.0.set(self.0.get() - 1);
    }
}

/// Bundles the cursor-count guard with the leaf cursor itself (mirroring the
/// SQLite leaf, where `OwnedSqliteRows` owns its guard), so dropping the
/// generator chain that owns the cursor — even after partial consumption, a
/// downstream `Take` breaking early — releases the "cursor" with no boxed
/// wrapper. The Rust analogue of the JS `for-of` calling `generator.return()`
/// → `finally`, deterministic and free.
struct GuardedRows<S> {
    rows: S,
    _guard: CursorGuard,
}
impl<S: RowStream> RowStream for GuardedRows<S> {
    type Row<'a>
        = S::Row<'a>
    where
        Self: 'a;
    fn next_row(&mut self) -> Option<Self::Row<'_>> {
        self.rows.next_row()
    }
}
