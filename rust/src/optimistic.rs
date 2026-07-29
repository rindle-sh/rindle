//! The **optimistic-writes fork/rebase loop**, productionized
//! (`OPTIMISTIC-WRITES-DESIGN.md` §1, §3) — the engine half the wasm `Db` exposes to
//! the JS `OptimisticBackend`.
//!
//! [`OptimisticTables`](crate::optimistic::OptimisticTables) owns the per-table `sync` forks and the §1.3 **rewind** (cycle
//! steps 1–4): `S' = fork(sync) + D`, `rewindDiff = structural_diff(head, S')` pushed
//! through the one live pipeline, then **`sync` re-forked from the live tree** — the
//! pointer-sharing GC that keeps every later diff bounded by the outstanding-mutation
//! set (§1.2). Step 5 (re-invoking pending mutators) is the *caller's*: client mutators
//! are host functions, re-run between `rewind` and delivery as ordinary pushes (the wasm
//! layer brackets the whole cycle and buffers every emitted event).
//!
//! The buffered cycle stream (rewind + re-invocation) is then forwarded **RAW** to the
//! consumer — the IVM sink emits a valid incremental order, so folding it in order
//! reconstructs `view(head_new)`, and the view's own once-per-transaction settle is the
//! §3 "deliver once" boundary. (Earlier revisions netted the stream through a §3
//! `Coalescer` before flattening; it was removed — folding a flattened, gated event log
//! shipped stale parent path locators and dropped gated child writes. Raw delivery is
//! correct; restoring event-minimality / per-row reference-stability for still-pending
//! rows is a view-level optimization, deferred.)
//!
//! Between server batches there is no cycle: an optimistic write is an ordinary
//! push + delivery (the §1.3 trivial case), and `sync` is untouched.
//!
//! **One source per table.** A table has exactly one authority; there is no per-row
//! provenance, no winner rule, and no hold-back overlay. The multi-source merge that
//! briefly lived here (`src/merge.rs`, `RINDLE-REALTIME-QUERY-ENABLEMENT-DESIGN.md` §5.2)
//! was removed by `designs-implemented/302-ROOM-STORE-SEPARATION-DESIGN.md`: a room owns its own
//! tables rather than co-backing a daemon table, so the engine never needs to know a room
//! exists.

use std::collections::BTreeMap;

use crate::btree::{apply_source_change_to_tree, BTree};
use crate::change::SourceChange;
use crate::error::RindleError;
use crate::graph::{Graph, NodeId};
use crate::value::Sort;

// ---------------------------------------------------------------------------
// OptimisticTables — the sync forks + the §1.3 rewind (steps 1–4)
// ---------------------------------------------------------------------------

struct SyncEntry {
    source: NodeId,
    /// The authoritative baseline tree (§1.1): fork-shared with the live source's
    /// primary tree except for the outstanding optimistic set.
    sync: BTree,
    sort: Sort,
}

/// The per-table `sync` baselines and the rewind driver. One per optimistic `Db`.
#[derive(Default)]
pub struct OptimisticTables {
    tables: BTreeMap<String, SyncEntry>,
}

impl OptimisticTables {
    pub fn new() -> OptimisticTables {
        OptimisticTables::default()
    }

    /// Start tracking `table` (whose shared source is `source`): `sync` forks the
    /// live primary tree as-is. Call at registration, **before** any optimistic
    /// write to the table, so the §1.2 invariant (`head` ⊖ `sync` = the pending set)
    /// holds from the start. Errors if `source` is not an in-memory source (the
    /// loop forks COW trees; a SQL-backed source has none).
    pub fn track(&mut self, graph: &Graph, table: &str, source: NodeId) -> Result<(), RindleError> {
        if self.tables.contains_key(table) {
            return Ok(());
        }
        let ms = graph.memory_source(source).ok_or_else(|| {
            RindleError::schema_violation(format!(
                "optimistic tracking needs an in-memory source for table {table:?}"
            ))
        })?;
        self.tables.insert(
            table.to_string(),
            SyncEntry {
                source,
                sync: ms.fork_primary(),
                sort: ms.primary_sort().clone(),
            },
        );
        Ok(())
    }

    pub fn is_tracked(&self, table: &str) -> bool {
        self.tables.contains_key(table)
    }

    /// Stop tracking `table`, dropping its `sync` baseline — the inverse of [`track`](Self::track).
    /// Call when the table's source is being removed (its last reading query is gone), so the
    /// per-table fork is reclaimed and a later re-`track` of the same name starts clean. A
    /// no-op for an untracked table.
    pub fn forget(&mut self, table: &str) {
        self.tables.remove(table);
    }

    /// §1.3 steps 1–4 over **every** tracked table: build `S' = fork(sync) + D`,
    /// push `structural_diff(head, S')` through the live pipeline (the rewind —
    /// simultaneously un-applying the optimistic layer and folding in the server
    /// delta; confirmed-correct predictions cancel out of the diff), then re-fork
    /// `sync` from the live tree (now at `S'`). Tables absent from `deltas` still
    /// rewind (their divergence is exactly their pending writes); an unknown table
    /// in `deltas` is an error. The caller drains the change sinks afterwards and,
    /// after re-invoking its pending mutators, coalesces the whole cycle (§3).
    ///
    /// On a push error the optimistic state is poisoned (the engine may hold a
    /// partial rewind): the caller's only safe recovery is a full re-hydrate.
    pub fn rewind(
        &mut self,
        graph: &Graph,
        deltas: &BTreeMap<String, Vec<SourceChange>>,
    ) -> Result<(), RindleError> {
        for table in deltas.keys() {
            if !self.tables.contains_key(table) {
                // I1 (`201-LOCAL-ONLY-TABLES-DESIGN.md` §9.3): an untracked table is a LOCAL-only
                // table (or unknown). The server must never drive one — a delta arriving for it is
                // a server/schema bug, and silently applying it would corrupt local-authoritative
                // state. Fail loud instead.
                return Err(RindleError::schema_violation(format!(
                    "server delta for untracked (local-only or unknown) table {table:?}"
                )));
            }
        }
        for (table, entry) in self.tables.iter_mut() {
            // Step 1: S' = fork(sync) + D (a cheap transient; value-exact server state).
            let mut s_prime = entry.sync.fork();
            for c in deltas.get(table).map(|v| v.as_slice()).unwrap_or(&[]) {
                apply_source_change_to_tree(&mut s_prime, c, &entry.sort);
            }
            // Step 2: the rewind diff, off the LIVE tree's current state (head).
            let ms = graph
                .memory_source(entry.source)
                .expect("tracked source is a memory source");
            let head = ms.fork_primary();
            let rewind = head.structural_diff(&s_prime, &entry.sort);
            // Step 3: push it through the one live pipeline → its input becomes S'.
            for c in rewind {
                graph.try_source_push(entry.source, c)?;
            }
            // Step 4: re-fork sync FROM THE LIVE TREE (head's lineage at the S'
            // value) — never from the transient s_prime — so sync and the upcoming
            // head stay pointer-identical except the re-applied optimistic set.
            entry.sync = ms.fork_primary();
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Test-only introspection for the rebase structural-sharing invariant (Q1).
// `#[cfg(test)]` ⇒ compiled ONLY for the crate's own unit tests; the `sync` reach into a
// private field, which a child `mod` may do. Zero production API surface or overhead.
// ---------------------------------------------------------------------------

#[cfg(test)]
impl OptimisticTables {
    /// The confirmed `sync` baseline of `table`.
    fn test_sync(&self, table: &str) -> &BTree {
        &self.tables.get(table).expect("tracked table").sync
    }
}

#[cfg(test)]
mod rebase_invariant_tests {
    use super::*;
    use crate::change::SourceChange;
    use crate::graph::Graph;
    use crate::value::{owned_row, OwnedRow, OwnedValue, SourceSchema};

    fn schema() -> SourceSchema {
        SourceSchema::new(vec!["id", "val"], vec![0], vec![(0, true)])
    }
    fn r(id: i64, val: i64) -> OwnedRow {
        owned_row(vec![OwnedValue::Int(id), OwnedValue::Int(val)])
    }
    fn table_delta(changes: Vec<SourceChange>) -> BTreeMap<String, Vec<SourceChange>> {
        let mut m = BTreeMap::new();
        m.insert("t".to_string(), changes);
        m
    }

    // --- Q1 Test A: a full-confirm rewind re-forks `sync` FROM the live tree (shares its COW root),
    //     rather than rebuilding it — so `sync` and the live tree are the SAME allocation. This is
    //     step 4's whole point: re-forking from the transient `s_prime` instead would rebuild. ---
    #[test]
    fn q1a_full_confirm_shares_root() {
        let mut graph = Graph::new();
        let logical = graph.try_add_source(schema(), vec![]).unwrap();
        let mut opt = OptimisticTables::new();
        opt.track(&graph, "t", logical).unwrap();
        // K optimistic writes (new keys) — the live head grows; `sync` stays.
        let k = 12i64;
        for i in 0..k {
            graph
                .try_source_push(logical, SourceChange::Add(r(i, i * 10)))
                .unwrap();
        }
        // Full-confirm rewind (server delta == those writes ⇒ head == S'): `sync` re-forks from live.
        let delta = table_delta((0..k).map(|i| SourceChange::Add(r(i, i * 10))).collect());
        opt.rewind(&graph, &delta).unwrap();

        let live = graph.memory_source(logical).unwrap().fork_primary();
        let sync = opt.test_sync("t");
        assert!(
            sync.shares_root(&live),
            "after a full-confirm rewind, sync must SHARE the live tree's COW root \
             (re-forked from live, not rebuilt from scratch)"
        );
        assert_eq!(
            sync.unshared_node_count(&live),
            0,
            "zero structural divergence after a full confirm"
        );
    }

    // --- Q1 Test B: the live tree diverges from `sync` by ONLY the pending footprint (≈ K·height
    //     path-copied nodes), never O(N) — the §1.2 bound every later diff rests on. ---
    #[test]
    fn q1b_pending_divergence_is_bounded() {
        let n = 20_000i64;
        let mut graph = Graph::new();
        let init: Vec<OwnedRow> = (0..n).map(|i| r(i, i)).collect();
        let logical = graph.try_add_source(schema(), init).unwrap();
        let mut opt = OptimisticTables::new();
        opt.track(&graph, "t", logical).unwrap();
        // Exercise the rewind's re-fork so `sync` is a fork of the live tree (shares its root).
        opt.rewind(&graph, &BTreeMap::new()).unwrap();
        let baseline_shares = {
            let sync = opt.test_sync("t");
            let live = graph.memory_source(logical).unwrap().fork_primary();
            sync.shares_root(&live)
        };
        assert!(
            baseline_shares,
            "after the (empty) rewind, sync must be a fork of the live tree (shares its root)"
        );

        // K optimistic writes (no confirm): the live head diverges by exactly the K pending paths.
        let k = 10i64;
        for j in 0..k {
            graph
                .try_source_push(logical, SourceChange::Add(r(n + j, j)))
                .unwrap();
        }
        let live = graph.memory_source(logical).unwrap().fork_primary();
        let sync = opt.test_sync("t");
        let unshared = live.unshared_node_count(sync);
        assert!(
            unshared < 300,
            "the live tree must diverge from sync by ≈ K·height nodes (the pending footprint), \
             not O(N): unshared={unshared}, K={k}, N={n}"
        );
        assert!(
            (unshared as i64) < n / 20,
            "divergence must be << N: unshared={unshared}, N={n}"
        );
    }
}
