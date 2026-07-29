//! Slice 1 of `OPTIMISTIC-WRITES-DESIGN.md`: the `sync`/`head` two-tree harness +
//! the §1.3 reconcile cycle, over ONE live local pipeline.
//!
//! This is a harness (not production code — the `OptimisticBackend` of §9 is a
//! later slice). It models the design's two COW source trees for one table:
//!
//! - `head` — what the single live pipeline (and the materialized view) runs off.
//! - `sync` — the authoritative baseline; never displayed, runs no pipeline.
//!
//! and drives the §1.3 cycle on a hand-fed server batch (NO mutators yet — pending
//! optimistic writes are raw row-ops, re-applied verbatim on rebase; the
//! function-registry re-invocation is Slice 3):
//!
//!   1. `S' = fork(sync) + apply(D)`
//!   2. `rewindDiff = structuralDiff(head, S')`     (the Slice-0 primitive)
//!   3. push `rewindDiff` through the live pipeline  → its source is now `S'`
//!   4. `sync = fork(head)` (head is at `S'` here)   → re-fork, reset sharing
//!   5. drop pending `mid <= lmid` / `rejected`, re-apply the rest → head = S'+pending'
//!
//! Oracle: the reconciled live view equals a FRESH hydrate of `sync + pending'`
//! (incremental reconcile == from-scratch materialization). The `head` BTree
//! mirrors the live source (same ops applied to both) so the diff runs against the
//! real COW fork lineage — exercising the efficient `ptr_eq` sharing path, not a
//! rebuild-from-rows.

use rindle::btree::{BTree, RowRef, RowStream};
use rindle::graph::{Graph, NodeId};
use rindle::value::{owned_row as row, OwnedRow, OwnedValue, Sort, SourceSchema};
use rindle::{
    build_pipeline, Ast, Condition, Dir, Lit, Op, OrderPart, SimpleCondition, SourceChange,
    ValuePosition,
};

use OwnedValue::Int;

// ---------------------------------------------------------------------------
// The table + query under test
// ---------------------------------------------------------------------------

// issue(id, owner, score); PK id.
fn source_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "owner", "score"], vec![0], vec![(0, true)])
}
fn primary_sort() -> Sort {
    vec![(0, true)] // pk id asc
}
fn irow(id: i64, owner: i64, score: i64) -> OwnedRow {
    row(vec![Int(id), Int(owner), Int(score)])
}
fn key_of(r: &OwnedRow) -> i64 {
    match &r.col(0).to_owned() {
        Int(v) => *v,
        other => panic!("non-int key {other:?}"),
    }
}

/// `WHERE score > 50 ORDER BY id` — the view is a strict, ordered projection of
/// the source, so the rewind diff has to drive a real filter, not a passthrough.
fn query_ast() -> Ast {
    Ast {
        table: "issue".into(),
        r#where: Some(Condition::Simple(SimpleCondition {
            op: Op::Gt,
            left: ValuePosition::Column {
                name: "score".into(),
            },
            right: ValuePosition::Literal {
                value: Lit::Number(50.0),
            },
        })),
        order_by: vec![OrderPart("id".into(), Dir::Asc)],
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// BTree helpers (the source-tree mutations the engine performs internally)
// ---------------------------------------------------------------------------

fn bt_from_rows(rows: &[OwnedRow], sort: &Sort) -> BTree {
    let mut t = BTree::new();
    for r in rows {
        t.add(r.clone(), sort);
    }
    t
}
fn apply_to_bt(t: &mut BTree, c: &SourceChange, sort: &Sort) {
    match c {
        SourceChange::Add(r) => {
            t.add(r.clone(), sort);
        }
        SourceChange::Remove(r) => {
            t.delete(r, sort);
        }
        SourceChange::Edit { old, row } => {
            t.delete(old, sort);
            t.add(row.clone(), sort);
        }
    }
}
fn drain(t: &BTree, sort: &Sort) -> Vec<OwnedRow> {
    let mut c = t.values_from(None, true, sort);
    let mut v = Vec::new();
    while let Some(r) = c.next_row() {
        v.push(r.to_owned_row());
    }
    v
}

/// Build a fresh source→pipeline→view over `rows`, hydrate, return (graph, view).
fn fresh_view(rows: Vec<OwnedRow>) -> (Graph, NodeId) {
    let mut g = Graph::new();
    let schema = source_schema();
    let src = g.add_source(schema.clone(), rows);
    let resolve = |t: &str| (t == "issue").then(|| (src, schema.clone()));
    let top = build_pipeline(&mut g, &query_ast(), &resolve).unwrap();
    let view = g.add_view(top, schema.clone().into_schema());
    g.wire_single(top, view);
    g.hydrate(view);
    (g, view)
}

// ---------------------------------------------------------------------------
// The harness
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct Pending {
    mid: u64,
    changes: Vec<SourceChange>,
}

/// Report of one reconcile cycle, for assertions.
struct CycleReport {
    rewind: Vec<SourceChange>,
    dropped: usize,
    replayed: usize,
}

struct Harness {
    g: Graph,
    src: NodeId,
    view: NodeId,
    /// Mirror of the live source's primary tree (the design's `head`). Maintained
    /// via the real COW fork lineage so `structural_diff` sees shared structure.
    head: BTree,
    /// Authoritative baseline (the design's `sync`).
    sync: BTree,
    pending: Vec<Pending>,
    sort: Sort,
}

impl Harness {
    fn new(initial: Vec<OwnedRow>) -> Harness {
        let sort = primary_sort();
        let mut g = Graph::new();
        let schema = source_schema();
        let src = g.add_source(schema.clone(), initial.clone());
        let resolve = |t: &str| (t == "issue").then(|| (src, schema.clone()));
        let top = build_pipeline(&mut g, &query_ast(), &resolve).unwrap();
        let view = g.add_view(top, schema.clone().into_schema());
        g.wire_single(top, view);
        g.hydrate(view);
        let head = bt_from_rows(&initial, &sort);
        let sync = head.fork();
        Harness {
            g,
            src,
            view,
            head,
            sync,
            pending: Vec::new(),
            sort,
        }
    }

    /// Apply a change to `head` AND the live pipeline (they stay identical).
    fn push_head(&mut self, c: &SourceChange) {
        apply_to_bt(&mut self.head, c, &self.sort);
        self.g.source_push(self.src, c.clone());
    }

    /// An optimistic write between server batches (the trivial case, §1.3): run it
    /// against `head`, push through the live pipeline, record it as pending.
    /// Nothing touches `sync`.
    fn optimistic_write(&mut self, mid: u64, changes: Vec<SourceChange>) {
        for c in &changes {
            apply_to_bt(&mut self.head, c, &self.sort);
            self.g.source_push(self.src, c.clone());
        }
        self.pending.push(Pending { mid, changes });
    }

    /// The §1.3 reconcile cycle on a server normalized batch.
    fn server_batch(&mut self, d: Vec<SourceChange>, lmid: u64, rejected: &[u64]) -> CycleReport {
        // 1. S' = fork(sync) + apply(D) — cheap transient authoritative-at-lmid.
        let mut s_prime = self.sync.fork();
        for c in &d {
            apply_to_bt(&mut s_prime, c, &self.sort);
        }

        // 2. rewindDiff = structuralDiff(head, S'): one delta that simultaneously
        //    removes the optimistic layer and folds in the server changes. Correct
        //    predictions appear in BOTH head and S' → omitted (zero churn, §3.3).
        let rewind = self.head.structural_diff(&s_prime, &self.sort);

        // 3. Push the rewind through the live pipeline → its source input is now S'.
        for c in &rewind {
            self.push_head(c);
        }
        // head == S' now.

        // 4. Re-fork: newSync = fork(head at S') REPLACES sync. Resets structure
        //    sharing to pointer-identical (GC of COW drift, §1.2).
        self.sync = self.head.fork();

        // 5. Rebase pending: drop confirmed (mid <= lmid) and rejected; re-apply
        //    the rest against the live pipeline → head = S' + pending'. (No mutators
        //    yet: "re-apply" replays the recorded raw row-ops; Slice 3 re-invokes
        //    the mutator functions instead.)
        let before = self.pending.len();
        self.pending
            .retain(|p| p.mid > lmid && !rejected.contains(&p.mid));
        let dropped = before - self.pending.len();
        let replay: Vec<SourceChange> = self
            .pending
            .iter()
            .flat_map(|p| p.changes.iter().cloned())
            .collect();
        let replayed = replay.len();
        for c in &replay {
            self.push_head(c);
        }

        // (Slice 2: coalesce the rewind+replay stream and deliver once. Here we only
        //  assert the final reconciled state via the oracle.)
        CycleReport {
            rewind,
            dropped,
            replayed,
        }
    }

    fn view_rows(&self) -> Vec<(Vec<i64>, Vec<Vec<i64>>)> {
        self.g.dump_view_rows(self.view)
    }

    /// The Slice-1 oracle: a FRESH hydrate of `sync + pending'`. After a cycle the
    /// live `head` equals this logically; the assertion is incremental == scratch.
    fn oracle_view_rows(&self) -> Vec<(Vec<i64>, Vec<Vec<i64>>)> {
        let mut t = self.sync.fork();
        for p in &self.pending {
            for c in &p.changes {
                apply_to_bt(&mut t, c, &self.sort);
            }
        }
        let (g, view) = fresh_view(drain(&t, &self.sort));
        g.dump_view_rows(view)
    }

    fn assert_reconciled(&self, ctx: &str) {
        assert_eq!(
            self.view_rows(),
            self.oracle_view_rows(),
            "live view != fresh-hydrate(sync + pending') ({ctx})"
        );
    }

    /// §1.2 invariant for ADD-only pending: `head` and `sync` diverge by exactly
    /// the still-pending optimistic keys, and ONLY by adds.
    fn assert_divergence_is_pending_adds(&self, ctx: &str) {
        let diff = self.sync.structural_diff(&self.head, &self.sort);
        let mut diverged: Vec<i64> = diff
            .iter()
            .map(|c| match c {
                SourceChange::Add(r) => key_of(r),
                other => panic!("divergence must be add-only, got {other:?} ({ctx})"),
            })
            .collect();
        diverged.sort();
        let mut pending: Vec<i64> = self
            .pending
            .iter()
            .flat_map(|p| {
                p.changes.iter().map(|c| match c {
                    SourceChange::Add(r) => key_of(r),
                    other => panic!("expected add-only pending, got {other:?}"),
                })
            })
            .collect();
        pending.sort();
        assert_eq!(
            diverged, pending,
            "head/sync must diverge by exactly the pending adds ({ctx})"
        );
    }
}

// -- deterministic PRNG --
struct Lcg(u64);
impl Lcg {
    fn new(s: u64) -> Lcg {
        Lcg(s)
    }
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 16
    }
    fn range(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

// ---------------------------------------------------------------------------
// Explicit scenarios
// ---------------------------------------------------------------------------

#[test]
fn accurate_optimism_is_zero_churn() {
    // Optimistically create issue 100 (in view: score 60). The server then confirms
    // the SAME row. The rewind diff must be EMPTY — the prediction was right.
    let mut h = Harness::new(vec![irow(1, 9, 70), irow(2, 9, 10)]);
    assert_eq!(h.view_rows(), vec![(vec![1, 9, 70], vec![])]); // only score>50

    h.optimistic_write(1, vec![SourceChange::Add(irow(100, 9, 60))]);
    let before = h.view_rows();
    assert_eq!(
        before,
        vec![(vec![1, 9, 70], vec![]), (vec![100, 9, 60], vec![])]
    );

    let rep = h.server_batch(vec![SourceChange::Add(irow(100, 9, 60))], 1, &[]);
    assert!(
        rep.rewind.is_empty(),
        "confirmed prediction must produce zero rewind churn, got {:?}",
        rep.rewind
    );
    assert_eq!(rep.dropped, 1, "the confirmed mutation is dropped");
    assert_eq!(rep.replayed, 0);
    assert_eq!(h.view_rows(), before, "view unchanged across the batch");
    h.assert_reconciled("accurate optimism");
    assert!(h.pending.is_empty());
}

#[test]
fn concurrent_server_change_folds_in() {
    // No pending. A server add to a NEW row (in view) folds into head via the
    // rewind diff.
    let mut h = Harness::new(vec![irow(1, 9, 70)]);
    let rep = h.server_batch(vec![SourceChange::Add(irow(2, 9, 80))], 0, &[]);
    assert_eq!(rep.rewind.len(), 1, "the new server row is the only change");
    assert_eq!(
        h.view_rows(),
        vec![(vec![1, 9, 70], vec![]), (vec![2, 9, 80], vec![])]
    );
    h.assert_reconciled("concurrent server add");
}

#[test]
fn still_pending_optimistic_write_survives() {
    // Optimistic create 300 (mid 5). A server batch touches a DIFFERENT row and does
    // NOT confirm mid 5 (lmid 0). The rewind removes 300 (head-only) and folds the
    // server's 200; the replay re-adds 300. Final view has both (the remove→re-add
    // flicker is what Slice 2 coalesces away).
    let mut h = Harness::new(vec![irow(1, 9, 99)]);
    h.optimistic_write(5, vec![SourceChange::Add(irow(300, 9, 80))]);
    let rep = h.server_batch(vec![SourceChange::Add(irow(200, 9, 70))], 0, &[]);

    // rewind = remove(300) + add(200) (descending order; just check membership).
    let keys: Vec<i64> = rep.rewind.iter().map(|c| key_of(c.row())).collect();
    assert!(
        keys.contains(&300) && keys.contains(&200),
        "rewind {keys:?}"
    );
    assert_eq!(rep.dropped, 0, "mid 5 not yet confirmed");
    assert_eq!(rep.replayed, 1, "mid 5 replayed");

    assert_eq!(
        h.view_rows(),
        vec![
            (vec![1, 9, 99], vec![]),
            (vec![200, 9, 70], vec![]),
            (vec![300, 9, 80], vec![]),
        ]
    );
    h.assert_reconciled("still-pending survives");
    h.assert_divergence_is_pending_adds("still-pending survives");
}

#[test]
fn rejected_optimistic_write_snaps_back() {
    // Optimistic create 400 (mid 7). The server rejects it. It must vanish (snap-
    // back), and never be replayed.
    let mut h = Harness::new(vec![irow(1, 9, 99)]);
    h.optimistic_write(7, vec![SourceChange::Add(irow(400, 9, 90))]);
    assert_eq!(h.view_rows().len(), 2);

    let rep = h.server_batch(vec![], 7, &[7]);
    assert_eq!(rep.dropped, 1);
    assert_eq!(rep.replayed, 0);
    let keys: Vec<i64> = rep.rewind.iter().map(|c| key_of(c.row())).collect();
    assert_eq!(keys, vec![400], "rewind removes the rejected row");
    assert_eq!(h.view_rows(), vec![(vec![1, 9, 99], vec![])]);
    h.assert_reconciled("rejection snap-back");
    assert!(h.pending.is_empty());
}

#[test]
fn optimistic_edit_and_remove_mix() {
    // Client-owned rows 100,101 (server never touches them). Optimistically EDIT 100
    // (raise score into view) and REMOVE 101; both stay pending. The server edits its
    // own row 1 and removes row 2. The reconcile must keep the optimistic edit/remove
    // (replayed) and fold the server changes.
    let mut h = Harness::new(vec![
        irow(1, 9, 60),   // in view
        irow(2, 9, 55),   // in view, server will remove
        irow(100, 1, 10), // out of view; optimistic edit raises it in
        irow(101, 1, 80), // in view; optimistic remove drops it
    ]);
    assert_eq!(
        h.view_rows(),
        vec![
            (vec![1, 9, 60], vec![]),
            (vec![2, 9, 55], vec![]),
            (vec![101, 1, 80], vec![]),
        ]
    );

    h.optimistic_write(
        10,
        vec![SourceChange::Edit {
            old: irow(100, 1, 10),
            row: irow(100, 1, 90),
        }],
    );
    h.optimistic_write(11, vec![SourceChange::Remove(irow(101, 1, 80))]);

    // Server: edit row 1 (score 60→51, stays in view), remove row 2. lmid 0 (no
    // optimistic confirmed). Keys disjoint from the client-owned 100/101.
    let rep = h.server_batch(
        vec![
            SourceChange::Edit {
                old: irow(1, 9, 60),
                row: irow(1, 9, 51),
            },
            SourceChange::Remove(irow(2, 9, 55)),
        ],
        0,
        &[],
    );
    assert_eq!(rep.dropped, 0);
    assert_eq!(rep.replayed, 2, "both optimistic ops replayed");

    assert_eq!(
        h.view_rows(),
        vec![
            (vec![1, 9, 51], vec![]), // server edit
            (vec![100, 1, 90], vec![]), // optimistic edit raised it in
                                      // 2 removed by server; 101 removed optimistically
        ]
    );
    h.assert_reconciled("edit/remove mix");
}

// ---------------------------------------------------------------------------
// Randomized rounds — incremental reconcile == fresh hydrate, every round
// ---------------------------------------------------------------------------

#[test]
fn randomized_rounds_match_fresh_hydrate() {
    // Discipline that keeps raw replay conflict-free (Slice 1 has no mutators):
    //  - optimistic writes are ADDS with globally-unique keys (>= 1_000_000),
    //  - server "own" changes live in the disjoint range [0, 1000),
    //  - the server confirms a PREFIX of pending mids (their adds appear in D and
    //    lmid covers them → dropped, never replayed),
    //  - so a still-pending key is never touched by D → no replay conflict.
    // The oracle (live view == fresh hydrate of sync+pending') still catches any
    // rewind/replay/refork bug regardless of how D is generated.
    for seed in 0..24u64 {
        let mut rng = Lcg::new(0x0B71_0000_u64.wrapping_add(seed.wrapping_mul(0x9E37_79B9)));
        // Random server-owned initial rows in [0,1000).
        let mut init = Vec::new();
        let mut present: std::collections::BTreeSet<i64> = std::collections::BTreeSet::new();
        for _ in 0..(rng.range(40)) {
            let k = rng.range(1000) as i64;
            if present.insert(k) {
                init.push(irow(k, rng.range(5) as i64, rng.range(100) as i64));
            }
        }
        let mut h = Harness::new(init);
        h.assert_reconciled(&format!("seed {seed} init"));

        let mut next_mid: u64 = 1;
        let mut next_opt_key: i64 = 1_000_000;
        let mut lmid: u64 = 0;

        for round in 0..8 {
            // A few optimistic creates (unique keys).
            for _ in 0..(rng.range(4)) {
                let k = next_opt_key;
                next_opt_key += 1;
                let mid = next_mid;
                next_mid += 1;
                h.optimistic_write(
                    mid,
                    vec![SourceChange::Add(irow(k, 7, rng.range(100) as i64))],
                );
            }

            // Build the server delta D against the CURRENT authoritative (== sync).
            let mut d: Vec<SourceChange> = Vec::new();
            let server_rows: Vec<OwnedRow> = drain(&h.sync, &h.sort)
                .into_iter()
                .filter(|r| key_of(r) < 1000)
                .collect();

            // Server own changes: an add, plus maybe an edit/remove of an existing row.
            let nk = rng.range(1000) as i64;
            if !present.contains(&nk) {
                present.insert(nk);
                d.push(SourceChange::Add(irow(nk, 3, rng.range(100) as i64)));
            }
            if !server_rows.is_empty() {
                let victim = server_rows[rng.range(server_rows.len() as u64) as usize].clone();
                if rng.range(2) == 0 {
                    let new_score = rng.range(100) as i64;
                    d.push(SourceChange::Edit {
                        old: victim.clone(),
                        row: irow(key_of(&victim), 4, new_score),
                    });
                } else {
                    present.remove(&key_of(&victim));
                    d.push(SourceChange::Remove(victim));
                }
            }

            // Confirm a prefix of pending: include their adds in D, advance lmid.
            let confirm = if h.pending.is_empty() {
                0
            } else {
                rng.range(h.pending.len() as u64 + 1) as usize
            };
            for p in h.pending.iter().take(confirm) {
                lmid = lmid.max(p.mid);
                d.extend(p.changes.iter().cloned());
            }

            h.server_batch(d, lmid, &[]);
            h.assert_reconciled(&format!("seed {seed} round {round}"));
            h.assert_divergence_is_pending_adds(&format!("seed {seed} round {round}"));
        }
    }
}
