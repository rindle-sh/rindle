//! Slice 3 of `OPTIMISTIC-WRITES-DESIGN.md`: custom mutators + `MutationTx`, with
//! rebase by RE-INVOCATION (§4).
//!
//! A mutator is a function registered by name, run against an abstract write
//! handle (`MutationTx` over the base table: `get` / `insert` / `update` /
//! `delete`). The pending stack stores **`(mid, name, args)` — not the row ops a
//! mutator produced**. On a server batch, rebase (step 5) **re-invokes** each
//! still-pending mutator against the new authoritative base, in `mid` order — it
//! does NOT replay recorded effects. That is the only correct rebase for
//! read-dependent mutators (`incrementScore` reads the current value before it
//! writes): an optimistic `+5` computed on a stale `10` must, after the server
//! concurrently sets `100`, become `105` — not the recorded `15`.
//!
//! Determinism (§5) is by construction: `MutationTx` exposes no clock / RNG / I/O,
//! only reads of the current base and `args`.
//!
//! Delivers RAW (the §3 `Coalescer` was removed); reuses the row helpers + the
//! pk-keyed `DeliveredView` from `optimistic_support`. The view settles order- and
//! coalescing-independently, so `assert_matches_engine` is the correctness oracle.

mod optimistic_support;
use optimistic_support::*;

use std::collections::{BTreeMap, HashMap};

use rindle::btree::BTree;
use rindle::value::{OwnedRow, Sort};
use rindle::SourceChange;

// ---------------------------------------------------------------------------
// MutationTx — the abstract write handle a mutator runs against
// ---------------------------------------------------------------------------

/// A client write transaction over the head source tree. Reads the CURRENT base
/// (so read-dependent mutators see prior writes in this txn / the rebased base),
/// and records each `insert`/`update`/`delete` as the [`SourceChange`] it implies.
/// The recorded changes drive both the live view and the coalescer; the tree
/// mutation is what makes a subsequent read in the same mutator see the write.
struct MutationTx<'a> {
    head: &'a mut BTree,
    sort: &'a Sort,
    emitted: Vec<SourceChange>,
}

impl MutationTx<'_> {
    /// Current row for `id`, or `None`. (pk = col 0; the probe's other cols are
    /// ignored by the pk-only sort.)
    fn get(&self, id: i64) -> Option<OwnedRow> {
        self.head.get(&irow(id, 0, 0), self.sort).cloned()
    }
    fn insert(&mut self, r: OwnedRow) {
        let newly = self.head.add(r.clone(), self.sort);
        debug_assert!(newly, "insert of an existing pk {}", key_of(&r));
        self.emitted.push(SourceChange::Add(r));
    }
    fn update(&mut self, r: OwnedRow) {
        let old = self.get(key_of(&r)).expect("update of a missing row");
        self.head.delete(&old, self.sort);
        self.head.add(r.clone(), self.sort);
        self.emitted.push(SourceChange::Edit { old, row: r });
    }
    fn delete(&mut self, id: i64) {
        if let Some(old) = self.get(id) {
            self.head.delete(&old, self.sort);
            self.emitted.push(SourceChange::Remove(old));
        }
    }
}

// ---------------------------------------------------------------------------
// The registry of mutator functions (the SAME shape a server registry would use)
// ---------------------------------------------------------------------------

type Args = Vec<i64>;
type Mutator = Box<dyn Fn(&mut MutationTx, &[i64])>;

fn registry() -> HashMap<String, Mutator> {
    let mut m: HashMap<String, Mutator> = HashMap::new();
    // createIssue(id, owner, score) — not read-dependent.
    m.insert(
        "createIssue".into(),
        Box::new(|tx, a| tx.insert(irow(a[0], a[1], a[2]))),
    );
    // incrementScore(id, delta) — READ-DEPENDENT: reads the current score, +delta.
    m.insert(
        "incrementScore".into(),
        Box::new(|tx, a| {
            if let Some(cur) = tx.get(a[0]) {
                tx.update(irow(a[0], col(&cur, 1), col(&cur, 2) + a[1]));
            }
        }),
    );
    // setOwner(id, owner) — read-dependent only on existence; preserves score.
    m.insert(
        "setOwner".into(),
        Box::new(|tx, a| {
            if let Some(cur) = tx.get(a[0]) {
                tx.update(irow(a[0], a[1], col(&cur, 2)));
            }
        }),
    );
    m.insert("deleteIssue".into(), Box::new(|tx, a| tx.delete(a[0])));
    m
}

/// Run a mutator by name against `head`, returning the changes it produced.
/// (Free fn so the harness can pass disjoint field borrows: `&registry`,
/// `&mut head`, `&sort`.)
fn invoke(
    reg: &HashMap<String, Mutator>,
    head: &mut BTree,
    sort: &Sort,
    name: &str,
    args: &[i64],
) -> Vec<SourceChange> {
    let m = reg
        .get(name)
        .unwrap_or_else(|| panic!("unknown mutator {name}"));
    let mut tx = MutationTx {
        head,
        sort,
        emitted: Vec::new(),
    };
    m(&mut tx, args);
    tx.emitted
}

// ---------------------------------------------------------------------------
// The harness
// ---------------------------------------------------------------------------

struct Pending {
    mid: u64,
    name: String,
    args: Args,
}

struct Harness {
    head: BTree,
    sync: BTree,
    sort: Sort,
    pending: Vec<Pending>,
    registry: HashMap<String, Mutator>,
    delivered: BTreeMap<i64, OwnedRow>,
    notifications: usize,
}

impl Harness {
    fn new(initial: Vec<OwnedRow>) -> Harness {
        let sort = primary_sort();
        let mut head = BTree::new();
        let mut delivered = BTreeMap::new();
        for r in &initial {
            head.add(r.clone(), &sort);
            delivered.insert(key_of(r), r.clone());
        }
        let sync = head.fork();
        Harness {
            head,
            sync,
            sort,
            pending: Vec::new(),
            registry: registry(),
            delivered,
            notifications: 0,
        }
    }

    fn deliver(&mut self, batch: &[Delivered]) {
        if batch.is_empty() {
            return;
        }
        for d in batch {
            match d {
                Delivered::Add(r) => {
                    self.delivered.insert(key_of(r), r.clone());
                }
                Delivered::Remove(r) => {
                    self.delivered.remove(&key_of(r));
                }
                Delivered::Edit { new, .. } => {
                    self.delivered.insert(key_of(new), new.clone());
                }
            }
        }
        self.notifications += 1;
    }

    /// Optimistic mutation between batches: run it against `head` now (the
    /// prediction), deliver RAW (no coalescing — the §3 Coalescer was removed), and
    /// record `(mid, name, args)` — NOT its effects.
    fn mutate(&mut self, mid: u64, name: &str, args: Args) -> Vec<Delivered> {
        let emitted = invoke(&self.registry, &mut self.head, &self.sort, name, &args);
        let batch = raw_deliver(&emitted);
        self.deliver(&batch);
        self.pending.push(Pending {
            mid,
            name: name.into(),
            args,
        });
        batch
    }

    /// The §1.3 cycle, with rebase = RE-INVOCATION (§4). Delivery is RAW: the rewind
    /// half (head → S') followed by the replay half, in order — exactly what
    /// `Db::server_batch_end` now forwards. The pk-keyed `delivered` view settles to
    /// the same state regardless of order, so `assert_matches_engine` is the oracle.
    fn server_batch(
        &mut self,
        d: Vec<SourceChange>,
        lmid: u64,
        rejected: &[u64],
    ) -> Vec<Delivered> {
        let mut batch: Vec<Delivered> = Vec::new();

        // S' = fork(sync) + D; rewind head → S'.
        let mut s_prime = self.sync.fork();
        for c in &d {
            apply_to_bt(&mut s_prime, c, &self.sort);
        }
        let rewind = self.head.structural_diff(&s_prime, &self.sort);
        for c in &rewind {
            apply_to_bt(&mut self.head, c, &self.sort);
            batch.push(delivered_of(c));
        }
        self.sync = self.head.fork(); // re-fork (head == S')

        // Drop confirmed / rejected; RE-INVOKE the rest against the new base
        // (head, now at S'), in mid order — reading current values, not replaying.
        self.pending
            .retain(|p| p.mid > lmid && !rejected.contains(&p.mid));
        let to_replay: Vec<(String, Args)> = self
            .pending
            .iter()
            .map(|p| (p.name.clone(), p.args.clone()))
            .collect();
        for (name, args) in &to_replay {
            let emitted = invoke(&self.registry, &mut self.head, &self.sort, name, args);
            for c in &emitted {
                batch.push(delivered_of(c));
            }
        }

        self.deliver(&batch);
        batch
    }

    fn delivered_rows(&self) -> Vec<Vec<i64>> {
        self.delivered.values().map(as_ints).collect()
    }
    fn score(&self, id: i64) -> i64 {
        col(&self.delivered[&id], 2)
    }
    fn assert_matches_engine(&self, ctx: &str) {
        let want = engine_view_rows(drain(&self.head, &self.sort));
        assert_eq!(
            self.delivered_rows(),
            want,
            "delivered != engine view ({ctx})"
        );
    }
}

// ---------------------------------------------------------------------------
// The headline: read-dependent rebase by re-invocation
// ---------------------------------------------------------------------------

#[test]
fn read_dependent_mutator_rebases_onto_server_value() {
    let mut h = Harness::new(vec![irow(1, 9, 10)]);
    // Optimistic incrementScore(1, +5): reads 10, writes 15.
    h.mutate(1, "incrementScore", vec![1, 5]);
    assert_eq!(h.score(1), 15);

    // The server concurrently sets score to 100 (a different write). mid 1 is NOT
    // confirmed (lmid 0), so it stays pending and must be RE-INVOKED.
    let batch = h.server_batch(
        vec![SourceChange::Edit {
            old: irow(1, 9, 10),
            row: irow(1, 9, 100),
        }],
        0,
        &[],
    );

    // Re-invocation computes +5 on the AUTHORITATIVE 100 → 105. Replaying the
    // recorded effect (edit 10→15) would have given 15 and silently dropped the
    // server's change — the whole reason pending stores (name,args), not effects.
    assert_eq!(
        h.score(1),
        105,
        "must rebase onto the server's value, not replay"
    );
    // Raw delivery is the cycle's rewind edit (15→100) followed by the re-invocation
    // edit (100→105); applied in order the pk-keyed view settles to 105.
    assert_eq!(delivered_keys(&batch), vec![('E', 1), ('E', 1)]);
    h.assert_matches_engine("reinvocation");
}

#[test]
fn pending_mutators_rebase_in_mid_order_and_accumulate() {
    let mut h = Harness::new(vec![irow(1, 9, 10)]);
    h.mutate(1, "incrementScore", vec![1, 5]); // 10 -> 15
    h.mutate(2, "incrementScore", vec![1, 3]); // 15 -> 18
    assert_eq!(h.score(1), 18);

    // Server sets 100; neither confirmed. Re-invoke mid 1 (100→105) then mid 2
    // (105→108): order and accumulation preserved.
    h.server_batch(
        vec![SourceChange::Edit {
            old: irow(1, 9, 10),
            row: irow(1, 9, 100),
        }],
        0,
        &[],
    );
    assert_eq!(h.score(1), 108);
    h.assert_matches_engine("ordered accumulate");
}

#[test]
fn confirmed_mutator_is_dropped_and_zero_churn() {
    let mut h = Harness::new(vec![irow(1, 9, 10)]);
    h.mutate(1, "incrementScore", vec![1, 5]); // 10 -> 15
    let notes = h.notifications;
    // Server applies the mutation authoritatively (same effect) and confirms mid 1.
    let batch = h.server_batch(
        vec![SourceChange::Edit {
            old: irow(1, 9, 10),
            row: irow(1, 9, 15),
        }],
        1,
        &[],
    );
    assert!(batch.is_empty(), "confirmed mutator => zero churn");
    assert_eq!(h.notifications, notes, "no notification");
    assert!(
        h.pending.is_empty(),
        "confirmed mutation dropped from pending"
    );
    assert_eq!(h.score(1), 15);
    h.assert_matches_engine("confirmed");
}

#[test]
fn create_then_delete_mutators_net_out() {
    let mut h = Harness::new(vec![]);
    h.mutate(1, "createIssue", vec![10, 1, 60]);
    assert_eq!(h.delivered_rows(), vec![vec![10, 1, 60]]);
    h.mutate(2, "deleteIssue", vec![10]);
    assert!(h.delivered_rows().is_empty());

    // Neither confirmed; server adds 20. Raw delivery is the server's add(20) plus the
    // re-invoked create(10) + delete(10); applied in order the pk-keyed view settles
    // with 10 absent (created then removed) and only 20 present.
    let batch = h.server_batch(vec![SourceChange::Add(irow(20, 2, 77))], 0, &[]);
    assert_eq!(
        delivered_keys(&batch),
        vec![('A', 20), ('A', 10), ('R', 10)]
    );
    assert_eq!(h.delivered_rows(), vec![vec![20, 2, 77]]);
    h.assert_matches_engine("create+delete net out");
}

#[test]
fn rejected_mutator_snaps_back() {
    let mut h = Harness::new(vec![irow(1, 9, 10)]);
    h.mutate(7, "incrementScore", vec![1, 5]); // optimistic 10 -> 15
    assert_eq!(h.score(1), 15);
    // Server rejects mid 7 (e.g. permission denied) and changes nothing.
    let batch = h.server_batch(vec![], 7, &[7]);
    // Snap back to the authoritative 10.
    assert_eq!(delivered_keys(&batch), vec![('E', 1)]);
    assert_eq!(h.score(1), 10);
    assert!(h.pending.is_empty());
    h.assert_matches_engine("rejected");
}

// ---------------------------------------------------------------------------
// Randomized: read-dependent rebase stays consistent + minimal over many rounds
// ---------------------------------------------------------------------------

#[test]
fn randomized_reinvocation_matches_engine() {
    // Client mutators on client-owned ids (>= 1_000_000); server own-changes in
    // [0,1000). Confirm a PREFIX of pending each round by running those mutators
    // against the authoritative base (a fork of sync) to produce D — exactly what
    // a server sharing the registry would do. Each round: the coalesced consumer
    // view == the engine view, and value-unchanged rows keep their identity.
    for seed in 0..16u64 {
        let mut rng = Lcg::new(0x3EED_0003_u64.wrapping_add(seed.wrapping_mul(0x9E37_79B9)));
        let mut init = Vec::new();
        let mut present: std::collections::BTreeSet<i64> = Default::default();
        for _ in 0..rng.range(20) {
            let k = rng.range(1000) as i64;
            if present.insert(k) {
                init.push(irow(k, rng.range(5) as i64, rng.range(100) as i64));
            }
        }
        let mut h = Harness::new(init);
        let mut next_mid = 1u64;
        let mut next_key = 1_000_000i64;
        let mut lmid = 0u64;

        for _round in 0..7 {
            // Optimistic mutations.
            for _ in 0..rng.range(4) {
                let client_ids: Vec<i64> = drain(&h.head, &h.sort)
                    .into_iter()
                    .map(|r| key_of(&r))
                    .filter(|&k| k >= 1_000_000)
                    .collect();
                // Bias toward create; otherwise a read-dependent op on an existing
                // client row.
                if client_ids.is_empty() || rng.range(2) == 0 {
                    let k = next_key;
                    next_key += 1;
                    h.mutate(next_mid, "createIssue", vec![k, 7, rng.range(100) as i64]);
                } else {
                    let id = client_ids[rng.range(client_ids.len() as u64) as usize];
                    match rng.range(3) {
                        0 => h.mutate(
                            next_mid,
                            "incrementScore",
                            vec![id, 1 + rng.range(20) as i64],
                        ),
                        1 => h.mutate(next_mid, "setOwner", vec![id, rng.range(9) as i64]),
                        _ => h.mutate(next_mid, "deleteIssue", vec![id]),
                    };
                }
                next_mid += 1;
            }

            // Server own-changes (disjoint id range).
            let mut d: Vec<SourceChange> = Vec::new();
            let nk = rng.range(1000) as i64;
            if present.insert(nk) {
                d.push(SourceChange::Add(irow(nk, 3, rng.range(100) as i64)));
            }
            let server_rows: Vec<OwnedRow> = drain(&h.sync, &h.sort)
                .into_iter()
                .filter(|r| key_of(r) < 1000)
                .collect();
            if !server_rows.is_empty() {
                let v = server_rows[rng.range(server_rows.len() as u64) as usize].clone();
                if rng.range(2) == 0 {
                    d.push(SourceChange::Edit {
                        old: v.clone(),
                        row: irow(key_of(&v), 4, rng.range(100) as i64),
                    });
                } else {
                    present.remove(&key_of(&v));
                    d.push(SourceChange::Remove(v));
                }
            }

            // Confirm a prefix: a registry-sharing server applies those mutators to
            // its authoritative state to get their effects (run against fork(sync),
            // in order) and folds them into D, advancing lmid past them.
            let k = rng.range(h.pending.len() as u64 + 1) as usize;
            let mut auth = h.sync.fork();
            for p in h.pending.iter().take(k) {
                let e = invoke(&h.registry, &mut auth, &h.sort, &p.name, &p.args);
                d.extend(e);
                lmid = lmid.max(p.mid);
            }

            h.server_batch(d, lmid, &[]);
            h.assert_matches_engine(&format!("seed {seed}"));
            // NOTE: raw delivery does NOT preserve per-row Arc identity for a
            // still-pending row across an unrelated poke — the rewind removes it and
            // the re-invocation re-adds a fresh row. The §3 Coalescer used to net that
            // remove+add away (preserving identity); restoring it is the view-level
            // Step 2 of the coalescer removal. Until then we assert only the settled
            // view (above), not Arc::ptr_eq stability.
        }
    }
}
