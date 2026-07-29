//! Slice 0 of `OPTIMISTIC-WRITES-DESIGN.md`: the `structuralDiff` primitive.
//!
//! Three test layers, all first-class (design §10 slice 0):
//!
//! 1. **Correctness oracle** — differential against a linear-merge reference over
//!    a COW-fork corpus (deep trees → multi-level cursor walk-back; independent
//!    trees → no pointer help; both diff directions; empty / one-empty edges).
//! 2. **Identity-disabled equivalence** — the same diff with `use_identity =
//!    false` emits the IDENTICAL change set, proving the `ptr_eq` skips are a pure
//!    optimization (§2.2.1).
//! 3. **Minimal-operations gate** — `DiffStats` counters show `fork + N
//!    point-mutations` does O(N·height) work and emits exactly N changes, where
//!    the identity-off run scans the whole tree (§2.2.2). A silent degradation to
//!    O(tree) is invisible to correctness alone, so this is its own gate.

use std::cmp::Ordering;

use rindle::btree::{BTree, DiffSink, DiffStats, RowRef, RowStream};
use rindle::change::SourceChange;
use rindle::value::{compare_rows, compare_values, owned_row as row, OwnedRow, OwnedValue, Sort};

use OwnedValue::Int;

// -- tiny deterministic PRNG (zero-dep, mirrors btree.rs tests) --
struct Lcg(u64);
impl Lcg {
    fn new(seed: u64) -> Lcg {
        Lcg(seed)
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

/// A row: col 0 is the key, cols 1.. are payload (so a payload-only change under a
/// key-only sort is an EDIT).
fn mk2(k: i64, p: i64) -> OwnedRow {
    row(vec![Int(k), Int(p)])
}
fn mk3(k: i64, p: i64, q: i64) -> OwnedRow {
    row(vec![Int(k), Int(p), Int(q)])
}

fn key_of(r: &OwnedRow) -> i64 {
    match &r.col(0).to_owned() {
        Int(v) => *v,
        other => panic!("expected Int key, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// The linear-merge oracle + canonicalized comparison
// ---------------------------------------------------------------------------

fn drain_rows(t: &BTree, sort: &Sort) -> Vec<OwnedRow> {
    let mut c = t.values_from(None, true, sort);
    let mut v = Vec::new();
    while let Some(r) = c.next_row() {
        v.push(r.to_owned_row());
    }
    v
}

/// Full-width row equality (`null == null`) — the oracle's edit detector, kept
/// independent of the implementation's `full_row_equal`.
fn rows_equal(a: &OwnedRow, b: &OwnedRow) -> bool {
    a.len() == b.len()
        && a.cells()
            .zip(b.cells())
            .all(|(x, y)| compare_values(x, y) == Ordering::Equal)
}

/// O(|a|+|b|) merge diff over the two fully-materialized sorted trees — obviously
/// correct, the differential reference for the cursor diff.
fn oracle_diff(a: &BTree, b: &BTree, sort: &Sort) -> Vec<SourceChange> {
    let ar = drain_rows(a, sort);
    let br = drain_rows(b, sort);
    let mut out = Vec::new();
    let (mut i, mut j) = (0usize, 0usize);
    while i < ar.len() && j < br.len() {
        match compare_rows(sort, &ar[i], &br[j]) {
            Ordering::Less => {
                out.push(SourceChange::Remove(ar[i].clone()));
                i += 1;
            }
            Ordering::Greater => {
                out.push(SourceChange::Add(br[j].clone()));
                j += 1;
            }
            Ordering::Equal => {
                if !rows_equal(&ar[i], &br[j]) {
                    out.push(SourceChange::Edit {
                        old: ar[i].clone(),
                        row: br[j].clone(),
                    });
                }
                i += 1;
                j += 1;
            }
        }
    }
    while i < ar.len() {
        out.push(SourceChange::Remove(ar[i].clone()));
        i += 1;
    }
    while j < br.len() {
        out.push(SourceChange::Add(br[j].clone()));
        j += 1;
    }
    out
}

fn change_key(c: &SourceChange) -> &OwnedRow {
    match c {
        SourceChange::Add(r) | SourceChange::Remove(r) => r,
        SourceChange::Edit { row, .. } => row,
    }
}
fn change_repr(c: &SourceChange) -> String {
    match c {
        SourceChange::Add(r) => format!("A {r:?}"),
        SourceChange::Remove(r) => format!("R {r:?}"),
        SourceChange::Edit { old, row } => format!("E {old:?} => {row:?}"),
    }
}
/// Every emitted change has a DISTINCT sort key (same-key differences are folded
/// into a single Edit), so ordering by sort key is a total order — canonicalizing
/// away the diff's descending emission order vs. the oracle's ascending one.
/// `SourceChange`/`OwnedValue` deliberately do not derive `PartialEq` (wrong
/// comparator hazard, value.rs), so we compare structural `Debug` reprs.
fn canon(mut v: Vec<SourceChange>, sort: &Sort) -> Vec<String> {
    v.sort_by(|a, b| compare_rows(sort, change_key(a), change_key(b)));
    v.into_iter().map(|c| change_repr(&c)).collect()
}

fn assert_diff_matches_oracle(a: &BTree, b: &BTree, sort: &Sort, ctx: &str) {
    let got = a.structural_diff(b, sort);
    let want = oracle_diff(a, b, sort);
    assert_eq!(
        canon(got, sort),
        canon(want, sort),
        "structural_diff != oracle ({ctx})"
    );
}

// ---------------------------------------------------------------------------
// Sinks
// ---------------------------------------------------------------------------

#[derive(Default)]
struct CollectSink(Vec<SourceChange>);
impl DiffSink for CollectSink {
    fn only_this(&mut self, r: &OwnedRow) {
        self.0.push(SourceChange::Remove(r.clone()));
    }
    fn only_other(&mut self, r: &OwnedRow) {
        self.0.push(SourceChange::Add(r.clone()));
    }
    fn edit(&mut self, this: &OwnedRow, other: &OwnedRow) {
        self.0.push(SourceChange::Edit {
            old: this.clone(),
            row: other.clone(),
        });
    }
}

#[derive(Default)]
struct CountSink {
    adds: u64,
    removes: u64,
    edits: u64,
}
impl DiffSink for CountSink {
    fn only_this(&mut self, _: &OwnedRow) {
        self.removes += 1;
    }
    fn only_other(&mut self, _: &OwnedRow) {
        self.adds += 1;
    }
    fn edit(&mut self, _: &OwnedRow, _: &OwnedRow) {
        self.edits += 1;
    }
}

fn diff_collect(a: &BTree, b: &BTree, sort: &Sort, use_identity: bool) -> Vec<SourceChange> {
    let mut sink = CollectSink::default();
    let mut stats = DiffStats::default();
    a.diff_visit(b, sort, use_identity, &mut stats, &mut sink);
    sink.0
}

// ---------------------------------------------------------------------------
// Tree builders
// ---------------------------------------------------------------------------

fn random_tree(rng: &mut Lcg, n: usize, key_space: i64, sort: &Sort) -> BTree {
    assert!(key_space as usize >= 2 * n + 4, "key space too tight");
    let mut t = BTree::new();
    while t.len() < n {
        let k = rng.range(key_space as u64) as i64;
        let p = rng.range(1000) as i64;
        t.add(mk2(k, p), sort);
    }
    t
}

/// Fork `base`, then apply `ops` random adds/deletes/edits (an edit = overwrite an
/// existing key's payload). Returns the mutated fork; `base` is untouched (COW).
fn mutate_fork(rng: &mut Lcg, base: &BTree, ops: usize, key_space: i64, sort: &Sort) -> BTree {
    let mut t = base.fork();
    for _ in 0..ops {
        let k = rng.range(key_space as u64) as i64;
        match rng.range(3) {
            0 => {
                t.add(mk2(k, rng.range(1000) as i64), sort);
            }
            1 => {
                t.delete(&mk2(k, 0), sort);
            }
            _ => {
                // Overwrite payload — an edit under a key-only sort.
                t.add(mk2(k, 1000 + rng.range(1000) as i64), sort);
            }
        }
    }
    t
}

// ---------------------------------------------------------------------------
// Layer 1 — correctness oracle
// ---------------------------------------------------------------------------

#[test]
fn edge_cases() {
    let sort: Sort = vec![(0, true)];
    let empty = BTree::new();
    assert!(empty.structural_diff(&empty, &sort).is_empty());

    let mut one = BTree::new();
    one.add(mk2(5, 1), &sort);
    // empty -> one : a single Add; one -> empty : a single Remove.
    assert_diff_matches_oracle(&empty, &one, &sort, "empty->one");
    assert_diff_matches_oracle(&one, &empty, &sort, "one->empty");
    // identical (fork, no change) -> nothing.
    assert!(one.fork().structural_diff(&one, &sort).is_empty());
    assert_diff_matches_oracle(&one, &one.fork(), &sort, "one->forked-one");
}

#[test]
fn forked_then_mutated_small_and_medium() {
    let sort: Sort = vec![(0, true)];
    for seed in 0..60u64 {
        let mut rng = Lcg::new(0xD1FF_0000 ^ seed.wrapping_mul(0x9E37_79B9));
        let n = (rng.range(250)) as usize; // 0..250 → heights 0 and 1
        let ks = (n as i64) * 3 + 8;
        let base = random_tree(&mut rng, n, ks, &sort);
        let ops = 1 + rng.range(40) as usize;
        let mutated = mutate_fork(&mut rng, &base, ops, ks, &sort);
        assert_diff_matches_oracle(&base, &mutated, &sort, &format!("seed {seed} fwd"));
        assert_diff_matches_oracle(&mutated, &base, &sort, &format!("seed {seed} rev"));
    }
}

#[test]
fn forked_then_mutated_deep_trees() {
    // > 4096 rows ⇒ height ≥ 2, the regime that exercises MULTI-LEVEL cursor
    // walk-back (the one place this port diverges from NIM's single-pop — see
    // btree.rs DiffCursor::step). If the truncation generalization were wrong,
    // these deep cases would diverge from the oracle.
    let sort: Sort = vec![(0, true)];
    for seed in 0..16u64 {
        let mut rng = Lcg::new(0xDEE9_F00D ^ seed.wrapping_mul(0x85EB_CA6B));
        let n = 5000 + rng.range(7000) as usize; // 5k..12k → height 2
        let ks = (n as i64) * 4 + 16;
        let base = random_tree(&mut rng, n, ks, &sort);
        let ops = 1 + rng.range(120) as usize;
        let mutated = mutate_fork(&mut rng, &base, ops, ks, &sort);
        assert_diff_matches_oracle(&base, &mutated, &sort, &format!("deep seed {seed} fwd"));
        assert_diff_matches_oracle(&mutated, &base, &sort, &format!("deep seed {seed} rev"));
    }
}

#[test]
fn independent_trees_no_sharing_and_height_mismatch() {
    // Two SEPARATELY-built trees share no structure, so the diff must be fully
    // value-correct with zero pointer help; differing sizes also exercise height
    // normalization (a height-1 tree vs a height-2 tree).
    let sort: Sort = vec![(0, true)];
    for seed in 0..40u64 {
        let mut rng = Lcg::new(0x12DE_45ED_u64.wrapping_add(seed.wrapping_mul(0xC2B2_AE35)));
        let na = rng.range(9000) as usize;
        let nb = rng.range(9000) as usize;
        let ks = 40_000;
        let a = random_tree(&mut rng, na, ks, &sort);
        let b = random_tree(&mut rng, nb, ks, &sort);
        assert_diff_matches_oracle(&a, &b, &sort, &format!("indep seed {seed} a->b"));
        assert_diff_matches_oracle(&b, &a, &sort, &format!("indep seed {seed} b->a"));
    }
}

#[test]
fn multi_column_sort_and_payload_edits() {
    // 3 columns. (a) key-only sort: payload changes surface as edits.
    // (b) full 3-col sort: the row IS the key, so a payload change is a
    //     remove+add, never an edit. Both regimes vs. the oracle.
    let key_only: Sort = vec![(0, true)];
    let full: Sort = vec![(0, true), (1, true), (2, true)];
    for seed in 0..30u64 {
        let mut rng = Lcg::new(0xC0D5 ^ seed.wrapping_mul(0x27D4_EB2F));
        for sort in [&key_only, &full] {
            let n = 200 + rng.range(6000) as usize;
            let mut a = BTree::new();
            while a.len() < n {
                let k = rng.range((n as u64) * 3) as i64;
                a.add(mk3(k, rng.range(50) as i64, rng.range(50) as i64), sort);
            }
            let mut b = a.fork();
            for _ in 0..(1 + rng.range(80)) {
                let k = rng.range((n as u64) * 3) as i64;
                match rng.range(3) {
                    0 => {
                        b.add(mk3(k, rng.range(50) as i64, rng.range(50) as i64), sort);
                    }
                    1 => {
                        b.delete(&mk3(k, rng.range(50) as i64, rng.range(50) as i64), sort);
                    }
                    _ => {
                        b.add(
                            mk3(k, 100 + rng.range(50) as i64, 100 + rng.range(50) as i64),
                            sort,
                        );
                    }
                }
            }
            assert_diff_matches_oracle(&a, &b, sort, &format!("mcol seed {seed} {sort:?}"));
            assert_diff_matches_oracle(&b, &a, sort, &format!("mcol seed {seed} rev {sort:?}"));
        }
    }
}

// ---------------------------------------------------------------------------
// Regression — adversarial-review HIGH #6: equal-key (multiset) runs
// ---------------------------------------------------------------------------

/// ADVERSARIAL REVIEW #6 — `diff_visit` paired equal-key positions asymmetrically: on an
/// `Equal` match the bottom-of-loop step advanced ONLY the `other` cursor, and the
/// `prev != Equal` emission gate then suppressed the next iteration, so a run of k≥2 rows
/// sharing one sort key was consumed unpaired. Diffing two VALUE-IDENTICAL multiset trees
/// emitted a spurious `Remove`. Multiset trees are constructible through the public API
/// (`from_sorted` accepts equal-key neighbours; bulk-load is multiset-tolerant), and pushed
/// through `OptimisticTables::rewind` the spurious `Remove` permanently diverges the source.
///
/// The canon()/oracle harness above can't catch it (its builders insert via `add`, which
/// overwrites equal keys, and `canon` assumes every change has a distinct sort key), so this
/// is a hand-built multiset case asserting an exact, distinct-key-free expected net.
#[test]
fn equal_key_run_diffs_without_spurious_remove() {
    let sort: Sort = vec![(0, true)]; // key-only sort → cols 1.. are payload

    // base holds DUPLICATE key 5 (5/1 and 5/2 differ only in payload) — a legal multiset built
    // from a pre-sorted input.
    let base = BTree::from_sorted(vec![mk2(4, 0), mk2(5, 1), mk2(5, 2)].into_iter(), &sort);
    assert_eq!(base.len(), 3, "from_sorted preserves the duplicate key");

    // head = fork + a point overwrite of key 4's payload (0 → 99). The two key-5 rows stay
    // Arc-shared with base; ONLY key 4 differs between the trees.
    let mut head = base.fork();
    head.add(mk2(4, 99), &sort);

    // head → base: the ONLY real difference is key 4 → exactly one Edit, no spurious Remove.
    let fwd: Vec<String> = head
        .structural_diff(&base, &sort)
        .iter()
        .map(change_repr)
        .collect();
    assert_eq!(
        fwd,
        vec!["E [Int(4), Int(99)] => [Int(4), Int(0)]".to_string()],
        "equal-key run was consumed unpaired → spurious Remove (got {fwd:?})"
    );

    // base → head: the reverse must also net to the single edit, not an Add.
    let rev: Vec<String> = base
        .structural_diff(&head, &sort)
        .iter()
        .map(change_repr)
        .collect();
    assert_eq!(
        rev,
        vec!["E [Int(4), Int(0)] => [Int(4), Int(99)]".to_string()],
        "reverse equal-key diff also nets to one edit (got {rev:?})"
    );
}

/// Identity-off must agree (the `ptr_eq` skips are a pure optimization — §2.2.1) even on a
/// multiset run, and a value-different duplicate key must surface as exactly one edit.
#[test]
fn equal_key_run_identity_off_and_value_edit() {
    let sort: Sort = vec![(0, true)];
    let base = BTree::from_sorted(vec![mk2(5, 1), mk2(5, 2), mk2(5, 3)].into_iter(), &sort);
    // Independent (no sharing) tree with the same three rows but the middle payload changed.
    let other = BTree::from_sorted(vec![mk2(5, 1), mk2(5, 9), mk2(5, 3)].into_iter(), &sort);

    let canon_set = |v: Vec<SourceChange>| {
        let mut s: Vec<String> = v.iter().map(change_repr).collect();
        s.sort();
        s
    };
    assert_eq!(
        canon_set(diff_collect(&base, &other, &sort, true)),
        canon_set(diff_collect(&base, &other, &sort, false)),
        "identity-on and identity-off must agree on a multiset run"
    );
    // Exactly one edit (5/2 → 5/9); the other two key-5 rows are unchanged.
    let off = diff_collect(&base, &other, &sort, false);
    assert_eq!(off.len(), 1, "one changed row in the run, got {off:?}");
    assert!(matches!(off[0], SourceChange::Edit { .. }));
}

// ---------------------------------------------------------------------------
// Layer 2 — identity-disabled equivalence (§2.2.1)
// ---------------------------------------------------------------------------

#[test]
fn identity_on_equals_identity_off() {
    let sort: Sort = vec![(0, true)];
    for seed in 0..40u64 {
        let mut rng = Lcg::new(0x1DE7_7177 ^ seed.wrapping_mul(0xD6E8_FEB8));
        // Mix of forked-and-mutated (lots of sharing) and independent (no sharing).
        let (a, b) = if seed % 2 == 0 {
            let n = 1000 + rng.range(8000) as usize;
            let ks = (n as i64) * 4 + 16;
            let base = random_tree(&mut rng, n, ks, &sort);
            let ops = 1 + rng.range(150) as usize;
            let m = mutate_fork(&mut rng, &base, ops, ks, &sort);
            (base, m)
        } else {
            let ks = 40_000;
            let na = rng.range(8000) as usize;
            let a = random_tree(&mut rng, na, ks, &sort);
            let nb = rng.range(8000) as usize;
            let b = random_tree(&mut rng, nb, ks, &sort);
            (a, b)
        };
        let on = diff_collect(&a, &b, &sort, true);
        let off = diff_collect(&a, &b, &sort, false);
        assert_eq!(
            canon(on, &sort),
            canon(off, &sort),
            "identity on != off, seed {seed} (skips must be a pure optimization)"
        );
    }
}

// ---------------------------------------------------------------------------
// Layer 3 — minimal-operations gate (§2.2.2)
// ---------------------------------------------------------------------------

#[test]
fn minimal_ops_gate_scattered_edits() {
    let sort: Sort = vec![(0, true)];
    let mut rng = Lcg::new(0x09C0_07ED);
    let base = random_tree(&mut rng, 8000, 100_000, &sort); // > 4096 rows ⇒ height ≥ 2
    let base_rows = drain_rows(&base, &sort);

    // N scattered EDITS (overwrite payload of N evenly-spread existing keys). Each
    // COW-copies one root→leaf path, leaving every other subtree pointer-shared.
    let n: usize = 16;
    let stride = base_rows.len() / (n + 1);
    let mut t = base.fork();
    for i in 1..=n {
        let k = key_of(&base_rows[i * stride]);
        t.add(mk2(k, 7_000_000 + i as i64), &sort);
    }

    // Identity ON: O(N·height) work, exactly N edits.
    let mut on = DiffStats::default();
    let mut on_sink = CountSink::default();
    base.diff_visit(&t, &sort, true, &mut on, &mut on_sink);
    assert_eq!(
        (on_sink.adds, on_sink.removes, on_sink.edits),
        (0, 0, n as u64),
        "expected exactly {n} edits"
    );

    // Identity OFF: identical emissions, but a full-tree scan.
    let mut off = DiffStats::default();
    let mut off_sink = CountSink::default();
    base.diff_visit(&t, &sort, false, &mut off, &mut off_sink);
    assert_eq!(
        (off_sink.adds, off_sink.removes, off_sink.edits),
        (0, 0, n as u64),
        "identity-off must emit the same changes"
    );

    eprintln!("minimal-ops gate: ON {on:?}\n                  OFF {off:?}");

    // The optimization fired, and only on the identity path.
    assert!(
        on.internal_node_skips >= 1,
        "subtree skip never fired: {on:?}"
    );
    assert_eq!(
        off.internal_node_skips, 0,
        "identity-off must not skip: {off:?}"
    );

    // Work tracks divergence (N·height), not data size.
    assert!(
        on.leaves_visited <= (n as u64) * 6,
        "identity-on visited too many leaves ({}); expected ~2N: {on:?}",
        on.leaves_visited
    );
    assert!(
        on.row_value_compares <= (n as u64) * 3,
        "identity-on did too many value compares ({}); expected ~N: {on:?}",
        on.row_value_compares
    );

    // ...and the off run is dramatically heavier (the degradation the gate exists
    // to make visible).
    assert!(
        on.leaves_visited * 4 <= off.leaves_visited,
        "identity-on ({}) should visit far fewer leaves than off ({})",
        on.leaves_visited,
        off.leaves_visited
    );
    assert!(
        off.row_value_compares >= 1000,
        "identity-off should value-compare the whole tree, got {}",
        off.row_value_compares
    );
}

#[test]
fn minimal_ops_gate_clustered_appends() {
    // N appends beyond the max key: all land in the rightmost leaves, so the diff
    // should touch only a handful of leaves regardless of tree size.
    let sort: Sort = vec![(0, true)];
    let mut rng = Lcg::new(0xA99E_4D00);
    let base = random_tree(&mut rng, 9000, 100_000, &sort);
    let n: usize = 20;
    let mut t = base.fork();
    let mut added = 0;
    while added < n {
        if t.add(mk2(200_000 + added as i64, 1), &sort) {
            added += 1;
        }
    }

    let mut on = DiffStats::default();
    let mut sink = CountSink::default();
    base.diff_visit(&t, &sort, true, &mut on, &mut sink);
    assert_eq!((sink.adds, sink.removes, sink.edits), (n as u64, 0, 0));
    assert!(on.internal_node_skips >= 1, "{on:?}");
    // Clustered ⇒ even tighter than scattered: a couple of rightmost leaves.
    assert!(
        on.leaves_visited <= 8,
        "clustered appends should touch a handful of leaves, got {}: {on:?}",
        on.leaves_visited
    );
}
