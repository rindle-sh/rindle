//! Guarded push fan-out: a reverse predicate index over source connections
//! (`designs/205-GUARDED-PUSH-FANOUT-DESIGN.md`).
//!
//! Every source write today fans out to **every** connection on the table and runs
//! its full compiled predicate against the changed row(s) — O(N) predicate
//! evaluations per write, N = connection slots, almost all concluding "drop" for
//! the `where col = ?` shapes that dominate `rindled`. This module is the
//! *conservative index* that prunes that fan-out to the connections that could
//! actually match.
//!
//! ## Why skipping is exact, not heuristic
//!
//! The index is only ever a **superset** filter — it may over-approximate (visit a
//! connection whose predicate then rejects the row), never under-approximate. Two
//! facts (design §"safety argument") make that sound:
//!
//! 1. [`filter_push`](crate::source_common::filter_push) remains the exact gate: a
//!    connection contributes nothing iff its predicate rejects **both** the old and
//!    the new row. So the index need only never *miss* a possibly-matching
//!    connection; a false positive re-runs the exact predicate, unchanged.
//! 2. Skipping is observationally equivalent for the reentrancy machinery — a
//!    skipped connection's stale `last_pushed_epoch` hides the overlay exactly as
//!    the connection's own predicate would have narrowed that overlay to nothing.
//!
//! Corollary: **every approximation here errs only toward visiting.** A guard-less
//! or un-guardable connection lands in the always-visited `scan` list; a
//! coarser-than-identity `GuardKey` bucket merely re-checks the exact predicate.
//! The one thing that would be a bug is a *finer* key.
//!
//! This module is intentionally standalone and free of the push orchestration: it
//! is pure data (`PushGuard`), a comparator newtype (`GuardKey`), and the index
//! (`PushIndex`). The extraction of a `PushGuard` from a query's `where` tree
//! lives in [`crate::builder`]; the wiring of a `PushIndex` into `ConnTable`'s
//! push path lives in [`crate::source_common`].

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use crate::change::SourceChange;
use crate::predicate::compare_predicate_values;
use crate::value::{ColId, OwnedValue, Value};

/// `predicate(row) ⇒ row[col] ∈ values` — a finite single-column implication of a
/// connection's pushed-down **equality-shaped** filter, extracted (in
/// [`crate::builder`]) from the same stripped condition the connection's
/// [`RowPredicate`](crate::source_common::RowPredicate) was compiled from.
/// Backend-neutral pure data (like [`SqlCondition`](crate::source_common::SqlCondition)),
/// populated for both the memory and SQLite leaves.
///
/// `values` is deduplicated by extraction and is **non-empty in practice** — an
/// empty set is the legitimate encoding of a *never-matching* predicate (an empty
/// `IN`, or `col = NULL` which SQL folds to false); such a guard indexes its slot
/// **nowhere** (not even the `scan` list), which is exact because the
/// connection can receive no pushes.
/// (No derived `PartialEq`: [`OwnedValue`] deliberately has none — the engine's
/// dual-comparator convention forces callers to pick `values_identical` vs
/// `values_equal` explicitly. Compare guards field-wise in tests.)
#[derive(Clone, Debug)]
pub struct PushGuard {
    pub col: ColId,
    pub values: Vec<OwnedValue>,
}

/// A [`BTreeMap`] key over an [`OwnedValue`] whose ordering is
/// [`compare_predicate_values`] — the SAME total order the `=` / `IN` predicate
/// identity ([`values_identical`](crate::value::values_identical)) collapses under
/// (`Null == Null`; `Int`/`Float` widen through `total_cmp`-over-`f64`).
///
/// **Coarsening direction is load-bearing.** The map's equality must never
/// *separate* two values that `values_identical` equates — else a row cell that the
/// predicate treats as identical to a guard literal could miss that literal's
/// bucket and wrongly skip a matching connection (a dropped delta). Coarser is
/// safe: two distinct large `Int`s that widen to one `f64` share a bucket, a false
/// positive the exact predicate re-rejects. `compare_predicate_values` is exactly
/// as coarse as `values_identical` on every equal pair and strictly finer nowhere
/// identity holds (checked pairwise by the property test below), so it is the right
/// key.
#[derive(Clone, Debug)]
struct GuardKey(OwnedValue);

impl Ord for GuardKey {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        compare_predicate_values(self.0.as_ref(), other.0.as_ref())
    }
}
impl PartialOrd for GuardKey {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Eq for GuardKey {}
impl PartialEq for GuardKey {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

/// The reverse index: guard value → connection slots, plus the always-visited scan
/// set and a refcounted union of split-edit keys.
///
/// A connection is registered in **exactly one place**: under its guard's column
/// (once per guard value) in the `eq` map, or in the `scan` set when it
/// has no extractable guard — *unless* its guard has empty `values` (never-matching),
/// in which case it is registered nowhere. [`insert`](Self::insert) /
/// [`remove`](Self::remove) are symmetric so the invariant survives slot recycling.
#[derive(Default, Debug)]
pub struct PushIndex {
    /// col → guard value → slots guarded on `(col, value)`.
    eq: BTreeMap<ColId, BTreeMap<GuardKey, Vec<u32>>>,
    /// Slots with no extractable guard (no filter, presence-only, un-guardable
    /// shape). Always a push candidate — these keep today's behavior verbatim.
    scan: BTreeSet<u32>,
    /// Refcounted union of every registered connection's `split_edit_keys`: a key
    /// is present iff some live slot lists it. Replaces the per-Edit O(N·keys) scan
    /// with an O(distinct keys) check.
    split_keys: BTreeMap<ColId, u32>,
}

impl PushIndex {
    pub fn new() -> PushIndex {
        PushIndex::default()
    }

    /// Register `slot` under `guard` (or the scan set when `None`), and refcount its
    /// `split_edit_keys` into the union. Idempotent per (slot, guard) pair only in
    /// the sense the caller enforces: `ConnTable` inserts once per `connect`.
    pub fn insert(&mut self, slot: u32, guard: Option<&PushGuard>, split_edit_keys: &[ColId]) {
        match guard {
            // Never-matching guard (empty `IN`, `col = NULL`): index nowhere. The
            // connection can receive no pushes, so skipping it always is exact.
            Some(g) if g.values.is_empty() => {}
            Some(g) => {
                let col_map = self.eq.entry(g.col).or_default();
                for v in &g.values {
                    col_map.entry(GuardKey(v.clone())).or_default().push(slot);
                }
            }
            None => {
                self.scan.insert(slot);
            }
        }
        for &k in split_edit_keys {
            *self.split_keys.entry(k).or_insert(0) += 1;
        }
    }

    /// Un-register `slot` — the exact inverse of [`insert`](Self::insert), driven by
    /// the connection's own still-present `guard` (so it names precisely which
    /// buckets to remove it from). Empty buckets and column maps are pruned so the
    /// per-push column iteration stays proportional to *live* guarded columns.
    pub fn remove(&mut self, slot: u32, guard: Option<&PushGuard>, split_edit_keys: &[ColId]) {
        match guard {
            Some(g) if g.values.is_empty() => {}
            Some(g) => {
                if let Some(col_map) = self.eq.get_mut(&g.col) {
                    for v in &g.values {
                        let key = GuardKey(v.clone());
                        if let Some(bucket) = col_map.get_mut(&key) {
                            if let Some(pos) = bucket.iter().position(|&s| s == slot) {
                                // Order within a bucket is irrelevant (candidates are
                                // sorted before the fan-out); swap_remove is O(1).
                                bucket.swap_remove(pos);
                            }
                            if bucket.is_empty() {
                                col_map.remove(&key);
                            }
                        }
                    }
                    if col_map.is_empty() {
                        self.eq.remove(&g.col);
                    }
                }
            }
            None => {
                self.scan.remove(&slot);
            }
        }
        for &k in split_edit_keys {
            if let Some(rc) = self.split_keys.get_mut(&k) {
                *rc -= 1;
                if *rc == 0 {
                    self.split_keys.remove(&k);
                }
            }
        }
    }

    /// Slots that may satisfy `predicate(old) || predicate(new)` for `change` — a
    /// **superset** by construction (module docs). Sorted ascending and deduped, so
    /// the fan-out visits in slot order exactly as the full-scan loop does today.
    ///
    /// Starts from every `scan` slot, then for each guarded column looks up the
    /// change's relevant cell(s): `Add`/`Remove` contribute the one row's cell;
    /// `Edit` contributes both `row[col]` and `old[col]` (an edit can leave one
    /// guard set and enter another).
    pub fn push_candidates(&self, change: &SourceChange) -> Vec<u32> {
        let mut out: Vec<u32> = self.scan.iter().copied().collect();
        for (&col, buckets) in &self.eq {
            match change {
                SourceChange::Add(r) | SourceChange::Remove(r) => {
                    Self::collect_bucket(buckets, r.col(col), &mut out);
                }
                SourceChange::Edit { row, old } => {
                    Self::collect_bucket(buckets, row.col(col), &mut out);
                    Self::collect_bucket(buckets, old.col(col), &mut out);
                }
            }
        }
        out.sort_unstable();
        out.dedup();
        out
    }

    /// Look up `cell`'s bucket in one column's map and append its slots.
    ///
    /// `to_owned` materializes the lookup key: a no-op copy for scalar cells; one
    /// `Arc` allocation for `Str`/`Json`. (An `Absent` cell — partial union row —
    /// finds no bucket, which is correct: guard literals are never `Absent`, and
    /// every guard-eligible leaf rejects an absent cell.)
    #[inline]
    fn collect_bucket(buckets: &BTreeMap<GuardKey, Vec<u32>>, cell: Value<'_>, out: &mut Vec<u32>) {
        if let Some(slots) = buckets.get(&GuardKey(cell.to_owned())) {
            out.extend_from_slice(slots);
        }
    }

    /// The distinct split-edit keys across all registered connections. `should_split`
    /// is `split_keys().any(|k| !values_equal(row[k], old[k]))` — exactly equivalent
    /// to today's `any`-of-`any` over per-connection key lists, at O(distinct keys).
    pub fn split_keys(&self) -> impl Iterator<Item = ColId> + '_ {
        self.split_keys.keys().copied()
    }

    /// `true` iff no connection lists any split-edit key — lets the caller skip the
    /// edit-split check entirely.
    pub fn has_split_keys(&self) -> bool {
        !self.split_keys.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::{owned_row, values_identical, OwnedRow};

    fn iv(n: i64) -> OwnedValue {
        OwnedValue::Int(n)
    }
    fn add(row: OwnedRow) -> SourceChange {
        SourceChange::Add(row)
    }
    fn guard(col: ColId, values: Vec<OwnedValue>) -> PushGuard {
        PushGuard { col, values }
    }

    // -- GuardKey coarseness: the never-finer property (design §Testing.3) --------

    /// The corpus of "identical-under-predicate" pairs and "distinct" values that
    /// exercise every arm of `values_identical` / `compare_predicate_values`.
    fn value_corpus() -> Vec<OwnedValue> {
        vec![
            OwnedValue::Null,
            OwnedValue::Bool(false),
            OwnedValue::Bool(true),
            OwnedValue::Int(0),
            OwnedValue::Int(7),
            OwnedValue::Float(7.0), // identical to Int(7) under the predicate domain
            OwnedValue::Float(7.5),
            OwnedValue::Float(f64::NAN),
            OwnedValue::str("7"),
            OwnedValue::str("apple"),
            OwnedValue::Json("{}".into()),
        ]
    }

    #[test]
    fn guardkey_never_finer_than_values_identical() {
        // The one direction that would be a correctness bug: two values the predicate
        // treats as identical MUST share a GuardKey bucket. (Coarser is fine.)
        let corpus = value_corpus();
        for a in &corpus {
            for b in &corpus {
                if values_identical(a.as_ref(), b.as_ref()) {
                    assert_eq!(
                        GuardKey(a.clone()),
                        GuardKey(b.clone()),
                        "values_identical({a:?}, {b:?}) but GuardKeys differ — would drop a delta",
                    );
                }
            }
        }
    }

    #[test]
    fn guardkey_int_float_widening_shares_bucket() {
        // The exact false-positive-tolerant coarsening the design relies on.
        let ki = GuardKey(OwnedValue::Int(7));
        let kf = GuardKey(OwnedValue::Float(7.0));
        assert_eq!(ki, kf);
        let mut m: BTreeMap<GuardKey, u32> = BTreeMap::new();
        m.insert(GuardKey(OwnedValue::Int(7)), 1);
        assert_eq!(m.get(&GuardKey(OwnedValue::Float(7.0))), Some(&1));
    }

    #[test]
    fn guardkey_null_is_its_own_bucket() {
        // Predicate identity is null==null (unlike join equality), so a `col IS NULL`
        // guard and a null row cell must meet in the same bucket.
        assert_eq!(GuardKey(OwnedValue::Null), GuardKey(OwnedValue::Null));
        assert_ne!(GuardKey(OwnedValue::Null), GuardKey(OwnedValue::Int(0)));
    }

    // -- PushIndex maintenance & the exactly-once invariant (design §Testing.2) ----

    /// Count every appearance of `slot` across `eq` buckets + `scan`. The invariant
    /// is: a live slot appears in exactly one *place* (its guard column, once per
    /// guard value) or in `scan`; a never-matching guard appears nowhere.
    fn places(idx: &PushIndex, slot: u32) -> (usize, bool) {
        let eq_hits: usize = idx
            .eq
            .values()
            .flat_map(|m| m.values())
            .flat_map(|v| v.iter())
            .filter(|&&s| s == slot)
            .count();
        (eq_hits, idx.scan.contains(&slot))
    }

    #[test]
    fn insert_guarded_then_remove_is_empty() {
        let mut idx = PushIndex::new();
        let g = guard(0, vec![iv(42)]);
        idx.insert(3, Some(&g), &[]);
        assert_eq!(places(&idx, 3), (1, false));
        idx.remove(3, Some(&g), &[]);
        assert_eq!(places(&idx, 3), (0, false));
        // Column map pruned to empty.
        assert!(idx.eq.is_empty(), "empty column map should be pruned");
    }

    #[test]
    fn insert_scan_then_remove() {
        let mut idx = PushIndex::new();
        idx.insert(5, None, &[]);
        assert_eq!(places(&idx, 5), (0, true));
        idx.remove(5, None, &[]);
        assert_eq!(places(&idx, 5), (0, false));
    }

    #[test]
    fn multi_value_guard_registers_once_per_value() {
        let mut idx = PushIndex::new();
        let g = guard(1, vec![iv(1), iv(2), iv(3)]); // col IN (1,2,3)
        idx.insert(9, Some(&g), &[]);
        let hits = places(&idx, 9).0;
        assert_eq!(hits, 3, "one appearance per guard value");
        idx.remove(9, Some(&g), &[]);
        assert_eq!(places(&idx, 9), (0, false));
        assert!(idx.eq.is_empty());
    }

    #[test]
    fn empty_values_guard_indexes_nowhere() {
        // `col = NULL` / `col IN ()` fold to never-matching: registered nowhere.
        let mut idx = PushIndex::new();
        let g = guard(0, vec![]);
        idx.insert(2, Some(&g), &[]);
        assert_eq!(places(&idx, 2), (0, false));
        assert!(idx.eq.is_empty() && idx.scan.is_empty());
        // Remove is a clean no-op.
        idx.remove(2, Some(&g), &[]);
        assert_eq!(places(&idx, 2), (0, false));
    }

    #[test]
    fn recycle_slot_reassigns_registration() {
        // destroy un-indexes -> slot recycled by a later connect under a new guard.
        let mut idx = PushIndex::new();
        let g_a = guard(0, vec![iv(10)]);
        idx.insert(4, Some(&g_a), &[]);
        idx.remove(4, Some(&g_a), &[]); // teardown
        let g_b = guard(1, vec![iv(20)]); // new tenant, different column
        idx.insert(4, Some(&g_b), &[]);
        assert_eq!(places(&idx, 4).0, 1);
        // It lives under the NEW column/value, not the old.
        assert!(idx.eq.get(&1).unwrap().contains_key(&GuardKey(iv(20))));
        assert!(!idx.eq.contains_key(&0), "old bucket fully pruned");
    }

    #[test]
    fn split_keys_refcount_drains_to_empty() {
        let mut idx = PushIndex::new();
        idx.insert(0, None, &[2, 5]);
        idx.insert(1, None, &[5]); // 5 now refcount 2
        let mut keys: Vec<_> = idx.split_keys().collect();
        keys.sort_unstable();
        assert_eq!(keys, vec![2, 5]);
        assert!(idx.has_split_keys());
        idx.remove(0, None, &[2, 5]); // 2 -> 0 (drop), 5 -> 1
        let keys: Vec<_> = idx.split_keys().collect();
        assert_eq!(keys, vec![5]);
        idx.remove(1, None, &[5]); // 5 -> 0 (drop)
        assert!(!idx.has_split_keys());
        assert_eq!(idx.split_keys().count(), 0);
    }

    // -- push_candidates: superset, sorted, deduped (design §Testing.2) -----------

    #[test]
    fn candidates_are_sorted_and_deduped() {
        let mut idx = PushIndex::new();
        // Two connections guarded on col 0 = 42, plus a scan connection.
        idx.insert(7, Some(&guard(0, vec![iv(42)])), &[]);
        idx.insert(3, Some(&guard(0, vec![iv(42)])), &[]);
        idx.insert(1, None, &[]);
        let cand = idx.push_candidates(&add(owned_row(vec![iv(42)])));
        assert_eq!(cand, vec![1, 3, 7], "scan + both guarded, sorted ascending");
    }

    #[test]
    fn candidates_exclude_non_matching_guard() {
        let mut idx = PushIndex::new();
        idx.insert(7, Some(&guard(0, vec![iv(42)])), &[]);
        idx.insert(1, None, &[]);
        // Write to a different value: only the scan connection is a candidate.
        let cand = idx.push_candidates(&add(owned_row(vec![iv(99)])));
        assert_eq!(cand, vec![1]);
    }

    #[test]
    fn candidates_edit_is_two_sided() {
        // An edit that moves col 0 from 42 -> 99 must visit BOTH the 42-guarded and
        // the 99-guarded connection (leaves one guard set, enters another).
        let mut idx = PushIndex::new();
        idx.insert(2, Some(&guard(0, vec![iv(42)])), &[]);
        idx.insert(5, Some(&guard(0, vec![iv(99)])), &[]);
        let change = SourceChange::Edit {
            row: owned_row(vec![iv(99)]),
            old: owned_row(vec![iv(42)]),
        };
        assert_eq!(idx.push_candidates(&change), vec![2, 5]);
    }

    #[test]
    fn candidates_edit_same_bucket_not_duplicated() {
        // Both sides of the edit hit the same bucket -> the slot appears once.
        let mut idx = PushIndex::new();
        idx.insert(4, Some(&guard(0, vec![iv(42)])), &[]);
        let change = SourceChange::Edit {
            row: owned_row(vec![iv(42), iv(1)]),
            old: owned_row(vec![iv(42), iv(2)]),
        };
        assert_eq!(idx.push_candidates(&change), vec![4]);
    }

    #[test]
    fn candidates_multi_column_guards() {
        // Guards on two different columns; a write hits only the matching column.
        let mut idx = PushIndex::new();
        idx.insert(1, Some(&guard(0, vec![iv(42)])), &[]); // projectId = 42
        idx.insert(2, Some(&guard(1, vec![iv(7)])), &[]); // ownerId = 7

        // Row: col0 = 42, col1 = 999 -> matches conn 1's guard only.
        let cand = idx.push_candidates(&add(owned_row(vec![iv(42), iv(999)])));
        assert_eq!(cand, vec![1]);
    }

    #[test]
    fn candidates_int_float_coarsening_is_a_superset() {
        // A guard registered under Int(7); a row cell of Float(7.0) must still be a
        // candidate (the predicate treats them identical).
        let mut idx = PushIndex::new();
        idx.insert(6, Some(&guard(0, vec![iv(7)])), &[]);
        let cand = idx.push_candidates(&add(owned_row(vec![OwnedValue::Float(7.0)])));
        assert_eq!(cand, vec![6]);
    }

    #[test]
    fn candidates_null_guard_matches_null_cell() {
        let mut idx = PushIndex::new();
        idx.insert(8, Some(&guard(0, vec![OwnedValue::Null])), &[]); // col IS NULL
        let cand = idx.push_candidates(&add(owned_row(vec![OwnedValue::Null])));
        assert_eq!(cand, vec![8]);
        // A non-null cell does not hit the null bucket.
        let miss = idx.push_candidates(&add(owned_row(vec![iv(1)])));
        assert!(miss.is_empty());
    }

    #[test]
    fn candidates_empty_index_is_empty() {
        let idx = PushIndex::new();
        assert!(idx.push_candidates(&add(owned_row(vec![iv(1)]))).is_empty());
    }
}
