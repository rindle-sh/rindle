//! Shared join-family helpers (ports of `ivm/join-utils.ts`), consumed by `Join`
//! (graph.rs) and — once they land — `FlippedJoin`, `FanIn`, `UnionFanIn`. Kept in
//! one place so the join family doesn't reinvent (and diverge on) the key
//! comparisons.
//!
//! **Name-collision note (resolved):** the JS `join-utils.ts` also exports a
//! *node-level* `generateWithOverlay`. The crate already owns that name at the
//! **row** level in [`crate::source_common`] (the source-fetch splice). The
//! node-level join overlay splice lives in `graph.rs` under the distinct name
//! `splice_join_overlay` (it is coupled to the spike `View`'s re-materialization
//! model; the production node-level generator + its `{change, position}` gate are
//! co-designed with the production `ArrayView`, spec `09`, and deferred). So the
//! `generate_with_overlay` name stays unambiguous.

use crate::value::{compare_values, values_equal, ColId, OwnedRow as Row, OwnedValue, Value};
use std::cmp::Ordering;
use std::sync::Arc;

/// `isJoinMatch` (`join-utils.ts:219`): does `parent` join to `child` on the
/// compound key? Uses [`values_equal`] (null ≠ null — join semantics: a null key
/// never matches), the *opposite* of [`row_equals_for_compound_key`]. The nth
/// `parent_key` column pairs with the nth `child_key` column.
pub fn is_join_match(parent: &Row, parent_key: &[ColId], child: &Row, child_key: &[ColId]) -> bool {
    debug_assert_eq!(parent_key.len(), child_key.len());
    parent_key
        .iter()
        .zip(child_key)
        .all(|(&pk, &ck)| values_equal(parent.col(pk), child.col(ck)))
}

/// `rowEqualsForCompoundKey` (`join-utils.ts:206`): are `a` and `b` equal on every
/// `key` column? Uses [`compare_values`] (null == null — identity/sort semantics),
/// the *opposite* of [`is_join_match`]. Used to assert an `Edit` did not change the
/// join key (`join.ts` parent/child edit asserts), where a null key column must
/// compare equal to itself.
pub fn row_equals_for_compound_key(a: &Row, b: &Row, key: &[ColId]) -> bool {
    key.iter()
        .all(|&c| compare_values(a.col(c), b.col(c)) == Ordering::Equal)
}

/// One column of a [`CanonKey`] — a type-tagged, `Hash + Eq` value
/// (`canonicalValue`, `flipped-join.ts:600`). The tag is the variant itself, so
/// `Int(1)` and `Str("1")` are distinct keys (the JS prefixes a type char; the
/// Rust enum carries the type for free). **Numerics canonicalize by their exact
/// equality class** (design 226 §5.4, [`crate::value::float_int_class`]): a finite, integral,
/// in-i64-range, non-`-0.0` `Float` shares `Int`'s key — so `Int(1)` and
/// `Float(1.0)` group together, exactly when [`values_equal`] says they are equal.
/// Every other float keeps [`f64::to_bits`] identity, matching `total_cmp`'s
/// classes (the `-0.0`/`0.0` and NaN-payload distinctions the engine makes).
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum CanonVal {
    /// Mirrors [`OwnedValue::Absent`] — never met on a join key in practice
    /// (presence-required, `PROJECTION-SUPPORT-DESIGN.md` §2.1), carried for totality.
    Absent,
    Null,
    Bool(bool),
    Int(i64),
    Float(u64),
    Str(Arc<str>),
    Json(Arc<str>),
}

impl CanonVal {
    fn of(v: &OwnedValue) -> CanonVal {
        match v {
            OwnedValue::Absent => CanonVal::Absent,
            OwnedValue::Null => CanonVal::Null,
            OwnedValue::Bool(b) => CanonVal::Bool(*b),
            OwnedValue::Int(i) => CanonVal::Int(*i),
            OwnedValue::Float(f) => match crate::value::float_int_class(*f) {
                Some(i) => CanonVal::Int(i),
                None => CanonVal::Float(f.to_bits()),
            },
            OwnedValue::Str(s) => CanonVal::Str(s.clone()),
            OwnedValue::Json(j) => CanonVal::Json(j.clone()),
        }
    }

    /// Borrowed-cell form of [`CanonVal::of`] (a flat row's cells are
    /// [`Value<'_>`]s). `Str`/`Json` allocate — the key escapes the row.
    fn of_value(v: Value<'_>) -> CanonVal {
        match v {
            Value::Absent => CanonVal::Absent,
            Value::Null => CanonVal::Null,
            Value::Bool(b) => CanonVal::Bool(b),
            Value::Int(i) => CanonVal::Int(i),
            Value::Float(f) => match crate::value::float_int_class(f) {
                Some(i) => CanonVal::Int(i),
                None => CanonVal::Float(f.to_bits()),
            },
            Value::Str(b) => CanonVal::Str(str_of_validated(b, "TEXT")),
            Value::Json(b) => CanonVal::Json(str_of_validated(b, "JSON")),
        }
    }
}

/// A flat row's text bytes are UTF-8-validated at row construction; re-check
/// cheaply here rather than carrying `unsafe`.
fn str_of_validated(b: &[u8], kind: &str) -> Arc<str> {
    Arc::from(std::str::from_utf8(b).unwrap_or_else(|_| panic!("row {kind} cell is not UTF-8")))
}

/// A canonical key over a compound join key (`canonicalKey`,
/// `flipped-join.ts:585`). Used by [`FlippedJoin`](crate::op::FlippedJoin) to
/// dedupe the IN-batch of child-derived parent constraints and to map a returned
/// parent row back to its grouped children. `Hash + Eq`, so it keys the grouping
/// map directly — no per-row `String` alloc (the JS concatenates a tagged string;
/// the typed tuple is the faster, exact Rust form, `06` §8).
///
/// Two rows produce the same key iff their key columns are pairwise
/// [`values_equal`] — *except* `Null`, which `values_equal` treats as "never
/// equal" but which still gets a distinct token here: a null-keyed parent row
/// therefore simply misses every (non-null) child group and is skipped, never
/// spuriously joined. (For numerics this contract HOLDS as of design 226 Stage B:
/// the exact comparator makes numeric equality transitive and [`CanonVal`]
/// canonicalizes `Float`s into their `Int` class — before that, `Int(1)` and
/// `Float(1.0)` hashed apart while comparing equal.)
pub type CanonKey = Vec<CanonVal>;

pub fn canonical_key(row: &Row, key: &[ColId]) -> CanonKey {
    key.iter()
        .map(|&c| CanonVal::of_value(row.col(c)))
        .collect()
}

/// Canonical key over a [`build_join_constraint`](crate::change::build_join_constraint)
/// output. The constraint's values are in `to_key` order, so this agrees with
/// `canonical_key(parent_row, to_key)` for any parent the constraint matches —
/// the two sides [`FlippedJoin`](crate::op::FlippedJoin) compares to group each
/// child under its parent's IN-batch entry.
pub fn canonical_key_of_constraint(c: &crate::change::Constraint) -> CanonKey {
    c.iter().map(|(_, v)| CanonVal::of(v)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::{owned_row, OwnedValue};

    fn r(vals: Vec<OwnedValue>) -> Row {
        owned_row(vals)
    }

    /// Design 226 §5.4 / §11 "canonical-key coherence": two numeric cells share a
    /// `CanonVal` exactly when `values_equal` says they are equal — the contract the
    /// module docs state, which held for no numeric pair before Stage B.
    #[test]
    fn canonical_keys_agree_with_values_equal_on_numerics() {
        use crate::value::values_equal;
        const TWO_53: i64 = 1 << 53;
        let vals = [
            OwnedValue::Int(0),
            OwnedValue::Int(1),
            OwnedValue::Int(TWO_53),
            OwnedValue::Int(TWO_53 + 1),
            OwnedValue::Int(i64::MIN),
            OwnedValue::Float(0.0),
            OwnedValue::Float(-0.0),
            OwnedValue::Float(1.0),
            OwnedValue::Float(1.5),
            OwnedValue::Float(TWO_53 as f64),
            OwnedValue::Float(i64::MIN as f64),
            OwnedValue::Float(f64::INFINITY),
            OwnedValue::Float(f64::NAN),
            OwnedValue::Float(-f64::NAN),
        ];
        for a in &vals {
            for b in &vals {
                assert_eq!(
                    CanonVal::of(a) == CanonVal::of(b),
                    values_equal(a.as_ref(), b.as_ref()),
                    "key-coherence violated for {a:?} vs {b:?}"
                );
                // The borrowed-cell form canonicalizes identically.
                assert_eq!(CanonVal::of(a), CanonVal::of_value(a.as_ref()), "{a:?}");
            }
        }
    }

    #[test]
    fn is_join_match_null_never_matches() {
        // parent.col1 == child.col0
        let parent = r(vec![OwnedValue::Int(7), OwnedValue::Int(1)]);
        let child = r(vec![OwnedValue::Int(7)]);
        assert!(is_join_match(&parent, &[0], &child, &[0]));
        assert!(!is_join_match(&parent, &[1], &child, &[0]));

        // null key ⇒ never a match (values_equal: null != null), even null vs null.
        let pn = r(vec![OwnedValue::Null]);
        let cn = r(vec![OwnedValue::Null]);
        assert!(!is_join_match(&pn, &[0], &cn, &[0]));
    }

    #[test]
    fn is_join_match_compound() {
        let parent = r(vec![OwnedValue::Int(1), OwnedValue::Int(2)]);
        let child = r(vec![OwnedValue::Int(2), OwnedValue::Int(1)]);
        // parent[0]==child[1] && parent[1]==child[0]
        assert!(is_join_match(&parent, &[0, 1], &child, &[1, 0]));
        assert!(!is_join_match(&parent, &[0, 1], &child, &[0, 1]));
    }

    #[test]
    fn row_equals_for_compound_key_null_equals_null() {
        // compare_values: null == null — so a null key column equals itself.
        let a = r(vec![OwnedValue::Int(1), OwnedValue::Null]);
        let b = r(vec![OwnedValue::Int(1), OwnedValue::Null]);
        assert!(row_equals_for_compound_key(&a, &b, &[0, 1]));

        let c = r(vec![OwnedValue::Int(1), OwnedValue::Int(9)]);
        assert!(!row_equals_for_compound_key(&a, &c, &[0, 1]));
        // differs only outside the key ⇒ still equal on the key.
        let d = r(vec![OwnedValue::Int(1), OwnedValue::Null]);
        assert!(row_equals_for_compound_key(&a, &d, &[0]));
    }

    #[test]
    fn canonical_key_groups_equal_rows_distinguishes_types() {
        // Same int key value on the same column ⇒ same canonical key (the grouping
        // contract: two children with key 7 land in one group).
        let a = r(vec![OwnedValue::Int(7), OwnedValue::Int(1)]);
        let b = r(vec![OwnedValue::Int(7), OwnedValue::Int(2)]);
        assert_eq!(canonical_key(&a, &[0]), canonical_key(&b, &[0]));

        // Type-tagged: Int(1) and Str("1") never collide (the JS `'d'1` vs `'s'1`).
        let i1 = r(vec![OwnedValue::Int(1)]);
        let s1 = r(vec![OwnedValue::str("1")]);
        assert_ne!(canonical_key(&i1, &[0]), canonical_key(&s1, &[0]));

        // Float keys hash by bits, agreeing with compare_values' total_cmp:
        // 1.0 == 1.0 but 1.0 != 2.0, and -0.0 is distinct from 0.0 (total_cmp).
        let f1 = r(vec![OwnedValue::Float(1.0)]);
        let f1b = r(vec![OwnedValue::Float(1.0)]);
        let f2 = r(vec![OwnedValue::Float(2.0)]);
        assert_eq!(canonical_key(&f1, &[0]), canonical_key(&f1b, &[0]));
        assert_ne!(canonical_key(&f1, &[0]), canonical_key(&f2, &[0]));
        let negzero = r(vec![OwnedValue::Float(-0.0)]);
        let poszero = r(vec![OwnedValue::Float(0.0)]);
        assert_ne!(canonical_key(&negzero, &[0]), canonical_key(&poszero, &[0]));
    }

    #[test]
    fn canonical_key_is_order_and_column_sensitive() {
        // Compound key: column order is part of the key (parent_key[i] pairs with
        // child_key[i]). (1,2) over [0,1] differs from (2,1) over [0,1].
        let ab = r(vec![OwnedValue::Int(1), OwnedValue::Int(2)]);
        let ba = r(vec![OwnedValue::Int(2), OwnedValue::Int(1)]);
        assert_ne!(canonical_key(&ab, &[0, 1]), canonical_key(&ba, &[0, 1]));
        // ...but reading ba's columns in swapped order recovers ab's key.
        assert_eq!(canonical_key(&ab, &[0, 1]), canonical_key(&ba, &[1, 0]));
    }
}
