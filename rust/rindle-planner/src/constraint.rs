//! `PlannerConstraint` + `merge_constraints` — the join-predicate column sets that
//! narrow table scans (port of `planner-constraint.ts`).
//!
//! JS models a constraint as `Record<string, undefined>`: a *key-set* (we know the
//! correlated column name(s), e.g. `assignee_id`, but not the runtime value, so the
//! values are all `undefined`). We model it as an **insertion-ordered**
//! `IndexSet<Box<str>>`.
//!
//! Insertion order is LOAD-BEARING (this is why it is not a `BTreeSet`): the flipped
//! join's `translate_constraints_for_flipped_join` maps the parent's i-th constraint
//! key to the child's i-th key, and those positions mirror the correlation
//! `parent_field[i]` ↔ `child_field[i]`. A sorted set would scramble that
//! correspondence whenever the correlation columns are not already alphabetical. The
//! merge below preserves JS `{...a, ...b}` order (a's keys, then b's new keys).

use indexmap::IndexSet;

/// A set of correlated column names that constrain a table scan (`PlannerConstraint`,
/// `planner-constraint.ts`), insertion-ordered to match JS object-key order.
pub type PlannerConstraint = IndexSet<Box<str>>;

/// Merge two optional constraints (`mergeConstraints`, `planner-constraint.ts`).
///
/// `None` is the identity. Two present constraints union with `a`'s keys first, then
/// `b`'s new keys — exactly the JS `{...a, ...b}` spread (keys only; values are all
/// `undefined`, so `b`-wins is a no-op).
pub fn merge_constraints(
    a: Option<&PlannerConstraint>,
    b: Option<&PlannerConstraint>,
) -> Option<PlannerConstraint> {
    match (a, b) {
        (None, b) => b.cloned(),
        (a, None) => a.cloned(),
        (Some(a), Some(b)) => Some(a.union(b).cloned().collect()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn c(items: &[&str]) -> PlannerConstraint {
        items.iter().map(|s| Box::from(*s)).collect()
    }

    #[test]
    fn none_is_identity() {
        assert_eq!(merge_constraints(None, None), None);
        assert_eq!(merge_constraints(Some(&c(&["a"])), None), Some(c(&["a"])));
        assert_eq!(merge_constraints(None, Some(&c(&["b"]))), Some(c(&["b"])));
    }

    #[test]
    fn union_merges_keys() {
        assert_eq!(
            merge_constraints(Some(&c(&["a"])), Some(&c(&["b"]))),
            Some(c(&["a", "b"]))
        );
        // Overlapping keys collapse (set semantics), matching the JS spread.
        assert_eq!(
            merge_constraints(Some(&c(&["a", "b"])), Some(&c(&["b", "x"]))),
            Some(c(&["a", "b", "x"]))
        );
    }

    #[test]
    fn merge_preserves_insertion_order() {
        // {...a, ...b}: a's order (b, a) first, then b's new keys (c). NOT sorted.
        let merged = merge_constraints(Some(&c(&["b", "a"])), Some(&c(&["a", "c"]))).unwrap();
        let order: Vec<&str> = merged.iter().map(|s| s.as_ref()).collect();
        assert_eq!(order, vec!["b", "a", "c"]);
    }
}
