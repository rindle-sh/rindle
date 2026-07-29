//! Shared partition-key helpers for the stateful, partition-aware limiters
//! ([`Take`](crate::op::Take) / [`Cap`](crate::op::Cap)). Both key their
//! per-partition scratch state by an encoded partition-value tuple, both gate a
//! fetch on whether the request constraint matches the partition key, and both
//! assert — on an Edit — that the partition key did not change.

use std::cmp::Ordering;

use crate::change::Constraint;
use crate::value::{compare_values, float_int_class, ColId, OwnedRow as Row, OwnedValue};

/// `constraintMatchesPartitionKey` (`take.ts:727`) for the both-non-`None` case:
/// the constraint's columns are exactly the partition-key columns (same arity,
/// same set). The `None`-partition / `None`-constraint cases are handled by the
/// callers' `partition_key.is_none()` / `constraint.is_some_and(..)` short-circuits.
pub(crate) fn constraint_matches_partition_key(c: &Constraint, pk: &[ColId]) -> bool {
    c.len() == pk.len() && pk.iter().all(|p| c.iter().any(|(col, _)| col == p))
}

/// True iff `old` and `new` agree on every partition-key column (`compare_values`,
/// null == null) — the invariant a limiter asserts on an Edit (the source
/// split-edits a partition-key change into remove+add, so an Edit never crosses a
/// partition). Mirrors `makePartitionKeyComparator(...) === 0` (`take.ts:745`).
pub(crate) fn partition_key_unchanged(old: &Row, new: &Row, pk: &[ColId]) -> bool {
    pk.iter()
        .all(|&c| compare_values(old.col(c), new.col(c)) == Ordering::Equal)
}

/// A collision-free string key over a partition-value tuple, prefixed with
/// `prefix` (`"take"` / `"cap"`) — the port of `getTakeStateKey`/`getCapStateKey`'s
/// `JSON.stringify([prefix, ...values])` (`take.ts:710`, `cap.ts:300`). A type tag
/// per value (length-prefixed for str/json) makes distinct tuples never alias; the
/// memory store only does point `get`/`set` on this key, so its ordering is moot.
pub(crate) fn encode_state_key(prefix: &str, partition_values: &[OwnedValue]) -> String {
    let mut s = String::from(prefix);
    for v in partition_values {
        s.push('\u{1f}'); // unit separator between elements
        encode_value(v, &mut s);
    }
    s
}

fn encode_value(v: &OwnedValue, s: &mut String) {
    match v {
        // Never met on a partition key in practice (presence-required); a distinct tag
        // keeps the encoding total and unambiguous.
        OwnedValue::Absent => s.push('a'),
        OwnedValue::Null => s.push('n'),
        OwnedValue::Bool(b) => {
            s.push('b');
            s.push(if *b { '1' } else { '0' });
        }
        OwnedValue::Int(i) => {
            s.push('i');
            s.push_str(&i.to_string());
        }
        // §5.4 canonicalization (design 226): the SAME equivalence relation as the
        // comparators and the join `CanonVal` — an exactly-integral float shares its
        // key with the `Int` it equals (`Int(5)` and `Float(5.0)` are one group /
        // Take partition / Exists slot, as `compare_values` says), while `-0.0`,
        // out-of-range integrals, and non-integrals keep float identity. Without
        // this, a plane-crossing Edit passes `partition_key_unchanged` (comparator-
        // based, so no split-edit) yet keys different state — reduce would subtract
        // the old contribution from the wrong group.
        OwnedValue::Float(f) => match float_int_class(*f) {
            Some(i) => {
                s.push('i');
                s.push_str(&i.to_string());
            }
            None => {
                s.push('f');
                s.push_str(&f.to_bits().to_string());
            }
        },
        OwnedValue::Str(x) => {
            s.push('s');
            s.push_str(&x.len().to_string());
            s.push(':');
            s.push_str(x);
        }
        OwnedValue::Json(x) => {
            s.push('j');
            s.push_str(&x.len().to_string());
            s.push(':');
            s.push_str(x);
        }
    }
}
