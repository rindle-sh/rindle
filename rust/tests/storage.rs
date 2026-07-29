//! Spec `10` §9 — operator-storage behavioral suite. Ports the JS
//! `memory-storage.test.ts` (§9.1) and the §9.6 invariants against
//! [`MemoryStorage`] and the `create_storage()` factory.
//!
//! `StorageValue` has no `PartialEq` (it carries `OwnedRow`s, and `OwnedValue`
//! deliberately has no derived comparison — you must pick `compare_values` vs
//! `values_equal`), so equality is spelled out here via `compare_values`
//! (null == null), the same convention `graph::CollectedChange` uses.

use std::cmp::Ordering;

use rindle::storage::{create_storage, MemoryStorage, Storage, StorageValue};
use rindle::{compare_values, owned_row, OwnedRow, OwnedValue};

// --- equality helpers (no derived PartialEq on StorageValue) ---------------

fn row_eq(a: &OwnedRow, b: &OwnedRow) -> bool {
    a.len() == b.len()
        && (0..a.len()).all(|i| compare_values(a.col(i), b.col(i)) == Ordering::Equal)
}

fn opt_row_eq(a: &Option<OwnedRow>, b: &Option<OwnedRow>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(x), Some(y)) => row_eq(x, y),
        _ => false,
    }
}

fn sv_eq(a: &StorageValue, b: &StorageValue) -> bool {
    use StorageValue::*;
    match (a, b) {
        (
            Take {
                size: s1,
                bound: b1,
            },
            Take {
                size: s2,
                bound: b2,
            },
        ) => s1 == s2 && opt_row_eq(b1, b2),
        (Bound(r1), Bound(r2)) => row_eq(r1, r2),
        (Cap { size: s1, pks: p1 }, Cap { size: s2, pks: p2 }) => s1 == s2 && p1 == p2,
        _ => false,
    }
}

#[track_caller]
fn assert_get(s: &dyn Storage, key: &str, expected: Option<StorageValue>) {
    match (s.get(key), &expected) {
        (None, None) => {}
        (Some(got), Some(exp)) => assert!(sv_eq(&got, exp), "get({key:?}) = {got:?}, want {exp:?}"),
        (got, _) => panic!("get({key:?}) = {got:?}, want {expected:?}"),
    }
}

// --- small constructors ----------------------------------------------------

fn bound_row(id: i64, name: &str) -> OwnedRow {
    owned_row(vec![OwnedValue::Int(id), OwnedValue::str(name)])
}
fn cap(size: u32, pks: &[&str]) -> StorageValue {
    StorageValue::Cap {
        size,
        pks: pks.iter().map(|s| (*s).into()).collect(),
    }
}

/// Collect a `scan` into `(key, value)` pairs (the iterator yields owned pairs).
fn scan_keys(s: &dyn Storage, prefix: &str) -> Vec<String> {
    s.scan(prefix).map(|(k, _)| k.to_string()).collect()
}

// ===========================================================================
// §9.1 — direct ports of memory-storage.test.ts
// ===========================================================================

#[test]
fn basics_set_get_del() {
    let s = MemoryStorage::new();
    assert_get(&s, "foo", None); // E1: absent ⇒ None

    s.set(
        "foo",
        StorageValue::Take {
            size: 3,
            bound: Some(bound_row(7, "x")),
        },
    );
    assert_get(
        &s,
        "foo",
        Some(StorageValue::Take {
            size: 3,
            bound: Some(bound_row(7, "x")),
        }),
    );

    s.del("foo");
    assert_get(&s, "foo", None); // gone after del
}

#[test]
fn default_via_unwrap_or() {
    // §9.6.1 / D1: there is no `def` param; the caller folds the default in with
    // `unwrap_or`, and the default is NOT stored.
    let s = MemoryStorage::new();
    let def = cap(0, &[]);

    let got = s.get("missing").unwrap_or_else(|| def.clone());
    assert!(sv_eq(&got, &def));
    assert_get(&s, "missing", None); // the default was not written through

    s.set("missing", cap(2, &["a", "b"]));
    let got = s.get("missing").unwrap_or(def);
    assert!(sv_eq(&got, &cap(2, &["a", "b"])));
}

#[test]
fn set_overwrites_same_key() {
    // E3 / §9.6.2: set then set the same key ⇒ one entry, latest value.
    let s = MemoryStorage::new();
    s.set("k", cap(1, &["a"]));
    s.set("k", cap(2, &["a", "b"]));
    assert_get(&s, "k", Some(cap(2, &["a", "b"])));
    assert_eq!(s.clone_data().len(), 1, "overwrite must not grow the map");
}

#[test]
fn del_absent_and_twice_are_noops() {
    // E4 / §9.6.3
    let s = MemoryStorage::new();
    s.del("nope"); // absent ⇒ no-op, no panic
    s.set("k", cap(1, &["a"]));
    s.del("k");
    s.del("k"); // twice ⇒ no-op
    assert_get(&s, "k", None);
}

#[test]
fn value_variants_round_trip() {
    // §9.1 "other types": every StorageValue variant round-trips. The JS generic
    // object/array/null cases map onto our tight enum (spec 10 §9.1):
    //   - Take{bound:None}  — the "no boundary yet" state (E9-ish: distinct shape)
    //   - Take{bound:Some}  — a nested row value (E10)
    //   - Bound(row)        — the maxBound slot
    //   - Cap{pks}          — an array-shaped value (E10)
    let s = MemoryStorage::new();
    s.set(
        "t_empty",
        StorageValue::Take {
            size: 0,
            bound: None,
        },
    );
    s.set(
        "t_full",
        StorageValue::Take {
            size: 5,
            bound: Some(bound_row(42, "z")),
        },
    );
    s.set("maxBound", StorageValue::Bound(bound_row(99, "max")));
    s.set("c", cap(3, &["p1", "p2", "p3"]));

    assert_get(
        &s,
        "t_empty",
        Some(StorageValue::Take {
            size: 0,
            bound: None,
        }),
    );
    assert_get(
        &s,
        "t_full",
        Some(StorageValue::Take {
            size: 5,
            bound: Some(bound_row(42, "z")),
        }),
    );
    assert_get(
        &s,
        "maxBound",
        Some(StorageValue::Bound(bound_row(99, "max"))),
    );
    assert_get(&s, "c", Some(cap(3, &["p1", "p2", "p3"])));

    // Take{bound:None} is a real, distinct value — not the same as absent (E9).
    assert!(s.get("t_empty").is_some());
    assert!(s.get("never_set").is_none());
}

#[test]
fn scan_ascending_and_prefixes() {
    // §9.1 scan / E6,E7,E8 — fixture keys mirror the JS suite's quu*/qu* + ba*.
    let s = MemoryStorage::new();
    for k in ["foo", "baz", "qux", "bar", "quux"] {
        s.set(k, cap(1, &[k])); // insert out of order; the store sorts
    }

    // E7: full scan is strictly ascending byte order.
    assert_eq!(scan_keys(&s, ""), ["bar", "baz", "foo", "quux", "qux"]);

    // E6/E8: prefix bounds both ends; an exact prefix-key is included (>=).
    assert_eq!(scan_keys(&s, "ba"), ["bar", "baz"]);
    assert_eq!(scan_keys(&s, "qu"), ["quux", "qux"]); // "quux" < "qux" byte-wise
    assert_eq!(scan_keys(&s, "quu"), ["quux"]);
    assert_eq!(scan_keys(&s, "qux"), ["qux"]); // prefix is itself a stored key (E8)

    // E6: a prefix with no matching keys ⇒ empty.
    assert!(scan_keys(&s, "zzz").is_empty());

    // values come through with their keys.
    let pairs: Vec<(String, StorageValue)> =
        s.scan("ba").map(|(k, v)| (k.to_string(), v)).collect();
    assert!(sv_eq(&pairs[0].1, &cap(1, &["bar"])));
    assert!(sv_eq(&pairs[1].1, &cap(1, &["baz"])));
}

#[test]
fn scan_empty_store_is_empty() {
    // E5
    let s = MemoryStorage::new();
    assert!(scan_keys(&s, "").is_empty());
    assert!(scan_keys(&s, "anything").is_empty());
}

// ===========================================================================
// §9.6 — invariants
// ===========================================================================

#[test]
fn get_result_survives_later_mutation() {
    // §9.6.5 / E11: a get result is an OWNED copy — a subsequent set/del of the
    // same key does not change a value already obtained. (Stricter than the JS
    // MemoryStorage, which aliases the stored value — spec 10 §3.2 inv.3.)
    let s = MemoryStorage::new();
    s.set("k", cap(1, &["a"]));
    let earlier = s.get("k").unwrap();

    s.set("k", cap(9, &["a", "b", "c"])); // mutate the slot
    s.del("k"); // and remove it

    assert!(
        sv_eq(&earlier, &cap(1, &["a"])),
        "owned get result must be independent"
    );
}

#[test]
fn scan_keys_are_unique_and_strictly_ascending() {
    // §9.6.4 — overwrites don't duplicate keys; order is strictly ascending.
    let s = MemoryStorage::new();
    s.set("a", cap(1, &["x"]));
    s.set("a", cap(2, &["y"])); // overwrite
    s.set("b", cap(1, &["z"]));

    let keys = scan_keys(&s, "");
    assert_eq!(keys, ["a", "b"]);
    for w in keys.windows(2) {
        assert!(w[0] < w[1], "scan keys must be strictly ascending");
    }
}

#[test]
fn empty_prefix_equals_full_keyspace() {
    let s = MemoryStorage::new();
    for k in ["m", "a", "z"] {
        s.set(k, cap(1, &[k]));
    }
    assert_eq!(scan_keys(&s, ""), ["a", "m", "z"]);
}

// ===========================================================================
// Factory + a backend-agnostic harness (seeds the §9.2 cross-backend diff:
// when OpStorage/sqlite lands, run `exercise` against it too and assert parity)
// ===========================================================================

/// A fixed op sequence run through *any* `&dyn Storage`, returning observable
/// results. Reused by the cross-backend equivalence test (§9.2) once the SQLite
/// backend exists; for now it pins the trait-object (object-safety) path.
fn exercise(s: &dyn Storage) -> (Vec<String>, Option<StorageValue>) {
    s.set(
        "take:0",
        StorageValue::Take {
            size: 2,
            bound: Some(bound_row(1, "a")),
        },
    );
    s.set(
        "take:10",
        StorageValue::Take {
            size: 0,
            bound: None,
        },
    );
    s.set("take:2", cap(4, &["k"])); // a Cap value under a take-ish key — store is value-agnostic
    s.set("maxBound", StorageValue::Bound(bound_row(7, "hi")));
    s.del("take:10");
    let take_keys = scan_keys(s, "take:");
    let mb = s.get("maxBound");
    (take_keys, mb)
}

#[test]
fn create_storage_factory_and_dyn_object() {
    let s = create_storage(); // Box<dyn Storage>
    let (take_keys, mb) = exercise(&*s);
    // "take:0" and "take:2" survive ("take:10" deleted); byte order: '0' < '2'.
    assert_eq!(take_keys, ["take:0", "take:2"]);
    assert!(sv_eq(
        &mb.unwrap(),
        &StorageValue::Bound(bound_row(7, "hi"))
    ));
}

#[test]
fn memory_and_factory_agree() {
    // The same sequence on a directly-constructed MemoryStorage agrees with the
    // boxed factory store — the local shape of the §9.2 cross-backend diff.
    let direct = MemoryStorage::new();
    let factory = create_storage();
    let a = exercise(&direct);
    let b = exercise(&*factory);
    assert_eq!(a.0, b.0);
    match (a.1, b.1) {
        (Some(x), Some(y)) => assert!(sv_eq(&x, &y)),
        (None, None) => {}
        _ => panic!("backends disagree on maxBound"),
    }
}

#[test]
fn clone_data_is_a_snapshot() {
    let s = MemoryStorage::new();
    s.set("k", cap(1, &["a"]));
    let snap = s.clone_data();
    s.set("k", cap(2, &["a", "b"])); // mutate after snapshot
    assert!(
        sv_eq(snap.get("k").unwrap(), &cap(1, &["a"])),
        "snapshot must be independent"
    );
}
