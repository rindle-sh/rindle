//! Spec `10` — SQLite operator-storage suite (`DatabaseStorage` / `OpStorage`),
//! cross-checked against the in-RAM `MemoryStorage`. The shared helpers
//! (`exercise`/`sv_eq`/…) mirror `rindle`'s `tests/storage.rs`; `StorageValue` has no
//! `PartialEq`, so equality is spelled out via `compare_values` (null == null).

use std::cmp::Ordering;
use std::rc::Rc;

use rindle::storage::{MemoryStorage, ReduceAcc, Storage, StorageFactory, StorageValue};
use rindle::{compare_values, owned_row, OwnedRow, OwnedValue};
use rindle_sqlite::DatabaseStorage;

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
        (
            Reduce {
                count: c1,
                accs: a1,
            },
            Reduce {
                count: c2,
                accs: a2,
            },
        ) => c1 == c2 && a1 == a2,
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
fn sqlite_op_storage_agrees_with_memory() {
    let db = DatabaseStorage::new_in_memory().unwrap();
    let sqlite = db.create_storage();
    let memory = MemoryStorage::new();

    let a = exercise(&memory);
    let b = exercise(&sqlite);
    assert_eq!(a.0, b.0);
    match (a.1, b.1) {
        (Some(x), Some(y)) => assert!(sv_eq(&x, &y)),
        (None, None) => {}
        _ => panic!("backends disagree on maxBound"),
    }
}

#[test]
fn sqlite_round_trips_all_row_value_variants() {
    let db = DatabaseStorage::new_in_memory().unwrap();
    let s = db.create_storage();
    let row = owned_row(vec![
        OwnedValue::Null,
        OwnedValue::Bool(true),
        OwnedValue::Int(-5),
        OwnedValue::Float(-0.0),
        OwnedValue::str("txt"),
        OwnedValue::Json(r#"{"a":1}"#.into()),
    ]);

    s.set("row", StorageValue::Bound(row.clone()));
    assert_get(&s, "row", Some(StorageValue::Bound(row)));
}

#[test]
fn sqlite_round_trips_reduce_accumulator() {
    let db = DatabaseStorage::new_in_memory().unwrap();
    let s = db.create_storage();

    // A plain count: the accumulator is just the running row count, no per-agg state.
    let count_only = StorageValue::Reduce {
        count: 42,
        accs: Vec::new(),
    };
    s.set("reduce", count_only.clone());
    assert_get(&s, "reduce", Some(count_only));

    // A negative/zero count round-trips too (i64, not u32).
    let zero = StorageValue::Reduce {
        count: 0,
        accs: Vec::new(),
    };
    s.set("reduce", zero.clone());
    assert_get(&s, "reduce", Some(zero));

    // A sum/avg accumulator (int + float sums, non-null and float counts) round-trips,
    // including the f64 field's exact bits.
    let with_accs = StorageValue::Reduce {
        count: 7,
        accs: vec![
            ReduceAcc {
                int_sum: 15,
                float_sum: 2.5,
                non_null: 6,
                float_count: 1,
            },
            ReduceAcc {
                int_sum: -3,
                float_sum: -0.0,
                non_null: 7,
                float_count: 0,
            },
        ],
    };
    s.set("reduce", with_accs.clone());
    assert_get(&s, "reduce", Some(with_accs));
}

#[test]
fn sqlite_op_ids_isolate_identical_keys() {
    // Two operators over the same database get distinct auto-incrementing `op_id`s, so
    // identical keys never collide — the guarantee that lets one shared `storage(op, key)`
    // table back every operator without a client-group column.
    let db = DatabaseStorage::new_in_memory().unwrap();
    let a = db.create_storage();
    let b = db.create_storage();

    a.set("k", cap(1, &["a"]));
    b.set("k", cap(2, &["b", "c"]));

    assert_get(&a, "k", Some(cap(1, &["a"])));
    assert_get(&b, "k", Some(cap(2, &["b", "c"])));
}

#[test]
fn sqlite_clear_drops_only_its_own_op_keyspace() {
    let db = DatabaseStorage::new_in_memory().unwrap();
    let a = db.create_storage();
    let b = db.create_storage();

    a.set("k", cap(1, &["a"]));
    b.set("k", cap(1, &["b"]));
    a.clear();

    assert_get(&a, "k", None);
    assert_get(&b, "k", Some(cap(1, &["b"])));
}

#[test]
fn sqlite_storage_factory_vends_isolated_op_storage() {
    let db = DatabaseStorage::new_in_memory().unwrap();
    let factory = StorageFactory::custom(Rc::new(db));
    let a = factory.create_storage();
    let b = factory.create_storage();

    a.set("k", cap(1, &["a"]));
    b.set("k", cap(1, &["b"]));

    assert_get(&*a, "k", Some(cap(1, &["a"])));
    assert_get(&*b, "k", Some(cap(1, &["b"])));
}
