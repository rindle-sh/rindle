//! Design 226 Stage B canaries (§11): the exact `Int`↔`Float` comparator.
//!
//! The load-bearing claim: **below 2^53 the exact and widened orders are bitwise
//! identical**, because `i64 → f64` is lossless there — that equivalence is what
//! makes Stage B landable without a data migration. Above 2^53 the two orders
//! deliberately diverge: the widened comparison made two distinct `Int`s compare
//! Equal to the same `Float` (non-transitive — 226 §3); the exact one does not.

use std::cmp::Ordering;

use rindle::value::{
    compare_int_f64, compare_values, float_int_class, owned_row, values_equal, values_identical,
    OwnedValue,
};

const TWO_53: i64 = 1 << 53;

fn widened(x: i64, y: f64) -> Ordering {
    (x as f64).total_cmp(&y)
}

/// Deterministic pseudo-random stream (an xorshift64*) — no `rand` dependency and
/// reproducible across runs.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
}

/// §11 "Order equivalence": across a large deterministic sample of pairs with
/// |Int| < 2^53, the exact comparator returns bitwise the same `Ordering` the old
/// `(x as f64).total_cmp(&y)` widening did — for floats of every flavor (near the
/// int, fractional, huge, tiny, ±0.0, ±inf, NaN).
#[test]
fn exact_order_matches_widened_below_2_53() {
    let mut rng = Rng(0x226_5EED);
    let specials = [
        0.0,
        -0.0,
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::NAN,
        -f64::NAN,
        f64::MIN_POSITIVE,
        f64::MAX,
        -f64::MAX,
        0.5,
        -0.5,
    ];
    for i in 0..1_000_000u64 {
        // An int uniformly inside ±(2^53 - 1).
        let x = (rng.next() % (2 * (TWO_53 as u64 - 1) + 1)) as i64 - (TWO_53 - 1);
        // A float: cycle through nearby-integral, fractional, special, and raw-bits
        // (raw bits stress every exponent; NaNs and infs included by construction).
        let y = match i % 4 {
            0 => (rng.next() % (2 * (TWO_53 as u64) + 1)) as i64 as f64 - TWO_53 as f64,
            1 => (x as f64) + ((rng.next() % 2001) as f64 - 1000.0) / 512.0,
            2 => specials[(rng.next() % specials.len() as u64) as usize],
            _ => f64::from_bits(rng.next()),
        };
        assert_eq!(
            compare_int_f64(x, y),
            widened(x, y),
            "Int({x}) vs Float({y}) ({:#x})",
            y.to_bits()
        );
    }
}

/// §5.1's two pinned placements: `Int(0)` vs `Float(-0.0)` stays `Greater` (the
/// `total_cmp` order — `Int` carries `+0.0`'s sign), and NaNs keep their
/// `total_cmp` positions relative to every `Int` (`-NaN` below, `+NaN` above).
#[test]
fn signed_zero_and_nan_placements_are_preserved() {
    assert_eq!(compare_int_f64(0, -0.0), Ordering::Greater);
    assert_eq!(compare_int_f64(0, 0.0), Ordering::Equal);
    for x in [i64::MIN, -1, 0, 1, TWO_53 + 1, i64::MAX] {
        assert_eq!(
            compare_int_f64(x, f64::NAN),
            Ordering::Less,
            "Int({x}) vs +NaN"
        );
        assert_eq!(
            compare_int_f64(x, -f64::NAN),
            Ordering::Greater,
            "Int({x}) vs -NaN"
        );
        assert_eq!(compare_int_f64(x, f64::INFINITY), Ordering::Less);
        assert_eq!(compare_int_f64(x, f64::NEG_INFINITY), Ordering::Greater);
    }
}

/// §11 "Transitivity above 2^53" — the §3 failure, pinned: under widening both
/// `Int(2^53)` and `Int(2^53 + 1)` compared Equal to `Float(2^53)`; exactly, the
/// three values form a strict total order and the two `Int`s are NOT equal.
#[test]
fn distinct_ints_above_2_53_no_longer_collapse() {
    let a = OwnedValue::Int(TWO_53);
    let b = OwnedValue::Int(TWO_53 + 1);
    let f = OwnedValue::Float(TWO_53 as f64);

    assert!(values_equal(a.as_ref(), f.as_ref()), "2^53 == 2^53.0");
    assert!(!values_equal(b.as_ref(), f.as_ref()), "2^53+1 != 2^53.0");
    assert!(!values_equal(a.as_ref(), b.as_ref()), "the §3 failure");
    assert_eq!(compare_values(f.as_ref(), b.as_ref()), Ordering::Less);
    assert_eq!(compare_values(a.as_ref(), b.as_ref()), Ordering::Less);
    assert!(values_identical(a.as_ref(), f.as_ref()));
    assert!(!values_identical(b.as_ref(), f.as_ref()));

    // The extremes stay exact too.
    assert!(!values_equal(
        OwnedValue::Int(i64::MAX).as_ref(),
        OwnedValue::Int(i64::MAX - 1).as_ref()
    ));
    assert!(values_equal(
        OwnedValue::Int(i64::MIN).as_ref(),
        OwnedValue::Float(i64::MIN as f64).as_ref() // -2^63 is exactly representable
    ));
}

/// §11 "Canonical-key coherence", stated on the public halves: `float_int_class(f)
/// == Some(i)` exactly when the exact comparator says `Int(i) == Float(f)`. (The
/// `CanonVal` walker consumes this — its own pin lives beside it in `join_util`.)
#[test]
fn float_int_class_matches_exact_equality() {
    let mut rng = Rng(0xC0_FFEE);
    for i in 0..1_000_000u64 {
        let f = match i % 3 {
            0 => (rng.next() as i64 % (TWO_53 * 2)) as f64 / 2.0, // half-integers + ints
            1 => f64::from_bits(rng.next()),
            _ => (rng.next() as i64) as f64, // huge integral floats, in and out of range
        };
        match float_int_class(f) {
            Some(cls) => assert_eq!(
                compare_int_f64(cls, f),
                Ordering::Equal,
                "class Some({cls}) must be exactly equal to {f}"
            ),
            None => {
                // No i64 is exactly equal to f: probe the only candidate (the
                // truncation) when it exists.
                if f.is_finite()
                    && (-9_223_372_036_854_775_808.0..9_223_372_036_854_775_808.0).contains(&f)
                {
                    assert_ne!(
                        compare_int_f64(f.trunc() as i64, f),
                        Ordering::Equal,
                        "{f} has no class yet compares equal to its truncation"
                    );
                }
            }
        }
    }
    // The §5.4 pins.
    assert_eq!(float_int_class(0.0), Some(0));
    assert_eq!(float_int_class(-0.0), None, "-0.0 keeps float identity");
    assert_eq!(float_int_class(TWO_53 as f64), Some(TWO_53));
    assert_eq!(float_int_class(f64::NAN), None);
    assert_eq!(float_int_class(f64::INFINITY), None);
    assert_eq!(float_int_class(0.5), None);
    assert_eq!(
        float_int_class(9_223_372_036_854_775_808.0),
        None,
        "2^63 is out of range"
    );
    assert_eq!(
        float_int_class(-9_223_372_036_854_775_808.0),
        Some(i64::MIN),
        "-2^63 IS i64::MIN"
    );
}

/// Rows sort identically before and after for sub-2^53 data — the whole-row form
/// of the equivalence claim (what a view's ordering actually consumes).
#[test]
fn row_ordering_is_unchanged_for_safe_data() {
    let rows = [
        owned_row(vec![OwnedValue::Int(3), OwnedValue::Float(1.5)]),
        owned_row(vec![OwnedValue::Float(3.0), OwnedValue::Float(-0.0)]),
        owned_row(vec![OwnedValue::Int(-7), OwnedValue::Null]),
    ];
    for a in &rows {
        for b in &rows {
            for c in 0..2 {
                let exact = compare_values(a.col(c), b.col(c));
                let via_f64 = match (a.col(c), b.col(c)) {
                    (rindle::value::Value::Int(x), rindle::value::Value::Float(y)) => {
                        Some((x as f64).total_cmp(&y))
                    }
                    (rindle::value::Value::Float(x), rindle::value::Value::Int(y)) => {
                        Some(x.total_cmp(&(y as f64)))
                    }
                    _ => None,
                };
                if let Some(w) = via_f64 {
                    assert_eq!(exact, w);
                }
            }
        }
    }
}
