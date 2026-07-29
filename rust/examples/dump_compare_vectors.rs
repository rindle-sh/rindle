//! Dump comparator conformance vectors from the **real engine** `compare_rows`
//! (`src/value.rs`) to JSON on stdout, for the TS `compare.ts` conformance test
//! (`WASM-CLIENT-DESIGN.md` §9, test 1). The expected orderings come from the engine
//! itself, so the TS comparator is checked against ground truth — no hand-authored
//! expectations to get wrong.
//!
//! Numbers are encoded by their exact f64 bit pattern (`{ "bits": "0x…" }`) so NaN, ±0,
//! and ±Inf round-trip losslessly through JSON; strings/bools/null cross plainly.
//!
//!   cargo run --example dump_compare_vectors > packages/client/test/fixtures/compare-vectors.json

use std::cmp::Ordering;

use rindle::{compare_rows, owned_row, OwnedRow, OwnedValue as V};
use serde_json::{json, Value as J};

/// An `OwnedValue` → the **bare** wire JSON the TS side will decode (numbers as exact
/// f64 bits; strings/bools/null plainly).
fn bare(v: &V) -> J {
    match v {
        V::Absent => J::Null, // compare-vector fixtures use full rows; never `Absent`
        V::Null => J::Null,
        V::Bool(b) => json!(*b),
        // wasm sees every number as f64; encode Int as its f64 image for parity.
        V::Int(i) => json!({ "bits": format!("0x{:016x}", (*i as f64).to_bits()) }),
        V::Float(f) => json!({ "bits": format!("0x{:016x}", f.to_bits()) }),
        V::Str(s) => json!(s.as_ref()),
        V::Json(s) => json!(s.as_ref()),
    }
}

/// The engine's ordering of two 1-column rows, as -1 / 0 / 1.
fn ord(a: &OwnedRow, b: &OwnedRow) -> i32 {
    match compare_rows(&vec![(0usize, true)], a, b) {
        Ordering::Less => -1,
        Ordering::Equal => 0,
        Ordering::Greater => 1,
    }
}

fn main() {
    // (column type, a, b). A `null` operand inside a typed column exercises null-first.
    let cases: Vec<(&str, V, V)> = vec![
        ("number", V::Float(1.0), V::Float(2.0)),
        ("number", V::Float(2.0), V::Float(2.0)),
        ("number", V::Float(-0.0), V::Float(0.0)), // total_cmp: -0 < +0
        ("number", V::Float(f64::NAN), V::Float(1.0)), // NaN sorts last
        ("number", V::Float(f64::NEG_INFINITY), V::Float(-1e308)),
        ("number", V::Float(f64::INFINITY), V::Float(1e308)),
        ("number", V::Float(-5.0), V::Float(5.0)),
        ("number", V::Null, V::Float(5.0)), // null-first within a number column
        ("string", V::str("apple"), V::str("banana")),
        ("string", V::str("Z"), V::str("a")), // 'Z'(0x5A) < 'a'(0x61) bytewise (not case-insensitive)
        ("string", V::str("\u{1F600}"), V::str("\u{FB00}")), // 😀 (U+1F600) vs ﬀ (U+FB00): UTF-8 ⇒ 😀 > ﬀ
        ("string", V::str(""), V::str("a")),
        ("string", V::str("cafe"), V::str("caf\u{e9}")), // "cafe" < "café" (prefix; é is multibyte)
        ("string", V::str("x"), V::Null),                // string > null
        ("boolean", V::Bool(false), V::Bool(true)),      // false < true
        ("boolean", V::Bool(true), V::Bool(true)),
        (
            "json",
            V::Json(std::sync::Arc::from("[1,2]")),
            V::Json(std::sync::Arc::from("[1,3]")),
        ), // bytewise
        ("null", V::Null, V::Null),
    ];

    let out: Vec<J> = cases
        .iter()
        .map(|(ty, a, b)| {
            let ra = owned_row(vec![a.clone()]);
            let rb = owned_row(vec![b.clone()]);
            json!({ "type": ty, "a": bare(a), "b": bare(b), "expected": ord(&ra, &rb) })
        })
        .collect();

    println!("{}", serde_json::to_string_pretty(&J::Array(out)).unwrap());
}
