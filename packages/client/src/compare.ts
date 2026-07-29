// The comparator — a faithful port of the engine's `compare_values` / `compare_rows`
// (src/value.rs §4 of FLAT-CHANGES-DESIGN.md), the single most correctness-critical unit
// (WASM-CLIENT-DESIGN.md §8.1). One fixed total order, NO collation.
//
// It is **value-driven** — it switches on the runtime JS type, exactly as the engine
// switches on the `OwnedValue` variant. Bare wire values collapse Int/Float → number and
// Str/Json → string, but within a sort column values are homogeneous, and number→total_cmp
// / string→bytewise cover both, so this matches the engine without needing column types:
//
//   - null sorts FIRST (null < anything; null == null)
//   - number:  IEEE-754 totalOrder (=== Rust `f64::total_cmp`) — NOT `<` / `-`
//   - boolean: false < true
//   - string:  UTF-8 BYTEWISE (=== SQLite BINARY / Rust `&str` Ord) — NOT `localeCompare`/`<`

import type { WireValue } from "./types.ts";

const enc = new TextEncoder();

/** The `compare_values` / `compare_rows` algorithm-contract version (=== the engine's
 *  `wire_schema::COMPARATOR_VERSION`). A remote subscriber hard-rejects a `hello` whose
 *  `comparatorVersion` differs — the total order is a code contract, not data, so a schema
 *  fingerprint can't cover it. Bump in lockstep with the Rust constant if the order changes.
 *
 *  v2 (design 226 Stage B): the engine's mixed Int/Float comparison became exact instead
 *  of f64-widening. No TS behavior change — every value a client can hold today is an f64
 *  `number`, on which v1 and v2 order identically (the §8 gate keeps int64 cells out of
 *  the browser until Stage E) — but the contract the version names is the engine's. */
export const COMPARATOR_VERSION = 2;

const SIGN = 0x8000000000000000n;
const ALL = 0xffffffffffffffffn;

/** Map an f64 to an unsigned 64-bit key whose unsigned order is IEEE-754 totalOrder
 *  (=== Rust `f64::total_cmp`): flip all bits for negatives (incl. -0 / -NaN), else set
 *  the sign bit. */
function orderedKey(x: number): bigint {
  const dv = new DataView(new ArrayBuffer(8));
  dv.setFloat64(0, x);
  const bits = dv.getBigUint64(0);
  return bits & SIGN ? bits ^ ALL : bits | SIGN;
}

/** number compare with `f64::total_cmp` semantics (NaN deterministic & last; -0 < +0). */
export function compareNumber(a: number, b: number): -1 | 0 | 1 {
  const ka = orderedKey(a);
  const kb = orderedKey(b);
  return ka < kb ? -1 : ka > kb ? 1 : 0;
}

/** UTF-8 bytewise string compare (=== SQLite `BINARY` / Rust `&str` Ord). Correct for
 *  supplementary-plane code points, where JS `<` / `localeCompare` (UTF-16) would disagree. */
export function compareString(a: string, b: string): -1 | 0 | 1 {
  const ba = enc.encode(a);
  const bb = enc.encode(b);
  const n = Math.min(ba.length, bb.length);
  for (let i = 0; i < n; i++) {
    if (ba[i] !== bb[i]) return ba[i] < bb[i] ? -1 : 1;
  }
  return ba.length === bb.length ? 0 : ba.length < bb.length ? -1 : 1;
}

/** Compare two bare cells, dispatching on the runtime type (null sorts first). */
export function compareValue(a: WireValue, b: WireValue): -1 | 0 | 1 {
  const an = a === null || a === undefined;
  const bn = b === null || b === undefined;
  if (an && bn) return 0;
  if (an) return -1;
  if (bn) return 1;
  switch (typeof a) {
    case "number":
      return compareNumber(a, b as number);
    case "boolean":
      return a === b ? 0 : a ? 1 : -1;
    case "string":
      return compareString(a, b as string);
    default:
      // A parsed-JSON object in a sort column (rare): compare its text bytewise.
      return compareString(JSON.stringify(a), JSON.stringify(b));
  }
}

/** Compare two rows by a resolved sort (`[columnIndex, ascending]` pairs). First non-equal
 *  column wins; a descending column negates. */
export function compareRows(a: WireValue[], b: WireValue[], sort: [number, boolean][]): -1 | 0 | 1 {
  for (const [col, asc] of sort) {
    const c = compareValue(a[col], b[col]);
    if (c !== 0) return asc ? c : ((-c) as -1 | 1);
  }
  return 0;
}
