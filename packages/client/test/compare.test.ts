import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

import { compareRows, compareString, compareValue } from "../src/compare.ts";
import type { ColType, WireValue } from "../src/types.ts";

const here = dirname(fileURLToPath(import.meta.url));

type Vector = { type: ColType | "null"; a: unknown; b: unknown; expected: number };
const vectors: Vector[] = JSON.parse(
  readFileSync(join(here, "fixtures", "compare-vectors.json"), "utf8"),
);

/** Decode the fixture's exact-f64 number encoding (`{ bits: "0x…" }`) back to a JS number. */
function f64FromBitsHex(hex: string): number {
  const dv = new DataView(new ArrayBuffer(8));
  dv.setBigUint64(0, BigInt(hex));
  return dv.getFloat64(0);
}

/** A fixture-encoded operand → the BARE value the comparator actually sees. */
function decode(encd: unknown, type: ColType): WireValue {
  if (encd === null) return null;
  if (type === "number" && typeof encd === "object") {
    return f64FromBitsHex((encd as { bits: string }).bits);
  }
  return encd as WireValue;
}

test("comparator matches the engine over Rust-dumped vectors", () => {
  assert.ok(vectors.length > 0, "fixture has vectors");
  for (const v of vectors) {
    // A `null`-typed case is the null/null pair; column type is then irrelevant.
    const type: ColType = v.type === "null" ? "number" : v.type;
    const a = decode(v.a, type);
    const b = decode(v.b, type);
    const got = compareValue(a, b);
    assert.strictEqual(
      got,
      v.expected,
      `compareValue(${JSON.stringify(a)}, ${JSON.stringify(b)}, ${type}) — engine said ${v.expected}, got ${got}`,
    );
  }
});

test("the supplementary-plane string trap (UTF-8 byte order, not UTF-16)", () => {
  // 😀 (U+1F600, supplementary) vs ﬀ (U+FB00, BMP): UTF-8 ⇒ 😀 > ﬀ.
  assert.strictEqual(compareString("\u{1F600}", "\u{FB00}"), 1, "UTF-8 bytewise: 😀 > ﬀ");
  // Sanity: the naive JS `<` gives the WRONG answer here (UTF-16 code-unit order).
  assert.strictEqual("\u{1F600}" < "\u{FB00}", true, "JS `<` wrongly says 😀 < ﬀ");
});

test("compareRows honors column order and a descending column", () => {
  const sort: [number, boolean][] = [
    [0, true], // col0 ascending
    [1, false], // tie-break col1 descending
  ];
  assert.strictEqual(compareRows(["a", 1], ["a", 2], sort), 1, "same c0; larger c1 sorts first (desc)");
  assert.strictEqual(compareRows(["a", 9], ["b", 1], sort), -1, "c0 decides");
  assert.strictEqual(compareRows(["a", 5], ["a", 5], sort), 0, "equal rows");
});
