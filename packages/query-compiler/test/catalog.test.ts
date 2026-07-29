import { test } from "node:test";
import assert from "node:assert/strict";

import type { Catalog } from "../src/index.ts";
import { issueTracker } from "./fixtures.ts";

/** Structural invariants every catalog must satisfy for the compiler to consume it. */
function assertWellFormed(catalog: Catalog): void {
  for (const [name, t] of Object.entries(catalog.tables)) {
    assert.ok(t.primaryKey.length > 0, `${name}: primary key must be non-empty`);
    const cols = new Set(t.columns);
    assert.equal(cols.size, t.columns.length, `${name}: duplicate column`);
    for (const pk of t.primaryKey) {
      assert.ok(cols.has(pk), `${name}: pk column "${pk}" is not in columns`);
    }
    for (const c of t.columns) {
      const ct = t.columnTypes[c];
      assert.ok(ct, `${name}: column "${c}" has no columnType`);
      assert.ok(ct.type.length > 0, `${name}.${c}: columnType.type must be non-empty`);
    }
    for (const [rel, card] of Object.entries(t.relationships)) {
      assert.ok(card === "one" || card === "many", `${name}.${rel}: bad cardinality "${card}"`);
    }
  }
}

test("issueTracker is a well-formed catalog", () => {
  assertWellFormed(issueTracker);
});

test("issueTracker exercises every Postgres cast branch (§6.2)", () => {
  const types = new Set<string>();
  let hasEnum = false;
  let hasArray = false;
  for (const t of Object.values(issueTracker.tables)) {
    for (const ct of Object.values(t.columnTypes)) {
      types.add(ct.type);
      hasEnum ||= ct.isEnum;
      hasArray ||= ct.isArray;
    }
  }
  for (const needed of [
    "uuid",
    "text",
    "bool",
    "int4",
    "float8",
    "numeric",
    "timestamptz",
    "date",
    "timetz",
    "jsonb",
  ]) {
    assert.ok(types.has(needed), `fixture should exercise the "${needed}" cast branch`);
  }
  assert.ok(hasEnum, "fixture should exercise an enum column");
  assert.ok(hasArray, "fixture should exercise an array column");
});
