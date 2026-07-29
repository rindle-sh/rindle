// 302 §2 point 3 — `remapAstTables`, the room-homed view's rewrite. It walks the KNOWN wire-AST
// shape (root / related / correlatedSubquery), never a blind `table`-key scan: `start.row` is a
// COLUMN-keyed record (Bound.row — `Record<columnName, LitValue>`), so a schema column literally
// named `table` used as a paging bound must keep its bound VALUE, and a literal condition value
// that happens to equal a mapped table name must survive untouched. The blind-scan regression
// silently paged the swapped view from a bound no row carries — a view-after-write violation
// with no error anywhere.

import { test } from "node:test";
import assert from "node:assert/strict";

import type { Ast, Condition } from "@rindle/client";
import { remapAstTables } from "../src/backend.ts";

const map = new Map([
  ["note", "note@room:d"],
  ["comment", "comment@room:d"],
]);

test("remapAstTables renames root/related/EXISTS tables; column-keyed bounds and literal values keep their cells", () => {
  const ast: Ast = {
    table: "note",
    // A schema column literally named `table`, used as a paging bound — a bound CELL, not a ref.
    start: { row: { table: "note", id: 7 }, exclusive: true },
    where: {
      type: "and",
      conditions: [
        // A literal VALUE equal to a mapped table name — data, not a reference.
        { type: "simple", op: "=", left: { type: "column", name: "table" }, right: { type: "literal", value: "note" } },
        {
          type: "correlatedSubquery",
          op: "EXISTS",
          related: { correlation: { parentField: ["id"], childField: ["noteId"] }, subquery: { table: "comment" } },
        },
      ],
    },
    related: [
      {
        correlation: { parentField: ["id"], childField: ["noteId"] },
        subquery: { table: "comment", orderBy: [["id", "asc"]] },
      },
    ],
    orderBy: [["id", "asc"]],
  };
  const snapshot = JSON.parse(JSON.stringify(ast)) as Ast;

  const out = remapAstTables(ast, map);
  assert.equal(out.table, "note@room:d", "root renamed");
  assert.equal(out.related![0].subquery.table, "comment@room:d", "related subquery renamed");
  const [litCond, existsCond] = (out.where as Extract<Condition, { type: "and" }>).conditions;
  assert.equal(
    (existsCond as Extract<Condition, { type: "correlatedSubquery" }>).related.subquery.table,
    "comment@room:d",
    "EXISTS subquery renamed",
  );
  assert.deepEqual(
    out.start,
    { row: { table: "note", id: 7 }, exclusive: true },
    "start.row is COLUMN-keyed — the bound cell for a column named `table` is not a table reference",
  );
  assert.deepEqual(
    (litCond as Extract<Condition, { type: "simple" }>).right,
    { type: "literal", value: "note" },
    "a literal value equal to a mapped table name is data, untouched",
  );
  assert.deepEqual(ast, snapshot, "structural clone — the input AST is never mutated");
});

test("remapAstTables leaves unmapped tables alone (the client-side join across kinds)", () => {
  const ast: Ast = {
    table: "note",
    related: [{ correlation: { parentField: ["id"], childField: ["nid"] }, subquery: { table: "acl" } }],
  };
  const out = remapAstTables(ast, map);
  assert.equal(out.table, "note@room:d");
  assert.equal(out.related![0].subquery.table, "acl", "a non-owned (context) table keeps its plain name");
});
