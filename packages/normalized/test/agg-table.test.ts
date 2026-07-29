import { test } from "node:test";
import assert from "node:assert/strict";

import type { Ast, CorrelatedSubquery } from "@rindle/client";
import { aggTableName, aggTableSchemas, rewriteAggregates } from "../src/agg-table.ts";

// `issue { commentCount: count(comment) }` — comment.issue_id ↔ issue.id, no filter.
function countAst(): Ast {
  return {
    table: "issue",
    related: [
      {
        correlation: { parentField: ["id"], childField: ["issue_id"] },
        subquery: { table: "comment", alias: "commentCount", aggregate: "count" },
      },
    ],
  };
}
const rel0 = (a: Ast): CorrelatedSubquery => a.related![0];

// --- the content-hash NAME (the byte-exact twin of Rust `agg_table_name`) ---

test("aggTableName cross-language vector (must match Rust agg_table_name)", () => {
  // Pinned in BOTH languages: `rindle-replica` test `agg_table_name_cross_language_vector`
  // asserts the same string for the structurally-identical AST. A drift in either FNV byte
  // protocol breaks one side — client and server would name the synthetic table differently
  // and the rows would never route.
  assert.strictEqual(aggTableName(rel0(countAst())), "__agg_725f6575e4f86856");
});

test("aggTableName cross-language vector — integer-literal filter takes the int plane", () => {
  // The exact-int plane (design 226 Stage B), pinned against Rust
  // `agg_table_name_cross_language_vector_integer_literal_filter`: a JS integral
  // number serializes as a JSON integer token, which serde deserializes as
  // `Lit::Int` and hashes tag 5 + exact i64 LE bytes — so `hashLit` must route
  // integral in-i64-range numbers onto the same plane. Under the old tag-2+f64
  // hashing the client would name a DIFFERENT synthetic table from the server's
  // and the local aggregate would stay empty.
  const filtered = (value: number): CorrelatedSubquery => ({
    correlation: { parentField: ["id"], childField: ["issue_id"] },
    subquery: {
      table: "comment",
      aggregate: "count",
      where: {
        type: "simple",
        op: "=",
        left: { type: "column", name: "score" },
        right: { type: "literal", value },
      },
    },
  });
  // `score = 5` — the canonical small-integer literal.
  assert.strictEqual(aggTableName(filtered(5)), "__agg_386c48c5334e7707");
  // 2^60 — above 2^53 the WIRE TOKEN is what serde parses, and JS serializes the
  // shortest round-trip decimal: `2 ** 60` goes out as "1152921504606847000", not the
  // binary-exact ...846976. The Rust vector pins this same name for
  // `Lit::Int(1152921504606847000)`; hashing `BigInt(2 ** 60)` instead would produce
  // `__agg_3908e94b423e01c2` and the aggregate would never route.
  assert.strictEqual(JSON.stringify(2 ** 60), "1152921504606847000");
  assert.strictEqual(aggTableName(filtered(2 ** 60)), "__agg_8eeccc486bcc3c3a");
  // A non-integral literal stays the f64 plane (tag 2) on both sides.
  assert.strictEqual(aggTableName(filtered(2.5)), "__agg_e504b86c532033e7");
});

test("aggTableName is stable and definition-sensitive", () => {
  const base = aggTableName(rel0(countAst()));
  assert.strictEqual(base, aggTableName(rel0(countAst())));
  assert.match(base, /^__agg_[0-9a-f]{16}$/);

  // A child filter → a different table (else filtered/unfiltered counts collide on (table,pk)).
  const filtered = countAst();
  rel0(filtered).subquery.where = {
    type: "simple",
    op: "!=",
    left: { type: "column", name: "body" },
    right: { type: "literal", value: "x" },
  };
  assert.notStrictEqual(aggTableName(rel0(filtered)), base);

  // A different group key (correlation child) → a different table.
  const regrouped = countAst();
  rel0(regrouped).correlation.childField = ["author_id"];
  assert.notStrictEqual(aggTableName(rel0(regrouped)), base);

  // A different child table → a different table.
  const other = countAst();
  rel0(other).subquery.table = "reaction";
  assert.notStrictEqual(aggTableName(rel0(other)), base);

  // The PARENT correlation field is excluded — same count per group key, so two parents share.
  const otherParent = countAst();
  rel0(otherParent).correlation.parentField = ["other_id"];
  assert.strictEqual(aggTableName(rel0(otherParent)), base);
});

// --- the synthetic SCHEMA ---

test("aggTableSchemas derives [childField…, count] with the group key as PK", () => {
  const tables = aggTableSchemas(countAst());
  assert.strictEqual(tables.length, 1);
  assert.deepStrictEqual(tables[0], {
    name: aggTableName(rel0(countAst())),
    columns: ["issue_id", "count"],
    primaryKey: [0],
  });
});

test("aggTableSchemas finds a nested aggregate under a materialized relationship", () => {
  const ast: Ast = {
    table: "issue",
    related: [
      {
        correlation: { parentField: ["id"], childField: ["issue_id"] },
        subquery: {
          table: "comment",
          alias: "comments",
          related: [
            {
              correlation: { parentField: ["id"], childField: ["comment_id"] },
              subquery: { table: "reaction", alias: "reactionCount", aggregate: "count" },
            },
          ],
        },
      },
    ],
  };
  const tables = aggTableSchemas(ast);
  assert.strictEqual(tables.length, 1);
  assert.deepStrictEqual(tables[0].columns, ["comment_id", "count"]);
});

// --- the local-engine AST REWRITE ---

test("rewriteAggregates turns a count into a precomputed source-backed singular relationship", () => {
  const local = rewriteAggregates(countAst());
  const r = rel0(local);
  // Same alias + correlation, but the subquery now reads the synthetic table (no reduce).
  assert.strictEqual(r.subquery.alias, "commentCount");
  assert.deepStrictEqual(r.correlation, { parentField: ["id"], childField: ["issue_id"] });
  assert.strictEqual(r.subquery.table, aggTableName(rel0(countAst())));
  assert.strictEqual(r.subquery.aggregate, "count");
  assert.strictEqual(r.subquery.aggregatePrecomputed, true);
  // The original child table + filter are gone (the count is precomputed server-side).
  assert.strictEqual(r.subquery.where, undefined);
});

test("rewriteAggregates leaves an ordinary relationship untouched and does not mutate input", () => {
  const ast: Ast = {
    table: "issue",
    related: [
      {
        correlation: { parentField: ["id"], childField: ["issue_id"] },
        subquery: { table: "comment", alias: "comments" },
      },
    ],
  };
  const before = JSON.stringify(ast);
  const local = rewriteAggregates(ast);
  assert.strictEqual(local.related![0].subquery.table, "comment");
  assert.strictEqual(local.related![0].subquery.aggregate, undefined);
  assert.strictEqual(JSON.stringify(ast), before, "input AST not mutated");
});

test("a bigint literal refuses informatively (no wire token exists to mirror)", () => {
  const filtered = (value: unknown): CorrelatedSubquery => ({
    correlation: { parentField: ["id"], childField: ["issue_id"] },
    subquery: {
      table: "comment",
      aggregate: "count",
      where: {
        type: "simple",
        op: "=",
        left: { type: "column", name: "score" },
        right: { type: "literal", value: value as never },
      },
    },
  });
  // Pre-fix this fell into the array branch and died on `lit.length`/iteration with
  // an uninformative TypeError from inside the hash.
  assert.throws(() => aggTableName(filtered(5n)), /bigint literals.*design 226/s);
});

test("a non-finite literal hashes as the null it becomes on the wire", () => {
  // `JSON.stringify(NaN)` emits `null` — there is no NaN token — so the server
  // parses `Lit::Null` and hashes the null tag. Pre-fix the client hashed tag-2 +
  // f64 bits, naming a different synthetic table from the server's, and the
  // aggregate rows never routed.
  const filtered = (value: unknown): CorrelatedSubquery => ({
    correlation: { parentField: ["id"], childField: ["issue_id"] },
    subquery: {
      table: "comment",
      aggregate: "count",
      where: {
        type: "simple",
        op: "=",
        left: { type: "column", name: "score" },
        right: { type: "literal", value: value as never },
      },
    },
  });
  assert.strictEqual(JSON.parse(JSON.stringify({ v: NaN })).v, null);
  const asNull = aggTableName(filtered(null));
  assert.strictEqual(aggTableName(filtered(NaN)), asNull);
  assert.strictEqual(aggTableName(filtered(Infinity)), asNull);
  assert.strictEqual(aggTableName(filtered(-Infinity)), asNull);
});
