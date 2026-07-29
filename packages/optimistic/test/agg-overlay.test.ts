// The pure aggregate overlay (AGGREGATE-SYNC-DESIGN.md §4–§6): the agg-def derivation +
// static `evaluable` gate (§6.2), the positional predicate evaluator, and the per-group
// delta accumulator. No engine — pure logic.

import { test } from "node:test";
import assert from "node:assert/strict";

import type { Ast, Condition } from "@rindle/client";
import { aggTableName } from "@rindle/normalized";
import { AggOverlay, collectAggDefs, evalCondition, isEvaluable } from "../src/agg-overlay.ts";

const COMMENT_COLS = ["id", "issueID", "body"];
const getColumns = (t: string): string[] | undefined => (t === "comment" ? COMMENT_COLS : undefined);

function countAst(where?: Condition): Ast {
  return {
    table: "issue",
    related: [
      {
        correlation: { parentField: ["id"], childField: ["issueID"] },
        subquery: { table: "comment", alias: "commentCount", aggregate: "count", ...(where ? { where } : {}) },
      },
    ],
  };
}

const bodyNe = (v: string): Condition => ({
  type: "simple",
  op: "!=",
  left: { type: "column", name: "body" },
  right: { type: "literal", value: v },
});

// --- agg-def derivation -------------------------------------------------------

test("collectAggDefs derives the child table, group-key indices, and evaluable flag", () => {
  const [def, ...rest] = collectAggDefs(countAst(), getColumns);
  assert.equal(rest.length, 0);
  assert.equal(def.aggTable, aggTableName(countAst().related![0]));
  assert.equal(def.childTable, "comment");
  assert.deepEqual(def.groupKeyCols, [1]); // issueID is column index 1
  assert.equal(def.evaluable, true); // no filter
});

test("collectAggDefs skips an aggregate whose child table is not in the client schema", () => {
  const ast = countAst();
  ast.related![0].subquery.table = "reaction"; // getColumns returns undefined
  assert.deepEqual(collectAggDefs(ast, getColumns), []);
});

// --- the §6.2 evaluable gate --------------------------------------------------

test("evaluable: child-column scalar filter is evaluable; LIKE / foreign col / subquery are not", () => {
  assert.equal(isEvaluable(bodyNe("x"), COMMENT_COLS), true);

  // LIKE is deferred → unevaluable (server-value fallback).
  assert.equal(
    isEvaluable({ type: "simple", op: "LIKE", left: { type: "column", name: "body" }, right: { type: "literal", value: "a%" } }, COMMENT_COLS),
    false,
  );
  // A column outside the child schema → unevaluable.
  assert.equal(
    isEvaluable({ type: "simple", op: "=", left: { type: "column", name: "author" }, right: { type: "literal", value: 1 } }, COMMENT_COLS),
    false,
  );
  // A correlatedSubquery (a join the client may not hold) → unevaluable.
  assert.equal(
    isEvaluable(
      { type: "correlatedSubquery", op: "EXISTS", related: { correlation: { parentField: ["id"], childField: ["x"] }, subquery: { table: "t" } } },
      COMMENT_COLS,
    ),
    false,
  );
  // and/or short-circuit to the strictest leaf.
  assert.equal(isEvaluable({ type: "and", conditions: [bodyNe("x"), bodyNe("y")] }, COMMENT_COLS), true);
  assert.equal(
    isEvaluable({ type: "or", conditions: [bodyNe("x"), { type: "simple", op: "LIKE", left: { type: "column", name: "body" }, right: { type: "literal", value: "a%" } }] }, COMMENT_COLS),
    false,
  );
});

// --- the predicate evaluator --------------------------------------------------

test("evalCondition: scalar ops over a positional child row with SQL null semantics", () => {
  const row = (body: unknown) => [1, 5, body] as (string | number | null)[];
  assert.equal(evalCondition(bodyNe("x"), row("y"), COMMENT_COLS), true);
  assert.equal(evalCondition(bodyNe("x"), row("x"), COMMENT_COLS), false);

  const eqHi: Condition = { type: "simple", op: "=", left: { type: "column", name: "body" }, right: { type: "literal", value: "hi" } };
  assert.equal(evalCondition(eqHi, row("hi"), COMMENT_COLS), true);
  assert.equal(evalCondition(eqHi, row(null), COMMENT_COLS), false, "= with NULL → false");

  const isNull: Condition = { type: "simple", op: "IS", left: { type: "column", name: "body" }, right: { type: "literal", value: null } };
  assert.equal(evalCondition(isNull, row(null), COMMENT_COLS), true, "IS NULL → true on null");
  assert.equal(evalCondition(isNull, row("x"), COMMENT_COLS), false);

  const gt3: Condition = { type: "simple", op: ">", left: { type: "column", name: "issueID" }, right: { type: "literal", value: 3 } };
  assert.equal(evalCondition(gt3, [1, 5, "a"], COMMENT_COLS), true);
  assert.equal(evalCondition(gt3, [1, 2, "a"], COMMENT_COLS), false);
});

// --- the delta accumulator ----------------------------------------------------

test("AggOverlay folds add/remove/edit into a per-group delta", () => {
  const ov = new AggOverlay();
  ov.register(collectAggDefs(countAst(), getColumns));
  const AGG = aggTableName(countAst().related![0]);
  assert.equal(ov.hasChild("comment"), true);
  assert.equal(ov.hasChild("issue"), false);

  ov.observe({ table: "comment", kind: "add", row: [10, 5, "a"] }); // group 5 +1
  ov.observe({ table: "comment", kind: "add", row: [11, 5, "b"] }); // group 5 +1
  ov.observe({ table: "comment", kind: "add", row: [12, 7, "c"] }); // group 7 +1
  ov.observe({ table: "comment", kind: "remove", row: [11, 5, "b"] }); // group 5 −1

  const byGroup = Object.fromEntries(ov.entries().map((e) => [JSON.stringify(e.cells), e.n]));
  assert.deepEqual(byGroup, { "[5]": 1, "[7]": 1 });

  // An edit that moves a comment across groups: −1 old, +1 new.
  ov.observe({ table: "comment", kind: "edit", old: [12, 7, "c"], row: [12, 9, "c"] });
  const after = Object.fromEntries(ov.entries().map((e) => [JSON.stringify(e.cells), e.n]));
  assert.deepEqual(after, { "[5]": 1, "[7]": 0, "[9]": 1 });

  ov.pruneZeros();
  assert.deepEqual(Object.fromEntries(ov.entries().map((e) => [JSON.stringify(e.cells), e.n])), { "[5]": 1, "[9]": 1 });

  assert.equal(ov.entries()[0].aggTable, AGG);
});

test("AggOverlay honors a child filter — only members count; reset clears", () => {
  const ov = new AggOverlay();
  ov.register(collectAggDefs(countAst(bodyNe("spam")), getColumns));

  ov.observe({ table: "comment", kind: "add", row: [1, 5, "ok"] }); // member → +1
  ov.observe({ table: "comment", kind: "add", row: [2, 5, "spam"] }); // filtered out → 0
  assert.deepEqual(ov.entries().map((e) => e.n), [1]);

  ov.reset();
  assert.deepEqual(ov.entries(), []);
});

test("an unevaluable filter contributes no delta (server-value fallback, §6.2)", () => {
  const likeBody: Condition = { type: "simple", op: "LIKE", left: { type: "column", name: "body" }, right: { type: "literal", value: "a%" } };
  const ov = new AggOverlay();
  ov.register(collectAggDefs(countAst(likeBody), getColumns));
  ov.observe({ table: "comment", kind: "add", row: [1, 5, "abc"] });
  assert.deepEqual(ov.entries(), [], "no optimistic delta for an unevaluable aggregate");
});

test("unregister drops the def, its child dispatch, and any accumulated delta (§4)", () => {
  const AGG = aggTableName(countAst().related![0]);
  const ov = new AggOverlay();
  ov.register(collectAggDefs(countAst(), getColumns));
  ov.observe({ table: "comment", kind: "add", row: [1, 5, "a"] }); // group 5 +1
  assert.equal(ov.hasDefs, true);
  assert.equal(ov.hasChild("comment"), true);
  assert.equal(ov.entries().length, 1);

  ov.unregister(AGG);
  assert.equal(ov.hasDefs, false, "def gone");
  assert.equal(ov.hasChild("comment"), false, "child dispatch gone → observe is a no-op");
  assert.deepEqual(ov.entries(), [], "accumulated delta gone");

  // After unregister, an observed child op is ignored (no stale def picks it up).
  ov.observe({ table: "comment", kind: "add", row: [2, 5, "b"] });
  assert.deepEqual(ov.entries(), []);

  ov.unregister(AGG); // idempotent
  ov.unregister("__agg_nonexistent"); // unknown → no-op
});
