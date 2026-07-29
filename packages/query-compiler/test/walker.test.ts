import { test } from "node:test";
import assert from "node:assert/strict";

import type { Ast, Condition, CorrelatedSubquery, LitValue, SimpleOp, ValuePosition } from "../src/index.ts";
import { CompileError } from "../src/errors.ts";
import { run } from "./oracle.ts";

// ── tiny AST builders ─────────────────────────────────────────────────────────────────────
const col = (name: string): ValuePosition => ({ type: "column", name });
const val = (value: LitValue): ValuePosition => ({ type: "literal", value });
const cmp = (left: ValuePosition, op: SimpleOp, right: ValuePosition): Condition => ({
  type: "simple",
  op,
  left,
  right,
});
const rel = (
  alias: string,
  table: string,
  parentField: string[],
  childField: string[],
  extra: Partial<Ast> = {},
): CorrelatedSubquery => ({
  correlation: { parentField, childField },
  subquery: { table, alias, ...extra },
});

// ── expected base rows (spread into nested expecteds) ───────────────────────────────────────
const I: Record<number, Record<string, unknown>> = {
  10: { id: 10, title: "Login bug", authorId: 1, priority: 3, closed: 0, assignee: "zeta" },
  11: { id: 11, title: "Slow query", authorId: 1, priority: 1, closed: 1, assignee: null },
  12: { id: 12, title: "UI polish", authorId: 2, priority: 2, closed: 0, assignee: null },
  13: { id: 13, title: "Crash", authorId: 3, priority: 3, closed: 0, assignee: "alpha" },
};
const U = {
  1: { id: 1, name: "Alice" },
  2: { id: 2, name: "Bob" },
  3: { id: 3, name: "Carol" },
} as const;
const C: Record<number, Record<string, unknown>> = {
  100: { id: 100, issueId: 10, body: "me too", authorId: 2 },
  101: { id: 101, issueId: 10, body: "fix incoming", authorId: 1 },
  102: { id: 102, issueId: 12, body: "nice", authorId: 3 },
};

// ── plural root / projection / filter ───────────────────────────────────────────────────────

test("all rows, PK-completed order", () => {
  assert.deepEqual(run({ table: "issue" }), [I[10], I[11], I[12], I[13]]);
});

test("projection: select id, title only", () => {
  assert.deepEqual(run({ table: "issue", select: ["id", "title"] }), [
    { id: 10, title: "Login bug" },
    { id: 11, title: "Slow query" },
    { id: 12, title: "UI polish" },
    { id: 13, title: "Crash" },
  ]);
});

test("filter: priority >= 3", () => {
  assert.deepEqual(run({ table: "issue", where: cmp(col("priority"), ">=", val(3)) }), [I[10], I[13]]);
});

test("filter with AND", () => {
  const where: Condition = {
    type: "and",
    conditions: [cmp(col("priority"), ">=", val(3)), cmp(col("closed"), "=", val(0))],
  };
  assert.deepEqual(run({ table: "issue", where }), [I[10], I[13]]);
});

test("empty AND folds to true (keeps all rows)", () => {
  assert.deepEqual(run({ table: "issue", where: { type: "and", conditions: [] } }), [
    I[10], I[11], I[12], I[13],
  ]);
});

// ── order + limit (top-N with correct array order) ──────────────────────────────────────────

test("order by priority desc, limit 2 (PK tiebreak)", () => {
  // priority desc → 3,3,2,1; the prio-3 tie breaks by the completed id asc ⇒ [10, 13].
  assert.deepEqual(run({ table: "issue", orderBy: [["priority", "desc"]], limit: 2 }), [I[10], I[13]]);
});

test("order by nullable column is null-low", () => {
  // assignee asc, null-low: nulls first (11, 12 by id), then "alpha" (13), then "zeta" (10).
  assert.deepEqual(run({ table: "issue", orderBy: [["assignee", "asc"]] }), [I[11], I[12], I[13], I[10]]);
});

// ── relationships (one / many), nested ──────────────────────────────────────────────────────

test("related: author (one) + comments (many)", () => {
  const ast: Ast = {
    table: "issue",
    related: [
      rel("author", "user", ["authorId"], ["id"]),
      rel("comments", "comment", ["id"], ["issueId"]),
    ],
  };
  assert.deepEqual(run(ast), [
    { ...I[10], author: U[1], comments: [C[100], C[101]] },
    { ...I[11], author: U[1], comments: [] },
    { ...I[12], author: U[2], comments: [C[102]] },
    { ...I[13], author: U[3], comments: [] },
  ]);
});

test("count aggregate: commentCount", () => {
  const ast: Ast = {
    table: "issue",
    related: [rel("commentCount", "comment", ["id"], ["issueId"], { aggregate: "count" })],
  };
  assert.deepEqual(run(ast), [
    { ...I[10], commentCount: 2 },
    { ...I[11], commentCount: 0 },
    { ...I[12], commentCount: 1 },
    { ...I[13], commentCount: 0 },
  ]);
});

// ── EXISTS ──────────────────────────────────────────────────────────────────────────────────

test("EXISTS: users who authored an issue (Dave excluded)", () => {
  const where: Condition = {
    type: "correlatedSubquery",
    op: "EXISTS",
    related: rel("authoredIssues", "issue", ["id"], ["authorId"]),
  };
  assert.deepEqual(run({ table: "user", where }), [U[1], U[2], U[3]]);
});

test("NOT EXISTS: users who authored nothing (only Dave)", () => {
  const where: Condition = {
    type: "correlatedSubquery",
    op: "NOT EXISTS",
    related: rel("authoredIssues", "issue", ["id"], ["authorId"]),
  };
  assert.deepEqual(run({ table: "user", where }), [{ id: 4, name: "Dave" }]);
});

// ── IN / NOT IN ───────────────────────────────────────────────────────────────────────────

test("IN list", () => {
  assert.deepEqual(run({ table: "issue", where: cmp(col("id"), "IN", val([10, 12])) }), [I[10], I[12]]);
});

test("empty IN folds to false (no rows)", () => {
  assert.deepEqual(run({ table: "issue", where: cmp(col("id"), "IN", val([])) }), []);
});

test("empty NOT IN folds to true (all rows)", () => {
  assert.deepEqual(run({ table: "issue", where: cmp(col("id"), "NOT IN", val([])) }), [
    I[10], I[11], I[12], I[13],
  ]);
});

// ── IS NULL / IS NOT NULL ───────────────────────────────────────────────────────────────────

test("IS NULL", () => {
  assert.deepEqual(run({ table: "issue", where: cmp(col("assignee"), "IS", val(null)) }), [I[11], I[12]]);
});

test("IS NOT NULL", () => {
  assert.deepEqual(run({ table: "issue", where: cmp(col("assignee"), "IS NOT", val(null)) }), [I[10], I[13]]);
});

// ── keyset paging (start) ─────────────────────────────────────────────────────────────────

test("start exclusive: after id 11", () => {
  assert.deepEqual(run({ table: "issue", start: { row: { id: 11 }, exclusive: true } }), [I[12], I[13]]);
});

test("start inclusive: at/after id 11", () => {
  assert.deepEqual(run({ table: "issue", start: { row: { id: 11 }, exclusive: false } }), [
    I[11], I[12], I[13],
  ]);
});

// ── .one() root (single object / null) ──────────────────────────────────────────────────────

test(".one() returns a single object", () => {
  assert.deepEqual(run({ table: "issue", where: cmp(col("id"), "=", val(10)), one: true }), I[10]);
});

test(".one() with no match returns null", () => {
  assert.equal(run({ table: "issue", where: cmp(col("id"), "=", val(999)), one: true }), null);
});

// ── reject-unsupported ──────────────────────────────────────────────────────────────────────

test("rejects start inside an EXISTS subquery", () => {
  const where: Condition = {
    type: "correlatedSubquery",
    op: "EXISTS",
    related: rel("authoredIssues", "issue", ["id"], ["authorId"], {
      start: { row: { id: 1 }, exclusive: false },
    }),
  };
  assert.throws(() => run({ table: "user", where }), CompileError);
});

test("rejects start inside a count aggregate", () => {
  const ast: Ast = {
    table: "issue",
    related: [rel("cc", "comment", ["id"], ["issueId"], { aggregate: "count", start: { row: { id: 1 }, exclusive: false } })],
  };
  assert.throws(() => run(ast), CompileError);
});

test("rejects limit inside a count aggregate", () => {
  const ast: Ast = {
    table: "issue",
    related: [rel("cc", "comment", ["id"], ["issueId"], { aggregate: "count", limit: 5 })],
  };
  assert.throws(() => run(ast), CompileError);
});

// ── LIKE / ILIKE (case-sensitive LIKE; ILIKE folds via lower()) ──────────────────────────────

test("LIKE is case-sensitive (engine parity)", () => {
  // 'Login bug' does NOT match 'login%' under case-sensitive LIKE.
  assert.deepEqual(run({ table: "issue", where: cmp(col("title"), "LIKE", val("login%")) }), []);
  assert.deepEqual(run({ table: "issue", where: cmp(col("title"), "LIKE", val("Login%")) }), [I[10]]);
});

test("ILIKE folds case via lower()", () => {
  assert.deepEqual(run({ table: "issue", where: cmp(col("title"), "ILIKE", val("crash")) }), [I[13]]);
});

// ── compound keyset paging (multi-column start, executed) ────────────────────────────────────

test("compound start: after (priority=3, id=10) over [priority desc, id asc]", () => {
  // order (PK-completed) = [priority desc, id asc] ⇒ [10, 13, 12, 11]; exclusive after 10 ⇒ [13, 12, 11].
  const ast: Ast = {
    table: "issue",
    orderBy: [["priority", "desc"]],
    start: { row: { priority: 3, id: 10 }, exclusive: true },
  };
  assert.deepEqual(run(ast), [I[13], I[12], I[11]]);
});
