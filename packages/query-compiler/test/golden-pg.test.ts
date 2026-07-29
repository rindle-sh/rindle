import { test } from "node:test";
import assert from "node:assert/strict";

import { compile } from "../src/index.ts";
import type { Ast, Condition, SimpleOp, ValuePosition } from "../src/index.ts";
import { issueTracker } from "./fixtures.ts";

// Golden SQL for the Postgres dialect (M3). The offline check for the PG leaf: the walker is
// already proven against `node:sqlite` (walker.test.ts), so these pin the Postgres-specific
// leaves — jsonb assembly, `::text::<type>` casts (§6.2), explicit NULLS ordering (§8), and the
// temporal projection reconciliation. Execution parity against a live Postgres is the Phase D
// docker lane; these goldens are the offline gate. The catalog (`issueTracker`) exercises every
// cast branch.

const pg = (ast: Ast) => compile(ast, issueTracker, { dialect: "postgres" });
const col = (name: string): ValuePosition => ({ type: "column", name });
const val = (value: unknown): ValuePosition => ({ type: "literal", value } as ValuePosition);
const where = (left: ValuePosition, op: SimpleOp, right: ValuePosition): Condition => ({
  type: "simple",
  op,
  left,
  right,
});

// ── full-SQL assembly snapshots ──────────────────────────────────────────────────────────────

test("assembly: plural root — jsonb_build_object / jsonb_agg / COALESCE '[]'::jsonb", () => {
  const { sql, params } = pg({ table: "issue", select: ["id", "title"] });
  assert.equal(
    sql,
    `SELECT COALESCE((SELECT jsonb_agg(jsonb_build_object('id', "issue_0"."id", 'title', "issue_0"."title") ORDER BY "issue_0"."id" ASC NULLS FIRST) FROM "issue" AS "issue_0"), '[]'::jsonb) AS "rindle_result"`,
  );
  assert.deepEqual(params, []);
});

test("assembly: nested one (LIMIT 1) + many (COALESCE jsonb_agg)", () => {
  const { sql } = pg({
    table: "issue",
    select: ["id"],
    related: [
      { correlation: { parentField: ["authorId"], childField: ["id"] }, subquery: { table: "user", alias: "author", select: ["id", "name"] } },
      { correlation: { parentField: ["id"], childField: ["issueId"] }, subquery: { table: "comment", alias: "comments", select: ["id"] } },
    ],
  });
  assert.equal(
    sql,
    `SELECT COALESCE((SELECT jsonb_agg(jsonb_build_object('id', "issue_0"."id", 'author', (SELECT jsonb_build_object('id', "user_1"."id", 'name', "user_1"."name") FROM "user" AS "user_1" WHERE "issue_0"."authorId" = "user_1"."id" ORDER BY "user_1"."id" ASC NULLS FIRST LIMIT 1), 'comments', COALESCE((SELECT jsonb_agg(jsonb_build_object('id', "comment_2"."id") ORDER BY "comment_2"."id" ASC NULLS FIRST) FROM "comment" AS "comment_2" WHERE "issue_0"."id" = "comment_2"."issueId"), '[]'::jsonb)) ORDER BY "issue_0"."id" ASC NULLS FIRST) FROM "issue" AS "issue_0"), '[]'::jsonb) AS "rindle_result"`,
  );
});

test("assembly: .one() root — single object, no COALESCE/agg, LIMIT 1", () => {
  const { sql, params } = pg({ table: "issue", select: ["id"], where: where(col("id"), "=", val("x")), one: true });
  assert.equal(
    sql,
    `SELECT (SELECT jsonb_build_object('id', "issue_0"."id") FROM "issue" AS "issue_0" WHERE "issue_0"."id" = $1::text::uuid ORDER BY "issue_0"."id" ASC NULLS FIRST LIMIT 1) AS "rindle_result"`,
  );
  assert.deepEqual(params, ["x"]);
});

// ── §6.2 filter casts (fragment + params; text-pinned) ────────────────────────────────────────

const whereFrag = (ast: Ast) => {
  const { sql, params } = pg(ast);
  return { sql, params };
};

test("cast: enum → $N::text::\"enumname\"", () => {
  const { sql, params } = whereFrag({ table: "issue", where: where(col("status"), "=", val("open")) });
  assert.ok(sql.includes(`"issue_0"."status" = $1::text::"issue_status"`), sql);
  assert.deepEqual(params, ["open"]);
});

test("cast: timestamptz → to_timestamp($N::text::numeric / 1000.0), no AT TIME ZONE", () => {
  const { sql, params } = whereFrag({ table: "issue", where: where(col("createdAt"), ">", val(1600000000000)) });
  assert.ok(sql.includes(`"issue_0"."createdAt" > to_timestamp($1::text::numeric / 1000.0)`), sql);
  assert.ok(!sql.includes(`AT TIME ZONE`), "timestamptz must not add AT TIME ZONE");
  assert.deepEqual(params, ["1600000000000"]);
});

test("cast: uuid → $N::text::uuid", () => {
  const { sql, params } = whereFrag({ table: "issue", where: where(col("id"), "=", val("a1b2")) });
  assert.ok(sql.includes(`"issue_0"."id" = $1::text::uuid`), sql);
  assert.deepEqual(params, ["a1b2"]);
});

test("cast: text comparison → bare $N::text (not the column's width)", () => {
  const { sql, params } = whereFrag({ table: "issue", where: where(col("title"), "=", val("Login bug")) });
  assert.ok(sql.includes(`"issue_0"."title" = $1::text)`), sql);
  assert.deepEqual(params, ["Login bug"]);
});

test("cast: number comparison → $N::text::double precision (IEEE-754 domain)", () => {
  const { sql, params } = whereFrag({ table: "issue", where: where(col("priority"), ">=", val(3)) });
  assert.ok(sql.includes(`"issue_0"."priority" >= $1::text::double precision`), sql);
  assert.deepEqual(params, ["3"]);
});

test("cast: IN list — each element cast with the column type", () => {
  const { sql, params } = whereFrag({ table: "issue", where: where(col("id"), "IN", val(["a", "b"])) });
  assert.ok(sql.includes(`"issue_0"."id" IN ($1::text::uuid, $2::text::uuid)`), sql);
  assert.deepEqual(params, ["a", "b"]);
});

test("cast: array column equality → ARRAY(SELECT … jsonb_array_elements_text)", () => {
  const { sql, params } = whereFrag({ table: "issue", where: where(col("labels"), "=", val(["bug", "urgent"])) });
  assert.ok(
    sql.includes(`"issue_0"."labels" = ARRAY(SELECT value::text FROM jsonb_array_elements_text($1::text::jsonb))`),
    sql,
  );
  assert.deepEqual(params, ['["bug","urgent"]']);
});

// ── §6.2 projection reconciliation (outbound) ────────────────────────────────────────────────

test("projection: timestamptz & date → epoch-ms snapped to integer (::bigint), not modular", () => {
  const { sql } = pg({ table: "issue", select: ["id", "createdAt", "dueDate"] });
  // Always snap to whole milliseconds: the value model is integer epoch-ms.
  assert.ok(sql.includes(`'createdAt', (EXTRACT(EPOCH FROM "issue_0"."createdAt") * 1000)::bigint`), sql);
  assert.ok(sql.includes(`'dueDate', (EXTRACT(EPOCH FROM "issue_0"."dueDate") * 1000)::bigint`), sql);
  // The design-critical divergence from z2s: full timestamps are NOT collapsed mod-1-day.
  assert.ok(!sql.includes("% 86400000"), "timestamptz/date must not be modular-normalized");
});

test("projection: timetz → modular-normalized (offset can be negative)", () => {
  const { sql } = pg({ table: "comment", select: ["id", "editWindow"] });
  assert.ok(
    sql.includes(`'editWindow', ((EXTRACT(EPOCH FROM "comment_0"."editWindow") * 1000)::bigint + 86400000) % 86400000`),
    sql,
  );
});

test("projection: enum / uuid / numeric / jsonb / array project raw", () => {
  const { sql } = pg({ table: "issue", select: ["status", "id", "estimate", "metadata", "labels"] });
  for (const c of ["status", "id", "estimate", "metadata", "labels"]) {
    assert.ok(sql.includes(`'${c}', "issue_0"."${c}"`), `${c} should project raw: ${sql}`);
  }
});

// ── §8 ordering / nulls ──────────────────────────────────────────────────────────────────────

test("ordering: DESC NULLS LAST + PK tiebreak ASC NULLS FIRST", () => {
  const { sql } = pg({ table: "issue", select: ["id"], orderBy: [["createdAt", "desc"]] });
  assert.ok(sql.includes(`ORDER BY "issue_0"."createdAt" DESC NULLS LAST, "issue_0"."id" ASC NULLS FIRST`), sql);
});

// ── keyset paging with temporal bound ────────────────────────────────────────────────────────

test("paging: temporal bound routed through the same to_timestamp cast", () => {
  const { sql, params } = pg({
    table: "issue",
    select: ["id"],
    orderBy: [["createdAt", "asc"]],
    start: { row: { createdAt: 1600000000000 }, exclusive: true },
  });
  assert.ok(
    sql.includes(
      `((to_timestamp($1::text::numeric / 1000.0) < "issue_0"."createdAt") OR ("issue_0"."createdAt" = to_timestamp($2::text::numeric / 1000.0) AND "issue_0"."id" IS NOT NULL))`,
    ),
    sql,
  );
  assert.deepEqual(params, ["1600000000000", "1600000000000"]);
});

// ── boolean fold in the Postgres dialect ─────────────────────────────────────────────────────

test("empty AND folds to TRUE (postgres boolean literal)", () => {
  const { sql } = pg({ table: "issue", select: ["id"], where: { type: "and", conditions: [] } });
  assert.ok(sql.includes("WHERE TRUE"), sql);
});

test("empty IN folds to FALSE (postgres boolean literal)", () => {
  const { sql } = pg({ table: "issue", select: ["id"], where: where(col("id"), "IN", val([])) });
  assert.ok(sql.includes("WHERE FALSE"), sql);
});

test("LIKE / ILIKE keep the lower() folding (not native ILIKE, §8)", () => {
  const like = pg({ table: "issue", select: ["id"], where: where(col("title"), "LIKE", val("Login%")) });
  assert.ok(like.sql.includes(`"issue_0"."title" LIKE $1::text ESCAPE '\\'`), like.sql);
  const ilike = pg({ table: "issue", select: ["id"], where: where(col("title"), "ILIKE", val("login%")) });
  assert.ok(ilike.sql.includes(`lower("issue_0"."title") LIKE lower($1::text) ESCAPE '\\'`), ilike.sql);
  assert.ok(!ilike.sql.includes("ILIKE"), "must not emit native ILIKE");
});
