import { test } from "node:test";
import assert from "node:assert/strict";

import { createSchema, json, number, string, table } from "@rindle/client";
import type { MutationOp } from "@rindle/client";

import {
  buildRenderIndex,
  postgresDialect,
  questionToDollarParams,
  renderOp,
  renderPointRead,
  sqliteDialect,
} from "../src/index.ts";

const schema = createSchema({
  tables: [
    table("issue")
      .columns({ id: string(), title: string(), ownerId: string(), updatedAt: number() })
      .primaryKey("id"),
    // A composite-pk table (WHERE / ON CONFLICT span both keys).
    table("tagLink").columns({ issueId: string(), tag: string(), weight: number() }).primaryKey("issueId", "tag"),
    // An all-pk table (upsert has no non-pk columns to SET → DO NOTHING).
    table("membership").columns({ a: string(), b: string() }).primaryKey("a", "b"),
  ],
});
const render = buildRenderIndex(schema);
const issue = render.issue;
const tagLink = render.tagLink;
const membership = render.membership;

test("buildRenderIndex captures column order, pk names, and types", () => {
  assert.deepEqual(issue.columns, ["id", "title", "ownerId", "updatedAt"]);
  assert.deepEqual(issue.pkNames, ["id"]);
  assert.equal(issue.types.updatedAt, "number");
  assert.deepEqual(tagLink.pkNames, ["issueId", "tag"]);
});

test("insert: full row, columns in schema order, dialect placeholders", () => {
  const op: MutationOp = { kind: "insert", table: "issue", row: { id: "i1", title: "hi", ownerId: "u1", updatedAt: 5 } };
  assert.deepEqual(renderOp(op, issue, sqliteDialect), {
    sql: 'INSERT INTO "issue" ("id", "title", "ownerId", "updatedAt") VALUES (?, ?, ?, ?)',
    params: ["i1", "hi", "u1", 5],
  });
  assert.deepEqual(renderOp(op, issue, postgresDialect), {
    sql: 'INSERT INTO "issue" ("id", "title", "ownerId", "updatedAt") VALUES ($1, $2, $3, $4)',
    params: ["i1", "hi", "u1", 5],
  });
});

test("insertIgnore: ON CONFLICT (pk) DO NOTHING", () => {
  const op: MutationOp = { kind: "insertIgnore", table: "issue", row: { id: "i1", title: "hi", ownerId: "u1", updatedAt: 5 } };
  assert.equal(
    renderOp(op, issue, postgresDialect)?.sql,
    'INSERT INTO "issue" ("id", "title", "ownerId", "updatedAt") VALUES ($1, $2, $3, $4) ON CONFLICT ("id") DO NOTHING',
  );
});

test("upsert: ON CONFLICT (pk) DO UPDATE SET nonpk = excluded.nonpk", () => {
  const op: MutationOp = { kind: "upsert", table: "issue", row: { id: "i1", title: "hi", ownerId: "u1", updatedAt: 5 } };
  assert.equal(
    renderOp(op, issue, sqliteDialect)?.sql,
    'INSERT INTO "issue" ("id", "title", "ownerId", "updatedAt") VALUES (?, ?, ?, ?) ' +
      'ON CONFLICT ("id") DO UPDATE SET "title" = excluded."title", "ownerId" = excluded."ownerId", "updatedAt" = excluded."updatedAt"',
  );
});

test("upsert on an all-pk table degrades to DO NOTHING", () => {
  const op: MutationOp = { kind: "upsert", table: "membership", row: { a: "x", b: "y" } };
  assert.equal(
    renderOp(op, membership, postgresDialect)?.sql,
    'INSERT INTO "membership" ("a", "b") VALUES ($1, $2) ON CONFLICT ("a", "b") DO NOTHING',
  );
});

test("update: SET = named non-pk cols (schema order), WHERE = pk; params SET-then-pk", () => {
  const op: MutationOp = { kind: "update", table: "issue", row: { id: "i1", updatedAt: 9, title: "new" } };
  assert.deepEqual(renderOp(op, issue, postgresDialect), {
    sql: 'UPDATE "issue" SET "title" = $1, "updatedAt" = $2 WHERE "id" = $3',
    params: ["new", 9, "i1"],
  });
});

test("update: a pk-only row emits NO statement (null) — matches the client no-op edit", () => {
  assert.equal(renderOp({ kind: "update", table: "issue", row: { id: "i1" } }, issue, sqliteDialect), null);
});

test("delete: WHERE by pk; composite pk spans all keys with AND", () => {
  assert.deepEqual(renderOp({ kind: "delete", table: "issue", pk: { id: "i1" } }, issue, sqliteDialect), {
    sql: 'DELETE FROM "issue" WHERE "id" = ?',
    params: ["i1"],
  });
  assert.deepEqual(
    renderOp({ kind: "delete", table: "tagLink", pk: { issueId: "i1", tag: "bug" } }, tagLink, postgresDialect),
    { sql: 'DELETE FROM "tagLink" WHERE "issueId" = $1 AND "tag" = $2', params: ["i1", "bug"] },
  );
});

test("update on a composite-pk table: WHERE spans both keys", () => {
  const op: MutationOp = { kind: "update", table: "tagLink", row: { issueId: "i1", tag: "bug", weight: 3 } };
  assert.deepEqual(renderOp(op, tagLink, sqliteDialect), {
    sql: 'UPDATE "tagLink" SET "weight" = ? WHERE "issueId" = ? AND "tag" = ?',
    params: [3, "i1", "bug"],
  });
});

test("renderPointRead: SELECT the known columns WHERE pk", () => {
  assert.deepEqual(renderPointRead("issue", { id: "i1" }, issue, postgresDialect), {
    sql: 'SELECT "id", "title", "ownerId", "updatedAt" FROM "issue" WHERE "id" = $1',
    params: ["i1"],
  });
});

test("validation: unknown column, missing insert column, and missing pk all throw with valid names", () => {
  assert.throws(
    () => renderOp({ kind: "insert", table: "issue", row: { id: "i1", bogus: 1 } }, issue, sqliteDialect),
    /unknown column bogus on issue/,
  );
  assert.throws(
    () => renderOp({ kind: "insert", table: "issue", row: { id: "i1" } }, issue, sqliteDialect),
    /missing columns title, ownerId, updatedAt on issue/,
  );
  assert.throws(
    () => renderOp({ kind: "delete", table: "issue", pk: {} }, issue, sqliteDialect),
    /missing primary-key column id on issue/,
  );
});

test("insert may omit a nullable column — it binds NULL and is not in the required set (design 206 §6.2)", () => {
  const s = createSchema({
    tables: [table("acct").columns({ id: string(), name: string(), nickname: string().nullable() }).primaryKey("id")],
  });
  const acct = buildRenderIndex(s).acct;

  // the nullable column is excluded from `required` (mirrors the client funnel)
  assert.deepEqual(acct.required, ["id", "name"]);

  // omitted `nickname` still appears in the INSERT column list, bound to NULL — so both tiers
  // produce the same row regardless of any DDL default.
  assert.deepEqual(renderOp({ kind: "insert", table: "acct", row: { id: "a1", name: "n" } }, acct, sqliteDialect), {
    sql: 'INSERT INTO "acct" ("id", "name", "nickname") VALUES (?, ?, ?)',
    params: ["a1", "n", null],
  });
  // an explicit null is identical to omission
  assert.deepEqual(
    renderOp({ kind: "insert", table: "acct", row: { id: "a1", name: "n", nickname: null } }, acct, sqliteDialect)?.params,
    ["a1", "n", null],
  );
  // omitting a NOT NULL column still throws with the valid names
  assert.throws(
    () => renderOp({ kind: "insert", table: "acct", row: { id: "a1" } }, acct, sqliteDialect),
    /missing column name on acct/,
  );
});

test("insert: a json column binds a stringified object, and a pre-stringified string passes through", () => {
  const s = createSchema({
    tables: [table("doc").columns({ id: string(), meta: json<{ n: number }>() }).primaryKey("id")],
  });
  const doc = buildRenderIndex(s).doc;

  // A typed mutator passes the parsed OBJECT → the funnel stringifies it into the bound param.
  assert.deepEqual(
    renderOp({ kind: "insert", table: "doc", row: { id: "d1", meta: { n: 42 } as unknown as string } }, doc, sqliteDialect),
    { sql: 'INSERT INTO "doc" ("id", "meta") VALUES (?, ?)', params: ["d1", '{"n":42}'] },
  );
  // An already-stringified json value is left as-is (backward compatible with hand-written mutators).
  assert.deepEqual(
    renderOp({ kind: "insert", table: "doc", row: { id: "d1", meta: '{"n":42}' } }, doc, sqliteDialect)?.params,
    ["d1", '{"n":42}'],
  );
});

test("parity: the postgres render is exactly questionToDollarParams of the sqlite render", () => {
  // The old dual-dialect bridge (hand `?`-SQL + questionToDollarParams) and the new renderer agree.
  for (const op of [
    { kind: "insert", table: "issue", row: { id: "i1", title: "hi", ownerId: "u1", updatedAt: 5 } },
    { kind: "insertIgnore", table: "issue", row: { id: "i1", title: "hi", ownerId: "u1", updatedAt: 5 } },
    { kind: "update", table: "issue", row: { id: "i1", title: "new", updatedAt: 9 } },
    { kind: "delete", table: "tagLink", pk: { issueId: "i1", tag: "bug" } },
  ] as MutationOp[]) {
    const sqlite = renderOp(op, render[op.table], sqliteDialect);
    const postgres = renderOp(op, render[op.table], postgresDialect);
    assert.equal(questionToDollarParams(sqlite!.sql), postgres!.sql, `dialect parity for ${op.kind} ${op.table}`);
    assert.deepEqual(sqlite!.params, postgres!.params);
  }
});
