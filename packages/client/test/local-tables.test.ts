// Local-only tables (201-LOCAL-ONLY-TABLES-DESIGN.md) — the SCHEMA + BUILDER half of the
// contract (the engine half is proven in @rindle/optimistic's local-tables suite):
//   - the `local` flag rides the table meta (§4) and is omitted from the hello schema (E1);
//   - createSchema bans reserved synthetic prefixes and duplicate names (N1);
//   - the server builder (newQueryBuilder) excludes local tables, the local builder
//     (queries(..., { includeLocal: true })) includes them (Q1 / E3 client half).

import { test } from "node:test";
import assert from "node:assert/strict";

import {
  createSchema,
  extendSchema,
  isLocalTable,
  isReservedTableName,
  localTableNames,
  normalizedTableSchemas,
  number,
  RESERVED_TABLE_PREFIXES,
  string,
  table,
  tableMeta,
} from "../src/schema.ts";
import { exists, newQueryBuilder, queries } from "../src/query.ts";

const issue = table("issue").columns({ id: number(), title: string() }).primaryKey("id");
const selection = table("selection", { local: true }).columns({ id: number(), issueId: number() }).primaryKey("id");
const draft = table("draft", { local: true }).columns({ id: number(), text: string() }).primaryKey("id");
const schema = createSchema({ tables: [issue, selection, draft] });

test("the `local` flag rides the table meta; synced tables are unmarked", () => {
  assert.equal(tableMeta(selection).local, true);
  assert.equal(tableMeta(issue).local, undefined);
  assert.equal(isLocalTable(schema, "selection"), true);
  assert.equal(isLocalTable(schema, "issue"), false);
  assert.equal(isLocalTable(schema, "nope"), false, "an unknown table reads as non-local");
  assert.deepEqual([...localTableNames(schema)].sort(), ["draft", "selection"]);
});

test("E1 — normalizedTableSchemas omits local tables (never advertised, never in the fingerprint)", () => {
  const names = normalizedTableSchemas(schema).map((t) => t.name);
  assert.deepEqual(names, ["issue"], "only the synced table crosses to the server");
});

test("N1 — createSchema bans the engine-reserved synthetic prefixes (synced OR local)", () => {
  for (const prefix of RESERVED_TABLE_PREFIXES) {
    assert.equal(isReservedTableName(`${prefix}x`), true);
    const bad = table(`${prefix}sneaky`).columns({ id: number() }).primaryKey("id");
    assert.throws(() => createSchema({ tables: [bad] }), /reserved prefix/);
    const badLocal = table(`${prefix}sneaky`, { local: true }).columns({ id: number() }).primaryKey("id");
    assert.throws(() => createSchema({ tables: [badLocal] }), /reserved prefix/);
  }
});

test("N1/302 — createSchema bans `@` in table names (the room-table namespace: a twin is \"table@sourceKey\")", () => {
  const bad = table("note@room:doc/1").columns({ id: number() }).primaryKey("id");
  assert.throws(() => createSchema({ tables: [bad] }), /contains "@"/);
  const badLocal = table("a@b", { local: true }).columns({ id: number() }).primaryKey("id");
  assert.throws(() => createSchema({ tables: [badLocal] }), /contains "@"/);
});

test("N1 — createSchema rejects a duplicate table name (incl. a local/synced clash)", () => {
  const a = table("dup").columns({ id: number() }).primaryKey("id");
  const b = table("dup", { local: true }).columns({ id: number() }).primaryKey("id");
  assert.throws(() => createSchema({ tables: [a, b] }), /duplicate table/);
});

test("extendSchema adds local-only tables to a generated/synced schema", () => {
  const generated = createSchema({ tables: [issue] });
  const extended = extendSchema(generated, { tables: [selection, draft] });

  assert.equal(isLocalTable(extended, "issue"), false);
  assert.equal(isLocalTable(extended, "selection"), true);
  assert.deepEqual([...localTableNames(extended)].sort(), ["draft", "selection"]);
  assert.deepEqual(
    normalizedTableSchemas(extended).map((t) => t.name),
    ["issue"],
    "local-only extensions still do not cross the wire",
  );

  const local = queries(extended, undefined, { includeLocal: true });
  assert.equal(local.selection.ast().table, "selection");
});

test("extendSchema refuses non-local extensions and duplicate names", () => {
  const generated = createSchema({ tables: [issue] });
  const syncedOnly = table("syncedOnly").columns({ id: number() }).primaryKey("id");
  assert.throws(() => extendSchema(generated, { tables: [syncedOnly] }), /not local-only/);
  assert.throws(() => extendSchema(generated, { tables: [issue] }), /not local-only/);
  const collidingLocal = table("issue", { local: true }).columns({ id: number() }).primaryKey("id");
  assert.throws(() => extendSchema(generated, { tables: [collidingLocal] }), /duplicate table/);
});

test("Q1 / E3 (client) — the SERVER builder excludes local tables; the LOCAL builder includes them", () => {
  const server = newQueryBuilder(schema);
  // A synced table is reachable in the server scope.
  assert.equal(server.issue.ast().table, "issue");
  // A local table is ABSENT from the server scope — naming it is a build error.
  assert.throws(() => server.selection, /local-only table "selection" may not be used in a server\/named query/);
  assert.throws(() => server.draft, /local-only/);

  // The LOCAL builder (what store.query is) admits both.
  const local = queries(schema, undefined, { includeLocal: true });
  assert.equal(local.selection.ast().table, "selection");
  assert.equal(local.issue.ast().table, "issue");

  // A typo is still an "unknown table" error in either scope (not a local-table error).
  assert.throws(() => (local as Record<string, unknown>).nope, /unknown table/);
});

test("Q1 / E3 (client) — a local table reached via a sub/countAs/exists CHILD is rejected at build time", () => {
  const server = newQueryBuilder(schema);
  // The root proxy never sees a CHILD table (it is passed to sub/countAs/exists as a value, not a
  // proxy property access), so the guard must fire when the server-scope query finalizes its AST —
  // not only on root access. Each of these builds with a synced ROOT (`issue`) and a local CHILD.
  assert.throws(
    () => server.issue.countAs("selCount", selection, { parent: ["id"], child: ["issueId"] }).ast(),
    /local-only table "selection" may not be used in a server\/named query/,
  );
  assert.throws(
    () => server.issue.sub("sels", selection, { parent: ["id"], child: ["issueId"] }).ast(),
    /local-only table "selection"/,
  );
  assert.throws(
    () => server.issue.where(exists(selection, { parent: ["id"], child: ["issueId"] })).ast(),
    /local-only table "selection"/,
  );

  // The LOCAL builder (includeLocal) still admits a local child — it opts in.
  const local = queries(schema, undefined, { includeLocal: true });
  const ast = local.issue.countAs("selCount", selection, { parent: ["id"], child: ["issueId"] }).ast();
  assert.equal(ast.related?.[0]?.subquery.table, "selection", "the local builder keeps the local child");
});
