// Column refinement (refineTable / refineSchema): narrowing a GENERATED column's TS type within
// its kind — `json<T>()` element types and string/number literal unions, the two refinements SQL
// can't carry — while the runtime shape stays exactly the daemon's wire schema. The load-bearing
// type assertions are the `@ts-expect-error` lines (this package typechecks its tests):
//   - a refined column narrows `where` args, `Row<typeof t>`, and the schema's query roots;
//   - a `rel(...)` anchored on the UNREFINED def is rejected by a refined-schema query (the reason
//     refinement lives on the TABLE DEF, not just the schema);
//   - refinement composes with `extendSchema` in either order.

import { test } from "node:test";
import assert from "node:assert/strict";

import {
  createSchema,
  extendSchema,
  json,
  number,
  refineSchema,
  refineTable,
  rel,
  string,
  table,
  tableMeta,
} from "../src/schema.ts";
import type { Row } from "../src/schema.ts";
import { eq } from "../src/operators.ts";
import { newQueryBuilder, queries } from "../src/query.ts";

type Status = "open" | "in-progress" | "closed";
type IssueMeta = { tags: string[]; estimate?: number };

// What `rindle schema gen` emits: kinds only, no refinements.
const issueGen = table("issue")
  .columns({ id: string(), status: string(), ownerId: string(), points: number(), meta: json() })
  .primaryKey("id");
const user = table("user").columns({ id: string(), name: string() }).primaryKey("id");
const schemaGen = createSchema({ tables: [issueGen, user] });

// The hand-written refinement, declared once.
const issue = refineTable(issueGen, { status: string<Status>(), meta: json<IssueMeta>() });
const schema = refineSchema(schemaGen, { tables: [issue] });

test("refineTable is a validated identity: same meta, same runtime conditions", () => {
  assert.equal(tableMeta(issue), tableMeta(issueGen), "the def is re-typed, not rebuilt");
  const cond = issue.status(eq("open"));
  assert.deepEqual(cond, issueGen.status(eq("open")), "field factories behave identically");
  assert.equal(cond.type, "simple");
});

test("refineTable narrows Row<> and where args at the type level", () => {
  const row: Row<typeof issue> = {
    id: "i1",
    status: "open",
    ownerId: "u1",
    points: 3,
    meta: { tags: ["a"] },
  };
  assert.equal(row.status, "open");
  const _bad: Row<typeof issue> = {
    id: "i1",
    // @ts-expect-error `status` is narrowed to the Status union.
    status: "bogus",
    ownerId: "u1",
    points: 3,
    meta: { tags: [] },
  };
  issue.status(eq("open"));
  // @ts-expect-error a refined field condition rejects values outside the union.
  issue.status(eq("bogus"));
  // `meta` is narrowed to IssueMeta (were it still `unknown`, this assignment would not compile).
  const _meta: IssueMeta = (undefined as unknown) as Row<typeof issue>["meta"];
});

test("refineTable rejects unknown columns and kind changes at runtime", () => {
  // @ts-expect-error no such column on the def.
  assert.throws(() => refineTable(issueGen, { nope: string() }), /no column "nope"/);
  // `string()` on a json column TYPE-checks (Col<string> narrows Col<unknown>) but flips the
  // runtime kind — exactly what the runtime guard is for.
  assert.throws(() => refineTable(issueGen, { meta: string() }), /is json, not string/);
  // @ts-expect-error cross-kind (number on a string column) is already a compile error…
  assert.throws(() => refineTable(issueGen, { status: number() }), /is string, not number/);
});

test("refineSchema is a validated identity and narrows the query roots", () => {
  assert.equal(schema.tables, schemaGen.tables, "same runtime schema");
  const q = newQueryBuilder(schema);
  const ast = q.issue.where.status("open").ast();
  assert.equal(ast.table, "issue");
  // @ts-expect-error the root's where proxy is narrowed to the Status union.
  q.issue.where.status("bogus");
  // An unrefined table is untouched.
  q.user.where.name("anyone");
});

test("a rel anchored on the REFINED def is accepted; on the unrefined def it is rejected", () => {
  const q = newQueryBuilder(schema);
  const owner = rel(issue, user, { ownerId: "id" });
  assert.equal(q.issue.sub("owner", owner).ast().related?.[0]?.subquery.alias, "owner");
  const staleOwner = rel(issueGen, user, { ownerId: "id" });
  // @ts-expect-error a rel built from the unrefined def does not fit the refined query root.
  q.issue.sub("owner", staleOwner);
});

test("refineSchema rejects unknown tables, double refinement, and shape drift", () => {
  const ghost = table("ghost").columns({ id: string() }).primaryKey("id");
  // @ts-expect-error `ghost` is not a table of this schema.
  assert.throws(() => refineSchema(schemaGen, { tables: [ghost] }), /has no table "ghost"/);
  assert.throws(() => refineSchema(schemaGen, { tables: [issue, issue] }), /refined twice/);
  // A same-named def that is NOT the generated table (missing columns) is drift, not refinement.
  const impostor = table("issue").columns({ id: string() }).primaryKey("id");
  assert.throws(() => refineSchema(schemaGen, { tables: [impostor] }), /does not match/);
  const rekinded = table("issue")
    .columns({ id: string(), status: number(), ownerId: string(), points: number(), meta: json() })
    .primaryKey("id");
  assert.throws(() => refineSchema(schemaGen, { tables: [rekinded] }), /does not match/);
  const repinned = table("issue")
    .columns({ id: string(), status: string(), ownerId: string(), points: number(), meta: json() })
    .primaryKey("id", "status");
  assert.throws(() => refineSchema(schemaGen, { tables: [repinned] }), /does not match/);
  const localized = table("issue", { local: true })
    .columns({ id: string(), status: string(), ownerId: string(), points: number(), meta: json() })
    .primaryKey("id");
  assert.throws(() => refineSchema(schemaGen, { tables: [localized] }), /does not match/);
});

test("refineSchema composes with extendSchema in either order", () => {
  const draft = table("draft", { local: true }).columns({ id: number(), text: string() }).primaryKey("id");

  const refinedThenExtended = extendSchema(refineSchema(schemaGen, { tables: [issue] }), { tables: [draft] });
  const extendedThenRefined = refineSchema(extendSchema(schemaGen, { tables: [draft] }), { tables: [issue] });

  for (const s of [refinedThenExtended, extendedThenRefined]) {
    assert.deepEqual(Object.keys(s.tables).sort(), ["draft", "issue", "user"]);
    const local = queries(s, undefined, { includeLocal: true });
    assert.equal(local.draft.ast().table, "draft");
    local.issue.where.status("open");
    // @ts-expect-error the refinement narrows through either composition order.
    local.issue.where.status("bogus");
  }

  // A LOCAL table added by extendSchema can be refined too (order: extend, then refine).
  const draftKind = table("draft", { local: true })
    .columns({ id: number(), text: string<"a" | "b">() })
    .primaryKey("id");
  const both = refineSchema(extendSchema(schemaGen, { tables: [draft] }), { tables: [issue, draftKind] });
  const local = queries(both, undefined, { includeLocal: true });
  local.draft.where.text("a");
  // @ts-expect-error the local table's refinement narrows as well.
  local.draft.where.text("c");
});
