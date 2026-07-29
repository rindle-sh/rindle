// Nullability at the type level (design 206): `.nullable()` widens a column to `T | null` in
// `Row<…>`; a bare column stays `T`. `rindle schema gen` emits `.nullable()` for nullable
// (non-`NOT NULL`) SQL columns; a local-only table may call it by hand. This package typechecks its
// tests, so the annotated assignment lines — and the `@ts-expect-error` — are the load-bearing
// assertions (the `test(...)` bodies run trivially).

import { test } from "node:test";
import assert from "node:assert/strict";

import { insertPlan, json, number, string, table, tableMeta } from "../src/schema.ts";
import type { Insert, Row } from "../src/schema.ts";

const issue = table("issue")
  .columns({
    id: string(),
    title: string(), // NOT NULL → bare builder
    assignee: string().nullable(), // nullable → string | null
    points: number().nullable(),
  })
  .primaryKey("id");

type Issue = Row<typeof issue>;

test("a bare column's Row field is exactly T (no null)", () => {
  const row = {} as Issue;
  // Compiles ⇒ `title` admits no `null` (a `string | null` field would be rejected here).
  const title: string = row.title;
  void title;
});

test("a .nullable() column's Row field is T | null", () => {
  const row = {} as Issue;
  const assignee: string | null = row.assignee; // null is a valid value
  const points: number | null = row.points;
  // @ts-expect-error — `assignee` is `string | null`, so it is NOT assignable to a bare `string`.
  const strict: string = row.assignee;
  void assignee;
  void points;
  void strict;
});

test("insertPlan: nullable columns are omittable, NOT NULL columns required (design 206 §6.2)", () => {
  // `.nullable()` also sets the runtime `Col.optional` marker the write funnels read to omit-to-null.
  assert.equal(tableMeta(issue).columns.title.optional, undefined); // NOT NULL ⇒ no marker
  assert.equal(tableMeta(issue).columns.assignee.optional, true); // nullable ⇒ marked
  const plan = insertPlan(tableMeta(issue));
  assert.deepEqual(plan.required, ["id", "title"]); // pk + NOT NULL only
  assert.deepEqual([...plan.nullable].sort(), ["assignee", "points"]);
});

test("Insert<T>: nullable columns are OPTIONAL (?:), NOT NULL columns required (design 206 §7)", () => {
  // a full row is fine…
  const full: Insert<typeof issue> = { id: "1", title: "t", assignee: "u", points: 3 };
  // …a nullable column may be OMITTED entirely (the whole point — no more `assignee: null`)…
  const omitNullable: Insert<typeof issue> = { id: "1", title: "t" };
  // …or still written as null.
  const explicitNull: Insert<typeof issue> = { id: "1", title: "t", assignee: null, points: null };
  // @ts-expect-error — omitting a NOT NULL column (`title`) is a type error.
  const omitRequired: Insert<typeof issue> = { id: "1" };
  // @ts-expect-error — an unknown column is still rejected (excess-property check survives the split).
  const unknownCol: Insert<typeof issue> = { id: "1", title: "t", bogus: 1 };
  void full;
  void omitNullable;
  void explicitNull;
  void omitRequired;
  void unknownCol;
});

test("Insert<T>: a bare json() (json<unknown>) column stays REQUIRED — the top-type guard (design 206 §7)", () => {
  const doc = table("doc").columns({ id: string(), meta: json() }).primaryKey("id");
  const withMeta: Insert<typeof doc> = { id: "1", meta: { anything: true } };
  // @ts-expect-error — `unknown | null` collapses to `unknown`, so a NOT NULL json column is NOT
  // wrongly made optional; it must be provided (declare json<T>() to make a nullable one omittable).
  const omitJson: Insert<typeof doc> = { id: "1" };
  void withMeta;
  void omitJson;
});

test("Insert<T>: a typed json<T>().nullable() column IS omittable (design 206 §7)", () => {
  const doc = table("doc").columns({ id: string(), meta: json<{ n: number }>().nullable() }).primaryKey("id");
  const omit: Insert<typeof doc> = { id: "1" }; // meta omittable
  const withMeta: Insert<typeof doc> = { id: "1", meta: { n: 1 } };
  const nullMeta: Insert<typeof doc> = { id: "1", meta: null };
  void omit;
  void withMeta;
  void nullMeta;
});
