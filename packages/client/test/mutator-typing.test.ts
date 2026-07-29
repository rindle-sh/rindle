// The typed mutator surface (`defineMutators(schema)`): `tx.insert`/`update`/`upsert`/`insertIgnore`/
// `delete` are checked against the schema — table + column names, each column's value type,
// nullable-omit on inserts, `json<T>` as a parsed object, and the EXACT primary-key columns on
// `update`/`delete`. This package typechecks its tests, so the annotated lines and the
// `@ts-expect-error`s ARE the assertions (the generator bodies never run).

import { test } from "node:test";

import { createSchema, json, number, string, table } from "../src/schema.ts";
import { defineMutators } from "../src/mutation-ops.ts";

const issue = table("issue")
  .columns({ id: string(), title: string(), assignee: string().nullable(), meta: json<{ n: number }>() })
  .primaryKey("id");
// A composite-pk table — both keys are required to identify a row.
const tagLink = table("tagLink").columns({ issueId: string(), tag: string(), weight: number() }).primaryKey("issueId", "tag");
const schema = createSchema({ tables: [issue, tagLink] });

const { shared } = defineMutators(schema);
const anyArgs = { parse: (r: unknown) => r as Record<string, unknown> };

test("insert: required vs nullable, value types, json-as-object, unknown table/column", () => {
  shared(anyArgs, function* (tx) {
    tx.insert("issue", { id: "1", title: "t", meta: { n: 1 } }); // `assignee` (nullable) omitted
    tx.insert("issue", { id: "1", title: "t", assignee: null, meta: { n: 1 } }); // …or written null
    // @ts-expect-error — `title` is NOT NULL: omitting it is a type error.
    tx.insert("issue", { id: "1", meta: { n: 1 } });
    // @ts-expect-error — `title` must be a string, not a number.
    tx.insert("issue", { id: "1", title: 5, meta: { n: 1 } });
    // @ts-expect-error — `meta` is json<{ n: number }>, not `{ x: string }`.
    tx.insert("issue", { id: "1", title: "t", meta: { x: "no" } });
    // @ts-expect-error — unknown column `nope`.
    tx.insert("issue", { id: "1", title: "t", meta: { n: 1 }, nope: 1 });
    // @ts-expect-error — unknown table.
    tx.insert("isseu", { id: "1" });
  });
});

test("update: exact pk REQUIRED, non-pk columns optional + typed", () => {
  shared(anyArgs, function* (tx) {
    tx.update("issue", { id: "1", title: "new" }); // pk + a non-pk change
    tx.update("issue", { id: "1" }); // pk-only (a no-op edit) is allowed
    // @ts-expect-error — missing the pk column `id`.
    tx.update("issue", { title: "new" });
    // @ts-expect-error — a non-pk column must still be the right type.
    tx.update("issue", { id: "1", title: 5 });
    tx.update("tagLink", { issueId: "i", tag: "bug", weight: 3 }); // composite pk + change
    // @ts-expect-error — a composite pk needs BOTH key columns.
    tx.update("tagLink", { issueId: "i", weight: 3 });
  });
});

test("delete: exact pk only", () => {
  shared(anyArgs, function* (tx) {
    tx.delete("issue", { id: "1" });
    tx.delete("tagLink", { issueId: "i", tag: "bug" });
    // @ts-expect-error — missing the pk column `id`.
    tx.delete("issue", {});
    // @ts-expect-error — a composite pk needs BOTH key columns.
    tx.delete("tagLink", { issueId: "i" });
    // @ts-expect-error — a non-pk column is not part of the delete key.
    tx.delete("tagLink", { issueId: "i", tag: "bug", weight: 3 });
  });
});
