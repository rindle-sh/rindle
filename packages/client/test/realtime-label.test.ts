// The §2.1 realtime LABEL on defineQuery (RINDLE-REALTIME-QUERY-ENABLEMENT, slice G-iv-a): a
// named query declares which room profile serves it and how its args map to the profile's key
// args. Properties gated here: the label rides the NamedQuery AND the stamped built Query (and
// survives chaining, like name/args); it is pure metadata — byte-identical AST, never part of the
// wire identity; unlabeled queries are completely unchanged (no property at all); and a malformed
// label throws loudly at definition time, not at first serve.

import assert from "node:assert/strict";
import { test } from "node:test";

import { defineQuery, newQueryBuilder } from "../src/query.ts";
import { createSchema, string, table } from "../src/schema.ts";

const slide = table("slide")
  .columns({ id: string(), deckId: string(), body: string() })
  .primaryKey("id");
const schema = createSchema({ tables: [slide] });
const qb = newQueryBuilder(schema);

test("realtime label rides the NamedQuery and the stamped Query; chaining preserves it; AST is byte-identical", () => {
  const label = { room: "document", args: (a: { deckId: string }) => ({ docId: a.deckId }) };
  const unlabeled = defineQuery("deckCanvas", (a: { deckId: string }) => qb.slide.where.deckId(a.deckId));
  const labeled = defineQuery("deckCanvas", (a: { deckId: string }) => qb.slide.where.deckId(a.deckId), {
    realtime: label,
  });

  // The NamedQuery carries the label (the server registers this exact value).
  assert.equal(labeled.realtime, label);

  // The stamped built Query carries it too, beside — never inside — the wire identity.
  const built = labeled({ deckId: "d1" });
  assert.equal(built.realtime, label);
  assert.equal(built.name, "deckCanvas");
  assert.deepEqual(built.args, { deckId: "d1" });

  // Chaining preserves it, exactly like name/args.
  const chained = built.where.id("s1");
  assert.equal(chained.realtime, label);
  assert.equal(chained.name, "deckCanvas");

  // Pure metadata: the compiled AST is byte-identical with and without the label.
  assert.deepEqual(built.ast(), unlabeled({ deckId: "d1" }).ast());

  // resolve() (the server's authority path) is untouched: same AST, no stamp.
  const resolved = labeled.resolve({ deckId: "d1" });
  assert.deepEqual(resolved.ast(), built.ast());
  assert.equal(resolved.name, undefined);
});

test("realtime label: the validator form takes options as a 4th argument; args mapping may be omitted (identity)", () => {
  const q = defineQuery(
    "slidesPage",
    (raw): { deckId: string } => raw as { deckId: string },
    ({ deckId }) => qb.slide.where.deckId(deckId),
    { realtime: { room: "document" } },
  );
  assert.deepEqual(q.realtime, { room: "document" });
  assert.deepEqual(q({ deckId: "d" }).realtime, { room: "document" });
});

test("unlabeled queries carry NO realtime property — zero behavior change", () => {
  const q = defineQuery("plainSlides", () => qb.slide.orderBy("id", "asc"));
  assert.equal(q.realtime, undefined);
  assert.equal(Object.prototype.hasOwnProperty.call(q, "realtime"), false);
  assert.equal(q().realtime, undefined);
});

test("a malformed realtime label throws at definition time", () => {
  assert.throws(() => defineQuery("x", () => qb.slide, { realtime: { room: "" } }), /realtime\.room/);
  assert.throws(
    () => defineQuery("x", () => qb.slide, { realtime: { room: "a/b" } }),
    /may not contain "\/"/,
  );
  assert.throws(
    () => defineQuery("x", () => qb.slide, { realtime: { room: "document", args: "nope" as never } }),
    /realtime\.args/,
  );
});
