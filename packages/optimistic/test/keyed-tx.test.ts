// The keyed MutationTx methods (insert/update/upsert/delete/row): named columns over the
// positional wire, schema-checked at runtime — typos throw immediately with the valid
// names listed (the loud-failure contract that makes mutators safe to author by hand or
// by an LLM).

import { test } from "node:test";
import assert from "node:assert/strict";

import {
  createSchema,
  defineMutators,
  defineQuery,
  initWasm,
  json,
  newQueryBuilder,
  number,
  string,
  table,
  type NormalizedEvent,
  type OptimisticSource,
  type ProgressFrame,
  type QueryId,
} from "@rindle/wasm";
import { createOptimisticStore, type MutationTx } from "../src/index.ts";

await initWasm();

const issue = table("issue").columns({ id: number(), title: string(), score: number() }).primaryKey("id");
const schema = createSchema({ tables: [issue] });
const q = newQueryBuilder(schema);
const queries = { all: defineQuery("all", () => q.issue.orderBy("id", "asc")) };

/** A source that never confirms — these tests only exercise the local predicted apply. */
class NullSource implements OptimisticSource {
  registerQuery(): void {}
  unregisterQuery(): void {}
  pushMutation(): Promise<void> {
    return Promise.resolve();
  }
  onNormalized(_h: (qid: QueryId, ev: NormalizedEvent) => void): void {}
  onProgress(_h: (frame: ProgressFrame) => void): void {}
}

function makeStore<R extends Record<string, (tx: MutationTx, args: any) => void>>(mutators: R) {
  return createOptimisticStore(schema, new NullSource(), mutators, { clientID: "c1" });
}

test("insert/row/update/upsert/delete round-trip with named columns", () => {
  const { store, mutate } = makeStore({
    seed: (tx) => {
      tx.insert("issue", { id: 1, title: "first", score: 10 });
      tx.insert("issue", { id: 2, title: "second", score: 20 });
    },
    bump: (tx, args: { id: number; by: number }) => {
      const cur = tx.row("issue", { id: args.id });
      if (!cur) return;
      tx.update("issue", { id: args.id, score: (cur.score as number) + args.by });
    },
    replace: (tx) => tx.upsert("issue", { id: 2, title: "second!", score: 0 }),
    grow: (tx) => tx.upsert("issue", { id: 3, title: "third", score: 30 }),
    drop: (tx) => tx.delete("issue", { id: 1 }),
  });
  const view = store.materialize(queries.all());

  mutate.seed(undefined);
  mutate.bump({ id: 1, by: 5 });
  mutate.replace(undefined);
  mutate.grow(undefined);
  assert.deepEqual(
    view.data.map((r) => [r.id, r.title, r.score]),
    [
      [1, "first", 15],
      [2, "second!", 0],
      [3, "third", 30],
    ],
  );
  mutate.drop(undefined);
  assert.deepEqual(view.data.map((r) => r.id), [2, 3]);
});

test("insertIgnore inserts when absent and is a no-op when the pk exists (isomorphic upsert-if-absent)", () => {
  const { store, mutate } = makeStore({
    ensure: (tx, args: { id: number; title: string; score: number }) =>
      tx.insertIgnore("issue", { id: args.id, title: args.title, score: args.score }),
  });
  const view = store.materialize(queries.all());

  mutate.ensure({ id: 1, title: "first", score: 10 });
  mutate.ensure({ id: 1, title: "IGNORED", score: 999 }); // pk exists → no change
  mutate.ensure({ id: 2, title: "second", score: 20 });
  assert.deepEqual(
    view.data.map((r) => [r.id, r.title, r.score]),
    [
      [1, "first", 10],
      [2, "second", 20],
    ],
  );
});

test("update and delete on a missing row are rebase-friendly no-ops", () => {
  const { store, mutate } = makeStore({
    update: (tx) => tx.update("issue", { id: 99, score: 1 }),
    del: (tx) => tx.delete("issue", { id: 99 }),
  });
  const view = store.materialize(queries.all());
  mutate.update(undefined);
  mutate.del(undefined);
  assert.equal(view.data.length, 0);
});

test("typo'd columns, missing columns, and unknown tables throw with the valid names", () => {
  const { mutate } = makeStore({
    typo: (tx) => tx.insert("issue", { id: 1, titel: "oops", score: 0 }),
    partial: (tx) => tx.insert("issue", { id: 1 }),
    noPk: (tx) => tx.update("issue", { score: 5 }),
    badTable: (tx) => tx.insert("issues", { id: 1 }),
  });

  assert.throws(() => mutate.typo(undefined), /unknown column titel on issue — columns: id, title, score/);
  assert.throws(() => mutate.partial(undefined), /missing columns title, score on issue/);
  assert.throws(() => mutate.noPk(undefined), /missing primary-key column id on issue/);
  assert.throws(() => mutate.badTable(undefined), /unknown table "issues" — tables: issue/);
});

test("a nullable column may be omitted from an insert and defaults to null (design 206 §6.2)", () => {
  const acct = table("acct").columns({ id: number(), name: string(), nickname: string().nullable() }).primaryKey("id");
  const s = createSchema({ tables: [acct] });
  const qb = newQueryBuilder(s);
  const { store, mutate } = createOptimisticStore(
    s,
    new NullSource(),
    {
      full: (tx: MutationTx) => tx.insert("acct", { id: 1, name: "a", nickname: "nn" }),
      omit: (tx: MutationTx) => tx.insert("acct", { id: 2, name: "b" }), // nickname left off ⇒ null
      omitNonNull: (tx: MutationTx) => tx.insert("acct", { id: 3, nickname: "x" }), // name (NOT NULL) missing ⇒ throws
    },
    { clientID: "c1" },
  );
  const view = store.materialize(defineQuery("all", () => qb.acct.orderBy("id", "asc"))());

  mutate.full(undefined);
  mutate.omit(undefined);
  assert.deepEqual(
    view.data.map((r) => [r.id, r.name, r.nickname]),
    [
      [1, "a", "nn"],
      [2, "b", null], // omitted nullable column filled with null
    ],
  );
  // a NOT NULL column omitted still throws (same loud contract, just the non-nullable subset)
  assert.throws(() => mutate.omitNonNull(undefined), /missing column name on acct/);
});

test("a typed shared mutator round-trips a json column as a parsed object (toCell funnel stringify)", () => {
  const doc = table("doc").columns({ id: number(), meta: json<{ n: number }>() }).primaryKey("id");
  const s = createSchema({ tables: [doc] });
  const qb = newQueryBuilder(s);
  const { shared } = defineMutators(s);
  const args = { parse: (r: unknown) => r as { id: number; meta: { n: number } } };
  const { store, mutate } = createOptimisticStore(
    s,
    new NullSource(),
    // The body passes `meta` as a PARSED OBJECT (the typed `tx.insert` accepts it); the funnel's
    // `toCell` stringifies it into the engine, and the view parses it back — a clean round-trip.
    { put: shared(args, function* (tx, a) { yield tx.insert("doc", { id: a.id, meta: a.meta }); }) },
    { clientID: "c1" },
  );
  const view = store.materialize(defineQuery("all", () => qb.doc.orderBy("id", "asc"))());

  mutate.put({ id: 1, meta: { n: 42 } });
  assert.deepEqual(
    view.data.map((r) => [r.id, r.meta]),
    [[1, { n: 42 }]], // the object survives insert → stringify → engine → parse
  );
});

test("a thrown keyed-validation error discards the staged prediction and burns no mid", () => {
  const { store, mutate } = makeStore({
    good: (tx, args: { id: number }) => tx.insert("issue", { id: args.id, title: "ok", score: 0 }),
    bad: (tx) => {
      tx.insert("issue", { id: 100, title: "staged then discarded", score: 0 });
      tx.insert("issue", { id: 101, titel: "typo" }); // throws — whole prediction rolls back
    },
  });
  const view = store.materialize(queries.all());

  assert.throws(() => mutate.bad(undefined));
  assert.equal(view.data.length, 0, "the partial staged write must not survive the throw");
  const mid = mutate.good({ id: 1 });
  assert.equal(mid, 1, "the failed invoke must not consume a mid");
  assert.deepEqual(view.data.map((r) => r.id), [1]);
});
