// SSR seed retirement over the NORMALIZED backend (issue #349). A seeded remote query must show its
// SSR seed until the server hydrates it, then switch to the live rows — never flash empty and never
// stay stuck on the stale seed. The normalized backend hydrates by MUTATING its embedded local engine
// (the view's first result set therefore arrives as a `batch`, not a `snapshot`), so it must (a) flip
// the query to `complete` BEFORE folding, and (b) stamp that fold `catchUp` so the Store treats it as
// the hydration point (retire the seed / phase it as a snapshot). This pins both.

import { test } from "node:test";
import assert from "node:assert/strict";

import {
  createSchema,
  defineQuery,
  gt,
  initWasm,
  newQueryBuilder,
  number,
  stableKey,
  string,
  table,
  type NormalizedEvent,
  type NormalizedOp,
  type NormalizedTableSchema,
  type NormalizedSource,
  type QueryId,
  type RemoteQuery,
} from "@rindle/wasm";

import { createNormalizedStore } from "../src/backend.ts";

await initWasm();

const issue = table("issue").columns({ id: number(), title: string() }).primaryKey("id");
const schema = createSchema({ tables: [issue] });
const qb = newQueryBuilder(schema);
const issueHello: NormalizedTableSchema[] = [{ name: "issue", columns: ["id", "title"], primaryKey: [0] }];

/** A hand-driven {@link NormalizedSource}: it records the sourceQid each query registers under and
 *  lets the test deliver the server's `hello`/`snapshot` on demand — so the seed genuinely bridges
 *  the register→hydrate gap instead of the source hydrating synchronously inside `registerQuery`. */
class ManualSource implements NormalizedSource {
  handler: (qid: QueryId, ev: NormalizedEvent) => void = () => {};
  registered: Array<{ qid: QueryId; remote: RemoteQuery }> = [];
  registerQuery(qid: QueryId, remote: RemoteQuery): void {
    this.registered.push({ qid, remote });
  }
  unregisterQuery(): void {}
  mutate(): Promise<void> {
    return Promise.resolve();
  }
  onNormalized(h: (qid: QueryId, ev: NormalizedEvent) => void): void {
    this.handler = h;
  }
  hello(qid: QueryId, tables: NormalizedTableSchema[]): void {
    this.handler(qid, { type: "hello", tables, comparatorVersion: 1, normalizedFp: "fp" });
  }
  snapshot(qid: QueryId, ops: NormalizedOp[]): void {
    this.handler(qid, { type: "snapshot", ops });
  }
  qidFor(name: string): QueryId {
    const rec = this.registered.find((r) => r.remote.name === name);
    if (!rec) throw new Error(`query "${name}" never registered with the source`);
    return rec.qid;
  }
}

const decksQuery = defineQuery("decks", (_a: { limit: number }) => qb.issue);

test("a seeded REMOTE query retires its SSR seed when the normalized snapshot hydrates it", () => {
  const source = new ManualSource();
  const store = createNormalizedStore(schema, source);

  const q = decksQuery({ limit: 10 });
  store.hydrate({ [stableKey(q.ast())]: { rows: [{ id: 1, title: "seeded" }], cvMin: 0 } });

  const view = store.materialize(q);
  const sourceQid = source.qidFor("decks");

  // The local engine's own pre-data snapshot (empty) fired synchronously inside `materialize`. The
  // seed must survive it — the query is PENDING (`unknown`), so it is NOT yet authoritative.
  assert.deepStrictEqual(view.data, [{ id: 1, title: "seeded" }], "seed shows until the server hydrates");
  assert.equal(view.resultType, "unknown", "a remote query on a lifecycle backend starts PENDING");

  // The server's first normalized snapshot IS the hydration point: the backend flips the query to
  // `complete`, then folds the rows into the local engine — a `catchUp`-stamped batch — so the Store
  // retires the seed and the live rows take over with no empty flash between them.
  source.hello(sourceQid, issueHello);
  source.snapshot(sourceQid, [{ table: "issue", op: "add", row: [2, "synced"] }]);

  assert.equal(view.resultType, "complete", "hydration flips the query authoritative");
  assert.deepStrictEqual(view.data, [{ id: 2, title: "synced" }], "live rows replace the seed on hydration");

  // Retired from the seeds map too: a fresh mount reads the live engine, never re-flashes the seed.
  assert.deepStrictEqual(
    store.materialize(q).data,
    [{ id: 2, title: "synced" }],
    "a later mount shows live rows, not the retired seed",
  );
});

// Level B for the 0-net-muts case (#349): a seeded query whose ENTIRE result is already present in
// the engine — folded in via an already-hydrated sibling — hydrates to ZERO net base mutations, so
// no fold-batch is emitted. The backend must still emit an empty catch-up so the Store retires the
// seed and reveals the covered rows (otherwise the view freezes on the stale seed).
test("a query covered by an already-hydrated sibling (0 net muts) still retires its seed", () => {
  const source = new ManualSource();
  const store = createNormalizedStore(schema, source);

  // Distinct ASTs (distinct seeds) with overlapping rows: the subset (id > 0) ⊆ the superset (all).
  const qAll = defineQuery("coverAll", (_a: { n: number }) => qb.issue)({ n: 0 });
  const qSubset = defineQuery("coverSubset", (_a: { n: number }) => qb.issue.where.id(gt(0)))({ n: 0 });
  store.hydrate({
    [stableKey(qAll.ast())]: { rows: [{ id: 7, title: "seedAll" }], cvMin: 0 },
    [stableKey(qSubset.ast())]: { rows: [{ id: 8, title: "seedSubset" }], cvMin: 0 },
  });

  const viewAll = store.materialize(qAll);
  const viewSubset = store.materialize(qSubset);
  const rows: NormalizedOp[] = [
    { table: "issue", op: "add", row: [1, "a"] },
    { table: "issue", op: "add", row: [2, "b"] },
  ];

  // The superset hydrates first, loading the shared rows into the engine (routed to BOTH views — the
  // subset folds them as a PLAIN batch under its own seed, staying PENDING).
  source.hello(source.qidFor("coverAll"), issueHello);
  source.snapshot(source.qidFor("coverAll"), rows);
  assert.deepStrictEqual(viewAll.data, [{ id: 1, title: "a" }, { id: 2, title: "b" }], "superset hydrated");
  assert.deepStrictEqual(viewSubset.data, [{ id: 8, title: "seedSubset" }], "subset still shows its seed");
  assert.equal(viewSubset.resultType, "unknown");

  // The subset hydrates over the SAME rows — already present, so 0 net muts and no fold-batch. The
  // empty catch-up must retire its seed and reveal the rows already in its tree.
  source.hello(source.qidFor("coverSubset"), issueHello);
  source.snapshot(source.qidFor("coverSubset"), rows);
  assert.equal(viewSubset.resultType, "complete", "subset became authoritative");
  assert.deepStrictEqual(
    viewSubset.data,
    [{ id: 1, title: "a" }, { id: 2, title: "b" }],
    "subset retired its seed and reveals the covered rows (not frozen)",
  );
});
