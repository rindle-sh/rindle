import { test } from "node:test";
import assert from "node:assert/strict";

import { createSchema, newQueryBuilder, number, stableKey, Store, string, table } from "@rindle/client";
import type { Backend, ChangeEvent, Mutation, QueryId, RemoteQuery } from "@rindle/client";

import { QueryCache, queryCacheKey } from "../src/index.ts";

const issue = table("issue").columns({ id: number(), title: string() }).primaryKey("id");
const schema = createSchema({ tables: [issue] });
const qb = newQueryBuilder(schema);

// A no-op backend: the server store never streams (mirrors `@rindle/client`'s OneShotBackend).
class NoopBackend implements Backend {
  registerQuery(_qid: QueryId, _ast: unknown, _remote?: RemoteQuery): void {}
  unregisterQuery(_qid: QueryId): void {}
  mutate(_mutations: Mutation[]): Promise<void> {
    return Promise.resolve();
  }
  onEvent(_handler: (qid: QueryId, ev: ChangeEvent) => void): void {}
}

test("serverSnapshot reads the store's seed WITHOUT retaining (the SSR getServerSnapshot path)", () => {
  const store = new Store(schema, new NoopBackend());
  const q = qb.issue;
  store.hydrate({ [stableKey(q.ast())]: { rows: [{ id: 1, title: "seeded" }], cvMin: 3 } });

  const cache = new QueryCache(store);
  const key = queryCacheKey(q);

  // No retain happened (the server never subscribes), yet the seed is surfaced for first paint.
  assert.equal(cache.size(), 0);
  assert.deepStrictEqual(cache.serverSnapshot(key, false), [{ id: 1, title: "seeded" }]);
  assert.equal(cache.serverResultType(key), "complete");
});

test("serverSnapshot unwraps a .one() query, and falls back to empty when not seeded", () => {
  const store = new Store(schema, new NoopBackend());
  const one = qb.issue.where.id(1).one();
  store.hydrate({ [stableKey(one.ast())]: { rows: [{ id: 1, title: "only" }], cvMin: 1 } });

  const cache = new QueryCache(store);
  assert.deepStrictEqual(cache.serverSnapshot(queryCacheKey(one), true), { id: 1, title: "only" });

  // An un-preloaded query renders like an unhydrated one.
  const cold = qb.issue.where.id(99);
  assert.deepStrictEqual(cache.serverSnapshot(queryCacheKey(cold), false), []);
  assert.deepStrictEqual(cache.serverSnapshot(queryCacheKey(cold.one()), true), null);
  assert.equal(cache.serverResultType(queryCacheKey(cold)), "unknown");
});

test("the React viewKey matches @rindle/client's stableKey (so getServerSnapshot finds the seed)", () => {
  const q = qb.issue.where.id(1);
  assert.equal(queryCacheKey(q), stableKey(q.ast()));
});
