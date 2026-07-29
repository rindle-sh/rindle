import { test } from "node:test";
import assert from "node:assert/strict";

import { createSchema, defineQuery, newQueryBuilder, number, Store, string, table } from "@rindle/client";
import type { Backend, ChangeEvent, FlatChange, Mutation, QueryId, RemoteQuery, ResultType, WireSchema } from "@rindle/client";

import { QueryCache, queryCacheKey } from "../src/index.ts";

const issue = table("issue").columns({ id: number(), title: string() }).primaryKey("id");
const schema = createSchema({ tables: [issue] });

const issueWire: WireSchema = {
  columns: ["id", "title"],
  primaryKey: [0],
  sort: [[0, true]],
  singular: false,
  relationships: [],
};

const addIssue = (id: number, title: string): FlatChange => ({
  path: [],
  op: { tag: "add", node: { row: [id, title], rels: [] } },
});

class FakeBackend implements Backend {
  private handler: (qid: QueryId, ev: ChangeEvent) => void = () => {};
  readonly registered: Array<{ qid: QueryId; ast: unknown; remote?: RemoteQuery }> = [];
  readonly unregistered: QueryId[] = [];
  readonly mutations: Mutation[][] = [];
  snapshot: FlatChange[] = [];

  registerQuery(qid: QueryId, ast: unknown, remote?: RemoteQuery): void {
    this.registered.push({ qid, ast, remote });
    this.handler(qid, { type: "hello", schema: issueWire, comparatorVersion: 1 });
    this.handler(qid, { type: "snapshot", adds: this.snapshot, last: true });
  }

  unregisterQuery(qid: QueryId): void {
    this.unregistered.push(qid);
  }

  mutate(mutations: Mutation[]): Promise<void> {
    this.mutations.push(mutations);
    return Promise.resolve();
  }

  onEvent(handler: (qid: QueryId, ev: ChangeEvent) => void): void {
    this.handler = handler;
  }
}

class SplitBackend extends FakeBackend {
  readonly remoteRetained: Array<{ qid: QueryId; remote: RemoteQuery; localQueryId?: QueryId }> = [];
  readonly remoteReleased: QueryId[] = [];

  retainRemoteQuery(qid: QueryId, remote: RemoteQuery, localQueryId?: QueryId): void {
    this.remoteRetained.push({ qid, remote, localQueryId });
  }

  releaseRemoteQuery(qid: QueryId): void {
    this.remoteReleased.push(qid);
  }
}

/** A backend with a per-query lifecycle, to drive `cache.resultType` routing. */
class LifecycleBackend extends FakeBackend {
  private rtHandler: (qid: QueryId, rt: ResultType) => void = () => {};
  onResultType(handler: (qid: QueryId, rt: ResultType) => void): void {
    this.rtHandler = handler;
  }
  pushResultType(qid: QueryId, rt: ResultType): void {
    this.rtHandler(qid, rt);
  }
}

const qb = newQueryBuilder(schema);
const issueQueries = {
  byID: defineQuery("byID", (args: { id: number }) => qb.issue.where.id(args.id)),
  byIDAlias: defineQuery("byIDAlias", (args: { id: number }) => qb.issue.where.id(args.id)),
};

test("queryCacheKey is stable for equivalent named query calls", () => {
  assert.equal(queryCacheKey(issueQueries.byID({ id: 1 })), queryCacheKey(issueQueries.byID({ id: 1 })));
  assert.equal(
    queryCacheKey(issueQueries.byID({ id: 1 })),
    queryCacheKey(issueQueries.byIDAlias({ id: 1 })),
    "the React-facing view is keyed by local AST, not remote identity",
  );
});

test("QueryCache shares one data view but sends every mounted query to the backend", () => {
  const backend = new FakeBackend();
  backend.snapshot = [addIssue(1, "first")];
  const store = new Store(schema, backend);
  const cache = new QueryCache(store);
  const first = issueQueries.byID({ id: 1 });
  const second = issueQueries.byID({ id: 1 });
  const key = queryCacheKey(first);

  const firstLease = cache.retain(key, first);
  const secondLease = cache.retain(key, second);

  assert.equal(cache.size(), 1);
  assert.equal(backend.registered.length, 2, "backend still sees both mounted query calls");
  assert.deepStrictEqual(cache.snapshot(key, false), [{ id: 1, title: "first" }]);

  cache.release(firstLease);
  assert.equal(cache.size(), 1);
  assert.deepStrictEqual(backend.unregistered, [1 as QueryId], "each hook releases its own backend lease");
  assert.deepStrictEqual(cache.snapshot(key, false), [{ id: 1, title: "first" }]);

  cache.release(secondLease);
  assert.equal(cache.size(), 0);
  assert.deepStrictEqual(backend.unregistered, [1 as QueryId, 2 as QueryId]);
});

test("QueryCache surfaces the backend's resultType through the cached view, and notifies on change", () => {
  const backend = new LifecycleBackend();
  const store = new Store(schema, backend);
  const cache = new QueryCache(store);
  const q = issueQueries.byID({ id: 1 });
  const key = queryCacheKey(q);

  assert.equal(cache.resultType(key), "unknown", "no entry retained yet → loading");

  const lease = cache.retain(key, q);
  // A REMOTE query on a lifecycle backend starts PENDING (`unknown`) until the backend confirms sync —
  // the Store marks it `unknown` at register, so it never spuriously reads `complete` before its first
  // synced snapshot (the seed-retirement / status-loading fix).
  assert.equal(cache.resultType(key), "unknown");

  let notified = 0;
  const unsubscribe = cache.subscribe(key, () => notified++);
  const qid = backend.registered[0].qid;
  backend.pushResultType(qid, "complete"); // the Store routes a qid-keyed lifecycle onto the view
  assert.equal(cache.resultType(key), "complete", "a backend lifecycle change reaches the view");
  assert.ok(notified > 0, "the change notifies cache subscribers (so a status hook re-renders)");

  unsubscribe();
  cache.release(lease);
  assert.equal(cache.resultType(key), "unknown", "no entry after release → loading default");
});

test("QueryCache uses the split Store API when available", () => {
  const backend = new SplitBackend();
  backend.snapshot = [addIssue(1, "first")];
  const store = new Store(schema, backend);
  const cache = new QueryCache(store);
  const first = issueQueries.byID({ id: 1 });
  const second = issueQueries.byID({ id: 1 });
  const key = queryCacheKey(first);

  const firstLease = cache.retain(key, first);
  const secondLease = cache.retain(key, second);

  assert.equal(cache.size(), 1);
  assert.equal(backend.registered.length, 1, "one local AST view is materialized");
  assert.deepStrictEqual(backend.registered[0].remote, undefined);
  assert.deepStrictEqual(
    backend.remoteRetained.map((r) => [r.qid, r.remote, r.localQueryId]),
    [
      [2 as QueryId, { name: "byID", args: { id: 1 } }, 1 as QueryId],
      [3 as QueryId, { name: "byID", args: { id: 1 } }, 1 as QueryId],
    ],
    "each hook still retains its own named backend lease, attached to the shared local view",
  );
  assert.deepStrictEqual(cache.snapshot(key, false), [{ id: 1, title: "first" }]);

  cache.release(firstLease);
  assert.equal(cache.size(), 1);
  assert.deepStrictEqual(backend.remoteReleased, [2 as QueryId]);
  assert.deepStrictEqual(backend.unregistered, [], "the local view stays alive until the last hook releases");

  cache.release(secondLease);
  assert.equal(cache.size(), 0);
  assert.deepStrictEqual(backend.remoteReleased, [2 as QueryId, 3 as QueryId]);
  assert.deepStrictEqual(backend.unregistered, [1 as QueryId]);
});
