import { test } from "node:test";
import assert from "node:assert/strict";

import { createSchema, defineQuery, newQueryBuilder, number, Store, string, table } from "@rindle/client";
import type { Backend, ChangeEvent, FlatChange, Mutation, QueryId, RemoteQuery, ResultType, WireSchema } from "@rindle/client";

import { QueryCache, queryCacheKey, SyncQueryCache } from "../src/index.ts";

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

const tick = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

test("QueryCache keeps a released query warm for the release delay, then tears it down", async () => {
  const backend = new SplitBackend();
  const store = new Store(schema, backend);
  const cache = new QueryCache(store, { releaseDelayMs: 40 });
  const q = issueQueries.byID({ id: 1 });
  const key = queryCacheKey(q);

  cache.release(cache.retain(key, q));
  assert.equal(cache.size(), 1, "the entry survives its last release");
  assert.deepStrictEqual(backend.remoteReleased, [], "the server lease is held through the window");
  assert.deepStrictEqual(backend.unregistered, [], "the local view stays materialized");

  await tick(80);
  assert.equal(cache.size(), 0);
  assert.deepStrictEqual(backend.remoteReleased, [2 as QueryId], "the window elapsed → server lease back");
  assert.deepStrictEqual(backend.unregistered, [1 as QueryId]);
});

test("QueryCache re-retained inside the release window keeps the entry and drops only the stale lease", async () => {
  const backend = new SplitBackend();
  const store = new Store(schema, backend);
  const cache = new QueryCache(store, { releaseDelayMs: 40 });
  const q = issueQueries.byID({ id: 1 });
  const key = queryCacheKey(q);

  cache.release(cache.retain(key, q));
  const revived = cache.retain(key, q); // a remount lands inside the grace window

  await tick(80);
  assert.equal(cache.size(), 1, "the pending timer must not evict an entry that was re-retained");
  assert.deepStrictEqual(backend.remoteReleased, [2 as QueryId], "only the stale lease goes back");
  assert.deepStrictEqual(backend.unregistered, [], "the local view was never destroyed");
  assert.equal(backend.registered.length, 1, "and never re-materialized — that is the point of the window");

  cache.release(revived);
  await tick(80);
  assert.equal(cache.size(), 0);
  assert.deepStrictEqual(backend.remoteReleased, [2 as QueryId, 3 as QueryId]);
});

test("QueryCache releaseDelayMs: 0 asks for no warm window of its own", () => {
  const backend = new SplitBackend();
  const store = new Store(schema, backend);
  const cache = new QueryCache(store, { releaseDelayMs: 40 });
  const q = issueQueries.byID({ id: 1 });
  const key = queryCacheKey(q);

  // The typeahead case: each keystroke is its own query and is never coming back.
  cache.release(cache.retain(key, q, 0));

  assert.equal(cache.size(), 0, "torn down synchronously, no timer");
  assert.deepStrictEqual(backend.remoteReleased, [2 as QueryId]);
  assert.deepStrictEqual(backend.unregistered, [1 as QueryId]);
});

test("QueryCache per-lease delays are max-wins across a shared query", async () => {
  const backend = new SplitBackend();
  const store = new Store(schema, backend);
  const cache = new QueryCache(store, { releaseDelayMs: 0 });
  const q = issueQueries.byID({ id: 1 });
  const key = queryCacheKey(q);

  const eager = cache.retain(key, q, 0);
  const warm = cache.retain(key, q, 40);

  cache.release(eager);
  assert.equal(cache.size(), 1, "a non-last lease never tears the entry down");
  assert.deepStrictEqual(backend.remoteReleased, [2 as QueryId], "but its server lease goes back at once");

  cache.release(warm);
  assert.equal(cache.size(), 1, "the longer window wins for the entry");
  await tick(80);
  assert.equal(cache.size(), 0);
  assert.deepStrictEqual(backend.remoteReleased, [2 as QueryId, 3 as QueryId]);
});

test("QueryCache max-wins does not depend on unmount ORDER — the warm lease leaves first", async () => {
  const backend = new SplitBackend();
  const store = new Store(schema, backend);
  const cache = new QueryCache(store, { releaseDelayMs: 0 });
  const q = issueQueries.byID({ id: 1 });
  const key = queryCacheKey(q);

  const warm = cache.retain(key, q, 300);
  const eager = cache.retain(key, q, 0);

  // Reverse of the test above. The window is a DEADLINE stamped at release, so `warm` leaving first
  // does not discard what it asked for — an opted-out sibling cannot shrink a live claim.
  cache.release(warm);
  cache.release(eager);
  assert.equal(cache.size(), 1, "warm's deadline still stands after the eager lease leaves last");

  await tick(500);
  assert.equal(cache.size(), 0, "and it expires on its own");
  assert.deepStrictEqual(backend.remoteReleased, [2 as QueryId, 3 as QueryId]);
});

test("QueryCache hands the stale lease back on re-retain and never re-materializes the view", () => {
  const backend = new SplitBackend();
  const store = new Store(schema, backend);
  const cache = new QueryCache(store, { releaseDelayMs: 300 });
  const q = issueQueries.byID({ id: 1 });
  const key = queryCacheKey(q);

  cache.release(cache.retain(key, q));
  assert.deepStrictEqual(backend.remoteReleased, [], "the server lease is held through the window");

  // Reviving the entry makes the deferred lease redundant: the new lease already covers the query, so
  // holding both would stack server subscriptions for as long as the churn lasts.
  cache.retain(key, q);
  assert.deepStrictEqual(backend.remoteReleased, [2 as QueryId], "the stale lease goes back at once");
  assert.equal(backend.registered.length, 1, "and the local view was never destroyed or rebuilt");
  assert.equal(cache.size(), 1);
});

test("QueryCache a later lease inherits the RESIDUE of an older window, not a fresh copy", async () => {
  const backend = new SplitBackend();
  const store = new Store(schema, backend);
  const cache = new QueryCache(store, { releaseDelayMs: 0 });
  const q = issueQueries.byID({ id: 1 });
  const key = queryCacheKey(q);

  cache.release(cache.retain(key, q, 300)); // deadline ≈ t+300
  await tick(200);
  cache.release(cache.retain(key, q, 0));   // t≈200: opted out, so it adds nothing of its own
  assert.equal(cache.size(), 1, "the older window has ~100ms left to run");

  await tick(220);
  // A duration-based window would restart here and survive to t≈500. A deadline expires on its own.
  assert.equal(cache.size(), 0, "torn down at the ORIGINAL deadline, not 300ms past the remount");
});

test("QueryCache never times out a mounted subscriber, and its clock starts when it LEAVES", async () => {
  const backend = new SplitBackend();
  const store = new Store(schema, backend);
  const cache = new QueryCache(store, { releaseDelayMs: 0 });
  const q = issueQueries.byID({ id: 1 });
  const key = queryCacheKey(q);

  const mounted = cache.retain(key, q, 300);
  // Churn an opted-out lease against the same key across several windows' worth of wall time.
  for (let i = 0; i < 4; i++) {
    await tick(100);
    cache.release(cache.retain(key, q, 0));
    assert.equal(cache.size(), 1, `still mounted after ${(i + 1) * 100}ms`);
    assert.deepStrictEqual(backend.unregistered, [], "the mounted subscriber's view is never destroyed");
  }

  cache.release(mounted); // ~400ms after mounting: the window is measured from HERE
  await tick(200);
  assert.equal(cache.size(), 1, "a full window from the moment the last subscriber left");
  await tick(220);
  assert.equal(cache.size(), 0);
});

test("SyncQueryCache honors a per-lease release delay", async () => {
  const backend = new SplitBackend();
  const store = new Store(schema, backend);
  const cache = new SyncQueryCache(store, { releaseDelayMs: 40 });
  const q = issueQueries.byID({ id: 1 });

  cache.release(cache.retain("cov", q, 0));
  assert.equal(cache.size(), 0, "an opted-out coverage lease releases synchronously");

  cache.release(cache.retain("cov", q));
  assert.equal(cache.size(), 1, "the cache default still applies when no override is passed");
  await tick(80);
  assert.equal(cache.size(), 0);
});

test("SyncQueryCache follows the same deadline rule — order-independent, residue-only", async () => {
  const backend = new SplitBackend();
  const store = new Store(schema, backend);
  const cache = new SyncQueryCache(store, { releaseDelayMs: 0 });
  const q = issueQueries.byID({ id: 1 });

  const warm = cache.retain("cov", q, 300);
  const eager = cache.retain("cov", q, 0);
  cache.release(warm); // the warm coverage lease leaves FIRST
  cache.release(eager);
  assert.equal(cache.size(), 1, "warm's deadline survives its own unmount");

  await tick(200);
  cache.release(cache.retain("cov", q, 0)); // a remount inside the window adds nothing
  await tick(220);
  assert.equal(cache.size(), 0, "expired at the original deadline");
});

test("QueryCache ignores the release delay for a local-only store", () => {
  const backend = new FakeBackend(); // no retain/releaseRemoteQuery → materialized mode
  const store = new Store(schema, backend);
  const cache = new QueryCache(store, { releaseDelayMs: 40 });
  const q = issueQueries.byID({ id: 1 });
  const key = queryCacheKey(q);

  cache.release(cache.retain(key, q));
  assert.equal(cache.size(), 0, "no server lease to hold → nothing to keep warm");
  assert.deepStrictEqual(backend.unregistered, [1 as QueryId]);
});
