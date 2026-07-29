import { test } from "node:test";
import assert from "node:assert/strict";

import { createSchema, json, number, string, table } from "../src/schema.ts";
import { type AssembledNode, Store } from "../src/store.ts";
import { defineQuery, newQueryBuilder, queries } from "../src/query.ts";
import { createServerStore, OneShotBackend, type OneShotResult } from "../src/ssr.ts";
import { stableKey } from "../src/key.ts";
import type { Backend, ChangeEvent, FlatChange, QueryId, RemoteQuery, ResultType, WireSchema } from "../src/types.ts";

const issue = table("issue")
  .columns({ id: number(), title: string(), meta: json<{ tags: string[] }>() })
  .primaryKey("id");
const comment = table("comment").columns({ id: number(), issueId: number(), body: string() }).primaryKey("id");
const schema = createSchema({ tables: [issue, comment] });
const qb = newQueryBuilder(schema);

// An assembled (nested-by-name) row as `POST /query` returns it (SSR-DESIGN.md §3.3).
type Assembled = AssembledNode;

test("createServerStore.preload seeds first-paint rows and dehydrate carries them + cvMin", async () => {
  const seen: Array<{ ast: unknown; visibilityKey?: string; ttlMs?: number }> = [];
  const query = async (input: { ast: unknown; visibilityKey?: string; ttlMs?: number }): Promise<OneShotResult> => {
    seen.push(input);
    return {
      cvMin: 421,
      rows: [
        { cols: { id: 1, title: "first", meta: '{"tags":["a"]}' } },
        { cols: { id: 2, title: "second", meta: "{}" } },
      ] satisfies Assembled[],
    };
  };
  const server = createServerStore(schema, { query, visibilityKey: "v1", ttlMs: 9000 });

  const q = qb.issue;
  await server.preload(q);

  // The one-shot read forwarded the AST + policy knobs.
  assert.deepStrictEqual(seen, [{ ast: q.ast(), visibilityKey: "v1", ttlMs: 9000 }]);

  // The render reads the seeded, projected rows (json column parsed) via the store's view.
  const view = server.store.materialize(q);
  assert.deepStrictEqual(view.data, [
    { id: 1, title: "first", meta: { tags: ["a"] } },
    { id: 2, title: "second", meta: {} },
  ]);

  // Dehydrate is keyed by viewKey and carries the projected rows + the cvMin baseline.
  const dehydrated = server.dehydrate();
  const key = stableKey(q.ast());
  assert.deepStrictEqual(dehydrated[key].cvMin, 421);
  assert.deepStrictEqual(dehydrated[key].rows, [
    { id: 1, title: "first", meta: { tags: ["a"] } },
    { id: 2, title: "second", meta: {} },
  ]);
});

test("preloadAll seeds every query and returns the dehydrated cache in one call", async () => {
  const query = async ({ ast }: { ast: unknown }): Promise<OneShotResult> =>
    JSON.stringify(ast).includes('"comment"')
      ? { cvMin: 2, rows: [{ cols: { id: 10, issueId: 1, body: "hi" } }] satisfies Assembled[] }
      : { cvMin: 1, rows: [{ cols: { id: 1, title: "first", meta: "{}" } }] satisfies Assembled[] };
  const server = createServerStore(schema, { query });

  const dehydrated = await server.preloadAll([qb.issue, qb.comment]);

  assert.deepStrictEqual(dehydrated[stableKey(qb.issue.ast())].rows, [{ id: 1, title: "first", meta: {} }]);
  assert.deepStrictEqual(dehydrated[stableKey(qb.comment.ast())].rows, [{ id: 10, issueId: 1, body: "hi" }]);
});

test("preloadAll degrades a failed read to no seed (onError fires) without rejecting the batch", async () => {
  const query = async ({ ast }: { ast: unknown }): Promise<OneShotResult> => {
    if (JSON.stringify(ast).includes('"comment"')) throw new Error("boom");
    return { cvMin: 1, rows: [{ cols: { id: 1, title: "ok", meta: "{}" } }] satisfies Assembled[] };
  };
  const server = createServerStore(schema, { query });
  const failed: Array<{ key: string; msg: string }> = [];

  // Resolves (never rejects) even though one read threw.
  const dehydrated = await server.preloadAll([qb.issue, qb.comment], {
    onError: (q, err) => failed.push({ key: stableKey(q.ast()), msg: (err as Error).message }),
  });

  assert.ok(dehydrated[stableKey(qb.issue.ast())], "the healthy query still seeded");
  assert.equal(dehydrated[stableKey(qb.comment.ast())], undefined, "the failed query has no seed");
  assert.deepStrictEqual(failed, [{ key: stableKey(qb.comment.ast()), msg: "boom" }]);
});

test("preload refuses a query that references a local-only table (E3 SSR backstop)", async () => {
  const sel = table("sel", { local: true }).columns({ id: number(), issueId: number() }).primaryKey("id");
  const localSchema = createSchema({ tables: [issue, comment, sel] });
  let called = false;
  const server = createServerStore(localSchema, {
    query: async () => {
      called = true;
      return { rows: [] };
    },
  });
  // Built via the LOCAL builder (includeLocal) so its `.ast()` does NOT self-guard — only the
  // preload backstop stops the local table's AST from reaching the daemon. (The OneShotBackend's
  // registerQuery is a no-op, so without this check the AST would be forwarded upstream.)
  const localBuilder = queries(localSchema, undefined, { includeLocal: true });
  await assert.rejects(() => server.preload(localBuilder.sel as never), /local-only table "sel"/);
  assert.equal(called, false, "the one-shot read never ran for a local-referencing query");
});

test("assembleSnapshot nests relationships by alias (plural, empty array)", () => {
  const store = new Store(schema, new OneShotBackend());
  const q = qb.issue.sub("comments", comment, { parent: ["id"], child: ["issueId"] });
  const ast = q.ast();

  const rows: Assembled[] = [
    {
      cols: { id: 1, title: "bug", meta: "{}" },
      comments: [
        { cols: { id: 10, issueId: 1, body: "me too" } },
        { cols: { id: 11, issueId: 1, body: "+1" } },
      ],
    },
    { cols: { id: 2, title: "lonely", meta: "{}" }, comments: [] },
  ];

  const projected = store.assembleSnapshot(ast, rows);
  assert.deepStrictEqual(projected, [
    {
      id: 1,
      title: "bug",
      meta: {},
      comments: [
        { id: 10, issueId: 1, body: "me too" },
        { id: 11, issueId: 1, body: "+1" },
      ],
    },
    { id: 2, title: "lonely", meta: {}, comments: [] },
  ]);
});

test("a top-level .one() seed materializes the single row (or null)", () => {
  const query = async (): Promise<OneShotResult> => ({
    cvMin: 1,
    rows: [{ cols: { id: 7, title: "only", meta: "{}" } }] satisfies Assembled[],
  });
  const server = createServerStore(schema, { query });
  const one = qb.issue.where.id(7).one();
  return server.preload(one).then(() => {
    const view = server.store.materialize(one);
    assert.deepStrictEqual(view.data, { id: 7, title: "only", meta: {} });
  });
});

test("the OneShot backend is read-only — mutate rejects", async () => {
  const store = new Store(schema, new OneShotBackend());
  await assert.rejects(() => store.write((tx) => tx.add("issue", { id: 1, title: "x", meta: { tags: [] } })));
});

// A scriptable async backend: register defers the hello/snapshot until `goLive` is called, so a
// view stays PENDING (showing its seed) until the live data lands — the browser hydration story.
class DeferredBackend implements Backend {
  private handler: (qid: QueryId, ev: ChangeEvent) => void = () => {};
  private pending: QueryId[] = [];
  snapshot: FlatChange[] = [];
  helloSchema: WireSchema = {
    columns: ["id", "title", "meta"],
    primaryKey: [0],
    sort: [[0, true]],
    singular: false,
    relationships: [],
  };
  registerQuery(qid: QueryId, _ast: unknown, _remote?: RemoteQuery): void {
    this.pending.push(qid);
  }
  unregisterQuery(_qid: QueryId): void {}
  mutate(): Promise<void> {
    return Promise.resolve();
  }
  onEvent(handler: (qid: QueryId, ev: ChangeEvent) => void): void {
    this.handler = handler;
  }
  goLive(): void {
    for (const qid of this.pending) {
      this.handler(qid, { type: "hello", schema: this.helloSchema, comparatorVersion: 1 });
      this.handler(qid, { type: "snapshot", adds: this.snapshot, last: true });
    }
    this.pending = [];
  }
}

test("browser hydrate shows the seed first, then the live snapshot reconciles and retires it", () => {
  const be = new DeferredBackend();
  const store = new Store(schema, be);

  const q = qb.issue;
  // Server dehydrated this query; the browser seeds it at boot.
  store.hydrate({ [stableKey(q.ast())]: { rows: [{ id: 1, title: "seeded", meta: {} }], cvMin: 9 } });

  const view = store.materialize(q);
  // First paint: the live backend has not delivered hello yet → the seed shows.
  assert.deepStrictEqual(view.data, [{ id: 1, title: "seeded", meta: {} }]);

  // Live data arrives (a different/fresher row) → reconciles, seed retired.
  be.snapshot = [{ path: [], op: { tag: "add", node: { row: [1, "live", "{}"], rels: [] } } }];
  be.goLive();
  assert.deepStrictEqual(view.data, [{ id: 1, title: "live", meta: {} }]);

  // The seed was consumed on the first live snapshot — a fresh mount no longer flashes it.
  const fresh = store.materialize(q);
  assert.deepStrictEqual(fresh.data, [], "no stale SSR flash after the view went live");
});

// Like DeferredBackend, but delivers `hello` and `snapshot` SEPARATELY so a test can observe the view
// in the window between them — exactly what the browser renders across the subscribe→snapshot network
// round-trip. `DeferredBackend.goLive` fires both synchronously and so can't expose that gap (which is
// why the seed→empty→live flash it was meant to prevent went uncaught).
class SteppedBackend implements Backend {
  private handler: (qid: QueryId, ev: ChangeEvent) => void = () => {};
  private readonly qids: QueryId[] = [];
  snapshot: FlatChange[] = [];
  helloSchema: WireSchema = {
    columns: ["id", "title", "meta"],
    primaryKey: [0],
    sort: [[0, true]],
    singular: false,
    relationships: [],
  };
  registerQuery(qid: QueryId, _ast: unknown, _remote?: RemoteQuery): void {
    this.qids.push(qid);
  }
  unregisterQuery(_qid: QueryId): void {}
  mutate(): Promise<void> {
    return Promise.resolve();
  }
  onEvent(handler: (qid: QueryId, ev: ChangeEvent) => void): void {
    this.handler = handler;
  }
  sendHello(): void {
    for (const qid of this.qids) this.handler(qid, { type: "hello", schema: this.helloSchema, comparatorVersion: 1 });
  }
  sendSnapshot(): void {
    for (const qid of this.qids) this.handler(qid, { type: "snapshot", adds: this.snapshot, last: true });
  }
}

test("a seeded view bridges the hello→snapshot gap without flashing empty (retired on snapshot)", () => {
  const be = new SteppedBackend();
  const store = new Store(schema, be);
  const q = qb.issue;
  store.hydrate({ [stableKey(q.ast())]: { rows: [{ id: 1, title: "seeded", meta: {} }], cvMin: 9 } });

  const view = store.materialize(q);
  // First paint (pre-hello): the seed shows.
  assert.deepStrictEqual(view.data, [{ id: 1, title: "seeded", meta: {} }]);

  // The live `hello` lands but its snapshot has NOT — the browser renders HERE across the network
  // round-trip. The seed must STILL show (the regression: `reset` used to null it → an empty flash).
  be.sendHello();
  assert.deepStrictEqual(view.data, [{ id: 1, title: "seeded", meta: {} }], "seed bridges the hello→snapshot gap");

  // The first snapshot lands → the maintained tree takes over and the seed is retired.
  be.snapshot = [{ path: [], op: { tag: "add", node: { row: [1, "live", "{}"], rels: [] } } }];
  be.sendSnapshot();
  assert.deepStrictEqual(view.data, [{ id: 1, title: "live", meta: {} }]);

  // Retired on the snapshot (not the hello): a fresh mount no longer flashes the stale seed.
  assert.deepStrictEqual(store.materialize(q).data, [], "seed retired after the first snapshot");
});

test("an empty first snapshot still retires the seed (0 rows is an authoritative answer)", () => {
  const be = new SteppedBackend();
  const store = new Store(schema, be);
  const q = qb.issue;
  store.hydrate({ [stableKey(q.ast())]: { rows: [{ id: 1, title: "seeded", meta: {} }], cvMin: 9 } });

  const view = store.materialize(q);
  be.sendHello();
  assert.deepStrictEqual(view.data, [{ id: 1, title: "seeded", meta: {} }], "seed shows across the gap");

  // The live answer is genuinely empty — the seed must give way to it, not linger.
  be.snapshot = [];
  be.sendSnapshot();
  assert.deepStrictEqual(view.data, [], "the seed is retired even by an empty snapshot");
  assert.deepStrictEqual(store.materialize(q).data, [], "and stays retired for later mounts");
});

test("dehydrate → hydrate round-trips through JSON (the wire embed)", async () => {
  const query = async (): Promise<OneShotResult> => ({
    cvMin: 5,
    rows: [{ cols: { id: 1, title: "x", meta: '{"tags":["t"]}' } }] satisfies Assembled[],
  });
  const server = createServerStore(schema, { query });
  await server.preload(qb.issue);

  const wire = JSON.parse(JSON.stringify(server.dehydrate()));
  const browser = new Store(schema, new OneShotBackend());
  browser.hydrate(wire);

  const view = browser.materialize(qb.issue);
  assert.deepStrictEqual(view.data, [{ id: 1, title: "x", meta: { tags: ["t"] } }]);
});

test("assembleSnapshot unwraps a countAs scalar aggregate", () => {
  const store = new Store(schema, new OneShotBackend());
  const q = qb.issue.countAs("commentCount", comment, { parent: ["id"], child: ["issueId"] });
  const rows: Assembled[] = [
    { cols: { id: 1, title: "bug", meta: "{}" }, commentCount: 2 },
    { cols: { id: 2, title: "lonely", meta: "{}" }, commentCount: 0 },
  ];
  assert.deepStrictEqual(store.assembleSnapshot(q.ast(), rows), [
    { id: 1, title: "bug", meta: {}, commentCount: 2 },
    { id: 2, title: "lonely", meta: {}, commentCount: 0 },
  ]);
});

// The OPTIMISTIC/wasm backend's real event ordering (confirmed against @rindle/optimistic): a
// SYNCHRONOUS `registerQuery` fires hello → a LOCAL, pre-sync (empty) snapshot → resultType `unknown`;
// then a server release flips the query to `complete` FIRST and delivers the authoritative rows one
// event later as a `catchUp` BATCH. This is the sequence the "retire on first snapshot" rule did NOT
// cover — the pre-sync snapshot retired the seed a whole server round-trip early (an empty flash).
class OptimisticLikeBackend implements Backend {
  private handler: (qid: QueryId, ev: ChangeEvent) => void = () => {};
  private rt: (qid: QueryId, rt: ResultType) => void = () => {};
  private readonly qids: QueryId[] = [];
  helloSchema: WireSchema = {
    columns: ["id", "title", "meta"],
    primaryKey: [0],
    sort: [[0, true]],
    singular: false,
    relationships: [],
  };
  registerQuery(qid: QueryId, _ast: unknown, _remote?: RemoteQuery): void {
    this.qids.push(qid);
    // Synchronous, in this order: hello, an empty pre-sync LOCAL snapshot, then resultType=unknown.
    this.handler(qid, { type: "hello", schema: this.helloSchema, comparatorVersion: 1 });
    this.handler(qid, { type: "snapshot", adds: [], last: true });
    this.rt(qid, "unknown");
  }
  unregisterQuery(_qid: QueryId): void {}
  mutate(): Promise<void> {
    return Promise.resolve();
  }
  onEvent(h: (qid: QueryId, ev: ChangeEvent) => void): void {
    this.handler = h;
  }
  onResultType(h: (qid: QueryId, rt: ResultType) => void): void {
    this.rt = h;
  }
  /** A server release: flip to `complete` FIRST (as the optimistic backend does) — rows NOT yet folded. */
  markComplete(): void {
    for (const qid of this.qids) this.rt(qid, "complete");
  }
  /** …then deliver the authoritative rows as a `catchUp` batch — the true hydration point. */
  deliverCatchUp(adds: FlatChange[]): void {
    for (const qid of this.qids) this.handler(qid, { type: "batch", events: adds, catchUp: true });
  }
}

test("a seeded REMOTE query bridges the FULL optimistic window (unknown snapshot → complete → catchUp batch) without flashing empty", () => {
  const be = new OptimisticLikeBackend();
  const store = new Store(schema, be);
  const decksQuery = defineQuery("decks", (_a: { limit: number }) => qb.issue);
  const q = decksQuery({ limit: 10 });
  store.hydrate({ [stableKey(q.ast())]: { rows: [{ id: 1, title: "seeded", meta: { tags: [] } }], cvMin: 9 } });

  const view = store.materialize(q);
  // `registerQuery` fired SYNCHRONOUSLY: hello + an empty pre-sync LOCAL snapshot + resultType=unknown.
  // The seed must survive that pre-sync snapshot (the regression: it was retired here → an empty flash).
  assert.deepStrictEqual(view.data, [{ id: 1, title: "seeded", meta: { tags: [] } }], "seed survives the pre-sync snapshot");
  assert.equal(view.resultType, "unknown", "a lifecycle-backed remote query starts PENDING, not complete");

  // The query flips to `complete` BEFORE its authoritative rows arrive (the optimistic ordering). The
  // seed must STILL show — retiring on `complete` here would blank the view for one event.
  be.markComplete();
  assert.deepStrictEqual(
    view.data,
    [{ id: 1, title: "seeded", meta: { tags: [] } }],
    "seed survives the complete-before-data gap",
  );
  assert.equal(view.resultType, "complete");

  // The authoritative rows land as a catchUp batch — the real hydration point — and the seed retires.
  be.deliverCatchUp([{ path: [], op: { tag: "add", node: { row: [2, "synced", "{}"], rels: [] } } }]);
  assert.deepStrictEqual(view.data, [{ id: 2, title: "synced", meta: {} }], "live rows take over on the catchUp batch");

  // Retired from the seeds map too: a fresh mount no longer flashes the stale seed.
  assert.deepStrictEqual(store.materialize(q).data, [], "seed retired after hydration");
});
