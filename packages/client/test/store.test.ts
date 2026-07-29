import { test } from "node:test";
import assert from "node:assert/strict";

import { createSchema, json, number, string, table } from "../src/schema.ts";
import { Store } from "../src/store.ts";
import { defineQuery, newQueryBuilder } from "../src/query.ts";
import { gt } from "../src/operators.ts";
import type { Ast } from "../src/ast.ts";
import type { Backend, ChangeEvent, FlatChange, Mutation, QueryId, RemoteQuery, ResultType, WireSchema } from "../src/types.ts";

const issue = table("issue")
  .columns({ id: number(), title: string(), meta: json<{ tags: string[] }>() })
  .primaryKey("id");
const schema = createSchema({ tables: [issue] });

const issueWire: WireSchema = {
  columns: ["id", "title", "meta"],
  primaryKey: [0],
  sort: [[0, true]],
  singular: false,
  relationships: [],
};
const addIssue = (id: number, title: string, metaJson: string): FlatChange => ({
  path: [],
  op: { tag: "add", node: { row: [id, title, metaJson], rels: [] } },
});

/** A scriptable fake backend: the test sets the hello schema + snapshot, and can push batches. */
class FakeBackend implements Backend {
  private handler: (qid: QueryId, ev: ChangeEvent) => void = () => {};
  readonly mutations: Mutation[][] = [];
  readonly registered: Array<{ qid: QueryId; ast: unknown; remote?: RemoteQuery }> = [];
  helloSchema: WireSchema = issueWire;
  snapshot: FlatChange[] = [];

  readonly unregistered: QueryId[] = [];

  registerQuery(qid: QueryId, ast: unknown, remote?: RemoteQuery): void {
    this.registered.push({ qid, ast, remote });
    this.handler(qid, { type: "hello", schema: this.helloSchema, comparatorVersion: 1 });
    this.handler(qid, { type: "snapshot", adds: this.snapshot, last: true });
  }
  unregisterQuery(qid: QueryId): void {
    this.unregistered.push(qid);
  }
  mutate(muts: Mutation[]): Promise<void> {
    this.mutations.push(muts);
    return Promise.resolve();
  }
  onEvent(h: (qid: QueryId, ev: ChangeEvent) => void): void {
    this.handler = h;
  }
  emit(qid: QueryId, ev: ChangeEvent): void {
    this.handler(qid, ev);
  }
}

/** A fake backend that delivers a coherent multi-query commit the way the in-process engine does:
 *  it brackets the per-query batch deliveries with `onCommitBoundary("begin"/"end")`, so the Store
 *  folds every affected view before notifying any subscriber (cross-view-atomic notification). */
class CommitFakeBackend extends FakeBackend {
  private boundary: (phase: "begin" | "end") => void = () => {};

  onCommitBoundary(handler: (phase: "begin" | "end") => void): void {
    this.boundary = handler;
  }

  /** Deliver one commit: fold all the per-query batches, then notify — exactly the
   *  `WasmBackend.dispatch` bracket. The deltas are pre-computed (the engine commits first), so the
   *  ORDER the batches are listed in must not affect what a subscriber observes. */
  commit(batches: Array<{ qid: QueryId; events: FlatChange[] }>): void {
    this.boundary("begin");
    try {
      for (const b of batches) this.emit(b.qid, { type: "batch", events: b.events });
    } finally {
      this.boundary("end");
    }
  }
}

class SplitFakeBackend extends FakeBackend {
  readonly remoteRetained: Array<{ qid: QueryId; remote: RemoteQuery; localQueryId?: QueryId }> = [];
  readonly remoteReleased: QueryId[] = [];

  retainRemoteQuery(qid: QueryId, remote: RemoteQuery, localQueryId?: QueryId): void {
    this.remoteRetained.push({ qid, remote, localQueryId });
  }

  releaseRemoteQuery(qid: QueryId): void {
    this.remoteReleased.push(qid);
  }
}

test("materialize → hello+snapshot builds a hydrated view; json column is parsed", () => {
  const be = new FakeBackend();
  be.snapshot = [addIssue(1, "first", '{"tags":["a","b"]}')];
  const store = new Store(schema, be);
  const view = store.query.issue.materialize();
  assert.deepStrictEqual(view.data, [{ id: 1, title: "first", meta: { tags: ["a", "b"] } }]);
});

test("a pushed batch routes to the right query's view", () => {
  const be = new FakeBackend();
  be.snapshot = [addIssue(1, "first", "{}")];
  const store = new Store(schema, be);
  const v1 = store.query.issue.materialize(); // qid 1
  const v2 = store.query.issue.materialize(); // qid 2

  be.emit(1 as QueryId, { type: "batch", events: [addIssue(2, "second", "{}")] });
  assert.strictEqual(v1.data.length, 2, "qid 1 received the batch");
  assert.strictEqual(v2.data.length, 1, "qid 2 is unaffected");
});

test("store.write positionalizes object rows (json stringified) → backend.mutate", async () => {
  const be = new FakeBackend();
  be.snapshot = [];
  const store = new Store(schema, be);
  store.query.issue.materialize();

  await store.write((tx) => {
    tx.add("issue", { id: 5, title: "x", meta: { tags: ["t"] } });
  });
  assert.deepStrictEqual(be.mutations, [
    [{ op: "add", table: "issue", row: [5, "x", '{"tags":["t"]}'] }],
  ]);
});

test("top-level .one() materializes a SingularArrayView (the row, or null) — not an array", () => {
  const be = new FakeBackend();
  be.helloSchema = { ...issueWire, singular: true };

  // Empty result → null.
  be.snapshot = [];
  const empty = new Store(schema, be).query.issue.one().materialize();
  assert.strictEqual(empty.data, null);

  // One row → the unwrapped object; subscribe sees the object, then the edited object.
  be.snapshot = [addIssue(1, "only", "{}")];
  const store = new Store(schema, be);
  const view = store.query.issue.where.id(1).one().materialize(); // fresh store ⇒ qid 1
  assert.deepStrictEqual(view.data, { id: 1, title: "only", meta: {} });

  const seen: unknown[] = [];
  view.subscribe((d) => seen.push(d));
  be.emit(1 as QueryId, {
    type: "batch",
    events: [{ path: [], op: { tag: "edit", old: [1, "only", "{}"], new: [1, "renamed", "{}"] } }],
  });
  assert.deepStrictEqual(seen, [
    { id: 1, title: "only", meta: {} },
    { id: 1, title: "renamed", meta: {} },
  ]);
});

test("subscribers fire as batches arrive", () => {
  const be = new FakeBackend();
  be.snapshot = [addIssue(1, "a", "{}")];
  const store = new Store(schema, be);
  const view = store.query.issue.materialize();
  const lengths: number[] = [];
  view.subscribe((d) => lengths.push(d.length));
  be.emit(1 as QueryId, { type: "batch", events: [addIssue(2, "b", "{}")] });
  assert.deepStrictEqual(lengths, [1, 2]);
});

test("destroy() unregisters the query from the backend", () => {
  const be = new FakeBackend();
  be.snapshot = [addIssue(1, "a", "{}")];
  const store = new Store(schema, be);
  const view = store.query.issue.materialize(); // qid 1
  assert.deepStrictEqual(be.unregistered, []);
  view.destroy();
  assert.deepStrictEqual(be.unregistered, [1 as QueryId], "destroy unregisters qid 1");
});

test("store.materialize(query) registers the local AST plus named remote metadata", () => {
  const be = new FakeBackend();
  const store = new Store(schema, be);
  const qb = newQueryBuilder(schema);
  const issueQueries = {
    byID: defineQuery("byID", (args: { id: number }) => qb.issue.where.id(args.id)),
  };

  const view = store.materialize(issueQueries.byID({ id: 7 }));
  assert.deepStrictEqual(view.data, []);
  assert.deepStrictEqual(be.registered[0], {
    qid: 1 as QueryId,
    ast: { table: "issue", where: { type: "simple", op: "=", left: { type: "column", name: "id" }, right: { type: "literal", value: 7 } } },
    remote: { name: "byID", args: { id: 7 } },
  });
});

test("createCachedQueryView materializes one local AST view and retains remote leases separately", () => {
  const be = new SplitFakeBackend();
  be.snapshot = [addIssue(1, "first", "{}")];
  const store = new Store(schema, be);
  const qb = newQueryBuilder(schema);
  const issueQueries = {
    byID: defineQuery("byID", (args: { id: number }) => qb.issue.where.id(args.id)),
  };

  const handle = store.createCachedQueryView(issueQueries.byID({ id: 1 }));
  assert.deepStrictEqual(handle.view.data, [{ id: 1, title: "first", meta: {} }]);
  assert.equal(be.registered.length, 1, "one local query is registered");
  assert.deepStrictEqual(be.registered[0].remote, undefined, "the local view is not also a remote lease");

  const release1 = handle.retain(issueQueries.byID({ id: 1 }));
  const release2 = handle.retain(issueQueries.byID({ id: 1 }));
  assert.deepStrictEqual(
    be.remoteRetained.map((r) => [r.qid, r.remote, r.localQueryId]),
    [
      [2 as QueryId, { name: "byID", args: { id: 1 } }, 1 as QueryId],
      [3 as QueryId, { name: "byID", args: { id: 1 } }, 1 as QueryId],
    ],
  );

  release1();
  assert.deepStrictEqual(be.remoteReleased, [2 as QueryId]);
  assert.deepStrictEqual(be.unregistered, []);

  release2();
  handle.destroy();
  assert.deepStrictEqual(be.remoteReleased, [2 as QueryId, 3 as QueryId]);
  assert.deepStrictEqual(be.unregistered, [1 as QueryId]);
});

test("retainSyncQuery retains remote coverage without creating a local materialized view", () => {
  const be = new SplitFakeBackend();
  const store = new Store(schema, be);
  const qb = newQueryBuilder(schema);
  const issueQueries = {
    byID: defineQuery("byID", (args: { id: number }) => qb.issue.where.id(args.id)),
  };

  const lease = store.retainSyncQuery(issueQueries.byID({ id: 1 }));
  assert.deepStrictEqual(be.registered, [], "sync-only coverage does not register a local AST view");
  assert.deepStrictEqual(be.remoteRetained, [
    { qid: 1 as QueryId, remote: { name: "byID", args: { id: 1 } }, localQueryId: 1 as QueryId },
  ]);

  lease.release();
  assert.deepStrictEqual(be.remoteReleased, [1 as QueryId]);
  assert.deepStrictEqual(be.unregistered, [], "there is no local view to unregister");
});

test("store.materialize(query) treats unstamped builder queries as local-only", () => {
  const be = new FakeBackend();
  const store = new Store(schema, be);
  const qb = newQueryBuilder(schema);

  store.materialize(qb.issue.where.id(9));
  assert.strictEqual((be.registered[0].ast as Ast).table, "issue");
  assert.strictEqual(be.registered[0].remote, undefined);
});

test("a destroyed query no longer receives events (no zombie that throws / aborts siblings)", () => {
  // Regression: re-materializing (e.g. a changed limit) destroys the old query. If the Store
  // kept routing the engine's still-emitted events to that now-empty view, the next remove/Child
  // would throw and — because dispatch is one loop — also skip delivery to live sibling queries.
  const be = new FakeBackend();
  be.snapshot = [addIssue(1, "a", "{}")];
  const store = new Store(schema, be);
  const v1 = store.query.issue.materialize(); // qid 1 (the one we'll destroy)
  const v2 = store.query.issue.materialize(); // qid 2 (the live one)
  v1.destroy();

  // A remove that the destroyed (now-empty) view could only satisfy by throwing.
  const removeOne: FlatChange = { path: [], op: { tag: "remove", row: [1, "a", "{}"] } };
  assert.doesNotThrow(() => be.emit(1 as QueryId, { type: "batch", events: [removeOne] }),
    "events for the destroyed query are dropped, not applied to an empty view");

  // The live sibling still updates.
  be.emit(2 as QueryId, { type: "batch", events: [addIssue(2, "b", "{}")] });
  assert.strictEqual(v2.data.length, 2, "qid 2 keeps receiving updates");
});

test("a grouped count() + having materializes [group, count] rows; synthetic count comes through as a number", () => {
  const be = new FakeBackend();
  // On hello the engine reports the aggregate's OUTPUT schema — [group…, count] — NOT the base
  // table; `count` is a synthetic column on no table (REDUCE-DESIGN §8).
  be.helloSchema = {
    columns: ["title", "count"],
    primaryKey: [0],
    sort: [[0, true]],
    singular: false,
    relationships: [],
  };
  be.snapshot = [
    { path: [], op: { tag: "add", node: { row: ["bug", 3], rels: [] } } },
    { path: [], op: { tag: "add", node: { row: ["chore", 1], rels: [] } } },
  ];
  const store = new Store(schema, be);

  const view = store.query.issue.groupBy("title").count().having((h) => h.count(gt(0))).materialize();

  // The wire AST carries exactly the aggregate fields the engine lowers.
  const ast = be.registered[0].ast as Ast;
  assert.strictEqual(ast.aggregate, "count");
  assert.deepStrictEqual(ast.groupBy, ["title"]);
  assert.deepStrictEqual(ast.having, {
    type: "simple",
    op: ">",
    left: { type: "column", name: "count" },
    right: { type: "literal", value: 0 },
  });

  assert.deepStrictEqual(view.data, [
    { title: "bug", count: 3 },
    { title: "chore", count: 1 },
  ]);
  // The synthetic `count` is not a base-table column, so `viewTypes` can't find its ColType and
  // falls back to "string" — harmless: the wire value is already a number and passes through.
  assert.strictEqual(typeof (view.data[0] as { count: number }).count, "number");
});

test("a global count() materializes a single { count } row (empty PK/sort singleton)", () => {
  const be = new FakeBackend();
  be.helloSchema = { columns: ["count"], primaryKey: [], sort: [], singular: false, relationships: [] };
  be.snapshot = [{ path: [], op: { tag: "add", node: { row: [7], rels: [] } } }];
  const store = new Store(schema, be);

  const view = store.query.issue.count().materialize();
  assert.strictEqual((be.registered[0].ast as Ast).aggregate, "count");
  assert.deepStrictEqual(view.data, [{ count: 7 }]);
});

test("having(alias, op, n) sends a gate EXISTS over the wire and materializes the surviving parents", () => {
  const comment = table("comment").columns({ id: number(), issueID: number() }).primaryKey("id");
  const gatedSchema = createSchema({ tables: [issue, comment] });
  const be = new FakeBackend();
  // The engine applies the gate server-side; the snapshot is the SURVIVING issues. (The display
  // `commentCount` scalar and the out-of-view gate are exercised by the Rust engine tests — here we
  // prove the TS→engine wire contract and that the filtered parent set flows into the view.)
  be.helloSchema = issueWire;
  be.snapshot = [addIssue(1, "popular", "{}"), addIssue(2, "alsoPopular", "{}")];
  const store = new Store(gatedSchema, be);

  const view = store.query.issue
    .countAs("commentCount", comment, { parent: ["id"], child: ["issueID"] })
    .having("commentCount", ">", 1)
    .materialize();

  // The wire AST carries the untouched display `countAs` PLUS the gate EXISTS — a count over the
  // same correlation, under a hidden alias, with `HAVING count > 1`.
  const ast = be.registered[0].ast as any;
  assert.strictEqual(ast.related[0].subquery.alias, "commentCount");
  assert.strictEqual(ast.related[0].subquery.having, undefined);
  assert.strictEqual(ast.where.type, "correlatedSubquery");
  assert.strictEqual(ast.where.op, "EXISTS");
  assert.strictEqual(ast.where.related.subquery.alias, "__having_commentCount");
  assert.strictEqual(ast.where.related.subquery.aggregate, "count");
  assert.deepStrictEqual(ast.where.related.subquery.having, {
    type: "simple",
    op: ">",
    left: { type: "column", name: "count" },
    right: { type: "literal", value: 1 },
  });

  // The surviving parents materialize.
  assert.deepStrictEqual(
    view.data.map((r) => ({ id: (r as { id: number }).id, title: (r as { title: string }).title })),
    [
      { id: 1, title: "popular" },
      { id: 2, title: "alsoPopular" },
    ],
  );
});

// --- cross-view-atomic notification (Backend.onCommitBoundary) ---------------------

test("a commit folds every view before notifying any subscriber (subscriber re-reads a fresh sibling)", () => {
  const be = new CommitFakeBackend();
  be.snapshot = [addIssue(1, "first", "{}")];
  const store = new Store(schema, be);
  const vA = store.query.issue.materialize(); // qid 1
  const vB = store.query.issue.materialize(); // qid 2

  // vA's subscriber re-reads vB inside its callback — the exact cross-view read in question.
  const seenInA: number[] = [];
  vA.subscribe(() => seenInA.push(vB.data.length));
  seenInA.length = 0; // ignore subscribe's immediate fire

  // ONE commit adds id=2 to both views. vA's batch is listed FIRST: without the barrier vA's
  // subscriber would run before vB folds and observe the stale (length-1) vB.
  be.commit([
    { qid: 1 as QueryId, events: [addIssue(2, "second", "{}")] },
    { qid: 2 as QueryId, events: [addIssue(2, "second", "{}")] },
  ]);

  assert.deepStrictEqual(vA.data.map((r: { id: number }) => r.id), [1, 2]);
  assert.deepStrictEqual(vB.data.map((r: { id: number }) => r.id), [1, 2]);
  assert.deepStrictEqual(seenInA, [2], "vA's subscriber saw vB already folded (fresh), not stale");
});

test("cross-view freshness is independent of the batch order within a commit", () => {
  // Same as above but the sibling read is the OTHER direction and the changed view is delivered
  // LAST — the guarantee must not depend on qid/dispatch order.
  const be = new CommitFakeBackend();
  be.snapshot = [addIssue(1, "first", "{}")];
  const store = new Store(schema, be);
  const vA = store.query.issue.materialize(); // qid 1
  const vB = store.query.issue.materialize(); // qid 2

  const seenInB: number[] = [];
  vB.subscribe(() => seenInB.push(vA.data.length));
  seenInB.length = 0;

  // vB's batch is delivered first; vB's subscriber must still see vA (folded later in the loop) fresh.
  be.commit([
    { qid: 2 as QueryId, events: [addIssue(2, "second", "{}")] },
    { qid: 1 as QueryId, events: [addIssue(2, "second", "{}")] },
  ]);

  assert.deepStrictEqual(seenInB, [2], "vB's subscriber saw vA fresh even though vA folded after vB");
});

test("each view's subscriber fires exactly once per commit; an unchanged view is not notified", () => {
  const be = new CommitFakeBackend();
  be.snapshot = [addIssue(1, "first", "{}")];
  const store = new Store(schema, be);
  const vA = store.query.issue.materialize(); // qid 1
  const vB = store.query.issue.materialize(); // qid 2 — untouched by the commit below

  let aFires = 0;
  let bFires = 0;
  vA.subscribe(() => aFires++);
  vB.subscribe(() => bFires++);
  aFires = 0;
  bFires = 0;

  be.commit([{ qid: 1 as QueryId, events: [addIssue(2, "second", "{}")] }]);

  assert.strictEqual(aFires, 1, "the changed view notifies once");
  assert.strictEqual(bFires, 0, "a view with no batch in the commit is not notified");
});

test("a throwing subscriber does not starve sibling views; the error surfaces after the flush", () => {
  const be = new CommitFakeBackend();
  be.snapshot = [addIssue(1, "first", "{}")];
  const store = new Store(schema, be);
  const vA = store.query.issue.materialize(); // qid 1
  const vB = store.query.issue.materialize(); // qid 2

  let armed = false; // don't throw on subscribe's immediate fire — only on the commit notification
  let bSawFreshA = false;
  vA.subscribe(() => {
    if (armed) throw new Error("boom in A");
  });
  vB.subscribe(() => {
    if (armed) bSawFreshA = vA.data.length === 2; // A folded before B is notified, despite A throwing
  });
  armed = true;

  assert.throws(
    () =>
      be.commit([
        { qid: 1 as QueryId, events: [addIssue(2, "second", "{}")] },
        { qid: 2 as QueryId, events: [addIssue(2, "second", "{}")] },
      ]),
    /boom in A/,
    "the first subscriber error is re-raised",
  );
  assert.ok(bSawFreshA, "vB was still notified (with a fully-folded vA) after vA's subscriber threw");
});

test("subscribeChanges is coalesced too: a change listener re-reads a fresh sibling view", () => {
  const be = new CommitFakeBackend();
  be.snapshot = [addIssue(1, "first", "{}")];
  const store = new Store(schema, be);
  const vA = store.query.issue.materialize(); // qid 1
  const vB = store.query.issue.materialize(); // qid 2

  // A change listener for qid 1 re-reads vB inside its callback — it must see vB already folded,
  // even though qid 1's frame is delivered first. Also record the frame order.
  const bLenWhenAChanged: number[] = [];
  const order: Array<{ qid: number; type: string }> = [];
  store.subscribeChanges((qid, ev) => {
    order.push({ qid: qid as number, type: ev.type });
    if (qid === 1) bLenWhenAChanged.push(vB.data.length);
  });

  be.commit([
    { qid: 1 as QueryId, events: [addIssue(2, "second", "{}")] },
    { qid: 2 as QueryId, events: [addIssue(2, "second", "{}")] },
  ]);

  assert.deepStrictEqual(bLenWhenAChanged, [2], "change listener for A saw B already folded (fresh)");
  // Each affected query's frame delivered once, in commit (arrival) order.
  assert.deepStrictEqual(order, [
    { qid: 1, type: "batch" },
    { qid: 2, type: "batch" },
  ]);
});

test("view subscribers run before the change stream at a commit boundary", () => {
  const be = new CommitFakeBackend();
  be.snapshot = [addIssue(1, "first", "{}")];
  const store = new Store(schema, be);
  const v = store.query.issue.materialize(); // qid 1

  const log: string[] = [];
  v.subscribe(() => log.push("subscriber"));
  store.subscribeChanges(() => log.push("change"));
  log.length = 0; // ignore subscribe's immediate fire

  be.commit([{ qid: 1 as QueryId, events: [addIssue(2, "second", "{}")] }]);

  assert.deepStrictEqual(log, ["subscriber", "change"], "subscribers flush before change frames");
});

test("without a commit boundary, a backend still notifies per event (unchanged behavior)", () => {
  // The plain FakeBackend never calls onCommitBoundary, so the Store stays in inline-notify mode.
  const be = new FakeBackend();
  be.snapshot = [addIssue(1, "first", "{}")];
  const store = new Store(schema, be);
  const v = store.query.issue.materialize();
  const seen: number[] = [];
  v.subscribe((d) => seen.push(d.length));
  be.emit(1 as QueryId, { type: "batch", events: [addIssue(2, "second", "{}")] });
  assert.deepStrictEqual(seen, [1, 2], "fires immediately on subscribe, then inline after the batch");
});

// ── store.readOnce: one-shot AUTHORITATIVE read ───────────────────────────────────────────────────

/** A backend WITH a server lifecycle: it registers a query as PENDING (`unknown`) with a pre-sync
 *  (empty) first snapshot — the shape the optimistic/wasm engine reports for a query it must still
 *  sync — and only reports the authoritative rows + `complete` later, via `complete()`. */
class LifecycleFakeBackend extends FakeBackend {
  private rt: (qid: QueryId, rt: ResultType) => void = () => {};
  onResultType(h: (qid: QueryId, rt: ResultType) => void): void {
    this.rt = h;
  }
  registerQuery(qid: QueryId, ast: unknown, remote?: RemoteQuery): void {
    this.registered.push({ qid, ast, remote });
    this.emit(qid, { type: "hello", schema: this.helloSchema, comparatorVersion: 1 });
    this.rt(qid, "unknown");
    this.emit(qid, { type: "snapshot", adds: [], last: true });
  }
  /** Deliver the authoritative snapshot, THEN flip the query to `complete`. */
  complete(qid: QueryId, adds: FlatChange[]): void {
    this.emit(qid, { type: "snapshot", adds, last: true });
    this.rt(qid, "complete");
  }
}

test("readOnce resolves with the current result once, then destroys the view", async () => {
  const be = new FakeBackend();
  be.snapshot = [addIssue(1, "first", '{"tags":["x"]}'), addIssue(2, "second", '{"tags":[]}')];
  const store = new Store(schema, be);

  const data = await store.readOnce(store.query.issue);

  assert.deepStrictEqual(data, [
    { id: 1, title: "first", meta: { tags: ["x"] } },
    { id: 2, title: "second", meta: { tags: [] } },
  ]);
  assert.deepStrictEqual(be.unregistered, [1 as QueryId], "the one-shot view is torn down (not left subscribed)");
});

test("readOnce on a top-level .one() returns the singular row", async () => {
  const be = new FakeBackend();
  be.snapshot = [addIssue(1, "only", '{"tags":[]}')];
  const store = new Store(schema, be);

  const row = await store.readOnce(store.query.issue.where.id(1).one());

  assert.deepStrictEqual(row, { id: 1, title: "only", meta: { tags: [] } });
});

test("readOnce waits for the query to become authoritative (resultType 'complete') before reading", async () => {
  const be = new LifecycleFakeBackend();
  const store = new Store(schema, be);

  let settled = false;
  const p = store.readOnce(store.query.issue).then((d) => {
    settled = true;
    return d;
  });

  // The pre-sync (empty) snapshot has landed but the query is still `unknown` — must NOT resolve yet.
  await Promise.resolve();
  assert.equal(settled, false, "does not resolve on the pre-sync snapshot");

  be.complete(1 as QueryId, [addIssue(1, "synced", '{"tags":["ok"]}')]);
  const data = await p;

  assert.deepStrictEqual(data, [{ id: 1, title: "synced", meta: { tags: ["ok"] } }]);
  assert.deepStrictEqual(be.unregistered, [1 as QueryId]);
});
