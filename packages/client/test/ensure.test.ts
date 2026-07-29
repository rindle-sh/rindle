import { test } from "node:test";
import assert from "node:assert/strict";

import { QueryEnsureCache } from "../src/ensure.ts";
import { defineQuery, newQueryBuilder } from "../src/query.ts";
import { createSchema, number, string, table } from "../src/schema.ts";
import { Store } from "../src/store.ts";
import type {
  Backend,
  ChangeEvent,
  FlatChange,
  Mutation,
  QueryId,
  RemoteQuery,
  ResultType,
  WireSchema,
} from "../src/types.ts";

const issue = table("issue").columns({ id: number(), title: string() }).primaryKey("id");
const schema = createSchema({ tables: [issue] });
const qb = newQueryBuilder(schema);
const issueQueries = {
  byID: defineQuery("issueByID", (args: { id: number }) => qb.issue.where.id(args.id).one()),
};

const wireSchema: WireSchema = {
  columns: ["id", "title"],
  primaryKey: [0],
  sort: [[0, true]],
  singular: true,
  relationships: [],
};

const addIssue = (id: number, title: string): FlatChange => ({
  path: [],
  op: { tag: "add", node: { row: [id, title], rels: [] } },
});

class EnsureBackend implements Backend {
  private eventHandler: (qid: QueryId, event: ChangeEvent) => void = () => {};
  private resultTypeHandler: (qid: QueryId, resultType: ResultType) => void = () => {};
  readonly registered: Array<{ qid: QueryId; remote?: RemoteQuery }> = [];
  readonly unregistered: QueryId[] = [];
  initial: FlatChange[] = [];

  registerQuery(qid: QueryId, _ast: unknown, remote?: RemoteQuery): void {
    this.registered.push({ qid, remote });
    this.eventHandler(qid, { type: "hello", schema: wireSchema, comparatorVersion: 1 });
    this.resultTypeHandler(qid, "unknown");
    this.eventHandler(qid, { type: "snapshot", adds: this.initial, last: true });
  }

  unregisterQuery(qid: QueryId): void {
    this.unregistered.push(qid);
  }

  mutate(_mutations: Mutation[]): Promise<void> {
    return Promise.resolve();
  }

  onEvent(handler: (qid: QueryId, event: ChangeEvent) => void): void {
    this.eventHandler = handler;
  }

  onResultType(handler: (qid: QueryId, resultType: ResultType) => void): void {
    this.resultTypeHandler = handler;
  }

  add(qid: QueryId, change: FlatChange): void {
    this.eventHandler(qid, { type: "batch", events: [change] });
  }

  resultType(qid: QueryId, resultType: ResultType): void {
    this.resultTypeHandler(qid, resultType);
  }
}

const nextMicrotask = () => Promise.resolve();
const nextTimer = () => new Promise<void>((resolve) => setTimeout(resolve, 0));

test("ensure defaults to server-authoritative completion even when a local result is present", async () => {
  const backend = new EnsureBackend();
  backend.initial = [addIssue(1, "local")];
  const cache = new QueryEnsureCache(new Store(schema, backend), { releaseDelayMs: 0 });

  let settled = false;
  const ready = cache.ensure(issueQueries.byID({ id: 1 })).then(() => {
    settled = true;
  });
  await nextMicrotask();
  assert.equal(settled, false);

  backend.resultType(1 as QueryId, "complete");
  await ready;
  await nextTimer();
  assert.deepStrictEqual(backend.unregistered, [1 as QueryId]);
});

test("until present resolves from a warm local result and keeps revalidating", async () => {
  const backend = new EnsureBackend();
  backend.initial = [addIssue(1, "local")];
  const cache = new QueryEnsureCache(new Store(schema, backend), { releaseDelayMs: 0 });

  await cache.ensure(issueQueries.byID({ id: 1 }), { until: "present" });
  assert.deepStrictEqual(backend.unregistered, [], "the retain survives while authority is unknown");

  backend.resultType(1 as QueryId, "complete");
  await nextTimer();
  assert.deepStrictEqual(backend.unregistered, [1 as QueryId], "completion starts the handoff release");
});

test("until present waits on a cold query, then resolves when a local row arrives", async () => {
  const backend = new EnsureBackend();
  const cache = new QueryEnsureCache(new Store(schema, backend), { releaseDelayMs: 0 });

  let settled = false;
  const ready = cache.ensure(issueQueries.byID({ id: 1 }), { until: "present" }).then(() => {
    settled = true;
  });
  await nextMicrotask();
  assert.equal(settled, false);

  backend.add(1 as QueryId, addIssue(1, "arrived locally"));
  await ready;
  assert.deepStrictEqual(backend.unregistered, [], "early resolution does not cancel revalidation");

  backend.resultType(1 as QueryId, "complete");
  await nextTimer();
  assert.deepStrictEqual(backend.unregistered, [1 as QueryId]);
});

test("until present resolves on authoritative empty, and concurrent policies share one query", async () => {
  const backend = new EnsureBackend();
  const cache = new QueryEnsureCache(new Store(schema, backend), { releaseDelayMs: 0 });
  const query = issueQueries.byID({ id: 404 });

  const present = cache.ensure(query, { until: "present" });
  const complete = cache.ensure(query);
  assert.equal(backend.registered.length, 1, "one materialization is shared by both waits");

  backend.resultType(1 as QueryId, "complete");
  await Promise.all([present, complete]);
  await nextTimer();
  assert.deepStrictEqual(backend.unregistered, [1 as QueryId]);
});

test("close rejects pending waits and releases their views", async () => {
  const backend = new EnsureBackend();
  const cache = new QueryEnsureCache(new Store(schema, backend));
  const ready = cache.ensure(issueQueries.byID({ id: 1 }));

  cache.close();

  await assert.rejects(ready, /closed before the query became ready/);
  assert.deepStrictEqual(backend.unregistered, [1 as QueryId]);
});

test("an aborted ensure rejects only its waiter and keeps the shared query retained", async () => {
  const backend = new EnsureBackend();
  const cache = new QueryEnsureCache(new Store(schema, backend), { releaseDelayMs: 0 });
  const controller = new AbortController();
  const ready = cache.ensure(issueQueries.byID({ id: 1 }), { signal: controller.signal });

  controller.abort();

  await assert.rejects(ready, (error: Error) => error.name === "AbortError");
  assert.deepStrictEqual(backend.unregistered, [], "the canceled wait leaves its useful prefetch running");

  backend.resultType(1 as QueryId, "complete");
  await nextTimer();
  assert.deepStrictEqual(backend.unregistered, [1 as QueryId]);
});
