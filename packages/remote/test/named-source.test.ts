import { test } from "node:test";
import assert from "node:assert/strict";

import { COMPARATOR_VERSION } from "@rindle/client";

import type { ServerMsg } from "../src/protocol.ts";
import { RemoteBackend } from "../src/backend.ts";
import { normalizedFp } from "../src/normalized.ts";
import { RemoteOptimisticSource } from "../src/optimistic-source.ts";
import { RemoteNormalizedSource } from "../src/remote-source.ts";
import type { ClientMsg } from "../src/protocol.ts";
import type { Transport } from "../src/transport.ts";
import { createSchema, defineQuery, newQueryBuilder, number, Store, string, table } from "@rindle/client";

const tables = [{ name: "issue", columns: ["id", "title"], primaryKey: [0] }];
const fp = normalizedFp(tables);
const issue = table("issue").columns({ id: number(), title: string() }).primaryKey("id");
const schema = createSchema({ tables: [issue] });

class TestTransport implements Transport {
  sent: ClientMsg[] = [];
  private handler: (msg: ServerMsg) => void = () => {};
  private reconnectHandler: () => void = () => {};

  send(msg: ClientMsg): void {
    this.sent.push(msg);
  }

  onMessage(handler: (msg: ServerMsg) => void): void {
    this.handler = handler;
  }

  onReconnect(handler: () => void): void {
    this.reconnectHandler = handler;
  }

  emit(msg: ServerMsg): void {
    this.handler(msg);
  }

  /** Simulate a transport reconnect (drives the source's resync). */
  reconnect(): void {
    this.reconnectHandler();
  }

  close(): void {}
}

test("RemoteNormalizedSource subscribes and gap-recovers with name/args, never AST", () => {
  const transport = new TestTransport();
  const source = new RemoteNormalizedSource(transport);
  source.expectClientSchema(tables);
  source.onNormalized(() => {});

  source.registerQuery(7, { name: "issuesForProject", args: { projectID: 42 } });
  assert.deepStrictEqual(transport.sent[0], {
    t: "subscribe",
    queryId: 7,
    name: "issuesForProject",
    args: { projectID: 42 },
    mode: "normalized",
  });
  assert.equal("ast" in transport.sent[0], false);

  transport.emit({
    t: "nhello",
    queryId: 7,
    hello: { epoch: 1, comparatorVersion: COMPARATOR_VERSION, tables, normalizedFp: fp },
  });
  // Deliver an incremental frame before the seq-0 snapshot. The source detects the gap and
  // re-subscribes using the stored named identity.
  transport.emit({
    t: "nbatch",
    queryId: 7,
    batch: { epoch: 1, seq: 1, normalizedFp: fp, ops: [] },
  });

  assert.deepStrictEqual(transport.sent[1], transport.sent[0]);
});

test("RemoteNormalizedSource can subscribe through API-issued leases", () => {
  const transport = new TestTransport();
  const source = new RemoteNormalizedSource(transport, {
    resolveSubscribe: ({ queryId, remote, mode }) => {
      assert.equal(queryId, 7);
      assert.equal(mode, "normalized");
      assert.deepEqual(remote, { name: "issuesForProject", args: { projectID: 42 } });
      return { leaseToken: "lease-abc" };
    },
  });
  source.expectClientSchema(tables);
  source.onNormalized(() => {});

  source.registerQuery(7, { name: "issuesForProject", args: { projectID: 42 } });

  assert.deepStrictEqual(transport.sent[0], {
    t: "subscribe",
    queryId: 7,
    leaseToken: "lease-abc",
    mode: "normalized",
  });
  assert.equal("name" in transport.sent[0], false);
  assert.equal("args" in transport.sent[0], false);
});

test("RemoteNormalizedSource ignores stale async lease resolution after unsubscribe", async () => {
  const transport = new TestTransport();
  let resolveLease!: (target: { leaseToken: string }) => void;
  const source = new RemoteNormalizedSource(transport, {
    resolveSubscribe: () =>
      new Promise((resolve) => {
        resolveLease = resolve;
      }),
  });
  source.onNormalized(() => {});

  source.registerQuery(7, { name: "issuesForProject", args: { projectID: 42 } });
  source.unregisterQuery(7);
  resolveLease({ leaseToken: "late-lease" });
  await Promise.resolve();

  assert.deepStrictEqual(transport.sent, [{ t: "unsubscribe", queryId: 7 }]);
});

test("RemoteBackend subscribes with the named query identity, never AST", () => {
  const transport = new TestTransport();
  const store = new Store(schema, new RemoteBackend(transport));
  const q = newQueryBuilder(schema);
  const queries = {
    issueByID: defineQuery("issueByID", (args: { id: number }) => q.issue.where.id(args.id)),
  };

  store.materialize(queries.issueByID({ id: 9 }));

  assert.deepStrictEqual(transport.sent[0], {
    t: "subscribe",
    queryId: 1,
    name: "issueByID",
    args: { id: 9 },
  });
  assert.equal("ast" in transport.sent[0], false);
});

test("RemoteBackend can use a lease resolver and API-routed raw mutation sender", async () => {
  const transport = new TestTransport();
  const sentMutations: unknown[] = [];
  const store = new Store(
    schema,
    new RemoteBackend(transport, {
      resolveSubscribe: ({ remote }) => ({ leaseToken: `lease:${remote.name}` }),
      sendMutation: (mutations) => {
        sentMutations.push(mutations);
      },
    }),
  );
  const q = newQueryBuilder(schema);
  const queries = {
    issueByID: defineQuery("issueByID", (args: { id: number }) => q.issue.where.id(args.id)),
  };

  store.materialize(queries.issueByID({ id: 9 }));
  await store.write((tx) => tx.add("issue", { id: 9, title: "from api" }));

  assert.deepStrictEqual(transport.sent[0], { t: "subscribe", queryId: 1, leaseToken: "lease:issueByID" });
  assert.deepStrictEqual(sentMutations, [[{ op: "add", table: "issue", row: [9, "from api"] }]]);
});

test("RemoteOptimisticSource can route mutator envelopes through the API server", async () => {
  const transport = new TestTransport();
  const envelopes: unknown[] = [];
  const source = new RemoteOptimisticSource(transport, "client-1", {
    resolveSubscribe: () => ({ leaseToken: "lease-optimistic" }),
    pushMutation: (envelope) => {
      envelopes.push(envelope);
    },
  });
  source.onNormalized(() => {});
  source.onProgress(() => {});

  source.registerQuery(3, { name: "allIssues", args: null });
  await source.pushMutation({ clientID: "client-1", mid: 1, name: "createIssue", args: { id: 1 } });

  assert.deepStrictEqual(transport.sent, [
    { t: "init", clientID: "client-1" },
    { t: "subscribe", queryId: 3, leaseToken: "lease-optimistic", mode: "normalized" },
  ]);
  assert.deepStrictEqual(envelopes, [{ clientID: "client-1", mid: 1, name: "createIssue", args: { id: 1 } }]);
});

test("RemoteOptimisticSource fires onRestart only when the daemon boot id changes", () => {
  const transport = new TestTransport();
  const source = new RemoteOptimisticSource(transport, "client-1", {
    resolveSubscribe: () => ({ leaseToken: "lease" }),
  });
  source.expectClientSchema(tables);
  source.onNormalized(() => {});
  source.onProgress(() => {});
  let restarts = 0;
  source.onRestart(() => {
    restarts++;
  });

  source.registerQuery(3, { name: "allIssues", args: null });
  const hello = (bootId?: string): ServerMsg => ({
    t: "nhello",
    queryId: 3,
    hello: { epoch: 1, comparatorVersion: COMPARATOR_VERSION, tables, normalizedFp: fp },
    bootId,
  });

  transport.emit(hello("boot-a")); // first observation — not a restart
  transport.emit(hello("boot-a")); // unchanged
  assert.equal(restarts, 0, "a stable daemon never looks restarted");
  transport.emit(hello("boot-b")); // changed → the daemon restarted
  assert.equal(restarts, 1);
});

test("RemoteOptimisticSource re-inits and re-subscribes (re-leasing) on reconnect", () => {
  const transport = new TestTransport();
  let leaseN = 0;
  const source = new RemoteOptimisticSource(transport, "client-1", {
    resolveSubscribe: () => ({ leaseToken: `lease-${++leaseN}` }),
  });
  source.onNormalized(() => {});
  source.onProgress(() => {});

  source.registerQuery(3, { name: "allIssues", args: null });
  assert.deepStrictEqual(transport.sent, [
    { t: "init", clientID: "client-1" },
    { t: "subscribe", queryId: 3, leaseToken: "lease-1", mode: "normalized" },
  ]);

  transport.sent.length = 0;
  transport.reconnect();

  // A fresh init + a re-subscribe with a FRESH lease (resolveSubscribe ran again).
  assert.deepStrictEqual(transport.sent, [
    { t: "init", clientID: "client-1" },
    { t: "subscribe", queryId: 3, leaseToken: "lease-2", mode: "normalized" },
  ]);
});
