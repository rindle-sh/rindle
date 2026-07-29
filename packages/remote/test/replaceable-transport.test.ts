import { test } from "node:test";
import assert from "node:assert/strict";

import { LMID_QUERY_NAME } from "@rindle/client";

import type { AffinityTicketStore } from "../src/affinity.ts";
import { RemoteOptimisticSource } from "../src/optimistic-source.ts";
import type { TransportFactory } from "../src/optimistic-source.ts";
import type { ClientMsg, ServerMsg } from "../src/protocol.ts";
import type { Transport } from "../src/transport.ts";

/** A controllable transport: records frames and exposes hooks to drive reconnect / down. */
class FakeTransport implements Transport {
  readonly endpoint: string;
  sent: ClientMsg[] = [];
  closed = false;
  private msgHandler: (m: ServerMsg) => void = () => {};
  private reconnectHandler: () => void = () => {};
  private downHandler: () => void = () => {};
  constructor(endpoint: string) {
    this.endpoint = endpoint;
  }
  send(m: ClientMsg): void {
    this.sent.push(m);
  }
  onMessage(h: (m: ServerMsg) => void): void {
    this.msgHandler = h;
  }
  onReconnect(h: () => void): void {
    this.reconnectHandler = h;
  }
  onDown(h: () => void): void {
    this.downHandler = h;
  }
  close(): void {
    this.closed = true;
  }
  emit(m: ServerMsg): void {
    this.msgHandler(m);
  }
  reconnect(): void {
    this.reconnectHandler();
  }
  down(): void {
    this.downHandler();
  }
}

function recordingFactory() {
  const created: FakeTransport[] = [];
  const factory: TransportFactory = (endpoint) => {
    const t = new FakeTransport(endpoint);
    created.push(t);
    return t;
  };
  const at = (endpoint: string) => created.find((t) => t.endpoint === endpoint);
  return { created, factory, at };
}

const wire = (source: RemoteOptimisticSource) => {
  source.onNormalized(() => {});
  source.onProgress(() => {});
};

test("pure-lazy: the first lease's wsEndpoint opens the transport, then subscribes there", () => {
  const { created } = recordingFactory();
  const f = recordingFactory();
  const source = new RemoteOptimisticSource({ factory: f.factory }, "c1", {
    resolveSubscribe: () => ({ leaseToken: "L1", wsEndpoint: "wssA" }),
  });
  wire(source);
  void created; // unused — keep types honest
  assert.equal(f.created.length, 0, "no transport before the first lease");

  source.registerQuery(1, { name: "allIssues", args: null });

  assert.equal(f.created.length, 1);
  assert.equal(f.created[0].endpoint, "wssA");
  assert.deepStrictEqual(f.created[0].sent, [
    { t: "init", clientID: "c1" },
    { t: "subscribe", queryId: 1, leaseToken: "L1", mode: "normalized" },
  ]);
  // wsEndpoint is client-side routing only — it must NOT leak onto the wire subscribe frame.
  assert.equal("wsEndpoint" in f.created[0].sent[1], false);
});

test("pure-lazy affinity can activate from the lease before its transport opens", () => {
  const f = recordingFactory();
  const tickets: string[] = [];
  let pending = 0;
  let cleared = 0;
  let enabled = false;
  const store: AffinityTicketStore = {
    get: () => tickets.at(-1),
    set: (ticket) => tickets.push(ticket),
    clear: () => {
      cleared += 1;
    },
    connectionPending: () => {
      pending += 1;
    },
    waitForTicket: async () => tickets.at(-1) ?? "",
    leaseTicket: async () => ({ ticket: tickets.at(-1), timedOut: false }),
  };
  const source = new RemoteOptimisticSource({ factory: f.factory }, "c1", {
    affinity: () => (enabled ? store : undefined),
    resolveSubscribe: () => {
      // Mirrors createRindleClient: its first HTTP lease records the returned placement ticket and
      // flips affinity on before handing the lease endpoint back to this source.
      store.set("aff.from-lease");
      enabled = true;
      return { leaseToken: "L1", wsEndpoint: "wssA" };
    },
  });
  wire(source);

  source.registerQuery(1, { name: "allIssues", args: null });
  assert.equal(pending, 1, "bringUp re-resolves the now-active store for the new connection");
  f.at("wssA")!.emit({ t: "affinity", ticket: "aff.from-ws" });
  assert.deepEqual(tickets, ["aff.from-lease", "aff.from-ws"]);
  f.at("wssA")!.down();
  assert.equal(cleared, 1, "the dynamically activated store participates in outage recovery");
});

test("the lmid system query defers until the first app lease opens a transport", () => {
  const f = recordingFactory();
  const source = new RemoteOptimisticSource({ factory: f.factory }, "c1", {
    resolveSubscribe: () => ({ leaseToken: "L1", wsEndpoint: "wssA" }),
  });
  wire(source);

  // The backend registers its endpoint-less lmid system query at construction, before any lease.
  source.registerQuery(0, { name: LMID_QUERY_NAME, args: {} });
  assert.equal(f.created.length, 0, "lmid alone cannot open a transport");

  source.registerQuery(1, { name: "allIssues", args: null });
  assert.deepStrictEqual(f.created[0].sent, [
    { t: "init", clientID: "c1" },
    { t: "subscribe", queryId: 0, name: LMID_QUERY_NAME, args: {}, mode: "normalized" },
    { t: "subscribe", queryId: 1, leaseToken: "L1", mode: "normalized" },
  ]);
});

test("same endpoint: a second lease multiplexes over the one transport", () => {
  const f = recordingFactory();
  const source = new RemoteOptimisticSource({ factory: f.factory }, "c1", {
    resolveSubscribe: () => ({ leaseToken: "L", wsEndpoint: "wssA" }),
  });
  wire(source);
  source.registerQuery(1, { name: "a", args: null });
  source.registerQuery(2, { name: "b", args: null });
  assert.equal(f.created.length, 1, "both queries share one transport");
  assert.deepStrictEqual(f.created[0].sent, [
    { t: "init", clientID: "c1" },
    { t: "subscribe", queryId: 1, leaseToken: "L", mode: "normalized" },
    { t: "subscribe", queryId: 2, leaseToken: "L", mode: "normalized" },
  ]);
});

test("different endpoint: a lease on a new follower migrates the whole session (re-lease all)", () => {
  const f = recordingFactory();
  let endpoint = "wssA";
  let n = 0;
  const source = new RemoteOptimisticSource({ factory: f.factory }, "c1", {
    resolveSubscribe: () => ({ leaseToken: `L${++n}`, wsEndpoint: endpoint }),
  });
  wire(source);

  source.registerQuery(1, { name: "a", args: null }); // opens wssA with L1
  endpoint = "wssB";
  source.registerQuery(2, { name: "b", args: null }); // routes to wssB → migrate

  assert.ok(f.at("wssA")!.closed, "old transport torn down");
  assert.ok(!f.at("wssB")!.closed);
  // wssB carries a fresh init + a re-subscribe of EVERY active query with FRESH leases (the old
  // tokens were follower-local and invalid on the new node).
  assert.deepStrictEqual(f.at("wssB")!.sent, [
    { t: "init", clientID: "c1" },
    { t: "subscribe", queryId: 1, leaseToken: "L3", mode: "normalized" },
    { t: "subscribe", queryId: 2, leaseToken: "L4", mode: "normalized" },
  ]);
});

test("a fixed transport ignores wsEndpoint (single-daemon / tests)", () => {
  const f = recordingFactory();
  const fixed = new FakeTransport("static");
  const source = new RemoteOptimisticSource({ transport: fixed }, "c1", {
    resolveSubscribe: () => ({ leaseToken: "L1", wsEndpoint: "wssZ" }),
  });
  wire(source);
  source.registerQuery(1, { name: "a", args: null });
  assert.equal(f.created.length, 0, "no factory transport built");
  assert.ok(!fixed.closed);
  assert.deepStrictEqual(fixed.sent, [
    { t: "init", clientID: "c1" },
    { t: "subscribe", queryId: 1, leaseToken: "L1", mode: "normalized" },
  ]);
});

test("hard-failure (onDown) re-leases; a moved key migrates off the dead follower", () => {
  const f = recordingFactory();
  let endpoint = "wssA";
  let n = 0;
  const source = new RemoteOptimisticSource({ factory: f.factory }, "c1", {
    resolveSubscribe: () => ({ leaseToken: `L${++n}`, wsEndpoint: endpoint }),
  });
  wire(source);
  source.registerQuery(1, { name: "a", args: null }); // wssA

  endpoint = "wssB"; // the router will now place this key elsewhere (wssA died / left live())
  f.at("wssA")!.down(); // sustained outage

  assert.ok(f.at("wssB"), "re-lease migrated onto a new follower");
  assert.ok(f.at("wssA")!.closed, "dead transport torn down");
  assert.deepStrictEqual(f.at("wssB")!.sent, [
    { t: "init", clientID: "c1" },
    { t: "subscribe", queryId: 1, leaseToken: "L3", mode: "normalized" },
  ]);
});

test("close() tears down the current transport", () => {
  const f = recordingFactory();
  const source = new RemoteOptimisticSource({ factory: f.factory }, "c1", {
    resolveSubscribe: () => ({ leaseToken: "L1", wsEndpoint: "wssA" }),
  });
  wire(source);
  source.registerQuery(1, { name: "a", args: null });
  source.close();
  assert.ok(f.at("wssA")!.closed);
});

test("onDown migration re-subscribes each active query EXACTLY once (no duplicate lease)", () => {
  // Regression: a migrate fired from inside resubscribeAll (onDown to a new endpoint) must not
  // re-subscribe an already-handled query a second time (the re-entrancy generation guard).
  const f = recordingFactory();
  let endpoint = "wssA";
  let n = 0;
  const source = new RemoteOptimisticSource({ factory: f.factory }, "c1", {
    resolveSubscribe: () => ({ leaseToken: `L${++n}`, wsEndpoint: endpoint }),
  });
  wire(source);
  source.registerQuery(1, { name: "a", args: null });
  source.registerQuery(2, { name: "b", args: null }); // both on wssA
  endpoint = "wssB";
  f.at("wssA")!.down(); // onDown → re-lease → migrate the whole session to wssB

  const subs = f.at("wssB")!.sent.filter((m) => m.t === "subscribe");
  assert.equal(subs.length, 2, "each query re-subscribed once, not twice");
  assert.deepStrictEqual(
    subs.map((m) => m.queryId).sort((a, b) => a - b),
    [1, 2],
  );
  assert.equal(new Set(subs.map((m) => (m as { leaseToken: string }).leaseToken)).size, 2, "no duplicate lease");
});

test("async-resolver migration re-leases each active query exactly once (production concurrency)", async () => {
  // The production resolveSubscribe is async (an HTTP POST). Under a migrate, every query's lease
  // re-resolves concurrently; the subscribeTicket guard must drop the superseded first-pass sends so
  // each query lands on the new endpoint exactly once.
  const f = recordingFactory();
  let endpoint = "wssA";
  let n = 0;
  const source = new RemoteOptimisticSource({ factory: f.factory }, "c1", {
    resolveSubscribe: async () => ({ leaseToken: `L${++n}`, wsEndpoint: endpoint }),
  });
  wire(source);
  const settle = async () => {
    for (let i = 0; i < 40; i++) await Promise.resolve();
  };
  source.registerQuery(1, { name: "a", args: null });
  source.registerQuery(2, { name: "b", args: null });
  await settle(); // both subscribed on wssA

  endpoint = "wssB";
  f.at("wssA")!.down(); // onDown → async re-lease → migrate to wssB
  await settle();

  const subs = f.at("wssB")!.sent.filter((m) => m.t === "subscribe");
  assert.equal(subs.length, 2, "each query re-subscribed exactly once (stale first-pass sends dropped)");
  assert.deepStrictEqual(
    subs.map((m) => m.queryId).sort((a, b) => a - b),
    [1, 2],
  );
  assert.ok(f.at("wssA")!.closed, "old transport torn down");
});

test("an UNROUTED daemon's onDown is a no-op (no extra lease POST, no new transport)", () => {
  const f = recordingFactory();
  let n = 0;
  const source = new RemoteOptimisticSource({ factory: f.factory, endpoint: "wssA" }, "c1", {
    resolveSubscribe: () => ({ leaseToken: `L${++n}` }), // single daemon: leases carry NO wsEndpoint
  });
  wire(source);
  source.registerQuery(1, { name: "a", args: null });
  const a = f.at("wssA")!;
  const leasesBefore = n;
  const sentBefore = a.sent.length;

  a.down();

  assert.equal(n, leasesBefore, "onDown did not re-lease for an unrouted daemon");
  assert.equal(a.sent.length, sentBefore, "no extra frames sent");
  assert.equal(f.created.length, 1, "no new transport built");
});

test("same-endpoint onDown (a follower reboot) re-subscribes over the SAME live transport", () => {
  const f = recordingFactory();
  let n = 0;
  const source = new RemoteOptimisticSource({ factory: f.factory, endpoint: "wssA" }, "c1", {
    resolveSubscribe: () => ({ leaseToken: `L${++n}`, wsEndpoint: "wssA" }), // router returns SAME endpoint
  });
  wire(source);
  source.registerQuery(1, { name: "a", args: null });
  const a = f.at("wssA")!;
  const subsBefore = a.sent.filter((m) => m.t === "subscribe").length;

  a.down();

  assert.ok(!a.closed, "the live transport is reused, not torn down");
  assert.equal(f.created.length, 1, "no migration / new transport");
  assert.equal(
    a.sent.filter((m) => m.t === "subscribe").length,
    subsBefore + 1,
    "the query re-leased over the same transport",
  );
});

test("close() makes an in-flight async lease inert (no transport opened after teardown)", async () => {
  const f = recordingFactory();
  let resolveLease!: (t: { leaseToken: string; wsEndpoint?: string }) => void;
  const source = new RemoteOptimisticSource({ factory: f.factory, endpoint: "wssA" }, "c1", {
    resolveSubscribe: () => new Promise((r) => (resolveLease = r)),
  });
  wire(source);
  source.registerQuery(1, { name: "a", args: null }); // lease pending

  source.close();
  assert.ok(f.at("wssA")!.closed, "current transport closed");

  resolveLease({ leaseToken: "L1", wsEndpoint: "wssB" }); // resolves AFTER close — must be inert
  await Promise.resolve();
  await Promise.resolve();

  assert.equal(f.at("wssB"), undefined, "no new transport opened after close");
});

test("pure-lazy: an app lease without a wsEndpoint warns once — the deferral is otherwise silent", () => {
  const f = recordingFactory();
  const warnings: string[] = [];
  const originalWarn = console.warn;
  console.warn = (message?: unknown) => warnings.push(String(message));
  try {
    const source = new RemoteOptimisticSource({ factory: f.factory }, "c1", {
      // An api-server with an explicit daemon and no rindle.wsUrl: leases carry no endpoint.
      resolveSubscribe: () => ({ leaseToken: "L1" }),
    });
    wire(source);
    source.registerQuery(1, { name: "allIssues", args: null });
    source.registerQuery(2, { name: "otherIssues", args: null });
    assert.equal(f.created.length, 0, "nothing can open a transport");
    assert.equal(warnings.length, 1, "the dead-subscription state is announced exactly once");
    assert.match(warnings[0]!, /wsEndpoint/);
  } finally {
    console.warn = originalWarn;
  }
});
