import { mock, test } from "node:test";
import assert from "node:assert/strict";

import { WS_SUBPROTOCOL, createAffinityTicketStore, offerSubprotocols } from "../src/affinity.ts";
import { RemoteOptimisticSource } from "../src/optimistic-source.ts";
import type { ClientMsg, ServerMsg } from "../src/protocol.ts";
import type { Transport } from "../src/transport.ts";

// --- the ticket store -------------------------------------------------------------------------

test("store: set/get/clear round-trips and persists through the adapter", () => {
  const saved: string[] = [];
  let cleared = 0;
  let backing: string | undefined;
  const store = createAffinityTicketStore({
    load: () => backing,
    save: (t) => {
      saved.push(t);
      backing = t;
    },
    clear: () => {
      cleared++;
      backing = undefined;
    },
  });

  assert.equal(store.get(), undefined);
  store.set("aff.a.1");
  assert.equal(store.get(), "aff.a.1");
  assert.deepEqual(saved, ["aff.a.1"]);
  store.clear();
  assert.equal(store.get(), undefined);
  assert.equal(cleared, 1);
});

test("store: loads a persisted ticket for the ws offer but waits for this connection's mint before leasing", async () => {
  const store = createAffinityTicketStore({ load: () => "aff.persisted.x", save: () => {}, clear: () => {} });
  assert.equal(store.get(), "aff.persisted.x", "the persisted ticket can route the ws handshake");
  let resolved: string | undefined;
  const waiting = store.waitForTicket().then((ticket) => (resolved = ticket));
  await Promise.resolve();
  assert.equal(resolved, undefined, "the persisted ticket is not lease-safe before the current mint frame");
  store.set("aff.refreshed.x");
  await waiting;
  assert.equal(resolved, "aff.refreshed.x");
});

test("store.waitForTicket resolves immediately when a ticket is held", async () => {
  const store = createAffinityTicketStore();
  store.set("aff.now.1");
  assert.equal(await store.waitForTicket(), "aff.now.1");
});

test("store.waitForTicket pends until the next set (a fresh ticketless connect's mint frame)", async () => {
  const store = createAffinityTicketStore();
  let resolved: string | undefined;
  const p = store.waitForTicket().then((t) => (resolved = t));
  await Promise.resolve();
  assert.equal(resolved, undefined, "still pending with no ticket");
  store.set("aff.minted.9");
  await p;
  assert.equal(resolved, "aff.minted.9");
});

test("store.clear leaves waiters pending — they resolve on the reassignment's fresh ticket", async () => {
  const store = createAffinityTicketStore();
  store.set("aff.old.1");
  store.clear(); // pinned follower gone
  let resolved: string | undefined;
  const p = store.waitForTicket().then((t) => (resolved = t));
  await Promise.resolve();
  assert.equal(resolved, undefined, "cleared ⇒ waits for the next mint");
  store.set("aff.new.2"); // reconnect anycasts to a live follower, which mints afresh
  await p;
  assert.equal(resolved, "aff.new.2");
});

test("store.leaseTicket latches ticketless after one shared timeout and leaves no stale waiter", async () => {
  mock.timers.enable({ apis: ["setTimeout"] });
  try {
    const store = createAffinityTicketStore();
    const first = store.leaseTicket(4_000);
    const concurrent = store.leaseTicket(4_000);
    assert.equal(first, concurrent, "concurrent first leases share one bounded wait");

    mock.timers.tick(4_000);
    assert.deepEqual(await first, { timedOut: true });
    assert.deepEqual(await concurrent, { timedOut: true });

    assert.deepEqual(
      await store.leaseTicket(4_000),
      { timedOut: false },
      "after the first timeout, later leases stay ticketless without another delay/warning",
    );

    store.set("aff.late.3");
    assert.deepEqual(
      await store.leaseTicket(4_000),
      { ticket: "aff.late.3", timedOut: false },
      "a late mint frame unlatches fallback and becomes immediately lease-safe",
    );
  } finally {
    mock.timers.reset();
  }
});

// --- the subprotocol offer --------------------------------------------------------------------

test("offerSubprotocols: base only when ticketless, base + ticket when held", () => {
  const store = createAffinityTicketStore();
  assert.deepEqual(offerSubprotocols(store), [WS_SUBPROTOCOL]);
  store.set("aff.tkt.7");
  assert.deepEqual(offerSubprotocols(store), [WS_SUBPROTOCOL, "aff.tkt.7"]);
});

// --- the source: frame capture + dead-follower clear ------------------------------------------

class FakeTransport implements Transport {
  sent: ClientMsg[] = [];
  private msgHandler: (m: ServerMsg) => void = () => {};
  private downHandler: () => void = () => {};
  private reconnectHandler: () => void = () => {};
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
  close(): void {}
  emit(m: ServerMsg): void {
    this.msgHandler(m);
  }
  down(): void {
    this.downHandler();
  }
  reconnect(): void {
    this.reconnectHandler();
  }
}

test("source records the ticket from a {t:'affinity'} frame into the store", () => {
  const store = createAffinityTicketStore();
  const transport = new FakeTransport();
  const source = new RemoteOptimisticSource({ transport }, "c1", { affinity: store });
  source.onNormalized(() => {});
  source.onProgress(() => {});

  transport.emit({ t: "affinity", ticket: "aff.minted.42" });
  assert.equal(store.get(), "aff.minted.42");
});

test("source makes the previous ticket handshake-only on reconnect until the new mint frame", async () => {
  const store = createAffinityTicketStore();
  const transport = new FakeTransport();
  const source = new RemoteOptimisticSource({ transport }, "c1", { affinity: store });
  source.onNormalized(() => {});
  source.onProgress(() => {});

  transport.emit({ t: "affinity", ticket: "aff.connection.1" });
  assert.deepEqual(await store.leaseTicket(10), { ticket: "aff.connection.1", timedOut: false });

  transport.reconnect();
  let resolved = false;
  const waiting = store.waitForTicket().then(() => (resolved = true));
  await Promise.resolve();
  assert.equal(resolved, false, "the old ticket cannot race the reconnect's HTTP re-lease");

  transport.emit({ t: "affinity", ticket: "aff.connection.2" });
  await waiting;
  assert.deepEqual(await store.leaseTicket(10), { ticket: "aff.connection.2", timedOut: false });
});

test("source clears the ticket on a sustained outage (dead follower ⇒ next connect ticketless)", () => {
  const store = createAffinityTicketStore();
  store.set("aff.pinned.1");
  const transport = new FakeTransport();
  const source = new RemoteOptimisticSource({ transport }, "c1", { affinity: store });
  source.onNormalized(() => {});
  source.onProgress(() => {});

  transport.down();
  assert.equal(store.get(), undefined, "the pinned follower is gone — drop the ticket");
});

test("no affinity store ⇒ an affinity frame is a harmless no-op (single-daemon path)", () => {
  const transport = new FakeTransport();
  const source = new RemoteOptimisticSource({ transport }, "c1");
  source.onNormalized(() => {});
  source.onProgress(() => {});
  // Must not throw; the frame is simply dropped.
  transport.emit({ t: "affinity", ticket: "aff.ignored.1" });
});
