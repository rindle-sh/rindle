import { test, mock } from "node:test";
import assert from "node:assert/strict";

import { WsTransport } from "../src/transport.ts";

/** A minimal stand-in for the global WebSocket: records instances and lets the test drive
 *  open/close events deterministically (no real sockets, no real timers). */
class FakeWS {
  static instances: FakeWS[] = [];
  url: string;
  protocols?: string | string[];
  closed = false;
  private listeners: Record<string, Array<() => void>> = {};
  constructor(url: string, protocols?: string | string[]) {
    this.url = url;
    this.protocols = protocols;
    FakeWS.instances.push(this);
  }
  addEventListener(type: string, fn: () => void): void {
    (this.listeners[type] ??= []).push(fn);
  }
  sent: unknown[] = [];
  send(data?: unknown): void {
    this.sent.push(data);
  }
  close(): void {
    this.closed = true;
  }
  emit(type: string): void {
    for (const fn of this.listeners[type] ?? []) fn();
  }
}

test("WsTransport offers no subprotocols by default (single-daemon path, byte-identical)", () => {
  const orig = (globalThis as { WebSocket?: unknown }).WebSocket;
  (globalThis as { WebSocket?: unknown }).WebSocket = FakeWS as unknown;
  try {
    FakeWS.instances.length = 0;
    new WsTransport("ws://x");
    assert.equal(FakeWS.instances[0].protocols, undefined);
  } finally {
    (globalThis as { WebSocket?: unknown }).WebSocket = orig;
  }
});

test("WsTransport offers the subprotocols thunk's list, re-evaluated per (re)connect", () => {
  const orig = (globalThis as { WebSocket?: unknown }).WebSocket;
  (globalThis as { WebSocket?: unknown }).WebSocket = FakeWS as unknown;
  mock.timers.enable({ apis: ["setTimeout"] });
  try {
    FakeWS.instances.length = 0;
    // The thunk's value CHANGES between connects (ticketless → ticketed), mirroring a mint frame
    // landing between the first connect and a reconnect.
    let offer: string[] = ["rindle.v1"];
    const t = new WsTransport("ws://x", { subprotocols: () => offer });
    void t;
    const latest = () => FakeWS.instances[FakeWS.instances.length - 1];
    assert.deepEqual(latest().protocols, ["rindle.v1"], "first connect: ticketless");

    latest().emit("open");
    offer = ["rindle.v1", "aff.tkt.1"]; // a ticket arrived
    latest().emit("close");
    mock.timers.tick(5000); // let the reconnect fire
    assert.deepEqual(latest().protocols, ["rindle.v1", "aff.tkt.1"], "reconnect: presents the fresh ticket");
  } finally {
    mock.timers.reset();
    (globalThis as { WebSocket?: unknown }).WebSocket = orig;
  }
});

test("WsTransport.onDown fires once per outage after downThreshold and re-arms after a reopen", () => {
  const orig = (globalThis as { WebSocket?: unknown }).WebSocket;
  (globalThis as { WebSocket?: unknown }).WebSocket = FakeWS as unknown;
  mock.timers.enable({ apis: ["setTimeout"] });
  try {
    FakeWS.instances.length = 0;
    let downs = 0;
    const t = new WsTransport("ws://x", { downThreshold: 4 });
    t.onDown(() => {
      downs++;
    });
    const latest = () => FakeWS.instances[FakeWS.instances.length - 1];

    latest().emit("open"); // establish the connection (everOpened = true)
    // Each failed cycle: the socket closes, then we advance past the backoff so the transport
    // spawns its next attempt. onDown should fire only once the failed-attempt count reaches 4.
    const failCycle = () => {
      latest().emit("close");
      mock.timers.tick(5000);
    };

    failCycle(); // attempt 1
    failCycle(); // attempt 2
    failCycle(); // attempt 3
    assert.equal(downs, 0, "below threshold: onDown has not fired");
    failCycle(); // attempt 4 — reaches downThreshold
    assert.equal(downs, 1, "fires once at the threshold");
    failCycle(); // attempt 5 — same outage episode
    assert.equal(downs, 1, "does not re-fire within the same episode");

    latest().emit("open"); // recover — re-arms onDown
    failCycle();
    failCycle();
    failCycle();
    failCycle(); // four fresh failures
    assert.equal(downs, 2, "re-arms after a successful reopen");
  } finally {
    mock.timers.reset();
    (globalThis as { WebSocket?: unknown }).WebSocket = orig;
  }
});

test("a bigint query arg throws typed into its sender and never strands queued frames", () => {
  const orig = (globalThis as { WebSocket?: unknown }).WebSocket;
  (globalThis as { WebSocket?: unknown }).WebSocket = FakeWS as unknown;
  try {
    FakeWS.instances.length = 0;
    const t = new WsTransport("ws://x");
    // Queued before open: a good frame, a poisoned frame (bigint arg — the typed
    // `int64()` schema makes `eq(5n)` type-check, and `JSON.stringify` throws on
    // bigint), then another good frame. Pre-fix the poison detonated inside the
    // socket's `open` listener: queryId 2's frame never sent, `pending` never
    // cleared, and the subscribe hung with no error anywhere.
    t.send({ type: "subscribe", queryId: 1 } as never);
    assert.throws(
      () => t.send({ type: "subscribe", queryId: 99, args: [5n] } as never),
      /bigint.*live-query wire.*design 226/s,
      "serialization happens at send time, typed, into the caller",
    );
    t.send({ type: "subscribe", queryId: 2 } as never);

    const ws = FakeWS.instances[0];
    ws.emit("open");
    assert.equal(ws.sent.length, 2, "both good frames flushed; nothing stranded");
    assert.match(String(ws.sent[0]), /"queryId":1/);
    assert.match(String(ws.sent[1]), /"queryId":2/);
  } finally {
    (globalThis as { WebSocket?: unknown }).WebSocket = orig;
  }
});
