// The zero-browser-config path: the client starts with no daemon/ws option, its first named-query
// lease returns the unified fleet endpoint + a fresh placement ticket, and that exact ticket is
// offered before the pure-lazy WebSocket opens.

import assert from "node:assert/strict";
import { test } from "node:test";
import { setTimeout as delay } from "node:timers/promises";

import { createSchema, defineQuery, initWasm, newQueryBuilder, number, table } from "@rindle/wasm";

import { createRindleClient } from "../src/index.ts";

await initWasm();

type Listener = (event: { data?: string }) => void;

class ScriptedWebSocket {
  static instances: ScriptedWebSocket[] = [];

  readonly url: string;
  readonly protocols: string[];
  readonly sent: string[] = [];
  private readonly listeners = new Map<string, Listener[]>();

  constructor(url: string | URL, protocols?: string | string[]) {
    this.url = String(url);
    this.protocols = protocols === undefined ? [] : typeof protocols === "string" ? [protocols] : [...protocols];
    ScriptedWebSocket.instances.push(this);
  }

  addEventListener(kind: string, listener: Listener): void {
    const listeners = this.listeners.get(kind) ?? [];
    listeners.push(listener);
    this.listeners.set(kind, listeners);
  }

  send(body: string): void {
    this.sent.push(body);
  }

  close(): void {
    this.emit("close");
  }

  emit(kind: string, event: { data?: string } = {}): void {
    for (const listener of this.listeners.get(kind) ?? []) listener(event);
  }
}

const issue = table("issue").columns({ id: number() }).primaryKey("id");
const schema = createSchema({ tables: [issue] });
const qb = newQueryBuilder(schema);
const allIssues = defineQuery("allIssues", () => qb.issue.orderBy("id", "asc"));
const allIssuesDescending = defineQuery("allIssuesDescending", () => qb.issue.orderBy("id", "desc"));
const firstIssue = defineQuery("firstIssue", () => qb.issue.orderBy("id", "asc").limit(1));

test("the first lease discovers and affinity-pins the pure-lazy WebSocket", async () => {
  const original = globalThis.WebSocket;
  ScriptedWebSocket.instances = [];
  globalThis.WebSocket = ScriptedWebSocket as unknown as typeof WebSocket;
  const calls: unknown[] = [];
  const fetchImpl = (async (_input: unknown, init?: { body?: string }) => {
    calls.push(JSON.parse(init?.body ?? "{}"));
    return new Response(
      JSON.stringify({
        materializationId: "m1",
        leaseToken: "lease-1",
        wsEndpoint: "ws://fleet.example",
        affinity: "aff.first-placement",
      }),
      { status: 200, headers: { "content-type": "application/json" } },
    );
  }) as typeof fetch;

  const client = await createRindleClient({
    schema,
    mutators: {},
    api: { url: "", fetch: fetchImpl },
    clientID: "client-1",
  });
  try {
    assert.equal(ScriptedWebSocket.instances.length, 0, "construction does not need a config endpoint");
    client.store.materialize(allIssues());
    await delay(20);

    assert.equal(calls.length, 1, "one query lease drives discovery");
    const socket = ScriptedWebSocket.instances[0];
    assert.ok(socket);
    assert.equal(socket.url, "ws://fleet.example");
    assert.deepEqual(socket.protocols, ["rindle.v1", "aff.first-placement"]);

    socket.emit("open");
    const frames = socket.sent.map((body) => JSON.parse(body) as Record<string, unknown>);
    assert.deepEqual(frames[0], { t: "init", clientID: "client-1" });
    assert.ok(frames.some((frame) => frame.t === "subscribe" && frame.leaseToken === "lease-1"));
  } finally {
    client.close();
    globalThis.WebSocket = original;
  }
});

test("concurrent first leases serialize discovery and share its placement", async () => {
  const original = globalThis.WebSocket;
  ScriptedWebSocket.instances = [];
  globalThis.WebSocket = ScriptedWebSocket as unknown as typeof WebSocket;
  const calls: Array<Record<string, unknown>> = [];
  let finishDiscovery!: (response: Response) => void;
  const discoveryResponse = new Promise<Response>((resolve) => {
    finishDiscovery = resolve;
  });
  const fetchImpl = (async (_input: unknown, init?: { body?: string }) => {
    const body = JSON.parse(init?.body ?? "{}") as Record<string, unknown>;
    calls.push(body);
    if (calls.length === 1) return discoveryResponse;
    return new Response(
      JSON.stringify({
        materializationId: "m2",
        leaseToken: "lease-2",
        wsEndpoint: "ws://follower-1.example",
        affinity: "aff.follower-1",
      }),
      { status: 200, headers: { "content-type": "application/json" } },
    );
  }) as typeof fetch;

  const client = await createRindleClient({
    schema,
    mutators: {},
    api: { url: "", fetch: fetchImpl },
    clientID: "client-1",
  });
  try {
    client.store.materialize(allIssues());
    client.store.materialize(allIssuesDescending());
    await delay(20);

    assert.equal(calls.length, 1, "only one ticketless discovery lease is in flight");
    assert.equal(calls[0]?.affinity, undefined);

    finishDiscovery(
      new Response(
        JSON.stringify({
          materializationId: "m1",
          leaseToken: "lease-1",
          wsEndpoint: "ws://follower-1.example",
          affinity: "aff.follower-1",
        }),
        { status: 200, headers: { "content-type": "application/json" } },
      ),
    );
    await delay(20);

    assert.equal(calls.length, 2);
    assert.equal(calls[1]?.affinity, "aff.follower-1", "the queued lease reuses the discovered placement");
    assert.equal(ScriptedWebSocket.instances.length, 1, "both leases share the placed socket");
    assert.equal(ScriptedWebSocket.instances[0]?.url, "ws://follower-1.example");
  } finally {
    client.close();
    globalThis.WebSocket = original;
  }
});

test("a ticketless backend settles discovery — later leases run unserialized", async () => {
  const original = globalThis.WebSocket;
  ScriptedWebSocket.instances = [];
  globalThis.WebSocket = ScriptedWebSocket as unknown as typeof WebSocket;
  const calls: Array<Record<string, unknown>> = [];
  const pending: Array<(response: Response) => void> = [];
  const fetchImpl = (async (_input: unknown, init?: { body?: string }) => {
    calls.push(JSON.parse(init?.body ?? "{}") as Record<string, unknown>);
    if (calls.length === 1) {
      // A backend that names an endpoint but mints no placement ticket (a single follower, or a
      // fleet behind an affinity-off control plane).
      return new Response(
        JSON.stringify({ materializationId: "m1", leaseToken: "lease-1", wsEndpoint: "ws://fleet.example" }),
        { status: 200, headers: { "content-type": "application/json" } },
      );
    }
    return new Promise<Response>((resolve) => pending.push(resolve));
  }) as typeof fetch;

  const client = await createRindleClient({
    schema,
    mutators: {},
    api: { url: "", fetch: fetchImpl },
    clientID: "client-1",
  });
  try {
    client.store.materialize(allIssues());
    await delay(20);
    assert.equal(calls.length, 1, "the first lease alone probes for placement");

    client.store.materialize(allIssuesDescending());
    client.store.materialize(firstIssue());
    await delay(20);
    assert.equal(
      calls.length,
      3,
      "after a ticketless reply the remaining leases go out concurrently, not one at a time",
    );
  } finally {
    for (const resolve of pending) {
      resolve(
        new Response(JSON.stringify({ materializationId: "mX", leaseToken: "lease-X" }), {
          status: 200,
          headers: { "content-type": "application/json" },
        }),
      );
    }
    await delay(0);
    client.close();
    globalThis.WebSocket = original;
  }
});

test("an active connection's frame discipline wins — lease-carried tickets no longer overwrite", async () => {
  const original = globalThis.WebSocket;
  ScriptedWebSocket.instances = [];
  globalThis.WebSocket = ScriptedWebSocket as unknown as typeof WebSocket;
  const calls: Array<Record<string, unknown>> = [];
  const fetchImpl = (async (_input: unknown, init?: { body?: string }) => {
    calls.push(JSON.parse(init?.body ?? "{}") as Record<string, unknown>);
    return new Response(
      JSON.stringify({
        materializationId: `m${calls.length}`,
        leaseToken: `lease-${calls.length}`,
        wsEndpoint: "ws://fleet.example",
        // The first ticket activates affinity; every later one must be IGNORED — after a
        // connection exists, only its {t:"affinity"} frames may move the pin.
        affinity: calls.length === 1 ? "aff.first-placement" : "aff.rotated-elsewhere",
      }),
      { status: 200, headers: { "content-type": "application/json" } },
    );
  }) as typeof fetch;

  const client = await createRindleClient({
    schema,
    mutators: {},
    api: { url: "", fetch: fetchImpl },
    clientID: "client-1",
  });
  try {
    client.store.materialize(allIssues());
    await delay(20);
    const socket = ScriptedWebSocket.instances[0];
    assert.ok(socket);
    socket.emit("open");
    socket.emit("message", { data: JSON.stringify({ t: "affinity", ticket: "aff.frame-1" }) });
    await delay(0);

    client.store.materialize(allIssuesDescending());
    await delay(20);
    assert.equal(calls[1]?.affinity, "aff.frame-1", "leases forward the connection's frame ticket");

    client.store.materialize(firstIssue());
    await delay(20);
    assert.equal(
      calls[2]?.affinity,
      "aff.frame-1",
      "the rotated ticket in lease replies did not displace the connection's own frame",
    );
  } finally {
    client.close();
    globalThis.WebSocket = original;
  }
});
