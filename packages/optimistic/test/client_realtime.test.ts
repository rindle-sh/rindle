// G-v resolve-then-register (RINDLE-REALTIME-QUERY-ENABLEMENT §2.1 client flow, re-based on
// 302-ROOM-STORE-SEPARATION): a `realtime`-labeled named query resolves its lease FIRST; a lease
// WITH a realtime block connects the room source (once per sourceKey), registers the room's OWNED
// tables as namespaced engine twins (302 §2), and retains the sub on the room channel presenting
// the roomToken — a lease WITHOUT one fails open to the daemon
// path byte-identically to an unlabeled query (one POST, the handed token). Re-resolution
// (reconnect / proactive renewal) is renewal-as-reauthorization through the same app route; a
// renewal without a realtime block (the DOWNGRADE signal) and a lease naming a different sourceKey
// than the live sub are surfaced LOUDLY (callback + console.error) — the graceful DOWNGRADE dance
// is Slice I-v. The UPGRADE dance shipped in Slice I-iv (the lanes at the bottom of this file):
// a daemon-attached labeled query reacts to the §4.1 occupancy doorbell with one debounced
// re-lease and retargets onto the room with no flicker.
//
// Everything here is scripted (fetch + transports); the live end-to-end twin is
// packages/room/test/dual-source.e2e.mjs. Requires the wasm artifact (pnpm run build:wasm).

import { test } from "node:test";
import assert from "node:assert/strict";
import { setTimeout as delay } from "node:timers/promises";

import {
  COMPARATOR_VERSION,
  createSchema,
  defineQuery,
  initWasm,
  LMID_QUERY_NAME,
  newQueryBuilder,
  number,
  string,
  table,
} from "@rindle/wasm";
import type { NormalizedOp } from "@rindle/wasm";
import { normalizedFp } from "@rindle/remote";
import type { ClientMsg, ServerMsg, Transport } from "@rindle/remote";

import { createRindleClient, type ClientRegistry, type MutationTx, type RealtimeAnomaly } from "../src/index.ts";

await initWasm();

// ------------------------------------------------------------------------------- fixture

const note = table("note").columns({ id: number(), body: string() }).primaryKey("id");
// `acl` is a CONTEXT ("followed") table: the daemon is its sole authority (302 §6) — a room may
// still relay its rows, and the gate must DROP them. Used by the 302 §6 lane below.
const acl = table("acl").columns({ id: number(), principal: string() }).primaryKey("id");
const schema = createSchema({ tables: [note, acl] });
const qb = newQueryBuilder(schema);

const liveNotes = defineQuery("liveNotes", () => qb.note.orderBy("id", "asc"), {
  realtime: { room: "doc" },
});
const hotNotes = defineQuery("hotNotes", () => qb.note.orderBy("id", "desc"), {
  realtime: { room: "doc" },
});
const allNotes = defineQuery("allNotes", () => qb.note.orderBy("id", "asc"));
// The daemon-served "global acl list" (§6): unlabeled ⇒ the daemon path, authoritative for `acl`.
const allAcl = defineQuery("allAcl", () => qb.acl.orderBy("id", "asc"));

const ROOM_KEY = "room:doc/main";
const ROOM_WS = "ws://room-1";
const RETAIN_BASE = 2 ** 30; // client-minted retain qids live in their own high band

// The wire hello for the `note` table (what the scripted "server" answers subscribes with).
const noteTables = [{ name: "note", columns: ["id", "body"], primaryKey: [0] }];
const fp = normalizedFp(noteTables);
const helloMsg = (queryId: number): ServerMsg => ({
  t: "nhello",
  queryId,
  hello: { epoch: 1, comparatorVersion: COMPARATOR_VERSION, tables: noteTables, normalizedFp: fp },
  bootId: "b1",
});
const batchMsg = (queryId: number, seq: number, ops: NormalizedOp[], cv: number): ServerMsg => ({
  t: "nbatch",
  queryId,
  batch: { epoch: 1, seq, normalizedFp: fp, ops, cv },
});
const progressMsg = (cvMin: number): ServerMsg => ({ t: "progress", frame: { cvMin } });
const add = (id: number, body: string): NormalizedOp => ({ table: "note", op: "add", row: [id, body] }) as NormalizedOp;

// §6 context-overlap wire helpers. A room DOC subscription follows both `note` (writable) and `acl`
// (context) — its hello/footprint lists both, and the room relays acl rows on that gate. A DAEMON
// acl subscription lists `acl` alone (the global list). One `acl` op helper serves both channels.
const docTables = [
  { name: "note", columns: ["id", "body"], primaryKey: [0] },
  { name: "acl", columns: ["id", "principal"], primaryKey: [0] },
];
const docFp = normalizedFp(docTables);
const docHello = (queryId: number): ServerMsg => ({
  t: "nhello",
  queryId,
  hello: { epoch: 1, comparatorVersion: COMPARATOR_VERSION, tables: docTables, normalizedFp: docFp },
  bootId: "b1",
});
const docBatch = (queryId: number, seq: number, ops: NormalizedOp[], cv: number): ServerMsg => ({
  t: "nbatch",
  queryId,
  batch: { epoch: 1, seq, normalizedFp: docFp, ops, cv },
});
const aclTables = [{ name: "acl", columns: ["id", "principal"], primaryKey: [0] }];
const aclFp = normalizedFp(aclTables);
const aclHello = (queryId: number): ServerMsg => ({
  t: "nhello",
  queryId,
  hello: { epoch: 1, comparatorVersion: COMPARATOR_VERSION, tables: aclTables, normalizedFp: aclFp },
  bootId: "b1",
});
const aclBatch = (queryId: number, seq: number, ops: NormalizedOp[], cv: number): ServerMsg => ({
  t: "nbatch",
  queryId,
  batch: { epoch: 1, seq, normalizedFp: aclFp, ops, cv },
});
const aclOp = (id: number, principal: string, op: "add" | "remove" = "add"): NormalizedOp =>
  ({ table: "acl", op, row: [id, principal] }) as NormalizedOp;

// The room DOC lease: `note` writable (predicate, no `where` ⇒ writable-`all`), `acl` context
// (`kind:"none"`, with the vacuous-true footprintWhere an unconstrained root emits).
const docContextLease = () =>
  roomLease({
    tables: [
      { table: "note", writable: { kind: "predicate", joinKeyCols: ["id"] } },
      { table: "acl", writable: { kind: "none" }, footprintWhere: { type: "and", conditions: [] } },
    ],
  });

/** A recording transport: `sent` is the wire transcript; `emit` plays the server. */
class TestTransport implements Transport {
  sent: ClientMsg[] = [];
  closed = false;
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
  reconnect(): void {
    this.reconnectHandler();
  }
  close(): void {
    this.closed = true;
  }

  /** The `subscribe` frames only (skipping init/unsubscribe). */
  subscribes(): Extract<ClientMsg, { t: "subscribe" }>[] {
    return this.sent.filter((m): m is Extract<ClientMsg, { t: "subscribe" }> => m.t === "subscribe");
  }
  /** The lease-token subscribes only (the room/daemon data subs — never the named lmid one). */
  leaseSubscribes(): Array<{ queryId: number; leaseToken: string }> {
    return this.subscribes()
      .filter((m): m is Extract<ClientMsg, { t: "subscribe"; leaseToken: string }> => "leaseToken" in m && m.leaseToken !== undefined)
      .map((m) => ({ queryId: m.queryId, leaseToken: m.leaseToken }));
  }
}

/** A scripted app-API fetch: each queued responder answers ONE lease POST (in order); it may throw
 *  (a network failure) or return a promise (a lease the test resolves later). */
function scriptedFetch() {
  const calls: { path: string; body: { name: string; args: unknown; clientId: string } }[] = [];
  const queue: Array<(body: unknown) => unknown> = [];
  const impl = (async (url: unknown, init?: { body?: string }) => {
    const body = init?.body ? JSON.parse(init.body) : {};
    calls.push({ path: String(url), body });
    const next = queue.shift();
    if (!next) throw new Error(`unexpected lease POST: ${String(url)} ${init?.body ?? ""}`);
    const payload = await next(body);
    return { ok: true, status: 200, text: async () => JSON.stringify(payload) };
  }) as unknown as typeof fetch;
  return { impl, calls, queue };
}

const roomLease = (over: Partial<{ sourceKey: string; wsEndpoint: string; roomToken: string; exp: number; doc: string; tables: unknown[] }> = {}) => ({
  leaseToken: "daemon-lease",
  materializationId: "m1",
  realtime: {
    sourceKey: ROOM_KEY,
    wsEndpoint: ROOM_WS,
    roomToken: "rt-1",
    exp: Date.now() + 60_000,
    doc: "doc/main",
    // An unconstrained node ⇒ `where` ABSENT ⇒ the engine's writable-`all` arm.
    tables: [{ table: "note", writable: { kind: "predicate", joinKeyCols: ["id"] } }],
    ...over,
  },
});

async function makeClient(
  realtimeOver: { renewMarginMs?: number; transport?: (endpoint: string) => Transport } = {},
  extra: { mutators?: ClientRegistry } = {},
) {
  const daemonT = new TestTransport();
  const roomTs = new Map<string, TestTransport>();
  const fetchS = scriptedFetch();
  const anomalies: RealtimeAnomaly[] = [];
  const client = await createRindleClient({
    schema,
    mutators: extra.mutators ?? {},
    api: { url: "http://api.test", fetch: fetchS.impl },
    daemon: { transport: daemonT },
    clientID: "c1",
    realtime: {
      transport: (endpoint) => {
        const t = new TestTransport();
        roomTs.set(endpoint, t);
        return t;
      },
      onAnomaly: (a) => anomalies.push(a),
      ...realtimeOver,
    },
  });
  // Record every room-table registration CALL (the public 302 seam G-v drives). The backend
  // dedups internally per (sourceKey, table), so re-leases legitimately re-call this — the
  // registration RECORD (`backend.roomTablesFor`) is the idempotence observable, not the count.
  const registrations: [string, readonly string[]][] = [];
  const origRegister = client.backend.registerRoomTables.bind(client.backend);
  client.backend.registerRoomTables = (sourceKey: string, tables: readonly string[]) => {
    registrations.push([sourceKey, [...tables]]);
    origRegister(sourceKey, tables);
  };
  return { client, daemonT, roomTs, fetchS, anomalies, registrations };
}

/** Let the resolve-then-register microtask chain (lease POST → attach → resolver → send) drain. */
const settle = () => delay(20);

// ============================================================================================
// labeled + covered ⇒ the room channel
// ============================================================================================

test("labeled+covered: resolve-then-register connects the room, registers its tables, and retains presenting the roomToken", async () => {
  const { client, daemonT, roomTs, fetchS, anomalies, registrations } = await makeClient();
  try {
    fetchS.queue.push(() => roomLease());
    const view = client.store.materialize(liveNotes());
    // Synchronous half: the local view exists, empty, NOT server-authoritative. (length, not
    // deepEqual-with-[] — the assertion signature would narrow `view.data` to never[].)
    assert.equal(view.data.length, 0);
    assert.equal(view.resultType, "unknown", "a labeled view is `unknown` through the lease window");

    await settle();
    // Exactly ONE lease POST — resolve-then-register, no second lease at subscribe time.
    assert.equal(fetchS.calls.length, 1);
    assert.deepEqual(fetchS.calls[0].body, { name: "liveNotes", args: null, clientId: "c1" });

    // The room transport opened at the lease's DEDICATED realtime.wsEndpoint…
    const roomT = roomTs.get(ROOM_WS);
    assert.ok(roomT, "a room transport opened at realtime.wsEndpoint");
    // …announced identity, subscribed its OWN lmid system query, and the retained sub presents the
    // roomToken as its leaseToken — the wire shape the room shell verifies.
    assert.deepEqual(roomT.sent[0], { t: "init", clientID: "c1" });
    const lmidSub = roomT.subscribes().find((m) => "name" in m && m.name === LMID_QUERY_NAME);
    assert.ok(lmidSub, "the room channel carries its own reserved lmid subscribe");
    assert.deepEqual(roomT.leaseSubscribes(), [{ queryId: RETAIN_BASE, leaseToken: "rt-1" }]);
    // The daemon channel never saw this query (no lease subscribe at all).
    assert.deepEqual(daemonT.leaseSubscribes(), []);

    // The room's OWNED table registered its namespaced engine twin BEFORE retaining (302 §2 —
    // one source per table; the writable-kind spec marks ownership, context would be skipped).
    assert.deepEqual(registrations, [[ROOM_KEY, ["note"]]]);
    assert.deepEqual([...client.backend.roomTablesFor(ROOM_KEY)], [["note", `note@${ROOM_KEY}`]]);
    const inspect = client.__realtimeInspect();
    assert.deepEqual(Object.keys(inspect.rooms), [ROOM_KEY]);
    assert.deepEqual(inspect.rooms[ROOM_KEY].promoted, { note: `note@${ROOM_KEY}` });
    const queryStates = Object.values(inspect.rooms[ROOM_KEY].queries);
    assert.equal(queryStates.length, 1);
    assert.deepEqual(queryStates[0], { name: "liveNotes", sourceQid: RETAIN_BASE, exp: queryStates[0].exp, refCount: 1 });

    // Live hydration through the ROOM gate: hello → cv-stamped snapshot → progress release. The
    // release folds the rows into `note@room` and the view SWAPS onto it at the release tail
    // (302 §4.1 swap-in).
    roomT.emit(helloMsg(RETAIN_BASE));
    roomT.emit(batchMsg(RETAIN_BASE, 0, [add(1, "a"), add(2, "b")], 1));
    assert.equal(view.resultType, "unknown", "buffered ≠ released");
    roomT.emit(progressMsg(1));
    assert.deepEqual(view.data.map((r) => [r.id, r.body]), [[1, "a"], [2, "b"]]);
    assert.equal(view.resultType, "complete", "hydrated by the ROOM gate's release");
    assert.equal(
      client.backend.__inspectDomains().swappedViews[view.qid],
      ROOM_KEY,
      "the view is registered on the room's namespaced table (302 §4.1)",
    );

    // Gate isolation: the room release never moved the daemon gate.
    const gates = client.backend.__inspectDomains().gates;
    assert.deepEqual(gates, {
      daemon: { appliedCv: 0, bufferedFrames: 0 },
      [ROOM_KEY]: { appliedCv: 1, bufferedFrames: 0 },
    });
    assert.deepEqual(anomalies, []);
  } finally {
    client.close();
  }
});

test("302 §4.1 LATE JOIN: a second view of the same labeled query materialized AFTER hydration swaps onto the room table too — never a plain-table ghost view", async () => {
  const { client, roomTs, fetchS, anomalies } = await makeClient();
  try {
    fetchS.queue.push(() => roomLease());
    const view1 = client.store.materialize(liveNotes());
    await settle();
    const roomT = roomTs.get(ROOM_WS)!;
    roomT.emit(helloMsg(RETAIN_BASE));
    roomT.emit(batchMsg(RETAIN_BASE, 0, [add(1, "a"), add(2, "b")], 1));
    roomT.emit(progressMsg(1));
    assert.deepEqual(view1.data.map((r) => [r.id, r.body]), [[1, "a"], [2, "b"]]);
    assert.equal(client.backend.__inspectDomains().swappedViews[view1.qid], ROOM_KEY, "view 1 swapped at hydration");

    // The late joiner: a second component materializes the SAME labeled query after the room sub
    // hydrated (the ordinary late-mount case — markSubHydrated's one-shot swap queue is long
    // gone). Pre-fix it stayed registered on the plain `note` table the room channel never
    // feeds: empty, reported complete, permanently diverging from view 1.
    fetchS.queue.push(() => roomLease());
    const view2 = client.store.materialize(liveNotes());
    await settle();
    assert.equal(view2.resultType, "complete", "late joiner hydrates off the sub's state");
    assert.equal(client.backend.__inspectDomains().swappedViews[view2.qid], ROOM_KEY, "…and swapped onto the room table");
    assert.deepEqual(view2.data.map((r) => [r.id, r.body]), [[1, "a"], [2, "b"]], "…reading the room state, not the empty plain table");

    // A further room delta reaches BOTH views identically — no permanent divergence.
    roomT.emit(batchMsg(RETAIN_BASE, 1, [add(3, "c")], 2));
    roomT.emit(progressMsg(2));
    assert.deepEqual(view1.data.map((r) => r.id), [1, 2, 3]);
    assert.deepEqual(view2.data.map((r) => r.id), [1, 2, 3]);
    assert.deepEqual(anomalies, []);
  } finally {
    client.close();
  }
});

test("302 §6: an ALL-CONTEXT lease installs an EMPTY rename map — relayed deltas are DROPPED, never folded into the daemon-owned plain tables", async () => {
  const { client, roomTs, fetchS, anomalies, registrations } = await makeClient();
  try {
    // Every footprint table is context (`writable.kind: "none"`) — legal per compileRoomScopeSpecs.
    fetchS.queue.push(() =>
      roomLease({ tables: [{ table: "note", writable: { kind: "none" }, footprintWhere: { type: "and", conditions: [] } }] }),
    );
    const view = client.store.materialize(liveNotes());
    await settle();
    // The registration MUST still happen with the empty owned set: the installed empty map is
    // what arms the gate's §6 drop rule. Pre-fix the call was skipped, `tableMap` stayed
    // undefined, and the room's relayed copies folded VERBATIM into the plain `note` table —
    // a second sync layer fighting the daemon over one baseline.
    assert.deepEqual(registrations, [[ROOM_KEY, []]], "registerRoomTables called with owned = []");
    assert.deepEqual([...client.backend.roomTablesFor(ROOM_KEY)], [], "empty rename map installed");

    const roomT = roomTs.get(ROOM_WS)!;
    roomT.emit(helloMsg(RETAIN_BASE));
    roomT.emit(batchMsg(RETAIN_BASE, 0, [add(1, "relayed")], 1));
    roomT.emit(progressMsg(1));
    assert.equal(view.data.length, 0, "the relayed copy never entered the store (302 §6)");

    // No anomaly: room-store separation still drops the relayed copies (§6), and the §6.1
    // covering diagnostic was removed with the covering machinery (the app declares coverage).
    assert.deepEqual(anomalies, []);
  } finally {
    client.close();
  }
});

// ============================================================================================
// labeled + uncovered ⇒ the daemon path, byte-identical to unlabeled
// ============================================================================================

test("labeled+uncovered fails open: the daemon path with ONE lease POST, indistinguishable from unlabeled", async () => {
  const { client, daemonT, roomTs, fetchS, anomalies } = await makeClient();
  try {
    fetchS.queue.push(() => ({ leaseToken: "L-labeled", materializationId: "m1" }));
    const labeled = client.store.materialize(liveNotes());
    assert.equal(labeled.resultType, "unknown");
    await settle();

    // ONE POST; the daemon subscribe presents exactly that lease's token (the handoff — never a
    // second POST for the same subscribe).
    assert.equal(fetchS.calls.length, 1);
    assert.deepEqual(daemonT.leaseSubscribes(), [{ queryId: RETAIN_BASE, leaseToken: "L-labeled" }]);
    assert.equal(roomTs.size, 0, "no room transport for an uncovered lease");
    assert.deepEqual(client.__realtimeInspect().rooms, {});

    // An UNLABELED query for comparison: its subscribe frame must be shape-identical (only the
    // qid identity and token value differ — the fail-open path is indistinguishable).
    fetchS.queue.push(() => ({ leaseToken: "L-plain", materializationId: "m2" }));
    const plain = client.store.materialize(allNotes());
    await settle();
    const [labeledFrame, plainFrame] = daemonT.subscribes().filter((m) => "leaseToken" in m);
    assert.deepEqual(Object.keys(labeledFrame).sort(), Object.keys(plainFrame).sort());
    assert.equal(labeledFrame.mode, plainFrame.mode);

    // Both hydrate through the DAEMON gate.
    for (const [frame, viewQid] of [[labeledFrame, RETAIN_BASE] as const, [plainFrame, plainFrame.queryId] as const]) {
      void viewQid;
      daemonT.emit(helloMsg(frame.queryId));
      daemonT.emit(batchMsg(frame.queryId, 0, [add(1, "a")], 1));
    }
    daemonT.emit(progressMsg(1));
    assert.equal(labeled.resultType, "complete");
    assert.equal(plain.resultType, "complete");
    assert.deepEqual(labeled.data.map((r) => [r.id, r.body]), [[1, "a"]]);
    assert.deepEqual(anomalies, []);
  } finally {
    client.close();
  }
});

test("a THROWING room attach fails open to the daemon lease (not local-only)", async () => {
  // The room transport refuses to construct — the attachRoom throw shape (the old version-skew
  // predicate-compile throw is gone with the predicates: 302 registers tables by NAME only).
  const { client, daemonT, fetchS, anomalies } = await makeClient({
    transport: () => {
      throw new Error("room socket refused");
    },
  });
  try {
    fetchS.queue.push(() => roomLease());
    const view = client.store.materialize(liveNotes());
    await settle();

    assert.equal(anomalies.length, 1);
    assert.equal(anomalies[0].kind, "lease-failed");
    assert.match(anomalies[0].message, /falling back to the daemon lease/);
    // The daemon sub presents the lease's daemon token; nothing registered, nothing retained —
    // and no room token leaked to the daemon.
    assert.deepEqual(daemonT.leaseSubscribes(), [{ queryId: RETAIN_BASE, leaseToken: "daemon-lease" }]);
    const rooms = client.__realtimeInspect().rooms;
    assert.deepEqual(rooms[ROOM_KEY]?.promoted ?? {}, {}, "the throwing attach registered nothing");
    assert.deepEqual(rooms[ROOM_KEY]?.queries ?? {}, {}, "no live room query state");
    assert.equal(client.backend.roomTablesFor(ROOM_KEY).size, 0, "no namespaced tables minted");

    // The view hydrates through the DAEMON gate — proof it is served, not local-only.
    daemonT.emit(helloMsg(RETAIN_BASE));
    daemonT.emit(batchMsg(RETAIN_BASE, 0, [add(1, "a")], 1));
    daemonT.emit(progressMsg(1));
    assert.equal(view.resultType, "complete");
    assert.deepEqual(view.data.map((r) => [r.id, r.body]), [[1, "a"]]);
  } finally {
    client.close();
  }
});

test("a failed lease POST fails open to the daemon (the resolver re-leases; recovery = unlabeled story)", async () => {
  const { client, daemonT, fetchS, anomalies } = await makeClient();
  try {
    fetchS.queue.push(() => {
      throw new Error("api unreachable");
    });
    // The daemon retain's own resolver then re-leases (no handoff was possible) — answer THAT one.
    fetchS.queue.push(() => ({ leaseToken: "L-retry", materializationId: "m1" }));
    client.store.materialize(liveNotes());
    await settle();
    assert.equal(anomalies.length, 1);
    assert.equal(anomalies[0].kind, "lease-failed");
    assert.equal(fetchS.calls.length, 2, "the daemon retain re-leased on its own");
    assert.deepEqual(daemonT.leaseSubscribes(), [{ queryId: RETAIN_BASE, leaseToken: "L-retry" }]);
  } finally {
    client.close();
  }
});

// ============================================================================================
// re-resolution / renewal — renewal-as-reauthorization
// ============================================================================================

test("a reconnect re-resolution re-leases through the app route and presents the FRESH roomToken", async () => {
  const { client, roomTs, fetchS, anomalies } = await makeClient();
  try {
    fetchS.queue.push(() => roomLease());
    client.store.materialize(liveNotes());
    await settle();
    const roomT = roomTs.get(ROOM_WS)!;
    assert.deepEqual(roomT.leaseSubscribes(), [{ queryId: RETAIN_BASE, leaseToken: "rt-1" }]);

    // The room ws drops and reconnects: the source re-resolves every sub — a FULL re-lease (the
    // handoff was consumed), and the fresh roomToken rides the re-subscribe.
    fetchS.queue.push(() => roomLease({ roomToken: "rt-2" }));
    roomT.reconnect();
    await settle();
    assert.equal(fetchS.calls.length, 2, "re-resolution = one fresh lease POST");
    assert.deepEqual(roomT.leaseSubscribes().at(-1), { queryId: RETAIN_BASE, leaseToken: "rt-2" });
    // A re-lease never re-registers: the record is idempotent per (sourceKey, table).
    assert.deepEqual([...client.backend.roomTablesFor(ROOM_KEY)], [["note", `note@${ROOM_KEY}`]]);
    assert.deepEqual(anomalies, []);
  } finally {
    client.close();
  }
});

test("proactive renewal: a fresh lease lands BEFORE exp and re-subscribes with the new token (no reconnect)", async () => {
  const { client, roomTs, fetchS, anomalies } = await makeClient({ renewMarginMs: 200 });
  try {
    // exp - margin = 1.1s (just above the 1s renewal floor) — the renewal must fire BEFORE exp.
    fetchS.queue.push(() => roomLease({ exp: Date.now() + 1_300 }));
    client.store.materialize(liveNotes());
    await settle();
    const roomT = roomTs.get(ROOM_WS)!;
    assert.equal(roomT.leaseSubscribes().length, 1);

    fetchS.queue.push(() => roomLease({ roomToken: "rt-renewed", exp: Date.now() + 60_000 }));
    await delay(1_500); // > the ~1.1s renewal point, < nothing else scheduled
    assert.equal(fetchS.calls.length, 2, "the renewal POSTed on its own timer");
    assert.deepEqual(roomT.leaseSubscribes().at(-1), { queryId: RETAIN_BASE, leaseToken: "rt-renewed" });
    assert.deepEqual(anomalies, []);
  } finally {
    client.close();
  }
});

test("renewal DOWNGRADE (no realtime block) is loud and does NOT re-subscribe the room sub", async () => {
  const { client, roomTs, fetchS, anomalies } = await makeClient({ renewMarginMs: 200 });
  try {
    fetchS.queue.push(() => roomLease({ exp: Date.now() + 1_300 }));
    client.store.materialize(liveNotes());
    await settle();
    const roomT = roomTs.get(ROOM_WS)!;

    fetchS.queue.push(() => ({ leaseToken: "L-downgraded", materializationId: "m2" }));
    await delay(1_500);
    assert.equal(anomalies.length, 1);
    assert.equal(anomalies[0].kind, "downgrade");
    assert.equal(anomalies[0].name, "liveNotes");
    assert.equal(roomT.leaseSubscribes().length, 1, "the stale token was never replaced/re-presented");
  } finally {
    client.close();
  }
});

test("a second lease naming a DIFFERENT sourceKey than the live sub is a loud error (no second room)", async () => {
  const { client, roomTs, fetchS, anomalies } = await makeClient();
  try {
    fetchS.queue.push(() => roomLease());
    fetchS.queue.push(() => roomLease({ sourceKey: "room:doc/other", wsEndpoint: "ws://room-2" }));
    const first = client.store.materialize(liveNotes());
    const second = client.store.materialize(liveNotes());
    void first;
    await settle();

    assert.equal(anomalies.length, 1);
    assert.equal(anomalies[0].kind, "source-key-changed");
    assert.deepEqual(Object.keys(client.__realtimeInspect().rooms), [ROOM_KEY], "no second room connected");
    assert.equal(roomTs.has("ws://room-2"), false, "no transport to the conflicting endpoint");
    const roomT = roomTs.get(ROOM_WS)!;
    assert.equal(roomT.leaseSubscribes().length, 1, "the conflicting view never retained");
    assert.equal(second.resultType, "unknown", "…and stays visibly un-hydrated (loud, not silent)");
  } finally {
    client.close();
  }
});

// ============================================================================================
// 302 §2 registration semantics: idempotence, ownership (writable-only), unknown tables
// ============================================================================================

test("registration is idempotent per (sourceKey, table); unknown tables skip; both queries share one room", async () => {
  const { client, roomTs, fetchS, registrations, anomalies } = await makeClient();
  try {
    const where = {
      type: "simple",
      op: "=",
      left: { type: "column", name: "body" },
      right: { type: "literal", value: "kept" },
    };
    fetchS.queue.push(() => roomLease());
    // The second labeled query on the SAME room: same `note` spec (must NOT mint a second engine
    // table), plus a spec for a table the client schema doesn't hold (must SKIP, not throw). The
    // lease's row-local `where` rides the wire for the ROOM's own gate — the client ignores it
    // (302 §5: routing is declared; the room gate is the enforcement backstop).
    fetchS.queue.push(() =>
      roomLease({
        tables: [
          { table: "note", writable: { kind: "predicate", where, joinKeyCols: ["id"] } },
          { table: "ghost", writable: { kind: "predicate", where, joinKeyCols: [] } },
        ],
      }),
    );
    client.store.materialize(liveNotes());
    client.store.materialize(hotNotes());
    await settle();

    // Two leases ⇒ two registration calls, ONE record: the backend dedups per (sourceKey, table)
    // and skips the schema-unknown "ghost" table.
    assert.deepEqual(registrations, [[ROOM_KEY, ["note"]], [ROOM_KEY, ["note", "ghost"]]]);
    assert.deepEqual([...client.backend.roomTablesFor(ROOM_KEY)], [["note", `note@${ROOM_KEY}`]]);
    const inspect = client.__realtimeInspect();
    assert.deepEqual(Object.keys(inspect.rooms), [ROOM_KEY], "both queries share the one room source/gate");
    assert.equal(Object.keys(inspect.rooms[ROOM_KEY].queries).length, 2);
    const roomT = roomTs.get(ROOM_WS)!;
    assert.deepEqual(roomT.leaseSubscribes().map((s) => s.queryId), [RETAIN_BASE, RETAIN_BASE + 1]);
    assert.deepEqual(anomalies, []);
  } finally {
    client.close();
  }
});

test("302 §6 ownership: a context table (writable kind `none`) is NOT registered — the daemon stays its sole authority", async () => {
  const { client, fetchS, registrations } = await makeClient();
  try {
    fetchS.queue.push(() => docContextLease()); // note writable + acl context
    client.store.materialize(liveNotes());
    await settle();

    // Only the OWNED table registered; the context table never minted a namespaced twin.
    assert.deepEqual(registrations, [[ROOM_KEY, ["note"]]]);
    const record = client.backend.roomTablesFor(ROOM_KEY);
    assert.equal(record.has("acl"), false, "context is not room-owned");
    assert.deepEqual([...record], [["note", `note@${ROOM_KEY}`]]);
    // __realtimeInspect reads the wire→engine map back from the backend record (no shadow copy).
    assert.deepEqual(client.__realtimeInspect().rooms[ROOM_KEY].promoted, { note: `note@${ROOM_KEY}` });
  } finally {
    client.close();
  }
});

// ============================================================================================
// teardown
// ============================================================================================

test("destroy before the lease resolves: nothing attaches, nothing leaks", async () => {
  const { client, daemonT, roomTs, fetchS } = await makeClient();
  try {
    let resolveLease!: (v: unknown) => void;
    fetchS.queue.push(() => new Promise((r) => (resolveLease = r)));
    const view = client.store.materialize(liveNotes());
    view.destroy();
    resolveLease(roomLease());
    await settle();
    assert.equal(roomTs.size, 0, "no room transport for a destroyed view");
    assert.deepEqual(client.__realtimeInspect().rooms, {});
    assert.deepEqual(daemonT.leaseSubscribes(), [], "…and no daemon fallback either");
  } finally {
    client.close();
  }
});

test("destroy after attach releases the room retain and drops the renewal bookkeeping", async () => {
  const { client, roomTs, fetchS } = await makeClient();
  try {
    fetchS.queue.push(() => roomLease());
    const view = client.store.materialize(liveNotes());
    await settle();
    const roomT = roomTs.get(ROOM_WS)!;
    const released: number[] = [];
    const origRelease = client.backend.releaseRemoteQuery.bind(client.backend);
    client.backend.releaseRemoteQuery = (qid) => {
      released.push(qid);
      origRelease(qid);
    };
    view.destroy();
    assert.deepEqual(released, [RETAIN_BASE], "the remote retain rides the view's destroy");
    assert.deepEqual(client.__realtimeInspect().rooms[ROOM_KEY].queries, {}, "the room query state is gone");
    assert.deepEqual(roomT.sent.at(-1), { t: "unsubscribe", queryId: RETAIN_BASE });
  } finally {
    client.close();
  }
});

// ============================================================================================
// Slice I-iii — the §4 lifecycle system-stream plane: attach / dedup / re-resolution / release
// ============================================================================================

/** The four system tables' wire schemas (mirror of the daemon DDL — system-streams.ts). */
const sessionsTables = [{ name: "_rindle_scope_sessions", columns: ["scope", "client_id", "expires_at"], primaryKey: [0, 1] }];
const sessionsFp = normalizedFp(sessionsTables);
const sessionsHello = (queryId: number): ServerMsg => ({
  t: "nhello",
  queryId,
  hello: { epoch: 1, comparatorVersion: COMPARATOR_VERSION, tables: sessionsTables, normalizedFp: sessionsFp },
  bootId: "b1",
});
const sessionsBatch = (queryId: number, seq: number, rows: unknown[][], cv: number): ServerMsg => ({
  t: "nbatch",
  queryId,
  batch: {
    epoch: 1,
    seq,
    normalizedFp: sessionsFp,
    ops: rows.map((row) => ({ table: "_rindle_scope_sessions", op: "add", row }) as NormalizedOp),
    cv,
  },
});

const lifecycleBlock = (suffix = "") => ({
  doorbell: { table: "_rindle_scope_sessions", leaseToken: `lt-doorbell${suffix}`, scope: "doc/main" },
  fence: [
    { table: "_rindle_room_watermark", leaseToken: `lt-wm${suffix}`, doc: "doc/main" },
    { table: "_rindle_room_client_mutations", leaseToken: `lt-ledger${suffix}`, doc: "doc/main", clientId: "c1" },
    { table: "_rindle_room_mutation_outcomes", leaseToken: `lt-outcomes${suffix}`, doc: "doc/main", clientId: "c1" },
  ],
});

test("I-iii lifecycle block: system subs retained on the DAEMON channel with the minted tokens; idempotent across views; released with the LAST view", async () => {
  const { client, daemonT, roomTs, fetchS, anomalies } = await makeClient();
  try {
    fetchS.queue.push(() => ({ ...roomLease(), lifecycle: lifecycleBlock() }));
    const view1 = client.store.materialize(liveNotes());
    await settle();

    // The labeled query rode the ROOM channel; the four system subs rode the DAEMON channel,
    // each presenting its minted token (the handoff — one lease POST total).
    assert.equal(fetchS.calls.length, 1);
    const roomT = roomTs.get(ROOM_WS)!;
    assert.deepEqual(roomT.leaseSubscribes(), [{ queryId: RETAIN_BASE, leaseToken: "rt-1" }]);
    assert.deepEqual(daemonT.leaseSubscribes(), [
      { queryId: RETAIN_BASE + 1, leaseToken: "lt-doorbell" },
      { queryId: RETAIN_BASE + 2, leaseToken: "lt-wm" },
      { queryId: RETAIN_BASE + 3, leaseToken: "lt-ledger" },
      { queryId: RETAIN_BASE + 4, leaseToken: "lt-outcomes" },
    ]);

    // The doorbell stream is LIVE end-to-end: its hello (a system table unknown to the app
    // schema) passes CRIT#4 because the backend's expected set now carries the four system
    // schemas, and its released rows fold into the occupancy map — through the real
    // RemoteOptimisticSource + NormalizedSubscriber, not a scripted backend seam.
    daemonT.emit(sessionsHello(RETAIN_BASE + 1));
    daemonT.emit(sessionsBatch(RETAIN_BASE + 1, 0, [["doc/main", "c1", 1000], ["doc/main", "c2", 2000]], 1));
    assert.deepEqual(client.backend.__inspectDomains().lifecycle.scopeSessions, {}, "buffered ≠ folded");
    daemonT.emit(progressMsg(1));
    assert.deepEqual(client.backend.__inspectDomains().lifecycle.scopeSessions, {
      "doc/main": { c1: 1000, c2: 2000 },
    });

    // A SECOND labeled view on the same scope: its lease re-presents the same block — the claims
    // dedup per (table, scope/doc/clientId), so NO new system subscribes hit the wire.
    fetchS.queue.push(() => ({ ...roomLease(), lifecycle: lifecycleBlock() }));
    const view2 = client.store.materialize(hotNotes());
    await settle();
    assert.equal(daemonT.leaseSubscribes().length, 4, "no duplicate system subs for the second view");

    // The FIRST view's destroy leaves the plane up (the second still claims it)…
    view1.destroy();
    assert.equal(daemonT.sent.filter((m) => m.t === "unsubscribe").length, 0, "still held by view 2");
    // …and the LAST view's destroy releases all four system subs.
    view2.destroy();
    const unsubs = daemonT.sent.filter((m) => m.t === "unsubscribe").map((m) => (m as { queryId: number }).queryId);
    assert.deepEqual(unsubs.sort((a, b) => a - b), [RETAIN_BASE + 1, RETAIN_BASE + 2, RETAIN_BASE + 3, RETAIN_BASE + 4]);
    assert.deepEqual(
      client.backend.__inspectDomains().lifecycle.scopeSessions,
      { "doc/main": { c1: 1000, c2: 2000 } },
      "the folded occupancy state survives the release (monotone bookkeeping, not sub state)",
    );
    assert.deepEqual(anomalies, []);
  } finally {
    client.close();
  }
});

test("I-iii re-resolution: a daemon reconnect re-leases each system sub through its PARENT query and presents the FRESH minted token", async () => {
  const { client, daemonT, fetchS, anomalies } = await makeClient();
  try {
    fetchS.queue.push(() => ({ ...roomLease(), lifecycle: lifecycleBlock() }));
    client.store.materialize(liveNotes());
    await settle();
    assert.equal(daemonT.leaseSubscribes().length, 4);

    // The daemon ws drops and reconnects: the source re-subscribes everything it holds — the
    // lmid system query (name path, no POST) and the four lifecycle subs, EACH re-leasing the
    // parent labeled query (renewal-as-reauthorization; `_rindle/lifecycle` is never leaseable
    // by name) and picking ITS entry out of the fresh block.
    for (let i = 0; i < 4; i++) fetchS.queue.push(() => ({ ...roomLease(), lifecycle: lifecycleBlock("-2") }));
    daemonT.reconnect();
    await settle();
    assert.equal(fetchS.calls.length, 5, "one mint-time POST + four re-resolution POSTs");
    assert.ok(fetchS.calls.slice(1).every((c) => c.body.name === "liveNotes"), "every re-resolution leased the PARENT");
    assert.deepEqual(daemonT.leaseSubscribes().slice(4), [
      { queryId: RETAIN_BASE + 1, leaseToken: "lt-doorbell-2" },
      { queryId: RETAIN_BASE + 2, leaseToken: "lt-wm-2" },
      { queryId: RETAIN_BASE + 3, leaseToken: "lt-ledger-2" },
      { queryId: RETAIN_BASE + 4, leaseToken: "lt-outcomes-2" },
    ]);
    assert.deepEqual(anomalies, []);
  } finally {
    client.close();
  }
});

test("I-iii fail-open shape: a labeled-but-daemon-served lease carrying ONLY the doorbell attaches it; absent block stays byte-identical", async () => {
  const { client, daemonT, fetchS, anomalies } = await makeClient();
  try {
    // The doorbell-only shape (labeled, not room-served, lifecycle on server-side).
    fetchS.queue.push(() => ({
      leaseToken: "L-labeled",
      materializationId: "m1",
      lifecycle: { doorbell: { table: "_rindle_scope_sessions", leaseToken: "lt-doorbell", scope: "doc/main" } },
    }));
    const view = client.store.materialize(liveNotes());
    await settle();
    assert.deepEqual(daemonT.leaseSubscribes(), [
      { queryId: RETAIN_BASE, leaseToken: "L-labeled" },
      { queryId: RETAIN_BASE + 1, leaseToken: "lt-doorbell" },
    ]);
    view.destroy();

    // And WITHOUT a block (a pre-lifecycle server): exactly the one subscribe — zero new wire
    // traffic, the inert-until-fed pin at the client edge.
    fetchS.queue.push(() => ({ leaseToken: "L-plain", materializationId: "m2" }));
    client.store.materialize(hotNotes());
    await settle();
    assert.deepEqual(daemonT.leaseSubscribes().slice(2), [{ queryId: RETAIN_BASE + 2, leaseToken: "L-plain" }]);
    assert.deepEqual(anomalies, []);
  } finally {
    client.close();
  }
});

// ============================================================================================
// Slice I-iv — the doorbell reaction + the upgrade retarget (§4.1)
// ============================================================================================

/** Arbitrary session ops on the doorbell stream (the I-iii helper only builds adds). */
const sessionsOps = (queryId: number, seq: number, ops: NormalizedOp[], cv: number): ServerMsg => ({
  t: "nbatch",
  queryId,
  batch: { epoch: 1, seq, normalizedFp: sessionsFp, ops, cv },
});
const sess = (client: string, expiresAt: number, op: "add" | "remove" = "add"): NormalizedOp =>
  ({ table: "_rindle_scope_sessions", op, row: ["doc/main", client, expiresAt] }) as NormalizedOp;

/** The suppressed-solo lease shape: daemon-served + doorbell (the api-server's occupancy gate). */
const suppressedLease = (n = 1) => ({
  leaseToken: `L-solo-${n}`,
  materializationId: `m-${n}`,
  lifecycle: { doorbell: { table: "_rindle_scope_sessions", leaseToken: `lt-doorbell`, scope: "doc/main" } },
});

/** Collect a view's onChanges deliveries as [phase, opCount] plus the raw ops (the no-flicker /
 *  R3 observation surface — netted per commit, so a value-equal cutover delivers NOTHING here). */
function changeLog() {
  const log: { phase: string; ops: unknown[] }[] = [];
  const onChanges = (chs: unknown[], phase: string) => log.push({ phase, ops: chs });
  const batches = () => log.filter((e) => e.phase === "batch");
  return { log, onChanges, batches };
}

test("I-iv full upgrade lane: doorbell row → ONE re-lease → retarget onto the room; the view keeps its rows across the swap (no flicker)", async () => {
  const { client, daemonT, roomTs, fetchS, anomalies, registrations } = await makeClient();
  try {
    const chs = changeLog();
    fetchS.queue.push(() => suppressedLease());
    const view = client.store.materialize(liveNotes(), { onChanges: chs.onChanges as never });
    await settle();

    // Daemon-attached (the occupancy gate suppressed the room-serve) + the doorbell system sub.
    assert.deepEqual(daemonT.leaseSubscribes(), [
      { queryId: RETAIN_BASE, leaseToken: "L-solo-1" },
      { queryId: RETAIN_BASE + 1, leaseToken: "lt-doorbell" },
    ]);
    daemonT.emit(helloMsg(RETAIN_BASE));
    daemonT.emit(batchMsg(RETAIN_BASE, 0, [add(1, "a"), add(2, "b")], 1));
    daemonT.emit(progressMsg(1));
    assert.deepEqual(view.data.map((r) => [r.id, r.body]), [[1, "a"], [2, "b"]]);
    assert.equal(view.resultType, "complete");

    // The doorbell rings: ANOTHER clientID's unexpired session appears where there was none.
    // The re-lease responder is queued BEFORE the release (the event fires synchronously at it).
    fetchS.queue.push(() => ({ ...roomLease(), lifecycle: lifecycleBlock() }));
    daemonT.emit(sessionsHello(RETAIN_BASE + 1));
    daemonT.emit(sessionsOps(RETAIN_BASE + 1, 0, [sess("c2", Date.now() + 60_000)], 2));
    daemonT.emit(progressMsg(2));
    await settle();

    // Exactly ONE re-lease, for the parent labeled query.
    assert.equal(fetchS.calls.length, 2, "one doorbell ⇒ one re-lease");
    assert.equal(fetchS.calls[1].body.name, "liveNotes");
    // The retarget: the SAME wire qid moved wholesale — the room sub presents the roomToken, the
    // daemon got the unsubscribe, and the engine promoted per the lease's table specs.
    const roomT = roomTs.get(ROOM_WS);
    assert.ok(roomT, "the room transport opened at realtime.wsEndpoint");
    assert.deepEqual(roomT.leaseSubscribes(), [{ queryId: RETAIN_BASE, leaseToken: "rt-1" }]);
    assert.deepEqual(
      daemonT.sent.filter((m) => m.t === "unsubscribe").map((m) => (m as { queryId: number }).queryId),
      [RETAIN_BASE],
      "the daemon sub was released (the doorbell sub stays)",
    );
    assert.deepEqual(registrations, [[ROOM_KEY, ["note"]]]);
    // The re-lease's fence bundle claimed (the doorbell key dedups onto the view's live claim).
    assert.deepEqual(daemonT.leaseSubscribes().slice(2), [
      { queryId: RETAIN_BASE + 2, leaseToken: "lt-wm" },
      { queryId: RETAIN_BASE + 3, leaseToken: "lt-ledger" },
      { queryId: RETAIN_BASE + 4, leaseToken: "lt-outcomes" },
    ]);
    // Renewal bookkeeping: the query is room-attached now (sourceQid = the MOVED sub's qid).
    const inspect = client.__realtimeInspect();
    const qstates = Object.values(inspect.rooms[ROOM_KEY].queries);
    assert.deepEqual(qstates, [{ name: "liveNotes", sourceQid: RETAIN_BASE, exp: qstates[0].exp, refCount: 1 }]);

    // NO FLICKER, phase 1 (retarget → room hydration pending): the daemon rows are deliberately
    // NOT dropped — the view keeps serving them.
    assert.deepEqual(view.data.map((r) => [r.id, r.body]), [[1, "a"], [2, "b"]], "rows KEPT through the window");
    assert.equal(view.resultType, "complete", "…still server-authoritative");

    // NO FLICKER, phase 2: the room's seq-0 snapshot carries the SAME rows — they fold into the
    // namespaced room table, the view SWAPS onto it at the release tail (302 §4.1, an in-place
    // Store reset delivered as a snapshot phase), and the deferred old-channel GC drops the plain
    // rows the view no longer reads.
    roomT.emit(helloMsg(RETAIN_BASE));
    roomT.emit(batchMsg(RETAIN_BASE, 0, [add(1, "a"), add(2, "b")], 1));
    roomT.emit(progressMsg(1));
    assert.deepEqual(view.data.map((r) => [r.id, r.body]), [[1, "a"], [2, "b"]], "rows identical across the swap");
    assert.deepEqual(chs.batches(), [], "ZERO batch-phase deliveries — the cutover emitted nothing (netted per commit)");
    assert.equal(client.backend.__inspectDomains().swappedViews[view.qid], ROOM_KEY, "the view swapped onto the room table");
    // Gate isolation held: the room gate released; the daemon gate's cv timeline is its own.
    const gates = client.backend.__inspectDomains().gates;
    assert.equal(gates[ROOM_KEY].appliedCv, 1);
    assert.deepEqual(anomalies, []);
    view.destroy();
    assert.deepEqual(roomT.sent.at(-1), { t: "unsubscribe", queryId: RETAIN_BASE }, "teardown releases the ROOM sub");
  } finally {
    client.close();
  }
});

test("I-iv debounce + idempotence: a second doorbell mid-flight coalesces (no double attach); post-upgrade doorbells are inert", async () => {
  const { client, daemonT, roomTs, fetchS } = await makeClient();
  try {
    fetchS.queue.push(() => suppressedLease());
    client.store.materialize(liveNotes());
    await settle();
    daemonT.emit(helloMsg(RETAIN_BASE));
    daemonT.emit(batchMsg(RETAIN_BASE, 0, [add(1, "a")], 1));
    daemonT.emit(progressMsg(1));

    // A DELAYED upgrade lease: the first doorbell's re-lease hangs in flight…
    let resolveLease!: () => void;
    fetchS.queue.push(
      () =>
        new Promise((r) => {
          resolveLease = () => r({ ...roomLease(), lifecycle: lifecycleBlock() });
        }),
    );
    daemonT.emit(sessionsHello(RETAIN_BASE + 1));
    daemonT.emit(sessionsOps(RETAIN_BASE + 1, 0, [sess("c2", Date.now() + 60_000)], 2));
    daemonT.emit(progressMsg(2));
    await settle();
    assert.equal(fetchS.calls.length, 2, "the re-lease is in flight");

    // …a second 0→≥1 transition rings WHILE in flight: coalesced (the inFlight debounce), and
    // `retargetRemoteQuery` is idempotent per (query, sourceKey) besides.
    daemonT.emit(sessionsOps(RETAIN_BASE + 1, 1, [sess("c2", 0, "remove")], 3));
    daemonT.emit(progressMsg(3));
    daemonT.emit(sessionsOps(RETAIN_BASE + 1, 2, [sess("c2", Date.now() + 60_000)], 4));
    daemonT.emit(progressMsg(4));
    await settle();
    assert.equal(fetchS.calls.length, 2, "repeat doorbells coalesce into the in-flight re-lease");

    resolveLease();
    await settle();
    const roomT = roomTs.get(ROOM_WS)!;
    assert.deepEqual(roomT.leaseSubscribes(), [{ queryId: RETAIN_BASE, leaseToken: "rt-1" }], "ONE room sub — no double attach");

    // Post-upgrade doorbells find no candidate: zero new wire traffic.
    daemonT.emit(sessionsOps(RETAIN_BASE + 1, 3, [sess("c2", 0, "remove")], 5));
    daemonT.emit(progressMsg(5));
    daemonT.emit(sessionsOps(RETAIN_BASE + 1, 4, [sess("c2", Date.now() + 60_000)], 6));
    daemonT.emit(progressMsg(6));
    await settle();
    assert.equal(fetchS.calls.length, 2, "an upgraded query has no candidate left to ring — zero new POSTs");
  } finally {
    client.close();
  }
});

test("I-iv still-suppressed re-lease: a blockless reply is SILENT (suppression is designed, not an anomaly) and the NEXT doorbell is the retry", async () => {
  const { client, daemonT, roomTs, fetchS, anomalies } = await makeClient();
  try {
    fetchS.queue.push(() => suppressedLease());
    client.store.materialize(liveNotes());
    await settle();
    daemonT.emit(helloMsg(RETAIN_BASE));
    daemonT.emit(batchMsg(RETAIN_BASE, 0, [add(1, "a")], 1));
    daemonT.emit(progressMsg(1));

    // Doorbell #1 → the re-lease comes back STILL without a block (e.g. a higher minSessions).
    fetchS.queue.push(() => suppressedLease(2));
    daemonT.emit(sessionsHello(RETAIN_BASE + 1));
    daemonT.emit(sessionsOps(RETAIN_BASE + 1, 0, [sess("c2", Date.now() + 60_000)], 2));
    daemonT.emit(progressMsg(2));
    await settle();
    assert.equal(fetchS.calls.length, 2);
    assert.equal(roomTs.size, 0, "no room");
    assert.deepEqual(anomalies, [], "a suppressed re-lease is NOT an anomaly");
    assert.equal(daemonT.sent.filter((m) => m.t === "unsubscribe").length, 0, "still daemon-attached");

    // Doorbell #2 (a fresh 0→≥1 transition) retries; this time the gate opened.
    fetchS.queue.push(() => ({ ...roomLease(), lifecycle: lifecycleBlock() }));
    daemonT.emit(sessionsOps(RETAIN_BASE + 1, 1, [sess("c2", 0, "remove")], 3));
    daemonT.emit(progressMsg(3));
    daemonT.emit(sessionsOps(RETAIN_BASE + 1, 2, [sess("c2", Date.now() + 60_000)], 4));
    daemonT.emit(progressMsg(4));
    await settle();
    assert.equal(fetchS.calls.length, 3, "the next doorbell IS the retry");
    assert.deepEqual(roomTs.get(ROOM_WS)!.leaseSubscribes(), [{ queryId: RETAIN_BASE, leaseToken: "rt-1" }]);
  } finally {
    client.close();
  }
});

test("I-iv attach failure fails OPEN and LOUD: the daemon retain is untouched, no retry storm — the next doorbell retries", async () => {
  const { client, daemonT, roomTs, fetchS, anomalies } = await makeClient({
    transport: () => {
      throw new Error("room socket refused");
    },
  });
  void roomTs;
  try {
    fetchS.queue.push(() => suppressedLease());
    const view = client.store.materialize(liveNotes());
    await settle();
    daemonT.emit(helloMsg(RETAIN_BASE));
    daemonT.emit(batchMsg(RETAIN_BASE, 0, [add(1, "a")], 1));
    daemonT.emit(progressMsg(1));

    fetchS.queue.push(() => ({ ...roomLease(), lifecycle: lifecycleBlock() }));
    daemonT.emit(sessionsHello(RETAIN_BASE + 1));
    daemonT.emit(sessionsOps(RETAIN_BASE + 1, 0, [sess("c2", Date.now() + 60_000)], 2));
    daemonT.emit(progressMsg(2));
    await settle();

    // Loud: one anomaly (also console.error'd by contract). Open: the daemon sub never moved —
    // the retarget primitive validates before mutating, and ensureRoom threw before it ran.
    assert.equal(anomalies.length, 1);
    assert.equal(anomalies[0].kind, "lease-failed");
    assert.match(anomalies[0].message, /upgrade retarget failed/);
    assert.equal(daemonT.sent.filter((m) => m.t === "unsubscribe").length, 0, "still daemon-attached");
    assert.deepEqual(view.data.map((r) => [r.id, r.body]), [[1, "a"]], "the view never blinked");
    assert.equal(view.resultType, "complete");
    assert.equal(fetchS.calls.length, 2, "no retry storm — one POST per doorbell");

    // The NEXT doorbell is the retry (and fails loudly again under this broken transport).
    fetchS.queue.push(() => ({ ...roomLease(), lifecycle: lifecycleBlock() }));
    daemonT.emit(sessionsOps(RETAIN_BASE + 1, 1, [sess("c2", 0, "remove")], 3));
    daemonT.emit(progressMsg(3));
    daemonT.emit(sessionsOps(RETAIN_BASE + 1, 2, [sess("c2", Date.now() + 60_000)], 4));
    daemonT.emit(progressMsg(4));
    await settle();
    assert.equal(fetchS.calls.length, 3, "exactly one more POST");
    assert.equal(anomalies.length, 2);
  } finally {
    client.close();
  }
});

// The 302 twin of the old R3 forcing lane: an upgrade retarget while a DAEMON-domain pending
// mutation is in flight on the (now room-owned) table, with the room's seq-0 snapshot carrying
// the PRE-write image. Under one-source-per-table there is no merge and no pin — the swap shows
// the ROOM's copy, which does not carry the un-echoed daemon write:
//
//   - 302 §4.1 (the ACCEPTED FLASH, decision 2026-07-16): at swap-in the room's copy may be
//     behind what the daemon view showed; and
//   - 302 §5.1 (declaration fails SOFT): a daemon-domain write to a room-visible table gives no
//     optimistic feedback on the room-homed view until the room echoes it — degraded UX with an
//     obvious symptom, never a divergence, never a stuck pin.
//
//   The daemon confirm + the room's echo then converge the view to the written value.
test("302 §4.1/§5.1: retarget under a pending daemon write — the swap shows the room's pre-write image (accepted flash / soft mode); the confirm + echo converge it", async () => {
  const putNote = (tx: MutationTx, a: { id: number; body: string }) => tx.upsert("note", { id: a.id, body: a.body });
  const { client, daemonT, roomTs, fetchS, anomalies } = await makeClient({}, { mutators: { putNote } });
  try {
    const chs = changeLog();
    fetchS.queue.push(() => suppressedLease());
    const view = client.store.materialize(liveNotes(), { onChanges: chs.onChanges as never });
    await settle();
    daemonT.emit(helloMsg(RETAIN_BASE));
    daemonT.emit(batchMsg(RETAIN_BASE, 0, [add(1, "old")], 1));
    daemonT.emit(progressMsg(1));
    assert.deepEqual(view.data.map((r) => [r.id, r.body]), [[1, "old"]]);

    // The pending daemon write (no domainPolicy configured ⇒ "daemon", 302 §5 declared default).
    fetchS.queue.push(() => [{ accepted: true, rejected: false }]); // the mutate POST
    (client.mutate as unknown as { putNote: (a: { id: number; body: string }) => number }).putNote({ id: 1, body: "new" });
    await settle();
    assert.deepEqual(view.data.map((r) => [r.id, r.body]), [[1, "new"]], "prediction applied");
    assert.deepEqual(
      client.backend.__inspectDomains().pending.map((p) => [p.mid, p.domain]),
      [[1, "daemon"]],
      "a daemon-domain pending write",
    );

    // Doorbell → retarget onto the room. The view still reads the PLAIN table until the room
    // hydrates, so the prediction stays visible through the retarget itself.
    fetchS.queue.push(() => ({ ...roomLease(), lifecycle: lifecycleBlock() }));
    daemonT.emit(sessionsHello(RETAIN_BASE + 1));
    daemonT.emit(sessionsOps(RETAIN_BASE + 1, 0, [sess("c2", Date.now() + 60_000)], 2));
    daemonT.emit(progressMsg(2));
    await settle();
    const roomT = roomTs.get(ROOM_WS)!;
    assert.deepEqual(view.data.map((r) => [r.id, r.body]), [[1, "new"]], "prediction intact through the retarget");
    chs.log.length = 0;

    // The room's seq-0 snapshot carries the PRE-write image. The release folds it into the
    // namespaced room table and the view SWAPS onto it — showing "old": THE ACCEPTED FLASH
    // (302 §4.1) doubling as §5.1's soft mode (the daemon-domain write has no optimistic
    // feedback on the room table; its re-invocations keep staging onto the PLAIN table).
    roomT.emit(helloMsg(RETAIN_BASE));
    roomT.emit(batchMsg(RETAIN_BASE, 0, [add(1, "old")], 1));
    roomT.emit(progressMsg(1));
    assert.deepEqual(view.data.map((r) => [r.id, r.body]), [[1, "old"]], "the swap shows the room's copy — the accepted flash");
    assert.equal(client.backend.__inspectDomains().swappedViews[view.qid], ROOM_KEY, "the view swapped onto the room table");
    assert.deepEqual(
      client.backend.__inspectDomains().pending.map((p) => [p.mid, p.domain]),
      [[1, "daemon"]],
      "the pending write stays daemon-domain (an assigned mid pins its domain, §7.1)",
    );

    // Convergence: the DAEMON confirms mid 1 (the `__testRelease` seam — this lane's daemon
    // transport never established a qid-0 lmid hello) → the entry retires; the view (on the room
    // table) is unmoved. The room's ECHO of the committed write then lands the value.
    client.backend.__testRelease("daemon", [], 1);
    assert.deepEqual(client.backend.__inspectDomains().pending, [], "retired by the daemon watermark");
    assert.deepEqual(view.data.map((r) => [r.id, r.body]), [[1, "old"]], "still the room's copy — the echo has not landed");
    roomT.emit(batchMsg(RETAIN_BASE, 1, [{ table: "note", op: "edit", old: [1, "old"], new: [1, "new"] } as NormalizedOp], 2));
    roomT.emit(progressMsg(2));
    assert.deepEqual(view.data.map((r) => [r.id, r.body]), [[1, "new"]], "the room echo converges the view");
    assert.deepEqual(anomalies, []);
  } finally {
    client.close();
  }
});

// ============================================================================================
// Slice I-v — the §4.2 graceful downgrade dance (renewal-with-fence ⇒ retarget → demote → ghost)
// ============================================================================================

/** The §4.2 watermark fence stream's wire schema (`_rindle_room_watermark(doc, flush_seq)`). */
const wmTables = [{ name: "_rindle_room_watermark", columns: ["doc", "flush_seq"], primaryKey: [0] }];
const wmFp = normalizedFp(wmTables);
const wmHello = (queryId: number): ServerMsg => ({
  t: "nhello",
  queryId,
  hello: { epoch: 1, comparatorVersion: COMPARATOR_VERSION, tables: wmTables, normalizedFp: wmFp },
  bootId: "b1",
});
const wmBatch = (queryId: number, seq: number, doc: string, flushSeq: number, cv: number): ServerMsg => ({
  t: "nbatch",
  queryId,
  batch: {
    epoch: 1,
    seq,
    normalizedFp: wmFp,
    ops: [{ table: "_rindle_room_watermark", op: "add", row: [doc, flushSeq] } as NormalizedOp],
    cv,
  },
});

test("I-v full downgrade dance: a renewal WITH a fence retargets to the daemon behind a frozen ghost — no flicker; the daemon watermark drops the ghost; a doorbell re-upgrades", async () => {
  const { client, daemonT, roomTs, fetchS, anomalies } = await makeClient({ renewMarginMs: 200 });
  try {
    const chs = changeLog();
    // Room-served initial attach WITH the lifecycle fence bundle; short exp forces a renewal soon.
    fetchS.queue.push(() => ({ ...roomLease({ exp: Date.now() + 1_300 }), lifecycle: lifecycleBlock() }));
    const view = client.store.materialize(liveNotes(), { onChanges: chs.onChanges as never });
    await settle();
    const roomT = roomTs.get(ROOM_WS)!;
    roomT.emit(helloMsg(RETAIN_BASE));
    roomT.emit(batchMsg(RETAIN_BASE, 0, [add(1, "a"), add(2, "b")], 1));
    roomT.emit(progressMsg(1));
    assert.deepEqual(view.data.map((r) => [r.id, r.body]), [[1, "a"], [2, "b"]]);
    assert.equal(view.resultType, "complete");
    // The fence bundle subscribed on the DAEMON channel (I-iii): doorbell +1, wm +2, ledger +3, outcomes +4.
    assert.deepEqual(daemonT.leaseSubscribes(), [
      { queryId: RETAIN_BASE + 1, leaseToken: "lt-doorbell" },
      { queryId: RETAIN_BASE + 2, leaseToken: "lt-wm" },
      { queryId: RETAIN_BASE + 3, leaseToken: "lt-ledger" },
      { queryId: RETAIN_BASE + 4, leaseToken: "lt-outcomes" },
    ]);

    // The renewal fires and comes back DOWNGRADED: no realtime block, WITH a §4.2 fence (+ the
    // fence bundle re-minted so the streams stay re-resolvable). finalFlushSeq = 4.
    chs.log.length = 0;
    fetchS.queue.push(() => ({
      leaseToken: "L-daemon",
      materializationId: "m-dg",
      realtimeFence: { sourceKey: ROOM_KEY, doc: "doc/main", finalFlushSeq: 4 },
      lifecycle: lifecycleBlock(),
    }));
    await delay(1_500);

    // The dance: retarget → the room transport closed, a frozen ghost recorded, the sub moved to
    // the daemon presenting the fresh daemon token (NO extra POST — the driving query's handoff).
    assert.equal(roomT.closed, true, "the room transport closed");
    assert.deepEqual(Object.keys(client.backend.__inspectDomains().lifecycle.ghosts), [ROOM_KEY], "a frozen ghost recorded");
    assert.equal(fetchS.calls.length, 2, "one initial lease + one renewal — the daemon re-subscribe was handed a token");
    assert.deepEqual(
      daemonT.leaseSubscribes().find((s) => s.queryId === RETAIN_BASE),
      { queryId: RETAIN_BASE, leaseToken: "L-daemon" },
      "the query retargeted onto the daemon with the handed token",
    );
    assert.deepEqual(anomalies, [], "graceful — NOT the loud legacy downgrade");
    // NO FLICKER phase 1: the view still reads the room's (now frozen) namespaced table through
    // the daemon re-subscribe window — the 302 §4.2 swap-back gate.
    assert.deepEqual(view.data.map((r) => [r.id, r.body]), [[1, "a"], [2, "b"]], "rows KEPT (frozen room table)");
    assert.equal(view.resultType, "complete");

    // NO FLICKER phase 2: the daemon re-hydrates the SAME rows into the PLAIN table — which the
    // view does not read until the fence clears, so nothing is emitted.
    daemonT.emit(helloMsg(RETAIN_BASE));
    daemonT.emit(batchMsg(RETAIN_BASE, 0, [add(1, "a"), add(2, "b")], 1));
    daemonT.emit(progressMsg(1));
    assert.deepEqual(view.data.map((r) => [r.id, r.body]), [[1, "a"], [2, "b"]], "rows identical across the swap");
    assert.deepEqual(chs.batches(), [], "ZERO batch-phase deliveries — the cutover emitted nothing");

    // The daemon delivers the watermark ≥ the fence (4) ⇒ the ghost drops: the view SWAPS BACK
    // onto the plain table (302 §4.2 — value-equal, the daemon holds the same rows) and the room
    // table unregisters. No visible change.
    daemonT.emit(wmHello(RETAIN_BASE + 2));
    daemonT.emit(wmBatch(RETAIN_BASE + 2, 0, "doc/main", 4, 2));
    daemonT.emit(progressMsg(2));
    assert.equal(client.backend.__inspectDomains().lifecycle.roomWatermarks["doc/main"], 4, "the fence folded");
    assert.deepEqual(Object.keys(client.backend.__inspectDomains().lifecycle.ghosts), [], "ghost dropped at the fence");
    assert.deepEqual(view.data.map((r) => [r.id, r.body]), [[1, "a"], [2, "b"]], "value-equal drop — nothing changed");

    // The doorbell re-arms the upgrade: a collaborator re-appears ⇒ ONE re-lease ⇒ re-attach to the
    // room (the round trip). The room transport is fresh (the old one closed).
    fetchS.queue.push(() => ({ ...roomLease(), lifecycle: lifecycleBlock() }));
    daemonT.emit(sessionsHello(RETAIN_BASE + 1));
    daemonT.emit(sessionsOps(RETAIN_BASE + 1, 0, [sess("c2", Date.now() + 60_000)], 3));
    daemonT.emit(progressMsg(3));
    await settle();
    assert.equal(fetchS.calls.length, 3, "the doorbell drove exactly one re-lease");
    const roomT2 = roomTs.get(ROOM_WS)!;
    assert.notEqual(roomT2, roomT, "a fresh room transport opened");
    assert.deepEqual(roomT2.leaseSubscribes(), [{ queryId: RETAIN_BASE, leaseToken: "rt-1" }], "re-attached to the room");
    assert.deepEqual(anomalies, [], "the whole round trip was silent");
  } finally {
    client.close();
  }
});

// ============================================================================================
// 302 §6 — context stays daemon-owned. One authority per table: the room gate DROPS any context
// (or unknown) rows a room still relays; the daemon is the sole feed for `acl`. There is no
// merge, no priority tier, and no cross-channel dedupe — the overlap is structurally gone.
// ============================================================================================

test("302 §6: the gate DROPS a room's relayed context rows — the daemon's copy is never touched, the room's owned rows flow", async () => {
  const { client, daemonT, roomTs, fetchS, anomalies } = await makeClient();
  try {
    // (1) The daemon serves the GLOBAL acl list (unlabeled ⇒ the daemon path). Hydrate it with the
    // authoritative row.
    const aclChs = changeLog();
    fetchS.queue.push(() => ({ leaseToken: "L-acl", materializationId: "m-acl" }));
    const globalAcl = client.store.materialize(allAcl(), { onChanges: aclChs.onChanges as never });
    await settle();
    const aclQid = daemonT.leaseSubscribes().find((s) => s.leaseToken === "L-acl")!.queryId;
    daemonT.emit(aclHello(aclQid));
    daemonT.emit(aclBatch(aclQid, 0, [aclOp(10, "daemon-fresh")], 1));
    daemonT.emit(progressMsg(1));
    assert.deepEqual(globalAcl.data.map((r) => [r.id, r.principal]), [[10, "daemon-fresh"]]);

    // (2) A room-served DOC query whose lease names `note` (writable ⇒ owned) and `acl` (context,
    // `kind:"none"` ⇒ NOT registered — the daemon stays its sole authority).
    fetchS.queue.push(() => docContextLease());
    const docView = client.store.materialize(liveNotes());
    await settle();
    const roomT = roomTs.get(ROOM_WS)!;
    const docQid = roomT.leaseSubscribes().find((s) => s.leaseToken === "rt-1")!.queryId;
    assert.deepEqual([...client.backend.roomTablesFor(ROOM_KEY)], [["note", `note@${ROOM_KEY}`]], "only the owned table registered");

    // (3) The room relays a (stale) copy of the SAME context row beside its own note row — a shell
    // that still publishes context. The gate maps `note` into the room table and DROPS `acl`.
    roomT.emit(docHello(docQid));
    roomT.emit(docBatch(docQid, 0, [add(1, "a"), aclOp(10, "room-stale")], 1));
    roomT.emit(progressMsg(1));

    // The daemon's context row is untouched — the room's relayed copy never entered the store…
    assert.deepEqual(
      globalAcl.data.map((r) => [r.id, r.principal]),
      [[10, "daemon-fresh"]],
      "the daemon copy is never touched by the room relay",
    );
    // …and the acl view was never handed the room's value at all (no flicker, no phantom row).
    assert.ok(
      !JSON.stringify(aclChs.log).includes("room-stale"),
      "the room's relayed context copy was never delivered anywhere",
    );

    // The room-served doc query shows its own owned note row (swapped onto the room table).
    assert.deepEqual(docView.data.map((r) => [r.id, r.body]), [[1, "a"]]);
    // Gate isolation held: the room release moved only the room gate.
    const gates = client.backend.__inspectDomains().gates;
    assert.equal(gates.daemon.appliedCv, 1);
    assert.equal(gates[ROOM_KEY].appliedCv, 1);
    assert.deepEqual(anomalies, []);
  } finally {
    client.close();
  }
});
