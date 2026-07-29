// The /room-boot host (RINDLE-REALTIME-DESIGN.md §10.1; RINDLE-REALTIME-ENABLEMENT-DESIGN.md
// §3.1): the DO shell's cold-boot callback. Properties gated here: the route is DISABLED until
// the `realtime` block is set (the deprecated bare `authorizeRoom` never activates it); the
// default boot gate is the shell secret over `Authorization: Bearer`; the response carries the
// claimed epoch + the upstream footprint lease + the flush leg; and the default-minted flush
// credential round-trips through the default flush gate on the trio — mint and verify are the
// two ends of one credential.

import assert from "node:assert/strict";
import test from "node:test";

import { createSchema, newQueryBuilder, number, string, table } from "@rindle/client";
import type {
  ClaimRoomEpochInput,
  ClaimRoomEpochOutput,
  MaterializeInput,
  MaterializeOutput,
  RindleDaemonClient,
  RoomLmidsInput,
  RoomLmidsOutput,
  RowChangeTxnOutput,
} from "@rindle/daemon-client";

import {
  createRindleApiServer,
  mintRoomFlushCredential,
  RindleApiError,
  ROOM_FLUSH_CREDENTIAL_HEADER,
  verifyRoomFlushCredential,
  type RoomBootResponse,
} from "../src/index.ts";

const SECRET = "shell-secret-1";
const FOOTPRINT = { table: "issue" };

/** A daemon that answers the boot legs and records what reached it. */
function bootDaemon(affinity?: string) {
  const materialized: MaterializeInput[] = [];
  const claimed: string[] = [];
  const daemon = {
    materialized,
    claimed,
    async claimRoomEpoch(input: ClaimRoomEpochInput): Promise<ClaimRoomEpochOutput> {
      claimed.push(input.doc);
      return { epoch: 3 };
    },
    async materialize(input: MaterializeInput): Promise<MaterializeOutput> {
      materialized.push(input);
      return {
        materializationId: "m1",
        leaseToken: "upstream-lease",
        ...(affinity !== undefined ? { affinity } : {}),
      };
    },
    async roomLmids(input: RoomLmidsInput): Promise<RoomLmidsOutput> {
      return { lmids: Object.fromEntries(input.clients.map((c) => [c, 0])) };
    },
    async applyRowChangeTxn(): Promise<RowChangeTxnOutput> {
      return { applied: true, cv: 7 };
    },
    dematerialize: () => Promise.reject(new Error("unused")),
    executeSqlTxn: () => Promise.reject(new Error("unused")),
    executeSqlRead: () => Promise.reject(new Error("unused")),
    rejectMutation: () => Promise.reject(new Error("unused")),
    query: () => Promise.reject(new Error("unused")),
    migrate: () => Promise.reject(new Error("unused")),
  };
  return daemon as typeof daemon & RindleDaemonClient;
}

/** Fetch-style request: `headers.get`. */
const fetchRequest = (headers: Record<string, string>) => ({
  headers: { get: (n: string) => headers[n.toLowerCase()] ?? null },
});
/** node:http-style request: a lowercased header map. */
const nodeRequest = (headers: Record<string, string>) => ({ headers });

function realtimeApi(daemon = bootDaemon()) {
  const api = createRindleApiServer({
    daemon,
    realtime: {
      shellSecret: SECRET,
      resolveFootprint: (doc) => {
        if (doc !== "doc-1") throw new RindleApiError("not-found", `unknown doc: ${doc}`, 404);
        return FOOTPRINT as never;
      },
    },
  });
  return { api, daemon };
}

const bootBody = { doc: "doc-1", instance: "do-abc" };
const bootCtx = { user: null, request: fetchRequest({ authorization: `Bearer ${SECRET}` }) };

test("room-boot is disabled until `realtime` is configured — authorizeRoom alone never activates it", async () => {
  const api = createRindleApiServer({ daemon: bootDaemon(), authorizeRoom: () => true });
  await assert.rejects(
    api.handleRoomBootJson(bootBody, { user: null }),
    (e: unknown) =>
      e instanceof RindleApiError && e.status === 403 && /realtime not configured/.test(e.message),
  );
});

test("the default boot gate is the shell secret, on both request shapes", async () => {
  const { api } = realtimeApi();
  // no request at all
  await assert.rejects(
    api.handleRoomBootJson(bootBody, { user: null }),
    (e: unknown) => e instanceof RindleApiError && e.status === 403,
  );
  // wrong secret
  await assert.rejects(
    api.handleRoomBootJson(bootBody, {
      user: null,
      request: fetchRequest({ authorization: "Bearer nope" }),
    }),
    (e: unknown) => e instanceof RindleApiError && e.status === 403,
  );
  // right secret — Fetch-style AND node-style
  const viaFetch = await api.handleRoomBootJson(bootBody, bootCtx);
  assert.equal(viaFetch.status, 200);
  const viaNode = await api.handleRoomBootJson(bootBody, {
    user: null,
    request: nodeRequest({ authorization: `Bearer ${SECRET}` }),
  });
  assert.equal(viaNode.status, 200);
});

test("a boot claims the epoch, mints the upstream lease, and returns the flush leg", async () => {
  const { api, daemon } = realtimeApi();
  const out = await api.handleRoomBootJson(bootBody, bootCtx);
  assert.equal(out.status, 200);
  const body = out.body as RoomBootResponse;

  assert.deepEqual(daemon.claimed, ["doc-1"], "the placement epoch was claimed for the doc");
  assert.equal(body.epoch, 3);
  assert.equal(body.upstreamLeaseToken, "upstream-lease");
  // With no configured `realtime.upstreamWsEndpoint`, the boot carries none (the room shell uses its
  // own default upstream); the static-override case is covered below.
  assert.equal(body.upstreamWsEndpoint, undefined);
  assert.equal(body.upstreamAffinity, undefined, "affinity-off daemon keeps the boot wire unchanged");

  // the lease is the ROOM's: the resolved footprint, on the normalized protocol
  assert.equal(daemon.materialized.length, 1);
  assert.equal(daemon.materialized[0].ast, FOOTPRINT);
  assert.equal(daemon.materialized[0].mode, "normalized");

  // the flush leg: route PATHS (resolved against the boot origin) + the default credential
  assert.deepEqual(body.flush.urls, {
    apply: "/api/rindle/apply-row-change-txn",
    claim: "/api/rindle/claim-room-epoch",
    lmids: "/api/rindle/room-lmids",
  });
  const credential = body.flush.headers[ROOM_FLUSH_CREDENTIAL_HEADER];
  assert.match(credential, /^rfc1\./);
  const payload = await verifyRoomFlushCredential(credential, SECRET);
  assert.equal(payload.doc, "doc-1");
  assert.equal(payload.epoch, 3);
});

test("an unknown doc surfaces resolveFootprint's 404", async () => {
  const { api } = realtimeApi();
  await assert.rejects(
    api.handleRoomBootJson({ doc: "doc-9" }, bootCtx),
    (e: unknown) => e instanceof RindleApiError && e.status === 404,
  );
});

test("the boot-minted credential passes the default flush gate; tampered or absent is refused", async () => {
  const { api } = realtimeApi();
  const boot = await api.handleRoomBootJson(bootBody, bootCtx);
  const headers = (boot.body as RoomBootResponse).flush.headers;
  const txn = {
    source: "room:doc-1:3",
    offset: "00000000000000000001",
    doc: "doc-1",
    epoch: 3,
    changes: [{ table: "issue", op: "add", row: [1, "a"] }],
  };

  const ok = await api.handleApplyRowChangeTxnJson(txn, { user: null, request: nodeRequest(headers) });
  assert.deepEqual(ok, { status: 200, body: { applied: true, cv: 7 } });

  const credential = headers[ROOM_FLUSH_CREDENTIAL_HEADER];
  const tampered = credential.slice(0, -2) + (credential.endsWith("A") ? "BB" : "AA");
  await assert.rejects(
    api.handleApplyRowChangeTxnJson(txn, {
      user: null,
      request: nodeRequest({ [ROOM_FLUSH_CREDENTIAL_HEADER]: tampered }),
    }),
    (e: unknown) => e instanceof RindleApiError && e.status === 403 && /refused/.test(e.message),
  );
  await assert.rejects(
    api.handleApplyRowChangeTxnJson(txn, { user: null }),
    (e: unknown) =>
      e instanceof RindleApiError &&
      e.status === 403 &&
      e.message.includes(ROOM_FLUSH_CREDENTIAL_HEADER),
  );
});

test("realtime.authorize overrides the default flush gate; the deprecated authorizeRoom loses to realtime", async () => {
  const seen: unknown[] = [];
  const api = createRindleApiServer<string>({
    daemon: bootDaemon(),
    // would ACCEPT everything — must lose to the realtime block below
    authorizeRoom: () => true,
    realtime: {
      shellSecret: SECRET,
      resolveFootprint: () => FOOTPRINT as never,
      authorize: ({ user }) => {
        seen.push(user);
        return user === "placed-room";
      },
    },
  });
  await assert.rejects(
    api.handleClaimRoomEpochJson({ doc: "doc-1" }, { user: "intruder" }),
    (e: unknown) => e instanceof RindleApiError && e.status === 403,
  );
  const out = await api.handleClaimRoomEpochJson({ doc: "doc-1" }, { user: "placed-room" });
  assert.deepEqual(out, { status: 200, body: { epoch: 3 } });
  assert.deepEqual(seen, ["intruder", "placed-room"], "the custom authorizer gated, not the default");
});

test("custom routes and upstream overrides ride the boot response; mintFlushHeaders overrides the credential", async () => {
  const daemon = bootDaemon("aff.room.follower");
  const api = createRindleApiServer({
    daemon,
    routes: { applyRowChangeTxn: "/x/apply", claimRoomEpoch: "/x/claim", roomLmids: "/x/lmids" },
    realtime: {
      shellSecret: SECRET,
      resolveFootprint: () => FOOTPRINT as never,
      upstreamLeaseTtlMs: 5_000,
      upstreamWsEndpoint: "ws://static-upstream",
      mintFlushHeaders: ({ doc, epoch }) => ({ "x-app-room": `${doc}:${epoch}` }),
    },
  });
  const out = await api.handleRoomBootJson(bootBody, bootCtx);
  const body = out.body as RoomBootResponse;
  assert.deepEqual(body.flush.urls, { apply: "/x/apply", claim: "/x/claim", lmids: "/x/lmids" });
  assert.deepEqual(body.flush.headers, { "x-app-room": "doc-1:3" });
  assert.equal(body.upstreamWsEndpoint, "ws://static-upstream", "the static override wins");
  assert.equal(body.upstreamAffinity, "aff.room.follower", "the static fleet endpoint is pinned by the lease's ticket");
  assert.equal(daemon.materialized[0].leaseTtlMs, 5_000);
});

test("a daemon without claimRoomEpoch is refused loudly", async () => {
  const daemon = bootDaemon();
  (daemon as { claimRoomEpoch?: unknown }).claimRoomEpoch = undefined;
  const api = createRindleApiServer({
    daemon,
    realtime: { shellSecret: SECRET, resolveFootprint: () => FOOTPRINT as never },
  });
  await assert.rejects(api.handleRoomBootJson(bootBody, bootCtx), /does not implement claimRoomEpoch/);
});

test("mint/verify are one credential: a foreign secret is refused", async () => {
  const credential = await mintRoomFlushCredential({ shellSecret: SECRET, doc: "d", epoch: 1 });
  await assert.rejects(verifyRoomFlushCredential(credential, "other-secret"), /bad signature/);
});

// ------------------------------------------------------------------ boot scope specs (H-iv-b)

test("a named-profile boot ships compiled scopes: writable split + joinKeyCols + footprintWhere (context tables included)", async () => {
  const deck = table("deck").columns({ id: string(), title: string() }).primaryKey("id");
  const slide = table("slide").columns({ id: string(), deckId: string(), rank: number() }).primaryKey("id");
  const member = table("member")
    .columns({ id: string(), deckId: string(), active: number() })
    .primaryKey("id");
  const schema = createSchema({ tables: [deck, slide, member] });
  const q = newQueryBuilder(schema);

  const api = createRindleApiServer({
    daemon: bootDaemon(),
    schema,
    realtime: {
      shellSecret: SECRET,
      rooms: {
        document: {
          key: (a: { docId: string }) => `doc:${a.docId}`,
          footprint: (docKey: string) =>
            q.deck
              .where.id(docKey)
              .sub("slides", slide, { parent: ["id"], child: ["deckId"] })
              // The context table carries its OWN row-local predicate — footprintWhere
              // must ship for it too (the gate proves absent reads on readable tables).
              .sub("members", member, { parent: ["id"], child: ["deckId"] }, (mq) => mq.where.active(1)),
          context: ["member"],
        },
      },
    },
  });

  const out = await api.handleRoomBootJson({ doc: "document/doc:d1" }, bootCtx);
  assert.equal(out.status, 200);
  const body = out.body as RoomBootResponse;
  const eq = (name: string, value: unknown) => ({
    type: "simple",
    op: "=",
    left: { type: "column", name },
    right: { type: "literal", value },
  });
  assert.deepEqual(body.scopes, [
    {
      table: "deck",
      // The ROOT node with a lossless row-local where ⇒ the EXACT membership predicate ships.
      footprintWhere: eq("id", "doc:d1"),
      writable: { kind: "predicate", where: eq("id", "doc:d1"), joinKeyCols: ["id"] },
    },
    // Context table — but a CHILD node: its implicit parent correlation is a dropped membership
    // constraint, so footprintWhere must NOT ship (a widened membership predicate would let the
    // gate over-claim "absent in room = absent in truth"); its absent reads fail closed. The
    // node's own row-local where still exists but is NOT the membership predicate.
    { table: "member", writable: { kind: "none" } },
    // No row-local predicate on the slide node (and a child anyway) ⇒ footprintWhere ABSENT
    // (its absent reads fail closed to a deopt room-side) — never fabricated.
    { table: "slide", writable: { kind: "predicate", joinKeyCols: ["deckId"] } },
  ]);
});

test("a legacy resolveFootprint doc ships scopes too — compiled with an empty context set", async () => {
  // The default fixture's footprint has no where at all: one all-writable table. An EXACT but
  // unconstrained root means the WHOLE table rides the footprint — membership is the vacuous-true
  // empty AND (the gate's combinator pins it true), so absent reads on it stay provable.
  const { api } = realtimeApi();
  const bare = (await api.handleRoomBootJson(bootBody, bootCtx)).body as RoomBootResponse;
  assert.deepEqual(bare.scopes, [
    {
      table: "issue",
      footprintWhere: { type: "and", conditions: [] },
      writable: { kind: "predicate", joinKeyCols: [] },
    },
  ]);

  // A legacy footprint WITH a row-local where ships it on both halves of the spec.
  const where = {
    type: "simple",
    op: "<",
    left: { type: "column", name: "id" },
    right: { type: "literal", value: 100 },
  };
  const api2 = createRindleApiServer({
    daemon: bootDaemon(),
    realtime: {
      shellSecret: SECRET,
      resolveFootprint: () => ({ table: "issue", where }) as never,
    },
  });
  const scoped = (await api2.handleRoomBootJson(bootBody, bootCtx)).body as RoomBootResponse;
  assert.deepEqual(scoped.scopes, [
    { table: "issue", footprintWhere: where, writable: { kind: "predicate", where, joinKeyCols: [] } },
  ]);
});
