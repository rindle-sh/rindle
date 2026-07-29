// The room-serve DECISION (RINDLE-REALTIME-QUERY-ENABLEMENT §2.1 lease-flow steps 2–5, 302 §5): a
// realtime-labeled named query naming a configured room profile gains the lease's `realtime` block
// (sourceKey/wsEndpoint/roomToken/exp/doc/tables) — room serving is DECLARED, not proven (no
// coverage check). Everything else — unlabeled, aggregate-shaped, or missing serving wiring —
// FAILS OPEN to the plain daemon lease, byte-identically, with a one-time (query, profile)
// diagnostic. The daemon here is a FAKE; these tests gate the DECISION layer.

import assert from "node:assert/strict";
import { test } from "node:test";

import type { Ast, Condition } from "@rindle/client";
import { createSchema, defineQuery, newQueryBuilder, number, string, table } from "@rindle/client";
import type {
  MaterializeInput,
  MaterializeOutput,
  RindleDaemonClient,
} from "@rindle/daemon-client";
import { verifyRoomToken } from "@rindle/room/token";

import {
  createRindleApiServer,
  registerQueries,
  type ApiQuery,
  type QueryLeaseResponse,
  type RindleApiServerOptions,
} from "../src/index.ts";
import { attachRealtimeLabel, compileRoomScopeSpecs, compileRoomTableSpecs } from "../src/rooms.ts";

// ------------------------------------------------------------------------------- the fixture

const deck = table("deck").columns({ id: string(), title: string() }).primaryKey("id");
const slide = table("slide").columns({ id: string(), deckId: string(), rank: number() }).primaryKey("id");
const member = table("member").columns({ id: string(), deckId: string() }).primaryKey("id");
const schema = createSchema({ tables: [deck, slide, member] });
const q = newQueryBuilder(schema);

/** The canonical unwindowed document footprint: the deck row, its slides, its members. */
const documentProfile = {
  key: (a: { docId: string }) => `doc:${a.docId}`,
  footprint: (docKey: string) =>
    q.deck
      .where.id(docKey)
      .sub("slides", slide, { parent: ["id"], child: ["deckId"] })
      .sub("members", member, { parent: ["id"], child: ["deckId"] }),
  context: ["member"],
};

/** A labeled query (§2.1): the deck canvas, windowed — the window is the query's, never the
 *  footprint's. Its args map to the profile's key args. */
const deckCanvas = defineQuery(
  "deckCanvas",
  (a: { deckId: string }) => q.slide.where.deckId(a.deckId).orderBy("rank", "asc").limit(10),
  { realtime: { room: "document", args: (a: { deckId: string }) => ({ docId: a.deckId }) } },
);

/** An unlabeled query — the legacy path that must stay byte-identical. */
const plainSlides = defineQuery("plainSlides", () => q.slide.orderBy("id", "asc"));

/** A fake daemon: just the lease surface. Room serving no longer consults it — the decision is
 *  DECLARED (302 §5), so there is no `coverQuery` here. */
class FakeDaemon implements RindleDaemonClient {
  materialized: MaterializeInput[] = [];

  async materialize(input: MaterializeInput): Promise<MaterializeOutput> {
    this.materialized.push(input);
    return {
      materializationId: `mat-${this.materialized.length}`,
      leaseToken: `lease-${this.materialized.length}`,
      queryKey: "qk-1",
      reused: false,
    };
  }

  async dematerialize() {
    return { removed: true };
  }
  async executeSqlTxn() {
    return { cv: 1 };
  }
  async executeSqlRead() {
    return { cols: [], rows: [] };
  }
  async applyRowChangeTxn() {
    return { cv: 1 };
  }
  async rejectMutation() {
    return {};
  }
  async query() {
    return { rows: [] };
  }
  async migrate() {
    return { applied: true };
  }
}

const TOKEN_KEY = { kid: "k1", secret: "room-token-secret" };

function makeApi(
  daemon: FakeDaemon,
  overrides?: {
    realtime?: Partial<RindleApiServerOptions<string>["realtime"] & object>;
    queries?: Record<string, ApiQuery<string, any>>;
    warnings?: string[];
  },
) {
  const warnings = overrides?.warnings ?? [];
  return {
    warnings,
    api: createRindleApiServer<string>({
      daemon,
      schema,
      queries: overrides?.queries ?? registerQueries<string>([deckCanvas, plainSlides]),
      subject: ({ user }) => (user ? `user:${user}` : undefined),
      realtime: {
        shellSecret: "shell-secret",
        rooms: { document: documentProfile },
        locateRoom: (doc: string) => ({ wsEndpoint: `ws://rooms.example/${doc}` }),
        roomTokenKey: TOKEN_KEY,
        warn: (m: string) => warnings.push(m),
        ...(overrides?.realtime as object),
      },
    }),
  };
}

const lease = (api: { createQueryLease(i: any): Promise<QueryLeaseResponse> }, name: string, args: unknown) =>
  api.createQueryLease({ user: "u1", name, args });

// -------------------------------------------------------------------- labeled ⇒ realtime block

test("a labeled + wired lease gains the realtime block: sourceKey/doc/token/tables; top-level untouched", async () => {
  const daemon = new FakeDaemon();
  const { api, warnings } = makeApi(daemon);

  const res = await lease(api, "deckCanvas", { deckId: "d1" });

  // Top-level fields are EXACTLY the daemon lease's — the block only ever adds a key.
  assert.equal(res.leaseToken, "lease-1");
  assert.equal(res.materializationId, "mat-1");

  const rt = res.realtime;
  assert.ok(rt, "a labeled + wired query ⇒ the realtime block is present");
  // The wire room doc is minted server-side from the label's args mapping + the profile key;
  // the sourceKey convention is `room:` + doc (anything ≠ "daemon" parses as a room source).
  assert.equal(rt.doc, "document/doc:d1");
  assert.equal(rt.sourceKey, "room:document/doc:d1");
  // The room endpoint rides the DEDICATED field, from locateRoom.
  assert.equal(rt.wsEndpoint, "ws://rooms.example/document/doc:d1");
  assert.ok(rt.exp > Date.now(), "exp is a future ms-epoch");

  // The token verifies against the room shell's plumbing (same doc + kid→secret ring) and seals
  // the APPROVED resolved AST + the caller's subject.
  const payload = await verifyRoomToken(rt.roomToken, {
    doc: rt.doc,
    keys: { [TOKEN_KEY.kid]: TOKEN_KEY.secret },
  });
  assert.equal(payload.sub, "user:u1");
  assert.equal(payload.exp, rt.exp);
  assert.deepEqual(payload.ast, deckCanvas.resolve({ deckId: "d1" }).ast());

  // RoomTableSpec compilation: context ⇒ none; writable ⇒ row-local predicate + join keys. Since
  // H-iii the lease specs ALSO carry footprintWhere — exact-root-only: the deck ROOT's lossless
  // `id = doc:d1` ships; the slide/member CHILD nodes get none (the implicit parent correlation
  // is a dropped membership constraint).
  const deckWhere = {
    type: "simple",
    op: "=",
    left: { type: "column", name: "id" },
    right: { type: "literal", value: "doc:d1" },
  };
  assert.deepEqual(rt.tables, [
    {
      table: "deck",
      footprintWhere: deckWhere,
      writable: {
        kind: "predicate",
        where: deckWhere,
        joinKeyCols: ["id"],
      },
    },
    { table: "member", writable: { kind: "none" } },
    // No row-local predicate on the slide node ⇒ `where` absent: every held row is writable.
    { table: "slide", writable: { kind: "predicate", joinKeyCols: ["deckId"] } },
  ]);

  assert.deepEqual(warnings, [], "a room-served lease warns nothing");
});

test("H-iii: the LEASE carries footprintWhere exactly where the BOOT wire does — one compiler, two wires", async () => {
  const daemon = new FakeDaemon();
  const { api } = makeApi(daemon);
  const res = await lease(api, "deckCanvas", { deckId: "d1" });
  assert.ok(res.realtime);
  // The lease's table specs are EXACTLY compileRoomScopeSpecs' output — the same objects the boot
  // wire ships the room gate (H-iv-b), so the client router and the room gate can never disagree
  // about the membership predicate.
  assert.deepEqual(
    res.realtime.tables,
    compileRoomScopeSpecs(documentProfile.footprint("doc:d1").ast(), new Set(["member"])),
  );
  // footprintWhere ships ONLY for the exact footprint root (deck); the child tables carry none.
  assert.deepEqual(
    res.realtime.tables.map((t) => [t.table, "footprintWhere" in t]),
    [["deck", true], ["member", false], ["slide", false]],
  );
});

test("an unlabeled lease is byte-identical legacy — no realtime key at all", async () => {
  const daemon = new FakeDaemon();
  const { api } = makeApi(daemon);
  const res = await lease(api, "plainSlides", null);
  assert.equal("realtime" in res, false, "the key is ABSENT, not undefined-valued");
  assert.deepEqual(res, {
    leaseToken: "lease-1",
    materializationId: "mat-1",
    queryKey: "qk-1",
    reused: false,
  });
});

// ------------------------------------------------------------------- fail-open + warn-once

test("aggregate/reduce-shaped labeled queries are refused room-serving", async () => {
  const daemon = new FakeDaemon();
  const aggregateQuery: ApiQuery<string, unknown> = () =>
    ({
      table: "slide",
      aggregate: "count",
      groupBy: ["deckId"],
      where: {
        type: "simple",
        op: "=",
        left: { type: "column", name: "deckId" },
        right: { type: "literal", value: "d1" },
      },
    }) as Ast;
  const { api, warnings } = makeApi(daemon, {
    queries: {
      slideCounts: attachRealtimeLabel(aggregateQuery, {
        room: "document",
        args: () => ({ docId: "d1" }),
      }),
    },
  });

  const res = await lease(api, "slideCounts", null);
  assert.equal(res.realtime, undefined, "aggregates never room-serve (the room gate drops __agg rows)");
  assert.equal(res.leaseToken, "lease-1");
  assert.equal(warnings.length, 1);
  assert.match(warnings[0], /aggregate\/reduce/);
});

test("locateRoom absent ⇒ room-serving is off: fail-open + warn-once", async () => {
  const daemon = new FakeDaemon();
  const { api, warnings } = makeApi(daemon, { realtime: { locateRoom: undefined } });
  const res = await lease(api, "deckCanvas", { deckId: "d1" });
  assert.equal(res.realtime, undefined);
  assert.equal(res.leaseToken, "lease-1");
  assert.equal(warnings.length, 1);
  assert.match(warnings[0], /locateRoom is not configured/);
});

test("no token subject (no subject resolver hit, no clientId) ⇒ fail-open + warn-once", async () => {
  const daemon = new FakeDaemon();
  const { api, warnings } = makeApi(daemon);
  // subject resolves undefined for an empty user; no clientId on the request.
  const res = await api.createQueryLease({ user: "", name: "deckCanvas", args: { deckId: "d1" } });
  assert.equal(res.realtime, undefined);
  assert.equal(warnings.length, 1);
  assert.match(warnings[0], /no token subject/);
});

// ------------------------------------------------------------- wsEndpoint non-migration guard

test("a room-served lease's room endpoint rides ONLY the realtime block (its own connection)", async () => {
  // The daemon session's ws host is fixed + placed by the affinity ticket; room-serving must never
  // touch it — the room opens a SEPARATE connection at `realtime.wsEndpoint`.
  const daemon = new FakeDaemon();
  const { api } = makeApi(daemon);

  const roomServed = await lease(api, "deckCanvas", { deckId: "d1" });

  assert.ok(roomServed.realtime);
  assert.equal(roomServed.realtime.wsEndpoint, "ws://rooms.example/document/doc:d1");
  // The top-level lease carries no ws endpoint at all — there is nothing for the room block to leak into.
  assert.equal("wsEndpoint" in roomServed, false);
});

// ------------------------------------------------------------------- RoomTableSpec extraction

test("compileRoomTableSpecs: AND keeps row-local conjuncts, drops EXISTS; partial ORs drop whole; multi-node tables OR", () => {
  const docWhere = (value: string) =>
    ({
      type: "simple",
      op: "=",
      left: { type: "column", name: "deckId" },
      right: { type: "literal", value },
    }) as const;
  const existsCond: Condition = {
    type: "correlatedSubquery",
    op: "EXISTS",
    related: {
      correlation: { parentField: ["deckId"], childField: ["id"] },
      subquery: { table: "deck" },
    },
  };

  // AND(docWhere, EXISTS): the EXISTS conjunct drops (widening — sound), the row-local stays.
  const andFootprint: Ast = {
    table: "slide",
    where: { type: "and", conditions: [docWhere("d1"), existsCond] },
  };
  const andSpecs = compileRoomTableSpecs(andFootprint, new Set(["deck"]));
  assert.deepEqual(andSpecs, [
    { table: "deck", writable: { kind: "none" } },
    {
      table: "slide",
      writable: { kind: "predicate", where: docWhere("d1"), joinKeyCols: ["deckId"] },
    },
  ]);

  // OR(docWhere, EXISTS): keeping only some disjuncts would NARROW — the whole OR drops.
  const orFootprint: Ast = {
    table: "slide",
    where: { type: "or", conditions: [docWhere("d1"), existsCond] },
  };
  const orSpecs = compileRoomTableSpecs(orFootprint, new Set(["deck"]));
  assert.deepEqual(orSpecs[1], {
    table: "slide",
    writable: { kind: "predicate", joinKeyCols: ["deckId"] },
  });

  // The same table read at TWO nodes (root + a related child): the row sets union ⇒ OR.
  const twoNodeFootprint: Ast = {
    table: "slide",
    where: docWhere("d1"),
    related: [
      {
        correlation: { parentField: ["deckId"], childField: ["deckId"] },
        subquery: { table: "slide", alias: "siblings", where: docWhere("d2") },
      },
    ],
  };
  const twoNode = compileRoomTableSpecs(twoNodeFootprint, new Set());
  assert.deepEqual(twoNode, [
    {
      table: "slide",
      writable: {
        kind: "predicate",
        where: { type: "or", conditions: [docWhere("d1"), docWhere("d2")] },
        joinKeyCols: ["deckId"],
      },
    },
  ]);
});

test("compileRoomScopeSpecs (H-iv-b): footprintWhere on every row-locally-constrained table — context ones included; compileRoomTableSpecs IS it since H-iii", () => {
  const rootWhere: Condition = {
    type: "simple",
    op: "=",
    left: { type: "column", name: "deckId" },
    right: { type: "literal", value: "d1" },
  };
  const memberWhere: Condition = {
    type: "simple",
    op: "=",
    left: { type: "column", name: "active" },
    right: { type: "literal", value: 1 },
  };
  const footprint: Ast = {
    table: "slide",
    where: rootWhere,
    related: [
      {
        correlation: { parentField: ["deckId"], childField: ["deckId"] },
        subquery: { table: "member", alias: "members", where: memberWhere },
      },
      {
        correlation: { parentField: ["id"], childField: ["slideId"] },
        subquery: { table: "note", alias: "notes" },
      },
    ],
  };
  const scopes = compileRoomScopeSpecs(footprint, new Set(["member"]));
  assert.deepEqual(scopes, [
    // Context table AND a child node: its own row-local where is NOT its membership predicate
    // (the implicit parent correlation is a dropped constraint), so footprintWhere must not
    // ship — a widened membership predicate would let the gate's absent-read proof over-claim.
    // Its absent reads fail closed to a deopt.
    { table: "member", writable: { kind: "none" } },
    // No row-local predicate (and a child anyway) ⇒ footprintWhere ABSENT (never fabricated):
    // absent reads on `note` fail closed to a deopt.
    { table: "note", writable: { kind: "predicate", joinKeyCols: ["slideId"] } },
    {
      // The ROOT node with a lossless extraction: the EXACT membership predicate ships.
      table: "slide",
      footprintWhere: rootWhere,
      writable: { kind: "predicate", where: rootWhere, joinKeyCols: ["deckId", "id"] },
    },
  ]);
  // ONE compiler, two wires: since H-iii the lease's RoomTableSpec[] carries footprintWhere too —
  // the two outputs are IDENTICAL (H-iv-b shipped the boot wire first; H-iii flipped the lease).
  assert.deepEqual(compileRoomTableSpecs(footprint, new Set(["member"])), scopes);
});
