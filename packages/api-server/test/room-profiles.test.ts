// Named room profiles + query labels (RINDLE-REALTIME-QUERY-ENABLEMENT §2, slice G-iv-a) — the
// DECLARATION layer. Properties gated here: profiles compile with statically-derived footprint
// facts (tables / writable scope / per-table join keys); every §2.3 "loud at registration" rule
// throws AT CONSTRUCTION with a pointed message (windowed footprint → offending AST path; context
// table missing from schema or footprint; label naming a missing profile); footprints that can't
// be statically resolved WARN and defer the unwindowed rule to a boot-time backstop; the realtime
// label survives the `registerQueries` seam behind the ONE accessor; and `/room-boot` splits
// "<profile>/<key>" wire docs to the named profile while every bare/unknown doc rides the legacy
// `resolveFootprint` alias byte-identically.

import assert from "node:assert/strict";
import { test } from "node:test";

import type { Ast } from "@rindle/client";
import { createSchema, defineQuery, exists, newQueryBuilder, number, string, table } from "@rindle/client";
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
  queryRealtimeLabel,
  queryResultToAst,
  registerQueries,
  RindleApiError,
  ROOM_FLUSH_CREDENTIAL_HEADER,
  verifyRoomFlushCredential,
  type RoomBootResponse,
} from "../src/index.ts";
import { compileRoomProfiles, mintRoomDoc, PROBE_DOC_KEY, splitRoomDoc } from "../src/rooms.ts";

const deck = table("deck").columns({ id: string(), title: string() }).primaryKey("id");
const slide = table("slide").columns({ id: string(), deckId: string(), rank: number() }).primaryKey("id");
const member = table("member").columns({ id: string(), deckId: string() }).primaryKey("id");
const schema = createSchema({ tables: [deck, slide, member] });
const q = newQueryBuilder(schema);

/** The canonical UNWINDOWED document footprint: a deck, its slides, its members. */
const deckFootprint = (docKey: string) =>
  q.deck
    .where.id(docKey)
    .sub("slides", slide, { parent: ["id"], child: ["deckId"] })
    .sub("members", member, { parent: ["id"], child: ["deckId"] });

const documentProfile = {
  key: (a: { docId: string }) => `doc:${a.docId}`,
  footprint: (docKey: string) => deckFootprint(docKey),
  context: ["member"],
};

// ------------------------------------------------------------------------- wire room keys

test("wire room keys: mintRoomDoc/splitRoomDoc round-trip; bare docs don't split", () => {
  assert.equal(mintRoomDoc("document", "doc:d1"), "document/doc:d1");
  assert.deepEqual(splitRoomDoc("document/doc:d1"), { profile: "document", key: "doc:d1" });
  // Split at the FIRST separator only — the key may itself contain "/".
  assert.deepEqual(splitRoomDoc("document/a/b"), { profile: "document", key: "a/b" });
  assert.equal(splitRoomDoc("bare-doc"), undefined);
  assert.equal(splitRoomDoc("/leading"), undefined);
});

// --------------------------------------------------------------- compileRoomProfiles (rules b/c/d)

test("a valid profile compiles with static footprint facts: tables, writable scope, per-table join keys", () => {
  const profiles = compileRoomProfiles({ rooms: { document: documentProfile }, schema });
  const p = profiles.get("document");
  assert.ok(p, "the profile compiled");
  assert.equal(p.name, "document");
  assert.equal(p.key({ docId: "d1" }), "doc:d1");
  assert.deepEqual([...p.context], ["member"]);

  assert.ok(p.static, "the footprint was statically resolvable");
  assert.deepEqual([...p.static.tables].sort(), ["deck", "member", "slide"]);
  // §2.2: writable scope = footprint minus context.
  assert.deepEqual([...p.static.writableTables].sort(), ["deck", "slide"]);
  // (d) correlation/join-key columns, per footprint table.
  assert.deepEqual([...(p.static.joinKeys.get("deck") ?? [])].sort(), ["id"]);
  assert.deepEqual([...(p.static.joinKeys.get("slide") ?? [])], ["deckId"]);
  assert.deepEqual([...(p.static.joinKeys.get("member") ?? [])], ["deckId"]);
});

test("(b) a WINDOWED footprint throws at registration, naming the offending AST path", () => {
  const compile = (footprint: (docKey: string) => unknown) =>
    compileRoomProfiles({
      rooms: { document: { key: documentProfile.key, footprint: footprint as never } },
      schema,
    });

  // root limit
  assert.throws(
    () => compile((k) => q.deck.where.id(k).limit(10)),
    /room profile "document": the footprint must be UNWINDOWED — found limit\(10\) at footprint\.limit/,
  );
  // a window on a RELATED child (top-N per parent is still a window)
  assert.throws(
    () => compile((k) => q.deck.where.id(k).sub("slides", slide, { parent: ["id"], child: ["deckId"] }, (s) => s.limit(3))),
    /found limit\(3\) at footprint\.related\[slides\]\.limit/,
  );
  // cursor paging (the skip lowering)
  assert.throws(
    () => compile((k) => q.deck.where.id(k).start({ id: "a" })),
    /found start \(cursor paging\) at footprint\.start/,
  );
  // one()
  assert.throws(
    () => compile((k) => q.deck.where.id(k).one()),
    /found one\(\) at footprint\.one/,
  );
  // a window buried in an EXISTS child
  assert.throws(
    () =>
      compile((k) =>
        q.deck.where.id(k).where(exists(slide, { parent: ["id"], child: ["deckId"] }, (s) => s.limit(1))),
      ),
    /found limit\(1\) at footprint\.where\.and\[1\]\.exists\[slide\]\.limit/,
  );
});

test("(c) context tables must exist in the schema and be loaded by the footprint", () => {
  assert.throws(
    () =>
      compileRoomProfiles({
        rooms: { document: { ...documentProfile, context: ["nope"] } },
        schema,
      }),
    /realtime\.rooms\.document: context table "nope" is not in the schema/,
  );
  assert.throws(
    () =>
      compileRoomProfiles({
        // footprint loads only deck — "member" is declared followed but never loaded
        rooms: { document: { key: documentProfile.key, footprint: (k) => q.deck.where.id(k), context: ["member"] } },
        schema,
      }),
    /context table "member" is not loaded by the footprint .*SUBSET/,
  );
});

test("profile shape is validated loudly: name may not contain the wire delimiter; key/footprint must be functions", () => {
  assert.throws(
    () => compileRoomProfiles({ rooms: { "a/b": documentProfile }, schema }),
    /invalid profile name "a\/b"/,
  );
  assert.throws(
    () => compileRoomProfiles({ rooms: { document: { ...documentProfile, key: "doc" as never } }, schema }),
    /`key` must be a function/,
  );
  assert.throws(
    () => compileRoomProfiles({ rooms: { document: { ...documentProfile, footprint: 7 as never } }, schema }),
    /`footprint` must be a function/,
  );
});

test("(d) a footprint that can't be statically resolved WARNS loudly and defers to boot (async form)", () => {
  const warnings: string[] = [];
  const profiles = compileRoomProfiles({
    rooms: {
      document: { key: documentProfile.key, footprint: async (k: string) => deckFootprint(k) },
    },
    schema,
    warn: (m) => warnings.push(m),
  });
  assert.equal(profiles.get("document")?.static, undefined, "no static facts for an async footprint");
  assert.equal(warnings.length, 1);
  assert.match(warnings[0], /realtime\.rooms\.document: the footprint could not be statically resolved/);
  assert.match(warnings[0], /the footprint is async/);
  assert.match(warnings[0], /join-key columns .* could not be derived/);
});

test("(d) a footprint that throws on the symbolic probe key WARNS (not throws) and still compiles", () => {
  const warnings: string[] = [];
  const profiles = compileRoomProfiles({
    rooms: {
      document: {
        key: documentProfile.key,
        footprint: (k: string) => {
          if (k === PROBE_DOC_KEY) throw new Error("needs a real doc");
          return deckFootprint(k);
        },
      },
    },
    schema,
    warn: (m) => warnings.push(m),
  });
  assert.ok(profiles.has("document"));
  assert.equal(profiles.get("document")?.static, undefined);
  assert.equal(warnings.length, 1);
  assert.match(warnings[0], /threw on the symbolic probe key: needs a real doc/);
});

// ------------------------------------------------------------ the label seam (registerQueries)

const deckCanvasLabel = { room: "document", args: (a: { deckId: string }) => ({ docId: a.deckId }) };
const deckCanvas = defineQuery(
  "deckCanvas",
  (a: { deckId: string }) => q.slide.where.deckId(a.deckId),
  { realtime: deckCanvasLabel },
);
const plainSlides = defineQuery("plainSlides", () => q.slide.orderBy("id", "asc"));

test("the realtime label round-trips defineQuery → registerQueries → queryRealtimeLabel", async () => {
  const registered = registerQueries([deckCanvas, plainSlides]);
  assert.equal(queryRealtimeLabel(registered.deckCanvas), deckCanvasLabel);
  assert.equal(queryRealtimeLabel(registered.plainSlides), undefined);
  assert.equal(queryRealtimeLabel(undefined), undefined);

  // The wrapper still resolves the authoritative AST exactly as before.
  const ast = queryResultToAst(await registered.deckCanvas({ user: null }, { deckId: "d1" }));
  assert.deepEqual(ast, deckCanvas.resolve({ deckId: "d1" }).ast());
});

// ---------------------------------------------------- construction-time label validation (rule a)

const bootDaemonStub = () => {
  const materialized: MaterializeInput[] = [];
  const claimed: string[] = [];
  const daemon = {
    materialized,
    claimed,
    async claimRoomEpoch(input: ClaimRoomEpochInput): Promise<ClaimRoomEpochOutput> {
      claimed.push(input.doc);
      return { epoch: 5 };
    },
    async materialize(input: MaterializeInput): Promise<MaterializeOutput> {
      materialized.push(input);
      return { materializationId: "m1", leaseToken: "upstream-lease" };
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
};

const SECRET = "shell-secret-profiles";
const fetchRequest = (headers: Record<string, string>) => ({
  headers: { get: (n: string) => headers[n.toLowerCase()] ?? null },
});
const bootCtx = { user: null, request: fetchRequest({ authorization: `Bearer ${SECRET}` }) };

test("(a) a label naming a missing profile fails createRindleApiServer construction, loudly", () => {
  // realtime configured, but under a different profile name
  assert.throws(
    () =>
      createRindleApiServer({
        daemon: bootDaemonStub(),
        schema,
        queries: registerQueries([deckCanvas]),
        realtime: { shellSecret: SECRET, rooms: { workspace: documentProfile } },
      }),
    /query "deckCanvas" is labeled realtime \(room profile "document"\) but no such profile is configured — configured profiles: "workspace"/,
  );
  // no realtime block at all — the label still points at nothing
  assert.throws(
    () =>
      createRindleApiServer({
        daemon: bootDaemonStub(),
        schema,
        queries: registerQueries([deckCanvas]),
      }),
    /no realtime\.rooms profiles are configured/,
  );
  // the matching profile constructs fine (and unlabeled queries never participate)
  createRindleApiServer({
    daemon: bootDaemonStub(),
    schema,
    queries: registerQueries([deckCanvas, plainSlides]),
    realtime: { shellSecret: SECRET, rooms: { document: documentProfile } },
  });
});

test("a realtime block with neither rooms nor resolveFootprint fails construction", () => {
  assert.throws(
    () => createRindleApiServer({ daemon: bootDaemonStub(), realtime: { shellSecret: SECRET } }),
    /configure at least one room profile \(realtime\.rooms\) or the legacy resolveFootprint/,
  );
});

// ----------------------------------------------------------------- /room-boot key routing (§2.1)

const legacyFootprint = (doc: string): Ast => ({ table: "deck", alias: doc });

function profileApi(opts?: { legacy?: boolean }) {
  const daemon = bootDaemonStub();
  const api = createRindleApiServer({
    daemon,
    schema,
    realtime: {
      shellSecret: SECRET,
      rooms: { document: documentProfile },
      ...(opts?.legacy === false ? {} : { resolveFootprint: legacyFootprint }),
    },
  });
  return { api, daemon };
}

test('boot doc "<profile>/<key>" resolves the NAMED profile with the bare key; identity stays the full wire doc', async () => {
  const { api, daemon } = profileApi();
  const out = await api.handleRoomBootJson({ doc: "document/doc:d1" }, bootCtx);
  assert.equal(out.status, 200);

  // The profile's footprint ran with the PROFILE-LOCAL key.
  assert.equal(daemon.materialized.length, 1);
  assert.deepEqual(daemon.materialized[0].ast, deckFootprint("doc:d1").ast());
  assert.equal(daemon.materialized[0].mode, "normalized");

  // The epoch claim AND the flush credential ride the FULL wire doc — the room's identity.
  assert.deepEqual(daemon.claimed, ["document/doc:d1"]);
  const body = out.body as RoomBootResponse;
  const payload = await verifyRoomFlushCredential(body.flush.headers[ROOM_FLUSH_CREDENTIAL_HEADER], SECRET);
  assert.equal(payload.doc, "document/doc:d1");
});

test("a bare doc rides the legacy resolveFootprint alias, byte-identically (the anonymous profile)", async () => {
  const { api, daemon } = profileApi();
  const out = await api.handleRoomBootJson({ doc: "doc-legacy" }, bootCtx);
  assert.equal(out.status, 200);
  assert.deepEqual(daemon.materialized[0].ast, legacyFootprint("doc-legacy"));
  assert.deepEqual(daemon.claimed, ["doc-legacy"]);
});

test('a doc whose "/"-prefix names NO profile falls through to legacy with the FULL doc', async () => {
  const { api, daemon } = profileApi();
  const out = await api.handleRoomBootJson({ doc: "other/x" }, bootCtx);
  assert.equal(out.status, 200);
  assert.deepEqual(daemon.materialized[0].ast, legacyFootprint("other/x"));
});

test("without the legacy alias, an unmatched doc is a loud 404", async () => {
  const { api } = profileApi({ legacy: false });
  await assert.rejects(
    api.handleRoomBootJson({ doc: "doc-legacy" }, bootCtx),
    (e: unknown) =>
      e instanceof RindleApiError && e.status === 404 && /no room profile matches doc "doc-legacy"/.test(e.message),
  );
  await assert.rejects(
    api.handleRoomBootJson({ doc: "other/x" }, bootCtx),
    (e: unknown) => e instanceof RindleApiError && e.status === 404,
  );
});

test("boot-time backstop: a statically-unresolvable footprint that turns out WINDOWED is refused at boot", async (t) => {
  const warned: string[] = [];
  t.mock.method(console, "warn", (...a: unknown[]) => {
    warned.push(a.map(String).join(" "));
  });
  const api = createRindleApiServer({
    daemon: bootDaemonStub(),
    schema,
    realtime: {
      shellSecret: SECRET,
      rooms: {
        // async ⇒ not statically resolvable at construction (warned), so the §2.3 rule
        // must catch the window at the boot backstop instead.
        wide: { key: (a: { id: string }) => a.id, footprint: async (k: string) => q.deck.where.id(k).limit(5) },
      },
    },
  });
  assert.equal(warned.length, 1);
  assert.match(warned[0], /realtime\.rooms\.wide: the footprint could not be statically resolved/);
  await assert.rejects(
    api.handleRoomBootJson({ doc: "wide/d1" }, bootCtx),
    /room profile "wide": the footprint must be UNWINDOWED — found limit\(5\) at footprint\.limit/,
  );
});
