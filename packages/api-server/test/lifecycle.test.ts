// The Slice I-iii lifecycle mint (RINDLE-REALTIME-QUERY-ENABLEMENT §4): under the OPT-IN
// `realtime.lifecycle` config a realtime-LABELED lease additionally mints the doorbell system
// lease (`_rindle_scope_sessions WHERE scope = <wire room doc>` — the §4.1 occupancy key,
// room-served or not) and a ROOM-SERVED lease the fence bundle (`_rindle_room_watermark` +
// `_rindle_room_client_mutations` + `_rindle_room_mutation_outcomes`, doc-scoped and
// client-scoped when the request carried `clientId`). Absent config ⇒ the response is
// byte-identical to today (pinned below); a mint failure fail-opens with a one-time warning,
// never blocking the lease — the same discipline as the G-iv-b serve decision.

import assert from "node:assert/strict";
import { test } from "node:test";

import type { Ast, Condition } from "@rindle/client";
import { createSchema, defineQuery, newQueryBuilder, number, string, table } from "@rindle/client";
import type { MaterializeInput, MaterializeOutput, RindleDaemonClient, SqlRead, SqlReadOutput, SqlTxn, WireValue } from "@rindle/daemon-client";

import {
  createRindleApiServer,
  registerQueries,
  type ApiQuery,
  type QueryLeaseResponse,
  type RindleApiServerOptions,
} from "../src/index.ts";

// ------------------------------------------------------------------------------- the fixture
// (the serve-proof fixture, verbatim: a labeled windowed canvas over an unwindowed doc footprint)

const deck = table("deck").columns({ id: string(), title: string() }).primaryKey("id");
const slide = table("slide").columns({ id: string(), deckId: string(), rank: number() }).primaryKey("id");
const member = table("member").columns({ id: string(), deckId: string() }).primaryKey("id");
const schema = createSchema({ tables: [deck, slide, member] });
const q = newQueryBuilder(schema);

const documentProfile = {
  key: (a: { docId: string }) => `doc:${a.docId}`,
  footprint: (docKey: string) =>
    q.deck
      .where.id(docKey)
      .sub("slides", slide, { parent: ["id"], child: ["deckId"] })
      .sub("members", member, { parent: ["id"], child: ["deckId"] }),
  context: ["member"],
};

const deckCanvas = defineQuery(
  "deckCanvas",
  (a: { deckId: string }) => q.slide.where.deckId(a.deckId).orderBy("rank", "asc").limit(10),
  { realtime: { room: "document", args: (a: { deckId: string }) => ({ docId: a.deckId }) } },
);
const plainSlides = defineQuery("plainSlides", () => q.slide.orderBy("id", "asc"));

/** A fake daemon (the serve-proof shape) + a scriptable per-materialize failure. */
class FakeDaemon implements RindleDaemonClient {
  materialized: MaterializeInput[] = [];
  /** When set, a materialize whose AST root passes the predicate THROWS (the never-enabled-
   *  lifecycle daemon shape). */
  failMaterialize?: (input: MaterializeInput) => boolean;

  async materialize(input: MaterializeInput): Promise<MaterializeOutput> {
    if (this.failMaterialize?.(input)) {
      throw new Error(`no such table: ${(input.ast as Ast).table}`);
    }
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

  // --- a SEMANTIC mini scope-sessions store (Slice I-iv): the fake EXECUTES the occupancy SQL
  // instead of scripting fixed rows, so the tests are violation-proof against the whole pipeline
  // (params mixed up / sweep bound wrong / gate inverted all surface as row-level divergence).
  scopeSessions = new Map<string, Map<string, number>>(); // scope → client_id → expires_at
  txns: SqlTxn[] = [];
  reads: SqlRead[] = [];
  failSqlTxn = false;

  seedSession(scope: string, clientId: string, expiresAt: number): void {
    let m = this.scopeSessions.get(scope);
    if (!m) this.scopeSessions.set(scope, (m = new Map()));
    m.set(clientId, expiresAt);
  }
  sessionRows(scope: string): Array<[string, number]> {
    return [...(this.scopeSessions.get(scope) ?? new Map<string, number>())];
  }

  async executeSqlTxn(input: SqlTxn) {
    if (this.failSqlTxn) throw new Error("no such table: _rindle_scope_sessions");
    this.txns.push(input);
    for (const st of input.statements) {
      const p = st.params ?? [];
      if (st.sql.startsWith("DELETE FROM _rindle_scope_sessions")) {
        const [scope, cutoff] = p as [string, number];
        const m = this.scopeSessions.get(scope);
        if (m) for (const [cid, exp] of [...m]) if (exp < cutoff) m.delete(cid);
      } else if (st.sql.startsWith("INSERT INTO _rindle_scope_sessions")) {
        const [scope, clientId, expiresAt] = p as [string, string, number];
        this.seedSession(scope, clientId, expiresAt);
      } else {
        throw new Error(`unexpected txn sql: ${st.sql}`);
      }
    }
    return { applied: true, cv: 1 };
  }
  async executeSqlRead(input: SqlRead): Promise<SqlReadOutput> {
    this.reads.push(input);
    if (!input.sql.includes("FROM _rindle_scope_sessions")) return { cols: [], rows: [] };
    // Positional per SESSION_COUNT[_OTHERS]_SQL: [now, now, now − grace, scope, (clientId)].
    const p = input.params as [number, number, number, string, string?];
    const [now, , graceCutoff, scope, excludeClient] = p;
    let live = 0;
    let recent = 0;
    let total = 0; // COUNT(*) matching (any expiry, pre-sweep) — the I-v "room plausibly exists" cell
    for (const [cid, exp] of this.scopeSessions.get(scope) ?? []) {
      if (excludeClient !== undefined && cid === excludeClient) continue;
      total++;
      if (exp > now) live++;
      else if (exp > graceCutoff) recent++;
    }
    return { cols: ["live", "recent", "total"], rows: [[live, recent, total] as WireValue[]] };
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
    realtime?: Partial<RindleApiServerOptions<string>["realtime"] & object> | null;
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

const lease = (
  api: { createQueryLease(i: any): Promise<QueryLeaseResponse> },
  name: string,
  args: unknown,
  clientId?: string,
  affinity?: string,
) =>
  api.createQueryLease({
    user: "u1",
    name,
    args,
    ...(clientId !== undefined ? { clientId } : {}),
    ...(affinity !== undefined ? { affinity } : {}),
  });

const DOC = "document/doc:d1";
const scopeEq = (col: string, value: string): Condition => ({
  type: "simple",
  op: "=",
  left: { type: "column", name: col },
  right: { type: "literal", value },
});

// ------------------------------------------------------------------- off ⇒ byte-identical

test("lifecycle OFF: a room-served labeled lease is byte-identical — no lifecycle key, no extra materializations", async () => {
  const daemon = new FakeDaemon();
  const { api } = makeApi(daemon); // realtime configured, lifecycle NOT
  const res = await lease(api, "deckCanvas", { deckId: "d1" }, "c1");
  assert.ok(res.realtime, "room-served (labeled + wired)");
  assert.equal("lifecycle" in res, false, "the opt-in is absent — NOTHING new is minted");
  assert.deepEqual(
    Object.keys(res).sort(),
    ["leaseToken", "materializationId", "queryKey", "realtime", "reused"],
    "the response shape is exactly the pre-I-iii one",
  );
  assert.equal(daemon.materialized.length, 1, "exactly the primary materialization — no system leases");
  assert.equal(daemon.txns.length, 0, "no occupancy write either (the I-iv step is inside the opt-in)");
  assert.equal(daemon.reads.length, 0, "…and no occupancy read");
});

// ------------------------------------------------------------------- on ⇒ the minted block

test("lifecycle ON + room-served: doorbell + the fence trio, with the scope-key/doc/clientId predicates asserted", async () => {
  const daemon = new FakeDaemon();
  // This lane targets the I-iii MINT SHAPES; the I-iv occupancy gate is disarmed via
  // `minSessions: 1` so a solo lease still room-serves (the gate has its own lanes below).
  const { api } = makeApi(daemon, { realtime: { lifecycle: { minSessions: 1 } } });
  const res = await lease(api, "deckCanvas", { deckId: "d1" }, "c1", "aff.same.follower");

  assert.ok(res.realtime, "still room-served — the block is additive");
  const lc = res.lifecycle;
  assert.ok(lc, "the lifecycle block is present");

  // The doorbell: minted over the §4.1 occupancy table, scoped to THE WIRE ROOM DOC — the same
  // "<profile>/<key>" the room is provisioned/addressed by (the scope-key decision).
  assert.equal(lc.doorbell.table, "_rindle_scope_sessions");
  assert.equal(lc.doorbell.scope, DOC);
  assert.equal(typeof lc.doorbell.leaseToken, "string");
  // The fence trio, in bundle order: watermark (doc), ledger (doc+client), outcomes (doc+client).
  assert.deepEqual(
    lc.fence?.map((f) => [f.table, f.doc, f.clientId]),
    [
      ["_rindle_room_watermark", DOC, undefined],
      ["_rindle_room_client_mutations", DOC, "c1"],
      ["_rindle_room_mutation_outcomes", DOC, "c1"],
    ],
  );
  // Every minted lease is attachable by the existing subscribe path: a distinct daemon leaseToken.
  const tokens = [lc.doorbell.leaseToken, ...(lc.fence ?? []).map((f) => f.leaseToken)];
  assert.equal(new Set(tokens).size, 4, "four distinct system lease tokens");

  // The hand-built ASTs, at the daemon: primary + doorbell + watermark + ledger + outcomes.
  assert.equal(daemon.materialized.length, 5);
  const asts = daemon.materialized.slice(1).map((m) => m.ast as Ast);
  assert.deepEqual(asts[0], { table: "_rindle_scope_sessions", where: scopeEq("scope", DOC) });
  assert.deepEqual(asts[1], { table: "_rindle_room_watermark", where: scopeEq("doc", DOC) });
  assert.deepEqual(asts[2], {
    table: "_rindle_room_client_mutations",
    where: { type: "and", conditions: [scopeEq("doc", DOC), scopeEq("client_id", "c1")] },
  });
  assert.deepEqual(asts[3], {
    table: "_rindle_room_mutation_outcomes",
    where: { type: "and", conditions: [scopeEq("doc", DOC), scopeEq("client_id", "c1")] },
  });
  // Placement parity: the system mints ride the SAME subject + routingKey as the primary lease
  // (a routed deploy must co-locate them with the client's daemon session).
  for (const m of daemon.materialized.slice(1)) {
    assert.equal(m.subject, "user:u1");
    assert.deepEqual(m.metadata, { routingKey: "c1" });
    assert.equal(m.affinity, "aff.same.follower", "every lifecycle lease preserves follower placement");
  }
});

test("lifecycle ON + labeled but NOT room-served: the doorbell still rides (occupancy is counted pre-room); no fence", async () => {
  // No locateRoom ⇒ the serve decision fail-opens (never room-served) — the doorbell must not
  // depend on it: the 1→2 upgrade trigger needs solo watchers counted BEFORE any room exists.
  const daemon = new FakeDaemon();
  const { api } = makeApi(daemon, { realtime: { locateRoom: undefined, lifecycle: {} } });
  const res = await lease(api, "deckCanvas", { deckId: "d1" }, "c1");
  assert.equal(res.realtime, undefined, "not room-served");
  assert.ok(res.lifecycle, "…but the lifecycle block is present");
  assert.equal(res.lifecycle.doorbell.scope, DOC);
  assert.equal(res.lifecycle.fence, undefined, "no room domain ⇒ nothing to fence");
  assert.equal(daemon.materialized.length, 2, "primary + doorbell only");
});

test("lifecycle ON + unlabeled query: no block, no extra materializations (there is no scope to count on)", async () => {
  const daemon = new FakeDaemon();
  const { api } = makeApi(daemon, { realtime: { lifecycle: {} } });
  const res = await lease(api, "plainSlides", null, "c1");
  assert.equal("lifecycle" in res, false);
  assert.equal(daemon.materialized.length, 1);
});

test("lifecycle ON, no clientId on the request: fence ledger/outcomes fall back to doc-only predicates (the client filters)", async () => {
  const daemon = new FakeDaemon();
  // A no-clientId caller contributes nothing to its own gate (I-iv), so occupancy must come from
  // OTHER sessions: one live session + `minSessions: 1` keeps this lane room-served.
  daemon.seedSession(DOC, "c-other", Date.now() + 60_000);
  const { api } = makeApi(daemon, { realtime: { lifecycle: { minSessions: 1 } } });
  const res = await lease(api, "deckCanvas", { deckId: "d1" }); // no clientId
  const lc = res.lifecycle;
  assert.ok(lc?.fence);
  assert.deepEqual(
    lc.fence.map((f) => [f.table, f.doc, "clientId" in f]),
    [
      ["_rindle_room_watermark", DOC, false],
      ["_rindle_room_client_mutations", DOC, false],
      ["_rindle_room_mutation_outcomes", DOC, false],
    ],
    "no clientId field minted",
  );
  const ledgerAst = daemon.materialized[3].ast as Ast;
  assert.deepEqual(ledgerAst.where, scopeEq("doc", DOC), "doc-only predicate — the client-side fold filters to its own rows");
});

test("lifecycle ON, mint failure: fail-open — the lease ships without the block, warned once per query", async () => {
  const daemon = new FakeDaemon();
  daemon.failMaterialize = (input) => (input.ast as Ast).table.startsWith("_rindle_");
  const { api, warnings } = makeApi(daemon, { realtime: { lifecycle: { minSessions: 1 } } });

  const res = await lease(api, "deckCanvas", { deckId: "d1" }, "c1");
  assert.ok(res.realtime, "the room-serve half is untouched");
  assert.equal("lifecycle" in res, false, "the block fail-opened");
  const mintWarnings = warnings.filter((w) => w.includes("lifecycle system leases were not minted"));
  assert.equal(mintWarnings.length, 1, "warned, loudly");

  await lease(api, "deckCanvas", { deckId: "d1" }, "c1");
  assert.equal(
    warnings.filter((w) => w.includes("lifecycle system leases were not minted")).length,
    1,
    "…exactly once per query",
  );
});

// ============================================================== the §4.1 occupancy gate (I-iv)

test("I-iv gate: a SOLO labeled lease upserts its session row, ships NO realtime block, and still gets the doorbell; a second client opens the gate", async () => {
  const daemon = new FakeDaemon();
  const { api } = makeApi(daemon, { realtime: { lifecycle: {} } }); // default minSessions = 2
  const before = Date.now();
  const res1 = await lease(api, "deckCanvas", { deckId: "d1" }, "c1");

  // The write txn: sweep + upsert, ONE executeSqlTxn, bare (no clientID/mid — never an lmid
  // advance; no idempotencyKey — a renewal's refresh must re-run).
  assert.equal(daemon.txns.length, 1);
  const txn = daemon.txns[0];
  assert.equal(txn.clientID, undefined);
  assert.equal(txn.mid, undefined);
  assert.equal(txn.idempotencyKey, undefined);
  assert.equal(txn.statements.length, 2, "sweep + upsert in the SAME txn (D4)");
  assert.match(txn.statements[0].sql, /^DELETE FROM _rindle_scope_sessions WHERE scope = \? AND expires_at < \?$/);
  assert.match(
    txn.statements[1].sql,
    /^INSERT INTO _rindle_scope_sessions \(scope, client_id, expires_at\) VALUES \(\?, \?, \?\) ON CONFLICT\(scope, client_id\) DO UPDATE SET expires_at = excluded\.expires_at$/,
  );
  assert.deepEqual(txn.statements[1].params?.slice(0, 2), [DOC, "c1"]);
  // The count read rode consistency:"strong" (read-your-writes off the write master).
  assert.equal(daemon.reads.length, 1);
  assert.equal(daemon.reads[0].consistency, "strong");
  assert.match(daemon.reads[0].sql, / AND client_id <> \?$/, "the caller's own row is excluded (it rides the +1)");
  // The row landed (the doorbell IS this row) with expires_at in the future.
  const rows1 = daemon.sessionRows(DOC);
  assert.equal(rows1.length, 1);
  assert.equal(rows1[0][0], "c1");
  assert.ok(rows1[0][1] >= before, "expires_at = now + sessionTtl, in the future");

  // VIOLATION-PROOF (the gate itself): solo ⇒ suppressed — an inverted/removed gate would ship
  // a realtime block here and fail this assert.
  assert.equal(res1.realtime, undefined, "solo: the room-serve is SUPPRESSED (count 1 < minSessions 2)");
  assert.ok(res1.lifecycle, "…but the doorbell block still rides (that is the point)");
  assert.equal(res1.lifecycle.doorbell.scope, DOC);
  assert.equal(res1.lifecycle.fence, undefined, "not room-served ⇒ no fence");

  // A SECOND client on the same scope: c1's unexpired row counts as the other session — gate open.
  const res2 = await lease(api, "deckCanvas", { deckId: "d1" }, "c2");
  assert.ok(res2.realtime, "two unexpired sessions ⇒ room-served");
  assert.ok(res2.lifecycle?.fence, "room-served ⇒ the fence bundle too");
  assert.equal(daemon.sessionRows(DOC).length, 2, "both session rows live");

  // And c1's next lease (the doorbell-triggered re-lease) now room-serves as well.
  const res3 = await lease(api, "deckCanvas", { deckId: "d1" }, "c1");
  assert.ok(res3.realtime, "c1 re-leases into an occupied scope ⇒ room-served");
});

test("I-iv renewal + sweep: a re-lease refreshes expires_at in place; the sweep deletes ONLY rows expired past grace+slack — an in-grace row LINGERS (the I-v read)", async () => {
  const daemon = new FakeDaemon();
  // Tight sessionTtl so the refresh is observable; grace 120s default, slack 60s constant.
  const { api } = makeApi(daemon, { realtime: { lifecycle: { sessionTtlMs: 1_000 } } });
  const now = Date.now();
  // Pre-seed: one row expired INSIDE the grace window (must linger — Slice I-v's downgrade
  // decision reads "the newest other row expired > graceMs ago" FROM this row), one expired far
  // past grace+slack (must be swept).
  daemon.seedSession(DOC, "c-recent", now - 1_000);
  daemon.seedSession(DOC, "c-ancient", now - (120_000 + 60_000 + 5_000));

  await lease(api, "deckCanvas", { deckId: "d1" }, "c1");
  const after1 = new Map(daemon.sessionRows(DOC));
  assert.equal(after1.has("c-ancient"), false, "expired > grace+slack ⇒ swept");
  assert.equal(after1.has("c-recent"), true, "expired < grace ⇒ LINGERS (pinned: I-v depends on it)");
  const exp1 = after1.get("c1")!;
  assert.ok(exp1 > now && exp1 <= now + 1_000 + 5_000, "expires_at = now + sessionTtlMs");

  // Renewal (same clientId, same scope): ONE row, expires_at refreshed (>=, not a duplicate).
  await lease(api, "deckCanvas", { deckId: "d1" }, "c1");
  const after2 = new Map(daemon.sessionRows(DOC));
  assert.equal([...after2.keys()].filter((c) => c === "c1").length, 1, "upsert, not insert — one row per (scope, clientId)");
  assert.ok(after2.get("c1")! >= exp1, "the renewal refreshed expires_at");
});

test("I-iv grace hysteresis: an other-session row expired WITHIN grace holds the gate open (a momentarily-solo room's renewal is never suppressed inside the window)", async () => {
  const daemon = new FakeDaemon();
  const { api } = makeApi(daemon, { realtime: { lifecycle: {} } });
  // The other session lapsed 10s ago — inside the 120s grace. This is exactly the renewal shape
  // the stateless server cannot distinguish from a fresh mint: the hysteresis (not renewal
  // detection) is what keeps the live room served. Past the window the gate closes — which is
  // I-v's downgrade condition, read from the same lingering rows.
  daemon.seedSession(DOC, "c-lapsed", Date.now() - 10_000);
  const res = await lease(api, "deckCanvas", { deckId: "d1" }, "c1");
  assert.ok(res.realtime, "in-grace other session ⇒ still room-served (hysteresis)");

  // Same shape but lapsed PAST grace (still inside the sweep's slack band, so the row survives
  // the sweep yet no longer holds the gate): suppressed.
  const daemon2 = new FakeDaemon();
  const { api: api2 } = makeApi(daemon2, { realtime: { lifecycle: {} } });
  daemon2.seedSession(DOC, "c-lapsed", Date.now() - 150_000); // grace 120s < 150s < grace+slack 180s
  const res2 = await lease(api2, "deckCanvas", { deckId: "d1" }, "c1");
  assert.equal(res2.realtime, undefined, "past-grace lapse ⇒ the gate closes (I-v turns this into the fenced downgrade)");
  assert.ok(daemon2.sessionRows(DOC).some(([c]) => c === "c-lapsed"), "…while the row itself still lingers for I-v");
});

test("I-iv knobs: minSessions=1 room-serves a solo lease; a MISSING clientId writes no row, counts nothing (not even itself), and still gets a doorbell", async () => {
  const daemon = new FakeDaemon();
  const { api } = makeApi(daemon, { realtime: { lifecycle: { minSessions: 1 } } });
  const res = await lease(api, "deckCanvas", { deckId: "d1" }, "c1");
  assert.ok(res.realtime, "minSessions=1: the knob restores solo room-serving");

  // Missing clientId (a non-shipped client — the shipped one always sends it): no session
  // identity ⇒ no row, no occupancy contribution (D7), and with minSessions=1 + an EMPTY scope
  // the gate stays closed for it (others alone must reach the threshold).
  const daemon2 = new FakeDaemon();
  const { api: api2 } = makeApi(daemon2, { realtime: { lifecycle: { minSessions: 1 } } });
  const res2 = await api2.createQueryLease({ user: "u1", name: "deckCanvas", args: { deckId: "d1" } });
  assert.equal(daemon2.txns[0]?.statements.length, 1, "sweep only — no upsert without a clientId");
  assert.deepEqual(daemon2.sessionRows(DOC), [], "no session row");
  assert.equal(daemon2.reads[0]?.sql.includes("client_id <>"), false, "no caller to exclude from the count");
  assert.equal(res2.realtime, undefined, "no session identity ⇒ no self-contribution ⇒ suppressed");
  assert.ok(res2.lifecycle?.doorbell, "…but the doorbell still rides");
});

test("I-iv fail-open: an occupancy write failure never suppresses — the lease serves exactly as pre-I-iv, warned once per query", async () => {
  const daemon = new FakeDaemon();
  daemon.failSqlTxn = true; // the never-enabled-lifecycle daemon shape (missing system table)
  const warnings: string[] = [];
  const { api } = makeApi(daemon, { realtime: { lifecycle: {} }, warnings });

  const res = await lease(api, "deckCanvas", { deckId: "d1" }, "c1");
  assert.ok(res.realtime, "fail-open: the gate falls away, the labeled lease room-serves as before I-iv");
  const occWarnings = () => warnings.filter((w) => w.includes("occupancy step failed"));
  assert.equal(occWarnings().length, 1, "warned, loudly");
  await lease(api, "deckCanvas", { deckId: "d1" }, "c1");
  assert.equal(occWarnings().length, 1, "…exactly once per query");
});

// ============================================================== the §4.2 downgrade fence (I-v)

test("I-v downgrade fence: a gate-CLOSED lease whose scope was SHARED drains the room and rides the fence ALONGSIDE the absent block + the fence bundle", async () => {
  const daemon = new FakeDaemon();
  const drained: string[] = [];
  const { api } = makeApi(daemon, {
    realtime: { lifecycle: { drainRoom: async (doc: string) => { drained.push(doc); return { finalFlushSeq: 7 }; } } },
  });
  // A collaborator lapsed PAST grace but still lingering (the I-v downgrade shape, read from the
  // same rows the hysteresis reads): the gate closes, but a room plausibly exists ⇒ drain.
  daemon.seedSession(DOC, "c-lapsed", Date.now() - 150_000);
  const res = await lease(api, "deckCanvas", { deckId: "d1" }, "c1");

  assert.equal(res.realtime, undefined, "gate closed ⇒ NO room block (block-absence is the downgrade signal)");
  assert.deepEqual(drained, [DOC], "the room was drained on its wire doc");
  assert.deepEqual(
    res.realtimeFence,
    { sourceKey: `room:${DOC}`, doc: DOC, finalFlushSeq: 7 },
    "the fence rides ALONGSIDE the absent block (a sibling of `realtime`, never nested)",
  );
  assert.ok(res.lifecycle?.fence, "the fence bundle rides the DOWNGRADE response (the client needs the streams to drop the ghost)");
  assert.equal(res.lifecycle.doorbell.scope, DOC, "…and the doorbell still rides (it re-arms the next upgrade)");
});

test("I-v downgrade fence: a NEVER-shared solo doc never drains (no wasted room boot)", async () => {
  const daemon = new FakeDaemon();
  const drained: string[] = [];
  const { api } = makeApi(daemon, {
    realtime: { lifecycle: { drainRoom: async (doc: string) => { drained.push(doc); return { finalFlushSeq: 1 }; } } },
  });
  // No other session EVER: the gate is closed (solo) but the doc is not room-plausible.
  const res = await lease(api, "deckCanvas", { deckId: "d1" }, "c1");
  assert.equal(res.realtime, undefined, "solo ⇒ gate closed");
  assert.deepEqual(drained, [], "never-shared ⇒ drainRoom is NEVER called (no room to boot just to drain)");
  assert.equal(res.realtimeFence, undefined, "…so no fence");
  assert.ok(res.lifecycle?.doorbell, "the doorbell still rides (the solo→shared 1→2 upgrade path)");
  assert.equal(res.lifecycle.fence, undefined, "no fence bundle for a solo daemon-served lease");
});

test("I-v downgrade fence: drainRoom UNCONFIGURED ⇒ no fence (the client hits its loud legacy path), warned once per query", async () => {
  const daemon = new FakeDaemon();
  const warnings: string[] = [];
  const { api } = makeApi(daemon, { realtime: { lifecycle: {} }, warnings }); // no drainRoom
  daemon.seedSession(DOC, "c-lapsed", Date.now() - 150_000);
  const res = await lease(api, "deckCanvas", { deckId: "d1" }, "c1");
  assert.equal(res.realtimeFence, undefined, "no drainRoom ⇒ no fence");
  const drainWarnings = () => warnings.filter((w) => w.includes("no §4.2 fence"));
  assert.equal(drainWarnings().length, 1, "warned once");
  await lease(api, "deckCanvas", { deckId: "d1" }, "c1"); // c-lapsed still lingers (< grace+slack)
  assert.equal(drainWarnings().length, 1, "…exactly once per query");
});

test("I-v downgrade fence: drainRoom THROWING fails OPEN — no fence, the lease is never blocked, warned once", async () => {
  const daemon = new FakeDaemon();
  const warnings: string[] = [];
  const { api } = makeApi(daemon, {
    realtime: { lifecycle: { drainRoom: async () => { throw new Error("room unreachable"); } } },
    warnings,
  });
  daemon.seedSession(DOC, "c-lapsed", Date.now() - 150_000);
  const res = await lease(api, "deckCanvas", { deckId: "d1" }, "c1");
  assert.equal(res.realtimeFence, undefined, "fail-open: no fence on a drain failure");
  assert.ok(res.leaseToken, "…and the lease itself was NOT blocked");
  assert.equal(warnings.filter((w) => w.includes("drainRoom threw")).length, 1, "warned once");
});
