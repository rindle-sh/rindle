// The §4 lifecycle SYSTEM-STREAM plane, client half (RINDLE-REALTIME-QUERY-ENABLEMENT-DESIGN.md
// Slice I-iii): `retainSystemQuery` subs with NO store view whose frames buffer on the DAEMON
// gate like the reserved lmid stream and fold at RELEASE time in a FIXED category order —
// (1) outcome rows → (2) room-ledger rows → (3) watermark rows → (4) scope-session rows — before
// any ordinary data op. This suite pins THE NAMED INVARIANT's client enforcement (**never retire
// a room-domain entry off a daemon-carried lmid without outcome resolution**, §3.3's shipped
// note):
//
//   - absence IS resolution: a covering daemon-carried lmid with no outcome row retires the
//     entry as APPLIED (I-ii co-commits an outcome row for every NON-applied mid in the same
//     daemon transaction as the ledger row, so absence-under-coverage means applied);
//   - a deopt ROW in the SAME release as the covering lmid must WIN over absence — the fold
//     order (outcomes before ledger) is what makes it win: the entry flips in place (daemon
//     domain, fresh daemon mid, ORIGINAL seq — the H-v machine, one verdict path for frames and
//     rows) instead of retiring as a silent success. Reversing the fold order makes these lanes
//     fail (the violation-proof run: the retire wins the race and the late row lands on the
//     not-found arm as a spurious fresh re-invocation with a NEW seq).
//
// The room-socket path stays UNAFFECTED when it is live: a frame that already resolved a mid
// makes the daemon-carried row a no-op (the H-v processed set doubles as the cross-release
// resolved-verdict memory). Doorbell/fence bookkeeping (`roomWatermarks`/`scopeSessions`) is
// maintained + exposed only — I-iv/I-v wire the reactions.
//
// Requires the wasm artifact (packages/wasm/build.sh) — same as the other suites.

import { test } from "node:test";
import assert from "node:assert/strict";

import {
  createSchema,
  defineQuery,
  initWasm,
  newQueryBuilder,
  number,
  string,
  table,
  type MutationEnvelope,
  type MutationOutcomeFrame,
  type NormalizedEvent,
  type NormalizedOp,
  type NormalizedTableSchema,
  type OptimisticSource,
  type ProgressFrame,
  type QueryId,
  type RemoteQuery,
} from "@rindle/wasm";
import {
  createOptimisticStore,
  LIFECYCLE_QUERY_NAME,
  ROOM_CLIENT_MUTATIONS_TABLE,
  ROOM_MUTATION_OUTCOMES_TABLE,
  ROOM_WATERMARK_TABLE,
  SCOPE_SESSIONS_TABLE,
  type ClientRegistry,
  type DowngradeStuckEvent,
  type FoldClock,
  type MutationTx,
  type ScopeSessionsEvent,
  type SystemStreamSpec,
} from "../src/index.ts";

await initWasm();

const note = table("note").columns({ id: number(), body: string() }).primaryKey("id");
const schema = createSchema({ tables: [note] });
const qb = newQueryBuilder(schema);
const allNotes = defineQuery("allNotes", () => qb.note.orderBy("id", "asc"));

/** A hand-scripted channel (the per_source_gate TestSource, verbatim shape). */
class TestSource implements OptimisticSource {
  normalized: (qid: QueryId, ev: NormalizedEvent) => void = () => {};
  progress: (frame: ProgressFrame) => void = () => {};
  restart: () => void = () => {};
  outcome: (frame: MutationOutcomeFrame) => void = () => {};
  resync: () => void = () => {};
  envelopes: MutationEnvelope[] = [];
  registered: { qid: QueryId; name: string }[] = [];
  unregistered: QueryId[] = [];

  registerQuery(qid: QueryId, remote: RemoteQuery): void {
    this.registered.push({ qid, name: remote.name });
  }
  unregisterQuery(qid: QueryId): void {
    this.unregistered.push(qid);
  }
  pushMutation(env: MutationEnvelope): Promise<void> {
    this.envelopes.push(env);
    return Promise.resolve();
  }
  onNormalized(h: (qid: QueryId, ev: NormalizedEvent) => void): void {
    this.normalized = h;
  }
  onProgress(h: (frame: ProgressFrame) => void): void {
    this.progress = h;
  }
  onRestart(h: () => void): void {
    this.restart = h;
  }
  onMutationOutcome(h: (frame: MutationOutcomeFrame) => void): void {
    this.outcome = h;
  }
  onResync(h: () => void): void {
    this.resync = h;
  }
  expectClientSchema(_schemas: NormalizedTableSchema[]): void {}

  lmid(mid: number, cv: number): void {
    this.normalized(0, {
      type: "batch",
      ops: [{ table: "_rindle_client_mutations", op: "add", row: ["c1", mid] } as NormalizedOp],
      cv,
    });
  }
  batch(qid: QueryId, ops: NormalizedOp[], cv: number): void {
    this.normalized(qid, { type: "batch", ops, cv });
  }
  snapshot(qid: QueryId, ops: NormalizedOp[], cv: number): void {
    this.normalized(qid, { type: "snapshot", ops, cv });
  }
  frame(cvMin: number): void {
    this.progress({ cvMin });
  }
}

const add = (id: number, body: string): NormalizedOp => ({ table: "note", op: "add", row: [id, body] }) as NormalizedOp;

const registry = {
  daemonWrite: (tx: MutationTx, a: { id: number; body: string }) => tx.upsert("note", { id: a.id, body: a.body }),
  roomWrite: (tx: MutationTx, a: { id: number; body: string }) => tx.upsert("note", { id: a.id, body: a.body }),
} satisfies ClientRegistry;

/** The doc + its domain/gate key: the same `room:<doc>` convention the lease wire mints. */
const DOC = "doc:1";
const ROOM = `room:${DOC}`;
const domainPolicy = (name: string): string => (name.startsWith("room") ? ROOM : "daemon");

// System retain qids (the client mints these from its own high band; any non-colliding ids work).
const LEDGER_QID = 201;
const OUTCOMES_QID = 202;
const WATERMARK_QID = 203;
const SESSIONS_QID = 204;

// --- wire rows, positional per the daemon DDL column order (system-streams.ts schemas) --------
const ledgerRow = (doc: string, client: string, lmid: number): NormalizedOp =>
  ({ table: ROOM_CLIENT_MUTATIONS_TABLE, op: "add", row: [doc, client, lmid] }) as NormalizedOp;
const outcomeRow = (
  doc: string,
  client: string,
  mid: number,
  kind: "deopt" | "rejected",
  extra: { reason?: string; name?: string; args?: unknown } = {},
): NormalizedOp =>
  ({
    table: ROOM_MUTATION_OUTCOMES_TABLE,
    op: "add",
    row: [doc, client, mid, kind, extra.reason ?? null, extra.name ?? null, extra.args !== undefined ? JSON.stringify(extra.args) : null],
  }) as NormalizedOp;
const watermarkRow = (doc: string, flushSeq: number): NormalizedOp =>
  ({ table: ROOM_WATERMARK_TABLE, op: "add", row: [doc, flushSeq] }) as NormalizedOp;
const sessionRow = (scope: string, client: string, expiresAt: number, op: "add" | "remove" = "add"): NormalizedOp =>
  ({ table: SCOPE_SESSIONS_TABLE, op, row: [scope, client, expiresAt] }) as NormalizedOp;

const sysRemote = (table: string, id: { scope?: string; doc?: string }): RemoteQuery => ({
  name: LIFECYCLE_QUERY_NAME,
  args: { table, ...id, parent: { name: "allNotes", args: null } },
});

function setup(opts: { onRejected?: (env: MutationEnvelope, reason: string) => void; room?: boolean; clock?: FoldClock } = {}) {
  const daemonSrc = new TestSource();
  const { store, backend, mutate } = createOptimisticStore(schema, daemonSrc, registry, {
    clientID: "c1",
    domainPolicy,
    ...(opts.onRejected !== undefined ? { onRejected: opts.onRejected } : {}),
    ...(opts.clock !== undefined ? { clock: opts.clock } : {}),
  });
  const view = store.materialize(allNotes());
  daemonSrc.snapshot(1, [], 1);
  daemonSrc.frame(1);
  // The fence bundle + doorbell, retained the way client.ts does (daemon channel, spec carries
  // the minted identity the folds filter on).
  const retain = (qid: QueryId, spec: SystemStreamSpec) =>
    backend.retainSystemQuery(qid, sysRemote(spec.table, { scope: spec.scope, doc: spec.doc }), spec);
  retain(OUTCOMES_QID, { table: ROOM_MUTATION_OUTCOMES_TABLE, doc: DOC });
  retain(LEDGER_QID, { table: ROOM_CLIENT_MUTATIONS_TABLE, doc: DOC });
  retain(WATERMARK_QID, { table: ROOM_WATERMARK_TABLE, doc: DOC });
  retain(SESSIONS_QID, { table: SCOPE_SESSIONS_TABLE, scope: DOC });
  // The live-room shape (the no-interference lanes): a connected room gate delivering the
  // out-of-band outcome frames beside the daemon-carried rows.
  const roomSrc = new TestSource();
  if (opts.room) backend.connectSource(ROOM, roomSrc);
  const rows = (): unknown[][] => view.data.map((r) => [r.id, r.body]);
  return { daemonSrc, roomSrc, backend, mutate, view, rows };
}

// ============================================================================================
// THE NAMED INVARIANT — the daemon-carried room-lmid fold (§3.3 shipped note / §7.1)
// ============================================================================================

test("invariant, withheld direction: a covering daemon-carried lmid with NO outcome row retires as APPLIED (absence IS resolution)", () => {
  const { daemonSrc, backend, mutate, rows } = setup();
  mutate.roomWrite({ id: 1, body: "r" }); // room mid 1, prediction applied
  assert.deepEqual(rows(), [[1, "r"]]);

  // One coherent daemon release: the room flush's data echo AND its doc-scoped ledger row
  // co-committed at cv 2 (I-ii's atomicity). No outcome row anywhere ⇒ the mid was APPLIED.
  daemonSrc.batch(1, [add(1, "r")], 2);
  daemonSrc.batch(LEDGER_QID, [ledgerRow(DOC, "c1", 1)], 2);
  daemonSrc.frame(2);

  const d = backend.__inspectDomains();
  assert.equal(d.watermark[ROOM], 1, "the daemon-carried lmid folded into the ROOM domain's watermark");
  assert.deepEqual(d.pending, [], "the entry retired as a success");
  assert.deepEqual(rows(), [[1, "r"]], "…and the co-committed echo keeps the row (no snap-back)");
});

test("fold order pinned: a deopt ROW arriving in the SAME release as its covering lmid WINS over absence — flip, never retire (R4)", () => {
  const { daemonSrc, backend, mutate, rows } = setup();
  mutate.roomWrite({ id: 1, body: "r" }); // room mid 1, seq 1
  assert.deepEqual(rows(), [[1, "r"]]);
  const daemonBefore = daemonSrc.envelopes.length;

  // ONE release carries BOTH: the outcome row for the burnt mid AND the ledger row that covers
  // it (co-committed, one cv — exactly I-ii's wire shape). The structural fold order (outcomes
  // BEFORE ledger) is what the flip's survival proves: processed the other way round, the
  // covering lmid retires the entry first and the row lands on the not-found arm as a fresh
  // re-invocation — a NEW pending entry with a NEW seq (2) and a rewind flicker. (That reversed
  // run was executed as the violation-proof; this lane fails under it.)
  daemonSrc.batch(OUTCOMES_QID, [outcomeRow(DOC, "c1", 1, "deopt", { name: "roomWrite", args: { id: 1, body: "r" } })], 2);
  daemonSrc.batch(LEDGER_QID, [ledgerRow(DOC, "c1", 1)], 2);
  daemonSrc.frame(2);

  const d = backend.__inspectDomains();
  assert.deepEqual(
    d.pending.map((p) => [p.name, p.domain, p.mid, p.seq]),
    [["roomWrite", "daemon", 1, 1]],
    "flipped IN PLACE: daemon domain, fresh daemon mid, ORIGINAL seq — the H-v flip, not a retire-then-reinvoke",
  );
  assert.deepEqual(
    daemonSrc.envelopes.slice(daemonBefore).map((e) => [e.name, e.mid]),
    [["roomWrite", 1]],
    "deal-and-send-now: the envelope re-shipped on the daemon channel",
  );
  assert.equal(d.watermark[ROOM], 1, "the burnt room mid is still confirmed — the ledger fold ran, AFTER the verdict");
  assert.deepEqual(rows(), [[1, "r"]], "the prediction stayed applied throughout (no rewind/flicker)");

  // The daemon confirms the flipped mid → retired by ITS OWN watermark, converged.
  daemonSrc.batch(1, [add(1, "r")], 3);
  daemonSrc.lmid(1, 3);
  daemonSrc.frame(3);
  assert.deepEqual(backend.__inspectDomains().pending, [], "retired by the daemon confirm");
  assert.deepEqual(rows(), [[1, "r"]]);
});

test("deopt row across releases: a verdict resolved in an EARLIER release holds when the covering lmid arrives later", () => {
  const { daemonSrc, backend, mutate, rows } = setup();
  mutate.roomWrite({ id: 1, body: "r" }); // room mid 1, seq 1

  // Release 1: the outcome row alone (its release raced ahead of the ledger row's — the fold
  // must NOT assume same-release delivery).
  daemonSrc.batch(OUTCOMES_QID, [outcomeRow(DOC, "c1", 1, "deopt", { name: "roomWrite", args: { id: 1, body: "r" } })], 2);
  daemonSrc.frame(2);
  assert.deepEqual(
    backend.__inspectDomains().pending.map((p) => [p.domain, p.mid, p.seq]),
    [["daemon", 1, 1]],
    "flipped at ITS release",
  );

  // Release 2: the covering lmid. The entry is off the room domain already — nothing retires;
  // the processed set (the resolved-verdict memory) also absorbs any re-delivered row.
  daemonSrc.batch(LEDGER_QID, [ledgerRow(DOC, "c1", 1)], 3);
  daemonSrc.batch(OUTCOMES_QID, [outcomeRow(DOC, "c1", 1, "deopt", { name: "roomWrite", args: { id: 1, body: "r" } })], 3);
  daemonSrc.frame(3);
  const d = backend.__inspectDomains();
  assert.equal(d.pending.length, 1, "the flipped entry survived the covering lmid");
  assert.equal(d.watermark[ROOM], 1);
  assert.deepEqual(rows(), [[1, "r"]], "no flicker at either release");
});

test("fold order, fresh-session direction: historical outcome rows are judged BEFORE the ledger adopts the counter — no spurious re-invocation (R4)", () => {
  const { daemonSrc, backend } = setup();
  // A FRESH session over a clientID with history: nothing pending, nextMid[room] = 1. The boot
  // snapshot delivers the doc's retained state in ONE release: the ledger row (lmid 7) and a
  // historical deopt row (mid 3 — some PREVIOUS session's burnt mid, retained under the ≤512
  // window). Outcomes fold FIRST, so the row hits the "never issued here — not ours" guard
  // (mid 3 ≥ nextMid 1) and is correctly ignored; only then does the ledger adopt nextMid = 8.
  // Reversed (the violation-proof run), the adoption runs first, mid 3 passes the guard, finds
  // no entry, and the not-found arm re-invokes a mutation a PREVIOUS session already handled — a
  // spurious fresh prediction + daemon envelope (the double-apply this order exists to prevent).
  daemonSrc.snapshot(OUTCOMES_QID, [outcomeRow(DOC, "c1", 3, "deopt", { name: "roomWrite", args: { id: 3, body: "old" } })], 2);
  daemonSrc.snapshot(LEDGER_QID, [ledgerRow(DOC, "c1", 7)], 2);
  daemonSrc.frame(2);

  const d = backend.__inspectDomains();
  assert.deepEqual(d.pending, [], "no spurious re-invocation of the historical deopt");
  assert.equal(daemonSrc.envelopes.length, 0, "nothing shipped");
  assert.equal(d.nextMid[ROOM], 8, "the fresh session still adopted the historical counter");
  assert.equal(d.watermark[ROOM], 7, "…and the watermark");
});

test("rejected via row: FINAL — onRejected parity, burnt-mid retire + snap-back through the existing failed-mutation machinery", () => {
  const rejections: [string, number | null, string][] = [];
  const { daemonSrc, backend, mutate, rows } = setup({
    onRejected: (env, reason) => rejections.push([env.name, env.mid, reason]),
  });
  mutate.roomWrite({ id: 1, body: "r" }); // room mid 1
  assert.deepEqual(rows(), [[1, "r"]], "prediction applied");

  // One release: the rejected row + the covering ledger row, NO data (the room burnt the mid
  // with zero effects). The row surfaces the reason; the covering lmid retires the entry and the
  // reconcile snaps the prediction back — the exact daemon-path processed-as-no-op shape.
  daemonSrc.batch(OUTCOMES_QID, [outcomeRow(DOC, "c1", 1, "rejected", { reason: "nope" })], 2);
  daemonSrc.batch(LEDGER_QID, [ledgerRow(DOC, "c1", 1)], 2);
  daemonSrc.frame(2);

  assert.deepEqual(rejections, [["roomWrite", 1, "nope"]], "row-plane parity with the frame's onRejected");
  const d = backend.__inspectDomains();
  assert.deepEqual(d.pending, [], "retired by the burnt-mid coverage — a rejection is final, no flip");
  assert.deepEqual(rows(), [], "the prediction snapped back");
});

test("double delivery: the room socket's frame first, the daemon-carried row second ⇒ the second is a no-op (processed-set dedup)", () => {
  const { daemonSrc, roomSrc, backend, mutate, rows } = setup({ room: true });
  mutate.roomWrite({ id: 1, body: "r" }); // room mid 1 — ships on the ROOM socket
  assert.deepEqual(
    roomSrc.envelopes.map((e) => [e.name, e.mid]),
    [["roomWrite", 1]],
    "sent-pins-domain: the live room socket carried the envelope",
  );
  const daemonBefore = daemonSrc.envelopes.length;

  // The live room path resolves FIRST (out-of-band frame) — unaffected by the daemon plane.
  roomSrc.outcome({ mid: 1, kind: "deopt", name: "roomWrite", args: { id: 1, body: "r" } });
  let d = backend.__inspectDomains();
  assert.deepEqual(d.pending.map((p) => [p.domain, p.mid, p.seq]), [["daemon", 1, 1]], "flipped by the frame");
  assert.equal(daemonSrc.envelopes.length, daemonBefore + 1, "one re-ship");

  // The daemon-carried row + covering lmid for the SAME mid arrive later: the processed set
  // absorbs the row; the ledger fold finds the entry already off the room domain.
  daemonSrc.batch(OUTCOMES_QID, [outcomeRow(DOC, "c1", 1, "deopt", { name: "roomWrite", args: { id: 1, body: "r" } })], 2);
  daemonSrc.batch(LEDGER_QID, [ledgerRow(DOC, "c1", 1)], 2);
  daemonSrc.frame(2);

  d = backend.__inspectDomains();
  assert.equal(d.pending.length, 1, "no double-invoke, no retire");
  assert.equal(d.pending[0].seq, 1, "still the flipped original (not a fresh re-invocation)");
  assert.equal(daemonSrc.envelopes.length, daemonBefore + 1, "no second re-ship");
  assert.equal(d.watermark[ROOM], 1, "the burnt mid's coverage still folded");
  assert.deepEqual(rows(), [[1, "r"]]);
});

test("cv coherence: system frames buffer behind the daemon gate — cv > cvMin folds NOTHING early (§5.1)", () => {
  const { daemonSrc, backend, mutate } = setup();
  mutate.roomWrite({ id: 1, body: "r" }); // room mid 1

  // A deopt row + a watermark row at cv 5, released only to cvMin 3: neither may fold.
  daemonSrc.batch(OUTCOMES_QID, [outcomeRow(DOC, "c1", 1, "deopt", { name: "roomWrite", args: { id: 1, body: "r" } })], 5);
  daemonSrc.batch(WATERMARK_QID, [watermarkRow(DOC, 7)], 5);
  daemonSrc.frame(3);
  let d = backend.__inspectDomains();
  assert.deepEqual(d.pending.map((p) => [p.domain, p.mid]), [[ROOM, 1]], "buffered ≠ folded: no early flip");
  assert.deepEqual(d.lifecycle.roomWatermarks, {}, "no early fence advance");
  assert.equal(d.gates.daemon.bufferedFrames, 2, "both frames held behind the gate");

  // The release point arrives: both fold in one coherent step.
  daemonSrc.frame(5);
  d = backend.__inspectDomains();
  assert.deepEqual(d.pending.map((p) => [p.domain, p.mid, p.seq]), [["daemon", 1, 1]], "flipped at the release");
  assert.deepEqual(d.lifecycle.roomWatermarks, { [DOC]: 7 });
});

// ============================================================================================
// Late/stale rows, and the doorbell/fence bookkeeping axes
// ============================================================================================

test("late/stale: rows for another doc or another clientID are ignored (defense in depth under a doc-only server predicate)", () => {
  const { daemonSrc, backend, mutate } = setup();
  mutate.roomWrite({ id: 1, body: "r" }); // room mid 1 pending

  daemonSrc.batch(
    LEDGER_QID,
    [
      ledgerRow("doc:9", "c1", 7), // another doc (a mis-scoped or doc-only predicate)
      ledgerRow(DOC, "c2", 7), // another client sharing the doc
    ],
    2,
  );
  daemonSrc.batch(
    OUTCOMES_QID,
    [outcomeRow(DOC, "c2", 1, "rejected", { reason: "not ours" })], // another client's verdict
    2,
  );
  daemonSrc.frame(2);

  const d = backend.__inspectDomains();
  assert.equal(d.watermark[ROOM] ?? 0, 0, "a foreign ledger row confirms nothing");
  assert.equal(d.watermark["room:doc:9"] ?? 0, 0, "…and never folds under a doc this retain wasn't minted for");
  assert.deepEqual(d.pending.map((p) => [p.domain, p.mid]), [[ROOM, 1]], "the pending entry is untouched");
  assert.equal(d.nextMid[ROOM], 2, "no counter adoption off a foreign row");
});

test("watermark fold: per-doc, monotone, exposed via __inspectDomains().lifecycle (the I-v fence input)", () => {
  const { daemonSrc, backend } = setup();
  daemonSrc.batch(WATERMARK_QID, [watermarkRow(DOC, 5)], 2);
  daemonSrc.frame(2);
  assert.deepEqual(backend.__inspectDomains().lifecycle.roomWatermarks, { [DOC]: 5 });

  // Monotone: a stale/re-delivered lower seq never regresses the fence; a remove never clears it.
  daemonSrc.batch(WATERMARK_QID, [watermarkRow(DOC, 3)], 3);
  daemonSrc.batch(WATERMARK_QID, [{ table: ROOM_WATERMARK_TABLE, op: "remove", row: [DOC, 5] } as NormalizedOp], 3);
  daemonSrc.frame(3);
  assert.deepEqual(backend.__inspectDomains().lifecycle.roomWatermarks, { [DOC]: 5 });

  // A foreign doc's row is not this retain's to fold.
  daemonSrc.batch(WATERMARK_QID, [watermarkRow("doc:9", 9)], 4);
  daemonSrc.frame(4);
  assert.deepEqual(backend.__inspectDomains().lifecycle.roomWatermarks, { [DOC]: 5 });
});

test("scope sessions fold: batch add/edit/remove maintain the occupancy map; a snapshot REPLACES it (the I-iv doorbell input; no reactions)", () => {
  const { daemonSrc, backend } = setup();
  // Two sessions arrive (the doorbell shape: our own row + a collaborator's).
  daemonSrc.batch(SESSIONS_QID, [sessionRow(DOC, "c1", 1_000), sessionRow(DOC, "c2", 2_000)], 2);
  daemonSrc.frame(2);
  assert.deepEqual(backend.__inspectDomains().lifecycle.scopeSessions, { [DOC]: { c1: 1_000, c2: 2_000 } });

  // A refresh (edit) bumps expiry; the age-out sweep's delete drops a session.
  daemonSrc.batch(
    SESSIONS_QID,
    [
      { table: SCOPE_SESSIONS_TABLE, op: "edit", old: [DOC, "c1", 1_000], new: [DOC, "c1", 5_000] } as NormalizedOp,
      sessionRow(DOC, "c2", 2_000, "remove"),
    ],
    3,
  );
  daemonSrc.frame(3);
  assert.deepEqual(backend.__inspectDomains().lifecycle.scopeSessions, { [DOC]: { c1: 5_000 } });

  // A foreign scope's row is ignored (the retain was minted for DOC).
  daemonSrc.batch(SESSIONS_QID, [sessionRow("other/scope", "c9", 9_000)], 4);
  daemonSrc.frame(4);
  assert.deepEqual(backend.__inspectDomains().lifecycle.scopeSessions, { [DOC]: { c1: 5_000 } });

  // A re-hydrate snapshot is authoritative: the scope map REPLACES (c1's stale entry drops).
  daemonSrc.snapshot(SESSIONS_QID, [sessionRow(DOC, "c3", 7_000)], 5);
  daemonSrc.frame(5);
  assert.deepEqual(backend.__inspectDomains().lifecycle.scopeSessions, { [DOC]: { c3: 7_000 } });
});

test("release: the last releaseSystemQuery unsubscribes the wire sub, sweeps its buffer, and keeps the folded state", () => {
  const { daemonSrc, backend } = setup();
  daemonSrc.batch(WATERMARK_QID, [watermarkRow(DOC, 5)], 2);
  daemonSrc.frame(2);
  // A buffered frame past the cvMin stays; releasing the retain must sweep it, not fold it.
  daemonSrc.batch(WATERMARK_QID, [watermarkRow(DOC, 9)], 9);
  backend.releaseSystemQuery(WATERMARK_QID);
  assert.deepEqual(daemonSrc.unregistered, [WATERMARK_QID], "unsubscribed from the daemon source");
  daemonSrc.frame(9);
  const d = backend.__inspectDomains();
  assert.deepEqual(d.lifecycle.roomWatermarks, { [DOC]: 5 }, "the swept frame never folded; the folded state survives");
});

// ============================================================================================
// The I-iv doorbell events (§4.1): (scope, other-unexpired count) after each occupancy fold
// ============================================================================================

/** A frozen-hand virtual clock for the doorbell lanes — expiry verdicts must be deterministic
 *  (the folded-oracle discipline: the count rule reads the INJECTED clock, never Date.now). No
 *  timers fire in these lanes, so the timer half is inert stubs. */
class TestClock implements FoldClock {
  t = 0;
  now(): number {
    return this.t;
  }
  setTimeout(): number {
    return 0;
  }
  clearTimeout(): void {}
}

test("I-iv doorbell event: fires once per touched scope with the OTHER-unexpired count; own rows and expired rows never count (virtual clock)", () => {
  const clock = new TestClock();
  clock.t = 10_000;
  const { daemonSrc, backend } = setup({ clock });
  const events: ScopeSessionsEvent[] = [];
  backend.onScopeSessions((ev) => events.push({ ...ev }));

  // Snapshot: our OWN row (c1, unexpired) + an EXPIRED other (c9). Both never count — the event
  // still fires (the fold touched the scope) but reports 0: the CLIENT's transition tracker is
  // what turns counts into triggers, and 0 cannot ring it. Violation-proof both ways: counting
  // own rows or expired rows would report 1 here and fail.
  daemonSrc.snapshot(SESSIONS_QID, [sessionRow(DOC, "c1", 99_000), sessionRow(DOC, "c9", 9_000)], 2);
  daemonSrc.frame(2);
  assert.deepEqual(events, [{ scope: DOC, others: 0 }], "own-only + expired-other ⇒ count 0");

  // Another clientID's UNEXPIRED row lands (the upgrade doorbell proper): count 1.
  daemonSrc.batch(SESSIONS_QID, [sessionRow(DOC, "c2", 99_000)], 3);
  daemonSrc.frame(3);
  assert.deepEqual(events.at(-1), { scope: DOC, others: 1 });
  assert.equal(backend.otherScopeSessions(DOC), 1, "the public counter is the SAME rule (registration-time check)");

  // Expiry is judged at the injected clock: advance past c2's expiry — the folded STATE is
  // unchanged, the verdict flips.
  clock.t = 100_000;
  assert.equal(backend.otherScopeSessions(DOC), 0, "expired other ⇒ no longer counted");

  // The D4 sweep's delete arrives as a remove op: event fires with the post-remove count.
  daemonSrc.batch(SESSIONS_QID, [sessionRow(DOC, "c2", 99_000, "remove")], 4);
  daemonSrc.frame(4);
  assert.deepEqual(events.at(-1), { scope: DOC, others: 0 });

  // A release with NO occupancy rows fires NO event (the seam is structurally silent otherwise).
  const before = events.length;
  daemonSrc.batch(1, [add(1, "x")], 5);
  daemonSrc.frame(5);
  assert.equal(events.length, before, "data-only release: no doorbell event");
});

test("I-iv doorbell event: fires AFTER the whole release applied (the reaction sees post-release state), and buffers behind the gate like any system frame", () => {
  const clock = new TestClock();
  clock.t = 10_000;
  const { daemonSrc, backend, rows } = setup({ clock });
  const seen: Array<{ others: number; dataRows: number }> = [];
  backend.onScopeSessions((ev) => seen.push({ others: ev.others, dataRows: rows().length }));

  // One release carrying BOTH a data batch and the occupancy row: when the event fires, the
  // data half of the release must already be visible (the doorbell reaction re-leases against
  // post-release state — never mid-fold).
  daemonSrc.batch(1, [add(7, "seven")], 2);
  daemonSrc.batch(SESSIONS_QID, [sessionRow(DOC, "c2", 99_000)], 2);
  assert.equal(seen.length, 0, "buffered ≠ folded (cv 2 > cvMin 1)");
  daemonSrc.frame(2);
  assert.deepEqual(seen, [{ others: 1, dataRows: 1 }], "event AFTER the co-released data applied");
});

// ============================================================================================
// Slice I-v — the §4.2 downgrade ghost: demote → freeze → watermark fence → drop
// ============================================================================================

/** setup() + a connected room gate. The ghost's fence/stuck machinery is table-independent
 *  (302: the swap-back gate is bookkeeping over the pending set + the watermark fold); the
 *  swapped-VIEW half is pinned by the dedicated swap lane below. The four system streams
 *  (incl. the §4.2 watermark fence) are already retained. */
function setupGhost() {
  return setup({ room: true });
}

test("302 §4 swap boundaries: a room-homed view swaps onto the room tables at hydration, HOLDS them under follower lag through a downgrade, and swaps back at the fence — value-equal, no visible regression (§4.2/§8.1)", () => {
  const { daemonSrc, roomSrc, backend, rows, view } = setupGhost();
  // Register the room's owned table, then MOVE the view's live sub onto the ROOM channel — the
  // I-iv upgrade retarget (setup()'s materialize already retained it on the daemon).
  backend.registerRoomTables(ROOM, ["note"]);
  const sourceQid = backend.retargetRemoteQuery({ name: "allNotes", args: null }, ROOM);

  // The room's seq-0 snapshot releases → the reconcile folds it into note@ROOM → the view SWAPS
  // onto the room table at the release tail (302 §4.1) and shows the room state.
  roomSrc.snapshot(sourceQid, [add(1, "final")], 1);
  roomSrc.frame(1);
  assert.deepEqual(rows(), [[1, "final"]], "swap-in: the view reads the room's own table");
  const d0 = backend.__inspectDomains();
  assert.equal(d0.swappedViews[view.qid], ROOM, "the view is recorded as room-swapped");
  assert.deepEqual(d0.roomTables[ROOM], { note: "note@" + ROOM }, "the namespaced twin exists");

  // A LAGGING FOLLOWER: the daemon carries a STALE pre-flush image (the gate-less release seam —
  // the retargeted sub no longer accepts daemon frames, exactly the wiring rule). One authority
  // per table — the daemon delta lands on PLAIN note, which the swapped view no longer reads.
  // No merge, no flicker (302 §2).
  backend.__testRelease("daemon", [{ op: "add", table: "note", row: [1, "stale"] }]);
  assert.deepEqual(rows(), [[1, "final"]], "the room table shields the view from the lagging daemon image");

  // Downgrade behind fence = 5 (the room's last committed flush seq): retarget the sub off the
  // room first (the demote validates), then demote. The view KEEPS reading note@ROOM.
  backend.retargetRemoteQuery({ name: "allNotes", args: null }, "daemon");
  backend.demoteRoomSource(ROOM, DOC, 5);
  assert.deepEqual(Object.keys(backend.__inspectDomains().lifecycle.ghosts), [ROOM], "ghost recorded");
  assert.deepEqual(rows(), [[1, "final"]], "the frozen room table still supplies the row");

  // Watermark BELOW the fence ⇒ the swap-back gate HOLDS (§4.2 monotone — the user never sees
  // the doc move backward). VIOLATION-PROOF: drop the ghost immediately (skip the fence check)
  // and "stale" surfaces here — the final assertion fails.
  daemonSrc.batch(WATERMARK_QID, [watermarkRow(DOC, 3)], 3);
  daemonSrc.frame(3);
  const d1 = backend.__inspectDomains();
  assert.equal(d1.lifecycle.roomWatermarks[DOC], 3, "watermark folded (still < fence)");
  assert.deepEqual(Object.keys(d1.lifecycle.ghosts), [ROOM], "fence unmet ⇒ ghost HOLDS");
  assert.deepEqual(rows(), [[1, "final"]], "the room table wins over the lagging daemon image");

  // The follower catches up: the daemon converges to the room's final value, THEN the watermark
  // reaches the fence. The ghost drops; the view swaps BACK onto the plain table — value-equal
  // under the fence — and the room table unregisters (§8.1: per-watermark equality every step).
  backend.__testRelease("daemon", [{ op: "edit", table: "note", old: [1, "stale"], new: [1, "final"] }]);
  assert.deepEqual(rows(), [[1, "final"]], "still shielded while the fence holds");
  daemonSrc.batch(WATERMARK_QID, [watermarkRow(DOC, 5)], 4);
  daemonSrc.frame(4);
  const d2 = backend.__inspectDomains();
  assert.deepEqual(Object.keys(d2.lifecycle.ghosts), [], "ghost dropped at the fence");
  assert.deepEqual(d2.swappedViews, {}, "the view swapped back to the plain table");
  assert.deepEqual(d2.roomTables, {}, "the namespaced twin unregistered");
  assert.deepEqual(rows(), [[1, "final"]], "value-equal: the swap-back surfaced nothing");
});

test("302 §4.2 bounce: a down→up re-upgrade CANCELS the pending swap-back — the old fence clearing later never dismantles the live room", () => {
  const { daemonSrc, roomSrc, backend, rows, view } = setupGhost();
  backend.registerRoomTables(ROOM, ["note"]);
  const q1 = backend.retargetRemoteQuery({ name: "allNotes", args: null }, ROOM);
  roomSrc.snapshot(q1, [add(1, "room")], 1);
  roomSrc.frame(1);
  assert.equal(backend.__inspectDomains().swappedViews[view.qid], ROOM, "swapped in");

  // Downgrade behind fence 5 (retarget off first; the demote validates), then IMMEDIATELY
  // re-upgrade — the self-heal case: a collaborator is still present at downgrade, the doorbell
  // re-rings before the old fence ever clears.
  backend.retargetRemoteQuery({ name: "allNotes", args: null }, "daemon");
  backend.demoteRoomSource(ROOM, DOC, 5);
  assert.deepEqual(Object.keys(backend.__inspectDomains().lifecycle.ghosts), [ROOM], "ghost armed");
  const roomSrc2 = new TestSource();
  backend.connectSource(ROOM, roomSrc2); // adopts the surviving twin tables…
  assert.deepEqual(Object.keys(backend.__inspectDomains().lifecycle.ghosts), [], "…and CANCELS the obsolete swap-back");
  const q2 = backend.retargetRemoteQuery({ name: "allNotes", args: null }, ROOM);
  roomSrc2.snapshot(q2, [add(1, "room2")], 1);
  roomSrc2.frame(1);
  assert.deepEqual(rows(), [[1, "room2"]], "the re-upgraded room feeds the still-swapped view");

  // The OLD fence clears through the daemon plane. Pre-fix this fired dropGhost against the LIVE
  // room: views un-swapped, twins unregistered under the connected gate's tableMap — and the next
  // room release threw from serverBatchBegin, wedging the whole feed.
  daemonSrc.batch(WATERMARK_QID, [watermarkRow(DOC, 5)], 2);
  daemonSrc.frame(2);
  const d = backend.__inspectDomains();
  assert.equal(d.swappedViews[view.qid], ROOM, "views stay swapped on the live room");
  assert.deepEqual(d.roomTables[ROOM], { note: "note@" + ROOM }, "the twin survives");
  roomSrc2.batch(q2, [add(2, "more")], 2);
  roomSrc2.frame(2);
  assert.deepEqual(rows(), [[1, "room2"], [2, "more"]], "the room feed still applies after the stale fence cleared");
});

test("302 §4.2 co-tenant double-demote: the second demote folds its fence in (monotone max) — the swap-back waits for the NEWEST flush", () => {
  const { daemonSrc, roomSrc, backend, rows, view } = setupGhost();
  backend.registerRoomTables(ROOM, ["note"]);
  const q1 = backend.retargetRemoteQuery({ name: "allNotes", args: null }, ROOM);
  roomSrc.snapshot(q1, [add(1, "final")], 1);
  roomSrc.frame(1);

  backend.retargetRemoteQuery({ name: "allNotes", args: null }, "daemon");
  backend.demoteRoomSource(ROOM, DOC, 5); // co-tenant 1's fence
  backend.demoteRoomSource(ROOM, DOC, 7); // co-tenant 2 demotes into the SAME ghost with a newer flush
  assert.equal(
    backend.__inspectDomains().lifecycle.ghosts[ROOM].finalFlushSeq,
    7,
    "the fence folded to the max (pre-fix: the second demote's early-return dropped it)",
  );

  // Converge the daemon first (value-equal swap-back), then clear only the OLD fence — must HOLD:
  // swapping back at 5 would surface the follower's pre-flush images for co-tenant 2's writes.
  backend.__testRelease("daemon", [{ op: "add", table: "note", row: [1, "final"] }]);
  daemonSrc.batch(WATERMARK_QID, [watermarkRow(DOC, 5)], 2);
  daemonSrc.frame(2);
  assert.deepEqual(Object.keys(backend.__inspectDomains().lifecycle.ghosts), [ROOM], "old fence ⇒ the ghost HOLDS");
  assert.equal(backend.__inspectDomains().swappedViews[view.qid], ROOM, "still reading the frozen room table");

  daemonSrc.batch(WATERMARK_QID, [watermarkRow(DOC, 7)], 3);
  daemonSrc.frame(3);
  const d = backend.__inspectDomains();
  assert.deepEqual(Object.keys(d.lifecycle.ghosts), [], "dropped at the NEWEST fence");
  assert.deepEqual(d.swappedViews, {}, "swap-back ran");
  assert.deepEqual(rows(), [[1, "final"]], "value-equal");
});

test("I-v ghost: a never-flushed room (fence 0) drops on the first evaluation — no watermark row", () => {
  const { daemonSrc, backend, rows } = setupGhost();
  // The room's seq-0 snapshot was built FROM the daemon, so both hold the row.
  daemonSrc.batch(1, [add(1, "r")], 2);
  daemonSrc.frame(2);
  assert.deepEqual(rows(), [[1, "r"]]);

  backend.demoteRoomSource(ROOM, DOC, 0); // fence 0 ⇒ trivially satisfied at demote time
  const d = backend.__inspectDomains();
  assert.deepEqual(Object.keys(d.lifecycle.ghosts), [], "dropped at demote, no watermark needed");
  assert.deepEqual(d.lifecycle.roomWatermarks, {}, "…and no watermark row ever folded");
  assert.deepEqual(rows(), [[1, "r"]], "value-equal drop — the daemon holds the same row");
});

test("I-v ghost: a SENT room-domain mid holds the ghost past its fence — exactly ONE onDowngradeStuck; resolution via the daemon-carried ledger drops it", () => {
  const { daemonSrc, backend, mutate } = setupGhost();
  const stuck: DowngradeStuckEvent[] = [];
  backend.onDowngradeStuck((e) => stuck.push({ ...e, mids: [...e.mids] }));

  mutate.roomWrite({ id: 1, body: "r" }); // routes to the room, deals + SENDS room mid 1
  const p = backend.__inspectDomains().pending.find((x) => x.domain === ROOM);
  assert.ok(p !== undefined && p.mid === 1, "a sent room-domain mid is pending");

  backend.demoteRoomSource(ROOM, DOC, 0); // fence trivially satisfied, but the sent mid remains
  assert.deepEqual(Object.keys(backend.__inspectDomains().lifecycle.ghosts), [ROOM], "the ghost HOLDS on the sent mid (§7.5)");
  assert.deepEqual(stuck.map((e) => [e.doc, e.mids]), [[DOC, [1]]], "one stuck event, naming the mid");

  // Another fence-satisfied release with the mid STILL unresolved ⇒ NO second event (fire-once).
  daemonSrc.batch(WATERMARK_QID, [watermarkRow(DOC, 9)], 2);
  daemonSrc.frame(2);
  assert.equal(stuck.length, 1, "at most once");
  assert.deepEqual(Object.keys(backend.__inspectDomains().lifecycle.ghosts), [ROOM], "still held");

  // The daemon-carried ledger finally covers the room mid ⇒ retire ⇒ the ghost drops (§7.1).
  daemonSrc.batch(LEDGER_QID, [ledgerRow(DOC, "c1", 1)], 3);
  daemonSrc.frame(3);
  assert.deepEqual(backend.__inspectDomains().pending, [], "the sent mid retired via the daemon-carried ledger");
  assert.deepEqual(Object.keys(backend.__inspectDomains().lifecycle.ghosts), [], "ghost drops after resolution");
});

test("I-v deopt continuity across downgrade: a room deopt unresolved at demote flips to the daemon via the daemon-carried outcome, prediction applied throughout; the ghost drops after", () => {
  const { daemonSrc, backend, mutate, rows } = setupGhost();
  mutate.roomWrite({ id: 1, body: "r" }); // room mid 1, prediction applied
  assert.deepEqual(rows(), [[1, "r"]], "prediction applied");

  // Demote while the room mid is still SENT/unresolved: the ghost HOLDS on it (§7.5).
  backend.demoteRoomSource(ROOM, DOC, 0);
  assert.deepEqual(Object.keys(backend.__inspectDomains().lifecycle.ghosts), [ROOM], "ghost holds on the sent mid");
  assert.deepEqual(rows(), [[1, "r"]], "prediction STILL applied post-disconnect (no room socket alive)");

  // The daemon-carried OUTCOME (deopt) + covering ledger — read THROUGH THE DAEMON with no room
  // socket, §7.1 — resolve it: the entry FLIPS to the daemon (fresh daemon mid, ORIGINAL seq — the
  // H-v flip), re-shipped on the daemon; the room ledger stays gapless. The prediction never lifts.
  daemonSrc.batch(OUTCOMES_QID, [outcomeRow(DOC, "c1", 1, "deopt", { name: "roomWrite", args: { id: 1, body: "r" } })], 2);
  daemonSrc.batch(LEDGER_QID, [ledgerRow(DOC, "c1", 1)], 2);
  daemonSrc.frame(2);
  const d = backend.__inspectDomains();
  assert.deepEqual(d.pending.map((p) => [p.domain, p.mid, p.seq]), [["daemon", 1, 1]], "flipped to the daemon (H-v)");
  assert.equal(d.watermark[ROOM], 1, "the room ledger is gapless (the burnt mid confirmed)");
  assert.deepEqual(rows(), [[1, "r"]], "prediction applied throughout — no rewind");
  // No room-domain pending remains (it flipped to daemon) ⇒ the ghost's second conjunct clears ⇒ drop.
  assert.deepEqual(Object.keys(backend.__inspectDomains().lifecycle.ghosts), [], "ghost drops once no room-domain pending remains");

  // The daemon confirms the flipped mid → converged, both ledgers gapless.
  daemonSrc.batch(1, [add(1, "r")], 3);
  daemonSrc.lmid(1, 3);
  daemonSrc.frame(3);
  assert.deepEqual(backend.__inspectDomains().pending, [], "converged on the daemon");
  assert.deepEqual(rows(), [[1, "r"]]);
});

test("I-v re-upgrade: demote KEEPS the room's nextMid (§7.1); a re-connected room CONTINUES the mid sequence, never restarts", () => {
  const { daemonSrc, backend, mutate } = setupGhost();
  mutate.roomWrite({ id: 1, body: "r" }); // burns room mid 1
  assert.equal(backend.__inspectDomains().nextMid[ROOM], 2, "room counter advanced 1→2");

  // Resolve the sent mid via the daemon-carried ledger, then demote (fence 0 ⇒ drop, nothing pending).
  daemonSrc.batch(LEDGER_QID, [ledgerRow(DOC, "c1", 1)], 2);
  daemonSrc.frame(2);
  assert.deepEqual(backend.__inspectDomains().pending, [], "room mid 1 resolved");
  backend.demoteRoomSource(ROOM, DOC, 0);
  assert.deepEqual(Object.keys(backend.__inspectDomains().lifecycle.ghosts), [], "ghost dropped");
  assert.equal(backend.__inspectDomains().nextMid[ROOM], 2, "nextMid SURVIVED the demote (§7.1)");

  // Re-upgrade the SAME doc from scratch: a fresh channel connect.
  backend.connectSource(ROOM, new TestSource());
  mutate.roomWrite({ id: 2, body: "r2" }); // the NEXT room write
  const d = backend.__inspectDomains();
  assert.equal(d.nextMid[ROOM], 3, "the mid sequence CONTINUED (2→3), never restarted");
  assert.equal(d.pending.find((p) => p.domain === ROOM)?.mid, 2, "the re-upgraded write got the CONTINUED mid 2");
});
