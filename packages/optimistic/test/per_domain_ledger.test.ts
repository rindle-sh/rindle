// Per-domain mutation ledger (RINDLE-REALTIME-QUERY-ENABLEMENT-DESIGN.md §7.1 domain-scoped ledger,
// §7.2 per-domain confirm-drop, §8.5 ledger isolation). Slice E-iii-a generalizes the client
// optimistic store's single (daemon) lmid counter to PER-DOMAIN, so a client writing through
// room + daemon concurrently cannot alias one lmid sequence. This suite pins the §8.5 isolation
// property directly — the exact Rev-1 collision the design fixes:
//
//   - per-domain mid gaplessness: each confirming stream deals `1,2,3…` from its OWN counter,
//     independent of the others (a daemon deal never bumps the room counter, and vice-versa);
//   - a DAEMON confirm (driven the live way — a daemon lmid frame folded by the release gate)
//     retires only daemon-domain pending and never perturbs the room ledger;
//   - a ROOM confirm (driven through the `__testRelease` per-source seam, since the real second
//     lmid stream is a later slice) retires only room-domain pending and leaves the daemon ledger
//     untouched — even when both entries carry the SAME raw mid (the collision that a single
//     scalar counter would mis-retire).
//
// The domain routing is DECLARED via `domainPolicy` (302 §5: declared, not derived); every
// live/default path still resolves "daemon", which the whole rest of the @rindle/optimistic
// suite proves is byte-for-byte unchanged.
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
  type NormalizedEvent,
  type NormalizedOp,
  type NormalizedTableSchema,
  type OptimisticSource,
  type ProgressFrame,
  type QueryId,
  type RemoteQuery,
} from "@rindle/wasm";
import { createOptimisticStore, type ClientRegistry, type MutationTx } from "../src/index.ts";

await initWasm();

const note = table("note").columns({ id: number(), body: string() }).primaryKey("id");
const schema = createSchema({ tables: [note] });
const qb = newQueryBuilder(schema);
const allNotes = defineQuery("allNotes", () => qb.note.orderBy("id", "asc"));

/** A hand-scripted source: the ledger tests drive at most a daemon lmid confirm (`lmid`+`frame`); the
 *  room confirm rides the `__testRelease` seam, so most hooks are no-op recorders. Mirrors the
 *  minimal `TestSource` in `backend.test.ts`. */
class TestSource implements OptimisticSource {
  normalized: (qid: QueryId, ev: NormalizedEvent) => void = () => {};
  progress: (frame: ProgressFrame) => void = () => {};
  envelopes: MutationEnvelope[] = [];

  registerQuery(_qid: QueryId, _remote: RemoteQuery): void {}
  unregisterQuery(_qid: QueryId): void {}
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
  onRestart(_h: () => void): void {}
  expectClientSchema(_schemas: NormalizedTableSchema[]): void {}

  /** The daemon lmid system query's data frame (qid 0): the server confirming `mid` at `cv`,
   *  co-transactional with that commit's data (lmid-as-data). Folds into `watermark["daemon"]`. */
  lmid(mid: number, cv: number): void {
    this.normalized(0, {
      type: "batch",
      ops: [{ table: "_rindle_client_mutations", op: "add", row: ["c1", mid] } as NormalizedOp],
      cv,
    });
  }
  frame(cvMin: number): void {
    this.progress({ cvMin });
  }
}

// Two mutators that write the same table at distinct pks. `upsert` (not `insert`) so a reconcile
// re-invocation is idempotent: the room-domain per-source rewind is a no-op under today's wasm ABI
// (no room source is wired — the E-iii-b/c gap), so a surviving daemon entry re-applies over head
// that still holds its own row; upsert absorbs that instead of a duplicate-pk throw.
const registry = {
  daemonWrite: (tx: MutationTx, a: { id: number; body: string }) => tx.upsert("note", { id: a.id, body: a.body }),
  roomWrite: (tx: MutationTx, a: { id: number; body: string }) => tx.upsert("note", { id: a.id, body: a.body }),
  /** READ-DEPENDENT room-domain mutator (the §5.3 replay-order probe): appends to whatever body
   *  it reads, so a replay against a different base produces a visibly different prediction. */
  roomAppend: (tx: MutationTx, a: { id: number }) => {
    const cur = tx.row("note", { id: a.id });
    tx.upsert("note", { id: a.id, body: `${(cur?.body as string) ?? ""}+B` });
  },
} satisfies ClientRegistry;

/** Route the `room*` mutators to the room domain, everything else to the daemon (§7.1). The live
 *  default (`() => "daemon"`) is what every OTHER suite exercises — this seam is the only
 *  per-domain input. */
const domainPolicy = (name: string): string => (name.startsWith("room") ? "room:doc:1" : "daemon");

function setup() {
  const source = new TestSource();
  const { store, backend, mutate } = createOptimisticStore(schema, source, registry, {
    clientID: "c1",
    domainPolicy,
  });
  return { source, store, backend, mutate };
}

test("per-domain mid gaplessness: each domain deals its own independent, gapless sequence (§7.1)", () => {
  const { mutate, backend } = setup();
  // Interleave the two streams; each mid must come from ITS domain's counter, not a shared one.
  assert.equal(mutate.roomWrite({ id: 1, body: "a" }), 1, "room deals 1 from its own counter");
  assert.equal(mutate.daemonWrite({ id: 101, body: "a" }), 1, "daemon deals 1 independently (not 2)");
  assert.equal(mutate.roomWrite({ id: 2, body: "b" }), 2, "room continues at 2 — the daemon deal did not bump it");
  assert.equal(mutate.daemonWrite({ id: 102, body: "b" }), 2, "daemon continues at 2");
  assert.equal(mutate.roomWrite({ id: 3, body: "c" }), 3, "room continues at 3 — gapless WITHIN its domain");

  const d = backend.__inspectDomains();
  assert.deepEqual(
    d.nextMid,
    { daemon: 3, "room:doc:1": 4 },
    "per-domain next-mid: room dealt 1,2,3 → 4; daemon dealt 1,2 → 3 — never aliased",
  );
  assert.equal(
    backend.__inspect().nextMid,
    3,
    "back-compat: the devtools OptimisticInspect scalar tracks the daemon domain only",
  );
});

test("a daemon confirm retires ONLY daemon-domain pending; the room ledger is untouched (§7.2)", () => {
  const { source, mutate, backend } = setup();
  const roomMid = mutate.roomWrite({ id: 1, body: "r" });
  const daemonMid = mutate.daemonWrite({ id: 2, body: "d" });
  // Both entries carry raw mid 1 — the Rev-1 collision a single scalar counter would mis-retire.
  assert.equal(roomMid, 1, "room entry's mid is 1 (its own counter)");
  assert.equal(daemonMid, 1, "daemon entry's mid is ALSO 1 (its own counter) — same raw value, different domain");

  let d = backend.__inspectDomains();
  // A domain enters the watermark map only when a confirm folds into it (absent ⇒ effective 0).
  assert.deepEqual(d.watermark, { daemon: 0 }, "nothing confirmed yet; the room domain has no watermark entry");
  assert.deepEqual(d.pending.map((p) => p.domain).sort(), ["daemon", "room:doc:1"], "both entries pending");

  // Daemon confirms mid 1 the LIVE way: its lmid frame releases at cvMin and the gate folds it into
  // watermark["daemon"] (computeRelease → foldLmidOps("daemon") → applyRelease("daemon")).
  source.lmid(1, 1);
  source.frame(1);

  d = backend.__inspectDomains();
  assert.deepEqual(d.watermark, { daemon: 1 }, "daemon watermark advanced to 1; the room domain was never even touched");
  assert.deepEqual(d.nextMid, { daemon: 2, "room:doc:1": 2 }, "neither counter perturbed by the confirm");
  assert.deepEqual(
    d.pending.map((p) => ({ mid: p.mid, domain: p.domain })),
    [{ mid: 1, domain: "room:doc:1" }],
    "the daemon entry (mid 1) retired; the ROOM entry (mid 1) SURVIVES — confirm-drop is per-domain",
  );
  // Devtools scalar reflects the daemon domain (back-compat).
  assert.equal(backend.__inspect().confirmedLmid, 1, "the devtools confirmedLmid scalar tracks the daemon watermark");
});

test("a room confirm retires ONLY room-domain pending; the daemon ledger is untouched (§7.2)", () => {
  const { mutate, backend } = setup();
  const roomMid = mutate.roomWrite({ id: 1, body: "r" });
  const daemonMid = mutate.daemonWrite({ id: 2, body: "d" });
  assert.equal(roomMid, 1);
  assert.equal(daemonMid, 1, "again both raw mids are 1 — the collision case");

  // A per-source release confirms ROOM mid 1 — the seam the real room lmid stream drives later
  // (E-iii-b/c). A room delta MUST be empty today (no room source exists to rewind into); the
  // watermarkUpdate carries the confirm.
  backend.__testRelease("room:doc:1", [], 1);

  const d = backend.__inspectDomains();
  assert.deepEqual(d.watermark, { daemon: 0, "room:doc:1": 1 }, "room watermark advanced to 1; DAEMON watermark still 0");
  assert.deepEqual(d.nextMid, { daemon: 2, "room:doc:1": 2 }, "neither counter perturbed by the confirm");
  assert.deepEqual(
    d.pending.map((p) => ({ mid: p.mid, domain: p.domain })),
    [{ mid: 1, domain: "daemon" }],
    "the room entry (mid 1) retired; the DAEMON entry (mid 1) SURVIVES — a room confirm can't reach it",
  );
  assert.equal(backend.__inspect().confirmedLmid, 0, "the daemon scalar (devtools) is unmoved by a room confirm");
});

test("both streams confirm independently → both entries retire, both watermarks advance (§7.1)", () => {
  const { source, mutate, backend } = setup();
  mutate.roomWrite({ id: 1, body: "r" });
  mutate.daemonWrite({ id: 2, body: "d" });

  // Room first (per-source seam), then daemon (live lmid path).
  backend.__testRelease("room:doc:1", [], 1);
  source.lmid(1, 1);
  source.frame(1);

  const d = backend.__inspectDomains();
  assert.deepEqual(d.watermark, { daemon: 1, "room:doc:1": 1 }, "each domain's watermark advanced by its own confirm");
  assert.deepEqual(d.pending, [], "both entries retired — no cross-domain leakage stranded either one");
});

test("replay order is the client-global send order, NOT per-domain mid order (§5.3)", () => {
  const { source, store, backend, mutate } = setup();
  const view = store.materialize(allNotes());
  const rows = (): unknown[][] => view.data.map((r) => [r.id, r.body]);
  // Hydrate the data query with an empty daemon snapshot so the view is live.
  source.normalized(1, { type: "snapshot", ops: [], cv: 1 });
  source.frame(1);

  // Send order [daemon, daemon, room] — but the room's mid (1) is NUMERICALLY below the second
  // daemon mid (2): a mid-keyed replay sort would run the room append FIRST, against a base
  // where note 1 does not exist, and the later daemon upsert would erase the "+B".
  mutate.daemonWrite({ id: 99, body: "x" }); // daemon mid 1, seq 1
  mutate.daemonWrite({ id: 1, body: "base" }); // daemon mid 2, seq 2
  mutate.roomAppend({ id: 1 }); // room mid 1, seq 3 — READS note 1 ("base") → writes "base+B"
  assert.deepEqual(
    backend.__inspectDomains().pending.map((p) => [p.domain, p.mid, p.seq]),
    [
      ["daemon", 1, 1],
      ["daemon", 2, 2],
      ["room:doc:1", 1, 3],
    ],
    "mids are per-domain (incomparable); seq is the one cross-domain total order",
  );
  assert.deepEqual(rows(), [[1, "base+B"], [99, "x"]], "the invocation-order prediction");

  // A daemon release with a foreign server row rewinds the daemon source and re-invokes ALL
  // pending (every write staged onto the daemon source in single-Collapsed). The re-staged
  // predictions must equal the invocation-order originals.
  backend.__testRelease("daemon", [{ op: "add", table: "note", row: [50, "server"] }], 0);
  assert.deepEqual(
    rows(),
    [[1, "base+B"], [50, "server"], [99, "x"]],
    "replayed in send order — the read-dependent append still sees the daemon write it read",
  );
});

test("a fresh session adopts a domain's historical watermark while ANOTHER domain has pending (§7.1)", () => {
  const { source, mutate, backend } = setup();
  // A room-domain mutation is in flight when the daemon lmid snapshot of a client WITH HISTORY
  // (lmid 7 from an earlier session) arrives. The counters are independent: the room entry must
  // not block adopting the daemon's high-water mark (pre-fix: a global pending-length check
  // threw "ahead of issued" here).
  mutate.roomWrite({ id: 1, body: "r" });
  source.lmid(7, 1);
  source.frame(1);
  const d = backend.__inspectDomains();
  assert.equal(d.nextMid.daemon, 8, "the daemon counter continues the historical sequence");
  assert.equal(d.watermark.daemon, 7, "the daemon watermark adopted the snapshot");
  assert.deepEqual(
    d.pending.map((p) => [p.domain, p.mid]),
    [["room:doc:1", 1]],
    "the room entry is untouched — it was never the daemon's second writer",
  );
});

test("ahead-of-issued still throws when THIS domain has an issued mid in flight (§7.1)", () => {
  const { source, mutate } = setup();
  // The guard the scoping must NOT lose: a daemon mid in flight while the daemon stream confirms
  // a mid we never issued IS two-writers-on-one-clientID (or corruption) — unrecoverable.
  mutate.daemonWrite({ id: 1, body: "d" });
  source.lmid(7, 1);
  assert.throws(() => source.frame(1), /ahead of issued/);
});

// ============================================================================================
// Mid-pinning under the DECLARED router (302 §5; §7.1 "an assigned mid pins its domain forever")
// ============================================================================================

const ROOM = "room:doc:1";

/** A store whose DECLARED policy is MUTABLE (302 §5: the user declares, the client never
 *  derives — flipping the declaration mid-flight is how these lanes separate what re-resolves
 *  from what stays pinned): `room*` mutators route to ROOM while `state.roomOn` holds, daemon
 *  otherwise. A room gate is connected so the room channel's envelopes are observable; NO room
 *  tables are registered — these lanes exercise ledger machinery only, so writes stage onto the
 *  plain `note` table (identity staging map) that the suite's views read. */
function setupDeclared() {
  const source = new TestSource();
  const state = { roomOn: true };
  const { store, backend, mutate } = createOptimisticStore(schema, source, registry, {
    clientID: "c1",
    domainPolicy: (name: string) => (name.startsWith("room") && state.roomOn ? ROOM : undefined),
  });
  const view = store.materialize(allNotes());
  source.normalized(1, { type: "snapshot", ops: [], cv: 1 });
  source.frame(1);
  const roomSrc = new TestSource();
  backend.connectSource(ROOM, roomSrc);
  return { source, roomSrc, store, backend, mutate, view, state };
}

test("an assigned mid PINS its declared domain — a rebase re-invocation never re-routes (§7.1)", () => {
  const { source, roomSrc, backend, mutate, state } = setupDeclared();
  // Declared room route: the policy answers ROOM while the declaration holds.
  mutate.roomWrite({ id: 1, body: "r" });
  let d = backend.__inspectDomains();
  assert.deepEqual(
    d.pending.map((p) => [p.domain, p.mid]),
    [[ROOM, 1]],
    "the DECLARED policy routed the room",
  );
  assert.deepEqual(roomSrc.envelopes.map((e) => e.mid), [1], "the envelope shipped on the room channel");

  // Flip the declaration OFF: a FRESH invocation would now resolve "daemon" — but the assigned
  // mid already pinned ROOM (§7.1).
  state.roomOn = false;

  // A daemon release with a foreign row rewinds the engine and RE-INVOKES the entry. The
  // re-invocation replays the PINNED `p.domain` — it never re-consults the (now flipped) policy
  // — and ships no second envelope on either channel.
  source.normalized(1, { type: "batch", ops: [{ table: "note", op: "add", row: [50, "server"] } as NormalizedOp], cv: 2 });
  source.frame(2);
  d = backend.__inspectDomains();
  assert.deepEqual(
    d.pending.map((p) => [p.domain, p.mid]),
    [[ROOM, 1]],
    "the re-invocation kept the pinned domain",
  );
  assert.equal(roomSrc.envelopes.length, 1, "no extra envelope shipped on the room channel");
  assert.equal(source.envelopes.length, 0, "…and none on the daemon channel");

  // Only the ROOM's watermark can retire it (per-domain confirm-drop, §7.2).
  backend.__testRelease("daemon", [], 99);
  assert.equal(backend.__inspectDomains().pending.length, 1, "a daemon confirm cannot reach the room entry");
  backend.__testRelease(ROOM, [], 1);
  assert.deepEqual(backend.__inspectDomains().pending, [], "the room confirm retires it");
});

test("a FOLD's flush RE-RESOLVES the declared policy (the provisional domain is never routing, §7.1)", () => {
  const { backend, roomSrc, source, mutate, state } = setupDeclared();
  // Open the fold while the policy answers DAEMON: the un-flushed entry carries a provisional
  // domain and no mid (nothing dealt, nothing shipped).
  state.roomOn = false;
  mutate.roomWrite.folded({ key: 1 }, { id: 1, body: "a" });
  let d = backend.__inspectDomains();
  assert.deepEqual(
    d.pending.map((p) => [p.domain, p.mid]),
    [["daemon", null]],
    "un-flushed: the provisional domain is a placeholder (never read; no mid dealt)",
  );
  assert.equal(roomSrc.envelopes.length + source.envelopes.length, 0, "nothing on either wire yet");

  // Flip the declaration ON before the flush: flushFolds RE-RESOLVES the policy from the FINAL
  // args (§7.1) — domain ROOM, mid 1 from the room's own counter, envelope on the room channel.
  state.roomOn = true;
  backend.flushFolds();
  d = backend.__inspectDomains();
  assert.deepEqual(
    d.pending.map((p) => [p.domain, p.mid]),
    [[ROOM, 1]],
    "the flush RE-RESOLVED the declared policy: domain ROOM, mid from the room's own counter",
  );
  assert.deepEqual(roomSrc.envelopes.map((e) => e.mid), [1], "the flushed envelope shipped on the ROOM channel");

  // A second fold while the declaration is OFF: its flush resolves the daemon and deals from
  // the DAEMON's own gapless counter.
  state.roomOn = false;
  mutate.roomWrite.folded({ key: 5 }, { id: 5, body: "z" });
  backend.flushFolds();
  d = backend.__inspectDomains();
  assert.deepEqual(
    d.pending.map((p) => [p.domain, p.mid]).slice(-1),
    [["daemon", 1]],
    "the second fold flushed onto the DAEMON stream (its own gapless counter)",
  );
  assert.deepEqual(source.envelopes.map((e) => e.mid), [1], "…and shipped on the daemon channel");
});
