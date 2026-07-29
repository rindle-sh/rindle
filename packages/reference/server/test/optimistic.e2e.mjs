// Optimistic ws round-trip e2e (OPTIMISTIC-WRITES-DESIGN.md §8): a real ws server
// (`createOptimisticServer` over the native SQLite replica + a JS server registry) and TWO
// remote optimistic clients (`createOptimisticStore` over `RemoteOptimisticSource`) — the
// productized twin of @rindle/optimistic's in-process e2e, now with real wire framing:
//
//   1. confirmed create — instant local prediction, async ws confirm (resultType
//      unknown → complete), cross-client delivery under the poke rule;
//   2. the read-dependent rebase race — c1's +5 push is HELD at the transport while a
//      foreign write lands; c1 rebases to 105 before the server knows, the server then
//      independently computes 105 → zero drift;
//   3. failure — the server's mutator throws; processed-as-no-op (lmid advances, effects
//      rolled back); the phantom snaps back on the lmid release; the other client never
//      sees the phantom — there is NO rejection signal in the protocol;
//   4. duplicate redelivery — an already-processed mid is silently skipped server-side,
//      nothing re-applies;
//   5. gap → re-hydrate — a dropped nbatch leaves c1 behind; the next frame's seq gap
//      re-subscribes under a new epoch, the fresh cv-stamped snapshot re-hydrates, and the
//      still-pending mutation SURVIVES (re-invoked against the recovered base).
//
// Run with `--conditions=@rindle/source`; needs the replica native addon + the wasm pkg built.
import assert from "node:assert/strict";
import { isDeepStrictEqual } from "node:util";

import { defineQuery, table, string, number, createSchema, newQueryBuilder } from "@rindle/client";
import { createOptimisticServer } from "@rindle/server";
import { WsTransport, RemoteOptimisticSource } from "@rindle/remote";
import { createOptimisticStore } from "@rindle/optimistic";
import { initWasm } from "@rindle/wasm";

// The optimistic client builds a local wasm engine synchronously — init the wasm first.
await initWasm();

const issue = table("issue").columns({ id: number(), title: string(), score: number() }).primaryKey("id");
const schema = createSchema({ tables: [issue] });

// --- the two registries (shared names, never the wire) ------------------------------

const serverRegistry = {
  createIssue: (txn, args) => {
    txn.exec("INSERT INTO issue (id, title, score) VALUES (?, ?, ?)", [args.id, args.title, args.score]);
  },
  // Read-dependent: reads the CURRENT authoritative score inside its own txn (§4.1).
  incrementScore: (txn, args) => {
    const rows = txn.queryRows("SELECT score FROM issue WHERE id = ?", [args.id]);
    if (!rows.length) throw new Error("no such issue");
    txn.exec("UPDATE issue SET score = ? WHERE id = ?", [rows[0][0] + args.delta, args.id]);
  },
  forbidden: () => {
    throw new Error("rejected by policy");
  },
};

const clientRegistry = {
  createIssue: (tx, args) => tx.add("issue", [args.id, args.title, args.score]),
  incrementScore: (tx, args) => {
    const cur = tx.get("issue", [args.id]);
    if (!cur) return;
    tx.edit("issue", cur, [cur[0], cur[1], cur[2] + args.delta]);
  },
  // The optimistic prediction the server will refuse — a visible snap-back (§4.3).
  forbidden: (tx, args) => tx.add("issue", [args.id, "phantom", 0]),
};

const clientQ = newQueryBuilder(schema);
const issueQueries = {
  allIssues: defineQuery("allIssues", () => clientQ.issue.orderBy("id", "asc")),
};
const serverQ = newQueryBuilder(schema);
const serverQueries = {
  allIssues: () => serverQ.issue.orderBy("id", "asc"),
};

// --- test plumbing -------------------------------------------------------------------

/** A WsTransport with the two e2e levers: HOLD outgoing pushMutation messages (the
 *  in-flight-push race) and DROP one matching incoming message (the seq-gap trigger). */
class LeveredTransport {
  constructor(url) {
    this.inner = new WsTransport(url);
    this.handler = () => {};
    this.held = null; // when an array, outgoing pushMutation msgs queue here
    this.dropNext = null; // a predicate; drops ONE matching incoming message
    this.progressSeen = 0;
    this.inner.onMessage((m) => {
      if (this.dropNext && this.dropNext(m)) {
        this.dropNext = null;
        return;
      }
      if (m.t === "progress") this.progressSeen++;
      this.handler(m);
    });
  }
  send(m) {
    if (this.held && m.t === "pushMutation") {
      this.held.push(m);
      return;
    }
    this.inner.send(m);
  }
  onMessage(h) {
    this.handler = h;
  }
  close() {
    this.inner.close();
  }
  holdPushes() {
    this.held = [];
  }
  releaseHeld() {
    const held = this.held;
    this.held = null;
    for (const m of held) this.inner.send(m);
  }
}

function waitFor(cond, label, ms = 3000) {
  return new Promise((resolve, reject) => {
    const start = Date.now();
    const tick = () => {
      if (cond()) return resolve();
      if (Date.now() - start > ms) return reject(new Error(`timeout waiting for: ${label}`));
      setTimeout(tick, 10);
    };
    tick();
  });
}

// --- setup: one server, two clients ---------------------------------------------------

const server = await createOptimisticServer({ schema, registry: serverRegistry, queries: serverQueries });
const url = `ws://127.0.0.1:${server.port}`;

const t1 = new LeveredTransport(url);
const c1 = createOptimisticStore(schema, new RemoteOptimisticSource(t1, "c1"), clientRegistry, {
  clientID: "c1",
});
const t2 = new WsTransport(url);
const c2 = createOptimisticStore(schema, new RemoteOptimisticSource(t2, "c2"), clientRegistry, {
  clientID: "c2",
});

const v1 = c1.store.materialize(issueQueries.allIssues()); // qid 1 on each store
const v2 = c2.store.materialize(issueQueries.allIssues());
const rows = (v) => v.data.map((r) => [r.id, r.title, r.score]);
const score = (v, id) => v.data.find((r) => r.id === id)?.score;

// --- 1. confirmed creation: instant prediction, async ws confirm, cross-client poke ---

c1.mutate.createIssue({ id: 1, title: "first", score: 10 });
assert.deepEqual(rows(v1), [[1, "first", 10]], "instant optimistic apply (before any round-trip)");
// §7: ResultType is server-channel-only; "is a write in flight?" is the separate pending axis.
assert.equal(c1.backend.pending(1), true, "push is in flight over the wire");

await waitFor(() => !c1.backend.pending(1), "c1's create confirmed");
assert.deepEqual(rows(v1), [[1, "first", 10]], "zero drift after the confirm");
await waitFor(() => isDeepStrictEqual(rows(v2), [[1, "first", 10]]), "c2 poked with c1's write");
console.log("✓ 1. confirmed create: instant apply, unknown → complete, c2 delivered");

// --- 2. the read-dependent rebase race ------------------------------------------------

t1.holdPushes();
c1.mutate.incrementScore({ id: 1, delta: 5 }); // reads 10 → predicts 15
assert.equal(score(v1, 1), 15, "instant +5 prediction");
assert.equal(c1.backend.pending(1), true, "the +5 is pending over the wire");

// A foreign write lands while c1's push is in flight (held): id1 score 10 → 100.
t2.send({ t: "mutate", mutations: [{ op: "edit", table: "issue", old: [1, "first", 10], new: [1, "first", 100] }] });

await waitFor(() => score(v1, 1) === 105, "c1 rebased: re-invocation reads the foreign 100");
await waitFor(() => score(v2, 1) === 100, "c2 (no pending) shows the authoritative 100");
assert.equal(c1.backend.pending(1), true, "the +5 is still unconfirmed");

t1.releaseHeld(); // the server's incrementScore reads 100 → writes 105, confirms
await waitFor(() => !c1.backend.pending(1), "the rebased +5 confirmed");
assert.equal(score(v1, 1), 105, "server agreed with the re-based prediction");
await waitFor(() => score(v2, 1) === 105, "c2 converged to 105");
console.log("✓ 2. rebase race: client AND server independently computed 105 — zero drift");

// --- 3. failure: snap-back on the lmid release, invisible to the other client ---------

t1.holdPushes();
c1.mutate.forbidden({ id: 666 });
assert.equal(v1.data.length, 2, "phantom visible optimistically");
assert.equal(c1.backend.pending(1), true, "pending while the push is held");

t1.releaseHeld(); // server: throws → rollback → lmid-only commit → its lmid row releases
await waitFor(() => v1.data.length === 1, "the phantom snapped back");
assert.equal(c1.backend.resultType(1), "complete", "processed-as-no-op — ResultType never went to error (§7.4)");
assert.equal(c1.backend.pending(1), false, "the snapped-back mutation cleared from the pending axis");
assert.equal(v2.data.length, 1, "c2 never saw the phantom (lmid-only commit, no data frames)");

c1.mutate.createIssue({ id: 2, title: "ok", score: 1 });
await waitFor(() => !c1.backend.pending(1), "the next create confirms");
await waitFor(() => isDeepStrictEqual(rows(v2), rows(v1)), "both clients converged on 2 rows");
console.log("✓ 3. failure: snap-back via the lmid release (no rejection signal), c2 untouched");

// --- 4. duplicate redelivery is idempotent (silent server-side skip) -------------------

const before = rows(v1);
t1.send({
  t: "pushMutation",
  envelope: { clientID: "c1", mid: 1, name: "createIssue", args: { id: 1, title: "first", score: 10 } },
});
// No re-confirm traffic exists for a dup — prove nothing re-applied by pushing a
// sentinel foreign write through afterwards and checking the only change is the sentinel.
t2.send({ t: "mutate", mutations: [{ op: "edit", table: "issue", old: [1, "first", 105], new: [1, "first", 106] }] });
await waitFor(() => score(v1, 1) === 106 && score(v2, 1) === 106, "the sentinel write landed on both");
t2.send({ t: "mutate", mutations: [{ op: "edit", table: "issue", old: [1, "first", 106], new: [1, "first", 105] }] });
await waitFor(() => score(v1, 1) === 105 && score(v2, 1) === 105, "sentinel reverted");
assert.deepEqual(rows(v1), before, "an already-processed mid is skipped, nothing re-applies");
assert.deepEqual(rows(v2), before, "the other client saw no traffic for it");
console.log("✓ 4. duplicate redelivery: silent dup-skip, nothing re-applies");

// --- 5. gap → re-hydrate with the pending mutation surviving --------------------------

// Drop ONE incremental nbatch of the DATA query (qid 1; qid 0 is the lmid system query).
t1.dropNext = (m) => m.t === "nbatch" && m.queryId === 1 && m.batch.seq > 0;
t1.holdPushes();
c1.mutate.incrementScore({ id: 2, delta: 7 }); // reads 1 → predicts 8 (stays pending)
assert.equal(score(v1, 2), 8, "instant +7 prediction");

// Foreign write A: its nbatch to c1 is DROPPED (the progress frame still advances cvMin).
t2.send({ t: "mutate", mutations: [{ op: "edit", table: "issue", old: [1, "first", 105], new: [1, "first", 200] }] });
await waitFor(() => score(v2, 1) === 200, "c2 sees foreign write A");
assert.equal(score(v1, 1), 105, "c1 did NOT see A (frame dropped)");

// Foreign write B: c1 sees a seq gap → re-subscribes → fresh cv-stamped snapshot + progress.
t2.send({ t: "mutate", mutations: [{ op: "edit", table: "issue", old: [1, "first", 200], new: [1, "first", 300] }] });
await waitFor(() => score(v1, 1) === 300, "c1 recovered via re-hydrate (new epoch)");
assert.equal(score(v1, 2), 8, "the still-pending +7 SURVIVED the re-hydrate (re-invoked)");

t1.releaseHeld();
await waitFor(() => !c1.backend.pending(1), "the +7 confirmed after recovery");
await waitFor(() => isDeepStrictEqual(rows(v2), rows(v1)), "both clients converged");
assert.equal(score(v1, 2), 8, "zero drift on the recovered base");
console.log("✓ 5. gap → re-hydrate: recovered under a new epoch, pending survived");

t1.close();
t2.close();
await server.close();
console.log(
  "✅ optimistic over ws: instant apply, rebase race (105), failure snap-back (lmid-as-data), dup-skip, gap re-hydrate",
);
