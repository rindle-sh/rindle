// Optimistic e2e against a real server authority (design Slices 5/6): the native SQLite
// replica stands in for the server here — a genuine SQLite-backed engine driven in-process,
// not a mock — with an OptimisticBackend client over it. The full loop, nothing scripted:
//
//   client mutator (instant prediction) → envelope → @rindle/replica MutationTxn (the JS
//   server registry runs SQL in ONE txn with the co-transactional lmid upsert) →
//   cv-stamped normalized batches + progress frame → client cv-release → engine
//   rewind/re-invoke cycle → coalesced view delivery.
//
// Covers: zero-drift confirmation (lmid-as-data: the confirmation is the lmid row on the
// client's own system query, delivered in the SAME commit as the data), the
// read-dependent rebase race (foreign write lands while the push is in flight → client
// AND server both compute 105), failed-mutation snap-back (processed-as-no-op — no
// rejection signal), and idempotent redelivery.
//
// Run after packages/wasm/build.sh and `pnpm --filter @rindle/replica build:native`.

import assert from "node:assert/strict";

import {
  createSchema,
  defineQuery,
  initWasm,
  newQueryBuilder,
  number,
  string,
  table,
  tableSpec,
} from "@rindle/wasm";
import { NativeReplicaDb } from "@rindle/replica";
import { createOptimisticStore } from "../src/index.ts";

await initWasm();

const issue = table("issue").columns({ id: number(), title: string(), score: number() }).primaryKey("id");
const comment = table("comment").columns({ id: number(), issueID: number(), body: string() }).primaryKey("id");
const schema = createSchema({ tables: [issue, comment] });

// --- the server stand-in: the native replica + a JS server registry over MutationTxn ---

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
  addComment: (txn, args) => {
    txn.exec("INSERT INTO comment (id, issueID, body) VALUES (?, ?, ?)", [args.id, args.issueID, args.body]);
  },
  removeComment: (txn, args) => {
    txn.exec("DELETE FROM comment WHERE id = ?", [args.id]);
  },
};

/** The in-process OptimisticSource over the native replica: per-query cv-tagged
 *  normalized frames + progress frames (`{cvMin}` only) + the §8.1/§8.2 mutation apply
 *  protocol (dup-skip, gap error, failure → rollback + lmid-only commit). The client's
 *  lmid system query is served like any query — confirmation IS data. One connection. */
class NativeOptimisticSource {
  constructor(schema, registry, queries, clientID) {
    this.db = new NativeReplicaDb();
    this.registry = registry;
    this.queries = queries;
    this.clientID = clientID;
    this.normalized = () => {};
    this.progress = () => {};
    this.held = null; // when an array, pushes queue instead of applying (the race lever)
    for (const name of Object.keys(schema.tables)) {
      const meta = schema.tables[name];
      const { columns, primaryKey } = tableSpec(meta);
      const types = columns.map((c) => meta.columns[c].type);
      this.db.registerTable(name, columns, primaryKey, types);
    }
    this.db.enableClientMutations();
  }

  registerQuery(qid, remote) {
    let ast;
    if (remote.name === "_rindle/clientLmid") {
      // The reserved system query: this connection's own lmid row (identity from the
      // connection, args ignored).
      ast = {
        table: "_rindle_client_mutations",
        where: {
          type: "simple",
          op: "=",
          left: { type: "column", name: "client_id" },
          right: { type: "literal", value: this.clientID },
        },
        orderBy: [["client_id", "asc"]],
      };
    } else {
      const query = this.queries[remote.name];
      if (!query) throw new Error(`unknown query: ${remote.name}`);
      ast = query(remote.args).ast();
    }
    const r = this.db.queryNormalized(qid, JSON.stringify(ast), 1);
    this.normalized(qid, {
      type: "hello",
      tables: r.hello.tables,
      comparatorVersion: r.hello.comparatorVersion,
      normalizedFp: r.hello.normalizedFp,
    });
    this.normalized(qid, { type: "snapshot", ops: r.snapshot.ops, cv: r.snapshot.cv });
    this.progress({ cvMin: r.snapshot.cv });
  }

  unregisterQuery(qid) {
    this.db.destroyQuery(qid);
  }

  onNormalized(h) {
    this.normalized = h;
  }

  onProgress(h) {
    this.progress = h;
  }

  pushMutation(env) {
    if (this.held) {
      this.held.push(env);
      return Promise.resolve();
    }
    this.apply(env);
    return Promise.resolve();
  }

  apply(env) {
    const stored = this.db.clientLmid(env.clientID);
    if (env.mid <= stored) return; // duplicate redelivery: already processed
    if (env.mid !== stored + 1) throw new Error(`mid gap: expected ${stored + 1}, got ${env.mid}`);

    let txn = this.db.beginMutation();
    let failed = false;
    try {
      this.registry[env.name](txn, env.args); // unknown name throws → processed-as-no-op
    } catch {
      failed = true;
    }
    if (failed) {
      // Effects roll back; lmid still advances in an lmid-only commit (§8.2) — the
      // durable record that the mid was processed. Its lmid row reaches the client's
      // system query like any data; there is no rejection signal.
      txn.rollback();
      txn = this.db.beginMutation();
    }
    const result = txn.commitWithLmid(env.clientID, env.mid);
    this.deliver(result.cv, result.batches);
  }

  /** A write from elsewhere (another client / the upstream): no mid, confirms nothing. */
  foreignWrite(mutations) {
    const batches = this.db.commitNormalized(mutations);
    const cv = Math.max(...batches.map((b) => b.batch.cv));
    this.deliver(cv, batches);
  }

  deliver(cv, batches) {
    for (const b of batches) {
      this.normalized(b.queryId, { type: "batch", ops: b.batch.ops, cv: b.batch.cv });
    }
    // Single-thread server: every query is current at the commit → cvMin = cv (§8.3's
    // degenerate footprint check). The frame is a pure release signal.
    this.progress({ cvMin: cv });
  }

  holdPushes() {
    this.held = [];
  }
  releaseHeld() {
    const held = this.held;
    this.held = null;
    for (const env of held) this.apply(env);
  }

  /** The authoritative rows, fresh from the server (the convergence oracle). */
  serverRows() {
    const r = this.db.queryNormalized(9999, JSON.stringify({ table: "issue", orderBy: [["id", "asc"]] }), 1);
    this.db.destroyQuery(9999);
    return r.snapshot.ops.map((op) => op.row).sort((a, b) => a[0] - b[0]);
  }
}

const clientRegistry = {
  createIssue: (tx, args) => tx.add("issue", [args.id, args.title, args.score]),
  incrementScore: (tx, args) => {
    const cur = tx.get("issue", [args.id]);
    if (!cur) return;
    tx.edit("issue", cur, [cur[0], cur[1], cur[2] + args.delta]);
  },
  // The optimistic prediction the server will refuse — a visible snap-back (§4.3).
  forbidden: (tx, args) => tx.add("issue", [args.id, "phantom", 0]),
  addComment: (tx, args) => tx.add("comment", [args.id, args.issueID, args.body]),
  removeComment: (tx, args) => tx.delete("comment", { id: args.id }),
};

const clientQ = newQueryBuilder(schema);
const issueQueries = {
  allIssues: defineQuery("allIssues", () => clientQ.issue.orderBy("id", "asc")),
  // The relationship aggregate — `count(comments)`, never recomputed locally (§3); the
  // optimistic delta overlays the client's pending comment mutations (§4).
  issuesWithCounts: defineQuery("issuesWithCounts", () =>
    clientQ.issue.orderBy("id", "asc").countAs("commentCount", comment, { parent: ["id"], child: ["issueID"] })),
};
const serverQ = newQueryBuilder(schema);
const serverQueries = {
  allIssues: () => serverQ.issue.orderBy("id", "asc"),
  issuesWithCounts: () =>
    serverQ.issue.orderBy("id", "asc").countAs("commentCount", comment, { parent: ["id"], child: ["issueID"] }),
};

const source = new NativeOptimisticSource(schema, serverRegistry, serverQueries, "c1");
const { store, backend, mutate } = createOptimisticStore(schema, source, clientRegistry, {
  clientID: "c1",
});

const view = store.materialize(issueQueries.allIssues());
const viewRows = () => view.data.map((r) => [r.id, r.title, r.score]);
const converged = () => {
  assert.deepEqual(viewRows(), source.serverRows(), "client view == authoritative server rows");
};

// --- 1. confirmed creation: instant prediction, zero drift after the round-trip -----
mutate.createIssue({ id: 1, title: "first", score: 10 });
assert.deepEqual(viewRows(), [[1, "first", 10]], "instant optimistic apply");
converged();
// §7: ResultType is server-channel-only; "is a prediction in flight?" is the separate pending axis.
assert.equal(backend.resultType(1), "complete");
assert.equal(backend.pending(1), false, "confirmed synchronously in-process");

// --- 2. the read-dependent race: foreign 100 lands while the +5 push is in flight ---
source.holdPushes();
mutate.incrementScore({ id: 1, delta: 5 }); // reads 10 → predicts 15
assert.equal(view.data[0].score, 15);
assert.equal(backend.pending(1), true, "the +5 is pending (push held)");
assert.equal(backend.resultType(1), "complete", "ResultType stays complete (server-channel-only, §7)");

source.foreignWrite([{ op: "edit", table: "issue", old: [1, "first", 10], new: [1, "first", 100] }]);
assert.equal(view.data[0].score, 105, "client rebased: re-invocation reads the foreign 100");
assert.equal(source.serverRows()[0][2], 100, "server has not seen the +5 yet");

source.releaseHeld(); // the server's incrementScore reads 100 → writes 105, confirms
assert.equal(view.data[0].score, 105, "server agreed with the re-based prediction");
converged();
assert.equal(backend.pending(1), false, "the rebased +5 confirmed");

// --- 3. failure: the server's mutator throws; the phantom snaps back (no-reject) ----
source.holdPushes();
mutate.forbidden({ id: 666 });
assert.equal(view.data.length, 2, "phantom visible optimistically");
assert.equal(backend.pending(1), true, "pending while the push is held");

source.releaseHeld(); // server: throws → rollback → lmid-only commit → its lmid row releases
assert.equal(view.data.length, 1, "the phantom snapped back");
assert.equal(backend.resultType(1), "complete", "processed-as-no-op — ResultType never went to error (§7.4)");
assert.equal(backend.pending(1), false, "the snapped-back mutation cleared from the pending axis");
converged();

// The next mutation applies normally.
mutate.createIssue({ id: 2, title: "ok", score: 1 });
assert.equal(backend.pending(1), false, "the next create confirms synchronously");
converged();

// --- 4. duplicate redelivery is idempotent (the server's dup-skip) ------------------
const before = viewRows();
source.apply({ clientID: "c1", mid: 1, name: "createIssue", args: { id: 1, title: "first", score: 10 } });
assert.deepEqual(viewRows(), before, "an already-processed mid is skipped, nothing re-applies");
converged();

// --- 5. optimistic relationship aggregate over the REAL round-trip ------------------
// The client displays a server-computed `count(comments)` it cannot recompute (comment rows
// are never synced) and overlays its OWN pending comment mutations on top (§4). The server
// runs the reduce and ships the `(issueID, count)` synthetic rows; the client predicts +1/−1
// and converges with zero drift.
const aggView = store.materialize(issueQueries.issuesWithCounts());
const countOf = (id) => aggView.data.find((r) => r.id === id)?.commentCount;
assert.equal(countOf(1), 0, "issue 1 starts with no comments (server count, no comment rows synced)");

// 5a. Optimistic add: the prediction shows BEFORE the server processes it.
source.holdPushes();
mutate.addComment({ id: 11, issueID: 1, body: "a" });
mutate.addComment({ id: 12, issueID: 1, body: "b" });
assert.equal(countOf(1), 2, "optimistic delta: base 0 ⊕ +2 pending");
assert.equal(backend.pending(1), false, "the issue list query (qid 1) is untouched by comments (§7 pending axis)");
source.releaseHeld(); // server INSERTs both, the reduce recomputes 0→2, lmid confirms co-transactionally
assert.equal(countOf(1), 2, "converged to the server count with no drift");

// 5b. Optimistic group BIRTH on a childless issue (§6.1), then confirm — no double-birth.
source.holdPushes();
mutate.addComment({ id: 21, issueID: 2, body: "first" });
assert.equal(countOf(2), 1, "optimistic birth 0 → 1");
source.releaseHeld();
assert.equal(countOf(2), 1, "the server's real birth row is absorbed (no double count)");

// 5c. Optimistic add + remove of the SAME comment within one pending batch: the row IS in the
// local store (its add is still pending), so the remove decrements — net zero, server agrees.
// (Removing an ALREADY-CONFIRMED comment is a local no-op — its row left the store on
// confirmation, the agg query never syncs comment rows — so that decrement only lands on the
// round-trip; the §6 self-correcting edge, covered in the unit suite.)
source.holdPushes();
mutate.addComment({ id: 13, issueID: 1, body: "tmp" });
assert.equal(countOf(1), 3, "optimistic +1");
mutate.removeComment({ id: 13 });
assert.equal(countOf(1), 2, "optimistic −1 on the locally-held pending comment → net 0");
source.releaseHeld();
assert.equal(countOf(1), 2, "server applied add then delete → still 2");

// Final convergence: the optimistic counts equal the server's authoritative reduce.
const serverCount = (id) => {
  const r = source.db.queryNormalized(
    9998,
    JSON.stringify(serverQueries.issuesWithCounts().ast()),
    1,
  );
  source.db.destroyQuery(9998);
  const aggName = r.hello.tables.find((t) => t.name.startsWith("__agg"))?.name;
  const row = r.snapshot.ops.find((op) => op.table === aggName && op.row[0] === id);
  return row ? row.row[1] : 0;
};
assert.equal(countOf(1), serverCount(1), "issue 1: client view == server reduce");
assert.equal(countOf(2), serverCount(2), "issue 2: client view == server reduce");

console.log("✅ @rindle/optimistic e2e: full loop vs the native replica — instant apply, " +
  "read-dependent rebase race (105), failed-mutation snap-back (lmid-as-data), dup-skip, " +
  "optimistic count (add/birth/remove) converging to the server reduce");
