// OptimisticBackend over a hand-scripted OptimisticSource (the server side of this
// contract is proven in Rust — rindle-replica's mutations/optimistic_envelope suites; these
// tests pin the TS wiring): instant optimistic apply via named mutators, cv-buffering
// with the coherent release at cvMin (§8.5) — confirmations arriving AS DATA on the
// reserved lmid system query (qid 0), released by the same cvMin — pending re-invocation
// through the engine's reconcile cycle (§1.3), failed-mutation snap-back (processed-as-
// no-op; no rejection signal), ResultType (§6), and the buffer-cap overflow escape.
//
// Requires the wasm artifact (packages/wasm/build.sh) — same as the other suites.

import { test } from "node:test";
import assert from "node:assert/strict";

import {
  createSchema,
  defineQuery,
  gt,
  initWasm,
  newQueryBuilder,
  number,
  QueryEnsureCache,
  stableKey,
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

const issue = table("issue").columns({ id: number(), title: string(), score: number() }).primaryKey("id");
const comment = table("comment").columns({ id: number(), issueID: number(), body: string() }).primaryKey("id");
const permission = table("permission").columns({ id: number(), flag: string() }).primaryKey("id");
const schema = createSchema({ tables: [issue, comment, permission] });

class TestSource implements OptimisticSource {
  normalized: (qid: QueryId, ev: NormalizedEvent) => void = () => {};
  progress: (frame: ProgressFrame) => void = () => {};
  registered: Array<[QueryId, RemoteQuery]> = [];
  unregistered: QueryId[] = [];
  envelopes: MutationEnvelope[] = [];
  expected: NormalizedTableSchema[][] = [];

  registerQuery(qid: QueryId, remote: RemoteQuery): void {
    this.registered.push([qid, remote]);
  }
  unregisterQuery(qid: QueryId): void {
    this.unregistered.push(qid);
  }
  pushMutation(env: MutationEnvelope): Promise<void> {
    this.envelopes.push(env);
    return Promise.resolve();
  }
  restart: () => void = () => {};
  onNormalized(h: (qid: QueryId, ev: NormalizedEvent) => void): void {
    this.normalized = h;
  }
  onProgress(h: (frame: ProgressFrame) => void): void {
    this.progress = h;
  }
  onRestart(h: () => void): void {
    this.restart = h;
  }
  expectClientSchema(schemas: NormalizedTableSchema[]): void {
    this.expected.push(schemas);
  }

  // test drivers
  snapshot(qid: QueryId, ops: NormalizedOp[], cv: number): void {
    this.normalized(qid, { type: "snapshot", ops, cv });
  }
  batch(qid: QueryId, ops: NormalizedOp[], cv: number): void {
    this.normalized(qid, { type: "batch", ops, cv });
  }
  /** The lmid system query's data frame (qid 0): the server confirming `mid` at `cv`
   *  — co-transactional with that commit's data (lmid-as-data). */
  lmid(mid: number, cv: number): void {
    this.normalized(0, {
      type: "batch",
      ops: [{ table: "_rindle_client_mutations", op: "add", row: ["c1", mid] }],
      cv,
    });
  }
  frame(cvMin: number): void {
    this.progress({ cvMin });
  }
}

const registry = {
  createIssue: (tx: MutationTx, args: { id: number; title: string; score: number }) =>
    tx.add("issue", [args.id, args.title, args.score]),
  incrementScore: (tx: MutationTx, args: { id: number; delta: number }) => {
    const cur = tx.get("issue", [args.id]);
    if (!cur) return;
    tx.edit("issue", cur, [cur[0], cur[1], (cur[2] as number) + args.delta]);
  },
  addComment: (tx: MutationTx, args: { id: number; issueID: number; body: string }) =>
    tx.add("comment", [args.id, args.issueID, args.body]),
  touchPermission: (tx: MutationTx, args: { id: number; flag: string }) =>
    tx.add("permission", [args.id, args.flag]),
  // Keyed update — touches only `title`, leaving any other column (incl. an Absent, un-projected
  // one) as it found it. Exercises the rebase seam over a projected row.
  editTitle: (tx: MutationTx, args: { id: number; title: string }) =>
    tx.update("issue", { id: args.id, title: args.title }),
} satisfies ClientRegistry;

const qb = newQueryBuilder(schema);
const issueQueries = {
  allIssues: defineQuery("allIssues", () => qb.issue),
  allComments: defineQuery("allComments", () => qb.comment),
};

/** A PROJECTED query: id + title only (omits `score`). Its server `hello` advertises the narrow
 *  column set and its rows stream 2 cells wide — the sync layer must scatter them into the shared
 *  full-width `issue` row (leaving `score` Absent) before the wasm `Db` applies them. */
const projQueries = {
  titlesOnly: defineQuery("titlesOnly", () => qb.issue.select("id", "title").orderBy("id", "asc")),
};

function setup(opts?: { bufferCap?: number }) {
  const source = new TestSource();
  const { store, backend, mutate } = createOptimisticStore(schema, source, registry, {
    clientID: "c1",
    bufferCap: opts?.bufferCap,
  });
  return { source, store, backend, mutate };
}

test("sync-only retain registers and releases aggregate synthetic schemas", () => {
  const { source, backend } = setup();
  const ast = qb.issue.countAs("commentCount", comment, { parent: ["id"], child: ["issueID"] }).ast();

  backend.retainRemoteQuery(99 as QueryId, { name: "issuesWithCount", args: null }, 99 as QueryId, ast);
  const retained = source.expected.at(-1) ?? [];
  const agg = retained.find((t) => t.name.startsWith("__agg_"));
  assert.ok(agg, "the retained coverage AST registered its synthetic aggregate table");
  assert.deepEqual(agg.columns, ["issueID", "count"]);
  assert.deepEqual(agg.primaryKey, [0]);

  backend.releaseRemoteQuery(99 as QueryId);
  assert.equal((source.expected.at(-1) ?? []).some((t) => t.name.startsWith("__agg_")), false);
});

test("hydrate buffers until the release point, then applies coherently", () => {
  const { source, store } = setup();
  const view = store.materialize(issueQueries.allIssues());
  assert.deepEqual(view.data, [], "pre-hydrate: empty");

  source.snapshot(1, [{ table: "issue", op: "add", row: [1, "a", 10] }], 5);
  assert.deepEqual(view.data, [], "a cv-tagged frame NEVER applies ahead of cvMin");

  source.frame(5);
  assert.deepEqual(view.data, [{ id: 1, title: "a", score: 10 }]);
});

test("a daemon restart resets the cv watermark so the post-restart stream is accepted", () => {
  const { source, store } = setup();
  const view = store.materialize(issueQueries.allIssues());

  // Hydrate, then advance the cv watermark with a live batch.
  source.snapshot(1, [{ table: "issue", op: "add", row: [1, "a", 10] }], 5);
  source.frame(5);
  source.batch(1, [{ table: "issue", op: "add", row: [2, "b", 20] }], 10);
  source.frame(10);
  assert.deepEqual(view.data.map((r) => r.id), [1, 2]);

  // The daemon restarted: the source signals onRestart (new boot id) and re-subscribes, so the
  // fresh snapshot arrives at a RESET, low cv. Without resetting the watermark, the post-restart
  // batch (cv 2 ≤ applied 10) would be dropped as stale and the stream would freeze.
  source.restart();
  source.snapshot(1, [{ table: "issue", op: "add", row: [1, "a", 10] }], 1);
  source.frame(1);
  source.batch(1, [{ table: "issue", op: "add", row: [3, "c", 30] }], 2);
  source.frame(2);

  assert.deepEqual(
    view.data.map((r) => r.id),
    [1, 3],
    "the snapshot re-hydrated (row 2 gone) AND the post-restart low-cv batch was accepted (row 3)",
  );
});

test("ad-hoc builder queries are local-only under the optimistic backend", () => {
  const { source, store } = setup();
  const view = store.query.issue.materialize();
  assert.deepEqual(view.data, []);
  const userRegs = source.registered.filter(([qid]) => qid !== 0);
  assert.deepEqual(userRegs, [], "no remote subscribe for an unstamped query (only the lmid system query)");
});

test("views pinned to the same named query share one remote subscription", () => {
  const { source, store } = setup();
  const base = issueQueries.allIssues();
  const list = store.materialize(base);
  const selected = store.materialize(base.where.id(1));

  const userRegs = source.registered.filter(([qid]) => qid !== 0);
  assert.equal(userRegs.length, 1, "one remote subscribe for the shared named footprint");
  assert.deepEqual(userRegs[0], [1, { name: "allIssues", args: null }]);

  list.destroy();
  assert.deepEqual(source.unregistered, [], "first local view only decrements the refcount");

  selected.destroy();
  assert.deepEqual(source.unregistered, [1], "last local view tears down the remote subscription");
});

test("split cached query view keeps one local AST and separate remote retains", () => {
  const { source, store, backend, mutate } = setup();
  const base = issueQueries.allIssues();
  const handle = store.createCachedQueryView(base);
  const release1 = handle.retain(base);
  const release2 = handle.retain(base);

  const userRegs = source.registered.filter(([qid]) => qid !== 0);
  assert.deepEqual(userRegs, [[2, { name: "allIssues", args: null }]], "remote retain uses its own source qid");

  source.normalized(2, {
    type: "hello",
    comparatorVersion: 1,
    normalizedFp: "test-only",
    tables: [
      { name: "issue", columns: ["id", "title", "score"], primaryKey: [0] },
      { name: "permission", columns: ["id", "flag"], primaryKey: [0] },
    ],
  });
  mutate.touchPermission({ id: 1, flag: "allowed" });
  assert.equal(backend.pending(1), true, "server-only dependencies attach to the shared local qid (pending axis, §7)");

  release1();
  assert.deepEqual(source.unregistered, [], "first remote retain only decrements backend refcount");
  release2();
  assert.deepEqual(source.unregistered, [2], "last remote retain tears down the source subscription");
  handle.destroy();
});

test("invoke applies the prediction instantly, ships the envelope, flips the pending axis (§7)", () => {
  const { source, store, backend, mutate } = setup();
  const view = store.materialize(issueQueries.allIssues());
  source.snapshot(1, [], 1);
  source.frame(1);

  const mid = mutate.createIssue({ id: 7, title: "mine", score: 60 });
  assert.equal(mid, 1);
  assert.deepEqual(view.data, [{ id: 7, title: "mine", score: 60 }], "instant optimistic apply");
  assert.deepEqual(source.envelopes, [
    { clientID: "c1", mid: 1, name: "createIssue", args: { id: 7, title: "mine", score: 60 } },
  ]);
  // §7: ResultType is the server channel's state only — a pending mutation no longer pins it; the
  // prediction's pending-ness is its own axis.
  assert.equal(backend.resultType(1), "complete", "ResultType stays complete (server-channel-only, §7)");
  assert.equal(backend.pending(1), true, "the pending axis flips true instead");
});

test("server-only dependency tables from normalized hello affect the pending axis (§7)", () => {
  const { source, store, backend, mutate } = setup();
  store.materialize(issueQueries.allIssues());
  assert.equal(backend.resultType(1), "unknown", "a remote query is unknown until its first snapshot");

  // Hydrate the query (an empty snapshot is still an authoritative answer) → complete.
  source.snapshot(1, [], 1);
  source.frame(1);
  assert.equal(backend.resultType(1), "complete");
  assert.equal(backend.pending(1), false);

  source.normalized(1, {
    type: "hello",
    comparatorVersion: 1,
    normalizedFp: "test-only",
    tables: [
      { name: "issue", columns: ["id", "title", "score"], primaryKey: [0] },
      { name: "permission", columns: ["id", "flag"], primaryKey: [0] },
    ],
  });

  const mid = mutate.touchPermission({ id: 1, flag: "allowed" });
  // §7: the pending mutator touching a server-only dependency flips the PENDING axis, not ResultType
  // (which stays complete — the prediction is the client's best current answer).
  assert.equal(backend.resultType(1), "complete", "ResultType stays complete (server-channel-only, §7)");
  assert.equal(backend.pending(1), true, "a pending mutator touching a server-only dependency makes the query pending");

  // The confirmation arrives as data on the lmid system query (even for a mutation the
  // server processed as a no-op — there is no rejection signal).
  source.lmid(mid, 2);
  source.frame(2);
  assert.equal(backend.resultType(1), "complete", "still complete after the release");
  assert.equal(backend.pending(1), false, "the released lmid row drops the pending mutation → pending clears");
});

test("a remote query's view is `unknown` until its first snapshot releases, then `complete`", () => {
  const { source, store, backend } = setup();
  const view = store.materialize(issueQueries.allIssues());
  assert.equal(view.resultType, "unknown", "loading: not yet server-authoritative");
  assert.equal(backend.resultType(1), "unknown");

  source.snapshot(1, [{ table: "issue", op: "add", row: [1, "a", 10] }], 1);
  assert.equal(view.resultType, "unknown", "a buffered (un-released) frame does not hydrate it");

  source.frame(1);
  assert.equal(view.resultType, "complete", "the released snapshot hydrates it → complete");
  assert.equal(backend.resultType(1), "complete");
});

test("an ad-hoc local-only query's view is `complete` immediately (no server to await)", () => {
  const { store } = setup();
  const view = store.query.issue.materialize();
  assert.equal(view.resultType, "complete");
});

// #349 (0-net-muts): a seeded query whose ENTIRE result is already present in the engine — folded in
// via an already-hydrated sibling — hydrates to ZERO net base mutations, so its reconcile emits no
// batch. The backend must still send an empty catch-up so the Store retires the SSR seed and reveals
// the covered rows, else the view freezes on the stale seed while reading `complete`.
test("a query covered by an already-hydrated sibling (0 net muts) still retires its SSR seed", () => {
  const { source, store } = setup();

  // Distinct ASTs (distinct seeds) with overlapping rows: the subset (id > 0) ⊆ the superset (all).
  const qAll = issueQueries.allIssues();
  const qSubset = defineQuery("coveredIssues", () => qb.issue.where.id(gt(0)))();
  store.hydrate({
    [stableKey(qAll.ast())]: { rows: [{ id: 7, title: "seedAll", score: 0 }], cvMin: 0 },
    [stableKey(qSubset.ast())]: { rows: [{ id: 8, title: "seedSubset", score: 0 }], cvMin: 0 },
  });

  const viewAll = store.materialize(qAll);
  const viewSubset = store.materialize(qSubset);
  const qidAll = source.registered.find(([, r]) => r.name === "allIssues")![0];
  const qidSubset = source.registered.find(([, r]) => r.name === "coveredIssues")![0];
  const rows: NormalizedOp[] = [
    { table: "issue", op: "add", row: [1, "a", 10] },
    { table: "issue", op: "add", row: [2, "b", 20] },
  ];

  // The superset hydrates first, loading the shared rows into the engine (routed to BOTH views — the
  // subset folds them as a PLAIN batch under its seed, staying PENDING).
  source.snapshot(qidAll, rows, 1);
  source.frame(1);
  assert.deepEqual(viewAll.data, [{ id: 1, title: "a", score: 10 }, { id: 2, title: "b", score: 20 }], "superset hydrated");
  assert.deepEqual(viewSubset.data, [{ id: 8, title: "seedSubset", score: 0 }], "subset still shows its seed");
  assert.equal(viewSubset.resultType, "unknown");

  // The subset hydrates over the SAME rows — already present, so 0 net muts and no reconcile batch. The
  // empty catch-up must retire its seed and reveal the rows already in its tree.
  source.snapshot(qidSubset, rows, 2);
  source.frame(2);
  assert.equal(viewSubset.resultType, "complete", "subset became authoritative");
  assert.deepEqual(
    viewSubset.data,
    [{ id: 1, title: "a", score: 10 }, { id: 2, title: "b", score: 20 }],
    "subset retired its seed and reveals the covered rows (not frozen)",
  );
});

test("ensure(until: present) resolves from sibling coverage before its own server round trip", async () => {
  const { source, store, backend } = setup();
  const all = store.materialize(issueQueries.allIssues());
  source.snapshot(1 as QueryId, [{ table: "issue", op: "add", row: [7, "covered", 10] }], 1);
  source.frame(1);
  assert.deepEqual(all.data, [{ id: 7, title: "covered", score: 10 }]);

  const byID = defineQuery("issueByID", (args: { id: number }) => qb.issue.where.id(args.id).one());
  const cache = new QueryEnsureCache(store, { releaseDelayMs: 0 });

  // Materializing issueByID synchronously reads row 7 from allIssues' normalized footprint. Its
  // own remote subscription is still unknown, but the route already has a usable projected row.
  await cache.ensure(byID({ id: 7 }), { until: "present" });
  assert.equal(backend.resultType(2 as QueryId), "unknown");
  assert.deepEqual(source.unregistered, [], "early readiness keeps the second query revalidating");

  // Its authoritative answer can be a 0-net-mutation snapshot because the row is already covered.
  source.snapshot(2 as QueryId, [{ table: "issue", op: "add", row: [7, "covered", 10] }], 2);
  source.frame(2);
  await new Promise<void>((resolve) => setTimeout(resolve, 0));
  assert.deepEqual(source.unregistered, [2 as QueryId]);
});

test("a confirmed prediction releases with zero churn and reference stability", () => {
  const { source, store, backend, mutate } = setup();
  const view = store.materialize(issueQueries.allIssues());
  source.snapshot(1, [{ table: "issue", op: "add", row: [1, "seed", 1] }], 1);
  source.frame(1);

  mutate.createIssue({ id: 7, title: "mine", score: 60 });
  const before = view.data;
  const seedRef = view.data[0];
  const mineRef = view.data[1];

  // The server echoes the same effect at cv 2 — the lmid row rides the SAME commit.
  source.batch(1, [{ table: "issue", op: "add", row: [7, "mine", 60] }], 2);
  source.lmid(1, 2);
  source.frame(2);

  assert.deepEqual(view.data, before, "values identical after confirmation");
  assert.equal(view.data[0], seedRef, "untouched row keeps its identity");
  assert.equal(view.data[1], mineRef, "the confirmed row never flickered (same object)");
  assert.equal(backend.resultType(1), "complete");
});

test("read-dependent pending re-invokes against the rebased base (105, not 15)", () => {
  const { source, store, mutate } = setup();
  const view = store.materialize(issueQueries.allIssues());
  source.snapshot(1, [{ table: "issue", op: "add", row: [1, "a", 10] }], 1);
  source.frame(1);

  mutate.incrementScore({ id: 1, delta: 5 }); // reads 10, predicts 15
  assert.equal(view.data[0].score, 15);

  // A FOREIGN write sets score=100 (confirms nothing: lmid stays 0).
  source.batch(1, [{ table: "issue", op: "edit", old: [1, "a", 10], new: [1, "a", 100] }], 2);
  source.frame(2);
  assert.equal(view.data[0].score, 105, "re-invocation reads the new base");

  // The server then runs the client's +5 itself (100 → 105) and confirms — the data
  // edit and the lmid row share cv 3, so they release together (no double-apply skew).
  source.batch(1, [{ table: "issue", op: "edit", old: [1, "a", 100], new: [1, "a", 105] }], 3);
  source.lmid(1, 3);
  source.frame(3);
  assert.equal(view.data[0].score, 105, "converged");
});

test("a failed mutation snaps back on the ordinary release (processed-as-no-op)", () => {
  const { source, store, backend, mutate } = setup();
  const view = store.materialize(issueQueries.allIssues());
  source.snapshot(1, [], 1);
  source.frame(1);

  mutate.createIssue({ id: 66, title: "forbidden", score: 0 });
  assert.equal(view.data.length, 1);

  // The server's mutator failed: effects rolled back, lmid still advanced in an
  // lmid-only commit (cv 2). Its lmid row is the only data — releasing it drops the
  // pending and the rewind snaps the phantom back. No error state: no rejection signal.
  source.lmid(1, 2);
  source.frame(2);
  assert.deepEqual(view.data, [], "the phantom row snapped back out");
  assert.equal(backend.resultType(1), "complete");

  // The next mutation applies normally.
  mutate.createIssue({ id: 2, title: "ok", score: 1 });
  source.batch(1, [{ table: "issue", op: "add", row: [2, "ok", 1] }], 3);
  source.lmid(2, 3);
  source.frame(3);
  assert.equal(backend.resultType(1), "complete");
  assert.deepEqual(view.data, [{ id: 2, title: "ok", score: 1 }]);
});

test("a multi-query transaction never tears across the release", () => {
  const { source, store } = setup();
  const issues = store.materialize(issueQueries.allIssues());
  const comments = store.materialize(issueQueries.allComments());
  source.snapshot(1, [], 1);
  source.snapshot(2, [], 1);
  source.frame(1);

  // One server txn (cv 2) touched both tables → two per-query frames, same cv. The
  // comments query is (simulated) touched-but-unprocessed, pinning cvMin at 1.
  source.batch(1, [{ table: "issue", op: "add", row: [5, "both", 0] }], 2);
  source.frame(1);
  assert.deepEqual(issues.data, [], "half a transaction must not show");

  source.batch(2, [{ table: "comment", op: "add", row: [50, 5, "half" ] }], 2);
  source.frame(2);
  assert.deepEqual(issues.data, [{ id: 5, title: "both", score: 0 }]);
  assert.deepEqual(comments.data, [{ id: 50, issueID: 5, body: "half" }]);
});

test("buffer overflow drops and re-registers; pending optimism survives the re-hydrate", () => {
  const { source, store, mutate } = setup({ bufferCap: 2 });
  const view = store.materialize(issueQueries.allIssues());
  source.snapshot(1, [{ table: "issue", op: "add", row: [1, "seed", 1] }], 1);
  source.frame(1);
  mutate.createIssue({ id: 500, title: "mine", score: 9 });

  // cvMin pinned while frames pile up past the cap (§8.5's pathological pairing).
  source.batch(1, [{ table: "issue", op: "add", row: [2, "b", 0] }], 2);
  source.batch(1, [{ table: "issue", op: "add", row: [3, "c", 0] }], 3);
  assert.equal(source.unregistered.length, 0);
  source.batch(1, [{ table: "issue", op: "add", row: [4, "d", 0] }], 4);
  assert.deepEqual(source.unregistered, [1, 0], "the cap tripped: user + lmid queries re-registered");
  assert.equal(source.registered.length, 4, "fresh registrations after the drop");

  // The fresh snapshot reflects the server's current state (cv 4); release it.
  source.snapshot(1, [
    { table: "issue", op: "add", row: [1, "seed", 1] },
    { table: "issue", op: "add", row: [2, "b", 0] },
    { table: "issue", op: "add", row: [3, "c", 0] },
    { table: "issue", op: "add", row: [4, "d", 0] },
  ], 4);
  source.frame(4);

  assert.deepEqual(view.data.map((r) => r.id), [1, 2, 3, 4, 500], "server rows + still-pending 500");
  assert.equal(view.data[4].title, "mine", "optimistic create survived the re-hydrate");
});

test("duplicate and stale frames are ignored; repeated progress is idempotent", () => {
  const { source, store } = setup();
  const view = store.materialize(issueQueries.allIssues());
  source.snapshot(1, [{ table: "issue", op: "add", row: [1, "a", 10] }], 1);
  source.frame(1);

  source.batch(1, [{ table: "issue", op: "edit", old: [1, "a", 10], new: [1, "a", 20] }], 2);
  source.frame(2);
  assert.equal(view.data[0].score, 20);

  // A network retry redelivers the already-released frame + frame: both no-ops.
  source.batch(1, [{ table: "issue", op: "edit", old: [1, "a", 10], new: [1, "a", 20] }], 2);
  source.frame(2);
  assert.deepEqual(view.data, [{ id: 1, title: "a", score: 20 }]);
});

// --- projection over the optimistic path (PROJECTION-SUPPORT-DESIGN §4/§5) -------------------
// A projected query advertises a NARROWER column set on its hello and streams rows positional over
// it. The optimistic backend must register that projection (colCounts + registerProjection) so the
// sync layer scatters the narrow rows into the shared full-width union; otherwise they reach the
// wasm `Db` un-scattered and fail its width check ("row has N cells but the schema declares M").

test("a projected query's narrow rows scatter to full width (no width-check throw)", () => {
  const { source, store } = setup();
  const view = store.materialize(projQueries.titlesOnly());

  // hello advertises issue WITHOUT `score`; rows then stream 2 cells wide.
  source.normalized(1, {
    type: "hello",
    comparatorVersion: 1,
    normalizedFp: "test-only",
    tables: [{ name: "issue", columns: ["id", "title"], primaryKey: [0] }],
  });
  source.snapshot(
    1,
    [
      { table: "issue", op: "add", row: [1, "a"] },
      { table: "issue", op: "add", row: [2, "b"] },
    ],
    1,
  );
  source.frame(1);

  assert.deepEqual(
    view.data.map((r) => [r.id, r.title]),
    [
      [1, "a"],
      [2, "b"],
    ],
    "narrow rows scattered into the shared union and materialized through the projected view",
  );

  // A narrow incremental edit scatters the same way.
  source.batch(1, [{ table: "issue", op: "edit", old: [1, "a"], new: [1, "a!"] }], 2);
  source.frame(2);
  assert.equal(view.data[0].title, "a!", "a narrow edit applies over the scattered row");
});

test("a server WIDER than the client drops the unknown column (expand-then-contract)", () => {
  // The server's `issue` was expanded with `assignee`; an old client doing `'*'` (allIssues) sees
  // a 4-column hello and 4-cell rows. The backend maps `assignee` to the DROP sentinel so the sync
  // layer discards that cell and stores the client's 3-wide row — otherwise the over-wide rows hit
  // the wasm `Db` and fail its width check. This is what makes `expand-then-contract` migrations work.
  const { source, store } = setup();
  const view = store.materialize(issueQueries.allIssues());

  source.normalized(1, {
    type: "hello",
    comparatorVersion: 1,
    normalizedFp: "test-only",
    tables: [{ name: "issue", columns: ["id", "title", "score", "assignee"], primaryKey: [0] }],
  });
  source.snapshot(
    1,
    [
      { table: "issue", op: "add", row: [1, "a", 5, "bob"] },
      { table: "issue", op: "add", row: [2, "b", 6, "carol"] },
    ],
    1,
  );
  source.frame(1);

  assert.deepEqual(
    view.data.map((r) => [r.id, r.title, r.score]),
    [
      [1, "a", 5],
      [2, "b", 6],
    ],
    "the extra `assignee` column is dropped; the client's full-width rows materialize",
  );

  // A wider incremental edit drops the same way.
  source.batch(1, [{ table: "issue", op: "edit", old: [1, "a", 5, "bob"], new: [1, "a", 9, "dave"] }], 2);
  source.frame(2);
  assert.equal(view.data[0].score, 9, "a wider edit applies after dropping the unknown column");
});

test("a server that expands (mid-row) then CONTRACTS back doesn't corrupt rows (stale-map clear)", () => {
  // The hello first advertises `issue` with an extra column in the MIDDLE: [id, assignee, title,
  // score] → map [0,-1,1,2]. Then the server contracts back to the exact [id, title, score] and
  // re-hydrates. The backend must revert the query to '*' (unregisterProjection) — otherwise the
  // now-3-cell rows scatter through the stale 4-wide `-1` map and corrupt (title <- score's slot).
  const { source, store } = setup();
  const view = store.materialize(issueQueries.allIssues());

  source.normalized(1, {
    type: "hello",
    comparatorVersion: 1,
    normalizedFp: "test-only",
    tables: [{ name: "issue", columns: ["id", "assignee", "title", "score"], primaryKey: [0] }],
  });
  source.snapshot(1, [{ table: "issue", op: "add", row: [1, "bob", "a", 5] }], 1);
  source.frame(1);
  assert.deepEqual(view.data.map((r) => [r.id, r.title, r.score]), [[1, "a", 5]], "mid-expanded row scattered");

  // Server contracted: the next hello advertises the exact schema; the re-hydrate streams 3-cell rows.
  source.normalized(1, {
    type: "hello",
    comparatorVersion: 1,
    normalizedFp: "test-only",
    tables: [{ name: "issue", columns: ["id", "title", "score"], primaryKey: [0] }],
  });
  source.snapshot(1, [{ table: "issue", op: "add", row: [1, "a", 5] }], 2);
  source.frame(2);

  assert.equal(view.data.length, 1);
  assert.equal(view.data[0].title, "a", "title survives (stale `-1` map cleared — not overwritten by score)");
  assert.equal(view.data[0].score, 5, "score survives");
});

test("a re-subscribe hello drops superseded-epoch buffered frames (no scatter through the new map)", () => {
  // The hazard the eager-hello-vs-buffered-data split creates: an epoch-1 snapshot sits BUFFERED
  // (cvMin lagging) when a gap-triggered re-subscribe brings an epoch-2 hello whose server has
  // EXPANDED. The hello mutates the column map eagerly; if the stale buffered row is later released
  // through that new map it silently corrupts ([1,"a",5] -> [1,5,_]). The hello must drop this qid's
  // buffered frames — the epoch-2 snapshot re-hydrates from scratch.
  const { source, store } = setup({ bufferCap: 1000 });
  const view = store.materialize(issueQueries.allIssues());

  source.normalized(1, {
    type: "hello",
    comparatorVersion: 1,
    normalizedFp: "test-only",
    tables: [{ name: "issue", columns: ["id", "title", "score"], primaryKey: [0] }],
  });
  source.snapshot(1, [{ table: "issue", op: "add", row: [1, "a", 5] }], 1); // BUFFERED — no frame yet

  // Re-subscribe; the server expanded (a mid-row column) in the meantime.
  source.normalized(1, {
    type: "hello",
    comparatorVersion: 1,
    normalizedFp: "test-only",
    tables: [{ name: "issue", columns: ["id", "assignee", "title", "score"], primaryKey: [0] }],
  });
  source.frame(1); // would release the buffered epoch-1 snapshot through the NEW (expanded) map
  // With the fix the stale snapshot was dropped on the re-subscribe hello, so nothing releases here
  // (the epoch-2 snapshot re-hydrates below). Without it, the stale row scatters through the new map
  // and lands corrupted ([1, 5, _] — score in the title slot), so the view would be non-empty.
  assert.equal(view.data.length, 0, "the superseded epoch-1 snapshot was dropped, not scattered through the new map");

  // Epoch 2 re-hydrates cleanly through the expanded map.
  source.snapshot(1, [{ table: "issue", op: "add", row: [1, "bob", "a", 5] }], 2);
  source.frame(2);
  assert.deepEqual(view.data.map((r) => [r.id, r.title, r.score]), [[1, "a", 5]], "re-hydrated correctly post-schema-change");
});

test("an optimistic update over a projected (Absent-bearing) row preserves the unprojected column", () => {
  const { source, store, mutate } = setup();
  const view = store.materialize(projQueries.titlesOnly());
  source.normalized(1, {
    type: "hello",
    comparatorVersion: 1,
    normalizedFp: "test-only",
    tables: [{ name: "issue", columns: ["id", "title"], primaryKey: [0] }],
  });
  source.snapshot(1, [{ table: "issue", op: "add", row: [1, "a"] }], 1);
  source.frame(1);

  // The shared `issue` row is now [1, "a", ABSENT] (score un-projected). A keyed `update` reads the
  // current row (incl. the Absent score, which marshals back as `undefined`), changes only `title`,
  // and writes it back — the rebase seam must carry the Absent cell through, not corrupt or drop it.
  mutate.editTitle({ id: 1, title: "a2" });
  assert.equal(view.data.length, 1, "the row survived the optimistic edit over an Absent column");
  assert.equal(view.data[0].title, "a2", "the optimistic title edit applied");

  // The server runs the same edit (narrow) and confirms via the lmid system query → converge.
  source.batch(1, [{ table: "issue", op: "edit", old: [1, "a"], new: [1, "a2"] }], 2);
  source.lmid(1, 2);
  source.frame(2);
  assert.equal(view.data[0].title, "a2", "converged after confirmation");
});
