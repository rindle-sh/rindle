// Tests for `203-MUTATOR-READS-DESIGN.md` — reads (one-shot queries) inside a client
// mutator, served by a lazy read-cache fork seeded from the staged buffer (§4), while
// commit stays plain buffer-replay (§6). Exercises the TS layers of the test plan (§12)
// against the real `MutationTx.query` primitive (Phase 1, §11):
//
//   - read-your-writes & semantics (the prefix oracle, §12.1): a query sees `live ⊕ this
//     mutator's writes-so-far` — incl. query → write → query (the write-forward into the fork);
//   - get-vs-query companion (§12.1): a single-row query equals the trusted `tx.get`/`tx.row`;
//   - isolation / no-leak (§12.2): a `tx.query` does not perturb a live view, and a
//     malformed-AST query throws without corrupting the engine;
//   - atomicity / burnt-mid (§12.2): a mutator that throws after a query consumes no mid;
//   - commit unchanged (§12.2): a within-mutator add-then-remove still nets out at the view;
//   - fold trap (§9.1): a `tx.query` in a folded call site is refused (non-absorbing);
//   - determinism (§12.2): the same in-mutator query is byte-identical, incl. orderBy ties.
//
// Requires the wasm artifact (packages/wasm/build.sh) — same as the other suites.

import { test } from "node:test";
import assert from "node:assert/strict";

import {
  createSchema,
  exists,
  initWasm,
  newQueryBuilder,
  number,
  string,
  table,
  type MutationEnvelope,
  type NormalizedEvent,
  type OptimisticSource,
  type ProgressFrame,
  type QueryId,
  type RemoteQuery,
  type WireValue,
} from "@rindle/wasm";
import {
  createOptimisticStore,
  type ClientRegistry,
  type IsoTx,
  type KeyedRow,
  type MutationGen,
  type MutationTx,
  type QueryArg,
  type QueryResultRow,
} from "../src/index.ts";

await initWasm();

const issue = table("issue").columns({ id: number(), owner: number(), score: number() }).primaryKey("id");
const comment = table("comment").columns({ id: number(), issueID: number(), body: string() }).primaryKey("id");
const schema = createSchema({ tables: [issue, comment] });
const qb = newQueryBuilder(schema);

/** A minimal in-process source: the reading tests never drive server frames, so every hook
 *  is a no-op recorder. (The rebase-cycle layer that DOES need frames is a separate todo.) */
class NoopSource implements OptimisticSource {
  envelopes: MutationEnvelope[] = [];
  registerQuery(_qid: QueryId, _remote: RemoteQuery): void {}
  unregisterQuery(_qid: QueryId): void {}
  pushMutation(env: MutationEnvelope): Promise<void> {
    this.envelopes.push(env);
    return Promise.resolve();
  }
  onNormalized(_h: (qid: QueryId, ev: NormalizedEvent) => void): void {}
  onProgress(_h: (frame: ProgressFrame) => void): void {}
  onRestart(_h: () => void): void {}
}

/** Where reading mutators stash what `tx.query` returned, so the test can assert it. */
const captured: {
  rows: QueryResultRow[];
  rows2: QueryResultRow[];
  pairs: Array<{ id: number; viaQuery?: QueryResultRow; viaGet?: KeyedRow }>;
} = {
  rows: [],
  rows2: [],
  pairs: [],
};

type WriteSpec =
  | { op: "add"; table: string; row: WireValue[] }
  | { op: "edit"; table: string; old: WireValue[]; new: WireValue[] }
  | { op: "remove"; table: string; row: WireValue[] }
  | { op: "insert"; table: string; row: KeyedRow }
  | { op: "update"; table: string; row: KeyedRow }
  | { op: "delete"; table: string; pk: KeyedRow };

function applyWrites(tx: MutationTx, writes: WriteSpec[]): void {
  for (const w of writes) {
    if (w.op === "add") tx.add(w.table, w.row);
    else if (w.op === "edit") tx.edit(w.table, w.old, w.new);
    else if (w.op === "remove") tx.remove(w.table, w.row);
    else if (w.op === "insert") tx.insert(w.table, w.row);
    else if (w.op === "update") tx.update(w.table, w.row);
    else tx.delete(w.table, w.pk);
  }
}

const registry = {
  seedIssue: (tx: MutationTx, a: { id: number; owner: number; score: number }) =>
    tx.add("issue", [a.id, a.owner, a.score]),
  seedComment: (tx: MutationTx, a: { id: number; issueID: number; body: string }) =>
    tx.add("comment", [a.id, a.issueID, a.body]),

  /** Apply a write prefix, then run a query and stash its rows (the §12.1 prefix-oracle subject). */
  probe: (tx: MutationTx, a: { writes?: WriteSpec[]; query: QueryArg; into?: "rows" | "rows2" }) => {
    if (a.writes) applyWrites(tx, a.writes);
    captured[a.into ?? "rows"] = tx.query(a.query);
  },

  /** add → query → add → query: covers query-NOT-last and the write-forward into the fork. */
  addQueryAddQuery: (tx: MutationTx, _a: Record<string, never>) => {
    tx.add("issue", [1, 10, 100]);
    captured.rows = tx.query(qb.issue.orderBy("id", "asc"));
    tx.add("issue", [2, 20, 200]);
    captured.rows2 = tx.query(qb.issue.orderBy("id", "asc"));
  },

  /** get-vs-query: a single-row query must equal the trusted point read for every id (§12.1). */
  getVsQuery: (tx: MutationTx, a: { writes?: WriteSpec[]; ids: number[] }) => {
    if (a.writes) applyWrites(tx, a.writes);
    captured.pairs = a.ids.map((id) => ({
      id,
      viaQuery: tx.query(qb.issue.where.id(id))[0],
      viaGet: tx.row("issue", { id }),
    }));
  },

  /** Query the live state (no writes) — for the isolation check: must not perturb a live view. */
  queryOnly: (tx: MutationTx, _a: Record<string, never>) => {
    captured.rows = tx.query(qb.issue.orderBy("id", "asc"));
  },

  /** add → query (sees it) → remove: read-your-writes mid-mutator, nets out at the view (§6). */
  addQueryRemove: (tx: MutationTx, _a: Record<string, never>) => {
    tx.add("issue", [9, 90, 900]);
    captured.rows = tx.query(qb.issue.where.id(9));
    tx.remove("issue", [9, 90, 900]);
  },

  /** Query, then throw — the staged write must roll back and NO mid burns (§12.2 / #10). */
  queryThenThrow: (tx: MutationTx, _a: Record<string, never>) => {
    tx.add("issue", [42, 4, 4]);
    captured.rows = tx.query(qb.issue.where.id(42));
    throw new Error("mutator blew up after its query");
  },

  /** The same query twice in one body — determinism: byte-identical rows (§12.2). */
  queryTwice: (tx: MutationTx, _a: Record<string, never>) => {
    captured.rows = tx.query(qb.issue.orderBy("owner", "asc"));
    captured.rows2 = tx.query(qb.issue.orderBy("owner", "asc"));
  },

  /** The SHARED (generator) form of a reading mutator — the SAME isomorphic body the API server
   *  drives asynchronously. Proves `runMutator` drives a FULL `tx.query` through the real wasm engine
   *  (read-your-writes) and that a write DERIVED from the query result lands. */
  sharedQueryDerive: function* (tx: IsoTx, _a: Record<string, never>): MutationGen {
    yield tx.insert("issue", { id: 7, owner: 70, score: 700 });
    const rows = (yield tx.query(qb.issue.orderBy("id", "asc"))) as unknown as QueryResultRow[];
    captured.rows = rows;
    yield tx.insert("comment", { id: 1, issueID: 7, body: `saw ${rows.length}` });
  },
} satisfies ClientRegistry;

function setup() {
  const source = new NoopSource();
  const { store, backend } = createOptimisticStore(schema, source, registry, { clientID: "c1" });
  return { source, store, backend };
}

const ISSUE = (id: number, owner: number, score: number): KeyedRow => ({ id, owner, score });

// --- read-your-writes & semantics (§12.1) ------------------------------------------

test("tx.query sees rows added earlier in the same mutator (prefix, incl. query-not-last)", () => {
  const { backend } = setup();
  backend.invoke("addQueryAddQuery", {});
  // The FIRST query ran after only `add(1)` — it must see exactly that row (prefix), not row 2.
  assert.deepEqual(captured.rows, [ISSUE(1, 10, 100)]);
  // The SECOND query ran after `add(2)` forwarded into the already-seeded fork.
  assert.deepEqual(captured.rows2, [ISSUE(1, 10, 100), ISSUE(2, 20, 200)]);
});

test("tx.query sees edits and removes made earlier in the same mutator", () => {
  const { backend } = setup();
  backend.invoke("seedIssue", { id: 1, owner: 10, score: 100 });
  backend.invoke("seedIssue", { id: 2, owner: 20, score: 200 });
  backend.invoke("seedIssue", { id: 3, owner: 30, score: 300 });
  backend.invoke("probe", {
    writes: [
      { op: "update", table: "issue", row: { id: 2, score: 250 } }, // edit score 200 → 250
      { op: "delete", table: "issue", pk: { id: 3 } }, // remove row 3
    ],
    query: qb.issue.orderBy("id", "asc"),
  });
  assert.deepEqual(captured.rows, [ISSUE(1, 10, 100), ISSUE(2, 20, 250)]);
});

test("a SHARED (generator) mutator runs a full tx.query mid-body (read-your-writes) and lands a derived write", () => {
  const { store, backend } = setup();
  backend.invoke("seedIssue", { id: 3, owner: 30, score: 300 });
  captured.rows = [];
  backend.invoke("sharedQueryDerive", {});
  // read-your-writes THROUGH THE GENERATOR DRIVE: the in-body query saw both the pre-seeded issue 3
  // and issue 7 inserted before the query, in id order — via the real wasm engine (`driveMutationSync`
  // → `tx.query`), not a mock.
  assert.deepEqual(captured.rows, [ISSUE(3, 30, 300), ISSUE(7, 70, 700)]);
  // the write DERIVED from the query result (its row count) committed to the live engine.
  const view = store.query.comment.materialize();
  assert.deepEqual(view.data, [{ id: 1, issueID: 7, body: "saw 2" }]);
  view.destroy();
});

test("tx.query with where / orderBy / limit returns the right subset of the fork", () => {
  const { backend } = setup();
  // Seed the base once (each `invoke` commits, so re-adding across invokes would conflict);
  // the two query variants then read the live base through query-only probes.
  backend.invoke("seedIssue", { id: 1, owner: 10, score: 100 });
  backend.invoke("seedIssue", { id: 2, owner: 10, score: 300 });
  backend.invoke("seedIssue", { id: 3, owner: 20, score: 200 });
  backend.invoke("seedIssue", { id: 4, owner: 20, score: 400 });
  // where owner = 10
  backend.invoke("probe", { query: qb.issue.where.owner(10).orderBy("id", "asc") });
  assert.deepEqual(captured.rows, [ISSUE(1, 10, 100), ISSUE(2, 10, 300)]);
  // orderBy score desc, limit 2 — the top two by score
  backend.invoke("probe", { query: qb.issue.orderBy("score", "desc").limit(2) });
  assert.deepEqual(captured.rows, [ISSUE(4, 20, 400), ISSUE(2, 10, 300)]);
});

test("tx.query joining a written table to an untouched table is correct (where-exists)", () => {
  const { backend } = setup();
  // Untouched base: a comment for issue 1 only (this table is forked O(1) off live, §4.1).
  backend.invoke("seedComment", { id: 100, issueID: 1, body: "x" });
  // In-mutator: add two issues; only issue 1 has a matching comment, so only it survives the join.
  backend.invoke("probe", {
    writes: [
      { op: "add", table: "issue", row: [1, 10, 100] },
      { op: "add", table: "issue", row: [2, 20, 200] },
    ],
    query: qb.issue.where(exists(comment, { parent: ["id"], child: ["issueID"] })).orderBy("id", "asc"),
  });
  assert.deepEqual(captured.rows, [ISSUE(1, 10, 100)]);
});

// --- materialized relationship children, nested by name (§5.2) ---------------------

test("tx.query reconstructs materialized relationship children, nested by name", () => {
  const { backend } = setup();
  backend.invoke("seedIssue", { id: 1, owner: 10, score: 100 });
  backend.invoke("seedIssue", { id: 2, owner: 20, score: 200 }); // childless
  backend.invoke("seedComment", { id: 100, issueID: 1, body: "a" });
  backend.invoke("seedComment", { id: 101, issueID: 1, body: "b" });
  backend.invoke("probe", {
    query: qb.issue
      .sub("comments", comment, { parent: ["id"], child: ["issueID"] }, (c) => c.orderBy("id", "asc"))
      .orderBy("id", "asc"),
  });
  assert.deepEqual(captured.rows, [
    {
      id: 1,
      owner: 10,
      score: 100,
      comments: [
        { id: 100, issueID: 1, body: "a" },
        { id: 101, issueID: 1, body: "b" },
      ],
    },
    { id: 2, owner: 20, score: 200, comments: [] }, // a childless parent → an empty array
  ]);
});

test("tx.query sees an in-mutator relationship write nested under its parent (read-your-writes)", () => {
  const { backend } = setup();
  backend.invoke("seedIssue", { id: 1, owner: 10, score: 100 });
  // The child table is read-cache-forked too (collect_ast_tables picks `comment` up from the
  // `related` subquery), so a comment added in THIS mutator shows up nested under its parent.
  backend.invoke("probe", {
    writes: [{ op: "add", table: "comment", row: [100, 1, "fresh"] }],
    query: qb.issue.sub("comments", comment, { parent: ["id"], child: ["issueID"] }).orderBy("id", "asc"),
  });
  assert.deepEqual(captured.rows, [
    { id: 1, owner: 10, score: 100, comments: [{ id: 100, issueID: 1, body: "fresh" }] },
  ]);
});

test("get-vs-query: tx.query(where id=k) equals tx.get/tx.row for every touched k", () => {
  const { backend } = setup();
  backend.invoke("seedIssue", { id: 1, owner: 10, score: 100 });
  backend.invoke("getVsQuery", {
    writes: [
      { op: "add", table: "issue", row: [2, 20, 200] }, // added this txn
      { op: "update", table: "issue", row: { id: 1, score: 150 } }, // edited this txn
    ],
    ids: [1, 2, 3], // 1 edited, 2 added, 3 absent
  });
  for (const p of captured.pairs) {
    assert.deepEqual(p.viaQuery, p.viaGet, `id ${p.id}: query and get must agree`);
  }
  // And the values are what we expect (anchor the differential's shared-bug blind spot, §12.1).
  assert.deepEqual(captured.pairs[0].viaQuery, ISSUE(1, 10, 150));
  assert.deepEqual(captured.pairs[1].viaQuery, ISSUE(2, 20, 200));
  assert.equal(captured.pairs[2].viaQuery, undefined);
});

// --- prefix-oracle differential (§12.1): in-mutator query == read-after-commit ------

test("the in-mutator query equals a fresh materialize over the same committed writes", () => {
  const { store, backend } = setup();
  backend.invoke("probe", {
    writes: [
      { op: "add", table: "issue", row: [5, 50, 500] },
      { op: "add", table: "issue", row: [6, 60, 600] },
    ],
    query: qb.issue.orderBy("id", "asc"),
  });
  const subject = captured.rows;
  // ORACLE: the probe's writes are now committed to the live engine; a materialized view over
  // the same query is the read-after-commit truth (the query-last case of the §12.1 oracle).
  const view = store.query.issue.orderBy("id", "asc").materialize();
  assert.deepEqual(subject, view.data);
  view.destroy();
});

// --- isolation / no-leak (§12.2) ---------------------------------------------------

test("a tx.query does not perturb a live query, and the engine stays healthy", () => {
  const { backend, store } = setup();
  backend.invoke("seedIssue", { id: 1, owner: 10, score: 100 });
  const view = store.query.issue.orderBy("id", "asc").materialize();
  assert.deepEqual(view.data, [ISSUE(1, 10, 100)]);

  // A query-only mutator runs scratch pipelines over forks — the live view must not move.
  backend.invoke("queryOnly", {});
  backend.invoke("queryOnly", {});
  assert.deepEqual(captured.rows, [ISSUE(1, 10, 100)], "the query itself saw the live row");
  assert.deepEqual(view.data, [ISSUE(1, 10, 100)], "a scratch query left the live view untouched");

  // The engine is uncorrupted: an ordinary write still fans out to the live view.
  backend.invoke("seedIssue", { id: 2, owner: 20, score: 200 });
  assert.deepEqual(view.data, [ISSUE(1, 10, 100), ISSUE(2, 20, 200)]);
  view.destroy();
});

test("a malformed-AST tx.query throws and leaks no engine state", () => {
  const { backend, store } = setup();
  backend.invoke("seedIssue", { id: 1, owner: 10, score: 100 });
  const view = store.query.issue.materialize();

  // A hand-crafted AST over a real table but a NON-EXISTENT column: passes the TS table check,
  // fails in `build_pipeline` — exercising the Rust error-path teardown (scratch sources + the
  // partial pipeline must be reclaimed, invariant 4).
  const badQuery: QueryArg = {
    ast: () => ({
      table: "issue",
      where: { type: "simple", op: "=", left: { type: "column", name: "nope" }, right: { type: "literal", value: 1 } },
    }),
  };
  assert.throws(() => backend.invoke("probe", { query: badQuery }), /nope|column|build/i);

  // The engine survived the failed teardown: a normal query and a normal write both still work.
  backend.invoke("probe", { query: qb.issue.orderBy("id", "asc") });
  assert.deepEqual(captured.rows, [ISSUE(1, 10, 100)]);
  backend.invoke("seedIssue", { id: 2, owner: 20, score: 200 });
  assert.deepEqual(view.data.map((r) => r.id), [1, 2]);
  view.destroy();
});

// --- atomicity / burnt-mid (§12.2) -------------------------------------------------

test("a mutator throwing after tx.query burns no mid and the next mutator works", () => {
  const { backend } = setup();
  const firstMid = backend.invoke("seedIssue", { id: 1, owner: 10, score: 100 });

  // The throwing mutator's query ran (read-your-writes), but the throw rolls the txn back and
  // consumes NO mid (a burnt mid would silently refuse every later mutation from this client, #10).
  assert.throws(() => backend.invoke("queryThenThrow", {}), /blew up/);
  assert.deepEqual(captured.rows, [ISSUE(42, 4, 4)], "the query saw the staged row before the throw");

  const nextMid = backend.invoke("seedIssue", { id: 2, owner: 20, score: 200 });
  assert.equal(nextMid, firstMid + 1, "the next mid is gapless — the thrown invoke consumed none");
});

// --- commit unchanged (§12.2) ------------------------------------------------------

test("a within-mutator add → query → remove reads its own write yet nets out at the view", () => {
  const { backend, store } = setup();
  const view = store.query.issue.materialize();
  backend.invoke("addQueryRemove", {});
  assert.deepEqual(captured.rows, [ISSUE(9, 90, 900)], "the mid-mutator query saw the row it had added");
  assert.deepEqual(view.data, [], "add-then-remove in one mutator leaves the view empty (no surviving row)");
  view.destroy();
});

// --- determinism (§12.2) -----------------------------------------------------------

test("the same in-mutator query is byte-identical, with the PK as the orderBy tiebreak", () => {
  const { backend } = setup();
  // Rows that TIE on `owner` (the sort key) — the PK (id) must break the tie deterministically.
  backend.invoke("seedIssue", { id: 3, owner: 10, score: 1 });
  backend.invoke("seedIssue", { id: 1, owner: 10, score: 2 });
  backend.invoke("seedIssue", { id: 2, owner: 10, score: 3 });
  backend.invoke("queryTwice", {});
  assert.deepEqual(captured.rows, captured.rows2, "repeated query returns identical rows");
  assert.deepEqual(
    captured.rows.map((r) => r.id),
    [1, 2, 3],
    "equal-owner rows are ordered by the PK tiebreak (id asc), not insertion order",
  );
});

// --- fold trap (§9.1) --------------------------------------------------------------

test("tx.query inside a folded call site is refused (a reading mutator is non-absorbing)", () => {
  const { backend } = setup();
  backend.invoke("seedIssue", { id: 1, owner: 10, score: 100 });
  assert.throws(
    () => backend.invokeFolded("probe", { key: 1 }, { query: qb.issue.orderBy("id", "asc") }),
    /cannot fold|absorbing|reads state/i,
  );
});

// --- rebase re-invocation (§8, §12.2) — deferred -----------------------------------
// Driving a full serverBatchBegin…End cycle with cv-buffered frames against a query-reading
// mutator (predict → deliver the authoritative delta → converge to a fresh-hydrate) is the one
// layer that needs the frame harness; it is tracked with the rebase suite, not here.
test("a query-reading mutator re-resolves on rebase and converges to the server state", { todo: true });
