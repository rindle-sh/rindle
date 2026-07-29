// Optimistic UPDATES of a relationship aggregate (AGGREGATE-SYNC-DESIGN.md §4–§6): the
// client overlays its OWN pending child mutations onto the server-authoritative count —
// `displayed = server_base ⊕ watermark-gated local_pending_delta`. A pending `addComment`
// bumps the displayed `commentCount` instantly; the server's recomputed count + the
// co-transactional lmid release retire the prediction with NO double count; a brand-new
// group is born optimistically and never double-births.
//
// Requires the wasm artifact (packages/wasm/build.sh).

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
import { aggTableName } from "@rindle/normalized";
import { createOptimisticStore, type ClientRegistry, type MutationTx } from "../src/index.ts";

await initWasm();

const issue = table("issue").columns({ id: number(), title: string(), score: number() }).primaryKey("id");
const comment = table("comment").columns({ id: number(), issueID: number(), body: string() }).primaryKey("id");
const schema = createSchema({ tables: [issue, comment] });

const registry = {
  addComment: (tx: MutationTx, args: { id: number; issueID: number; body: string }) =>
    tx.insert("comment", { id: args.id, issueID: args.issueID, body: args.body }),
  removeComment: (tx: MutationTx, args: { id: number }) => tx.delete("comment", { id: args.id }),
  /** Re-parent a comment to a different issue — a group-key move (−1 old group, +1 new). */
  moveComment: (tx: MutationTx, args: { id: number; issueID: number }) =>
    tx.update("comment", { id: args.id, issueID: args.issueID }),
} satisfies ClientRegistry;

const qb = newQueryBuilder(schema);
const queries = {
  issuesWithCounts: defineQuery("issuesWithCounts", () => qb.issue.countAs("commentCount", comment, { parent: ["id"], child: ["issueID"] })),
  // A child-column filter is EVALUABLE (§6.2) — the optimistic delta respects membership.
  notSpamCounts: defineQuery("notSpamCounts", () =>
    qb.issue.countAs("commentCount", comment, { parent: ["id"], child: ["issueID"] }, (q) =>
      q.where({ type: "simple", op: "!=", left: { type: "column", name: "body" }, right: { type: "literal", value: "spam" } }),
    )),
  // A LIKE filter is UNEVALUABLE in v1 → no optimistic delta, bare server value (§6.2 fallback).
  likeCounts: defineQuery("likeCounts", () =>
    qb.issue.countAs("commentCount", comment, { parent: ["id"], child: ["issueID"] }, (q) =>
      q.where({ type: "simple", op: "LIKE", left: { type: "column", name: "body" }, right: { type: "literal", value: "%x%" } }),
    )),
};
const queriesList = { allComments: defineQuery("allComments", () => qb.comment.orderBy("id", "asc")) };
const AGG = aggTableName(queries.issuesWithCounts().ast().related![0]);
const AGG_NOTSPAM = aggTableName(queries.notSpamCounts().ast().related![0]);
const AGG_LIKE = aggTableName(queries.likeCounts().ast().related![0]);

class TestSource implements OptimisticSource {
  normalized: (qid: QueryId, ev: NormalizedEvent) => void = () => {};
  progress: (frame: ProgressFrame) => void = () => {};
  registered: Array<[QueryId, RemoteQuery]> = [];
  registerQuery(qid: QueryId, remote: RemoteQuery): void {
    this.registered.push([qid, remote]);
  }
  /** The source qid the backend subscribed for `name` (so a test can drive its stream). */
  qidFor(name: string): QueryId {
    const hit = this.registered.find(([, r]) => r.name === name);
    if (!hit) throw new Error(`no registered query ${name}`);
    return hit[0];
  }
  unregisterQuery(): void {}
  pushMutation(_env: MutationEnvelope): Promise<void> {
    return Promise.resolve();
  }
  expectClientSchema(_tables: NormalizedTableSchema[]): void {}
  onNormalized(h: (qid: QueryId, ev: NormalizedEvent) => void): void {
    this.normalized = h;
  }
  onProgress(h: (frame: ProgressFrame) => void): void {
    this.progress = h;
  }
  snapshot(qid: QueryId, ops: NormalizedOp[], cv: number): void {
    this.normalized(qid, { type: "snapshot", ops, cv });
  }
  batch(qid: QueryId, ops: NormalizedOp[], cv: number): void {
    this.normalized(qid, { type: "batch", ops, cv });
  }
  /** The lmid system query (qid 0) confirming `mid` at `cv` (lmid-as-data, co-transactional
   *  with that commit's `__agg` effects). */
  lmid(mid: number, cv: number): void {
    this.normalized(0, { type: "batch", ops: [{ table: "_rindle_client_mutations", op: "add", row: ["c1", mid] }], cv });
  }
  frame(cvMin: number): void {
    this.progress({ cvMin });
  }
}

function setup() {
  const source = new TestSource();
  const { store, backend, mutate } = createOptimisticStore(schema, source, registry, { clientID: "c1" });
  return { source, store, backend, mutate };
}

/** Seed one issue (id 5) with a server count of 2 and release it. */
function seedIssue5(source: TestSource) {
  source.snapshot(
    1,
    [
      { table: "issue", op: "add", row: [5, "five", 0] },
      { table: AGG, op: "add", row: [5, 2] },
    ],
    1,
  );
  source.frame(1);
}

const count = (view: { data: ReadonlyArray<{ commentCount: number }> }) => view.data.map((r) => r.commentCount);

test("a pending addComment bumps the displayed count instantly (server_base ⊕ delta)", () => {
  const { source, store, mutate } = setup();
  const view = store.materialize(queries.issuesWithCounts());
  seedIssue5(source);
  assert.deepEqual(count(view), [2], "server base");

  mutate.addComment({ id: 101, issueID: 5, body: "a" });
  assert.deepEqual(count(view), [3], "base 2 ⊕ +1 pending");

  mutate.addComment({ id: 102, issueID: 5, body: "b" });
  assert.deepEqual(count(view), [4], "base 2 ⊕ +2 pending");
});

test("an optimistic add then remove of the same comment nets to the server base", () => {
  const { source, store, mutate } = setup();
  const view = store.materialize(queries.issuesWithCounts());
  seedIssue5(source);

  mutate.addComment({ id: 101, issueID: 5, body: "a" });
  assert.deepEqual(count(view), [3]);
  mutate.removeComment({ id: 101 });
  assert.deepEqual(count(view), [2], "delta back to 0 → server base");
});

test("the co-transactional lmid + __agg release retires the prediction with NO double count (§5)", () => {
  const { source, store, mutate } = setup();
  const view = store.materialize(queries.issuesWithCounts());
  seedIssue5(source);

  mutate.addComment({ id: 101, issueID: 5, body: "a" }); // mid 1
  mutate.addComment({ id: 102, issueID: 5, body: "b" }); // mid 2
  assert.deepEqual(count(view), [4], "base 2 ⊕ +2 pending");

  // The server confirms mid 1: its real comment makes the recomputed count 2→3, and the lmid
  // row advances to 1 — BOTH at the same cv, released together. The pending delta for mid 1
  // must vanish exactly as server_base absorbs it: display stays 4 (base 3 ⊕ +1), NOT 5.
  source.batch(1, [{ table: AGG, op: "edit", old: [5, 2], new: [5, 3] }], 2);
  source.lmid(1, 2);
  source.frame(2);
  assert.deepEqual(count(view), [4], "base 3 ⊕ +1 pending — the confirmed mutation is not counted twice");

  // Confirm mid 2 likewise → fully settled at the server value, no pending delta left.
  source.batch(1, [{ table: AGG, op: "edit", old: [5, 3], new: [5, 4] }], 3);
  source.lmid(2, 3);
  source.frame(3);
  assert.deepEqual(count(view), [4], "base 4, no pending");
});

test("§6.1 optimistic birth: a first comment on a server-childless issue, then confirm (no double-birth)", () => {
  const { source, store, mutate } = setup();
  const view = store.materialize(queries.issuesWithCounts());
  // issue 7 exists server-side but has no comments → no __agg row → displayed 0.
  source.snapshot(1, [{ table: "issue", op: "add", row: [7, "seven", 0] }], 1);
  source.frame(1);
  assert.deepEqual(count(view), [0]);

  mutate.addComment({ id: 201, issueID: 7, body: "first" }); // mid 1
  assert.deepEqual(count(view), [1], "optimistic group birth 0 → 1");

  // The server confirms: the real group is born (count 1) co-transactionally with lmid 1.
  source.batch(1, [{ table: AGG, op: "add", row: [7, 1] }], 2);
  source.lmid(1, 2);
  source.frame(2);
  assert.deepEqual(count(view), [1], "no double-birth — the optimistic row is absorbed by the server's");
});

test("§6.1 optimistic birth that the server never confirms snaps the phantom group away", () => {
  const { source, store, mutate } = setup();
  const view = store.materialize(queries.issuesWithCounts());
  source.snapshot(1, [{ table: "issue", op: "add", row: [8, "eight", 0] }], 1);
  source.frame(1);

  mutate.addComment({ id: 301, issueID: 8, body: "x" }); // mid 1
  assert.deepEqual(count(view), [1], "optimistic birth");

  // The mutation is processed as a no-op server-side (no rejection signal): lmid advances but
  // NO __agg row ever ships. The rewind removes the phantom group.
  source.lmid(1, 2);
  source.frame(2);
  assert.deepEqual(count(view), [0], "phantom group removed when the prediction is not confirmed");
});

test("a pending add to one group leaves a sibling group's count untouched", () => {
  const { source, store, mutate } = setup();
  const view = store.materialize(queries.issuesWithCounts());
  source.snapshot(
    1,
    [
      { table: "issue", op: "add", row: [5, "five", 0] },
      { table: "issue", op: "add", row: [6, "six", 0] },
      { table: AGG, op: "add", row: [5, 2] },
      { table: AGG, op: "add", row: [6, 9] },
    ],
    1,
  );
  source.frame(1);
  assert.deepEqual(count(view), [2, 9]);

  mutate.addComment({ id: 101, issueID: 5, body: "a" });
  assert.deepEqual(count(view), [3, 9], "only issue 5's group moves");
});

test("§6.2 evaluable child filter: only members move the optimistic delta", () => {
  const { source, store, mutate } = setup();
  const view = store.materialize(queries.notSpamCounts());
  source.snapshot(1, [{ table: "issue", op: "add", row: [5, "five", 0] }, { table: AGG_NOTSPAM, op: "add", row: [5, 2] }], 1);
  source.frame(1);
  assert.deepEqual(count(view), [2]);

  mutate.addComment({ id: 101, issueID: 5, body: "ok" }); // body != "spam" → member → +1
  assert.deepEqual(count(view), [3]);
  mutate.addComment({ id: 102, issueID: 5, body: "spam" }); // filtered OUT → no optimistic delta
  assert.deepEqual(count(view), [3], "a non-member pending child does not move the count");
});

test("§6.2 unevaluable filter (LIKE): no optimistic delta, but the query reports pending; server corrects", () => {
  const { source, store, backend, mutate } = setup();
  const view = store.materialize(queries.likeCounts()); // first user query → local qid 1
  const QID = 1;
  source.snapshot(QID, [{ table: "issue", op: "add", row: [5, "five", 0] }, { table: AGG_LIKE, op: "add", row: [5, 2] }], 1);
  source.frame(1);
  assert.deepEqual(count(view), [2]);

  // The pending child WOULD match LIKE %x% — but the filter is unevaluable, so the optimistic
  // delta is not computed: the display holds the bare server value (always correct, not optimistic).
  mutate.addComment({ id: 101, issueID: 5, body: "xyz" });
  assert.deepEqual(count(view), [2], "unevaluable filter → server-value fallback, no optimistic bump");
  // §7: the child mutation flips the PENDING axis (ResultType stays complete — server-channel-only).
  assert.equal(backend.resultType(QID), "complete", "ResultType stays complete (server-channel-only, §7)");
  assert.equal(backend.pending(QID), true, "but the query is still flagged pending (child mutation)");

  // The server evaluates the filter authoritatively (2 → 3) and confirms.
  source.batch(QID, [{ table: AGG_LIKE, op: "edit", old: [5, 2], new: [5, 3] }], 2);
  source.lmid(1, 2);
  source.frame(2);
  assert.deepEqual(count(view), [3], "the round-trip applies the server-computed filtered count");
  assert.equal(backend.resultType(QID), "complete");
});

test("deleting a server-only child (never synced) does not decrement optimistically; the round-trip corrects it", () => {
  // The client holds the COUNT (2) but NOT the comment rows. Deleting one it can't see is a
  // local no-op → no optimistic decrement (a bounded, self-correcting limitation, §6 edge).
  const { source, store, mutate } = setup();
  const view = store.materialize(queries.issuesWithCounts());
  seedIssue5(source);
  assert.deepEqual(count(view), [2]);

  mutate.removeComment({ id: 999 }); // a server comment the client never synced
  assert.deepEqual(count(view), [2], "no optimistic decrement — the row isn't in the local store");

  // The server actually deletes it (count 2 → 1) and confirms; the display corrects.
  source.batch(1, [{ table: AGG, op: "edit", old: [5, 2], new: [5, 1] }], 2);
  source.lmid(1, 2);
  source.frame(2);
  assert.deepEqual(count(view), [1], "self-corrected on the round-trip to the authoritative count");
});

test("rebase race: a foreign comment lands while a local add is pending → both counted once", () => {
  // The count analog of the read-dependent score race. The local prediction must REBASE onto
  // the foreign server update, not stack on a stale base or double-count itself.
  const { source, store, mutate } = setup();
  const view = store.materialize(queries.issuesWithCounts());
  seedIssue5(source);

  mutate.addComment({ id: 101, issueID: 5, body: "mine" }); // mid 1, NOT yet confirmed
  assert.deepEqual(count(view), [3], "base 2 ⊕ +1 pending");

  // A DIFFERENT client's comment lands: the server count moves 2 → 3 (NO lmid for our mutation).
  source.batch(1, [{ table: AGG, op: "edit", old: [5, 2], new: [5, 3] }], 2);
  source.frame(2);
  assert.deepEqual(count(view), [4], "rebased: foreign +1 folded into base (3), our +1 still pending");

  // Now our mutation confirms (3 → 4) co-transactionally with its lmid.
  source.batch(1, [{ table: AGG, op: "edit", old: [5, 3], new: [5, 4] }], 3);
  source.lmid(1, 3);
  source.frame(3);
  assert.deepEqual(count(view), [4], "converged with no drift — neither comment counted twice");
});

test("optimistic birth then optimistic removal (before any confirm) snaps the phantom row away", () => {
  // Exercises reconcileAggHead's REMOVE branch: a born group (no server row) whose delta
  // returns to 0 must drop the optimistic `__agg` row entirely (→ projection identity 0).
  const { source, store, mutate } = setup();
  const view = store.materialize(queries.issuesWithCounts());
  source.snapshot(1, [{ table: "issue", op: "add", row: [9, "nine", 0] }], 1); // childless, no __agg row
  source.frame(1);
  assert.deepEqual(count(view), [0]);

  mutate.addComment({ id: 401, issueID: 9, body: "x" }); // optimistic birth 0 → 1
  assert.deepEqual(count(view), [1]);
  mutate.removeComment({ id: 401 }); // the comment is locally held → birth undone
  assert.deepEqual(count(view), [0], "the phantom group row was removed, not left at 0");
});

test("optimistic group-key move re-parents the count across two groups", () => {
  const { source, store, mutate } = setup();
  const view = store.materialize(queries.issuesWithCounts());
  source.snapshot(
    1,
    [
      { table: "issue", op: "add", row: [5, "five", 0] },
      { table: "issue", op: "add", row: [6, "six", 0] },
      { table: AGG, op: "add", row: [5, 2] },
      { table: AGG, op: "add", row: [6, 1] },
    ],
    1,
  );
  source.frame(1);
  assert.deepEqual(count(view), [2, 1]);

  mutate.addComment({ id: 101, issueID: 5, body: "a" });
  assert.deepEqual(count(view), [3, 1]);
  mutate.moveComment({ id: 101, issueID: 6 }); // −1 group 5, +1 group 6
  assert.deepEqual(count(view), [2, 2], "the move shifts the optimistic count between groups");
});

test("a count query and a comments-list query coexist: the comment drives the list, the overlay drives the count, no double count", () => {
  // The realistic app shape: one query DISPLAYS count(comments) (no comment rows synced), a
  // sibling query LISTS the comments (rows synced). An optimistic add must show in the list
  // (head comment row) AND bump the count (overlay delta) — independently, with no double count
  // when the server confirms (the synced comment row and the __agg edit arrive on separate
  // streams but the count is driven only by the overlay, never by the synced rows).
  const { source, store, mutate } = setup();
  const countView = store.materialize(queries.issuesWithCounts());
  const listView = store.materialize(queriesList.allComments());
  const aggQid = source.qidFor("issuesWithCounts");
  const listQid = source.qidFor("allComments");

  // Hydrate: the count query's footprint is issue + __agg (NO comments); the list query's is
  // the comment rows.
  source.snapshot(aggQid, [{ table: "issue", op: "add", row: [5, "five", 0] }, { table: AGG, op: "add", row: [5, 2] }], 1);
  source.snapshot(
    listQid,
    [
      { table: "comment", op: "add", row: [1, 5, "one"] },
      { table: "comment", op: "add", row: [2, 5, "two"] },
    ],
    1,
  );
  source.frame(1);
  assert.deepEqual(count(countView), [2]);
  assert.deepEqual(listView.data.map((c) => c.id), [1, 2]);

  // Optimistic add: shows in the list (head row) and bumps the count (overlay), before confirm.
  mutate.addComment({ id: 3, issueID: 5, body: "three" });
  assert.deepEqual(listView.data.map((c) => c.id), [1, 2, 3], "comment appears in the list optimistically");
  assert.deepEqual(count(countView), [3], "and the count bumps via the overlay");

  // Confirm: the server syncs the real comment row (list stream) + the __agg edit (count
  // stream) + the lmid — all co-transactional.
  source.batch(listQid, [{ table: "comment", op: "add", row: [3, 5, "three"] }], 2);
  source.batch(aggQid, [{ table: AGG, op: "edit", old: [5, 2], new: [5, 3] }], 2);
  source.lmid(1, 2);
  source.frame(2);
  assert.deepEqual(listView.data.map((c) => c.id), [1, 2, 3], "list converged");
  assert.deepEqual(count(countView), [3], "count converged — NOT double-counted from the synced row");
});

test("invoke notifies a count view and its list view cross-view-atomically (one commit, not two brackets)", () => {
  // §4 + cross-view-atomic notification: one `addComment` runs the prediction (the comment row →
  // the LIST view) and then the `__agg`-head reconcile (the count → the COUNT view) as TWO engine
  // commits, but it is ONE logical mutation — so the Store must fold BOTH views before notifying
  // either. A list subscriber re-reading the count (or vice versa) must see the post-commit value,
  // never the sibling's stale half. (Before `inOneCommit`, the prediction commit flushed the list
  // view's subscribers while the count was still its pre-bump value.)
  const { source, store, mutate } = setup();
  const countView = store.materialize(queries.issuesWithCounts());
  const listView = store.materialize(queriesList.allComments());
  const aggQid = source.qidFor("issuesWithCounts");
  const listQid = source.qidFor("allComments");

  source.snapshot(aggQid, [{ table: "issue", op: "add", row: [5, "five", 0] }, { table: AGG, op: "add", row: [5, 2] }], 1);
  source.snapshot(listQid, [{ table: "comment", op: "add", row: [1, 5, "one"] }], 1);
  source.frame(1);
  assert.deepEqual(count(countView), [2]);
  assert.deepEqual(listView.data.map((c) => c.id), [1]);

  // A list subscriber re-reads the count; a count subscriber re-reads the list. Each must observe
  // the sibling already folded. Drop the immediate on-subscribe fire.
  const listSawCount: number[][] = [];
  const countSawList: number[][] = [];
  listView.subscribe(() => listSawCount.push(count(countView)));
  countView.subscribe(() => countSawList.push(listView.data.map((c) => c.id)));
  listSawCount.length = 0;
  countSawList.length = 0;

  mutate.addComment({ id: 2, issueID: 5, body: "two" });

  // Final state is consistent...
  assert.deepEqual(listView.data.map((c) => c.id), [1, 2], "list shows the optimistic comment");
  assert.deepEqual(count(countView), [3], "count bumped via the overlay");
  // ...and each subscriber saw the sibling's post-commit half when it fired (no tear):
  assert.deepEqual(listSawCount, [[3]], "list subscriber saw the count already bumped, not the stale 2");
  assert.deepEqual(countSawList, [[1, 2]], "count subscriber saw the new comment already in the list");
});

test("destroying the last reader RECLAIMS the synthetic table; re-materializing re-creates it cleanly (§4)", () => {
  const { source, store, mutate } = setup();
  const first = store.materialize(queries.issuesWithCounts());
  seedIssue5(source);
  assert.deepEqual(count(first), [2]);
  // Last reader gone → releaseSyntheticTables frees the engine source + baseline + overlay def
  // (the engine REFUSES a live-reader free, so reaching this without a throw proves 0 conns).
  first.destroy();

  // Re-register the same aggregate from a clean slate: the synthetic table is re-created, the
  // server re-hydrates it fresh, and the (re-registered) overlay def still drives the delta.
  const second = store.materialize(queries.issuesWithCounts());
  source.snapshot(2, [{ table: "issue", op: "add", row: [5, "five", 0] }, { table: AGG, op: "add", row: [5, 4] }], 3);
  source.frame(3);
  assert.deepEqual(count(second), [4], "fresh server base after re-create");
  mutate.addComment({ id: 301, issueID: 5, body: "x" });
  assert.deepEqual(count(second), [5], "the re-registered overlay def still computes the optimistic delta");
});

test("refcount: a shared aggregate survives one reader's teardown, freed only at the last (§4)", () => {
  const { source, store } = setup();
  const a = store.materialize(queries.issuesWithCounts());
  const b = store.materialize(queries.issuesWithCounts()); // second reader of the SAME __agg
  seedIssue5(source);
  assert.deepEqual(count(a), [2]);
  assert.deepEqual(count(b), [2]);

  // Dropping one reader must NOT free the table — the other still has a live engine connection
  // (if the refcount were wrong, unregisterTable would throw "live connection(s)").
  a.destroy();
  assert.deepEqual(count(b), [2], "the surviving reader still displays the count");

  // Drop the last reader → table reclaimed; a fresh materialize re-creates and re-hydrates it.
  b.destroy();
  const c = store.materialize(queries.issuesWithCounts());
  source.snapshot(9, [{ table: "issue", op: "add", row: [5, "five", 0] }, { table: AGG, op: "add", row: [5, 7] }], 9);
  source.frame(9);
  assert.deepEqual(count(c), [7], "re-created fresh after the last reader was reclaimed");
});

// ============================================================================================
// 302 room-domain lanes: the overlay under one-source-per-table
// ============================================================================================

const ROOM = "room:doc/agg";

/** The room shape: `addComment` DECLARED a room mutator (302 §5), `comment` room-owned — its
 *  writes stage onto the namespaced twin while the count view reads the daemon-fed `__agg` head. */
function setupRoom() {
  const source = new TestSource();
  const { store, backend, mutate } = createOptimisticStore(schema, source, registry, {
    clientID: "c1",
    domainPolicy: (name) => (name === "addComment" ? ROOM : "daemon"),
  });
  const roomSrc = new TestSource();
  backend.connectSource(ROOM, roomSrc);
  backend.registerRoomTables(ROOM, ["comment"]);
  return { source, roomSrc, store, backend, mutate };
}

test("302 §5: a room-DECLARED addComment bumps the displayed count instantly — the staged ChildOp dispatches on the WIRE child name", () => {
  const { source, store, mutate } = setupRoom();
  const view = store.materialize(queries.issuesWithCounts());
  seedIssue5(source);
  assert.deepEqual(count(view), [2], "server base");

  // The write stages onto `comment@ROOM` (the room's twin), but the overlay's defs are keyed by
  // the ORIGINAL AST's child table (`comment`) — pre-fix the staged name missed the dispatch and
  // the badge lagged the whole write-behind echo behind the list it sits beside.
  mutate.addComment({ id: 101, issueID: 5, body: "a" });
  assert.deepEqual(count(view), [3], "base 2 ⊕ +1 pending — the room-staged write feeds the overlay");
});

test("302: a ROOM release's global rewind must NOT wipe the optimistic count — the overlay rebuilds on EVERY cycle, not only daemon ones", () => {
  const { source, roomSrc, store, backend, mutate } = setupRoom();
  void roomSrc;
  const view = store.materialize(queries.issuesWithCounts());
  seedIssue5(source);
  mutate.addComment({ id: 101, issueID: 5, body: "a" }); // room-domain pending: 2 → 3
  assert.deepEqual(count(view), [3]);

  // ANY room release (here: a collaborator's comment relayed through the room channel) runs the
  // whole-store rewind — including the optimistic `__agg` head edit. Pre-fix the daemonCycle gate
  // skipped the overlay rebuild and the badge visibly snapped back to the server base until the
  // next daemon release.
  backend.__testRelease(ROOM, [{ op: "add", table: "comment@" + ROOM, row: [500, 5, "peer"] }]);
  assert.deepEqual(count(view), [3], "the optimistic ⊕1 survives the room cycle");
});
