// The per-view CHANGE channel (`view.onChanges` / `store.materialize(query, { onChanges })`) — the
// narration seam that replaces filtering the store-global `subscribeChanges` by qid. Covers: snapshot
// vs batch phase, SUBTRACTIVE no-op-cycle netting (a rebase's balanced remove+add / edit round-trip
// cancels, so a correct prediction delivers nothing), genuine-change preservation, commit-boundary
// deferral, and per-view removed-subtree enrichment (no global opt-in).

import { test } from "node:test";
import assert from "node:assert/strict";

import { createSchema, number, string, table } from "../src/schema.ts";
import { Store } from "../src/store.ts";
import { FlatArrayView } from "../src/view.ts";
import type { Backend, ChangeEvent, FlatChange, QueryId, WireSchema } from "../src/types.ts";

const issue = table("issue").columns({ id: number(), title: string() }).primaryKey("id");
const schema = createSchema({ tables: [issue] });

const issueWire: WireSchema = { columns: ["id", "title"], primaryKey: [0], sort: [[0, true]], singular: false, relationships: [] };
const add = (id: number, title: string): FlatChange => ({ path: [], op: { tag: "add", node: { row: [id, title], rels: [] } } });
const remove = (id: number, title: string): FlatChange => ({ path: [], op: { tag: "remove", row: [id, title] } });
const edit = (id: number, o: string, n: string): FlatChange => ({ path: [], op: { tag: "edit", old: [id, o], new: [id, n] } });

/** The id cell of whichever op shape a change carries. */
function rowId(c: FlatChange): number {
  const op = c.op;
  const row = op.tag === "add" ? op.node.row : op.tag === "remove" ? op.row : op.new;
  return row[0] as number;
}

/** A scriptable fake backend: `registerQuery` synchronously fires hello+snapshot (the in-process
 *  shape); the test drives later batches with `emit`. */
class FakeBackend implements Backend {
  private handler: (qid: QueryId, ev: ChangeEvent) => void = () => {};
  helloSchema: WireSchema = issueWire;
  snapshot: FlatChange[] = [];
  registerQuery(qid: QueryId): void {
    this.handler(qid, { type: "hello", schema: this.helloSchema, comparatorVersion: 1 });
    this.handler(qid, { type: "snapshot", adds: this.snapshot, last: true });
  }
  unregisterQuery(): void {}
  mutate(): Promise<void> {
    return Promise.resolve();
  }
  onEvent(h: (qid: QueryId, ev: ChangeEvent) => void): void {
    this.handler = h;
  }
  emit(qid: QueryId, ev: ChangeEvent): void {
    this.handler(qid, ev);
  }
}

/** Brackets a commit the way the in-process engine does, so the Store defers the view's fold +
 *  change delivery to the boundary. */
class CommitBackend extends FakeBackend {
  private boundary: (p: "begin" | "end") => void = () => {};
  onCommitBoundary(h: (p: "begin" | "end") => void): void {
    this.boundary = h;
  }
  commit(qid: QueryId, events: FlatChange[]): void {
    this.boundary("begin");
    try {
      this.emit(qid, { type: "batch", events });
    } finally {
      this.boundary("end");
    }
  }
  /** Fold a `snapshot` AND a `batch` into the same view inside ONE commit bracket — the contended
   *  shape the pending buffer must keep phase-separate. */
  commitMixed(qid: QueryId, snapshotAdds: FlatChange[], batchEvents: FlatChange[]): void {
    this.boundary("begin");
    try {
      this.emit(qid, { type: "snapshot", adds: snapshotAdds, last: true });
      this.emit(qid, { type: "batch", events: batchEvents });
    } finally {
      this.boundary("end");
    }
  }
}

// A parent table with one non-singular child relationship ("kids"), for nested-subtree netting.
const childWire: WireSchema = { columns: ["cid", "pid"], primaryKey: [0], sort: [[0, true]], singular: false, relationships: [] };
const parentWire: WireSchema = {
  columns: ["id", "name"],
  primaryKey: [0],
  sort: [[0, true]],
  singular: false,
  relationships: [{ name: "kids", slot: 0, child: childWire, project: null }],
};
/** An `add` of parent `id` whose "kids" slot holds the given child ids. */
const addParent = (id: number, kids: number[]): FlatChange => ({
  path: [],
  op: { tag: "add", node: { row: [id, "p" + id], rels: [{ rel: 0, children: kids.map((c) => ({ row: [c, id], rels: [] })) }] } },
});
const removeParent = (id: number): FlatChange => ({ path: [], op: { tag: "remove", row: [id, "p" + id] } });

test("onChanges delivers the initial snapshot then batches, tagged by phase", () => {
  const be = new FakeBackend();
  be.snapshot = [add(1, "one")];
  const store = new Store(schema, be);
  const seen: Array<{ ids: number[]; phase: string }> = [];
  const view = store.materialize(store.query.issue, {
    onChanges: (changes, phase) => seen.push({ ids: changes.map(rowId), phase }),
  });
  // The synchronous snapshot is caught because onChanges was wired before registerQuery.
  assert.deepEqual(seen, [{ ids: [1], phase: "snapshot" }]);
  be.emit(view.qid, { type: "batch", events: [add(2, "two")] });
  assert.deepEqual(seen, [
    { ids: [1], phase: "snapshot" },
    { ids: [2], phase: "batch" },
  ]);
});

test("onChanges nets a balanced remove+add of the same row to nothing (rebase no-op)", () => {
  const be = new FakeBackend();
  be.snapshot = [add(1, "one")];
  const store = new Store(schema, be);
  const seen: FlatChange[][] = [];
  const view = store.materialize(store.query.issue, { onChanges: (c) => seen.push(c) });
  seen.length = 0; // drop the snapshot delivery
  // A rebase re-invokes a still-pending prediction: remove R, then re-add the identical R.
  be.emit(view.qid, { type: "batch", events: [remove(1, "one"), add(1, "one")] });
  assert.equal(seen.length, 0, "a correctly predicted rebase delivers no change");
  assert.equal(view.data.length, 1, "and the view is unchanged");
});

test("onChanges nets an edit round-trip (a→b then b→a) to nothing", () => {
  const be = new FakeBackend();
  be.snapshot = [add(1, "a")];
  const store = new Store(schema, be);
  const seen: FlatChange[][] = [];
  const view = store.materialize(store.query.issue, { onChanges: (c) => seen.push(c) });
  seen.length = 0;
  be.emit(view.qid, { type: "batch", events: [edit(1, "a", "b"), edit(1, "b", "a")] });
  assert.equal(seen.length, 0);
  assert.equal((view.data[0] as { title: string }).title, "a");
});

test("onChanges keeps genuine changes (distinct remove+add, real edit) through the net", () => {
  const be = new FakeBackend();
  be.snapshot = [add(1, "one"), add(2, "two")];
  const store = new Store(schema, be);
  const seen: FlatChange[][] = [];
  const view = store.materialize(store.query.issue, { onChanges: (c) => seen.push(c) });
  seen.length = 0;
  be.emit(view.qid, { type: "batch", events: [remove(1, "one"), add(3, "three")] });
  assert.equal(seen.length, 1);
  assert.deepEqual([...seen[0].map(rowId)].sort(), [1, 3], "remove 1 + add 3 both survive");
  seen.length = 0;
  be.emit(view.qid, { type: "batch", events: [edit(2, "two", "TWO")] });
  assert.equal(seen.length, 1);
  assert.equal(rowId(seen[0][0]), 2);
});

test("onChanges is deferred to the commit boundary and netted across it", () => {
  const be = new CommitBackend();
  be.snapshot = [add(1, "one")];
  const store = new Store(schema, be);
  const seen: FlatChange[][] = [];
  const view = store.materialize(store.query.issue, { onChanges: (c) => seen.push(c) });
  seen.length = 0;
  be.commit(view.qid, [remove(1, "one"), add(1, "one")]); // a no-op cycle inside one commit
  assert.equal(seen.length, 0, "netted to nothing at the commit boundary");
  be.commit(view.qid, [add(2, "two")]);
  assert.deepEqual(
    seen.map((b) => b.map(rowId)),
    [[2]],
  );
});

test("onChanges maps a catchUp batch to the snapshot phase (optimistic hydration is not a change)", () => {
  // The optimistic backend hydrates a fresh query through its reconcile cycle, so the initial result
  // set arrives as a `batch` with `catchUp` set — the Store must phase it `snapshot`, not `batch`, or
  // a "what CHANGED" narrator narrates the whole initial dataset as fresh adds.
  const be = new FakeBackend();
  const store = new Store(schema, be);
  const seen: Array<{ ids: number[]; phase: string }> = [];
  const view = store.materialize(store.query.issue, {
    onChanges: (changes, phase) => seen.push({ ids: changes.map(rowId), phase }),
  });
  seen.length = 0; // drop the (empty) initial snapshot
  be.emit(view.qid, { type: "batch", events: [add(1, "one"), add(2, "two")], catchUp: true });
  assert.deepEqual(seen, [{ ids: [1, 2], phase: "snapshot" }], "catchUp batch phases as snapshot");
  be.emit(view.qid, { type: "batch", events: [add(3, "three")] });
  assert.deepEqual(seen.at(-1), { ids: [3], phase: "batch" }, "a normal later batch stays a batch");
});

test("onChanges delivers only the ops that actually changed the view (no-ops and dups are dropped)", () => {
  const be = new FakeBackend();
  be.snapshot = [add(1, "one")];
  const store = new Store(schema, be);
  const seen: FlatChange[][] = [];
  const view = store.materialize(store.query.issue, { onChanges: (c) => seen.push(c) });
  seen.length = 0;
  // A no-op edit (old == new) and a real add in one batch: only the add is a change, so only it is
  // narrated — a rejected/no-op op must never reach a narrator just because a batch-mate applied.
  be.emit(view.qid, { type: "batch", events: [edit(1, "one", "one"), add(2, "two")] });
  assert.equal(seen.length, 1);
  assert.deepEqual(seen[0].map(rowId), [2], "the no-op edit is dropped, only the real add survives");
  // A batch of ONLY no-ops delivers nothing at all.
  seen.length = 0;
  be.emit(view.qid, { type: "batch", events: [edit(1, "one", "one")] });
  assert.equal(seen.length, 0, "an all-no-op batch makes no delivery");
});

test("opRowKey distinguishes special numbers JSON folds together (Infinity/null must not cancel)", () => {
  // JSON.stringify serializes Infinity/NaN as null and -0 as 0; keying on raw JSON would let a
  // `remove [1, Infinity]` cancel an `add [1, null]`, silently dropping a real change. The sentinel
  // encoding keeps them distinct, so both survive the net.
  const be = new FakeBackend();
  be.snapshot = [{ path: [], op: { tag: "add", node: { row: [1, Infinity], rels: [] } } }];
  const store = new Store(schema, be);
  const seen: FlatChange[][] = [];
  const view = store.materialize(store.query.issue, { onChanges: (c) => seen.push(c) });
  seen.length = 0;
  be.emit(view.qid, {
    type: "batch",
    events: [
      { path: [], op: { tag: "remove", row: [1, Infinity] } },
      { path: [], op: { tag: "add", node: { row: [1, null], rels: [] } } },
    ],
  });
  assert.equal(seen.length, 1, "the remove and add are NOT an inverse pair, so the batch delivers");
  assert.equal(seen[0].length, 2, "both the remove and the re-add survive the net");
});

test("a buffered commit and a snapshot in one bracket keep their own phases (no last-writer-wins)", () => {
  // A single commit bracket that folds both a snapshot and a batch into one view must deliver each
  // under its OWN phase — not merge them under whichever arrived last.
  const be = new CommitBackend();
  const store = new Store(schema, be);
  const seen: Array<{ ids: number[]; phase: string }> = [];
  const view = store.materialize(store.query.issue, {
    onChanges: (changes, phase) => seen.push({ ids: changes.map(rowId), phase }),
  });
  seen.length = 0;
  be.commitMixed(view.qid, [add(5, "five")], [add(6, "six")]);
  assert.deepEqual(
    seen,
    [
      { ids: [5], phase: "snapshot" },
      { ids: [6], phase: "batch" },
    ],
    "the snapshot stays a snapshot and the batch stays a batch",
  );
});

test("reset clears the pending change buffer so pre-epoch changes never deliver against the new schema", () => {
  const view = new FlatArrayView<{ id: number; title: string }>(issueWire, undefined, 1);
  const seen: FlatChange[][] = [];
  view.onChanges((c) => seen.push(c));
  // Buffer a change (deferred), then re-hydrate (reset) before the flush: the buffered pre-epoch
  // change must be discarded, not delivered against the new epoch's schema.
  view.applyChanges([add(1, "one")], false, true, "batch");
  view.reset(issueWire);
  view.flush();
  assert.equal(seen.length, 0, "the pre-reset buffered change is dropped");
});

test("a view with an onChanges listener enriches its own remove ops with the subtree (no global opt-in)", () => {
  const childWire: WireSchema = { columns: ["cid", "pid"], primaryKey: [0], sort: [[0, true]], singular: false, relationships: [] };
  const parentWire: WireSchema = {
    columns: ["id", "name"],
    primaryKey: [0],
    sort: [[0, true]],
    singular: false,
    relationships: [{ name: "kids", slot: 0, child: childWire, project: null }],
  };
  const view = new FlatArrayView(parentWire, undefined, 1);
  const got: FlatChange[][] = [];
  view.onChanges((c) => got.push(c));
  // snapshot: parent 1 with one kid.
  view.applyChanges(
    [{ path: [], op: { tag: "add", node: { row: [1, "p1"], rels: [{ rel: 0, children: [{ row: [10, 1], rels: [] }] }] } } }],
    false,
    false,
    "snapshot",
  );
  got.length = 0;
  // A bare remove carries only the leaving row; because a narrator is attached, the view reconstructs
  // the evicted subtree onto op.node — the per-view twin of the store's global removedSubtree counter.
  view.applyChanges([{ path: [], op: { tag: "remove", row: [1, "p1"] } }], false, false, "batch");
  assert.equal(got.length, 1);
  const op = got[0][0].op;
  assert.ok(op.tag === "remove" && op.node, "the remove carries the reconstructed subtree");
  assert.deepEqual(op.tag === "remove" ? op.node?.rels : null, [{ rel: 0, children: [{ row: [10, 1], rels: [] }] }]);
});

test("netting cancels a remove+re-add with an IDENTICAL subtree (a true nested no-op cycle)", () => {
  const view = new FlatArrayView(parentWire, undefined, 1);
  const got: FlatChange[][] = [];
  view.onChanges((c) => got.push(c));
  view.applyChanges([addParent(1, [10])], false, false, "snapshot"); // parent 1 with kid 10
  got.length = 0;
  // Remove parent 1 (enriched subtree [10]) and re-add it with the SAME subtree [10] in one batch:
  // a genuine inverse pair — it nets to nothing.
  view.applyChanges([removeParent(1), addParent(1, [10])], false, false, "batch");
  assert.equal(got.length, 0, "identical-subtree remove+re-add cancels");
  assert.deepEqual((view.data[0] as { kids: unknown[] }).kids.length, 1, "and the view is unchanged");
});

test("netting does NOT cancel a remove+re-add whose SUBTREE changed (child rode in on the re-add)", () => {
  const view = new FlatArrayView(parentWire, undefined, 1);
  const got: FlatChange[][] = [];
  view.onChanges((c) => got.push(c));
  view.applyChanges([addParent(1, [10])], false, false, "snapshot"); // parent 1 with kid 10
  got.length = 0;
  // Parent 1 leaves (subtree [10]) and re-enters with a DIFFERENT child set [11] in the same batch —
  // the child never got its own event (it changed while the parent was gated out), so the re-add is
  // its only carrier. Cancelling on the root row alone would silently drop it; the subtree check
  // keeps BOTH ops so the nested change is narrated.
  view.applyChanges([removeParent(1), addParent(1, [11])], false, false, "batch");
  assert.equal(got.length, 1, "the pair is not an inverse, so it delivers");
  assert.deepEqual(got[0].map(rowId), [1, 1], "both the remove and the re-add of parent 1 survive");
  assert.deepEqual((view.data[0] as { kids: Array<{ cid: number }> }).kids.map((k) => k.cid), [11], "view shows the new child");
});

test("a throwing change listener neither starves siblings/data nor is silently swallowed", () => {
  const be = new FakeBackend();
  be.snapshot = [add(1, "one")];
  const store = new Store(schema, be);
  const data: number[] = [];
  const second: number[] = [];
  const view = store.materialize(store.query.issue);
  // Attach after materialize (onChanges does not replay), so the throwing listener does not fire on
  // the synchronous snapshot — we exercise isolation on the later batch. Throwing listener FIRST so a
  // sibling registered after it must still run.
  view.onChanges(() => {
    throw new Error("boom from a narration template");
  });
  view.onChanges((c) => second.push(...c.map(rowId)));
  view.subscribe((d) => data.push(d.length));
  data.length = 0;
  // The first (throwing) change listener must not prevent the fold, the data notify, or the second
  // change listener; the error still surfaces (mirroring the Store's per-view flush isolation).
  assert.throws(() => be.emit(view.qid, { type: "batch", events: [add(2, "two")] }), /boom from a narration template/);
  assert.equal(view.data.length, 2, "the fold still happened");
  assert.deepEqual(data, [2], "the data subscriber still fired");
  assert.deepEqual(second, [2], "the sibling change listener still received the change");
});
