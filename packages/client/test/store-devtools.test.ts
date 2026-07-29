// The dev-only Store introspection seam (DEBUG-TOOLS-BROWSER-DESIGN.md §2/§6.2): `__inspect()` over
// the live views, and the supported `subscribeChanges` / `subscribeResultType` taps (the same seams
// a narrator rides, also used by devtools) that mirror the per-query delta + resultType streams
// WITHOUT displacing the backend's single handlers (so a pane can't perturb the app).

import { test } from "node:test";
import assert from "node:assert/strict";

import { createSchema, number, string, table } from "../src/schema.ts";
import { Store } from "../src/store.ts";
import type { Backend, ChangeEvent, FlatChange, Mutation, QueryId, RemoteQuery, ResultType, WireSchema } from "../src/types.ts";

const issue = table("issue").columns({ id: number(), title: string() }).primaryKey("id");
const schema = createSchema({ tables: [issue] });

const wire: WireSchema = { columns: ["id", "title"], primaryKey: [0], sort: [[0, true]], singular: false, relationships: [] };
const addIssue = (id: number, title: string): FlatChange => ({ path: [], op: { tag: "add", node: { row: [id, title], rels: [] } } });

/** A fake backend that emits hello+snapshot on register and exposes a resultType pump. */
class FakeBackend implements Backend {
  private handler: (qid: QueryId, ev: ChangeEvent) => void = () => {};
  private rt: (qid: QueryId, rt: ResultType) => void = () => {};
  snapshot: FlatChange[] = [];

  registerQuery(qid: QueryId, _ast: unknown, _remote?: RemoteQuery): void {
    this.handler(qid, { type: "hello", schema: wire, comparatorVersion: 1 });
    this.handler(qid, { type: "snapshot", adds: this.snapshot, last: true });
  }
  unregisterQuery(): void {}
  mutate(_m: Mutation[]): Promise<void> {
    return Promise.resolve();
  }
  onEvent(h: (qid: QueryId, ev: ChangeEvent) => void): void {
    this.handler = h;
  }
  onResultType(h: (qid: QueryId, rt: ResultType) => void): void {
    this.rt = h;
  }
  emit(qid: QueryId, ev: ChangeEvent): void {
    this.handler(qid, ev);
  }
  pushResultType(qid: QueryId, rt: ResultType): void {
    this.rt(qid, rt);
  }
}

test("__inspect surfaces each live view's ast, resultType, row count, and a sample", () => {
  const be = new FakeBackend();
  be.snapshot = [addIssue(1, "first"), addIssue(2, "second")];
  const store = new Store(schema, be);
  store.query.issue.materialize();

  const snap = store.__inspect();
  assert.equal(snap.queries.length, 1);
  const q = snap.queries[0];
  assert.equal(q.ast.table, "issue");
  assert.equal(q.resultType, "complete");
  assert.equal(q.rowCount, 2);
  assert.deepEqual(q.sample, [{ id: 1, title: "first" }, { id: 2, title: "second" }]);

  // sampleRows caps the peek without changing the reported count.
  assert.equal(store.__inspect(1).queries[0].sample.length, 1);
  assert.equal(store.__inspect(1).queries[0].rowCount, 2);
});

test("subscribeChanges + subscribeResultType tap deltas + resultType additively, and detach cleanly", () => {
  const be = new FakeBackend();
  be.snapshot = [addIssue(1, "first")];
  const store = new Store(schema, be);

  const deltas: Array<{ qid: QueryId; type: string }> = [];
  const rts: Array<{ qid: QueryId; rt: ResultType }> = [];
  const detachDeltas = store.subscribeChanges((qid, ev) => deltas.push({ qid, type: ev.type }));
  const detachRts = store.subscribeResultType((qid, rt) => rts.push({ qid, rt }));

  const view = store.query.issue.materialize(); // qid 1: hello + snapshot
  assert.deepEqual(deltas.map((d) => d.type), ["hello", "snapshot"], "tap sees the register-time stream");

  be.emit(1 as QueryId, { type: "batch", events: [addIssue(2, "second")] });
  assert.equal(deltas.at(-1)?.type, "batch");
  // The app's own routing still works (the tap is additive, not a replacement).
  assert.equal(view.data.length, 2);

  be.pushResultType(1 as QueryId, "unknown");
  assert.deepEqual(rts, [{ qid: 1, rt: "unknown" }]);
  assert.equal(view.resultType, "unknown", "the view's resultType still updated");

  detachDeltas();
  detachRts();
  be.emit(1 as QueryId, { type: "batch", events: [addIssue(3, "third")] });
  be.pushResultType(1 as QueryId, "complete");
  assert.equal(deltas.filter((d) => d.type === "batch").length, 1, "no taps after detach");
  assert.equal(rts.length, 1);
  assert.equal(view.data.length, 3, "but the app keeps receiving events");
});

test("subscribeChanges({ removedSubtree }) attaches the removed subtree to a remove op", () => {
  // A parent view with a child relationship: removing the parent ships only its leaving row on the
  // wire; the opted-in stream reconstructs the full subtree (parent + child) onto `op.node`.
  const wireParent: WireSchema = {
    columns: ["id", "title"],
    primaryKey: [0],
    sort: [[0, true]],
    singular: false,
    relationships: [{ name: "kids", slot: 0, child: { columns: ["id", "pid"], primaryKey: [0], sort: [[0, true]], singular: false, relationships: [] } }],
  };
  class RelBackend extends FakeBackend {
    registerQuery(qid: QueryId): void {
      this.emit(qid, { type: "hello", schema: wireParent, comparatorVersion: 1 });
      this.emit(qid, {
        type: "snapshot",
        adds: [{ path: [], op: { tag: "add", node: { row: [1, "p"], rels: [{ rel: 0, children: [{ row: [10, 1], rels: [] }] }] } } }],
        last: true,
      });
    }
  }
  const be = new RelBackend();
  const store = new Store(schema, be);

  const removes: FlatChange[] = [];
  store.subscribeChanges(
    (_qid, ev) => {
      if (ev.type === "batch") removes.push(...ev.events.filter((c) => c.op.tag === "remove"));
    },
    { removedSubtree: true },
  );

  store.query.issue.materialize();
  be.emit(1 as QueryId, { type: "batch", events: [{ path: [], op: { tag: "remove", row: [1, "p"] } }] });

  assert.equal(removes.length, 1);
  const op = removes[0].op;
  assert.equal(op.tag, "remove");
  if (op.tag !== "remove") return;
  assert.ok(op.node, "the removed subtree rode along on the remove op");
  assert.deepEqual(op.node?.row, [1, "p"]);
  assert.deepEqual(op.node?.rels, [{ rel: 0, children: [{ row: [10, 1], rels: [] }] }], "the child slot was reconstructed");
});

test("subscribeChanges hands the post-fold view (always the plural view, even for a .one() query)", () => {
  const be = new FakeBackend();
  be.snapshot = [addIssue(1, "first")];
  const store = new Store(schema, be);

  const seen: Array<{ type: string; rowCount: number | undefined }> = [];
  store.subscribeChanges((_qid, ev, view) => {
    // The view is handed in ALREADY folded — re-reading it here sees the post-apply state.
    seen.push({ type: ev.type, rowCount: view?.data.length });
  });

  // A top-level `.one()`: the app handle is a SingularArrayView (its `.data` is the row | null),
  // but the change stream hands the PLURAL view (its `.data` is an array) so list context works.
  const singular = store.query.issue.where.id(1).one().materialize();
  assert.deepEqual(singular.data, { id: 1, title: "first" }, "the app handle unwraps to the single row");

  const snap = seen.find((s) => s.type === "snapshot");
  assert.equal(snap?.rowCount, 1, "the listener got the plural, post-fold view — not an unwrapped singular");

  be.emit(1 as QueryId, { type: "batch", events: [{ path: [], op: { tag: "remove", row: [1, "first"] } }] });
  assert.equal(seen.at(-1)?.type, "batch");
  assert.equal(seen.at(-1)?.rowCount, 0, "the view passed reflects the just-applied removal");
});

test("subscribeChanges without the option leaves a remove op row-only (no reconstruction cost)", () => {
  const be = new FakeBackend();
  be.snapshot = [addIssue(1, "first")];
  const store = new Store(schema, be);

  let lastRemove: FlatChange | undefined;
  store.subscribeChanges((_qid, ev) => {
    if (ev.type === "batch") for (const c of ev.events) if (c.op.tag === "remove") lastRemove = c;
  });

  store.query.issue.materialize();
  be.emit(1 as QueryId, { type: "batch", events: [{ path: [], op: { tag: "remove", row: [1, "first"] } }] });

  assert.ok(lastRemove);
  assert.equal(lastRemove?.op.tag, "remove");
  if (lastRemove?.op.tag === "remove") assert.equal(lastRemove.op.node, undefined, "no subtree attached when not opted in");
});
