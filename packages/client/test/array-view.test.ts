import { test } from "node:test";
import assert from "node:assert/strict";

import { FlatArrayView } from "../src/view.ts";
import type { FlatChange, WireNode, WireSchema } from "../src/types.ts";

const commentSchema: WireSchema = {
  columns: ["id", "issue", "body"],
  primaryKey: [0],
  sort: [[0, true]],
  singular: false,
  relationships: [],
};
const issueSchema: WireSchema = {
  columns: ["id", "title"],
  primaryKey: [0],
  sort: [[0, true]],
  singular: false,
  relationships: [{ name: "comments", slot: 0, child: commentSchema }],
};

const comment = (id: number, issue: number, body: string): WireNode => ({ row: [id, issue, body], rels: [] });
const issueAdd = (id: number, title: string, comments: WireNode[]): FlatChange => ({
  path: [],
  op: { tag: "add", node: { row: [id, title], rels: comments.length ? [{ rel: 0, children: comments }] : [] } },
});
const childAdd = (parentRow: (number | string)[], c: WireNode): FlatChange => ({
  path: [{ rel: 0, parentRow }],
  op: { tag: "add", node: c },
});
const childRemove = (parentRow: (number | string)[], row: (number | string)[]): FlatChange => ({
  path: [{ rel: 0, parentRow }],
  op: { tag: "remove", row },
});
const childEdit = (
  parentRow: (number | string)[],
  oldRow: (number | string)[],
  newRow: (number | string)[],
): FlatChange => ({ path: [{ rel: 0, parentRow }], op: { tag: "edit", old: oldRow, new: newRow } });

test("hydrate snapshot → nested, named tree", () => {
  const v = new FlatArrayView<any>(issueSchema);
  v.applyChanges([
    issueAdd(1, "first", [comment(10, 1, "a"), comment(11, 1, "b")]),
    issueAdd(2, "second", [comment(20, 2, "z")]),
  ]);
  assert.deepStrictEqual(v.data, [
    { id: 1, title: "first", comments: [{ id: 10, issue: 1, body: "a" }, { id: 11, issue: 1, body: "b" }] },
    { id: 2, title: "second", comments: [{ id: 20, issue: 2, body: "z" }] },
  ]);
});

test("child add keeps sort order; edit-in-place; remove", () => {
  const v = new FlatArrayView<any>(issueSchema);
  v.applyChanges([issueAdd(1, "first", [comment(10, 1, "a"), comment(12, 1, "c")])]);

  v.applyChanges([childAdd([1, "first"], comment(11, 1, "b"))]);
  assert.deepStrictEqual(v.data[0].comments.map((c: any) => c.id), [10, 11, 12]);

  v.applyChanges([childEdit([1, "first"], [10, 1, "a"], [10, 1, "A"])]); // sort key (id) unchanged
  assert.strictEqual(v.data[0].comments[0].body, "A");

  v.applyChanges([childRemove([1, "first"], [11, 1, "b"])]);
  assert.deepStrictEqual(v.data[0].comments.map((c: any) => c.id), [10, 12]);
});

test("in-view gate drops a change addressed at a gating (child: null) slot", () => {
  const gated: WireSchema = {
    ...issueSchema,
    relationships: [
      { name: "comments", slot: 0, child: commentSchema },
      { name: "gate", slot: 1, child: null },
    ],
  };
  const v = new FlatArrayView<any>(gated);
  v.applyChanges([issueAdd(1, "first", [comment(10, 1, "a")])]);
  const before = JSON.stringify(v.data);
  // A child change addressed at the gating slot (rel 1) is dropped (no-op).
  v.applyChanges([{ path: [{ rel: 1, parentRow: [1, "first"] }], op: { tag: "add", node: comment(99, 1, "x") } }]);
  assert.strictEqual(JSON.stringify(v.data), before, "gating-slot change is a no-op");
  assert.ok(!("gate" in v.data[0]), "gating slot is not projected into .data");
});

test("root edit-move (rc==1) relocates by the non-PK sort key", () => {
  // root sorted by val asc, then id (so editing `val` moves the row)
  const rs: WireSchema = {
    columns: ["id", "val"],
    primaryKey: [0],
    sort: [[1, true], [0, true]],
    singular: false,
    relationships: [],
  };
  const v = new FlatArrayView<any>(rs);
  const add = (id: number, val: number): FlatChange => ({ path: [], op: { tag: "add", node: { row: [id, val], rels: [] } } });
  v.applyChanges([add(1, 10), add(2, 20), add(3, 30)]);
  assert.deepStrictEqual(v.data.map((r: any) => r.id), [1, 2, 3]);

  v.applyChanges([{ path: [], op: { tag: "edit", old: [1, 10], new: [1, 25] } }]); // 10 → 25, between 20 and 30
  assert.deepStrictEqual(v.data.map((r: any) => r.id), [2, 1, 3]);
  assert.deepStrictEqual(v.data.map((r: any) => r.val), [20, 25, 30]);
});

test("structural sharing: untouched subtrees keep object identity across a change", () => {
  const v = new FlatArrayView<any>(issueSchema);
  v.applyChanges([issueAdd(1, "first", [comment(10, 1, "a")]), issueAdd(2, "second", [comment(20, 2, "z")])]);
  const d1 = v.data;

  v.applyChanges([childAdd([1, "first"], comment(11, 1, "b"))]); // touches issue 1 only
  const d2 = v.data;

  assert.notStrictEqual(d1, d2, "top array is a fresh reference after a change");
  assert.strictEqual(d1[1], d2[1], "issue 2 (untouched) keeps the same object reference");
  assert.notStrictEqual(d1[0], d2[0], "issue 1 (changed) is a new object reference");
});

test("projected no-op child edit does not notify or invalidate the parent", () => {
  const idOnlyCommentSchema: WireSchema = {
    columns: ["id"],
    primaryKey: [0],
    sort: [[0, true]],
    singular: false,
    relationships: [],
  };
  const idOnlyIssueSchema: WireSchema = {
    ...issueSchema,
    relationships: [{ name: "comments", slot: 0, child: idOnlyCommentSchema }],
  };
  const v = new FlatArrayView<any>(idOnlyIssueSchema);
  v.applyChanges([{ path: [], op: { tag: "add", node: { row: [1, "first"], rels: [{ rel: 0, children: [{ row: [10], rels: [] }] }] } } }]);
  const before = v.data;
  let notifications = 0;
  const unsub = v.subscribe(() => {
    notifications++;
  });
  notifications = 0; // ignore subscribe's immediate fire

  v.applyChanges([childEdit([1, "first"], [10], [10])]);

  assert.equal(notifications, 0);
  assert.strictEqual(v.data, before);
  assert.strictEqual(v.data[0], before[0]);
  unsub();
});

// --- scalar projection (REDUCE-DESIGN.md §9): issue { commentCount: count(comments) } ---
//
// The aggregate lives in the tree as a singular one-row relationship whose child is the
// synthetic [issueID, count] row; the `project` annotation unwraps it to a scalar field.
// The reduce delivers the child via the same Add/Edit/Remove flat changes; the receiver
// reconstructs the (plural one-row) child and the projection presents the scalar.

const aggChildSchema: WireSchema = {
  columns: ["issueID", "count"],
  primaryKey: [0],
  sort: [[0, true]],
  singular: true,
  relationships: [],
};
// commentCount projects child col 1 (count); a childless issue → identity 0.
const projectedIssueSchema: WireSchema = {
  columns: ["id", "title"],
  primaryKey: [0],
  sort: [[0, true]],
  singular: false,
  relationships: [{ name: "commentCount", slot: 0, child: aggChildSchema, project: { col: 1, identity: 0 } }],
};
const aggRow = (issueID: number, count: number): WireNode => ({ row: [issueID, count], rels: [] });
const issueWithAgg = (id: number, title: string, agg: WireNode[]): FlatChange => ({
  path: [],
  op: { tag: "add", node: { row: [id, title], rels: agg.length ? [{ rel: 0, children: agg }] : [] } },
});

test("scalar projection unwraps the one-row aggregate child to a scalar field", () => {
  const v = new FlatArrayView<any>(projectedIssueSchema);
  v.applyChanges([
    issueWithAgg(10, "ten", [aggRow(10, 2)]),
    issueWithAgg(20, "twenty", [aggRow(20, 1)]),
    issueWithAgg(30, "thirty", []), // childless → identity
  ]);
  assert.deepStrictEqual(v.data, [
    { id: 10, title: "ten", commentCount: 2 },
    { id: 20, title: "twenty", commentCount: 1 },
    { id: 30, title: "thirty", commentCount: 0 }, // empty relationship → aggregate identity 0
  ]);
});

test("scalar projection tracks Child{Edit/Add/Remove} on the aggregate child", () => {
  const v = new FlatArrayView<any>(projectedIssueSchema);
  v.applyChanges([issueWithAgg(10, "ten", [aggRow(10, 2)]), issueWithAgg(30, "thirty", [])]);

  // Edit (count shift): the singular child's sort key (issueID) is unchanged → edit in place.
  v.applyChanges([childEdit([10, "ten"], [10, 2], [10, 3])]);
  assert.strictEqual(v.data[0].commentCount, 3);

  // Birth on the childless issue: the relationship gains its one row → 0 becomes 1.
  v.applyChanges([childAdd([30, "thirty"], aggRow(30, 1))]);
  assert.strictEqual(v.data[1].commentCount, 1);

  // Death: removing the only row drains back to empty → identity 0.
  v.applyChanges([childRemove([30, "thirty"], [30, 1])]);
  assert.strictEqual(v.data[1].commentCount, 0);
});

test("subscribe fires immediately, then after each applied batch", () => {
  const v = new FlatArrayView<any>(issueSchema);
  const seen: number[] = [];
  const unsub = v.subscribe((data) => seen.push(data.length));
  assert.deepStrictEqual(seen, [0], "fires immediately with current (empty) data");
  v.applyChanges([issueAdd(1, "first", [])]);
  assert.deepStrictEqual(seen, [0, 1], "fires after a batch");
  unsub();
  v.applyChanges([issueAdd(2, "second", [])]);
  assert.deepStrictEqual(seen, [0, 1], "no fire after unsubscribe");
});
