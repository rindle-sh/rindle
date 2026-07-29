import { test } from "node:test";
import assert from "node:assert/strict";

import type { Mutation } from "@rindle/client";
import { NormalizedSync, type NormalizedOp } from "../src/sync.ts";

// Golden-vector conformance for the cross-query refcount layer (§5). Each test drives a
// `NormalizedSync` and asserts the NET `Mutation[]` it forwards to the wasm `Db` — the
// property the local IVM depends on (a row shared by N queries lands once; GC at the last).

const issuePk = { issue: [0] };

test("snapshot adds are forwarded one-for-one (0→1 each)", () => {
  const sync = new NormalizedSync(issuePk);
  const ops: NormalizedOp[] = [
    { table: "issue", op: "add", row: [1, "a"] },
    { table: "issue", op: "add", row: [2, "b"] },
  ];
  const muts: Mutation[] = sync.applyBatch(1, ops);
  assert.deepStrictEqual(muts, [
    { op: "add", table: "issue", row: [1, "a"] },
    { op: "add", table: "issue", row: [2, "b"] },
  ]);
  assert.strictEqual(sync.baseSize(), 2);
});

test("a row shared by two queries is synced once; removed only at the last reference", () => {
  const sync = new NormalizedSync(issuePk);

  // q1 adds issue 1 → base 0→1 → forwarded.
  assert.deepStrictEqual(sync.applyBatch(1, [{ table: "issue", op: "add", row: [1, "a"] }]), [
    { op: "add", table: "issue", row: [1, "a"] },
  ]);
  // q2 also references issue 1 → base 1→2 → NOT forwarded (dup-key add would be rejected).
  assert.deepStrictEqual(sync.applyBatch(2, [{ table: "issue", op: "add", row: [1, "a"] }]), []);
  assert.strictEqual(sync.refCount("issue", [1, "a"]), 2);

  // q1 drops it → base 2→1 → NOT forwarded (q2 still references it).
  assert.deepStrictEqual(sync.applyBatch(1, [{ table: "issue", op: "remove", row: [1, "a"] }]), []);
  assert.strictEqual(sync.refCount("issue", [1, "a"]), 1);
  // q2 drops it → base 1→0 → forwarded; the local row is GC'd.
  assert.deepStrictEqual(sync.applyBatch(2, [{ table: "issue", op: "remove", row: [1, "a"] }]), [
    { op: "remove", table: "issue", row: [1, "a"] },
  ]);
  assert.strictEqual(sync.baseSize(), 0);
});

test("intra-query re-add of an already-footprinted row is a no-op (not double-counted)", () => {
  const sync = new NormalizedSync(issuePk);
  sync.applyBatch(1, [{ table: "issue", op: "add", row: [1, "a"] }]);
  assert.deepStrictEqual(sync.applyBatch(1, [{ table: "issue", op: "add", row: [1, "a"] }]), []);
  assert.strictEqual(sync.refCount("issue", [1, "a"]), 1);
});

test("an edit on a shared row is forwarded once, deduped on the sibling query's stream", () => {
  const sync = new NormalizedSync(issuePk);
  sync.applyBatch(1, [{ table: "issue", op: "add", row: [1, "a"] }]);
  sync.applyBatch(2, [{ table: "issue", op: "add", row: [1, "a"] }]); // shared, count 2

  // The edit arrives on BOTH query streams (each footprints the row, §5.2 trade).
  assert.deepStrictEqual(
    sync.applyBatch(1, [{ table: "issue", op: "edit", old: [1, "a"], new: [1, "A"] }]),
    [{ op: "edit", table: "issue", old: [1, "a"], new: [1, "A"] }],
  );
  // Second stream: the base row is already [1,"A"] → deduped, no second Db edit.
  assert.deepStrictEqual(
    sync.applyBatch(2, [{ table: "issue", op: "edit", old: [1, "a"], new: [1, "A"] }]),
    [],
  );
  // PK is stable → refcount unchanged.
  assert.strictEqual(sync.refCount("issue", [1, "A"]), 2);
});

test("cross-query refcount is per (table, pk): EXISTS witnesses share like any base row", () => {
  const sync = new NormalizedSync({ issue: [0], comment: [0] });
  // q1: issue 1 + one comment witness.
  sync.applyBatch(1, [
    { table: "issue", op: "add", row: [1, "a"] },
    { table: "comment", op: "add", row: [10, 1, "x"] },
  ]);
  // q2: the same issue 1 (shared) + a different comment.
  const muts = sync.applyBatch(2, [
    { table: "issue", op: "add", row: [1, "a"] },
    { table: "comment", op: "add", row: [11, 1, "y"] },
  ]);
  // Only the new comment is forwarded; issue 1 is already synced.
  assert.deepStrictEqual(muts, [{ op: "add", table: "comment", row: [11, 1, "y"] }]);
  assert.strictEqual(sync.refCount("issue", [1, "a"]), 2);
  assert.strictEqual(sync.refCount("comment", [10, 1, "x"]), 1);
  assert.strictEqual(sync.baseSize(), 3);
});

test("re-hydrate diffs the new footprint: rows leave/enter, shared rows untouched (§5.3)", () => {
  const sync = new NormalizedSync(issuePk);
  // q1 footprint {1,2}; q2 footprint {2,3} (issue 2 shared).
  sync.applyBatch(1, [
    { table: "issue", op: "add", row: [1, "a"] },
    { table: "issue", op: "add", row: [2, "b"] },
  ]);
  sync.applyBatch(2, [
    { table: "issue", op: "add", row: [2, "b"] },
    { table: "issue", op: "add", row: [3, "c"] },
  ]);

  // q1 re-hydrates to {2, 4}: issue 1 leaves, issue 4 enters, issue 2 stays.
  const muts = sync.rehydrate(1, [
    { table: "issue", op: "add", row: [2, "b"] },
    { table: "issue", op: "add", row: [4, "d"] },
  ]);
  assert.deepStrictEqual(muts, [
    { op: "remove", table: "issue", row: [1, "a"] }, // 1→0
    { op: "add", table: "issue", row: [4, "d"] }, // 0→1
  ]);
  // issue 2 is still referenced by BOTH q1 and q2 — its count is untouched.
  assert.strictEqual(sync.refCount("issue", [2, "b"]), 2);
  assert.strictEqual(sync.refCount("issue", [3, "c"]), 1); // q2 untouched
});

test("re-hydrate emits an edit for an intersecting row whose value changed during the gap", () => {
  const sync = new NormalizedSync(issuePk);
  sync.applyBatch(1, [{ table: "issue", op: "add", row: [1, "a"] }]);
  const muts = sync.rehydrate(1, [{ table: "issue", op: "add", row: [1, "A"] }]);
  assert.deepStrictEqual(muts, [{ op: "edit", table: "issue", old: [1, "a"], new: [1, "A"] }]);
});

test("dropQuery decrements the footprint and GCs rows at the last reference (§5.1)", () => {
  const sync = new NormalizedSync(issuePk);
  sync.applyBatch(1, [
    { table: "issue", op: "add", row: [1, "a"] },
    { table: "issue", op: "add", row: [2, "b"] },
  ]);
  sync.applyBatch(2, [{ table: "issue", op: "add", row: [2, "b"] }]); // issue 2 shared, count 2

  const muts = sync.dropQuery(1);
  // issue 1 (count 1→0) is GC'd; issue 2 (count 2→1) survives for q2.
  assert.deepStrictEqual(muts, [{ op: "remove", table: "issue", row: [1, "a"] }]);
  assert.strictEqual(sync.refCount("issue", [2, "b"]), 1);
  assert.strictEqual(sync.baseSize(), 1);
  // Dropping an unknown query is a harmless no-op.
  assert.deepStrictEqual(sync.dropQuery(99), []);
});

test("registerTable lets a dynamically-registered synthetic aggregate table route + refcount", () => {
  const sync = new NormalizedSync(issuePk);
  // The synthetic `__agg_*` count table is not in the typed schema; the backend registers it.
  sync.registerTable("__agg_abc", [0]); // group key (issue_id) is the PK
  const muts = sync.applyBatch(1, [
    { table: "issue", op: "add", row: [1, "a"] },
    { table: "__agg_abc", op: "add", row: [1, 2] }, // issue 1 → count 2
  ]);
  assert.deepStrictEqual(muts, [
    { op: "add", table: "issue", row: [1, "a"] },
    { op: "add", table: "__agg_abc", row: [1, 2] },
  ]);
  // A server re-count is a value edit on the synthetic row (group key is the PK).
  assert.deepStrictEqual(
    sync.applyBatch(1, [{ table: "__agg_abc", op: "edit", old: [1, 2], new: [1, 3] }]),
    [{ op: "edit", table: "__agg_abc", old: [1, 2], new: [1, 3] }],
  );
  // It refcounts/GCs like any base table: a second query sharing the row, then a drop.
  sync.applyBatch(2, [{ table: "__agg_abc", op: "add", row: [1, 3] }]);
  assert.strictEqual(sync.refCount("__agg_abc", [1, 3]), 2);
  // Dropping q1 GCs its exclusive `issue` row but NOT the synthetic row q2 still holds.
  assert.deepStrictEqual(sync.dropQuery(1), [{ op: "remove", table: "issue", row: [1, "a"] }]);
  assert.strictEqual(sync.refCount("__agg_abc", [1, 3]), 1);
  assert.deepStrictEqual(sync.dropQuery(2), [{ op: "remove", table: "__agg_abc", row: [1, 3] }]);
});

test("unregisterTable drops a synthetic table's PK + residual rows so a re-register starts clean (§4)", () => {
  const sync = new NormalizedSync(issuePk);
  sync.registerTable("__agg_abc", [0]);
  sync.applyBatch(1, [{ table: "__agg_abc", op: "add", row: [1, 2] }]);
  assert.strictEqual(sync.refCount("__agg_abc", [1, 2]), 1);
  assert.strictEqual(sync.baseSize(), 1);

  // A balanced stream GC's the rows; this is the inverse of registerTable. Even with a residual
  // row still present (we did NOT dropQuery), unregisterTable sweeps it and forgets the PK.
  sync.unregisterTable("__agg_abc");
  assert.strictEqual(sync.baseSize(), 0, "residual base row swept");
  // The PK is gone → the table is unknown again (keying it would throw).
  assert.throws(() => sync.baseRow("__agg_abc", [1]), /no primary key registered/);

  // A fresh re-register works exactly like the first time (no stale state collides).
  sync.registerTable("__agg_abc", [0]);
  assert.deepStrictEqual(
    sync.applyBatch(5, [{ table: "__agg_abc", op: "add", row: [1, 9] }]),
    [{ op: "add", table: "__agg_abc", row: [1, 9] }],
    "0→1 again after re-register",
  );

  sync.unregisterTable("__agg_unknown"); // no-op for an unregistered table
});
