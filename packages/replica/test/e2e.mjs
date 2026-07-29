// A3 e2e: the SAME Store/ArrayView/query builder, driven by the NATIVE replica backend,
// must reconstruct EXACTLY what the wasm backend does — proving the `Backend` seam is truly
// engine-agnostic (native SQLite replica vs wasm memory engine). We compare the two backends'
// materialized views step-by-step over a nested query (issue → comments) across hydrate +
// writes. Run with `--conditions=@rindle/source` (build-free); needs the wasm pkg built.
import assert from "node:assert/strict";

import { table, string, number, createSchema, createReplicaStore } from "@rindle/replica";
import { createWasmStore } from "@rindle/wasm";

const issue = table("issue").columns({ id: number(), title: string() }).primaryKey("id");
const comment = table("comment").columns({ id: number(), issueID: number(), body: string() }).primaryKey("id");
const schema = createSchema({ tables: [issue, comment] });

const build = (root) =>
  root.issue
    .sub("comments", comment, { parent: ["id"], child: ["issueID"] }, (c) => c.orderBy("id", "asc"))
    .orderBy("id", "asc");

const rep = createReplicaStore(schema); // native SQLite replica
const wasm = await createWasmStore(schema); // wasm memory engine (oracle)
const rv = build(rep.query).materialize();
const wv = build(wasm.query).materialize();

const apply = (tx, ops) => {
  for (const o of ops) {
    if (o.k === "add") tx.add(o.t, o.r);
    else if (o.k === "remove") tx.remove(o.t, o.r);
    else tx.edit(o.t, o.old, o.r);
  }
};

let n = 0;
async function step(label, ops) {
  await rep.write((tx) => apply(tx, ops));
  await wasm.write((tx) => apply(tx, ops));
  assert.deepStrictEqual(rv.data, wv.data, `step ${++n} (${label})`);
  console.log("✓", label);
}

assert.deepStrictEqual(rv.data, wv.data, "empty hydrate");
console.log("✓ empty hydrate");

await step("seed issues + comments", [
  { k: "add", t: "issue", r: { id: 1, title: "first" } },
  { k: "add", t: "issue", r: { id: 2, title: "second" } },
  { k: "add", t: "comment", r: { id: 10, issueID: 1, body: "a" } },
  { k: "add", t: "comment", r: { id: 11, issueID: 1, body: "b" } },
  { k: "add", t: "comment", r: { id: 20, issueID: 2, body: "z" } },
]);
await step("child add", [{ k: "add", t: "comment", r: { id: 12, issueID: 1, body: "c" } }]);
await step("child edit (body)", [
  { k: "edit", t: "comment", old: { id: 10, issueID: 1, body: "a" }, r: { id: 10, issueID: 1, body: "A" } },
]);
await step("child remove", [{ k: "remove", t: "comment", r: { id: 11, issueID: 1, body: "b" } }]);
await step("root add", [{ k: "add", t: "issue", r: { id: 3, title: "third" } }]);
await step("mixed: child + root add", [
  { k: "add", t: "comment", r: { id: 30, issueID: 3, body: "n" } },
  { k: "add", t: "issue", r: { id: 4, title: "fourth" } },
]);
await step("root remove", [{ k: "remove", t: "issue", r: { id: 2, title: "second" } }]);

// Also confirm the polish features (.one() unwrap + .start() paging) work through the native
// backend, since they exercise the engine's limit/skip lowering over SQLite.
const top = rep.query.issue.orderBy("id", "asc").one().materialize();
assert.deepStrictEqual(top.data, { id: 1, title: "first" }, ".one() over the replica");
const page = rep.query.issue.orderBy("id", "asc").start({ id: 1 }, { exclusive: true }).materialize();
assert.deepStrictEqual(page.data.map((r) => r.id), [3, 4], ".start() paging over the replica");
console.log("✓ .one() + .start() over the native replica");

// --- cross-view-atomic notification (Backend.onCommitBoundary) over the native replica ----------
// ReplicaBackend fans one commit out to many query views exactly like the wasm engine, so a single
// write touching two views must fold BOTH before notifying either: a subscriber that re-reads a
// sibling view in its callback must see post-commit data, never that sibling's pre-commit state.
{
  const a = rep.query.issue.orderBy("id", "asc").materialize();
  const b = rep.query.issue.orderBy("id", "asc").materialize();
  const aSawB = [];
  const bSawA = [];
  a.subscribe(() => aSawB.push(b.data.length));
  b.subscribe(() => bSawA.push(a.data.length));
  aSawB.length = 0; // ignore the immediate on-subscribe fire
  bSawA.length = 0;
  const n0 = a.data.length;
  await rep.write((tx) => tx.add("issue", { id: 999, title: "atomic" }));
  // Whichever query's batch the engine delivers first, the barrier defers BOTH notifications until
  // both views have folded — so each subscriber sees the sibling already at the post-commit length.
  assert.deepStrictEqual(aSawB, [n0 + 1], "A's subscriber saw B already folded (fresh)");
  assert.deepStrictEqual(bSawA, [n0 + 1], "B's subscriber saw A already folded (fresh)");
  a.destroy();
  b.destroy();
  console.log("✓ cross-view-atomic notification over the native replica");
}

console.log("\n✅ native replica backend reconstructs the SAME view as the wasm backend (8 steps)");
