// Slice 5/6/7 e2e (packaged form): the dream API via the published packages —
// `await createWasmStore(schema)`, `store.query.<table>…materialize()`, `store.write(...)` —
// reconstructs EXACTLY what the wasm `RindleView` materializes, across hydrate + writes.
// Run after `pnpm --filter @rindle/wasm build` (or packages/wasm/build.sh).
import assert from "node:assert/strict";

import {
  table,
  string,
  number,
  createSchema,
  queries,
  createWasmStore,
  initWasm,
} from "@rindle/wasm"; // @rindle/wasm re-exports @rindle/client + adds the backend
import { RindleView } from "../pkg/rindle.js";

await initWasm();

const issue = table("issue").columns({ id: number(), title: string() }).primaryKey("id");
const comment = table("comment").columns({ id: number(), issueID: number(), body: string() }).primaryKey("id");
const schema = createSchema({ tables: [issue, comment] });
const buildQuery = (root) =>
  root.issue
    .sub("comments", comment, { parent: ["id"], child: ["issueID"] }, (c) => c.orderBy("id", "asc"))
    .orderBy("id", "asc");

// Under test: the full local Store, query materialized BEFORE any data (empty hydrate).
const store = await createWasmStore(schema);
const view = buildQuery(store.query).materialize();

// Ground truth: RindleView (materialize-in-wasm), same AST, driven incrementally.
const ast = buildQuery(queries(schema)).ast();
const rindleSchemas = {
  issue: { columns: ["id", "title"], primaryKey: [0], relationships: [{ name: "comments", schema: { columns: ["id", "issueID", "body"], primaryKey: [0] } }] },
  comment: { columns: ["id", "issueID", "body"], primaryKey: [0] },
};
const rindle = RindleView.build(ast, rindleSchemas, {});

const cols = { issue: ["id", "title"], comment: ["id", "issueID", "body"] };
const arr = (t, o) => cols[t].map((c) => o[c]);

let n = 0;
async function step(label, ops) {
  await store.write((tx) => {
    for (const o of ops) {
      if (o.k === "add") tx.add(o.t, o.r);
      else if (o.k === "remove") tx.remove(o.t, o.r);
      else tx.edit(o.t, o.old, o.r);
    }
  });
  for (const o of ops) {
    if (o.k === "add") rindle.push(o.t, { type: "add", row: arr(o.t, o.r) });
    else if (o.k === "remove") rindle.push(o.t, { type: "remove", row: arr(o.t, o.r) });
    else rindle.push(o.t, { type: "edit", row: arr(o.t, o.r), old: arr(o.t, o.old) });
  }
  rindle.flush();
  assert.deepStrictEqual(view.data, rindle.data(), `step ${++n} (${label})`);
  console.log("✓", label);
}

assert.deepStrictEqual(view.data, rindle.data(), "empty hydrate");
console.log("✓ empty hydrate");

await step("seed issues + comments", [
  { k: "add", t: "issue", r: { id: 1, title: "first" } },
  { k: "add", t: "issue", r: { id: 2, title: "second" } },
  { k: "add", t: "comment", r: { id: 10, issueID: 1, body: "a" } },
  { k: "add", t: "comment", r: { id: 11, issueID: 1, body: "b" } },
  { k: "add", t: "comment", r: { id: 20, issueID: 2, body: "z" } },
]);
await step("child add", [{ k: "add", t: "comment", r: { id: 12, issueID: 1, body: "c" } }]);
await step("child edit (body)", [{ k: "edit", t: "comment", old: { id: 10, issueID: 1, body: "a" }, r: { id: 10, issueID: 1, body: "A" } }]);
await step("child remove", [{ k: "remove", t: "comment", r: { id: 11, issueID: 1, body: "b" } }]);
await step("root add", [{ k: "add", t: "issue", r: { id: 3, title: "third" } }]);
await step("mixed: child + root add", [
  { k: "add", t: "comment", r: { id: 30, issueID: 3, body: "n" } },
  { k: "add", t: "issue", r: { id: 4, title: "fourth" } },
]);
await step("root remove", [{ k: "remove", t: "issue", r: { id: 2, title: "second" } }]);

console.log("\n✅ packaged Store + WasmBackend reconstructs the wasm RindleView across hydrate + 7 write steps");
