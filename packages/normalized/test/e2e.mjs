// Slice 5 live e2e: a NORMALIZED local-first Store reconstructs EXACTLY the same result as a
// flat Store, across hydrate + writes.
//
// Under test: `createNormalizedStore(schema, source)` — a Store whose base tables are fed by the
// server stand-in's NORMALIZED footprint stream (the native `@rindle/replica` engine's `NormalizeFold`,
// Slices 1-3), folded by `NormalizedSync` (Slice 4) into the local `@rindle/wasm` engine, which
// materializes the result via the same `FlatArrayView`.
//
// Oracle: a plain flat `createWasmStore(schema)` over the SAME query + the SAME writes. For a
// materialized-`related` query the normalized footprint is exactly the base rows the result
// needs, so the two must agree byte-for-byte.
//
// Run after building the wasm artifact (packages/wasm/build.sh) and the native addon
// (pnpm --filter @rindle/replica build:native).
import assert from "node:assert/strict";

import {
  defineQuery,
  table,
  string,
  number,
  createSchema,
  createWasmStore,
  initWasm,
  newQueryBuilder,
  tableSpec,
} from "@rindle/wasm";
import { createNormalizedStore } from "@rindle/normalized";
import { NativeReplicaDb } from "@rindle/replica";

await initWasm();

const issue = table("issue").columns({ id: number(), title: string() }).primaryKey("id");
const comment = table("comment").columns({ id: number(), issueID: number(), body: string() }).primaryKey("id");
const schema = createSchema({ tables: [issue, comment] });

// The server stand-in for the normalized path: the native replica engine exposing per-query
// normalized footprint streams (what a real server emits). Implements the `NormalizedSource` seam.
class NativeNormalizedSource {
  constructor(schema, queries) {
    this.db = new NativeReplicaDb();
    this.queries = queries;
    this.handler = () => {};
    for (const name of Object.keys(schema.tables)) {
      const meta = schema.tables[name];
      const { columns, primaryKey } = tableSpec(meta);
      const types = columns.map((c) => meta.columns[c].type);
      this.db.registerTable(name, columns, primaryKey, types);
    }
  }
  registerQuery(qid, remote) {
    const query = this.queries[remote.name];
    if (!query) throw new Error(`unknown query: ${remote.name}`);
    const ast = query(remote.args).ast();
    const r = this.db.queryNormalized(qid, JSON.stringify(ast), 1); // in-process: epoch fixed at 1
    this.handler(qid, {
      type: "hello",
      tables: r.hello.tables,
      comparatorVersion: r.hello.comparatorVersion,
      normalizedFp: r.hello.normalizedFp,
    });
    this.handler(qid, { type: "snapshot", ops: r.snapshot.ops });
  }
  unregisterQuery(qid) {
    this.db.destroyQuery(qid);
  }
  mutate(mutations) {
    for (const b of this.db.commitNormalized(mutations)) {
      this.handler(b.queryId, { type: "batch", ops: b.batch.ops });
    }
    return Promise.resolve();
  }
  onNormalized(h) {
    this.handler = h;
  }
}

const buildQuery = (root) =>
  root.issue
    .sub("comments", comment, { parent: ["id"], child: ["issueID"] }, (c) => c.orderBy("id", "asc"))
    .orderBy("id", "asc");

// A relationship AGGREGATE (AGGREGATE-SYNC-DESIGN.md §3): the normalized store CANNOT
// recompute `commentCount` (it never receives comment rows) — it displays the server's count,
// shipped as a synthetic base table. The flat oracle store reduces its local comment rows. The
// two must still agree, which is the whole point of the synthetic-table display path.
const buildCountQuery = (root) =>
  root.issue.countAs("commentCount", comment, { parent: ["id"], child: ["issueID"] }).orderBy("id", "asc");

const clientQ = newQueryBuilder(schema);
const issueQueries = {
  issuesWithComments: defineQuery("issuesWithComments", () => buildQuery(clientQ)),
  issuesWithCount: defineQuery("issuesWithCount", () => buildCountQuery(clientQ)),
};
const serverQ = newQueryBuilder(schema);
const serverQueries = {
  issuesWithComments: () => buildQuery(serverQ),
  issuesWithCount: () => buildCountQuery(serverQ),
};

// Under test: normalized local-first Store over the native server's normalized stream.
const norm = createNormalizedStore(schema, new NativeNormalizedSource(schema, serverQueries));
const normView = norm.materialize(issueQueries.issuesWithComments());
const normCountView = norm.materialize(issueQueries.issuesWithCount());

// Oracle: a plain flat local Store.
const oracle = await createWasmStore(schema);
const oracleView = buildQuery(oracle.query).materialize();
const oracleCountView = buildCountQuery(oracle.query).materialize();

let n = 0;
async function step(label, ops) {
  // Drive BOTH stores with the same object-row writes.
  for (const store of [norm, oracle]) {
    await store.write((tx) => {
      for (const o of ops) {
        if (o.k === "add") tx.add(o.t, o.r);
        else if (o.k === "remove") tx.remove(o.t, o.r);
        else tx.edit(o.t, o.old, o.r);
      }
    });
  }
  assert.deepStrictEqual(normView.data, oracleView.data, `step ${++n} (${label})`);
  // The synthetic-table count display must equal the flat store's reduced count, step for step.
  assert.deepStrictEqual(normCountView.data, oracleCountView.data, `step ${n} count (${label})`);
  console.log("✓", label);
}

assert.deepStrictEqual(normView.data, oracleView.data, "empty hydrate");
assert.deepStrictEqual(normView.data, [], "empty hydrate is []");
assert.deepStrictEqual(normCountView.data, oracleCountView.data, "empty hydrate count");
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
await step("root remove (with its comments)", [{ k: "remove", t: "issue", r: { id: 2, title: "second" } }]);

// Sanity: the final shape is what we expect (issue 1 with comments 10,12; 3 with 30; 4 with none).
assert.deepStrictEqual(
  normView.data.map((i) => [i.id, i.comments.map((c) => c.id)]),
  [
    [1, [10, 12]],
    [3, [30]],
    [4, []],
  ],
);

// The server-computed counts the normalized store DISPLAYS (it never held the comments):
// issue 1 → 2 comments (10,12), issue 3 → 1 (30), issue 4 → 0 (childless → identity 0).
assert.deepStrictEqual(
  normCountView.data.map((i) => [i.id, i.commentCount]),
  [
    [1, 2],
    [3, 1],
    [4, 0],
  ],
);

console.log("\n✅ @rindle/normalized: NormalizedStore (server normalized stream → NormalizedSync → wasm) == flat Store over hydrate + 7 steps");
console.log("✅ @rindle/normalized: a relationship count is DISPLAYED from the server's synthetic table (never recomputed)");
