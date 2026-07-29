// e2e for the publish-polish builder features against the REAL wasm engine:
//   - top-level `.one()` → a SingularArrayView (`data` is the row or null, not an array)
//   - `.start(cursor, { exclusive })` cursor paging → lowers to a `Skip` in the engine
// Run after building the wasm artifact (packages/wasm/build.sh).
import assert from "node:assert/strict";

import { table, string, number, createSchema, createWasmStore, initWasm } from "@rindle/wasm";

await initWasm();

const issue = table("issue").columns({ id: number(), title: string() }).primaryKey("id");
const schema = createSchema({ tables: [issue] });
const store = await createWasmStore(schema);

// Seed five rows (ids 1..5).
await store.write((tx) => {
  for (let i = 1; i <= 5; i++) tx.add("issue", { id: i, title: `t${i}` });
});

// ----------------------------- top-level .one() -----------------------------
const one = store.query.issue.orderBy("id", "asc").one().materialize();
assert.deepStrictEqual(one.data, { id: 1, title: "t1" }, ".one() yields the single first row as an object");

const none = store.query.issue.where.id(999).one().materialize();
assert.strictEqual(none.data, null, ".one() with no match yields null");
console.log("✓ .one() unwraps to the row (or null)");

// .one() stays live: editing the top row in place updates the singular result …
await store.write((tx) => tx.edit("issue", { id: 1, title: "t1" }, { id: 1, title: "T1" }));
assert.deepStrictEqual(one.data, { id: 1, title: "T1" }, ".one() reflects an in-place edit");

// … and removing the top row backfills the next one (limit-1 Take maintenance).
await store.write((tx) => tx.remove("issue", { id: 1, title: "T1" }));
assert.deepStrictEqual(one.data, { id: 2, title: "t2" }, ".one() backfills after the top row is removed");
console.log("✓ .one() is live (edit + backfill)");

// ----------------------------- .start() paging -----------------------------
// Rows now: ids 2,3,4,5.
const after2 = store.query.issue.orderBy("id", "asc").start({ id: 2 }, { exclusive: true }).materialize();
assert.deepStrictEqual(
  after2.data.map((r) => r.id),
  [3, 4, 5],
  "exclusive start drops the cursor row (> 2)",
);

const from2 = store.query.issue.orderBy("id", "asc").start({ id: 2 }).materialize();
assert.deepStrictEqual(
  from2.data.map((r) => r.id),
  [2, 3, 4, 5],
  "inclusive start keeps the cursor row (>= 2)",
);
console.log("✓ .start() pages inclusively / exclusively");

// .start() stays live: a new row past the bound appears; one before it does not.
await store.write((tx) => {
  tx.add("issue", { id: 6, title: "t6" }); // > 2 → enters the page
  tx.add("issue", { id: 1, title: "t1" }); // <= 2 → stays out (id 1 was removed earlier)
});
assert.deepStrictEqual(
  after2.data.map((r) => r.id),
  [3, 4, 5, 6],
  "exclusive start admits a new row past the bound, ignores one before it",
);
assert.deepStrictEqual(
  from2.data.map((r) => r.id),
  [2, 3, 4, 5, 6],
  "inclusive start tracks the same new row",
);
console.log("✓ .start() pages are live");

console.log("\n✅ .one() unwrap + .start() cursor paging verified against the wasm engine");

