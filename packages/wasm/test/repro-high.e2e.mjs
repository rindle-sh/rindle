// REPRODUCTIONS — adversarial-review HIGH findings in WasmBackend (packages/wasm/src/index.ts):
//
//   #15 — reentrant write from a view subscriber delivers sibling-query batches OUT of commit
//         order. While dispatching commit N's batches, a subscriber that synchronously calls
//         store.write() commits N+1 and dispatches it IMMEDIATELY — so a sibling query folds
//         N+1 before N and is left silently stale.
//   #11 — no exception isolation around the dispatch loop: one throwing view (or subscriber)
//         aborts delivery mid-flight, silently dropping sibling queries' batches.
//
// Both drive the REAL wasm engine through the public Store API and assert the CORRECT outcome,
// so each FAILS on HEAD and passes after the fix. Run after building the wasm artifact.

import assert from "node:assert/strict";

import { table, string, number, createSchema, createWasmStore, initWasm } from "@rindle/wasm";

await initWasm();

const issue = table("issue").columns({ id: number(), title: string(), score: number() }).primaryKey("id");
const schema = createSchema({ tables: [issue] });

// ====================================================================================
// HIGH #15 — reentrant write must not deliver sibling batches out of commit order.
// ====================================================================================
{
  const store = await createWasmStore(schema);
  const viewA = store.query.issue.orderBy("id", "asc").materialize();
  const viewB = store.query.issue.orderBy("id", "asc").materialize();

  await store.write((tx) => tx.add("issue", { id: 1, title: "a", score: 1 }));

  // A subscriber on A reacts to the trigger row (id 2) by synchronously editing issue 1's
  // score 2 → 3. On HEAD this inner commit dispatches reentrantly, reaching B before B has
  // received the OUTER commit's batch.
  let reacted = false;
  viewA.subscribe((data) => {
    if (!reacted && data.some((r) => r.id === 2)) {
      reacted = true;
      void store.write((tx) =>
        tx.edit("issue", { id: 1, title: "a", score: 2 }, { id: 1, title: "a", score: 3 }),
      );
    }
  });

  // Outer commit: edit issue 1 (1 → 2) AND add the trigger row 2.
  await store.write((tx) => {
    tx.edit("issue", { id: 1, title: "a", score: 1 }, { id: 1, title: "a", score: 2 });
    tx.add("issue", { id: 2, title: "trigger", score: 0 });
  });

  // Engine head is issue 1 @ score 3. Both views must agree with head; on HEAD viewB is left
  // at 2 (the outer batch overwrote the reentrant inner batch out of order).
  const aScore = viewA.data.find((r) => r.id === 1).score;
  const bScore = viewB.data.find((r) => r.id === 1).score;
  assert.equal(aScore, 3, "view A reflects the engine head");
  assert.equal(bScore, 3, "view B must not be left stale at 2 (reentrant out-of-order delivery)");
  console.log("✓ HIGH#15: reentrant write delivers sibling batches in commit order");
}

// ====================================================================================
// HIGH #11 — one throwing view/subscriber must not drop a sibling query's batch.
// ====================================================================================
{
  const store = await createWasmStore(schema);
  const viewA = store.query.issue.where.id(1).materialize();
  const viewB = store.query.issue.orderBy("id", "asc").materialize();

  await store.write((tx) => tx.add("issue", { id: 1, title: "a", score: 0 }));

  // Arm a throwing subscriber on A (skip the immediate fire-on-subscribe).
  let armed = false;
  viewA.subscribe(() => {
    if (armed) throw new Error("boom in A's subscriber");
  });
  armed = true;

  // A write that touches BOTH queries. On HEAD A's throw aborts the dispatch loop, so B's
  // batch is dropped and B stays stale. After the fix B is delivered regardless (the error is
  // isolated, then re-raised once every query has been delivered).
  let caught;
  try {
    await store.write((tx) =>
      tx.edit("issue", { id: 1, title: "a", score: 0 }, { id: 1, title: "a", score: 9 }),
    );
  } catch (e) {
    caught = e;
  }

  // The discriminator: B got its batch even though A's subscriber threw.
  assert.equal(viewB.data.find((r) => r.id === 1).score, 9, "sibling view B still received its batch");
  assert.ok(caught, "the subscriber error still surfaces (not silently swallowed)");
  console.log("✓ HIGH#11: a throwing view does not drop sibling queries' batches");
}

console.log("✅ wasm repro-high: reentrancy ordering (#15) + dispatch isolation (#11)");
