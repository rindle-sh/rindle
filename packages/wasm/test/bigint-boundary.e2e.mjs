// Design 226: a BigInt cell crossing INTO the wasm engine is a typed
// `unsupported_value` error — never the silent NULL it became before (the
// `JSON.stringify`-throws → `Err(_) => Null` swallow at `wasm/marshal.rs`). The
// browser IVM has no int64 column type until Stage E, so accepting a BigInt could
// only round or corrupt. The outbound `strict_i64` check (productionization 09.8)
// is MANDATORY since Stage C (§8) — always on, no toggle — and must not disturb
// normal safe writes/reads.
import assert from "node:assert/strict";

import { initWasm } from "@rindle/wasm";
import { Db } from "../pkg/rindle.js";

await initWasm();

const db = new Db();
db.registerTable("t", { columns: ["id", "n"], primaryKey: [0] }, false);

// A BigInt cell in a write is refused with a typed error (was: silently NULL).
{
  const tx = db.write();
  assert.throws(
    () => tx.add("t", [1, 9007199254740993n]),
    (e) => e.kind === "unsupported_value" && /BigInt/.test(e.message),
    "a BigInt cell must be a typed unsupported_value error",
  );
  tx.rollback();
  console.log("✓ BigInt cell → typed error (kind=unsupported_value), not NULL");
}

// A symbol-ish unstringifiable value is the same typed error, not a silent NULL.
{
  const tx = db.write();
  const cyclic = {};
  cyclic.self = cyclic;
  assert.throws(
    () => tx.add("t", [2, cyclic]),
    (e) => e.kind === "unsupported_value",
    "an unstringifiable cell must be a typed error",
  );
  tx.rollback();
  console.log("✓ cyclic/unstringifiable cell → typed error, not NULL");
}

// The mandatory strict_i64 check does not disturb normal safe writes/reads.
{
  const tx = db.write();
  tx.add("t", [3, 42]);
  const out = tx.commit();
  assert.ok(Array.isArray(out), "safe writes commit under the mandatory check");
  console.log("✓ strict_i64 mandatory; safe values unaffected");
}

console.log("bigint-boundary e2e: all green");
