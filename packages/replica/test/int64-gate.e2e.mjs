// Design 226 Stage C4: BIGINT/INT8 columns exist — the §8 gate pins at all three
// IVM entries over a REAL int64 column, while the SQL plane reads/writes the
// exact values, and the NORMALIZED wire's project-at-emit keeps an admissible
// projected query deliverable even when the int64 column holds unsafe values.
import assert from "node:assert/strict";

import { Db } from "../index.js";

const UNSAFE = "9007199254740993"; // 2^53 + 1

const db = new Db();
db.registerTable("ledger", ["id", "ref", "amount"], [0], ["number", "string", "int64"]);
db.registerTable("plain", ["id", "name"], [0], ["number", "string"]);
db.enableClientMutations();

// Entry 1 — a one-shot (flat) query whose footprint includes the int64 column:
// refused at build with the typed §8 error naming the column.
assert.throws(
  () => db.query(1, JSON.stringify({ table: "ledger" })),
  (e) => e.message.includes("int64 column") && e.message.includes("amount"),
  "one-shot full-row query over an int64 table must be refused",
);
console.log("✓ entry 1 (one-shot): refused, names ledger.amount");

// Entry 2 — sync (normalized) registration: same refusal, same chokepoint.
assert.throws(
  () => db.queryNormalized(2, JSON.stringify({ table: "ledger" }), 1),
  (e) => e.message.includes("int64 column"),
  "normalized registration over an int64 footprint must be refused",
);
console.log("✓ entry 2 (normalized): refused");

// An unrelated table and a projection excluding the column stay admissible.
db.query(3, JSON.stringify({ table: "plain" }));
const projected = db.queryNormalized(
  5,
  JSON.stringify({ table: "ledger", select: ["id", "ref"] }),
  1,
);
assert.equal(projected.queryId, 5);
console.log("✓ admissible: int64-free table + projected ledger query");

// Entry 3 — a mutator transaction IS the SQL plane: it writes and reads the
// exact plane. The full-range value inserts exactly (an exact-value predicate
// matches it), and the cell itself refuses to cross to JS (mandatory
// strict_i64) rather than rounding.
{
  const txn = db.beginMutation();
  txn.exec(`INSERT INTO ledger VALUES (1, 'a', ${UNSAFE})`, []);
  const byExact = txn.queryRows(`SELECT ref FROM ledger WHERE amount = ${UNSAFE}`, []);
  assert.deepStrictEqual(byExact, [["a"]], "the exact value round-tripped in SQL");
  const byNeighbor = txn.queryRows(
    "SELECT ref FROM ledger WHERE amount = 9007199254740992",
    [],
  );
  assert.deepStrictEqual(byNeighbor, [], "no f64 collapse: the neighbor does not match");
  assert.throws(
    () => txn.queryRows("SELECT amount FROM ledger", []),
    (e) => e.message.includes("strict_i64"),
    "the unsafe cell must not cross to JS as a rounded number",
  );
  // The committed write derives the projected query's batch: project-at-emit
  // drops the int64 cell, so the delivery neither errors nor leaks the value.
  const out = txn.commitWithLmid("c1", 1);
  const batch = out.batches.find((b) => b.queryId === 5);
  assert.ok(batch, "the projected query saw the commit");
  const text = JSON.stringify(batch);
  assert.ok(!text.includes("9007199254740"), `no unsafe/rounded cell on the wire: ${text}`);
  console.log("✓ entry 3 (mutator SQL plane): exact write/predicate; strict cell refusal;");
  console.log("✓ projected normalized delivery: project-at-emit excludes the int64 cell");
}

console.log("int64-gate e2e: all green");
