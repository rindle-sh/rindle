// The `strict_i64` boundary check (productionization 09.8) on the napi surface,
// MANDATORY since design 226 Stage C (§8). SQL text can express any i64, so a
// mutator's `queryRows` is the one place an out-of-safe-range INTEGER reaches JS
// today — the exact silent-rounding leak the check closes. An unsafe INTEGER
// crossing to JS is a typed error naming the value — never a rounded number.
// (Stage E adds the bigint lane and makes the check column-aware.)
import assert from "node:assert/strict";

import { Db } from "../index.js";

const UNSAFE = "9007199254740993"; // 2^53 + 1 — not expressible as a JS number

// The default (and only) behavior: an unsafe INTEGER is a typed error.
{
  const db = new Db();
  const txn = db.beginMutation();
  assert.throws(
    () => txn.queryRows(`SELECT ${UNSAFE}`, []),
    (e) => e.message.includes(UNSAFE) && e.message.includes("strict_i64"),
    "queryRows must throw a typed error naming the value",
  );
  // The same handle keeps working for safe values — the error is per-call, not poison.
  const ok = txn.queryRows("SELECT 42", []);
  assert.deepStrictEqual(ok, [[42]], "safe values still read after a refusal");
  txn.rollback();
  console.log("✓ mandatory: 2^53+1 is a typed error; safe values unaffected");
}

// Boundary value: MAX_SAFE_INTEGER itself is fine; one past it is not.
{
  const db = new Db();
  const txn = db.beginMutation();
  const max = txn.queryRows("SELECT 9007199254740991", []);
  assert.deepStrictEqual(max, [[Number.MAX_SAFE_INTEGER]], "MAX_SAFE_INTEGER passes");
  assert.throws(() => txn.queryRows("SELECT 9007199254740992", []));
  assert.throws(() => txn.queryRows("SELECT -9007199254740992", []));
  txn.rollback();
  console.log("✓ the bound is exactly Number.MAX_SAFE_INTEGER");
}

console.log("strict-i64 e2e: all green");
