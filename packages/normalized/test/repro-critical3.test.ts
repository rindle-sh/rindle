// REPRODUCTION — Adversarial-review CRITICAL #3
// "NormalizedSync.add silently overwrites the shared-row mirror without forwarding an
//  edit — net server edit suppressed, permanent silent divergence." (sync.ts:157)
//
// These tests assert the CORRECT (oracle) behaviour, so they FAIL on HEAD exactly where
// the finding predicts. Each models the documented scenario at the pure-NormalizedSync
// level (no wasm/ws needed): one server commit edits a row X (v1→v2) such that X ENTERS
// query B's results while query A already references X. The server emits `edit old=v1
// new=v2` on A's stream and `add [v2]` on B's stream for the SAME commit. If B's add is
// delivered first, the edit is suppressed and the local Db never moves to v2.

import { test } from "node:test";
import assert from "node:assert/strict";

import type { Mutation } from "@rindle/client";
import { NormalizedSync, type NormalizedOp } from "../src/sync.ts";

const issuePk = { issue: [0] };

/** A tiny stand-in for the local wasm Db: apply the NET mutations NormalizedSync forwards
 *  and read back the row a query's view would re-evaluate against. Keyed by pk (col 0). */
class FakeDb {
  private rows = new Map<unknown, unknown[]>();
  apply(muts: Mutation[]): void {
    for (const m of muts) {
      if (m.op === "add") this.rows.set(m.row[0], m.row);
      else if (m.op === "remove") this.rows.delete(m.row[0]);
      else this.rows.set(m.new[0], m.new); // edit: pk stable
    }
  }
  get(pk: unknown): unknown[] | undefined {
    return this.rows.get(pk);
  }
}

test("CRIT#3: shared-row add carrying a NEW value forwards an edit (mirror == Db)", () => {
  const sync = new NormalizedSync(issuePk);
  const db = new FakeDb();

  // Query A (all issues) hydrates with issue 1 at value "y".
  db.apply(sync.applyBatch(2, [{ table: "issue", op: "add", row: [1, "y"] }]));
  assert.deepStrictEqual(db.get(1), [1, "y"], "precondition: Db holds the old value");

  // One server commit: issue 1 ENTERS query B (title === "x") and is edited on A's stream
  // y → x. The two per-query frames of the SAME commit, delivered B-add FIRST:
  const mB: NormalizedOp[] = [{ table: "issue", op: "add", row: [1, "x"] }];
  const mA: NormalizedOp[] = [{ table: "issue", op: "edit", old: [1, "y"], new: [1, "x"] }];
  db.apply(sync.applyBatch(1, mB)); // B gains the row at its new value
  db.apply(sync.applyBatch(2, mA)); // A's edit to the same value

  // ORACLE: both queries (and the Db) must now hold issue 1 at "x".
  // HEAD: add() does `e.row = row` with no forwarded edit, then edit()'s rowEq-dedup
  //       swallows A's edit → the Db is never updated and stays at "y".
  assert.deepStrictEqual(db.get(1), [1, "x"], "the net server edit must reach the Db");
});

test("CRIT#3: re-hydrate of a shared row with a gap-changed value is forwarded", () => {
  // The same omission is reused by rehydrate()'s `add` path (sync.ts:107). A shared row
  // whose value changed during a gap must still reconcile the Db.
  const sync = new NormalizedSync(issuePk);
  const db = new FakeDb();

  db.apply(sync.applyBatch(1, [{ table: "issue", op: "add", row: [1, "y"] }])); // q1 holds "y"
  // q2 re-hydrates and footprints issue 1, but the server's snapshot value is "x":
  db.apply(sync.rehydrate(2, [{ table: "issue", op: "add", row: [1, "x"] }]));

  // ORACLE: the gap-changed value reconciles into the Db.
  assert.deepStrictEqual(db.get(1), [1, "x"], "re-hydrate must reconcile the gap-changed value");
});
