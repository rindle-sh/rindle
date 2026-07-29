// REPRODUCTIONS — adversarial-review HIGH findings in OptimisticBackend (backend.ts):
//
//   #7  — a pending mutator that THROWS during re-invocation leaves the server-batch cycle
//         permanently open (serverBatchEnd never called) → every view freezes, every later
//         release fails. (runReconcileCycle had no try/finally.)
//   #10 — a mutator that throws on its FIRST invoke burns a mid (allocated before the write),
//         so every later mutation is refused by the server's gap check (permanent silent
//         write outage).
//   #16 — a defensive re-invocation that no-ops overwrites the mutation's footprint with {},
//         so a still-pending mutation wrongly reports 'complete' (its lifecycle signal is
//         masked while the server hasn't processed it yet).
//
// These drive the REAL wasm engine through a hand-scripted OptimisticSource and assert the
// CORRECT outcome — so each FAILS on HEAD where the finding predicts, and passes after the fix.

import { test } from "node:test";
import assert from "node:assert/strict";

import {
  createSchema,
  initWasm,
  number,
  string,
  table,
  type MutationEnvelope,
  type NormalizedEvent,
  type NormalizedOp,
  type OptimisticSource,
  type ProgressFrame,
  type QueryId,
} from "@rindle/wasm";
import { createOptimisticStore, type ClientRegistry, type MutationTx } from "../src/index.ts";

await initWasm();

const issue = table("issue").columns({ id: number(), title: string(), score: number() }).primaryKey("id");
const schema = createSchema({ tables: [issue] });

class TestSource implements OptimisticSource {
  normalized: (qid: QueryId, ev: NormalizedEvent) => void = () => {};
  progress: (frame: ProgressFrame) => void = () => {};
  envelopes: MutationEnvelope[] = [];
  registerQuery(): void {}
  unregisterQuery(): void {}
  pushMutation(env: MutationEnvelope): Promise<void> {
    this.envelopes.push(env);
    return Promise.resolve();
  }
  onNormalized(h: (qid: QueryId, ev: NormalizedEvent) => void): void {
    this.normalized = h;
  }
  onProgress(h: (frame: ProgressFrame) => void): void {
    this.progress = h;
  }
  snapshot(qid: QueryId, ops: NormalizedOp[], cv: number): void {
    this.normalized(qid, { type: "snapshot", ops, cv });
  }
  batch(qid: QueryId, ops: NormalizedOp[], cv: number): void {
    this.normalized(qid, { type: "batch", ops, cv });
  }
  /** The lmid system query's data frame (qid 0): the server confirming `mid` at `cv`. */
  lmid(mid: number, cv: number): void {
    this.normalized(0, {
      type: "batch",
      ops: [{ table: "_rindle_client_mutations", op: "add", row: ["c1", mid] }],
      cv,
    });
  }
  frame(cvMin: number): void {
    this.progress({ cvMin });
  }
}

// ====================================================================================
// HIGH #10 — a throwing FIRST invoke must not burn a mid.
// ====================================================================================

test("HIGH#10: a mutator that throws on first invoke does not burn a mid (no server gap)", () => {
  const source = new TestSource();
  const registry = {
    bad: (_tx: MutationTx, _args: Record<string, never>) => {
      throw new Error("client-side validation failed");
    },
    good: (tx: MutationTx, args: { id: number }) => tx.add("issue", [args.id, "ok", 0]),
  } satisfies ClientRegistry;
  const { store, mutate } = createOptimisticStore(schema, source, registry, { clientID: "c1" });
  store.query.issue.materialize();
  source.snapshot(1, [], 1);
  source.frame(1);

  // First invoke throws (ordinary client-side validation) — nothing should be pushed/sent.
  assert.throws(() => mutate.bad({}), /validation/);

  // The next successful invoke must ship mid 1 (contiguous). On HEAD it ships mid 2, which the
  // server's gap check (mid !== stored + 1) refuses, silently dropping every later write.
  const mid = mutate.good({ id: 5 });
  assert.equal(mid, 1, "a throwing mutator must not consume a mid");
  assert.deepEqual(
    source.envelopes.map((e) => e.mid),
    [1],
    "the shipped envelope carries the contiguous mid 1",
  );
});

// ====================================================================================
// HIGH #7 — a re-invocation throw must close the cycle (not wedge the engine forever).
// ====================================================================================

test("HIGH#7: a re-invocation throw drops the mutation and closes the cycle (engine not wedged)", () => {
  const source = new TestSource();
  const registry = {
    // A read-dependent mutator that ASSERTS its base row exists (throws if not) — the canonical
    // conflict: a foreign delete removes the row the pending mutator reads.
    bumpStrict: (tx: MutationTx, args: { id: number }) => {
      const cur = tx.get("issue", [args.id]);
      if (!cur) throw new Error("base row vanished");
      tx.edit("issue", cur, [cur[0], cur[1], (cur[2] as number) + 1]);
    },
    createIssue: (tx: MutationTx, args: { id: number }) => tx.add("issue", [args.id, "ok", 0]),
  } satisfies ClientRegistry;
  const { store, backend, mutate } = createOptimisticStore(schema, source, registry, { clientID: "c1" });
  const view = store.query.issue.materialize();
  source.snapshot(1, [{ table: "issue", op: "add", row: [1, "a", 10] }], 1);
  source.frame(1);

  mutate.bumpStrict({ id: 1 }); // pending; predicts score 11
  assert.equal(view.data[0].score, 11);

  // A FOREIGN delete removes the base row (confirms nothing: lmid stays 0). The release
  // re-invokes bumpStrict, whose get() now returns undefined → it THROWS inside the cycle.
  // On HEAD the throw escapes onProgress with the cycle still open → this call throws and the
  // engine is wedged. After the fix the cycle always closes and the mutation is dropped.
  source.batch(1, [{ table: "issue", op: "remove", row: [1, "a", 10] }], 2);
  source.frame(2);

  // The cycle closed: the view reflects the server state (row gone), and the failed mutation
  // is dropped (snap-back). §7.4: a throwing re-invocation no longer taints the query to 'error' —
  // that variant is reserved for a future server-side query-level signal. The drop surfaces on the
  // pending axis (the mutation is gone → not pending), not on ResultType.
  assert.deepEqual(view.data, [], "rewound to server state; cycle closed");
  assert.equal(backend.resultType(1), "complete", "the dropped re-invocation no longer surfaces as 'error' (§7.4)");
  assert.equal(backend.pending(1), false, "the throwing mutation was dropped → no longer pending");

  // The backend is still functional — a new mutation applies and ships its envelope.
  const mid = mutate.createIssue({ id: 9 });
  assert.deepEqual(view.data, [{ id: 9, title: "ok", score: 0 }], "engine not wedged");
  assert.ok(
    source.envelopes.some((e) => e.mid === mid),
    "the post-throw mutation's envelope still ships",
  );
});

// ====================================================================================
// HIGH #16 — a defensive re-invocation must not mask the pending lifecycle state.
// ====================================================================================

test("HIGH#16: a defensive (no-op) re-invocation does not mask a pending mutation's state", () => {
  const source = new TestSource();
  const registry = {
    createIssue: (tx: MutationTx, args: { id: number }) => tx.add("issue", [args.id, "t", 0]),
    // The shipped-e2e style: read the row, no-op if it's gone.
    editTitle: (tx: MutationTx, args: { id: number; title: string }) => {
      const cur = tx.get("issue", [args.id]);
      if (!cur) return; // defensive no-op → touched becomes {} on rebase
      tx.edit("issue", cur, [cur[0], args.title, cur[2]]);
    },
  } satisfies ClientRegistry;
  const { store, backend, mutate } = createOptimisticStore(schema, source, registry, { clientID: "c1" });
  store.query.issue.materialize();
  source.snapshot(1, [], 1);
  source.frame(1);

  // (1) create issue 9, confirmed (the lmid row rides the same commit, cv 2).
  mutate.createIssue({ id: 9 });
  source.batch(1, [{ table: "issue", op: "add", row: [9, "t", 0] }], 2);
  source.lmid(1, 2);
  source.frame(2);
  assert.equal(backend.pending(1), false, "confirmed create → nothing pending");

  // (2) pending editTitle(9) — touches the issue query → flips the PENDING axis (§7).
  mutate.editTitle({ id: 9, title: "new" });
  assert.equal(backend.pending(1), true, "pending edit touches the query");

  // (3) a FOREIGN delete of row 9 arrives (confirms nothing: no lmid frame). The release
  // re-invokes editTitle, whose get() returns undefined → it no-ops → touched = {}.
  source.batch(1, [{ table: "issue", op: "remove", row: [9, "t", 0] }], 3);
  source.frame(3);
  // (a) editTitle (mid 2) is STILL pending → the no-op re-invocation must NOT shrink the footprint
  // (the touched UNION is preserved across re-invocations, §7.2), else the axis clears too early.
  assert.equal(backend.pending(1), true, "still pending → the no-op re-invocation didn't shrink the footprint");
  assert.equal(backend.resultType(1), "complete", "ResultType is server-channel-only — unaffected (§7)");

  // (4) the server processes editTitle (mid 2) — as a no-op or not, the lmid-only
  // commit's lmid row (cv 4) is the whole signal. The release drops the pending.
  source.lmid(2, 4);
  source.frame(4);
  assert.equal(backend.pending(1), false, "processed → the pending axis clears");
});
