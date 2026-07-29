// REPRODUCTION — Adversarial-review CRITICAL #2
// "unregisterQuery GC removals bypass the sync baseline — rewind resurrects rows; deleted/
//  stale server rows are served to later queries forever." (backend.ts:141)
//
// OptimisticBackend.unregisterQuery applies NormalizedSync.dropQuery's GC removals via
// `this.local.mutate(muts)` — an ordinary HEAD write — while the engine's authoritative
// `sync` baselines only move via serverBatchBegin. So the GC remove looks like a pending
// optimistic remove: the NEXT release's rewind (diff head vs sync+D) RE-ADDS the GC'd rows.
//
// This drives the REAL wasm engine (createOptimisticStore's backend) through a TestSource,
// and asserts the CORRECT outcome — so it FAILS on HEAD where the finding predicts.

import { test } from "node:test";
import assert from "node:assert/strict";

import {
  createSchema,
  initWasm,
  number,
  queries,
  string,
  table,
  type ChangeEvent,
  type MutationEnvelope,
  type NormalizedEvent,
  type NormalizedOp,
  type OptimisticSource,
  type ProgressFrame,
  type QueryId,
  type RemoteQuery,
} from "@rindle/wasm";
import { OptimisticBackend } from "../src/index.ts";

await initWasm();

const issue = table("issue").columns({ id: number(), title: string() }).primaryKey("id");
const schema = createSchema({ tables: [issue] });
const allIssues = queries(schema).issue.ast();
const whereId1 = queries(schema).issue.where.id(1).ast();

// A hand-scripted server (same shape as backend.test.ts's TestSource).
class TestSource implements OptimisticSource {
  normalized: (qid: QueryId, ev: NormalizedEvent) => void = () => {};
  progress: (frame: ProgressFrame) => void = () => {};
  registerQuery(_qid: QueryId, _remote: RemoteQuery): void {}
  unregisterQuery(): void {}
  pushMutation(_env: MutationEnvelope): Promise<void> {
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
  frame(cvMin: number): void {
    this.progress({ cvMin });
  }
}

/** Pull the row ids out of a query's local snapshot ChangeEvent. */
function snapshotIds(events: Array<{ qid: QueryId; ev: ChangeEvent }>, qid: QueryId): number[] {
  const snap = events.find((c) => c.qid === qid && c.ev.type === "snapshot");
  assert.ok(snap, `expected a snapshot event for qid ${qid}`);
  const adds = (snap.ev as { adds: Array<{ op: { tag: string; node?: { row: unknown[] } } }> }).adds;
  return adds
    .filter((c) => c.op.tag === "add")
    .map((c) => c.op.node!.row[0] as number)
    .sort((a, b) => a - b);
}

test("CRIT#2: a row GC'd by unregisterQuery is not resurrected and not served to later queries", () => {
  const source = new TestSource();
  const backend = new OptimisticBackend(schema, source, {}, { clientID: "c1" });
  const events: Array<{ qid: QueryId; ev: ChangeEvent }> = [];
  backend.onEvent((qid, ev) => events.push({ qid, ev }));

  // q1 = where id=1 (footprints row 1); q2 = all issues (footprints rows 1 AND 2).
  backend.registerQuery(1, whereId1, { name: "whereId1", args: null });
  backend.registerQuery(2, allIssues, { name: "allIssues", args: null });
  source.snapshot(1, [{ table: "issue", op: "add", row: [1, "one"] }], 1);
  source.snapshot(
    2,
    [
      { table: "issue", op: "add", row: [1, "one"] },
      { table: "issue", op: "add", row: [2, "two"] },
    ],
    1,
  );
  source.frame(1); // coherent release: head = sync = {row1, row2}

  // Drop q2. Its sole-referenced row 2 hits refcount 0 → NormalizedSync GCs it via
  // local.mutate (a HEAD-only write). The engine's `sync` baseline still holds row 2.
  backend.unregisterQuery(2);

  // A foreign edit of row 1 pokes a reconcile cycle. The rewind diffs head (no row 2)
  // against sync+D (still has row 2) → re-ADDS row 2 to head. Row 2 is resurrected.
  source.batch(1, [{ table: "issue", op: "edit", old: [1, "one"], new: [1, "ONE"] }], 2);
  source.frame(2);

  // A later query registers and is served the local engine snapshot.
  events.length = 0;
  backend.registerQuery(3, allIssues, { name: "allIssues", args: null });

  // ORACLE: row 2 was GC'd and the server no longer footprints it for anyone — q3 must see
  // only issue 1. On HEAD the rewind resurrected row 2, so q3's snapshot contains [1, 2].
  assert.deepStrictEqual(
    snapshotIds(events, 3),
    [1],
    "the GC'd row was resurrected by the rewind and served to a later query",
  );

  // And the server cannot heal it: NormalizedSync believes row 2 is absent, so even q3's
  // authoritative server snapshot (row 1 only) emits no remove — row 2 is permanent.
  events.length = 0;
  source.snapshot(3, [{ table: "issue", op: "add", row: [1, "ONE"] }], 3);
  source.frame(3);
  // q3 has no further snapshot here; assert the divergence persists via a fresh registration.
  events.length = 0;
  backend.registerQuery(4, allIssues, { name: "allIssues", args: null });
  assert.deepStrictEqual(
    snapshotIds(events, 4),
    [1],
    "row 2 remained in the local engine after the server's authoritative snapshot",
  );
});
