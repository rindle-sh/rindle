// Folded-mutation oracle (FOLDED-MUTATIONS-DESIGN §9): the fold logic lives entirely in the TS
// backend (the Rust engine sees only "one entry whose args changed"), so this is a TS test in the
// style of `backend.test.ts`. Timing is DETERMINISTIC — a virtual clock + scheduler is injected in
// place of `setTimeout`; nothing touches wall time.
//
// Two halves, only one a true oracle (§9):
//   - STATE ("does the view match reality?") — a true differential oracle: re-materialize the query
//     from scratch over `server_state ⊕ pending` (a plain-JS recompute, independent of fold logic)
//     and compare. Exercised by the rebase/confirm/fuzz slices.
//   - POLICY ("did the debounce produce the right wire trace?") — no independent ground truth, so we
//     assert TIMING-INDEPENDENT INVARIANTS (gaplessness, economy bound, last-value-wins, …) that any
//     correct fold must satisfy under EVERY schedule, plus a few hand-pinned example traces (§9.3).
//
// Requires the wasm artifact (packages/wasm/build.sh) — same as the other suites.

import { test } from "node:test";
import assert from "node:assert/strict";

import {
  createSchema,
  defineQuery,
  initWasm,
  newQueryBuilder,
  number,
  string,
  table,
  type MutationEnvelope,
  type NormalizedEvent,
  type NormalizedOp,
  type OptimisticSource,
  type ProgressFrame,
  type QueryId,
  type RemoteQuery,
} from "@rindle/wasm";
import { createOptimisticStore, type ClientRegistry, type FoldClock, type MutationTx } from "../src/index.ts";

await initWasm();

const issue = table("issue").columns({ id: number(), title: string(), score: number() }).primaryKey("id");
const comment = table("comment").columns({ id: number(), issueID: number(), body: string() }).primaryKey("id");
const schema = createSchema({ tables: [issue, comment] });

// --- a deterministic virtual clock + scheduler (§9) ---------------------------------------------
// Replaces `setTimeout`/`clearTimeout`/`Date.now`. `tick(dt)` advances virtual time, firing every
// timer whose due time falls within [now, now+dt] in (due, insertion) order — exactly the firing a
// real event loop would do over that wall-clock interval, but reproducible.
class VirtualClock implements FoldClock {
  private t = 0;
  private seq = 0;
  private timers: { id: number; due: number; seq: number; cb: () => void }[] = [];

  now(): number {
    return this.t;
  }
  setTimeout(cb: () => void, ms: number): number {
    const id = ++this.seq;
    this.timers.push({ id, due: this.t + ms, seq: id, cb });
    return id;
  }
  clearTimeout(handle: unknown): void {
    this.timers = this.timers.filter((x) => x.id !== handle);
  }
  /** Advance virtual time by `dt`, firing due timers in order. A fired timer that re-arms is handled
   *  by the loop (it re-enters `timers`); flushFold timers simply clear, so this terminates. */
  tick(dt: number): void {
    const target = this.t + dt;
    for (;;) {
      const due = this.timers
        .filter((x) => x.due <= target)
        .sort((a, b) => a.due - b.due || a.seq - b.seq);
      const next = due[0];
      if (!next) break;
      this.timers = this.timers.filter((x) => x.id !== next.id);
      this.t = Math.max(this.t, next.due);
      next.cb();
    }
    this.t = target;
  }
}

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
  onRestart(): void {}

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

const registry = {
  createIssue: (tx: MutationTx, a: { id: number; title: string; score: number }) =>
    tx.add("issue", [a.id, a.title, a.score]),
  // ABSORBING (foldable): sets one column, preserving the rest. `update` reads the current row
  // internally to preserve unspecified columns, but that is NOT a public `tx.get`/`tx.row`, so the
  // fold read-trap allows it (§5).
  setScore: (tx: MutationTx, a: { id: number; score: number }) => tx.update("issue", { id: a.id, score: a.score }),
  setTitle: (tx: MutationTx, a: { id: number; title: string }) => tx.update("issue", { id: a.id, title: a.title }),
  // A normal write to a DIFFERENT table — never overlaps an `issue` fold.
  addComment: (tx: MutationTx, a: { id: number; issueID: number; body: string }) =>
    tx.add("comment", [a.id, a.issueID, a.body]),
  // READ-DEPENDENT (NOT foldable): reads `score` (a public read) and writes a derived `title`. Used
  // both as the §4.2 read-dependent normal write and — when wrongly folded — the negative lane.
  deriveTitle: (tx: MutationTx, a: { id: number }) => {
    const r = tx.row("issue", { id: a.id });
    if (!r) return;
    tx.update("issue", { id: a.id, title: `s${r.score}` });
  },
  // The canonical non-absorbing shape (reads to compute its write).
  incrementScore: (tx: MutationTx, a: { id: number; delta: number }) => {
    const r = tx.row("issue", { id: a.id });
    if (!r) return;
    tx.update("issue", { id: a.id, score: (r.score as number) + a.delta });
  },
} satisfies ClientRegistry;

const qb = newQueryBuilder(schema);
const allIssues = defineQuery("allIssues", () => qb.issue.orderBy("id", "asc"));

function setup(clock: VirtualClock) {
  const source = new TestSource();
  const { store, backend, mutate } = createOptimisticStore(schema, source, registry, { clientID: "c1", clock });
  return { source, store, backend, mutate, clock };
}

const issueRows = (view: { data: readonly { id: number; title: string; score: number }[] }) =>
  view.data.map((r) => [r.id, r.title, r.score]);

// Wire-trace projections (the policy-invariant surface): mids in arrival order, (mid,name) and
// (mid,args) pairs, and args by position. (Helpers take `TestSource` explicitly so element types
// resolve regardless of how the destructured `source` is inferred at the call site.)
const wireMids = (s: TestSource): number[] => s.envelopes.map((e) => e.mid);
const wireNames = (s: TestSource): [number, string][] => s.envelopes.map((e) => [e.mid, e.name]);
const wireMidArgs = (s: TestSource): [number, unknown][] => s.envelopes.map((e) => [e.mid, e.args]);
const argsAt = (s: TestSource, i: number): unknown => s.envelopes.at(i)?.args;

// ================================================================================================
// Slice 0 — single-key drag, no rebase: gaplessness (#1), economy bound (#2), last-value-wins (#3),
// and the pinned-example trace.
// ================================================================================================

test("slice 0: a single-key drag folds to ONE debounced envelope carrying the last value", async () => {
  const clock = new VirtualClock();
  const { source, store, backend, mutate } = setup(clock);
  const view = store.materialize(allIssues());
  source.snapshot(1, [{ table: "issue", op: "add", row: [1, "x", 0] }], 1);
  source.frame(1);

  // 5 folded invokes for one key, each within the 100ms debounce of the prior.
  const scores = [10, 20, 30, 40, 50];
  let handle!: ReturnType<typeof mutate.setScore.folded>;
  scores.forEach((score, i) => {
    if (i > 0) clock.tick(30);
    handle = mutate.setScore.folded({ key: 1, debounceMs: 100 }, { id: 1, score });
    assert.equal(view.data[0].score, score, "LIVE local apply on every intermediate");
  });

  // Mid-drag: nothing on the wire, no mid burnt, the query is pending but NOT loading (§7).
  assert.deepEqual(source.envelopes, [], "no envelope while the debounce window is open");
  assert.equal(backend.pending(1), true, "the drag is pending");
  assert.equal(backend.resultType(1), "complete", "ResultType stays complete (server-channel-only, §7)");

  // Idle past the debounce → exactly one flush.
  clock.tick(100);
  // PINNED EXAMPLE TRACE (§9.3): one envelope, mid 1, the LAST value.
  assert.deepEqual(source.envelopes, [{ clientID: "c1", mid: 1, name: "setScore", args: { id: 1, score: 50 } }]);

  // #1 gaplessness: contiguous mids in arrival order.
  assert.deepEqual(wireMids(source), [1]);
  // #2 economy bound: 1 envelope ≤ 5 invokes.
  assert.ok(source.envelopes.length <= scores.length);
  // #3 last-value-wins on the wire.
  assert.deepEqual(argsAt(source, -1), { id: 1, score: 50 });
  // The handle's mid resolves with the assigned wire id (§3 — no mid until flush).
  assert.equal(await handle.mid, 1);
  assert.equal(view.data[0].score, 50);
});

test("slice 0: flush() forces the window now; flushFolds() drains everything outstanding", async () => {
  const clock = new VirtualClock();
  const { source, backend, mutate } = setup(clock);
  const h1 = mutate.setScore.folded({ key: 1, debounceMs: 1000 }, { id: 1, score: 7 });
  assert.deepEqual(source.envelopes, [], "long debounce → nothing yet");
  h1.flush();
  assert.deepEqual(wireMidArgs(source), [[1, { id: 1, score: 7 }]], "flush() shipped immediately");
  assert.equal(await h1.mid, 1);

  // Two more keys, drained together by flushFolds (the explicit / beforeunload path) in creation order.
  mutate.setScore.folded({ key: 2, debounceMs: 1000 }, { id: 2, score: 8 });
  mutate.setScore.folded({ key: 3, debounceMs: 1000 }, { id: 3, score: 9 });
  assert.equal(source.envelopes.length, 1, "the new keys are still deferred");
  backend.flushFolds();
  assert.deepEqual(wireMids(source), [1, 2, 3], "flushFolds drained in creation order, gapless");
});

// ================================================================================================
// Slice 1 — interleaved normal write (default mode): flush-on-enqueue (§4.2) keeps the wire gapless
// across the fold/normal interleave (#1) and yields NO cross-mutation snap (#6); ResultType stays
// complete and the pending axis tracks correctly (#9).
// ================================================================================================

test("slice 1: an overlapping normal write drains the fold first — no cross-mutation snap (default)", () => {
  const clock = new VirtualClock();
  const { source, store, backend, mutate } = setup(clock);
  const view = store.materialize(allIssues());
  source.snapshot(1, [{ table: "issue", op: "add", row: [1, "x", 10] }], 1);
  source.frame(1);

  // Drag the score to 50 (deferred — debounce still open).
  mutate.setScore.folded({ key: 1, debounceMs: 100 }, { id: 1, score: 50 });
  assert.equal(view.data[0].score, 50, "live fold apply");
  assert.deepEqual(source.envelopes, [], "fold deferred");

  // A read-dependent NORMAL write that reads the folded cell. Flush-on-enqueue: the fold takes its
  // mid NOW (before this write), so wire order == local-apply order.
  mutate.deriveTitle({ id: 1 }); // reads score 50 → title "s50"
  assert.deepEqual(
    wireNames(source),
    [
      [1, "setScore"],
      [2, "deriveTitle"],
    ],
    "#1: the overlapping write flushed the fold first → gapless [fold, write]",
  );
  assert.deepEqual(argsAt(source, 0), { id: 1, score: 50 }, "the drained fold shipped its LATEST value");
  assert.equal(view.data[0].title, "s50", "deriveTitle read the folded score optimistically");
  const optimisticTitle = view.data[0].title;

  // The server applies BOTH in mid order and confirms co-transactionally. setScore→score 50,
  // deriveTitle reads 50→title "s50". The authoritative row equals the optimistic prediction.
  source.batch(1, [{ table: "issue", op: "edit", old: [1, "x", 10], new: [1, "s50", 50] }], 2);
  source.lmid(2, 2);
  source.frame(2);

  // #6 NO SNAP: the title is identical optimistically and after the confirm.
  assert.equal(view.data[0].title, optimisticTitle, "#6: no cross-mutation snap (default flush-on-enqueue)");
  assert.deepEqual(issueRows(view), [[1, "s50", 50]]);
  assert.equal(backend.pending(1), false, "#9: converged → nothing pending");
  assert.equal(backend.resultType(1), "complete", "#9: ResultType stayed complete throughout");
});

test("slice 1: a NON-overlapping normal write does NOT flush the fold (economy preserved)", () => {
  const clock = new VirtualClock();
  const { source, store, mutate } = setup(clock);
  store.materialize(allIssues());
  source.snapshot(1, [{ table: "issue", op: "add", row: [1, "x", 0] }], 1);
  source.frame(1);

  mutate.setScore.folded({ key: 1, debounceMs: 100 }, { id: 1, score: 5 });
  // A normal write to a DIFFERENT table (`comment`) — no table overlap → the fold keeps deferring.
  mutate.addComment({ id: 100, issueID: 1, body: "hi" });
  assert.deepEqual(
    wireNames(source),
    [[1, "addComment"]],
    "only the non-overlapping write shipped; the fold is still deferred",
  );
  // The fold flushes later, taking the NEXT mid — gapless [addComment(1), setScore(2)].
  clock.tick(100);
  assert.deepEqual(
    wireNames(source),
    [
      [1, "addComment"],
      [2, "setScore"],
    ],
    "the deferred fold flushed on idle with a later mid (gapless)",
  );
});

// ================================================================================================
// Slice 2 — server-write rebases: the state oracle (§9.2) + no backward flicker (#5). A rebase
// landing mid-drag re-derives the folded cell from the SINGLE latest-args entry, never an earlier
// intermediate (the §6 self-correcting rewind).
// ================================================================================================

test("slice 2: a rebase mid-drag re-derives the folded cell from the LATEST args (no backward flicker)", () => {
  const clock = new VirtualClock();
  const { source, store, backend, mutate } = setup(clock);
  const view = store.materialize(allIssues());
  source.snapshot(1, [{ table: "issue", op: "add", row: [1, "x", 10] }], 1);
  source.frame(1);

  // Drag through several intermediates (deferred); head holds the latest, 30.
  for (const score of [10, 20, 30]) {
    mutate.setScore.folded({ key: 1, debounceMs: 1000 }, { id: 1, score });
    clock.tick(10);
  }
  assert.equal(view.data[0].score, 30, "head at the latest intermediate");

  // A FOREIGN server write (edits the title; confirms nothing — lmid stays 0) lands mid-drag → a
  // rebase. The single unflushed fold entry (mid:null) is RETAINED and re-invoked with its latest
  // args (30) against the rebased base. The score must read 30 — never 10/20 (no backward flicker).
  source.batch(1, [{ table: "issue", op: "edit", old: [1, "x", 10], new: [1, "y", 10] }], 2);
  source.frame(2);

  // #5 no backward flicker + STATE ORACLE (§9.2): view == fresh materialization over
  // server_state(title "y", score 10) ⊕ pending(setScore 30) = (title "y", score 30).
  assert.deepEqual(issueRows(view), [[1, "y", 30]], "#5: latest fold value survived the rebase; title rebased");
  assert.equal(backend.pending(1), true, "the unflushed fold is still pending (mid:null retained)");
  assert.deepEqual(source.envelopes, [], "still nothing on the wire (fold never crossed a rebase as a send)");

  // Now flush + confirm → convergence.
  clock.tick(1000);
  assert.deepEqual(wireMidArgs(source), [[1, { id: 1, score: 30 }]], "one flush, last value");
  source.batch(1, [{ table: "issue", op: "edit", old: [1, "y", 10], new: [1, "y", 30] }], 3);
  source.lmid(1, 3);
  source.frame(3);
  assert.deepEqual(issueRows(view), [[1, "y", 30]], "#7 convergence: view == final server state");
  assert.equal(backend.pending(1), false, "#7: pending empty at quiescence (no mid:null stranded)");
});

// ================================================================================================
// Slice 3 — out-of-phase serverConfirm: a confirm can land before/between/after the matching flush.
// Convergence (#7) and no-loss (#4): an unflushed fold (mid:null) is never dropped by a confirm for
// some OTHER mutation; it persists until its own flush, and converges.
// ================================================================================================

test("slice 3: a confirm landing while a fold is mid-window leaves the unflushed fold intact (#4/#7)", () => {
  const clock = new VirtualClock();
  const { source, store, backend, mutate } = setup(clock);
  const view = store.materialize(allIssues());
  source.snapshot(1, [], 1);
  source.frame(1);

  // A normal createIssue (mid 1) and a fold opened on the same row — the fold stays deferred.
  mutate.createIssue({ id: 1, title: "a", score: 0 }); // mid 1, ships now
  mutate.setScore.folded({ key: 1, debounceMs: 1000 }, { id: 1, score: 9 }); // deferred (mid:null)
  assert.equal(view.data[0].score, 9, "fold prediction applied live");
  assert.deepEqual(wireMids(source), [1], "only createIssue shipped so far");

  // The server confirms createIssue (mid 1) co-transactionally while the fold is STILL open. The
  // confirm-drop must retain the mid:null fold (§4.1) — its prediction survives.
  source.batch(1, [{ table: "issue", op: "add", row: [1, "a", 0] }], 2);
  source.lmid(1, 2);
  source.frame(2);
  assert.equal(backend.pending(1), true, "the unflushed fold is still pending (mid:null retained across the confirm)");
  assert.equal(view.data[0].score, 9, "the fold's prediction survived the confirm-drop + rebase");

  // Flush the fold (mid 2 — gapless after the confirm) and confirm it.
  clock.tick(1000);
  assert.deepEqual(wireMidArgs(source), [
    [1, { id: 1, title: "a", score: 0 }],
    [2, { id: 1, score: 9 }],
  ], "#4 no-loss: the fold produced its one envelope; gapless after the confirm");
  source.batch(1, [{ table: "issue", op: "edit", old: [1, "a", 0], new: [1, "a", 9] }], 3);
  source.lmid(2, 3);
  source.frame(3);
  assert.equal(backend.pending(1), false, "#7 convergence: pending empty at quiescence");
  assert.deepEqual(issueRows(view), [[1, "a", 9]]);
});

// ================================================================================================
// Slice 4 — multi-key + maxWaitMs: independent keys fold SEPARATELY (one entry/envelope each, they
// do not flush one another), and a never-idle drag still persists via the maxWaitMs cap (#4).
// ================================================================================================

test("slice 4: two independent same-table keys fold separately (do not flush each other, §2)", () => {
  const clock = new VirtualClock();
  const { source, store, mutate } = setup(clock);
  const view = store.materialize(allIssues());
  source.snapshot(
    1,
    [
      { table: "issue", op: "add", row: [1, "a", 0] },
      { table: "issue", op: "add", row: [2, "b", 0] },
    ],
    1,
  );
  source.frame(1);

  // Interleave two keys on the SAME table. A fold is absorbing (never read-dependent, §5), so one
  // fold can't be the §4.2 reader of another — they must coexist, not flush each other.
  mutate.setScore.folded({ key: 1, debounceMs: 100 }, { id: 1, score: 10 });
  mutate.setScore.folded({ key: 2, debounceMs: 100 }, { id: 2, score: 20 });
  mutate.setScore.folded({ key: 1, debounceMs: 100 }, { id: 1, score: 11 });
  mutate.setScore.folded({ key: 2, debounceMs: 100 }, { id: 2, score: 22 });
  assert.deepEqual(source.envelopes, [], "neither key flushed the other — both still folding");
  assert.deepEqual(issueRows(view), [
    [1, "a", 11],
    [2, "b", 22],
  ], "both live predictions apply independently");

  clock.tick(100);
  // Each key folds to ONE envelope carrying its last value, in creation order, gapless.
  assert.deepEqual(wireMidArgs(source), [
    [1, { id: 1, score: 11 }],
    [2, { id: 2, score: 22 }],
  ], "independent keys → one envelope each (economy preserved)");
});

test("slice 4: a never-idle drag still persists via the maxWaitMs cap (#4)", () => {
  const clock = new VirtualClock();
  const { source, store, mutate } = setup(clock);
  store.materialize(allIssues());
  source.snapshot(1, [{ table: "issue", op: "add", row: [3, "c", 0] }], 1);
  source.frame(1);

  // Invoke faster than the debounce (every 40ms < 100ms) so the trailing debounce NEVER idles —
  // only the maxWaitMs cap (200ms) can flush it. Without the cap this stream would never persist.
  mutate.setScore.folded({ key: 3, debounceMs: 100, maxWaitMs: 200 }, { id: 3, score: 0 });
  for (let v = 1; v <= 12; v++) {
    clock.tick(40);
    mutate.setScore.folded({ key: 3, debounceMs: 100, maxWaitMs: 200 }, { id: 3, score: v });
  }
  // The cap fired at least once mid-stream (the debounce alone would still be open at every step).
  assert.ok(source.envelopes.length >= 1, "#4: the never-idle drag persisted via maxWaitMs");
  for (const e of source.envelopes) assert.equal(e.name, "setScore");
  // Economy bound (#2): never more envelopes than invokes (13 invokes here).
  assert.ok(source.envelopes.length <= 13, "#2: economy is a bound — never more envelopes than invokes");
});

// ================================================================================================
// Slice 5 — deferAcrossWrites: the opt-in keeps deferring across an overlapping read-dependent
// write, so the cross-mutation snap (#6) is ASSERTED TO OCCUR (the documented cost) — while
// gaplessness (#1) STILL holds.
// ================================================================================================

test("slice 5: deferAcrossWrites keeps deferring — the snap occurs (documented cost), #1 still holds", () => {
  const clock = new VirtualClock();
  const { source, store, mutate } = setup(clock);
  const view = store.materialize(allIssues());
  source.snapshot(1, [{ table: "issue", op: "add", row: [1, "x", 10] }], 1);
  source.frame(1);

  // Fold the score (deferAcrossWrites) → it will NOT flush when an overlapping write arrives.
  mutate.setScore.folded({ key: 1, debounceMs: 100, deferAcrossWrites: true }, { id: 1, score: 50 });
  // A read-dependent normal write reads the FOLDED (predicted) score → title "s50", takes mid 1.
  mutate.deriveTitle({ id: 1 });
  assert.deepEqual(wireNames(source), [[1, "deriveTitle"]], "the fold kept deferring — only the normal write shipped");
  const optimisticTitle = view.data[0].title;
  assert.equal(optimisticTitle, "s50", "optimistically, deriveTitle read the fold's predicted score (50)");

  // Flush the fold (mid 2). #1 gaplessness STILL holds across the deferred interleave.
  clock.tick(100);
  assert.deepEqual(wireMids(source), [1, 2], "#1: gaplessness holds in deferAcrossWrites too");

  // The server applies in MID order: deriveTitle(1) reads the OLD score 10 → "s10"; setScore(2) → 50.
  // The authoritative title ("s10") differs from the optimistic prediction ("s50") → the snap.
  source.batch(1, [{ table: "issue", op: "edit", old: [1, "x", 10], new: [1, "s10", 50] }], 2);
  source.lmid(2, 2);
  source.frame(2);
  assert.equal(view.data[0].title, "s10", "#6: the cross-mutation snap OCCURRED (s50 → s10) — deferAcrossWrites' cost");
  assert.notEqual(view.data[0].title, optimisticTitle, "the optimistic title was not what the server computed");
  assert.equal(view.data[0].score, 50, "score converged");
});

// ================================================================================================
// Slice 6 — negative lane: the folded path REFUSES a read-dependent / non-absorbing mutator (#10),
// with no false positives (an absorbing keyed `update` is still foldable) and no mid burnt.
// ================================================================================================

test("slice 6: the folded path refuses read-dependent mutators and burns no mid (#10)", () => {
  const clock = new VirtualClock();
  const { source, store, backend, mutate } = setup(clock);
  const view = store.materialize(allIssues());
  source.snapshot(1, [{ table: "issue", op: "add", row: [1, "x", 10] }], 1);
  source.frame(1);

  // `incrementScore` reads via tx.row to compute its write — the canonical non-absorbing shape.
  assert.throws(
    () => mutate.incrementScore.folded({ key: 1 }, { id: 1, delta: 1 }),
    /not absorbing|reads state/,
    "#10: a read-dependent mutator is refused on the folded path",
  );
  // `deriveTitle` also reads (tx.row) → refused.
  assert.throws(() => mutate.deriveTitle.folded({ key: 1 }, { id: 1 }), /not absorbing|reads state/);

  // No false positive AND no mid burnt: nothing shipped, nothing pending, the view is untouched, and
  // the next NORMAL mutation still gets mid 1 (the gapless sequence was never disturbed).
  assert.deepEqual(source.envelopes, [], "the refused folds shipped nothing");
  assert.equal(backend.pending(1), false, "the refused folds left no pending entry");
  assert.deepEqual(issueRows(view), [[1, "x", 10]], "the view is untouched by the refused folds");

  // No false positives: an ABSORBING keyed `update` (reads internally only to preserve columns) folds fine.
  const h = mutate.setScore.folded({ key: 1 }, { id: 1, score: 99 });
  h.flush();
  assert.deepEqual(wireMidArgs(source), [[1, { id: 1, score: 99 }]], "the absorbing fold was accepted, mid 1 (no mid burnt earlier)");
});

// ================================================================================================
// Slice 7 — the fuzz lane (FOLDED-MUTATIONS-DESIGN §9.2/§9.3): random interleavings of folded /
// normal invokes, virtual-clock ticks, out-of-phase server confirms, and foreign server writes,
// over a deterministic seeded PRNG, in BOTH modes. The STATE ORACLE is a from-scratch JS replay
// (a different substrate than the wasm engine — genuinely differential): at every server release
// the delivered view must equal `server_truth ⊕ replay(sent-unconfirmed in mid order, unflushed
// folds last)`. Plus the timing-independent policy invariants (#1 gaplessness, #2 economy, #4
// no-loss, #7 convergence, #9 pending axis) every step.
// ================================================================================================

// A seeded PRNG (mulberry32) — reproducible schedules without wall-clock / Math.random.
function mulberry32(seed: number): () => number {
  let s = seed >>> 0;
  return () => {
    s = (s + 0x6d2b79f5) | 0;
    let t = Math.imul(s ^ (s >>> 15), 1 | s);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

// The independent reference substrate: a plain-JS store + a MutationTx over it, mirroring the keyed
// semantics of the real `trackingTx` (single-column `id` pk). The fold/rebase machinery never runs
// here — this is the "fresh materialization" ground truth (§9.2).
type IssueCells = [number, string, number];
type JsStore = { issue: Map<number, IssueCells> };
const ISSUE_COLS = ["id", "title", "score"] as const;

function jsTx(store: JsStore): MutationTx {
  const toKeyed = (cells: IssueCells): KeyedRowLike => ({ id: cells[0], title: cells[1], score: cells[2] });
  const tx = {
    get: (_t: string, pk: unknown[]) => store.issue.get(pk[0] as number),
    add: (_t: string, row: unknown[]) => store.issue.set(row[0] as number, [...(row as IssueCells)]),
    remove: (_t: string, row: unknown[]) => store.issue.delete(row[0] as number),
    edit: (_t: string, oldRow: unknown[], newRow: unknown[]) => {
      store.issue.delete(oldRow[0] as number);
      store.issue.set(newRow[0] as number, [...(newRow as IssueCells)]);
    },
    row: (_t: string, keyed: KeyedRowLike) => {
      const c = store.issue.get(keyed.id as number);
      return c ? toKeyed(c) : undefined;
    },
    insert: (_t: string, keyed: KeyedRowLike) =>
      store.issue.set(keyed.id as number, ISSUE_COLS.map((c) => keyed[c]) as IssueCells),
    update: (_t: string, keyed: KeyedRowLike) => {
      const cur = store.issue.get(keyed.id as number);
      if (!cur) return;
      store.issue.set(keyed.id as number, ISSUE_COLS.map((c, i) => (c in keyed ? keyed[c] : cur[i])) as IssueCells);
    },
    upsert: (_t: string, keyed: KeyedRowLike) =>
      store.issue.set(keyed.id as number, ISSUE_COLS.map((c) => keyed[c]) as IssueCells),
    delete: (_t: string, keyed: KeyedRowLike) => store.issue.delete(keyed.id as number),
  };
  return tx as unknown as MutationTx;
}
type KeyedRowLike = Record<string, unknown>;

const cloneIssue = (m: Map<number, IssueCells>): Map<number, IssueCells> =>
  new Map([...m].map(([k, v]) => [k, [...v] as IssueCells]));
const cellsEq = (a: IssueCells, b: IssueCells) => a[0] === b[0] && a[1] === b[1] && a[2] === b[2];
const sortedIssueRows = (m: Map<number, IssueCells>): IssueCells[] =>
  [...m.values()].sort((a, b) => a[0] - b[0]).map((c) => [...c] as IssueCells);

function diffIssue(before: Map<number, IssueCells>, after: Map<number, IssueCells>): NormalizedOp[] {
  const ops: NormalizedOp[] = [];
  for (const [id, cells] of after) {
    const old = before.get(id);
    if (!old) ops.push({ table: "issue", op: "add", row: cells });
    else if (!cellsEq(old, cells)) ops.push({ table: "issue", op: "edit", old, new: cells });
  }
  for (const [id, old] of before) if (!after.has(id)) ops.push({ table: "issue", op: "remove", row: old });
  return ops;
}

// Re-run a recorded mutator (the SAME registry code) over the JS reference store.
const applyJs = (store: JsStore, name: string, args: unknown): void =>
  (registry[name as keyof typeof registry] as (t: MutationTx, a: unknown) => void)(jsTx(store), args);

function runFuzzScenario(seed: number, mode: "default" | "defer"): void {
  const r = mulberry32(seed);
  const pick = <T>(arr: readonly T[]): T => arr[Math.floor(r() * arr.length)];
  const ints = (n: number): number => Math.floor(r() * n);

  const clock = new VirtualClock();
  const { source, store, backend, mutate } = setup(clock);
  const view = store.materialize(allIssues());

  // A fixed 5-row world (edits only — adds/removes are well-covered elsewhere; the fold surface is
  // the edit/rebase interaction). server_truth starts == the client's hydrated base.
  const ids = [1, 2, 3, 4, 5] as const;
  const server: { issue: Map<number, IssueCells>; lmid: number; cv: number } = {
    issue: new Map(ids.map((id) => [id, [id, `t${id}`, 0] as IssueCells])),
    lmid: 0,
    cv: 1,
  };
  source.snapshot(1, [...server.issue.values()].map((row) => ({ table: "issue", op: "add", row }) as NormalizedOp), 1);
  source.frame(1);

  // Harness-side mirror of the CLIENT's pending set (the contract, not the engine): one entry per
  // live fold key (unflushed), plus sent-but-unconfirmed envelopes (derived from the wire trace).
  const activeFolds = new Map<string, { id: number; score: number }>();
  let envCursor = 0;
  let foldedInvokes = 0;
  let normalInvokes = 0;
  const foldKeysInvoked = new Set<number>();

  // A setScore envelope appearing on the wire means that fold flushed → it leaves the unflushed set.
  const syncFlushes = (): void => {
    for (; envCursor < source.envelopes.length; envCursor++) {
      const e = source.envelopes[envCursor];
      if (e.name === "setScore") activeFolds.delete(`setScore\0${(e.args as { id: number }).id}`);
    }
  };

  const checkPolicy = (label: string): void => {
    // #1 gaplessness: mids are 1,2,3,… contiguous in arrival order.
    assert.deepEqual(wireMids(source), source.envelopes.map((_, i) => i + 1), `#1 gaplessness ${label}`);
    // #2 economy is a BOUND: never more envelopes than invokes.
    assert.ok(source.envelopes.length <= foldedInvokes + normalInvokes, `#2 economy ${label}`);
    // #9 ResultType is server-channel-only — a pending mutation never moves a hydrated query.
    assert.equal(backend.resultType(1), "complete", `#9 resultType ${label}`);
  };

  // The §9.2 state oracle: fresh JS replay over server_truth.
  const expectedIssueRows = (): IssueCells[] => {
    const m: JsStore = { issue: cloneIssue(server.issue) };
    for (const e of source.envelopes.filter((e) => e.mid > server.lmid).sort((a, b) => a.mid - b.mid)) {
      applyJs(m, e.name, e.args);
    }
    for (const args of activeFolds.values()) applyJs(m, "setScore", args); // unflushed folds last
    return sortedIssueRows(m.issue);
  };

  const serverStep = (confirmAll: boolean): void => {
    const before = cloneIssue(server.issue);
    // A foreign (other-client) write sometimes lands — forcing a rebase against a moving baseline.
    if (!confirmAll && r() < 0.5) {
      const id = pick(ids);
      const cur = server.issue.get(id)!;
      server.issue.set(id, [id, cur[1], cur[2] + 1 + ints(5)]);
    }
    // Confirm client envelopes up to a (possibly partial, out-of-phase) point.
    const maxMid = source.envelopes.length;
    const upto = confirmAll ? maxMid : server.lmid + ints(maxMid - server.lmid + 1);
    const toApply = source.envelopes.filter((e) => e.mid > server.lmid && e.mid <= upto).sort((a, b) => a.mid - b.mid);
    for (const e of toApply) applyJs(server, e.name, e.args);
    const advanced = toApply.length > 0;
    if (advanced) server.lmid = Math.max(server.lmid, ...toApply.map((e) => e.mid));
    const ops = diffIssue(before, server.issue);
    server.cv += 1;
    if (ops.length) source.batch(1, ops, server.cv);
    if (advanced) source.lmid(server.lmid, server.cv);
    source.frame(server.cv);
    syncFlushes();

    // The §9.2 state oracle (mid-order replay, unflushed folds last) matches the client head only
    // when the head is in CANONICAL form. The engine re-derives canonically on every reconcile; a
    // reconcile fires iff this release carried server data OR confirmed a client mutation
    // (`ops.length || advanced` — the harness twin of the engine's `muts.length || pendingChanged`).
    // In DEFAULT mode the head is *always* canonical (flush-on-enqueue keeps invoke order == mid
    // order), so assert unconditionally; in deferAcrossWrites mode the head holds the transient SNAP
    // between reconciles (the documented cost), so the canonical oracle is only valid right after a
    // reconcile actually fired.
    const didReconcile = ops.length > 0 || advanced;
    if (mode === "default" || didReconcile) {
      assert.deepEqual(issueRows(view), expectedIssueRows(), `state oracle (${mode}) @cv${server.cv}`);
    }
    // #9: pending axis tracks exactly the unconfirmed-sent ∪ unflushed-fold set (all touch issue).
    const anyPending = source.envelopes.some((e) => e.mid > server.lmid) || activeFolds.size > 0;
    assert.equal(backend.pending(1), anyPending, `#9 pending axis (${mode}) @cv${server.cv}`);
  };

  const STEPS = 30;
  for (let i = 0; i < STEPS; i++) {
    const op = pick(["fold", "fold", "fold", "normal", "tick", "tick", "server"] as const);
    if (op === "fold") {
      const id = pick(ids);
      const score = ints(100);
      activeFolds.set(`setScore\0${id}`, { id, score });
      foldKeysInvoked.add(id);
      foldedInvokes++;
      mutate.setScore.folded(
        { key: id, debounceMs: 100, maxWaitMs: 500, deferAcrossWrites: mode === "defer" },
        { id, score },
      );
    } else if (op === "normal") {
      normalInvokes++;
      const kind = pick(["setTitle", "deriveTitle"] as const);
      const id = pick(ids);
      if (kind === "setTitle") mutate.setTitle({ id, title: `u${ints(9)}` });
      else mutate.deriveTitle({ id }); // read-dependent — drains overlapping folds in default mode
      syncFlushes(); // a normal issue write may have drained folds (flush-on-enqueue)
    } else if (op === "tick") {
      clock.tick(20 + ints(180));
      syncFlushes();
    } else {
      serverStep(false);
    }
    checkPolicy(`step ${i} (${mode})`);
  }

  // Quiescence: drain timers + any deferred folds, then confirm EVERYTHING.
  clock.tick(1000);
  syncFlushes();
  backend.flushFolds();
  syncFlushes();
  serverStep(true);

  // #4 no-loss: every fold key that was ever invoked produced ≥1 envelope (none stranded).
  assert.equal(activeFolds.size, 0, `#4 no-loss: every fold flushed by quiescence (${mode}, seed ${seed})`);
  for (const id of foldKeysInvoked) {
    assert.ok(source.envelopes.some((e) => e.name === "setScore" && (e.args as { id: number }).id === id),
      `#4 no-loss: fold key ${id} produced an envelope (${mode}, seed ${seed})`);
  }
  // #7 convergence: view == final server state, pending empty.
  assert.equal(backend.pending(1), false, `#7 convergence: pending empty (${mode}, seed ${seed})`);
  assert.deepEqual(issueRows(view), sortedIssueRows(server.issue), `#7 convergence: view == server (${mode}, seed ${seed})`);
}

test("slice 7: fuzz — state oracle + policy invariants under random schedules (default mode)", () => {
  for (let seed = 1; seed <= 50; seed++) runFuzzScenario(seed, "default");
});

test("slice 7: fuzz — state oracle + policy invariants under random schedules (deferAcrossWrites)", () => {
  for (let seed = 1; seed <= 50; seed++) runFuzzScenario(seed, "defer");
});

// #8 — snap-back once: a rejected fold (server processes-as-no-op: lmid advances, no effect) snaps
// the cell back to the server's authoritative value, and the pending axis clears exactly once.
test("slice 7 / #8: a rejected fold snaps back to the server value, pending clears once", () => {
  const clock = new VirtualClock();
  const { source, store, backend, mutate } = setup(clock);
  const view = store.materialize(allIssues());
  source.snapshot(1, [{ table: "issue", op: "add", row: [1, "x", 10] }], 1);
  source.frame(1);

  let pendingFlips = 0;
  backend.onPending((qid, p) => {
    if (qid === 1) pendingFlips++;
  });

  const h = mutate.setScore.folded({ key: 1, debounceMs: 100 }, { id: 1, score: 50 });
  clock.tick(100); // flush → mid 1
  assert.equal(view.data[0].score, 50, "optimistic prediction shown");
  assert.equal(backend.pending(1), true);

  // The server rejects it: lmid advances to 1 with NO data effect (processed-as-no-op).
  source.lmid(1, 2);
  source.frame(2);
  assert.equal(view.data[0].score, 10, "#8: snapped back to the server's authoritative value");
  assert.equal(backend.pending(1), false, "pending cleared");
  assert.equal(pendingFlips, 2, "the pending axis flipped exactly twice (true on invoke, false on the snap-back)");
  void h;
});

// ================================================================================================
// Slice §9.3 — roomDebounceMs: a fold that routes into a ROOM flushes at the short (streaming)
// cadence so intermediates reach the shared head; OFF the room the caller's collapse debounce
// governs. The room decision is the DECLARED domain at fold-open (302 §5 — no write-set proof).
// ================================================================================================

test("§9.3: roomDebounceMs is IGNORED off the room — the caller's collapse debounce governs", () => {
  const clock = new VirtualClock();
  const { source, store, mutate } = setup(clock);
  const view = store.materialize(allIssues());
  source.snapshot(1, [{ table: "issue", op: "add", row: [1, "x", 0] }], 1);
  source.frame(1);

  // No room connected ⇒ the fold routes to the daemon ⇒ roomDebounceMs is ignored, debounceMs wins.
  mutate.setScore.folded({ key: 1, debounceMs: 1000, roomDebounceMs: 20 }, { id: 1, score: 5 });
  clock.tick(20);
  assert.equal(source.envelopes.length, 0, "roomDebounceMs must NOT flush a daemon-routed fold");
  clock.tick(1000);
  assert.equal(source.envelopes.length, 1, "the caller's debounceMs (1000ms) collapse flushed it");
  assert.deepEqual(argsAt(source, 0), { id: 1, score: 5 });
  void view;
});

test("§9.3: roomDebounceMs governs when the fold's DECLARED domain is a room — intermediates stream, last-value-wins per window", () => {
  const clock = new VirtualClock();
  // An inline store (not `setup`): setScore is DECLARED a room mutator (302 §5 — declaration, not
  // a write-set proof). NO registerRoomTables, so the prediction stages onto the plain `issue`
  // table the view reads; the connected room gate makes the flush cadence + transport observable.
  const source = new TestSource();
  const { store, backend, mutate } = createOptimisticStore(schema, source, registry, {
    clientID: "c1",
    clock,
    domainPolicy: (name: string) => (name === "setScore" ? "room:R" : undefined),
  });
  const view = store.materialize(allIssues());
  source.snapshot(1, [{ table: "issue", op: "add", row: [1, "x", 0] }], 1);
  source.frame(1);

  const roomSrc = new TestSource();
  backend.connectSource("room:R", roomSrc);

  // A drag: three folds inside one 20ms window. The long debounceMs (1000ms) is IGNORED — the
  // room cadence (20ms) flushes, and only the LAST value ships (economy within the window).
  mutate.setScore.folded({ key: 1, debounceMs: 1000, roomDebounceMs: 20 }, { id: 1, score: 1 });
  clock.tick(8);
  mutate.setScore.folded({ key: 1, debounceMs: 1000, roomDebounceMs: 20 }, { id: 1, score: 2 });
  clock.tick(8);
  mutate.setScore.folded({ key: 1, debounceMs: 1000, roomDebounceMs: 20 }, { id: 1, score: 3 });
  assert.equal(roomSrc.envelopes.length, 0, "still coalescing inside the window");

  clock.tick(20); // cross the room cadence
  assert.equal(source.envelopes.length, 0, "the daemon never saw it — the fold's declared domain is the room");
  assert.equal(roomSrc.envelopes.length, 1, "roomDebounceMs flushed to the room well before 1000ms");
  assert.deepEqual(roomSrc.envelopes[0].args, { id: 1, score: 3 }, "last-value-wins within the window");
  assert.equal(view.data[0].score, 3, "the local prediction reflects the latest frame");
});
