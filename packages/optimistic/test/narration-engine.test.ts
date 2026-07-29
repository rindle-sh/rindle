// Narration END-TO-END against the REAL wasm engine (not hand-built FlatChange fixtures): an
// OptimisticBackend over a scripted OptimisticSource drives the genuine fork/rebase loop, and a
// `@rindle/narrator` rides the view's `onChanges` channel exactly as `useNarration` does. This is the
// coverage the deleted example-event-planner e2e used to hold — it proves the netting cancels the
// engine's OWN rebase output (a confirmed prediction's raw remove+add), that catch-up hydration is
// phased as a snapshot (not narrated), and that nested `related` templates resolve real engine paths.
//
// Requires the wasm artifact (packages/wasm/build.sh) — same as the other suites here.

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
  type NormalizedTableSchema,
  type OptimisticSource,
  type ProgressFrame,
  type QueryId,
  type RemoteQuery,
} from "@rindle/wasm";
import { createNarrator, type NarratorRegistry } from "@rindle/narrator";
import { createOptimisticStore, type ClientRegistry, type MutationTx } from "../src/index.ts";

await initWasm();

const issue = table("issue").columns({ id: number(), title: string(), score: number() }).primaryKey("id");
const comment = table("comment").columns({ id: number(), issueID: number(), body: string() }).primaryKey("id");
const schema = createSchema({ tables: [issue, comment] });

class TestSource implements OptimisticSource {
  normalized: (qid: QueryId, ev: NormalizedEvent) => void = () => {};
  progress: (frame: ProgressFrame) => void = () => {};
  registerQuery(): void {}
  unregisterQuery(): void {}
  pushMutation(_env: MutationEnvelope): Promise<void> {
    return Promise.resolve();
  }
  restart: () => void = () => {};
  onNormalized(h: (qid: QueryId, ev: NormalizedEvent) => void): void {
    this.normalized = h;
  }
  onProgress(h: (frame: ProgressFrame) => void): void {
    this.progress = h;
  }
  onRestart(h: () => void): void {
    this.restart = h;
  }
  expectClientSchema(_schemas: NormalizedTableSchema[]): void {}

  snapshot(qid: QueryId, ops: NormalizedOp[], cv: number): void {
    this.normalized(qid, { type: "snapshot", ops, cv });
  }
  batch(qid: QueryId, ops: NormalizedOp[], cv: number): void {
    this.normalized(qid, { type: "batch", ops, cv });
  }
  lmid(mid: number, cv: number): void {
    this.normalized(0, { type: "batch", ops: [{ table: "_rindle_client_mutations", op: "add", row: ["c1", mid] }], cv });
  }
  frame(cvMin: number): void {
    this.progress({ cvMin });
  }
}

const registry = {
  createIssue: (tx: MutationTx, a: { id: number; title: string; score: number }) => tx.add("issue", [a.id, a.title, a.score]),
  addComment: (tx: MutationTx, a: { id: number; issueID: number; body: string }) => tx.add("comment", [a.id, a.issueID, a.body]),
} satisfies ClientRegistry;

const qb = newQueryBuilder(schema);
const issuesWithComments = defineQuery("issuesWithComments", () =>
  qb.issue.sub("comments", comment, { parent: ["id"], child: ["issueID"] }, (c) => c.orderBy("id", "asc")).orderBy("id", "asc"),
);

const NARRATORS: NarratorRegistry = {
  issuesWithComments: {
    salience: "info",
    root: {
      add: ({ row }) => `Added issue "${row.title as string}".`,
      remove: ({ row }) => `Removed issue "${row.title as string}".`,
    },
    related: {
      comments: {
        salience: "alert",
        add: ({ row, parent }) => `New comment on "${parent?.title as string}": "${row.body as string}".`,
      },
    },
  },
};

/** Wire a narrator onto a query's view exactly as `useNarration` does (batch-phase only by default),
 *  returning the running list of digest blocks so a test can assert what was narrated. */
function narrateView(store: ReturnType<typeof createOptimisticStore>["store"]) {
  const narrator = createNarrator(NARRATORS);
  const lines: string[] = [];
  const view = store.materialize(issuesWithComments(), {
    onChanges: (changes, phase, schema) => {
      if (phase !== "batch") return; // the useNarration default: the initial snapshot is not a change
      const block = narrator.digest(narrator.narrate("issuesWithComments", schema, changes, phase, {}));
      if (block) lines.push(block);
    },
  });
  return { view, lines };
}

test("real engine: catch-up hydration is phased as a snapshot and NOT narrated as changes", () => {
  const source = new TestSource();
  const { store } = createOptimisticStore(schema, source, registry, { clientID: "c1" });
  const { lines } = narrateView(store);

  // The server hydrates the query with two existing issues — delivered through the reconcile cycle as
  // a catch-up batch, which the Store phases as a snapshot. A "what CHANGED" narrator ignores it.
  source.snapshot(1 as QueryId, [
    { table: "issue", op: "add", row: [1, "alpha", 10] },
    { table: "issue", op: "add", row: [2, "beta", 20] },
  ], 1);
  source.frame(1);

  assert.deepEqual(lines, [], "the initial result set is not narrated as a batch of adds");
});

test("real engine: a confirmed optimistic prediction narrates the local write ONCE, then nets to nothing", () => {
  const source = new TestSource();
  const { store, mutate } = createOptimisticStore(schema, source, registry, { clientID: "c1" });
  const { view, lines } = narrateView(store);
  source.snapshot(1 as QueryId, [], 1);
  source.frame(1);

  // The optimistic prediction is a real local change → narrated once.
  mutate.createIssue({ id: 7, title: "mine", score: 5 });
  assert.deepEqual(lines, ['• Added issue "mine".'], "the optimistic write narrates immediately");
  assert.equal(view.data.length, 1);
  lines.length = 0;

  // The server echoes the SAME effect and confirms the mid: the reconcile rewinds the prediction and
  // re-applies the server row, emitting a balanced remove+add from the REAL engine. The view's netting
  // must cancel it, so a correctly predicted write adds NOTHING to the agent's context.
  source.batch(1 as QueryId, [{ table: "issue", op: "add", row: [7, "mine", 5] }], 2);
  source.lmid(1, 2);
  source.frame(2);
  assert.deepEqual(lines, [], "the confirmed rebase nets to nothing — no churn narrated");
  assert.deepEqual(view.data.map((r) => r.id), [7], "and the view still shows the row exactly once");
});

test("real engine: a foreign change and a nested comment add each narrate exactly once", () => {
  const source = new TestSource();
  const { store } = createOptimisticStore(schema, source, registry, { clientID: "c1" });
  const { lines } = narrateView(store);
  source.snapshot(1 as QueryId, [{ table: "issue", op: "add", row: [8, "other", 1] }], 1);
  source.frame(1);
  lines.length = 0; // drop the hydration (snapshot phase, already ignored — belt and suspenders)

  // Another client renames-by-adding a new issue: a genuine root add → narrated once.
  source.batch(1 as QueryId, [{ table: "issue", op: "add", row: [9, "fresh", 2] }], 2);
  source.frame(2);
  assert.deepEqual(lines, ['• Added issue "fresh".'], "a foreign root add narrates once");
  lines.length = 0;

  // A comment lands on an existing issue: a NESTED change whose path the real engine emits — it must
  // resolve through the `related.comments` template with the parent issue in hand (alert salience).
  source.batch(1 as QueryId, [{ table: "comment", op: "add", row: [100, 8, "looks good"] }], 3);
  source.frame(3);
  assert.deepEqual(lines, ['⚠️ New comment on "other": "looks good".'], "the nested related template fires against real engine paths");
});
