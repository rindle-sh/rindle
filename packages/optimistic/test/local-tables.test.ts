// Local-only tables (201-LOCAL-ONLY-TABLES-DESIGN.md) — the ENGINE half over the real wasm
// engine + a hand-scripted OptimisticSource. Pins the load-bearing §9 invariants:
//   C1  a local row SURVIVES an unrelated server frame (untracked ⇒ never rewound) — §10.2's
//       "single most important invariant";
//   Q2  a query touching a local table opens NO subscription and ships nothing upstream;
//   L1  countAs over a LOCAL child is a native reduce reflecting local writes, with NO synthetic
//       __agg_* base table registered;
//   M1  a replayable mutator may neither READ nor WRITE a local table;
//   M2  writeLocal rejects a synced/tracked table;
//   E3  the backend rejects a REMOTE query that references a local table;
//   A6  local data survives a daemon restart/resync.
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
  type Ast,
  type MutationEnvelope,
  type NormalizedEvent,
  type NormalizedOp,
  type NormalizedTableSchema,
  type OptimisticSource,
  type ProgressFrame,
  type QueryId,
  type RemoteQuery,
} from "@rindle/wasm";
import { createOptimisticStore, type ClientRegistry, type MutationTx } from "../src/index.ts";

await initWasm();

const issue = table("issue").columns({ id: number(), title: string() }).primaryKey("id");
const selection = table("selection", { local: true }).columns({ id: number(), issueId: number() }).primaryKey("id");
const draft = table("draft", { local: true }).columns({ id: number(), text: string() }).primaryKey("id");
const schema = createSchema({ tables: [issue, selection, draft] });

class TestSource implements OptimisticSource {
  normalized: (qid: QueryId, ev: NormalizedEvent) => void = () => {};
  progress: (frame: ProgressFrame) => void = () => {};
  registered: Array<[QueryId, RemoteQuery]> = [];
  unregistered: QueryId[] = [];
  envelopes: MutationEnvelope[] = [];
  expected: NormalizedTableSchema[][] = [];
  restart: () => void = () => {};

  registerQuery(qid: QueryId, remote: RemoteQuery): void {
    this.registered.push([qid, remote]);
  }
  unregisterQuery(qid: QueryId): void {
    this.unregistered.push(qid);
  }
  pushMutation(env: MutationEnvelope): Promise<void> {
    this.envelopes.push(env);
    return Promise.resolve();
  }
  expectClientSchema(tables: NormalizedTableSchema[]): void {
    this.expected.push(tables);
  }
  onNormalized(h: (qid: QueryId, ev: NormalizedEvent) => void): void {
    this.normalized = h;
  }
  onProgress(h: (frame: ProgressFrame) => void): void {
    this.progress = h;
  }
  onRestart(h: () => void): void {
    this.restart = h;
  }

  // test drivers
  snapshot(qid: QueryId, ops: NormalizedOp[], cv: number): void {
    this.normalized(qid, { type: "snapshot", ops, cv });
  }
  batch(qid: QueryId, ops: NormalizedOp[], cv: number): void {
    this.normalized(qid, { type: "batch", ops, cv });
  }
  frame(cvMin: number): void {
    this.progress({ cvMin });
  }
  /** The names advertised in the latest expectClientSchema call (the client's expected-schema set). */
  lastExpectedNames(): string[] {
    const last = this.expected[this.expected.length - 1] ?? [];
    return last.map((t) => t.name).sort();
  }
  userRegs(): Array<[QueryId, RemoteQuery]> {
    return this.registered.filter(([qid]) => qid !== 0); // qid 0 is the reserved lmid system query
  }
}

const registry = {
  createIssue: (tx: MutationTx, args: { id: number; title: string }) => tx.add("issue", [args.id, args.title]),
  // M1 violators — a replayable mutator that touches a local table.
  selectViaMutator: (tx: MutationTx, args: { id: number; issueId: number }) =>
    tx.add("selection", [args.id, args.issueId]),
  readSelectionInMutator: (tx: MutationTx, args: { id: number }) => {
    tx.row("selection", { id: args.id });
  },
} satisfies ClientRegistry;

const qb = newQueryBuilder(schema);
const allIssues = defineQuery("allIssues", () => qb.issue);

function setup() {
  const source = new TestSource();
  const { store, backend, mutate } = createOptimisticStore(schema, source, registry, { clientID: "c1" });
  return { source, store, backend, mutate };
}

test("C1 — a local row survives an unrelated server frame (untracked ⇒ never rewound)", () => {
  const { source, store } = setup();

  // A real synced subscription, hydrated.
  const issues = store.materialize(allIssues());
  source.snapshot(1, [{ table: "issue", op: "add", row: [1, "a"] }], 1);
  source.frame(1);
  assert.deepEqual(issues.data.map((r) => r.id), [1]);

  // A local view + a direct-commit local write.
  const sel = store.query.selection.materialize();
  store.writeLocal((tx) => tx.add("selection", { id: 100, issueId: 1 }));
  assert.deepEqual(sel.data, [{ id: 100, issueId: 1 }], "the local write shows immediately");

  // An UNRELATED server frame for the SYNCED table drives a full reconcile cycle (rewind). If the
  // local source were TRACKED, structural_diff(head, fork(sync)) would delete the local row here.
  source.batch(1, [{ table: "issue", op: "add", row: [2, "b"] }], 2);
  source.frame(2);
  assert.deepEqual(issues.data.map((r) => r.id), [1, 2], "the synced table advanced");
  assert.deepEqual(sel.data, [{ id: 100, issueId: 1 }], "the local row SURVIVED the server cycle (C1)");
});

test("Q2 — a query touching a local table opens NO subscription and ships nothing upstream", () => {
  const { source, store } = setup();
  store.query.selection.materialize();
  // A MIXED local⊕synced query is also local-only (nameless) — it reads already-synced issues.
  store.query.issue.countAs("selCount", selection, { parent: ["id"], child: ["issueId"] }).materialize();
  assert.deepEqual(source.userRegs(), [], "no remote subscribe for any local-touching query");
  store.writeLocal((tx) => tx.add("selection", { id: 1, issueId: 1 }));
  assert.deepEqual(source.envelopes, [], "a local write ships no mutation envelope");
});

test("L1 — countAs over a LOCAL child is a native reduce; no synthetic __agg_* is registered", () => {
  const { source, store } = setup();

  // Issues arrive via a SEPARATE subscription (§5.1 — the local query reads what is already synced).
  store.materialize(allIssues());
  source.snapshot(
    1,
    [
      { table: "issue", op: "add", row: [1, "a"] },
      { table: "issue", op: "add", row: [2, "b"] },
    ],
    1,
  );
  source.frame(1);

  const expectedBefore = source.lastExpectedNames();
  const view = store.query.issue
    .countAs("selCount", selection, { parent: ["id"], child: ["issueId"] })
    .orderBy("id", "asc")
    .materialize();
  assert.deepEqual(
    view.data,
    [
      { id: 1, title: "a", selCount: 0 },
      { id: 2, title: "b", selCount: 0 },
    ],
    "a childless parent reads 0 via the reduce identity",
  );

  // L1: NO synthetic aggregate base table was registered for the local child.
  const expectedAfter = source.lastExpectedNames();
  assert.ok(!expectedAfter.some((n) => n.startsWith("__agg_")), "no synthetic __agg_* for a local child");
  assert.deepEqual(expectedAfter, expectedBefore, "the expected-schema set did not grow");

  // Local selection writes flow straight through the native reduce.
  store.writeLocal((tx) => tx.add("selection", { id: 10, issueId: 1 }));
  store.writeLocal((tx) => tx.add("selection", { id: 11, issueId: 1 }));
  assert.equal(view.data.find((r) => r.id === 1)!.selCount, 2, "two selections on issue 1");
  assert.equal(view.data.find((r) => r.id === 2)!.selCount, 0, "issue 2 untouched");

  store.writeLocal((tx) => tx.remove("selection", { id: 10, issueId: 1 }));
  assert.equal(view.data.find((r) => r.id === 1)!.selCount, 1, "removal decrements via the reduce");
});

test("M1 — a replayable mutator may not WRITE a local table", () => {
  const { mutate } = setup();
  assert.throws(() => mutate.selectViaMutator({ id: 1, issueId: 1 }), /cannot write local-only table "selection"/);
});

test("M1 — a replayable mutator may not READ a local table", () => {
  const { mutate } = setup();
  assert.throws(() => mutate.readSelectionInMutator({ id: 1 }), /cannot read local-only table "selection"/);
});

test("M2 — writeLocal rejects a synced/tracked table (as a REJECTED promise, not a sync throw)", async () => {
  const { store } = setup();
  await assert.rejects(
    () => store.writeLocal((tx) => tx.add("issue", { id: 1, title: "x" })),
    /"issue" is not a local-only table/,
  );
});

test("E3 (backend) — a REMOTE query referencing a local table is rejected", () => {
  const { backend } = setup();
  const ast: Ast = { table: "selection" };
  assert.throws(
    () => backend.registerQuery(999, ast, { name: "evil", args: null }),
    /references local-only table "selection"/,
  );
});

test("A6 — local data survives a daemon restart/resync", () => {
  const { source, store } = setup();
  const sel = store.query.selection.materialize();
  store.writeLocal((tx) => tx.add("selection", { id: 7, issueId: 1 }));
  assert.deepEqual(sel.data, [{ id: 7, issueId: 1 }]);
  source.restart(); // resync re-subscribes remote queries only; it never touches local sources.
  assert.deepEqual(sel.data, [{ id: 7, issueId: 1 }], "untracked local data survives reconnect");
});

test("writeLocal commits across multiple local tables coherently (edit + remove too)", () => {
  const { store } = setup();
  const sel = store.query.selection.materialize();
  const drafts = store.query.draft.materialize();
  store.writeLocal((tx) => {
    tx.add("selection", { id: 1, issueId: 9 });
    tx.add("draft", { id: 1, text: "hi" });
  });
  assert.deepEqual(sel.data, [{ id: 1, issueId: 9 }]);
  assert.deepEqual(drafts.data, [{ id: 1, text: "hi" }]);

  store.writeLocal((tx) => tx.edit("draft", { id: 1, text: "hi" }, { id: 1, text: "bye" }));
  assert.deepEqual(drafts.data, [{ id: 1, text: "bye" }]);

  store.writeLocal((tx) => tx.remove("selection", { id: 1, issueId: 9 }));
  assert.deepEqual(sel.data, []);
});
