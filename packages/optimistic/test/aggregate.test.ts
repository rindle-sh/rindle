// Optimistic backend, relationship-aggregate DISPLAY path (AGGREGATE-SYNC-DESIGN.md §3,
// ported into `OptimisticBackend` — slice 0 of the optimism work). A `count(comments)` is
// never recomputed on the client (comment rows are never synced); the server ships the
// reduce's `(issueID, count)` output as a synthetic base table `__agg_*`, and the client
// registers it + rewrites the local AST to read it with a plain projected join. These tests
// pin that the OPTIMISTIC store displays the server count exactly — hydrate + server churn —
// WITHOUT any comment row ever entering the footprint. The optimistic overlay (§4–§6) lands
// in later slices; here writes are absent, so the displayed value is the bare server value.
//
// Requires the wasm artifact (packages/wasm/build.sh).

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
import { aggTableName } from "@rindle/normalized";
import { createOptimisticStore, type ClientRegistry } from "../src/index.ts";

await initWasm();

const issue = table("issue").columns({ id: number(), title: string(), score: number() }).primaryKey("id");
const comment = table("comment").columns({ id: number(), issueID: number(), body: string() }).primaryKey("id");
const schema = createSchema({ tables: [issue, comment] });

const qb = newQueryBuilder(schema);
const queries = {
  issuesWithCounts: defineQuery("issuesWithCounts", () => qb.issue.countAs("commentCount", comment, { parent: ["id"], child: ["issueID"] })),
};

/** The synthetic agg table's name — the TS twin of Rust `agg_table_name` over this query's
 *  `count` subquery. Server and client derive it identically, so the rows route. */
const AGG = aggTableName(queries.issuesWithCounts().ast().related![0]);

class TestSource implements OptimisticSource {
  normalized: (qid: QueryId, ev: NormalizedEvent) => void = () => {};
  progress: (frame: ProgressFrame) => void = () => {};
  registered: Array<[QueryId, RemoteQuery]> = [];
  unregistered: QueryId[] = [];
  envelopes: MutationEnvelope[] = [];
  expectedSchema: NormalizedTableSchema[] = [];

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
    this.expectedSchema = tables;
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

function setup() {
  const source = new TestSource();
  const { store, backend, mutate } = createOptimisticStore(schema, source, {} satisfies ClientRegistry, {
    clientID: "c1",
  });
  return { source, store, backend, mutate };
}

test("the synthetic agg table joins the expected-schema set on registration", () => {
  const { source, store } = setup();
  store.materialize(queries.issuesWithCounts());
  assert.ok(
    source.expectedSchema.some((t) => t.name === AGG),
    "the client advertises the synthetic table so the server hello passes validation",
  );
  const agg = source.expectedSchema.find((t) => t.name === AGG)!;
  assert.deepEqual(agg, { name: AGG, columns: ["issueID", "count"], primaryKey: [0] });
});

test("optimistic store displays the server count from the synthetic table (childless → 0)", () => {
  const { source, store } = setup();
  const view = store.materialize(queries.issuesWithCounts());
  assert.deepEqual(view.data, [], "pre-hydrate empty");

  // The server footprint: issue rows + the synthetic (issueID, count) rows. NO comment rows.
  source.snapshot(
    1,
    [
      { table: "issue", op: "add", row: [1, "a", 0] },
      { table: "issue", op: "add", row: [2, "b", 0] },
      { table: "issue", op: "add", row: [3, "c", 0] }, // childless
      { table: AGG, op: "add", row: [1, 2] },
      { table: AGG, op: "add", row: [2, 1] },
    ],
    5,
  );
  assert.deepEqual(view.data, [], "buffered until the release point (cvMin)");

  source.frame(5);
  assert.deepEqual(view.data, [
    { id: 1, title: "a", score: 0, commentCount: 2 },
    { id: 2, title: "b", score: 0, commentCount: 1 },
    { id: 3, title: "c", score: 0, commentCount: 0 }, // empty group → projection identity 0
  ]);
});

test("server count churn (edit / birth / death) flows to the displayed scalar", () => {
  const { source, store } = setup();
  const view = store.materialize(queries.issuesWithCounts());
  source.snapshot(
    1,
    [
      { table: "issue", op: "add", row: [1, "a", 0] },
      { table: "issue", op: "add", row: [2, "b", 0] },
      { table: AGG, op: "add", row: [1, 2] },
      { table: AGG, op: "add", row: [2, 1] },
    ],
    1,
  );
  source.frame(1);
  assert.deepEqual(view.data, [
    { id: 1, title: "a", score: 0, commentCount: 2 },
    { id: 2, title: "b", score: 0, commentCount: 1 },
  ]);

  // issue 1: count 2→3 (edit); issue 3 born with a first comment (group birth); issue 2's
  // last comment removed (group death → back to identity 0).
  source.batch(
    1,
    [
      { table: "issue", op: "add", row: [3, "c", 0] },
      { table: AGG, op: "edit", old: [1, 2], new: [1, 3] },
      { table: AGG, op: "add", row: [3, 1] },
      { table: AGG, op: "remove", row: [2, 1] },
    ],
    2,
  );
  source.frame(2);
  assert.deepEqual(view.data, [
    { id: 1, title: "a", score: 0, commentCount: 3 },
    { id: 2, title: "b", score: 0, commentCount: 0 }, // group death → 0
    { id: 3, title: "c", score: 0, commentCount: 1 },
  ]);
});
