import { test } from "node:test";
import assert from "node:assert/strict";

import {
  createSchema,
  initWasm,
  newQueryBuilder,
  number,
  string,
  table,
  type NormalizedEvent,
  type NormalizedSource,
  type NormalizedTableSchema,
  type QueryId,
  type RemoteQuery,
} from "@rindle/wasm";

import { NormalizedBackend } from "../src/backend.ts";

await initWasm();

const issue = table("issue").columns({ id: number(), title: string() }).primaryKey("id");
const comment = table("comment").columns({ id: number(), issueID: number(), body: string() }).primaryKey("id");
const schema = createSchema({ tables: [issue, comment] });
const qb = newQueryBuilder(schema);

class TestSource implements NormalizedSource {
  normalized: (qid: QueryId, ev: NormalizedEvent) => void = () => {};
  registered: Array<{ qid: QueryId; remote: RemoteQuery }> = [];
  unregistered: QueryId[] = [];
  expected: NormalizedTableSchema[][] = [];

  registerQuery(qid: QueryId, remote: RemoteQuery): void {
    this.registered.push({ qid, remote });
  }
  unregisterQuery(qid: QueryId): void {
    this.unregistered.push(qid);
  }
  mutate(): Promise<void> {
    return Promise.resolve();
  }
  onNormalized(handler: (qid: QueryId, ev: NormalizedEvent) => void): void {
    this.normalized = handler;
  }
  expectClientSchema(schemas: NormalizedTableSchema[]): void {
    this.expected.push(schemas);
  }
}

test("sync-only retain registers and releases aggregate synthetic schemas", () => {
  const source = new TestSource();
  const backend = new NormalizedBackend(schema, source);
  const ast = qb.issue.countAs("commentCount", comment, { parent: ["id"], child: ["issueID"] }).ast();

  backend.retainRemoteQuery(1 as QueryId, { name: "issuesWithCount", args: null }, 1 as QueryId, ast);
  assert.deepStrictEqual(source.registered, [{ qid: 1 as QueryId, remote: { name: "issuesWithCount", args: null } }]);

  const retained = source.expected.at(-1) ?? [];
  const agg = retained.find((t) => t.name.startsWith("__agg_"));
  assert.ok(agg, "the retained coverage AST registered its synthetic aggregate table");
  assert.deepStrictEqual(agg.columns, ["issueID", "count"]);
  assert.deepStrictEqual(agg.primaryKey, [0]);

  backend.releaseRemoteQuery(1 as QueryId);
  assert.deepStrictEqual(source.unregistered, [1 as QueryId]);
  assert.equal((source.expected.at(-1) ?? []).some((t) => t.name.startsWith("__agg_")), false);
});
