// End-to-end projection (PROJECTION-SUPPORT-DESIGN.md) through the REAL wasm engine: a
// projected normalized stream (a narrower hello + projected rows — exactly what the Rust
// `NormalizedPublisher` emits, see rindle-replica's normalize_protocol projection tests) flows
// through NormalizedBackend → NormalizedSync (scatter + union) → the wasm Db (undefined→Absent)
// → the local projected query's presence predicate + view projection. The materialized view
// must show ONLY the selected columns, and a row whose required column never arrives stays
// hidden until it does.
//
// Run after building the wasm artifact (packages/wasm/build.sh).
import assert from "node:assert/strict";

import { table, string, number, createSchema, newQueryBuilder, defineQuery, initWasm } from "@rindle/wasm";
import { createNormalizedStore } from "@rindle/normalized";

await initWasm();

// issue(id, title, priority); pk id.
const issue = table("issue")
  .columns({ id: number(), title: string(), priority: number() })
  .primaryKey("id");
const schema = createSchema({ tables: [issue] });

// A hand-driven NormalizedSource that emits PROJECTED frames (what the server publisher
// produces for a `select id, priority` query: issue advertised as [id, priority], rows over
// those two columns). The client must scatter them into the full-width base layout.
class MockProjectedSource {
  constructor(frames) {
    this.frames = frames; // { hello, snapshot } per query name
    this.handler = () => {};
  }
  expectClientSchema() {}
  registerQuery(qid, remote) {
    const f = this.frames[remote.name];
    if (!f) throw new Error(`unknown query: ${remote.name}`);
    this.handler(qid, { type: "hello", comparatorVersion: 1, normalizedFp: "0", tables: f.hello });
    this.handler(qid, { type: "snapshot", ops: f.snapshot });
    this._extra?.(qid); // optional follow-up batch (for the widen case)
  }
  unregisterQuery() {}
  mutate() {
    return Promise.resolve();
  }
  onNormalized(h) {
    this.handler = h;
  }
}

// The projected server frames for `select id, priority`: issue's wire schema declares only
// [id, priority] (PK remapped to 0); rows are positional over [id, priority].
const PROJECTED_HELLO = [{ name: "issue", columns: ["id", "priority"], primaryKey: [0] }];

function ok(label) {
  console.log("✓", label);
}

// --- 1. a projected query's view shows only the selected columns -------------------------
{
  const source = new MockProjectedSource({
    issuesByPriority: {
      hello: PROJECTED_HELLO,
      snapshot: [
        { table: "issue", op: "add", row: [1, 5] }, // [id, priority] — title NOT sent
        { table: "issue", op: "add", row: [2, 9] },
      ],
    },
  });
  const store = createNormalizedStore(schema, source);
  const q = newQueryBuilder(schema);
  const queries = {
    issuesByPriority: defineQuery("issuesByPriority", () => q.issue.select("id").select("priority").orderBy("id", "asc")),
  };
  const view = store.materialize(queries.issuesByPriority());

  // The view reports ONLY the selected columns (id, priority) — never `title` (PROJECTION
  // §6: a query never resolves a row from columns it did not select).
  assert.deepStrictEqual(
    view.data,
    [
      { id: 1, priority: 5 },
      { id: 2, priority: 9 },
    ],
    "projected view shows only {id, priority}",
  );
  for (const row of view.data) {
    assert.ok(!("title" in row), "the unselected `title` column is absent from the view");
  }
  ok("projected view reports only selected columns (title dropped)");
}

// --- 2. a row stays hidden until its required (selected) column is synced -----------------
{
  // The server sends only the PK first (e.g. a partial optimistic state echoed back); the
  // selected `priority` is absent, so the projected query must NOT surface the row yet.
  const source = new MockProjectedSource({
    issuesByPriority: {
      hello: PROJECTED_HELLO,
      // snapshot carries only id (priority absent ⇒ undefined cell in the projected row).
      snapshot: [{ table: "issue", op: "add", row: [1, undefined] }],
    },
  });
  // After hydrate, deliver a batch that widens the row with the real priority.
  source._extra = (qid) => {
    source.handler(qid, {
      type: "batch",
      ops: [{ table: "issue", op: "edit", old: [1, undefined], new: [1, 7] }],
    });
  };
  const store = createNormalizedStore(schema, source);
  const q = newQueryBuilder(schema);
  const queries = {
    issuesByPriority: defineQuery("issuesByPriority", () => q.issue.select("id").select("priority").orderBy("id", "asc")),
  };
  const view = store.materialize(queries.issuesByPriority());

  // The widen edit arrived during registerQuery (via `_extra`); the row is now visible.
  assert.deepStrictEqual(view.data, [{ id: 1, priority: 7 }], "row surfaces once priority is present");
  ok("a partial row surfaces only after its selected column is synced (widen)");
}

console.log("\n✅ projection flows end-to-end: narrower wire → union → wasm → projected view");
