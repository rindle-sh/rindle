import { test } from "node:test";
import assert from "node:assert/strict";

import type { Mutation } from "@rindle/client";
import { NormalizedSync, type NormalizedOp } from "../src/sync.ts";

// The client column UNION for projection (PROJECTION-SUPPORT-DESIGN.md §4). A projected query
// streams rows positional over its OWN columns; `NormalizedSync` scatters them into the shared
// base-table layout via the registered projection and unions across referencing queries —
// widening on add/edit, narrowing on remove/drop. A `'*'` query contributes every column, so a
// row it holds stays full (no Absent ever introduced, §7).
//
// `undefined` is the Absent token in a positional union row (§5.3); the wasm `mutate` ABI maps
// it to `OwnedValue::Absent`. Mutation rows below therefore carry `undefined` for absent cells.

// issue(id, title, priority): pk [0], width 3.
const pk = { issue: [0] };
const widths = { issue: 3 };

// Projected wire rows are positional over the query's OWN columns:
//   q1 select [id, title]    → cols [0, 1]
//   q2 select [id, priority] → cols [0, 2]
const Q1 = [0, 1];
const Q2 = [0, 2];

test("two projections of one row union their columns (widen)", () => {
  const sync = new NormalizedSync(pk, widths);
  sync.registerProjection(1, "issue", Q1);
  sync.registerProjection(2, "issue", Q2);

  // q1 brings {id, title}: enters as a partial union row (priority Absent).
  const m1: Mutation[] = sync.applyBatch(1, [{ table: "issue", op: "add", row: [1, "a"] }]);
  assert.deepStrictEqual(m1, [{ op: "add", table: "issue", row: [1, "a", undefined] }]);

  // q2 brings {id, priority}: widens the SAME row — a single edit at the wider union.
  const m2: Mutation[] = sync.applyBatch(2, [{ table: "issue", op: "add", row: [1, 5] }]);
  assert.deepStrictEqual(m2, [
    { op: "edit", table: "issue", old: [1, "a", undefined], new: [1, "a", 5] },
  ]);
  assert.strictEqual(sync.refCount("issue", [1]), 2);
  assert.strictEqual(sync.baseSize(), 1);
});

test("dropping a query narrows the shared union (the client `remove-keys`)", () => {
  const sync = new NormalizedSync(pk, widths);
  sync.registerProjection(1, "issue", Q1);
  sync.registerProjection(2, "issue", Q2);
  sync.applyBatch(1, [{ table: "issue", op: "add", row: [1, "a"] }]);
  sync.applyBatch(2, [{ table: "issue", op: "add", row: [1, 5] }]); // union now [1,"a",5]

  // Drop q2: priority is no longer contributed → reset to Absent (a narrowing edit).
  const m: Mutation[] = sync.dropQuery(2);
  assert.deepStrictEqual(m, [
    { op: "edit", table: "issue", old: [1, "a", 5], new: [1, "a", undefined] },
  ]);
  assert.strictEqual(sync.refCount("issue", [1]), 1);

  // Drop q1: last reference → the row leaves entirely.
  const m2: Mutation[] = sync.dropQuery(1);
  assert.deepStrictEqual(m2, [{ op: "remove", table: "issue", row: [1, "a", undefined] }]);
  assert.strictEqual(sync.baseSize(), 0);
});

test("a query's remove of a still-referenced row narrows instead of GCing", () => {
  const sync = new NormalizedSync(pk, widths);
  sync.registerProjection(1, "issue", Q1);
  sync.registerProjection(2, "issue", Q2);
  sync.applyBatch(1, [{ table: "issue", op: "add", row: [1, "a"] }]);
  sync.applyBatch(2, [{ table: "issue", op: "add", row: [1, 5] }]); // union [1,"a",5]

  // q2 removes the row (e.g. it fell out of q2's filter) but q1 still references it.
  const m: Mutation[] = sync.applyBatch(2, [{ table: "issue", op: "remove", row: [1, 5] }]);
  assert.deepStrictEqual(m, [
    { op: "edit", table: "issue", old: [1, "a", 5], new: [1, "a", undefined] },
  ]);
  assert.strictEqual(sync.refCount("issue", [1]), 1);
});

test("a projected edit column-merges only its own columns", () => {
  const sync = new NormalizedSync(pk, widths);
  sync.registerProjection(1, "issue", Q1);
  sync.registerProjection(2, "issue", Q2);
  sync.applyBatch(1, [{ table: "issue", op: "add", row: [1, "a"] }]);
  sync.applyBatch(2, [{ table: "issue", op: "add", row: [1, 5] }]); // union [1,"a",5]

  // q2 edits priority 5→9: only column 2 changes; title is untouched.
  const m: Mutation[] = sync.applyBatch(2, [{ table: "issue", op: "edit", old: [1, 5], new: [1, 9] }]);
  assert.deepStrictEqual(m, [{ op: "edit", table: "issue", old: [1, "a", 5], new: [1, "a", 9] }]);
});

test("a '*' query keeps a shared row fully present even as a projection drops", () => {
  const sync = new NormalizedSync(pk, widths);
  // qStar is '*': no projection registered → contributes every column.
  sync.registerProjection(1, "issue", Q1);

  // '*' adds the full row; the projected q1 references it but contributes a subset.
  const mStar: Mutation[] = sync.applyBatch(7, [
    { table: "issue", op: "add", row: [1, "a", 5] } as NormalizedOp,
  ]);
  assert.deepStrictEqual(mStar, [{ op: "add", table: "issue", row: [1, "a", 5] }]);
  // q1's add carries {id,title}; merging Absent priority over the present value is a no-op.
  const m1: Mutation[] = sync.applyBatch(1, [{ table: "issue", op: "add", row: [1, "a"] }]);
  assert.deepStrictEqual(m1, []);

  // Drop q1: a '*' query still holds the row → presence is full → no narrowing.
  const m: Mutation[] = sync.dropQuery(1);
  assert.deepStrictEqual(m, []);
  assert.strictEqual(sync.refCount("issue", [1]), 1);
});

test("an expanded server row drops the column the client doesn't have (-1 sentinel)", () => {
  // The server's `issue` is wider than the client: it streams [id, title, priority, assignee],
  // but the client only has [id, title, priority]. The backend maps `assignee` to the DROP
  // sentinel -1; scatter discards that cell and stores the client's full-width row. The query
  // contributes every REAL client column, so the row is fully present (no Absent) — like '*'.
  const sync = new NormalizedSync(pk, widths);
  sync.registerProjection(1, "issue", [0, 1, 2, -1]); // id, title, priority, <drop assignee>

  const m: Mutation[] = sync.applyBatch(1, [{ table: "issue", op: "add", row: [1, "a", 5, "bob"] }]);
  assert.deepStrictEqual(m, [{ op: "add", table: "issue", row: [1, "a", 5] }], "assignee dropped, row at full width");

  // A '*'-equivalent: dropping the only query GCs the row outright (no narrowing edit first,
  // because the query already contributed every real column).
  const m2: Mutation[] = sync.dropQuery(1);
  assert.deepStrictEqual(m2, [{ op: "remove", table: "issue", row: [1, "a", 5] }]);
  assert.strictEqual(sync.baseSize(), 0);
});

test("an extra server column in the MIDDLE scatters the rest by position", () => {
  // assignee lands between id and title on the wire: [id, assignee, title, priority]. The map
  // [0, -1, 1, 2] drops cell 1 and places the rest at their base ColIds.
  const sync = new NormalizedSync(pk, widths);
  sync.registerProjection(1, "issue", [0, -1, 1, 2]);

  const m: Mutation[] = sync.applyBatch(1, [{ table: "issue", op: "add", row: [1, "bob", "a", 5] }]);
  assert.deepStrictEqual(m, [{ op: "add", table: "issue", row: [1, "a", 5] }]);
});

test("unregisterProjection clears a stale expanded map (expand→contract across epochs)", () => {
  // A subscription whose server EXPANDED with a mid-row column registers [0,-1,1,2]. If the server
  // later CONTRACTS back to the exact schema, the backend reverts the query to '*' via
  // unregisterProjection — otherwise the now-3-cell exact rows would scatter through the stale
  // 4-wide `-1` map and corrupt (title lost, priority shifted into its slot: [1,5,undefined]).
  const sync = new NormalizedSync(pk, widths);
  sync.registerProjection(1, "issue", [0, -1, 1, 2]);
  sync.unregisterProjection(1, "issue"); // server contracted → '*' (verbatim full-width rows)

  const m: Mutation[] = sync.applyBatch(1, [{ table: "issue", op: "add", row: [1, "a", 5] }]);
  assert.deepStrictEqual(m, [{ op: "add", table: "issue", row: [1, "a", 5] }], "exact row stored verbatim, not corrupted");
  assert.strictEqual(sync.refCount("issue", [1]), 1);
});

test("widening forwards the newer value for a shared column (CRIT#3, projected)", () => {
  const sync = new NormalizedSync(pk, widths);
  sync.registerProjection(1, "issue", Q1); // {id, title}
  sync.registerProjection(2, "issue", Q1); // {id, title} as well

  sync.applyBatch(1, [{ table: "issue", op: "add", row: [1, "a"] }]); // base [1,"a",_]
  // q2 references the same row but with a NEWER title — must forward an edit (not be swallowed).
  const m: Mutation[] = sync.applyBatch(2, [{ table: "issue", op: "add", row: [1, "b"] }]);
  assert.deepStrictEqual(m, [
    { op: "edit", table: "issue", old: [1, "a", undefined], new: [1, "b", undefined] },
  ]);
});

test("the PK at a shifted (non-zero) wire index still keys/refcounts correctly", () => {
  // The server's `id` is NOT first on the wire: [assignee, id, title] → map [-1, 0, 1] (assignee
  // dropped, id→0, title→1). The PK cell arrives at wire index 1; after scatter it must sit at base
  // index 0 so the refcount keys on the real id (not the dropped assignee, and not Absent).
  const sync = new NormalizedSync(pk, widths);
  sync.registerProjection(1, "issue", [-1, 0, 1]);

  const m: Mutation[] = sync.applyBatch(1, [{ table: "issue", op: "add", row: ["bob", 7, "a"] }]);
  assert.deepStrictEqual(m, [{ op: "add", table: "issue", row: [7, "a", undefined] }], "id gathered to base 0");
  assert.strictEqual(sync.refCount("issue", [7]), 1, "keyed by the real id");
  // A remove keyed by the same (shifted) layout finds and GCs the row.
  const m2: Mutation[] = sync.applyBatch(1, [{ table: "issue", op: "remove", row: ["x", 7, "a"] }]);
  assert.deepStrictEqual(m2, [{ op: "remove", table: "issue", row: [7, "a", undefined] }]);
  assert.strictEqual(sync.baseSize(), 0);
});

test("an expanded '*' query keeps a shared row full as a projection drops (presence skips -1)", () => {
  // qExp is a '*' query against a WIDER server: [id, title, priority, assignee] → map [0,1,2,-1].
  // It must contribute every REAL client column ({0,1,2}, the -1 skipped), so the shared row stays
  // full when a co-referencing projection is dropped — identical to a true '*' (no narrowing).
  const sync = new NormalizedSync(pk, widths);
  sync.registerProjection(7, "issue", [0, 1, 2, -1]); // expanded '*'
  sync.registerProjection(1, "issue", Q1); // projection {id, title}

  sync.applyBatch(7, [{ table: "issue", op: "add", row: [1, "a", 5, "bob"] }]); // full [1,"a",5]
  const m1: Mutation[] = sync.applyBatch(1, [{ table: "issue", op: "add", row: [1, "a"] }]);
  assert.deepStrictEqual(m1, [], "projection adds no new columns over the already-full row");

  // Drop the projection: the expanded '*' still holds it → full presence → NO narrowing edit.
  const m: Mutation[] = sync.dropQuery(1);
  assert.deepStrictEqual(m, [], "the expanded '*' contributes all columns → priority not narrowed to Absent");
  assert.strictEqual(sync.refCount("issue", [1]), 1);
});

// --- property test: scatter is a name-GATHER over any project/expand/skew column map ----------
// Deterministic (seeded) randomized coverage of the positional heart — the TS analog of fuzzing
// (the Rust differential fuzzer uses one shared schema, so it can't model a client narrower than
// the server). Invariant: for any client schema and any server hello, the row the sync layer
// stores places each client column's value from the server cell of the SAME NAME, and leaves a
// client column the server didn't send Absent. This covers projection (drops), expansion (extras
// at any position — leading/middle/trailing/multiple), and a PK at a shifted wire index at once.
function mulberry32(seed: number): () => number {
  return () => {
    seed = (seed + 0x6d2b79f5) | 0;
    let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

test("property: scatter gathers each client column by name (random project/expand maps)", () => {
  const rand = mulberry32(0x5ca77e2);
  const pool = ["id", "p", "q", "r", "s", "t", "u"]; // client column-name pool (unique)
  const extraPool = ["x", "y", "z", "w"]; //            server-only (expanded) names, disjoint
  for (let iter = 0; iter < 3000; iter++) {
    const client = pool.filter(() => rand() < 0.6);
    if (client.length < 1) continue;
    const pkIdx = Math.floor(rand() * client.length);

    // A realistic accepted hello: keep a subsequence of client (always the PK — the server forces
    // it into every projection), then splice server-only extras in at random positions.
    const server = client.filter((_, j) => j === pkIdx || rand() < 0.7);
    for (const e of extraPool) if (rand() < 0.4) server.splice(Math.floor(rand() * (server.length + 1)), 0, e);

    // The backend's name→ColId map: `indexOf` yields -1 for a server-only column (the DROP sentinel).
    const cols = server.map((name) => client.indexOf(name));
    const sync = new NormalizedSync({ t: [pkIdx] }, { t: client.length });
    sync.registerProjection(1, "t", cols);

    const row = server.map((_, i) => `v${i}`); // a distinct, decodable value per wire position
    const muts = sync.applyBatch(1, [{ table: "t", op: "add", row } as NormalizedOp]);

    // Oracle: client position j takes the server cell whose NAME equals client[j], else Absent.
    const expected = client.map((cn) => {
      const i = server.indexOf(cn);
      return i >= 0 ? `v${i}` : undefined;
    });
    assert.deepStrictEqual(
      muts,
      [{ op: "add", table: "t", row: expected }],
      `iter ${iter}: client=[${client}] pk=${pkIdx} server=[${server}] cols=[${cols}]`,
    );
  }
});
