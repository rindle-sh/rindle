// Tests for RINDLE-REALTIME-QUERY-ENABLEMENT-DESIGN.md §3.2 ("the instrumentation") — Slice B
// capture, completed by Slice H-ii: pure engine read/write CAPTURE, no routing logic (H-iii's
// prove-or-slow-path router consumes this capture). Covers:
//
//   - pk-granular write capture (§3.2 #1): `add`/`edit`/`remove` (`backend.ts`'s `trackingTx`)
//     capture a `(table, pk, row)` {@link WriteRecord} per write, collapsing a same-pk write within
//     one invocation to its final image. `touched` — the PRE-EXISTING table-granular `Set<string>`
//     the pending axis reads (§7.2) — is derived from the write-set's table keys, never separately
//     populated, so it cannot drift from it (the back-compat pin this slice must not break).
//   - recording read mode (§3.2 #2): a SIBLING of the folded read trap (`FoldReadError`) — armed on
//     the ordinary (non-folded) `invoke` path, `tx.get`/`tx.row` record `(table, pk, outcome)` and
//     `tx.query` records its resolved AST, with no change to what the mutator itself observes.
//   - edit pre-images + the H-ii coalescing matrix (see {@link WriteRecord}): an edit captures
//     `oldRow` like a remove; edit-after-add collapses to an add (no pre-image), edit-after-edit
//     keeps the FIRST pre-image, remove-after-edit keeps the original, re-inserts drop it.
//   - keyed-writer probe recording (H-ii, §3.2 #3): the pre-existence probe `update`/`upsert`/
//     `insertIgnore`/`delete` branch on records through the same ReadRecord path — the silent-drop
//     shape (a room-side update no-op) is only refusable if the proof can see the probe.
//   - the folded trap path is untouched: `FoldReadError` still fires exactly as before, a FOLDED
//     write-only mutator still captures its write-set normally with an EMPTY read-log (recording
//     never arms on the trapped path), and keyed writers stay fold-legal.
//
// Requires the wasm artifact (packages/wasm/build.sh) — same as the other suites.

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
  type OptimisticSource,
  type ProgressFrame,
  type QueryId,
  type WireValue,
} from "@rindle/wasm";
import {
  createOptimisticStore,
  type ClientRegistry,
  type MutationTx,
  type QueryArg,
  type WriteRecord,
} from "../src/index.ts";

await initWasm();

const issue = table("issue").columns({ id: number(), owner: number(), score: number() }).primaryKey("id");
const comment = table("comment").columns({ id: number(), issueID: number(), body: string() }).primaryKey("id");
const schema = createSchema({ tables: [issue, comment] });
const qb = newQueryBuilder(schema);

/** A source that never confirms — these tests only exercise the local predicted apply and
 *  `__inspect()`'s read-only capture snapshot; no frame ever releases a pending entry, so
 *  `pendingMutations` accumulates in invocation order across a test. */
class NullSource implements OptimisticSource {
  registerQuery(): void {}
  unregisterQuery(): void {}
  pushMutation(): Promise<void> {
    return Promise.resolve();
  }
  onNormalized(_h: (qid: QueryId, ev: NormalizedEvent) => void): void {}
  onProgress(_h: (frame: ProgressFrame) => void): void {}
}

const registry = {
  seedIssue: (tx: MutationTx, a: { id: number; owner: number; score: number }) =>
    tx.add("issue", [a.id, a.owner, a.score]),
  seedComment: (tx: MutationTx, a: { id: number; issueID: number; body: string }) =>
    tx.add("comment", [a.id, a.issueID, a.body]),

  /** One invocation: add a fresh issue, edit an existing one, remove an existing comment — the
   *  add + edit + remove shape spanning two tables, the DoD's write-capture subject. */
  mixedWrites: (
    tx: MutationTx,
    a: { addIssue: WireValue[]; editOld: WireValue[]; editNew: WireValue[]; removeComment: WireValue[] },
  ) => {
    tx.add("issue", a.addIssue);
    tx.edit("issue", a.editOld, a.editNew);
    tx.remove("comment", a.removeComment);
  },

  /** Two point reads (one present, one absent) and one query — nothing written. */
  readsOnly: (tx: MutationTx, a: { presentId: number; absentId: number; query: QueryArg }) => {
    tx.row("issue", { id: a.presentId });
    tx.row("issue", { id: a.absentId });
    tx.query(a.query);
  },

  /** G-iii coalescing probes (see {@link WriteRecord}): remove-after-add / remove-then-re-insert
   *  of ONE pk inside one invocation. */
  addThenRemove: (tx: MutationTx, a: { row: WireValue[] }) => {
    tx.add("issue", a.row);
    tx.remove("issue", a.row);
  },
  removeThenAdd: (tx: MutationTx, a: { oldRow: WireValue[]; newRow: WireValue[] }) => {
    tx.remove("issue", a.oldRow);
    tx.add("issue", a.newRow);
  },

  /** A positional read on its own — the folded read-trap canary. */
  getOnly: (tx: MutationTx, a: { id: number }) => {
    tx.get("issue", [a.id]);
  },

  /** H-ii keyed-writer probes (§3.2 #3): one keyed writer per mutator, both branches driven by args. */
  keyedUpdate: (tx: MutationTx, a: { id: number; score: number }) => tx.update("issue", { id: a.id, score: a.score }),
  keyedUpsert: (tx: MutationTx, a: { id: number; owner: number; score: number }) => tx.upsert("issue", a),
  keyedInsertIgnore: (tx: MutationTx, a: { id: number; owner: number; score: number }) => tx.insertIgnore("issue", a),
  keyedDelete: (tx: MutationTx, a: { id: number }) => tx.delete("issue", { id: a.id }),

  /** H-ii edit pre-image + coalescing-matrix probes (see {@link WriteRecord}). */
  editOnce: (tx: MutationTx, a: { old: WireValue[]; new: WireValue[] }) => tx.edit("issue", a.old, a.new),
  addThenEdit: (tx: MutationTx, a: { row: WireValue[]; next: WireValue[] }) => {
    tx.add("issue", a.row);
    tx.edit("issue", a.row, a.next);
  },
  editThenEdit: (tx: MutationTx, a: { a: WireValue[]; b: WireValue[]; c: WireValue[] }) => {
    tx.edit("issue", a.a, a.b);
    tx.edit("issue", a.b, a.c);
  },
  editThenRemove: (tx: MutationTx, a: { old: WireValue[]; new: WireValue[] }) => {
    tx.edit("issue", a.old, a.new);
    tx.remove("issue", a.new);
  },
} satisfies ClientRegistry;

function setup() {
  return createOptimisticStore(schema, new NullSource(), registry, { clientID: "c1" });
}

/** Canonical sort so a Map's insertion-order flattening doesn't couple a test to it. */
function sortWrites(ws: WriteRecord[]): WriteRecord[] {
  return [...ws].sort((a, b) =>
    `${a.table}:${JSON.stringify(a.pk)}`.localeCompare(`${b.table}:${JSON.stringify(b.pk)}`),
  );
}

// --- pk-granular write capture (§3.2 #1) --------------------------------------------

test("captures a pk-granular write-set across add + edit + remove, spanning two tables", () => {
  const { backend } = setup();
  backend.invoke("seedIssue", { id: 1, owner: 10, score: 100 });
  backend.invoke("seedComment", { id: 50, issueID: 1, body: "x" });

  backend.invoke("mixedWrites", {
    addIssue: [2, 20, 200],
    editOld: [1, 10, 100],
    editNew: [1, 10, 150],
    removeComment: [50, 1, "x"],
  });

  const { pending } = backend.__inspect();
  const entry = pending.at(-1);
  assert.ok(entry, "expected a pending entry for mixedWrites");

  assert.deepEqual(
    sortWrites(entry!.writes),
    sortWrites([
      { table: "issue", pk: [2], row: [2, 20, 200] },
      // An edit carries its PRE-IMAGE too (H-ii): the txn-visible row read via `tx.get`
      // immediately before it staged — H-iii's join-key no-change input, and the row the
      // engine routes the Edit by (H-i).
      { table: "issue", pk: [1], row: [1, 10, 150], oldRow: [1, 10, 100] },
      // A remove keeps `row: undefined` as its marker and carries the PRE-IMAGE read via `tx.get`
      // immediately before it staged (G-iii, the §7.3 tombstone's input).
      { table: "comment", pk: [50], row: undefined, oldRow: [50, 1, "x"] },
    ]),
  );
  // `touched` — the pre-existing table-granular Set the pending axis reads (§7.2) — is exactly the
  // write-set's table keys: the back-compat pin this slice must not break.
  assert.deepEqual([...entry!.tables].sort(), ["comment", "issue"]);
  assert.deepEqual(new Set(entry!.tables), new Set(entry!.writes.map((w) => w.table)));
});

test("remove pre-image coalescing (G-iii): remove-after-add keeps the txn-visible pre-image; a re-insert drops it", () => {
  const { backend } = setup();

  // Remove AFTER an add of the same pk, one invocation: last-write-wins collapses the record to
  // the remove; its pre-image is the row `tx.get` saw immediately before the remove staged —
  // read-your-writes, so the just-added transient row shows through.
  backend.invoke("addThenRemove", { row: [7, 70, 700] });
  let entry = backend.__inspect().pending.at(-1)!;
  assert.deepEqual(entry.writes, [{ table: "issue", pk: [7], row: undefined, oldRow: [7, 70, 700] }]);

  // Remove then RE-INSERT of the same pk: the add replaces the record wholesale — `oldRow` is
  // dropped (a presence hold-back pins `row`; the pre-image belongs to removes only).
  backend.invoke("seedIssue", { id: 8, owner: 80, score: 800 });
  backend.invoke("removeThenAdd", { oldRow: [8, 80, 800], newRow: [8, 81, 810] });
  entry = backend.__inspect().pending.at(-1)!;
  assert.deepEqual(entry.writes, [{ table: "issue", pk: [8], row: [8, 81, 810] }]);
});

test("a write-only invocation's read-log is empty", () => {
  const { backend } = setup();
  backend.invoke("seedIssue", { id: 1, owner: 10, score: 100 });
  const entry = backend.__inspect().pending.at(-1)!;
  assert.deepEqual(entry.reads, { reads: [], queries: [] });
});

// --- recording read mode (§3.2 #2) --------------------------------------------------

test("recording mode captures point-read outcomes and the resolved query AST", () => {
  const { backend } = setup();
  backend.invoke("seedIssue", { id: 1, owner: 10, score: 100 });

  const query = qb.issue.where.owner(10);
  backend.invoke("readsOnly", { presentId: 1, absentId: 999, query });

  const entry = backend.__inspect().pending.at(-1);
  assert.ok(entry, "expected a pending entry for readsOnly");
  assert.deepEqual(entry!.reads.reads, [
    { table: "issue", pk: [1], outcome: "present" },
    { table: "issue", pk: [999], outcome: "absent" },
  ]);
  assert.equal(entry!.reads.queries.length, 1);
  assert.deepEqual(entry!.reads.queries[0], query.ast());

  // A read-only invocation writes nothing: `touched` and the write-set both stay empty.
  assert.deepEqual(entry!.tables, []);
  assert.deepEqual(entry!.writes, []);
});

// --- the folded trap path is untouched (regression) ---------------------------------

test("the folded read trap still refuses tx.query, byte-for-byte as before", () => {
  const { backend } = setup();
  backend.invoke("seedIssue", { id: 1, owner: 10, score: 100 });
  assert.throws(
    () =>
      backend.invokeFolded(
        "readsOnly",
        { key: 1 },
        { presentId: 1, absentId: 999, query: qb.issue.orderBy("id", "asc") },
      ),
    /cannot fold|absorbing|reads state/i,
  );
});

test("a folded write-only mutator still captures its write-set, with an EMPTY read-log", () => {
  const { backend } = setup();
  backend.invoke("seedIssue", { id: 1, owner: 10, score: 100 });
  backend.invoke("seedComment", { id: 50, issueID: 1, body: "x" });

  backend.invokeFolded(
    "mixedWrites",
    { key: "x" },
    { addIssue: [2, 20, 200], editOld: [1, 10, 100], editNew: [1, 10, 150], removeComment: [50, 1, "x"] },
  );

  const entry = backend.__inspect().pending.at(-1);
  assert.ok(entry, "expected the folded pending entry");
  assert.equal(entry!.mid, null, "an unflushed fold carries no mid yet");
  assert.deepEqual([...entry!.tables].sort(), ["comment", "issue"]);
  assert.deepEqual(
    sortWrites(entry!.writes),
    sortWrites([
      { table: "issue", pk: [2], row: [2, 20, 200] },
      { table: "issue", pk: [1], row: [1, 10, 150], oldRow: [1, 10, 100] },
      { table: "comment", pk: [50], row: undefined, oldRow: [50, 1, "x"] },
    ]),
  );
  // Recording (§3.2 #2) never arms on the trapped path — the read-log stays empty even though the
  // mutator wrote successfully.
  assert.deepEqual(entry!.reads, { reads: [], queries: [] });
});

// --- keyed-writer probe recording (H-ii, §3.2 #3) ------------------------------------

test("keyed writers record their pre-existence probes, both branches, with staged writes unchanged", () => {
  const { backend } = setup();
  backend.invoke("seedIssue", { id: 1, owner: 10, score: 100 });
  const lastEntry = () => backend.__inspect().pending.at(-1)!;

  // update, present: the probe is a recorded present read; the staged edit carries the pre-image.
  backend.invoke("keyedUpdate", { id: 1, score: 150 });
  let e = lastEntry();
  assert.deepEqual(e.reads.reads, [{ table: "issue", pk: [1], outcome: "present" }]);
  assert.deepEqual(e.writes, [{ table: "issue", pk: [1], row: [1, 10, 150], oldRow: [1, 10, 100] }]);

  // update, absent: THE silent-drop branch (design §3 scout: the update no-ops, the commit
  // "succeeds" with zero effects) — a recorded ABSENT probe, no write, return behavior unchanged.
  backend.invoke("keyedUpdate", { id: 999, score: 5 });
  e = lastEntry();
  assert.deepEqual(e.reads.reads, [{ table: "issue", pk: [999], outcome: "absent" }]);
  assert.deepEqual(e.writes, []);

  // upsert, present branch (edit with pre-image)…
  backend.invoke("keyedUpsert", { id: 1, owner: 10, score: 200 });
  e = lastEntry();
  assert.deepEqual(e.reads.reads, [{ table: "issue", pk: [1], outcome: "present" }]);
  assert.deepEqual(e.writes, [{ table: "issue", pk: [1], row: [1, 10, 200], oldRow: [1, 10, 150] }]);

  // …and absent branch (add, no pre-image).
  backend.invoke("keyedUpsert", { id: 2, owner: 20, score: 20 });
  e = lastEntry();
  assert.deepEqual(e.reads.reads, [{ table: "issue", pk: [2], outcome: "absent" }]);
  assert.deepEqual(e.writes, [{ table: "issue", pk: [2], row: [2, 20, 20] }]);

  // insertIgnore, present (no write) and absent (add).
  backend.invoke("keyedInsertIgnore", { id: 1, owner: 99, score: 999 });
  e = lastEntry();
  assert.deepEqual(e.reads.reads, [{ table: "issue", pk: [1], outcome: "present" }]);
  assert.deepEqual(e.writes, []);
  backend.invoke("keyedInsertIgnore", { id: 3, owner: 30, score: 300 });
  e = lastEntry();
  assert.deepEqual(e.reads.reads, [{ table: "issue", pk: [3], outcome: "absent" }]);
  assert.deepEqual(e.writes, [{ table: "issue", pk: [3], row: [3, 30, 300] }]);

  // delete, present (remove with pre-image) and absent (no write).
  backend.invoke("keyedDelete", { id: 3 });
  e = lastEntry();
  assert.deepEqual(e.reads.reads, [{ table: "issue", pk: [3], outcome: "present" }]);
  assert.deepEqual(e.writes, [{ table: "issue", pk: [3], row: undefined, oldRow: [3, 30, 300] }]);
  backend.invoke("keyedDelete", { id: 999 });
  e = lastEntry();
  assert.deepEqual(e.reads.reads, [{ table: "issue", pk: [999], outcome: "absent" }]);
  assert.deepEqual(e.writes, []);
});

// --- edit pre-images + the H-ii coalescing matrix (see {@link WriteRecord}) ----------

test("edit pre-images: a lone edit captures the txn-visible base; the coalescing matrix pins oldRow", () => {
  const { backend } = setup();
  backend.invoke("seedIssue", { id: 1, owner: 10, score: 100 });

  // A lone edit: pre-image = the row read via `tx.get` immediately BEFORE the edit staged.
  backend.invoke("editOnce", { old: [1, 10, 100], new: [1, 10, 110] });
  let e = backend.__inspect().pending.at(-1)!;
  assert.deepEqual(e.writes, [{ table: "issue", pk: [1], row: [1, 10, 110], oldRow: [1, 10, 100] }]);

  // edit-after-add: collapses to an ADD — post-image only, NO pre-image (the pk did not pre-exist
  // this invocation's base).
  backend.invoke("addThenEdit", { row: [2, 20, 200], next: [2, 20, 250] });
  e = backend.__inspect().pending.at(-1)!;
  assert.deepEqual(e.writes, [{ table: "issue", pk: [2], row: [2, 20, 250] }]);

  // edit-after-edit: the FIRST pre-image (the txn-entry base) survives the chain.
  backend.invoke("editThenEdit", { a: [1, 10, 110], b: [1, 10, 120], c: [1, 10, 130] });
  e = backend.__inspect().pending.at(-1)!;
  assert.deepEqual(e.writes, [{ table: "issue", pk: [1], row: [1, 10, 130], oldRow: [1, 10, 110] }]);

  // remove-after-edit: the ORIGINAL pre-image, never the edited transient.
  backend.invoke("seedIssue", { id: 3, owner: 30, score: 300 });
  backend.invoke("editThenRemove", { old: [3, 30, 300], new: [3, 30, 333] });
  e = backend.__inspect().pending.at(-1)!;
  assert.deepEqual(e.writes, [{ table: "issue", pk: [3], row: undefined, oldRow: [3, 30, 300] }]);

  // add-after-remove (the re-insert dropping oldRow) stays pinned by the G-iii test above.
});

// --- the fold matrix is untouched by H-ii (regression) --------------------------------

test("fold matrix unchanged: tx.get still traps; keyed writers stay fold-legal with nothing recorded", () => {
  const { backend } = setup();
  backend.invoke("seedIssue", { id: 1, owner: 10, score: 100 });

  // tx.get still traps, byte-for-byte (tx.row + tx.query are pinned by the trap test above).
  assert.throws(() => backend.invokeFolded("getOnly", { key: 1 }, { id: 1 }), /cannot fold|reads state/i);

  // A folded KEYED writer: legal before H-ii, legal after — its pre-existence probe (now routed
  // through the recorded read path) and the edit's pre-image capture must neither trap nor record
  // (recording never arms on the trapped path), and the present⇒edit branch is untouched.
  backend.invokeFolded("keyedUpdate", { key: 1 }, { id: 1, score: 555 });
  const e = backend.__inspect().pending.at(-1)!;
  assert.equal(e.mid, null, "an unflushed fold carries no mid yet");
  assert.deepEqual(e.writes, [{ table: "issue", pk: [1], row: [1, 10, 555], oldRow: [1, 10, 100] }]);
  assert.deepEqual(e.reads, { reads: [], queries: [] });
});
