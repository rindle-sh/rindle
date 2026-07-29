// Optimistic PARTIAL writes (PROJECTION-SUPPORT-DESIGN.md §8 / OQ-3) through the real wasm
// engine. A cell that is `undefined` on the wire marshals to OwnedValue::Absent (§5.3); the
// WriteTxn resolves it at the staging boundary:
//   - edit: an Absent cell = "leave this column unchanged" — column-wise read-modify-write
//     against the current effective row (so operators only ever see whole rows);
//   - add: Absent = genuinely absent — the row carries presence only for the columns set, and
//     the connection presence predicate hides it from a query that selects a missing column
//     until it is widened.
//
// Driven at the WasmBackend level with RAW positional cells (the Store's object API fills
// missing keys with null; partial writes are the optimistic-rebase / low-level path).
import assert from "node:assert/strict";

import { Store, WasmBackend, table, string, number, createSchema, initWasm } from "@rindle/wasm";

await initWasm();

const issue = table("issue")
  .columns({ id: number(), title: string(), priority: number() })
  .primaryKey("id");
const schema = createSchema({ tables: [issue] });

const ok = (label) => console.log("✓", label);

// --- 1. a partial EDIT leaves the omitted column unchanged (Absent = unchanged) ----------
{
  const backend = new WasmBackend(schema);
  const store = new Store(schema, backend);
  const view = store.query.issue.orderBy("id", "asc").materialize();

  await backend.mutate([{ op: "add", table: "issue", row: [1, "a", 5] }]);
  assert.deepStrictEqual(view.data, [{ id: 1, title: "a", priority: 5 }]);

  // Edit only `priority` (title cell is `undefined` ⇒ Absent ⇒ unchanged).
  await backend.mutate([{ op: "edit", table: "issue", old: [1, "a", 5], new: [1, undefined, 9] }]);
  assert.deepStrictEqual(view.data, [{ id: 1, title: "a", priority: 9 }], "title unchanged, priority updated");
  ok("partial edit: an Absent cell falls through to the current value");
}

// --- 2. a partial ADD is hidden from a query selecting the absent column, shown otherwise -
{
  const backend = new WasmBackend(schema);
  const store = new Store(schema, backend);
  // A query that SELECTS title (so title is required) and one that selects only id+priority.
  const needsTitle = store.query.issue.select("id").select("title").orderBy("id", "asc").materialize();
  const needsPriority = store.query.issue.select("id").select("priority").orderBy("id", "asc").materialize();

  // Add a row WITHOUT title (title cell undefined ⇒ Absent).
  await backend.mutate([{ op: "add", table: "issue", row: [1, undefined, 5] }]);
  assert.deepStrictEqual(needsTitle.data, [], "a row missing the selected `title` stays hidden");
  assert.deepStrictEqual(needsPriority.data, [{ id: 1, priority: 5 }], "a row with the selected cols shows");
  ok("partial add: hidden from a query needing the absent column, visible to one that doesn't");

  // Widen: set title — the row now satisfies the title query too.
  await backend.mutate([{ op: "edit", table: "issue", old: [1, undefined, 5], new: [1, "a", undefined] }]);
  assert.deepStrictEqual(needsTitle.data, [{ id: 1, title: "a" }], "widened in once title is set");
  // The priority query is unaffected and keeps priority (the edit left it Absent = unchanged).
  assert.deepStrictEqual(needsPriority.data, [{ id: 1, priority: 5 }], "priority preserved across the widen");
  ok("widen: setting the missing column admits the row, other columns preserved");
}

console.log("\n✅ optimistic partial writes: Absent-as-unchanged (edit) + partial presence (add)");
