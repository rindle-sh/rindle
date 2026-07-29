// Offline end-to-end smoke of the REAL tiers — no network, no Wikimedia:
//
//   spawn the PAIR (write-master + follower; schema = the master's base `tables`)  →  start the API +
//   ingester tier (writes → master, reads → follower)  →  drive it with a stock client
//
// Asserts the pinned "just edited" board hydrates over the lease+ws path (top pages by recency, each
// carrying its newest edits), that the MIRROR board (most-recent editors — the same edit stream under
// a different correlation, with a `where edits > 1` filter) hydrates and respects the filter, that a
// live edit reorders the page board, that TWO subscribers SHARE one materialization (the dedup the
// daemon gives us), and that a windowed PRUNE (deleting old edits → decrementing/removing pages AND
// editors) propagates as removals through the live pipelines.
//
//   node --conditions=@rindle/source test/smoke.e2e.ts

import assert from "node:assert/strict";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { createRindleClient } from "@rindle/optimistic";

import { startPair } from "../src/daemon.ts";
import { queries, schema } from "../src/schema.ts";
import type { EditorRow, PageRow, WikiEvent } from "../src/schema.ts";
import { startTinyMachine } from "../src/tier.ts";

let rev = 0;
/** Build one change event for `title` edited by `user` at edit-time `ts`. */
function ev(title: string, user: string, ts: number, delta = 100): WikiEvent {
  rev++;
  return {
    page: { id: `enwiki:${title}`, wiki: "enwiki", title, url: `https://en.wikipedia.org/wiki/${title}`, ts, user },
    edit: { id: `r${rev}`, user, comment: `edit ${rev}`, ts, delta, bot: 0 },
  };
}

/** Wait for the first view snapshot that satisfies `ready`. */
function waitFor<T>(
  view: { subscribe(cb: (data: readonly unknown[]) => void): () => void },
  ready: (rows: readonly T[]) => boolean,
  label: string,
): Promise<readonly T[]> {
  return new Promise((resolve, reject) => {
    const t = setTimeout(() => reject(new Error(`timed out waiting for ${label}`)), 8000);
    const unsub = view.subscribe((data) => {
      const rows = data as readonly T[];
      if (ready(rows)) {
        clearTimeout(t);
        unsub();
        resolve(rows);
      }
    });
  });
}

const dataDir = mkdtempSync(join(tmpdir(), "wiki-smoke-"));
// The ONE topology (design 214): write-master + follower on ephemeral loopback ports. Writes (ingest/
// prune) go to the master; reads/subscriptions come off the follower.
const pair = await startPair({ dataDir, nWorkers: 2, httpPort: 0, wsPort: 0, masterHttpPort: 0, masterWsPort: 0 });
process.on("exit", () => pair.close());
const machine = await startTinyMachine({
  daemonUrl: pair.httpUrl,
  daemonWsUrl: pair.wsUrl,
  masterUrl: pair.masterUrl,
  sourceName: "synthetic",
});
const apiUrl = `http://127.0.0.1:${machine.apiPort}`;

// Subscribe FIRST (empty), then ingest — the writes drive the progress frames that release the
// stream, exactly like the live demo.
const a = await createRindleClient({ schema, mutators: {}, api: { url: apiUrl }, daemon: { wsUrl: pair.wsUrl } });
const viewA = a.store.materialize(queries.latest());

// Ava edits P-top 3×, Ben edits P-mid 2×, Cyd edits P-low 1× — so the page board AND the editor
// board have a clear ranking, and Cyd (a one-off editor) is below the editor board's `edits > 1`.
await machine.ingest([
  ev("P-top", "Ava", 100), ev("P-top", "Ava", 110), ev("P-top", "Ava", 120), // Ava: 3 edits
  ev("P-mid", "Ben", 105), ev("P-mid", "Ben", 115), // Ben: 2 edits
  ev("P-low", "Cyd", 108), // Cyd: 1 edit
]);

// The "just edited" board ranks by recency (last_ts). The data is staged so the newest-edited page
// (P-top, last edit ts 120) leads, then P-mid (115), then P-low (108).
const rows = await waitFor<PageRow>(viewA, (r) => r.length === 3, "the just-edited snapshot");
assert.equal(rows[0].title, "P-top", "most-recently-edited page first");
assert.equal(rows[0].edits, 3, "P-top has 3 edits (count still denormalized, just not the sort key)");
assert.equal(rows[1].title, "P-mid");
assert.equal(rows[2].title, "P-low");
assert.ok(rows[0].edits_recent && rows[0].edits_recent.length === 3, "top page carries its 3 edits nested");
assert.equal(rows[0].edits_recent![0].ts, 120, "newest edit first");
console.log(`[smoke] OK — just-edited over the 3 tiers: ${rows.length} pages, newest "${rows[0].title}" (ts ${rows[0].last_ts})`);

// The MIRROR board: most-recent editors (the same edit stream, correlated editor.name ← edit.user),
// ranked by recency with a `where edits > 1` filter. Ava (last 120) and Ben (last 115) qualify; Cyd
// (1 edit) is filtered out — proving both the second materialization and a Filter riding under the Take.
const viewEditors = a.store.materialize(queries.recentEditors());
const editorRows = await waitFor<EditorRow>(viewEditors, (r) => r.length === 2, "the editors snapshot");
assert.equal(editorRows[0].name, "Ava", "most-recent qualifying editor first");
assert.equal(editorRows[0].edits, 3, "Ava has 3 edits");
assert.equal(editorRows[1].name, "Ben");
assert.ok(!editorRows.some((e) => e.name === "Cyd"), "one-off editor Cyd is filtered by `where edits > 1`");
assert.ok(editorRows[0].edits_recent && editorRows[0].edits_recent.length === 3, "editor carries their edits nested");
assert.equal(editorRows[0].edits_recent![0].ts, 120, "newest edit first");
console.log(`[smoke] OK — recent-editors board: ${editorRows.length} editors (Cyd filtered), newest "${editorRows[0].name}"`);

// A second reader of the SAME query shares the one pinned materialization (the dedup story).
const b = await createRindleClient({ schema, mutators: {}, api: { url: apiUrl }, daemon: { wsUrl: pair.wsUrl } });
const viewB = b.store.materialize(queries.latest());
await waitFor<PageRow>(viewB, (r) => r.length === 3, "the second reader's snapshot");
const metrics = await (await fetch(`${apiUrl}/metrics`)).json();
// latest + recentEditors are pinned (2); each of the two clients also has its own lmid system query.
// The two readers of `latest` do NOT add a second `latest` pipeline, and client a watching
// `recentEditors` reuses the pinned one — that is the dedup.
assert.ok(metrics.materializations <= 5, `readers share the pinned pipelines: ${JSON.stringify(metrics)}`);
console.log(`[smoke] OK — two readers, materializations=${metrics.materializations} (latest+recentEditors pinned, shared)`);

// The footprint badge: vCPU count + the app's RSS against the VM size are present and sane. (CPU% is
// null until the sampler's first interval; viewers is 0 here — the smoke clients connect to the
// daemon ws directly, not through the tier's proxy where deployed tabs are counted.)
assert.ok(metrics.vcpus >= 1, `vcpus reported: ${JSON.stringify(metrics)}`);
assert.ok(metrics.memUsedBytes > 0 && metrics.memUsedBytes <= metrics.memLimitBytes, `memory sane: ${JSON.stringify(metrics)}`);
assert.ok(metrics.cpuPercent === null || (metrics.cpuPercent >= 0 && metrics.cpuPercent <= 100), `cpu% in range: ${JSON.stringify(metrics)}`);
assert.equal(typeof metrics.viewers, "number", `viewers is a number: ${JSON.stringify(metrics)}`);
console.log(`[smoke] OK — footprint badge: ${metrics.vcpus} vCPU, RSS ${(metrics.memUsedBytes / 1e6).toFixed(0)}MB / ${(metrics.memLimitBytes / 1e6).toFixed(0)}MB, viewers=${metrics.viewers}`);

// A live edit reorders the recency board: three fresh edits to P-low (newest ts 220) lift it to the
// top of "just edited", and its denormalized count rises to 4 in lockstep.
const reordered = waitFor<PageRow>(viewA, (r) => r.length === 3 && r[0].title === "P-low", "the reorder");
await machine.ingest([ev("P-low", "Cyd", 200), ev("P-low", "Cyd", 210), ev("P-low", "Cyd", 220)]);
const afterReorder = await reordered;
assert.equal(afterReorder[0].edits, 4, "P-low climbed with 4 edits");
console.log("[smoke] OK — a fresh edit lifted P-low to the top of just-edited");

// A windowed prune drops edits older than ts 150 → P-top/P-mid empty out (removed) and so do their
// editors Ava/Ben; P-low and Cyd lose only their oldest edit. Exercises removals propagating
// through BOTH live pipelines.
const pruned = waitFor<PageRow>(viewA, (r) => r.length === 1 && r[0].title === "P-low", "the page prune");
const editorsPruned = waitFor<EditorRow>(viewEditors, (r) => r.length === 1 && r[0].name === "Cyd", "the editor prune");
await machine.prune(150);
const afterPrune = await pruned;
assert.equal(afterPrune[0].edits, 3, "P-low kept its 3 in-window edits");
assert.ok(afterPrune[0].edits_recent!.every((e) => e.ts >= 150), "pruned edits are gone from the nested list");
const afterEditorPrune = await editorsPruned;
assert.equal(afterEditorPrune[0].edits, 3, "Cyd kept their 3 in-window edits");
assert.ok(afterEditorPrune[0].edits_recent!.every((e) => e.ts >= 150), "pruned edits are gone from the editor's nested list");
console.log("[smoke] OK — windowed prune removed stale pages, editors + edits through both live pipelines");

a.close();
b.close();
await machine.close();
pair.close();
console.log("[smoke] PASS");
process.exit(0);
