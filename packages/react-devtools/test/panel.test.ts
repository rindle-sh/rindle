// Render smoke tests for <RindleDevtools/>. Server-rendered to a string (no jsdom needed) — the
// component's hooks (useState / useSyncExternalStore / useMemo) run, so this proves the panel mounts,
// reads the core's read-model, and surfaces the §4 panes. The data logic itself is pinned in
// `@rindle/devtools`'s core suite; here we only assert the wiring renders.
//
// Uses `createElement` (Node strips TS types but not JSX), like the panel source itself.

import { test } from "node:test";
import assert from "node:assert/strict";

import { createElement as h } from "react";
import { renderToStaticMarkup } from "react-dom/server";

import { DevtoolsCore } from "@rindle/devtools";
import type { DevtoolsBackend, DevtoolsStore, OptimisticInspect, StoreInspect } from "@rindle/devtools";
import { RindleDevtools } from "../src/index.ts";

const store: DevtoolsStore = {
  subscribeChanges: () => () => {},
  subscribeResultType: () => () => {},
  __inspect: (): StoreInspect => ({
    queries: [{ qid: 1, ast: { table: "issue" }, resultType: "complete", rowCount: 2, sample: [{ id: 1 }] }],
  }),
};

const backend: DevtoolsBackend = {
  __inspect: (): OptimisticInspect => ({
    pending: [{ key: "m:1", mid: 1, name: "createIssue", args: { id: 1 }, tables: ["issue"] }],
    confirmedLmid: 0,
    nextMid: 2,
    appliedCv: 0,
    bufferedFrames: 0,
    pendingTables: ["issue"],
  }),
};

const newCore = () => new DevtoolsCore({ store, backend }, { autoFlush: false, pollMs: 0 });

test("renders the launcher when closed", () => {
  const html = renderToStaticMarkup(h(RindleDevtools, { core: newCore() }));
  assert.match(html, /🌊 Rindle/);
  assert.doesNotMatch(html, />Timeline/, "the panel tabs are not shown while closed");
});

test("open panel shows the three panes and the live mutation + query rows", () => {
  const html = renderToStaticMarkup(h(RindleDevtools, { core: newCore(), defaultOpen: true }));
  assert.match(html, /Rindle devtools/);
  assert.match(html, /Timeline/);
  assert.match(html, /Queries/);
  assert.match(html, /Deltas/);
  assert.match(html, /createIssue/, "the pending mutation row renders");
  assert.match(html, /mid 1/);
});

test("with no core attached, prompts to call attachDevtools", () => {
  const html = renderToStaticMarkup(h(RindleDevtools, { defaultOpen: true }));
  assert.match(html, /No Rindle client attached/);
});
