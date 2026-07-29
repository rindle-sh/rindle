// @rindle/react-devtools — a floating, dev-only panel over `@rindle/devtools`
// (DEBUG-TOOLS-BROWSER-DESIGN.md §6.1). It discovers a running client through the global hub (or an
// explicit `core` prop) and renders the §4 panes: the mutation TIMELINE (the fork/rebase loop, with
// the snap-back highlight), the QUERIES inspector, and the live DELTA stream. Pure React + inline
// styles — no CSS import. Like `@rindle/react` it uses `createElement` rather than JSX (Node's test
// runner strips TS types but not JSX). Mount it once near your app root in development:
//
//   {import.meta.env.DEV && createElement(RindleDevtools)}
//
// and attach a client in dev: `import("@rindle/devtools").then(d => d.attachDevtools(app))`.

import { createElement as h, useCallback, useMemo, useState, useSyncExternalStore } from "react";
import type { CSSProperties, ReactNode } from "react";
import {
  getDevtoolsHub,
  type DeltaEntry,
  type DeltaKind,
  type DevtoolsCore,
  type DevtoolsState,
  type MutationState,
  type QueryEntry,
  type ResultType,
  type TimelineEntry,
} from "@rindle/devtools";

export interface RindleDevtoolsProps {
  /** Bind to a specific core. Omit to auto-discover the most recently attached client via the
   *  global hub (the common case). */
  core?: DevtoolsCore;
  /** Open the panel on first mount. Default false (starts as the launcher button). */
  defaultOpen?: boolean;
}

/** The floating Rindle devtools panel. Renders just a launcher until a client is attached via
 *  `attachDevtools(app)`. */
export function RindleDevtools({ core: explicit, defaultOpen = false }: RindleDevtoolsProps): ReactNode {
  const core = useDiscoveredCore(explicit);
  const [open, setOpen] = useState(defaultOpen);
  if (!open) {
    return h(
      "div",
      { style: S.launcher },
      h("button", { type: "button", style: S.launchBtn, onClick: () => setOpen(true), title: "Open Rindle devtools" }, "🌊 Rindle"),
    );
  }
  return h(Panel, { core, onClose: () => setOpen(false) });
}

/** Discover the active core: an explicit prop wins; otherwise track the global hub's latest core,
 *  re-rendering when a client attaches/detaches (so the panel can be mounted before the app). */
function useDiscoveredCore(explicit?: DevtoolsCore): DevtoolsCore | undefined {
  const hub = useMemo(() => getDevtoolsHub(), []);
  const subscribe = useCallback((cb: () => void) => hub.subscribe(cb), [hub]);
  const latest = useCallback(() => hub.cores[hub.cores.length - 1], [hub]);
  const discovered = useSyncExternalStore(subscribe, latest, () => undefined);
  return explicit ?? discovered;
}

type Tab = "timeline" | "queries" | "deltas";

function Panel({ core, onClose }: { core: DevtoolsCore | undefined; onClose: () => void }): ReactNode {
  const [tab, setTab] = useState<Tab>("timeline");
  return h(
    "div",
    { style: S.panel },
    h(
      "div",
      { style: S.header },
      h("strong", { style: { fontSize: 12 } }, "🌊 Rindle devtools"),
      h("div", { style: { flex: 1 } }),
      h("button", { type: "button", style: S.iconBtn, onClick: onClose, title: "Close" }, "✕"),
    ),
    core
      ? h(CorePanel, { core, tab, setTab })
      : h(
          "div",
          { style: S.empty },
          "No Rindle client attached. ",
          h("br"),
          "Call ",
          h("code", { style: S.code }, "attachDevtools(app)"),
          " from ",
          h("code", { style: S.code }, "@rindle/devtools"),
          " in dev.",
        ),
  );
}

function CorePanel({ core, tab, setTab }: { core: DevtoolsCore; tab: Tab; setTab: (t: Tab) => void }): ReactNode {
  const subscribe = useCallback((cb: () => void) => core.subscribe(cb), [core]);
  const getState = useCallback(() => core.getState(), [core]);
  const state = useSyncExternalStore(subscribe, getState, getState);
  const pendingCount = state.timeline.filter((t) => t.state === "pending").length;

  return h(
    "div",
    { style: { display: "contents" } },
    h(
      "div",
      { style: S.tabs },
      h(TabButton, { active: tab === "timeline", onClick: () => setTab("timeline") }, "Timeline ", pendingCount > 0 ? h("span", { style: S.dot }, pendingCount) : null),
      h(TabButton, { active: tab === "queries", onClick: () => setTab("queries") }, "Queries ", h("span", { style: S.count }, state.queries.length)),
      h(TabButton, { active: tab === "deltas", onClick: () => setTab("deltas") }, "Deltas ", h("span", { style: S.count }, state.deltas.length)),
    ),
    h(
      "div",
      { style: S.body },
      tab === "timeline" ? h(TimelinePane, { state, core }) : null,
      tab === "queries" ? h(QueriesPane, { state }) : null,
      tab === "deltas" ? h(DeltasPane, { state, core }) : null,
    ),
  );
}

// --- Timeline (§4.1) ---------------------------------------------------------

function TimelinePane({ state, core }: { state: DevtoolsState; core: DevtoolsCore }): ReactNode {
  if (!state.capabilities.optimistic) {
    return h("div", { style: S.empty }, "This backend has no optimistic loop — the mutation timeline is empty.");
  }
  const rows = [...state.timeline].reverse(); // newest first
  const o = state.optimistic;
  return h(
    "div",
    null,
    h(
      "div",
      { style: S.toolbar },
      h(
        "span",
        { style: S.muted },
        `confirmed lmid ${o?.confirmedLmid ?? 0} · next mid ${o?.nextMid ?? 1}${o && o.bufferedFrames > 0 ? ` · ${o.bufferedFrames} buffered` : ""}`,
      ),
      h("div", { style: { flex: 1 } }),
      h("button", { type: "button", style: S.smallBtn, onClick: () => core.clearHistory() }, "clear settled"),
    ),
    rows.length === 0
      ? h("div", { style: S.empty }, "No mutations yet. Invoke a mutator to watch the fork/rebase loop.")
      : rows.map((t) => h(TimelineRow, { key: t.id, t })),
  );
}

function TimelineRow({ t }: { t: TimelineEntry }): ReactNode {
  const [open, setOpen] = useState(false);
  const elapsed = (t.settledAt ?? Date.now()) - t.invokedAt;
  return h(
    "div",
    { style: S.row },
    h(
      "div",
      { style: S.rowHead, onClick: () => setOpen((v) => !v) },
      h(StateBadge, { state: t.state }),
      h("span", { style: S.mono }, t.name),
      h("span", { style: S.muted }, t.mid != null ? `mid ${t.mid}` : t.folded ? "folding…" : "—"),
      t.folded ? h("span", { style: S.tag }, "folded") : null,
      t.reconciledWithChurn
        ? h(
            "span",
            { style: S.warn, title: "View churn coincided with this confirmation — a possible snap-back (prediction diverged from the server)." },
            "⚡ snap-back?",
          )
        : null,
      h("div", { style: { flex: 1 } }),
      h("span", { style: S.muted }, fmtMs(elapsed)),
    ),
    open
      ? h(
          "div",
          { style: S.rowBody },
          h(KV, { k: "args", v: h("pre", { style: S.pre }, safeJson(t.args)) }),
          h(KV, { k: "tables", v: t.tables.join(", ") || "—" }),
          h(KV, { k: "affects queries", v: t.affectedQueries.length ? t.affectedQueries.map((q) => `#${q}`).join(", ") : "—" }),
          t.fold
            ? h(KV, {
                k: "fold",
                v: `${t.fold.foldKey} — debounce ${t.fold.debounceMs}ms${t.fold.maxWaitMs ? `, maxWait ${t.fold.maxWaitMs}ms` : ""}${
                  t.fold.deferAcrossWrites ? ", deferAcrossWrites" : ""
                }${t.fold.flushed ? " (flushed)" : ""}`,
              })
            : null,
        )
      : null,
  );
}

// --- Queries (§4.2) ----------------------------------------------------------

function QueriesPane({ state }: { state: DevtoolsState }): ReactNode {
  if (state.queries.length === 0) return h("div", { style: S.empty }, "No materialized views are mounted.");
  return h("div", null, state.queries.map((q) => h(QueryRow, { key: q.qid, q })));
}

function QueryRow({ q }: { q: QueryEntry }): ReactNode {
  const [open, setOpen] = useState(false);
  return h(
    "div",
    { style: S.row },
    h(
      "div",
      { style: S.rowHead, onClick: () => setOpen((v) => !v) },
      h("span", { style: S.muted }, `#${q.qid}`),
      h(ResultBadge, { rt: q.resultType }),
      q.pending ? h("span", { style: S.tagPending }, "pending") : null,
      h("span", { style: S.mono, title: q.summary }, q.summary),
      h("div", { style: { flex: 1 } }),
      h("span", { style: S.muted }, `${q.rowCount} rows`),
    ),
    open
      ? h(
          "div",
          { style: S.rowBody },
          h(KV, { k: "ast", v: h("pre", { style: S.pre }, safeJson(q.ast)) }),
          h(KV, { k: "reads tables", v: q.tables.join(", ") }),
          h(KV, { k: `sample (${q.sample.length}/${q.rowCount})`, v: h("pre", { style: S.pre }, safeJson(q.sample)) }),
        )
      : null,
  );
}

// --- Delta stream (§4.3) -----------------------------------------------------

const DELTA_KINDS: DeltaKind[] = ["hello", "snapshot", "add", "remove", "edit"];

function DeltasPane({ state, core }: { state: DevtoolsState; core: DevtoolsCore }): ReactNode {
  const [paused, setPaused] = useState(false);
  const [frozen, setFrozen] = useState<DeltaEntry[]>([]);
  const [hidden, setHidden] = useState<Set<DeltaKind>>(() => new Set());

  const live = state.deltas;
  const shown = paused ? frozen : live;
  const rows = shown.filter((d) => !hidden.has(d.kind)).slice(-300).reverse();

  const toggleKind = (k: DeltaKind) =>
    setHidden((prev) => {
      const next = new Set(prev);
      if (next.has(k)) next.delete(k);
      else next.add(k);
      return next;
    });

  return h(
    "div",
    null,
    h(
      "div",
      { style: S.toolbar },
      ...DELTA_KINDS.map((k) =>
        h(
          "button",
          {
            key: k,
            type: "button",
            style: { ...S.chip, ...(hidden.has(k) ? S.chipOff : null) },
            onClick: () => toggleKind(k),
            title: hidden.has(k) ? `show ${k}` : `hide ${k}`,
          },
          k,
        ),
      ),
      h("div", { style: { flex: 1 } }),
      h(
        "button",
        {
          type: "button",
          style: S.smallBtn,
          onClick: () => {
            if (!paused) setFrozen(live.slice());
            setPaused((p) => !p);
          },
        },
        paused ? "▶ resume" : "⏸ pause",
      ),
      h("button", { type: "button", style: S.smallBtn, onClick: () => core.clearDeltas() }, "clear"),
    ),
    rows.length === 0
      ? h("div", { style: S.empty }, `No deltas${hidden.size ? " match the filter" : " yet"}.`)
      : rows.map((d) =>
          h(
            "div",
            { key: d.seq, style: S.deltaRow },
            h("span", { style: S.muted }, `#${d.qid}`),
            h(DeltaBadge, { kind: d.kind }),
            h("span", { style: S.mono }, d.label),
          ),
        ),
  );
}

// --- small components --------------------------------------------------------

function TabButton({ active, onClick, children }: { active: boolean; onClick: () => void; children?: ReactNode }): ReactNode {
  return h("button", { type: "button", style: { ...S.tab, ...(active ? S.tabActive : null) }, onClick }, children);
}

function KV({ k, v }: { k: string; v: ReactNode }): ReactNode {
  return h("div", { style: S.kv }, h("span", { style: S.kvKey }, k), h("span", { style: S.kvVal }, v));
}

const STATE_COLOR: Record<MutationState, string> = { pending: "#d9a106", confirmed: "#2ea043", dropped: "#cf3b3b" };
function StateBadge({ state }: { state: MutationState }): ReactNode {
  return h("span", { style: { ...S.badge, background: STATE_COLOR[state] } }, state);
}

function ResultBadge({ rt }: { rt: ResultType }): ReactNode {
  const color = rt === "complete" ? "#2ea043" : rt === "error" ? "#cf3b3b" : "#6b7785";
  return h("span", { style: { ...S.badge, background: color } }, rt);
}

const DELTA_COLOR: Record<DeltaKind, string> = { hello: "#6b7785", snapshot: "#8957e5", add: "#2ea043", remove: "#cf3b3b", edit: "#1f6feb" };
function DeltaBadge({ kind }: { kind: DeltaKind }): ReactNode {
  return h("span", { style: { ...S.badge, background: DELTA_COLOR[kind] } }, kind);
}

// --- formatting --------------------------------------------------------------

function fmtMs(ms: number): string {
  if (ms < 1000) return `${Math.round(ms)}ms`;
  if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
  return `${Math.round(ms / 1000)}s`;
}

function safeJson(v: unknown): string {
  try {
    return JSON.stringify(v, null, 2) ?? String(v);
  } catch {
    return String(v);
  }
}

// --- styles (inline; no CSS dependency) --------------------------------------

const mono = "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace";
const S: Record<string, CSSProperties> = {
  launcher: { position: "fixed", bottom: 12, right: 12, zIndex: 2147483000 },
  launchBtn: { font: `600 12px ${mono}`, color: "#e6edf3", background: "#161b22", border: "1px solid #30363d", borderRadius: 8, padding: "6px 10px", cursor: "pointer", boxShadow: "0 2px 8px rgba(0,0,0,.35)" },
  panel: { position: "fixed", bottom: 12, right: 12, width: 440, maxWidth: "calc(100vw - 24px)", height: "60vh", maxHeight: 640, display: "flex", flexDirection: "column", font: `12px ${mono}`, color: "#e6edf3", background: "#0d1117", border: "1px solid #30363d", borderRadius: 10, boxShadow: "0 8px 32px rgba(0,0,0,.5)", zIndex: 2147483000, overflow: "hidden" },
  header: { display: "flex", alignItems: "center", gap: 8, padding: "8px 10px", borderBottom: "1px solid #21262d" },
  iconBtn: { background: "transparent", color: "#8b949e", border: "none", cursor: "pointer", fontSize: 13 },
  tabs: { display: "flex", gap: 2, padding: "6px 8px 0", borderBottom: "1px solid #21262d" },
  tab: { font: `600 11px ${mono}`, color: "#8b949e", background: "transparent", border: "none", borderBottom: "2px solid transparent", padding: "6px 10px", cursor: "pointer" },
  tabActive: { color: "#e6edf3", borderBottom: "2px solid #1f6feb" },
  body: { flex: 1, overflowY: "auto", padding: "6px 8px" },
  toolbar: { display: "flex", alignItems: "center", gap: 6, padding: "4px 2px 8px", flexWrap: "wrap" },
  row: { borderBottom: "1px solid #161b22" },
  rowHead: { display: "flex", alignItems: "center", gap: 6, padding: "5px 4px", cursor: "pointer" },
  rowBody: { padding: "2px 4px 8px 4px", display: "flex", flexDirection: "column", gap: 3 },
  deltaRow: { display: "flex", alignItems: "center", gap: 6, padding: "3px 4px", borderBottom: "1px solid #161b22" },
  badge: { font: `600 10px ${mono}`, color: "#fff", borderRadius: 4, padding: "1px 5px", textTransform: "uppercase" },
  tag: { font: `10px ${mono}`, color: "#c9a0ff", border: "1px solid #3b2d57", borderRadius: 4, padding: "0 4px" },
  tagPending: { font: `10px ${mono}`, color: "#f0c552", border: "1px solid #5a4a16", borderRadius: 4, padding: "0 4px" },
  warn: { font: `600 10px ${mono}`, color: "#f0883e" },
  mono: { fontFamily: mono, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" },
  muted: { color: "#6e7681", fontSize: 11, whiteSpace: "nowrap" },
  count: { color: "#6e7681" },
  dot: { background: "#d9a106", color: "#0d1117", borderRadius: 8, padding: "0 5px", marginLeft: 2, fontWeight: 700 },
  empty: { color: "#6e7681", padding: 16, textAlign: "center", lineHeight: 1.6 },
  smallBtn: { font: `11px ${mono}`, color: "#c9d1d9", background: "#21262d", border: "1px solid #30363d", borderRadius: 5, padding: "2px 7px", cursor: "pointer" },
  chip: { font: `10px ${mono}`, color: "#c9d1d9", background: "#161b22", border: "1px solid #30363d", borderRadius: 10, padding: "1px 8px", cursor: "pointer" },
  chipOff: { color: "#484f58", textDecoration: "line-through" },
  kv: { display: "flex", gap: 6, alignItems: "baseline" },
  kvKey: { color: "#6e7681", minWidth: 96, flexShrink: 0 },
  kvVal: { color: "#c9d1d9", wordBreak: "break-word", flex: 1 },
  pre: { margin: 0, padding: "4px 6px", background: "#161b22", borderRadius: 5, maxHeight: 180, overflow: "auto", whiteSpace: "pre-wrap" },
  code: { background: "#161b22", borderRadius: 4, padding: "0 4px" },
};

export { getDevtoolsCore, getDevtoolsHub, attachDevtools } from "@rindle/devtools";
export type { DevtoolsCore, DevtoolsState } from "@rindle/devtools";
