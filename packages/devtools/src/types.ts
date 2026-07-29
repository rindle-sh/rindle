// The devtools read-model (DEBUG-TOOLS-BROWSER-DESIGN.md §4) and the read-only seams the core
// attaches to. Everything here is derived, in dev, from state the client already holds — the core
// adds no hot-path instrumentation (§1.1 "surface, not instrument").

import type {
  Ast,
  BackendDevObserver,
  BackendServerDelta,
  ChangeEvent,
  QueryId,
  ResultType,
  StoreInspect,
} from "@rindle/client";

export type {
  Ast,
  BackendDevObserver,
  BackendServerDelta,
  ChangeEvent,
  FlatChange,
  FlatOp,
  NormalizedOp,
  QueryId,
  ResultType,
  StoreInspect,
} from "@rindle/client";

// --- the attach target (duck-typed) ------------------------------------------------
// The core binds to a running client by structural type only — it never imports `@rindle/optimistic`
// (which would drag the wasm artifact into its typecheck). The optimistic backend's `__inspect()`
// shape below is kept IDENTICAL to `OptimisticInspect` in `@rindle/optimistic/backend.ts`; the two
// are cross-referenced by comment rather than a shared import to keep this package wasm-free.

/** One folded entry's debounce window — mirror of `@rindle/optimistic`'s `FoldInspect`. */
export interface FoldInspect {
  foldKey: string;
  debounceMs: number;
  maxWaitMs?: number;
  deferAcrossWrites: boolean;
  flushed: boolean;
}

/** One pending mutation — mirror of `@rindle/optimistic`'s `PendingInspect`. */
export interface PendingInspect {
  key: string;
  mid: number | null;
  name: string;
  args: unknown;
  tables: string[];
  fold?: FoldInspect;
}

/** A snapshot of the optimistic loop — mirror of `@rindle/optimistic`'s `OptimisticInspect`. */
export interface OptimisticInspect {
  pending: PendingInspect[];
  confirmedLmid: number;
  nextMid: number;
  appliedCv: number;
  bufferedFrames: number;
  pendingTables: string[];
}

/** The Store surface the core reads (a structural subset of `@rindle/client`'s `Store`). The delta
 *  + resultType taps are the SUPPORTED app-facing seams (no private devtools back door). */
export interface DevtoolsStore {
  subscribeChanges(listener: (qid: QueryId, ev: ChangeEvent) => void): () => void;
  subscribeResultType(listener: (qid: QueryId, rt: ResultType) => void): () => void;
  __inspect(sampleRows?: number): StoreInspect;
}

/** Optional backend-side devtools capabilities. `__inspect` is present on `OptimisticBackend`;
 *  `__attachDevtoolsServerDeltas` is present on backends that can surface authoritative server
 *  frames separately from the Store's post-apply view stream. */
export interface DevtoolsBackend {
  __inspect?(): OptimisticInspect;
  __attachDevtoolsServerDeltas?(observer: BackendDevObserver): () => void;
}

/** What {@link attachDevtools} binds to: a `createRindleClient` app, or any `{ store, backend }`. */
export interface DevtoolsTarget {
  store: DevtoolsStore;
  /** Narrowed to {@link DevtoolsBackend} at runtime when it carries `__inspect` (capability probe). */
  backend?: unknown;
}

// --- the read-model the panel renders ----------------------------------------------

/** A mutation's place in the fork/rebase lifecycle (DEBUG-TOOLS-BROWSER-DESIGN §4.1). */
export type MutationState = "pending" | "confirmed" | "dropped";

/** One row of the mutation timeline — the optimistic loop made visible. */
export interface TimelineEntry {
  /** Stable identity across snapshots: the pending key (`m:<mid>` once a mid is dealt, else
   *  `f:<foldKey>` while a fold debounces). Retained after the entry settles. */
  id: string;
  /** The wire mutation id, or `null` for a still-folding entry. */
  mid: number | null;
  name: string;
  args: unknown;
  /** Tables the mutator touched (its pending-axis footprint). */
  tables: string[];
  state: MutationState;
  /** True for a debounced/folded write; `fold` carries its window while it is live. */
  folded: boolean;
  fold?: FoldInspect;
  /** Devtools-clock ms at first observation (invoke). */
  invokedAt: number;
  /** Devtools-clock ms when it left the pending stack (confirmed or dropped). */
  settledAt?: number;
  /** Heuristic (§4.1): view churn coincided with this mutation's confirmation — a POSSIBLE
   *  snap-back (the optimistic prediction diverged from the authoritative server result). Labeled
   *  "possible" because unrelated server data released in the same coherent batch also shows churn;
   *  a precise signal needs a reconcile-boundary event (a future engine seam). */
  reconciledWithChurn: boolean;
  /** qids of live queries whose tables this mutation touches (computed against the current views). */
  affectedQueries: number[];
}

/** One materialized view in the queries inspector (DEBUG-TOOLS-BROWSER-DESIGN §4.2). */
export interface QueryEntry {
  qid: number;
  ast: Ast;
  /** The root table (`ast.table`). */
  table: string;
  /** A one-line human summary of the AST (table, filters, order, limit, relationships). */
  summary: string;
  /** Every base table the query reads (root + correlated subqueries). */
  tables: string[];
  resultType: ResultType;
  rowCount: number;
  sample: readonly unknown[];
  /** Does any pending mutation touch this query's tables? (the §7.2 pending axis). */
  pending: boolean;
}

/** The kind of a delta-stream row. `child` is an Add/Remove/Edit addressed at a NESTED path
 *  (`depth > 0`) — rindle's relationship-level change (DEBUG-TOOLS-BROWSER-DESIGN §4.3). */
export type DeltaKind = "hello" | "snapshot" | "add" | "remove" | "edit";

/** One entry of the live delta stream — the IVM change primitive made visible (§4.3). */
export interface DeltaEntry {
  seq: number;
  at: number;
  qid: number;
  kind: DeltaKind;
  /** Nesting depth of the change path: 0 = top-level row, >0 = a child (relationship) change. */
  depth: number;
  /** A compact, human-readable description of the change. */
  label: string;
}

/** The whole devtools snapshot a panel renders. Reference-stable arrays between updates where the
 *  underlying data did not change is NOT guaranteed — panels should treat each `getState()` as fresh. */
export interface DevtoolsState {
  /** Newest-last mutation timeline (capped). */
  timeline: TimelineEntry[];
  /** Every live materialized view. */
  queries: QueryEntry[];
  /** Newest-last delta stream ring (capped). */
  deltas: DeltaEntry[];
  /** The raw optimistic-loop snapshot, when the backend exposes one (a "loop" summary line). */
  optimistic?: OptimisticInspect;
  capabilities: { optimistic: boolean };
}

/** Construction options for {@link DevtoolsCore}. */
export interface DevtoolsCoreOptions {
  /** Max timeline rows retained (oldest settled rows drop first). Default 200. */
  timelineCap?: number;
  /** Max delta-stream rows retained. Default 500. */
  deltaCap?: number;
  /** Per-query row sample size pulled from the store. Default 25. */
  sampleRows?: number;
  /** Safety-net poll interval (ms) that catches state moves with no event — a fold's debounced
   *  flush, or a pending flip on an already-`complete` query. Default 0 (off); {@link attachDevtools}
   *  turns it on. */
  pollMs?: number;
  /** Coalesce event-driven recomputes onto a microtask. Default true; tests pass `false` and drive
   *  recomputes explicitly via {@link DevtoolsCore.refresh}. */
  autoFlush?: boolean;
  /** Injectable clock (ms). Default `Date.now`; tests inject a deterministic counter. */
  now?: () => number;
}
