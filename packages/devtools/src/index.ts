// @rindle/devtools — the framework-agnostic in-browser devtools core (DEBUG-TOOLS-BROWSER-DESIGN.md).
// Attaches to a running client over the read-only seams and maintains the §4 read-model: a mutation
// TIMELINE (the fork/rebase loop made visible), a QUERIES inspector, and a DELTA stream. Dev-only;
// pair it with a panel (`@rindle/react-devtools`) or read `getDevtoolsCore().getState()` directly.

export { DevtoolsCore } from "./core.ts";
export { attachDevtools, getDevtoolsCore, getDevtoolsHub } from "./global.ts";
export type { DevtoolsHub } from "./global.ts";

export { collectTables, summarizeAst } from "./ast.ts";

export type {
  BackendDevObserver,
  BackendServerDelta,
  DeltaEntry,
  DeltaKind,
  DevtoolsBackend,
  DevtoolsCoreOptions,
  DevtoolsState,
  DevtoolsStore,
  DevtoolsTarget,
  FoldInspect,
  MutationState,
  OptimisticInspect,
  PendingInspect,
  QueryEntry,
  TimelineEntry,
} from "./types.ts";

// Re-export the client types a panel or a custom attach target needs, so it can depend on
// `@rindle/devtools` alone.
export type { Ast, ChangeEvent, NormalizedOp, QueryId, ResultType, StoreInspect } from "@rindle/client";
