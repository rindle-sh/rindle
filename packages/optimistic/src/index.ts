// @rindle/optimistic — optimistic writes over the local-first wasm engine
// (OPTIMISTIC-WRITES-DESIGN.md): named client mutators (invoked optimistically,
// RE-INVOKED on every rebase), the cv-buffered coherent release (§8.5), and the
// engine-side fork/rebase reconcile cycle (§1.3) behind the ordinary `Backend` seam.

import { Store } from "@rindle/client";
import type { ColsMap, Schema, OptimisticSource } from "@rindle/client";

import {
  OptimisticBackend,
  type ClientRegistry,
  type FoldHandle,
  type FoldOptions,
  type OptimisticBackendOptions,
} from "./backend.ts";

/** One entry of the {@link createOptimisticStore} `mutate` facade: call it for a normal optimistic
 *  write (returns the `mid`), or `.folded(opts, args)` for a debounced, last-value-wins folded write
 *  (returns a {@link FoldHandle} — the `mid` is assigned at flush, FOLDED-MUTATIONS-DESIGN §3). */
export type MutateFn<Args> = ((args: Args) => number) & {
  folded(opts: FoldOptions, args: Args): FoldHandle;
};

export { OptimisticBackend } from "./backend.ts";
export type {
  ClientMutator,
  ClientRegistry,
  DowngradeStuckEvent,
  FoldClock,
  FoldHandle,
  FoldInspect,
  FoldOptions,
  KeyedRow,
  MutationTx,
  OptimisticBackendOptions,
  OptimisticInspect,
  PendingInspect,
  QueryArg,
  QueryResultRow,
  ReadLog,
  ReadOutcome,
  ReadRecord,
  ResultType,
  ScopeSessionsEvent,
  SystemStreamSpec,
  SystemStreamTable,
  WriteRecord,
  WriteSet,
} from "./backend.ts";
// The §4 lifecycle system-stream leaf vocabulary (Slice I-iii): table names + wire schemas +
// the reserved system-sub query name (see ./system-streams.ts).
export {
  LIFECYCLE_QUERY_NAME,
  LIFECYCLE_TABLE_SCHEMAS,
  ROOM_CLIENT_MUTATIONS_TABLE,
  ROOM_MUTATION_OUTCOMES_TABLE,
  ROOM_WATERMARK_TABLE,
  roomDomainKey,
  SCOPE_SESSIONS_TABLE,
} from "./system-streams.ts";
export type {
  EnsureQueryOptions,
  EnsureQueryUntil,
  MutationEnvelope,
  OptimisticSource,
  ProgressFrame,
} from "@rindle/client";
// The shared (generator) mutator seam — a registry may hold plain client mutators OR these isomorphic
// generators (the SAME body the API server runs); re-exported here so an app registers from one import.
export { isoTx } from "@rindle/client";
export type { IsoTx, MutationGen, MutatorCtx, SharedMutator, YieldEffect } from "@rindle/client";

// The one-call client for the daemon/serverless topology (API server + daemon ws).
export { createRindleClient } from "./client.ts";
export type { RindleClient, RindleClientOptions } from "./client.ts";
// Rindle Realtime client surface (G-v resolve-then-register): the lease-wire mirror types, the
// loud anomaly channel, and the `__realtimeInspect` snapshot shape.
export type {
  LifecycleLeaseBlock,
  LifecycleLeaseEntry,
  RealtimeAnomaly,
  RealtimeAnomalyKind,
  RealtimeClientOptions,
  RealtimeInspect,
  RealtimeLeaseBlock,
  RealtimeLeaseTableSpec,
} from "./client.ts";
export { resetStableClientID, stableClientID } from "./client-id.ts";

// Local-table persistence (207-LOCAL-TABLE-PERSISTENCE-DESIGN.md): durable, cross-tab `local: true`
// tables. `createRindleClient` wires it via the `persistLocal` option; a standalone backend attaches
// with `attachLocalPersistence`; `deleteLocalPersistence` is the sanctioned logout hook (§3.2).
export { attachLocalPersistence, deleteLocalPersistence } from "./local-persist.ts";
export type {
  LocalPersistence,
  PersistChannel,
  PersistDb,
  PersistEnv,
  PersistLocalOptions,
  PersistMeta,
  RowState,
  StoredRow,
} from "./local-persist.ts";

/** A {@link Store} over an {@link OptimisticBackend}, plus the named-mutator entry
 *  (`mutate.createIssue(args)` — the §9 dream shape) and the §6 lifecycle surface. */
export function createOptimisticStore<S extends ColsMap, R extends ClientRegistry>(
  schema: Schema<S>,
  source: OptimisticSource,
  registry: R,
  opts: OptimisticBackendOptions,
): {
  store: Store<S>;
  backend: OptimisticBackend<S>;
  mutate: { [K in keyof R]: MutateFn<Parameters<R[K]>[1]> };
} {
  const backend = new OptimisticBackend(schema, source, registry, opts);
  const store = new Store(schema, backend);
  const mutate = new Proxy(
    {},
    {
      get: (_t, name: string) => {
        const fn = ((args: unknown) => backend.invoke(name, args)) as MutateFn<unknown>;
        fn.folded = (foldOpts: FoldOptions, args: unknown) => backend.invokeFolded(name, foldOpts, args);
        return fn;
      },
    },
  ) as { [K in keyof R]: MutateFn<Parameters<R[K]>[1]> };
  return { store, backend, mutate };
}
