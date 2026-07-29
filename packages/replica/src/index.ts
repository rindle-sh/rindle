// @rindle/replica — the native (napi-rs) backend for @rindle/client: the SQLite-backed
// `rindle-replica` live-query engine, in-process. It speaks the same `Backend` seam as
// @rindle/wasm, so the SAME Store/ArrayView drives it — only the engine differs (native
// SQLite + BEGIN CONCURRENT CDC instead of the wasm memory engine). Re-exports @rindle/client
// so a local app can import everything from here.
//
// Unlike @rindle/wasm there is no async init: the native addon loads synchronously when this
// module is imported (`../index.js` is the napi loader that `require`s the `.node`).

import { Db } from "../index.js";
import { Store, tableSpec } from "@rindle/client";
import type { Ast, Backend, ChangeEvent, ColsMap, Mutation, QueryId, Schema } from "@rindle/client";

export * from "@rindle/client";

// The raw native `Db` (register/query/commit + the normalized `queryNormalized`/
// `commitNormalized`) — the low-level handle a normalized server source wraps.
export { Db as NativeReplicaDb } from "../index.js";

// One server-side mutation's open transaction (`Db.beginMutation`) — what a server
// mutator registry runs against (exec/queryRows; commitWithLmid lands effects + lmid).
export type { NativeMutationTxn } from "../index.js";

// The CLUSTER-backed, async-push engine (CLUSTER-FOLD-IN-DESIGN.md WS6): `commit*` returns
// `{cv}` immediately; per-query batches / per-connection progress frames / faults arrive on
// `onEvent`. Used by the optimistic server to derive queries in parallel across worker threads.
export { ClusterDb as NativeClusterDb } from "../index.js";

// One cluster-backed mutation's open transaction (`ClusterDb.beginMutation`).
export type { ClusterMutationTxn } from "../index.js";

// The native `Db` surface this backend uses (kept local so it doesn't depend on the
// generated `.d.ts` shape, which types the JSON returns as `any`).
interface NativeDb {
  registerTable(name: string, columns: string[], primaryKey: number[], columnTypes: string[]): void;
  query(queryId: number, astJson: string): { comparatorVersion: number; schema: unknown; snapshot: unknown[] };
  destroyQuery(queryId: number): void;
  commit(mutations: unknown): Array<{ queryId: number; events: unknown[] }>;
}

/** One engine commit's per-query batches (the shape `NativeDb.commit` returns) — identical to the
 *  wasm engine's, so the same `dispatch` bracket gives the same cross-view-atomic guarantee. */
type BatchSet = Array<{ queryId: number; events: unknown[] }>;

/** The in-process, SQLite-backed native backend. Each instance owns its own replica (a
 *  temp wal2 database). Registers every schema table (DDL + engine source) on construction. */
export class ReplicaBackend<S extends ColsMap> implements Backend {
  private readonly db: NativeDb;
  private handler: (qid: QueryId, ev: ChangeEvent) => void = () => {};
  // The Store's commit-boundary handler ({@link Backend.onCommitBoundary}): `dispatch` brackets each
  // commit's per-query batch delivery with `begin`/`end` so the Store folds every affected view
  // before notifying any subscriber (cross-view-atomic notification). `ReplicaBackend` has the SAME
  // `commit() → per-query BatchSet` fan-out as the wasm engine, so it needs the SAME barrier — a
  // subscriber re-reading a sibling view would otherwise observe that sibling's pre-commit state.
  // Defaults to a no-op so a Store that never registers it just gets per-event notification.
  private boundaryHandler: (phase: "begin" | "end") => void = () => {};

  // Non-reentrant delivery (mirrors `WasmBackend`): a subscriber that synchronously triggers another
  // write enqueues that commit's batches; they drain only after the in-flight commit's batches
  // finish, preserving per-query commit order regardless of what subscribers do.
  private readonly dispatchQueue: BatchSet[] = [];
  private draining = false;

  constructor(schema: Schema<S>) {
    this.db = new Db() as unknown as NativeDb;
    for (const name of Object.keys(schema.tables)) {
      const meta = schema.tables[name];
      const { columns, primaryKey } = tableSpec(meta);
      const cols = meta.columns as unknown as Record<string, { type: string }>;
      const columnTypes = columns.map((c) => cols[c].type);
      this.db.registerTable(name, columns, primaryKey, columnTypes);
    }
  }

  registerQuery(qid: QueryId, ast: Ast): void {
    const r = this.db.query(qid, JSON.stringify(ast));
    this.handler(qid, { type: "hello", schema: r.schema as never, comparatorVersion: r.comparatorVersion });
    this.handler(qid, { type: "snapshot", adds: r.snapshot as never, last: true });
  }

  unregisterQuery(qid: QueryId): void {
    this.db.destroyQuery(qid);
  }

  mutate(mutations: Mutation[]): Promise<void> {
    this.dispatch(this.db.commit(mutations) as BatchSet);
    return Promise.resolve();
  }

  onEvent(handler: (qid: QueryId, ev: ChangeEvent) => void): void {
    this.handler = handler;
  }

  /** Register the Store's commit-boundary handler ({@link Backend.onCommitBoundary}): `dispatch`
   *  calls it around each commit's per-query batch delivery so the Store can fold every affected
   *  view before notifying any subscriber. */
  onCommitBoundary(handler: (phase: "begin" | "end") => void): void {
    this.boundaryHandler = handler;
  }

  /** Deliver one commit's per-query batches — a faithful mirror of `WasmBackend.dispatch`.
   *  Non-reentrant: if a delivery is already in progress (a subscriber re-entered via `write()`),
   *  enqueue and let the active drain pick it up FIFO, so each query folds commits in order.
   *  Per-query isolated: a view's fold or subscriber throwing does NOT drop sibling queries'
   *  batches — the first error is re-raised only after every query has been delivered.
   *
   *  Cross-view-atomic notification: each commit is bracketed with `boundaryHandler("begin"/"end")`
   *  so the Store folds ALL of this commit's views before notifying ANY subscriber (a subscriber
   *  re-reading a sibling view then sees post-commit data). `begin`/`end` stay balanced even if a
   *  fold throws (the `finally`), and a re-entrant write enqueued during the `end` flush drains as
   *  its own bracketed commit in the next loop turn. */
  private dispatch(batches: BatchSet): void {
    this.dispatchQueue.push(batches);
    if (this.draining) return; // an outer drain is running; it will deliver this set
    this.draining = true;
    let firstError: unknown;
    let hasError = false;
    const note = (err: unknown) => {
      if (!hasError) {
        hasError = true;
        firstError = err;
      }
    };
    try {
      while (this.dispatchQueue.length) {
        const next = this.dispatchQueue.shift()!;
        if (next.length === 0) continue; // an empty commit (no query changed) — no barrier needed
        this.boundaryHandler("begin");
        try {
          for (const b of next) {
            try {
              this.handler(b.queryId, { type: "batch", events: b.events as never });
            } catch (err) {
              note(err);
            }
          }
        } finally {
          try {
            this.boundaryHandler("end"); // flush: notify every folded view's subscribers
          } catch (err) {
            note(err);
          }
        }
      }
    } finally {
      this.draining = false;
    }
    if (hasError) throw firstError;
  }
}

/** Convenience: a ready local {@link Store} backed by a fresh native replica. */
export function createReplicaStore<S extends ColsMap>(schema: Schema<S>): Store<S> {
  return new Store(schema, new ReplicaBackend(schema));
}
