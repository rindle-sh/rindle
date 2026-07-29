// @rindle/wasm — the WASM backend: the in-process IVM engine (src/wasm/db.rs) behind the
// @rindle/client `Backend` seam (WASM-CLIENT-DESIGN.md §2.1). Also re-exports @rindle/client so a
// local app can import everything from here.
//
// The wasm is a `--target web` ESM artifact (packages/wasm/pkg, built by ./build.sh): it
// needs an explicit, one-time init — `await initWasm()` — before constructing a backend.
// In Node the bytes are read from the package; in a browser/bundler the wasm is fetched.

import init, { Db } from "../pkg/rindle.js";
import { localTableNames, Store, tableSpec } from "@rindle/client";
import type {
  Ast,
  Backend,
  ChangeEvent,
  ColsMap,
  Mutation,
  QueryId,
  Schema,
} from "@rindle/client";

export * from "@rindle/client";

// The wasm Db surface this backend uses (kept local so it doesn't depend on the generated .d.ts).
// One source per table (302-ROOM-STORE-SEPARATION-DESIGN.md): the engine holds ordinary tables
// only — a room-backed table is just another registered table whose deltas happen to arrive on a
// room channel. The multi-source merge surface that briefly lived here was removed with
// `rust/src/merge.rs`.
interface WasmDb {
  registerTable(table: string, spec: { columns: string[]; primaryKey: number[] }, local: boolean): void;
  unregisterTable(table: string): void;
  query(queryId: number, ast: Ast): { comparatorVersion: number; schema: unknown; snapshot: unknown[] };
  destroyQuery(queryId: number): void;
  write(): WasmWriteTxn;
  serverBatchBegin(deltas: ServerDeltaOp[]): void;
  serverBatchEnd(): Array<{ queryId: number; events: unknown[] }>;
}

/** A raw staged write transaction over the wasm engine — the surface an optimistic client
 *  mutator runs against (`get` reads the live state under this txn's staged overlay, so a
 *  read-dependent mutator sees its own writes; OPTIMISTIC-WRITES-DESIGN.md §4.1). */
export interface WasmWriteTxn {
  add(table: string, row: unknown[]): void;
  remove(table: string, row: unknown[]): void;
  edit(table: string, oldRow: unknown[], newRow: unknown[]): void;
  get(table: string, pk: unknown[]): unknown[] | undefined;
  /** Run a one-shot query over the state this txn is mutating — the live base plus this
   *  txn's own staged writes-so-far (read-your-writes; 203-MUTATOR-READS-DESIGN.md §5.2).
   *  Synchronous (over a lazy read-cache fork of the staged buffer). Returns the query's rows
   *  as keyed objects with their materialized `related` children nested by name (identical in
   *  shape to a `view.data` row), in the query's order. */
  query(ast: Ast): unknown[];
  commit(): Array<{ queryId: number; events: unknown[] }>;
  rollback(): void;
}

/** One base-table row op of a coherent server delta (the §1.3 `D`), bare cells. */
export type ServerDeltaOp =
  | { table: string; type: "add"; row: unknown[] }
  | { table: string; type: "remove"; row: unknown[] }
  | { table: string; type: "edit"; row: unknown[]; old: unknown[] };

let initialized: Promise<void> | null = null;

/** Initialize the wasm module (idempotent). Call once at startup before `new WasmBackend`
 *  / `createWasmStore`. Browser/bundler: no args (the wasm is fetched). Node: the bytes are
 *  read from the package. Pass `moduleOrPath` to override (a `WebAssembly.Module`, URL, or bytes). */
export function initWasm(moduleOrPath?: unknown): Promise<void> {
  if (!initialized) {
    initialized = (async () => {
      if (moduleOrPath !== undefined) {
        await init({ module_or_path: moduleOrPath });
      } else if ((globalThis as { process?: { versions?: { node?: string } } }).process?.versions?.node) {
        const { readFile } = await import("node:fs/promises");
        const bytes = await readFile(new URL("../pkg/rindle_bg.wasm", import.meta.url));
        await init({ module_or_path: bytes });
      } else {
        await init();
      }
    })();
  }
  return initialized;
}

/** The in-process WASM backend. Requires {@link initWasm} to have resolved first. */
/** One engine commit's per-query batches (the shape `WriteTxn.commit` / `serverBatchEnd` return). */
type BatchSet = Array<{ queryId: number; events: unknown[] }>;

export class WasmBackend<S extends ColsMap> implements Backend {
  private readonly db: WasmDb;
  private handler: (qid: QueryId, ev: ChangeEvent) => void = () => {};
  // The Store's commit-boundary handler ({@link Backend.onCommitBoundary}): `dispatch` brackets
  // each commit's per-query batch delivery with `begin`/`end` so the Store folds every affected
  // view before notifying any subscriber (cross-view-atomic notification). Defaults to a no-op so
  // a Store that never registers it (or none at all) just gets per-event notification.
  private boundaryHandler: (phase: "begin" | "end") => void = () => {};
  /** Local-only table names (`201-LOCAL-ONLY-TABLES-DESIGN.md` §4): registered UNTRACKED (so the
   *  optimistic rewind never reverts them) and the only tables {@link writeLocal} accepts. */
  private readonly localTables: Set<string>;

  // Non-reentrant delivery (#15): commits dispatch through a FIFO queue. A subscriber that
  // synchronously triggers another write enqueues that commit's batches; they drain only after
  // the in-flight commit's batches finish, preserving per-query commit order regardless of what
  // subscribers do.
  private readonly dispatchQueue: BatchSet[] = [];
  private draining = false;

  constructor(schema: Schema<S>) {
    this.db = new Db() as unknown as WasmDb;
    this.localTables = localTableNames(schema);
    for (const name of Object.keys(schema.tables)) {
      // A local table registers as a source but skips optimistic tracking (C1) — the `local` bit
      // crosses to the engine here.
      this.db.registerTable(name, tableSpec(schema.tables[name]), this.localTables.has(name));
    }
  }

  /** Register an additional base table after construction — for a SYNTHETIC aggregate table
   *  (`AGGREGATE-SYNC-DESIGN.md` §3.3) that is not in the typed schema. The
   *  `NormalizedBackend` registers each such `__agg_*` table once, before a query that reads
   *  it, so the local engine can join to it (the relationship `count` it backs is shipped by
   *  the server, never recomputed). Always synced (tracked) — synthetic tables are never local. */
  registerTable(name: string, spec: { columns: string[]; primaryKey: number[] }): void {
    this.db.registerTable(name, spec, false);
  }

  /** Direct-commit a batch of LOCAL-only writes (`201-LOCAL-ONLY-TABLES-DESIGN.md` §6): push them
   *  straight through the engine on the ordinary delivery path, OUTSIDE any optimistic cycle (a
   *  local table is untracked, so it never rebases). Rejects a synced/tracked table (M2).
   *  `onCommitted` fires between the engine commit and subscriber delivery ({@link writeWith}) —
   *  the post-commit anchor the persistence tap needs (207 §5.1). */
  writeLocal(mutations: Mutation[], onCommitted?: () => void): void {
    for (const m of mutations) {
      if (!this.localTables.has(m.table)) {
        throw new Error(`writeLocal: table "${m.table}" is not local-only — a direct commit to a synced table is reverted on the next rewind (M2).`);
      }
    }
    this.writeWith((tx) => {
      for (const m of mutations) {
        if (m.op === "add") tx.add(m.table, m.row);
        else if (m.op === "remove") tx.remove(m.table, m.row);
        else tx.edit(m.table, m.old, m.new);
      }
    }, onCommitted);
  }

  /** Remove a synthetic aggregate table registered by {@link registerTable}, once the last
   *  query reading it has been unregistered (`AGGREGATE-SYNC-DESIGN.md` §4): frees the engine
   *  source + its optimistic baseline so aggregate state is reclaimed, not permanent. Throws
   *  if a query still reads it (the backend refcounts readers, so it calls this only at 0). */
  unregisterTable(name: string): void {
    this.db.unregisterTable(name);
  }

  registerQuery(qid: QueryId, ast: Ast): void {
    const r = this.db.query(qid, ast);
    this.handler(qid, { type: "hello", schema: r.schema as never, comparatorVersion: r.comparatorVersion });
    this.handler(qid, { type: "snapshot", adds: r.snapshot as never, last: true });
  }

  unregisterQuery(qid: QueryId): void {
    this.db.destroyQuery(qid);
  }

  mutate(mutations: Mutation[]): Promise<void> {
    const tx = this.db.write();
    for (const m of mutations) {
      if (m.op === "add") tx.add(m.table, m.row);
      else if (m.op === "remove") tx.remove(m.table, m.row);
      else tx.edit(m.table, m.old, m.new);
    }
    this.dispatch(tx.commit() as BatchSet);
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

  /** Deliver one commit's per-query batches. Non-reentrant (#15): if a delivery is already in
   *  progress (a subscriber re-entered via write()), enqueue and let the active drain pick it
   *  up FIFO, so each query folds commits in order. Per-query isolated (#11): a view's fold or
   *  subscriber throwing does NOT drop sibling queries' batches — the first error is re-raised
   *  only after every query has been delivered.
   *
   *  Cross-view-atomic notification: each commit is bracketed with `boundaryHandler("begin"/"end")`
   *  so the Store folds ALL of this commit's views before notifying ANY subscriber (a subscriber
   *  re-reading a sibling view then sees post-commit data). `begin`/`end` stay balanced even if a
   *  fold throws (the `finally`), and a re-entrant write enqueued during the `end` flush drains as
   *  its own bracketed commit in the next loop turn — preserving per-query commit order (#15). */
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

  // --- the optimistic-cycle surface (OPTIMISTIC-WRITES-DESIGN.md §1/§3/§9) ----------
  //
  // `@rindle/optimistic` drives the engine's fork/rebase loop through these three; the
  // plain local path never calls them.

  /** Run `f` against a raw staged write txn (with the §4.1 `get` read path), commit, and
   *  dispatch the resulting batches on the ordinary event stream. Inside an open server
   *  batch the engine buffers the events into the cycle instead (commit returns `[]`),
   *  so re-invocations dispatch nothing here — delivery is `serverBatchEnd`'s.
   *
   *  `onCommitted` runs after `tx.commit()` returns but before `dispatch` delivers to
   *  subscribers: a throw from `f` (pre-commit — engine untouched) skips it; a subscriber
   *  throw re-raised by `dispatch` happens after it. It is the only point where "the commit
   *  is applied" is knowable to a caller that must not confuse the two failure modes. */
  writeWith(f: (tx: WasmWriteTxn) => void, onCommitted?: () => void): void {
    const tx = this.db.write();
    f(tx);
    const batches = tx.commit() as BatchSet;
    onCommitted?.();
    this.dispatch(batches);
  }

  /** Open a §1.3 reconcile cycle against the coherent server delta: the engine rewinds every
   *  tracked table (optimistic layer un-applied, delta folded in, each table's baseline re-forked)
   *  and starts buffering. Re-invoke the still-pending mutators via {@link writeWith}, then call
   *  {@link serverBatchEnd}. On error the rebase state is poisoned — discard the backend and
   *  re-hydrate. */
  serverBatchBegin(deltas: ServerDeltaOp[]): void {
    this.db.serverBatchBegin(deltas);
  }

  /** Close the cycle: the whole buffered stream (rewind + re-invocations) coalesces to
   *  the minimal net per query (§3 — a confirmed-correct prediction delivers nothing)
   *  and dispatches as ONE batch per affected query on the ordinary event stream. */
  serverBatchEnd(): void {
    this.dispatch(this.db.serverBatchEnd() as BatchSet);
  }
}

/** Convenience: init the wasm + return a ready local {@link Store}. */
export async function createWasmStore<S extends ColsMap>(schema: Schema<S>): Promise<Store<S>> {
  await initWasm();
  return new Store(schema, new WasmBackend(schema));
}
