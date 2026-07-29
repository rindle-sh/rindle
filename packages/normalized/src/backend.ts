// NormalizedBackend — the composition that turns a NORMALIZED server stream into the flat
// `Backend` seam the existing `Store` already drives (NORMALIZED-CHANGES-DESIGN.md §5/§7).
//
// A normalized client runs its OWN local engine and queries over its base tables. This
// backend wires that up by composing three pieces behind one `Backend`:
//   - a local `@rindle/wasm` engine (the base tables + local IVM that materializes results),
//   - `NormalizedSync` (cross-query refcount → net base-table mutations), and
//   - a `NormalizedSource` (the server's per-query normalized stream).
//
// The flow per query: `registerQuery` registers it BOTH on the local engine (→ an empty
// `FlatArrayView`) and on the server (→ its normalized footprint stream). Each normalized
// batch folds through `NormalizedSync` into net base mutations, which are applied to the
// local engine — whose own flat change stream then updates every affected view. Because the
// local engine fans a base write to ALL local queries, a row synced for one query updates
// any other query that reads it (the "local query resolution" the design is built for).
//
// Since this implements `Backend`, `new Store(schema, new NormalizedBackend(...))` reuses the
// whole Store/ArrayView machinery unchanged — the normalized path adds composition, not a
// second materialization layer.

import { normalizedTableSchemas, Store, tableSpec } from "@rindle/client";
import type {
  Ast,
  Backend,
  BackendDevObserver,
  ChangeEvent,
  ColsMap,
  Mutation,
  NormalizedEvent,
  NormalizedSource,
  NormalizedTableSchema,
  QueryId,
  RemoteQuery,
  ResultType,
  Schema,
} from "@rindle/client";
import { WasmBackend } from "@rindle/wasm";

import { aggTableSchemas, rewriteAggregates } from "./agg-table.ts";
import { NormalizedSync, type ColCounts, type PkCols } from "./sync.ts";

// The normalized seam + event/table-schema types live in `@rindle/client` (sibling of `Backend`),
// so a ws (`@rindle/remote`) source and the in-process native source emit the same shape and
// `@rindle/normalized` needs no dependency on `@rindle/remote`. Re-exported for callers.
export type { NormalizedEvent, NormalizedSource, NormalizedTableSchema } from "@rindle/client";

/** Each table's primary-key column indices, from the typed schema. */
function pkColsFromSchema<S extends ColsMap>(schema: Schema<S>): PkCols {
  const out: PkCols = {};
  for (const name of Object.keys(schema.tables)) out[name] = tableSpec(schema.tables[name]).primaryKey;
  return out;
}

/** Each table's FULL column count (the union-row width), from the typed schema. */
function colCountsFromSchema<S extends ColsMap>(schema: Schema<S>): ColCounts {
  const out: ColCounts = {};
  for (const name of Object.keys(schema.tables)) out[name] = tableSpec(schema.tables[name]).columns.length;
  return out;
}

/** Each table's column name → base ColId, from the typed schema (for mapping a projected
 *  hello's columns back to base positions, PROJECTION-SUPPORT-DESIGN.md §5.2). */
function colIndexFromSchema<S extends ColsMap>(schema: Schema<S>): Record<string, Map<string, number>> {
  const out: Record<string, Map<string, number>> = {};
  for (const name of Object.keys(schema.tables)) {
    const cols = tableSpec(schema.tables[name]).columns;
    out[name] = new Map(cols.map((c, i) => [c, i]));
  }
  return out;
}

export class NormalizedBackend<S extends ColsMap> implements Backend {
  private readonly local: WasmBackend<S>;
  private readonly sync: NormalizedSync;
  private readonly source: NormalizedSource;
  private handler: (qid: QueryId, ev: ChangeEvent) => void = () => {};
  /** The Store's commit-boundary handler ({@link Backend.onCommitBoundary}), forwarded from the
   *  local engine's `dispatch` brackets so the Store folds every affected view before notifying any
   *  subscriber (cross-view-atomic notification). A normalized batch becomes net base mutations
   *  applied to the local engine in one commit, which fans to every view reading those rows — this
   *  carries that commit's boundary up so all those views notify together. */
  private boundaryHandler: (phase: "begin" | "end") => void = () => {};
  private readonly devObservers = new Set<BackendDevObserver>();
  private readonly remoteSubs = new Map<string, RemoteSub>();
  private readonly localToRemote = new Map<QueryId, string>();
  private readonly remoteRetainToLocal = new Map<QueryId, QueryId | undefined>();
  private readonly sourceToRemote = new Map<QueryId, string>();
  private readonly resultTypes = new Map<QueryId, ResultType>();
  private readonly hydrated = new Set<QueryId>();
  /** Local qids that are JUST hydrating on the in-flight `onNormalized` snapshot — set only for the
   *  duration of its `local.mutate`, so the local-event forwarder stamps their fold `catchUp`: the
   *  whole first result set arrives as a `batch` (we hydrate the view by mutating the embedded engine,
   *  which speaks in batches — there is no second `snapshot`), and `catchUp` is the flag that tells the
   *  Store this batch IS hydration (retire the SSR seed, phase it as a snapshot for narration). The
   *  optimistic backend does the identical thing across its reconcile cycle. `null` outside a hydrate. */
  private catchUpQids: Set<QueryId> | null = null;
  private resultTypeHandler: (qid: QueryId, rt: ResultType) => void = () => {};
  /** The client's OWN typed per-table schemas (for CRIT#4 validation), fixed at construction. */
  private readonly clientTables: NormalizedTableSchema[];
  /** Synthetic aggregate tables (`__agg_*`) registered so far, by name (§3.3). Per aggregate
   *  DEFINITION (not per query), so two queries over the same count share one table. */
  private readonly synthetic = new Map<string, NormalizedTableSchema>();
  /** table → (column name → base ColId), for mapping a projected hello to base positions. */
  private readonly colIndex: Record<string, Map<string, number>>;
  /** table → full column count, to detect whether a hello's table is projected. */
  private readonly colCounts: ColCounts;
  /** Synthetic table name → how many registered queries reference it. Materialized on `0→1`,
   *  reclaimed (engine source + refcount layer) on `1→0` — so aggregate state is not permanent (§4). */
  private readonly syntheticRefs = new Map<string, number>();
  /** queryId → the synthetic tables it referenced at registration, to decrement on teardown. */
  private readonly queryAggTables = new Map<QueryId, string[]>();

  constructor(schema: Schema<S>, source: NormalizedSource) {
    this.local = new WasmBackend(schema);
    // The local engine's flat stream IS this backend's stream upward (→ the Store's views). A
    // just-hydrating query's first result set arrives here as a `batch` (we fold it by mutating the
    // engine); stamp it `catchUp` so the Store treats it as hydration (retire the SSR seed, phase it
    // as a snapshot) rather than an incremental change — see {@link catchUpQids}.
    this.local.onEvent((qid, ev) => {
      const stamped = ev.type === "batch" && this.catchUpQids?.has(qid) ? { ...ev, catchUp: true } : ev;
      this.handler(qid, stamped);
    });
    // Forward the local engine's commit brackets up to the Store (cross-view-atomic notification):
    // a normalized batch's net base mutations commit to `this.local` in one transaction, so its
    // commit boundary is ours — every view those rows touch then notifies together.
    this.local.onCommitBoundary((phase) => this.boundaryHandler(phase));
    this.colIndex = colIndexFromSchema(schema);
    this.colCounts = colCountsFromSchema(schema);
    this.sync = new NormalizedSync(pkColsFromSchema(schema), this.colCounts);
    this.source = source;
    // Hand the source our OWN typed schema so it validates each server hello against it and
    // rejects a column-order / PK skew instead of silently transposing positional cells (CRIT#4).
    this.clientTables = normalizedTableSchemas(schema);
    this.source.expectClientSchema?.(this.clientTables);
    this.source.onNormalized((qid, ev) => this.onNormalized(qid, ev));
  }

  registerQuery(qid: QueryId, ast: Ast, remote?: RemoteQuery): void {
    // A relationship aggregate (`count(child)`) is DISPLAYED from a server-authoritative
    // synthetic base table, not recomputed locally (AGGREGATE-SYNC-DESIGN.md §3.3): register
    // that table (engine + refcount layer + hello validation), then drive the local engine
    // off a rewritten AST whose `count` relationships read it with a plain projected join.
    this.ensureSyntheticTables(qid, ast);
    // Local first: builds the (empty) view synchronously. Then the server stream hydrates it.
    this.local.registerQuery(qid, rewriteAggregates(ast));
    if (remote) {
      this.retainRemote(qid, remote, qid);
    } else {
      this.hydrated.add(qid);
      this.setResultType(qid, "complete");
    }
  }

  /** Register every synthetic aggregate table `ast` needs that we haven't seen yet: on the
   *  local engine (so it can join to it), on `NormalizedSync` (so its rows refcount/GC by
   *  group key), and into the source's expected-schema set (so the server's `hello` — which
   *  advertises the same table — passes CRIT#4 validation, the client deriving the schema
   *  identically). Idempotent across queries that share an aggregate definition. */
  private ensureSyntheticTables(qid: QueryId, ast: Ast): void {
    if (this.queryAggTables.has(qid)) return; // idempotent per qid
    let added = false;
    const names: string[] = [];
    for (const t of aggTableSchemas(ast)) {
      names.push(t.name);
      const prev = this.syntheticRefs.get(t.name) ?? 0;
      this.syntheticRefs.set(t.name, prev + 1);
      if (prev > 0) continue; // another query already materialized it — just refcount
      this.synthetic.set(t.name, t);
      this.local.registerTable(t.name, { columns: t.columns, primaryKey: t.primaryKey });
      this.sync.registerTable(t.name, t.primaryKey);
      added = true;
    }
    if (names.length) this.queryAggTables.set(qid, names);
    if (added) this.source.expectClientSchema?.([...this.clientTables, ...this.synthetic.values()]);
  }

  /** Decrement each synthetic table query `qid` referenced; remove the ones that reach 0 (no
   *  reader left) from the engine + refcount layer — aggregate state reclaimed, not permanent
   *  (§4). Runs AFTER `local.unregisterQuery(qid)` so the source has no live connection when
   *  `unregisterTable` frees it. */
  private releaseSyntheticTables(qid: QueryId): void {
    const names = this.queryAggTables.get(qid);
    if (!names) return;
    this.queryAggTables.delete(qid);
    let removed = false;
    for (const name of names) {
      const next = (this.syntheticRefs.get(name) ?? 1) - 1;
      if (next > 0) {
        this.syntheticRefs.set(name, next);
        continue;
      }
      this.syntheticRefs.delete(name);
      this.local.unregisterTable(name);
      this.sync.unregisterTable(name);
      this.synthetic.delete(name);
      removed = true;
    }
    if (removed) this.source.expectClientSchema?.([...this.clientTables, ...this.synthetic.values()]);
  }

  unregisterQuery(qid: QueryId): void {
    const remoteQid = this.releaseRemote(qid);
    if (remoteQid !== undefined) {
      // Drop this remote footprint's refcounts; rows referenced by no other named query are
      // GC'd from the local base tables (→ other views shrink if they shared them).
      const muts = this.sync.dropQuery(remoteQid);
      if (muts.length) void this.local.mutate(muts);
    }
    this.local.unregisterQuery(qid);
    // Pipeline gone (no live conn) + `__agg` rows GC'd above → free any now-unread synthetic table.
    this.releaseSyntheticTables(qid);
    this.resultTypes.delete(qid);
    this.hydrated.delete(qid);
  }

  retainRemoteQuery(qid: QueryId, remote: RemoteQuery, localQueryId?: QueryId, ast?: Ast): void {
    if (ast) this.ensureSyntheticTables(qid, ast);
    this.retainRemote(qid, remote, localQueryId);
  }

  releaseRemoteQuery(qid: QueryId): void {
    const remoteQid = this.releaseRemote(qid);
    this.resultTypes.delete(qid);
    this.hydrated.delete(qid);
    this.releaseSyntheticTables(qid);
    if (remoteQid === undefined) return;
    const muts = this.sync.dropQuery(remoteQid);
    if (muts.length) void this.local.mutate(muts);
  }

  /** Writes are authoritative-only here: send to the server, the stream reconciles locally.
   *  (Optimistic local apply + rebase is Slice 6.) */
  mutate(mutations: Mutation[]): Promise<void> {
    return this.source.mutate(mutations);
  }

  onEvent(handler: (qid: QueryId, ev: ChangeEvent) => void): void {
    this.handler = handler;
  }

  onCommitBoundary(handler: (phase: "begin" | "end") => void): void {
    this.boundaryHandler = handler;
  }

  onResultType(handler: (qid: QueryId, rt: ResultType) => void): void {
    this.resultTypeHandler = handler;
  }

  __attachDevtoolsServerDeltas(observer: BackendDevObserver): () => void {
    this.devObservers.add(observer);
    return () => {
      this.devObservers.delete(observer);
    };
  }

  private onNormalized(qid: QueryId, ev: NormalizedEvent): void {
    this.emitServerDelta(qid, ev);
    // The slim hello carries table schemas + fingerprint; envelope validation (epoch/seq/gap)
    // is the source's job (the ws Subscriber, §5.3) — it precedes every (re)hydrate snapshot.
    if (ev.type === "hello") {
      // Learn this query's per-table column map (PROJECTION-SUPPORT-DESIGN.md §5.2): map each
      // advertised column to its base ColId BY NAME. The hello may carry FEWER columns than the
      // client's schema (a projection) or MORE (an EXPANDED server table mid an
      // `expand-then-contract` migration) — a column the client lacks maps to `-1`, a DROP
      // sentinel the sync layer discards. Register the map so the sync layer scatters the rows
      // into the shared full-width union; a table whose map is an in-order, drop-free full width
      // IS '*' (a verbatim full row) and is left unregistered. Idempotent across re-hydrate epochs.
      for (const t of ev.tables) {
        const full = this.colCounts[t.name];
        if (full === undefined) continue; // unknown/synthetic table → '*' (full presence)
        const index = this.colIndex[t.name];
        const cols = t.columns.map((name) => index?.get(name) ?? -1);
        // Register a non-trivial map; otherwise revert to '*' — and CLEAR any stale map a prior
        // epoch left (a server that expanded then contracted back), so the now-exact rows don't
        // scatter through a `-1`-bearing layout (silent cell corruption).
        if (cols.length !== full || cols.some((c, i) => c !== i)) this.sync.registerProjection(qid, t.name, cols);
        else this.sync.unregisterProjection(qid, t.name);
      }
      return;
    }
    // A `snapshot` is the seq-0 baseline — initial OR a re-hydrate under a new epoch (§5.3). Both
    // go through `rehydrate` (a footprint DIFF): for the first one the footprint is empty so it
    // degenerates to all-adds; for a re-hydrate it removes rows that left during the gap. A
    // `batch` is an incremental delta → `applyBatch`.
    const muts = ev.type === "snapshot" ? this.sync.rehydrate(qid, ev.ops) : this.sync.applyBatch(qid, ev.ops);
    if (ev.type !== "snapshot") {
      if (muts.length) void this.local.mutate(muts); // incremental delta → local engine → views
      return;
    }
    // A snapshot is the query's hydration point. Flip its views to `complete` BEFORE folding the rows
    // (the Store retires the SSR seed only once the query is authoritative), and capture the local
    // qids that transition into hydrated on THIS snapshot so the forwarder stamps their fold `catchUp`
    // — the first result set is delivered by mutating the engine, i.e. as a `batch`, not a `snapshot`.
    // (An already-live view reading the same rows sees them as a genuine incremental add, so it is NOT
    // in this set and folds a normal batch.) Order mirrors the optimistic backend (hydrate → complete,
    // THEN the catch-up delta).
    const wasHydrated = new Set(this.hydrated);
    this.markSubHydrated(qid);
    let newly: Set<QueryId> | null = null;
    for (const localQid of this.hydrated) {
      if (!wasHydrated.has(localQid)) (newly ??= new Set()).add(localQid);
    }
    this.catchUpQids = newly;
    try {
      if (muts.length) void this.local.mutate(muts); // → local engine → flat stream → views
    } finally {
      this.catchUpQids = null;
    }
    // A hydration can fold NOTHING: a 0-row result, or one whose rows are already present in the engine
    // (fully covered by an already-hydrated sibling → the shared rows dedup to 0 net muts). Then no
    // batch was emitted above, so the Store never saw the hydration point and the SSR seed would stay
    // stuck. Signal it explicitly with an EMPTY catch-up per just-hydrated qid — the Store retires the
    // seed and reveals whatever is already in the view's tree.
    if (!muts.length && newly) {
      for (const localQid of newly) this.handler(localQid, { type: "batch", events: [], catchUp: true });
    }
  }

  private emitServerDelta(sourceQid: QueryId, ev: NormalizedEvent): void {
    if (!this.devObservers.size) return;
    for (const qid of this.localQidsForSource(sourceQid)) {
      for (const o of this.devObservers) o.onServerDelta?.(qid, { format: "normalized", event: ev });
    }
  }

  private localQidsForSource(sourceQid: QueryId): QueryId[] {
    const key = this.sourceToRemote.get(sourceQid);
    const sub = key ? this.remoteSubs.get(key) : undefined;
    if (!sub) return [sourceQid];
    const localQids = [...sub.localQids.keys()];
    return localQids.length ? localQids : [sourceQid];
  }

  private retainRemote(retainQid: QueryId, remote: RemoteQuery, localQueryId: QueryId | undefined = retainQid): void {
    const key = remoteKey(remote);
    let sub = this.remoteSubs.get(key);
    let isNew = false;
    if (!sub) {
      sub = { sourceQid: retainQid, remote, refCount: 0, localQids: new Map(), hydrated: false };
      this.remoteSubs.set(key, sub);
      this.sourceToRemote.set(sub.sourceQid, key);
      isNew = true;
    }
    sub.refCount++;
    if (localQueryId !== undefined) {
      sub.localQids.set(localQueryId, (sub.localQids.get(localQueryId) ?? 0) + 1);
      if (sub.hydrated) this.hydrated.add(localQueryId);
      else this.hydrated.delete(localQueryId);
      this.recomputeResultType(localQueryId);
    }
    this.localToRemote.set(retainQid, key);
    this.remoteRetainToLocal.set(retainQid, localQueryId);
    if (isNew) this.source.registerQuery(sub.sourceQid, remote);
  }

  private releaseRemote(retainQid: QueryId): QueryId | undefined {
    const key = this.localToRemote.get(retainQid);
    if (!key) return undefined;
    this.localToRemote.delete(retainQid);
    const localQueryId = this.remoteRetainToLocal.get(retainQid);
    this.remoteRetainToLocal.delete(retainQid);
    const sub = this.remoteSubs.get(key);
    if (!sub) return undefined;
    sub.refCount--;
    if (localQueryId !== undefined) {
      const refs = (sub.localQids.get(localQueryId) ?? 0) - 1;
      if (refs > 0) {
        sub.localQids.set(localQueryId, refs);
      } else {
        sub.localQids.delete(localQueryId);
        if (!this.hasRemoteDependency(localQueryId)) {
          this.hydrated.add(localQueryId);
          this.recomputeResultType(localQueryId);
        }
      }
    }
    if (sub.refCount > 0) return undefined;
    this.source.unregisterQuery(sub.sourceQid);
    this.sourceToRemote.delete(sub.sourceQid);
    this.remoteSubs.delete(key);
    return sub.sourceQid;
  }

  private markSubHydrated(sourceQid: QueryId): void {
    const key = this.sourceToRemote.get(sourceQid);
    if (!key) return;
    const sub = this.remoteSubs.get(key);
    if (!sub || sub.hydrated) return;
    sub.hydrated = true;
    for (const localQid of sub.localQids.keys()) {
      this.hydrated.add(localQid);
      this.recomputeResultType(localQid);
    }
  }

  private hasRemoteDependency(localQid: QueryId): boolean {
    for (const sub of this.remoteSubs.values()) {
      if (sub.localQids.has(localQid)) return true;
    }
    return false;
  }

  private setResultType(qid: QueryId, rt: ResultType): void {
    if (this.resultTypes.get(qid) === rt) return;
    this.resultTypes.set(qid, rt);
    this.resultTypeHandler(qid, rt);
  }

  private recomputeResultType(qid: QueryId): void {
    this.setResultType(qid, this.hydrated.has(qid) ? "complete" : "unknown");
  }
}

interface RemoteSub {
  sourceQid: QueryId;
  remote: RemoteQuery;
  refCount: number;
  localQids: Map<QueryId, number>;
  hydrated: boolean;
}

function remoteKey(remote: RemoteQuery): string {
  return stableJson([remote.name, remote.args]);
}

function stableJson(value: unknown): string {
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(stableJson).join(",")}]`;
  const obj = value as Record<string, unknown>;
  return `{${Object.keys(obj)
    .sort()
    .map((k) => `${JSON.stringify(k)}:${stableJson(obj[k])}`)
    .join(",")}}`;
}

/** A local-first {@link Store} whose base tables are fed by a server's normalized stream.
 *  The returned Store is the ordinary `@rindle/client` Store — `store.query…materialize()` and
 *  `store.write(…)` work as always; only the backend composition differs. */
export function createNormalizedStore<S extends ColsMap>(
  schema: Schema<S>,
  source: NormalizedSource,
): Store<S> {
  return new Store(schema, new NormalizedBackend(schema, source));
}
