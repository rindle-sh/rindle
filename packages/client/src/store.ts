// The Store — backend-agnostic glue (WASM-CLIENT-DESIGN.md §2). Holds the typed schema +
// a `Backend`, exposes `store.query.<table>…materialize()` and `store.write(tx => …)`, and
// routes the backend's per-query `ChangeEvent` stream into per-query `ArrayView`s.
//
// It never knows whether the backend is the in-process WASM engine or a remote server —
// both speak `registerQuery` / `mutate` / `onEvent`. The Store owns: query-id assignment,
// building each `ArrayView` (typed from the schema, so json columns parse), dispatching
// hello/snapshot/batch events, and turning object-shaped writes into positional mutations.

import type { Ast } from "./ast.ts";
import { stableKey } from "./key.ts";
import { queries, type Query, type QueryRoot } from "./query.ts";
import type { ColsMap, InsertOf, RowOf, Schema } from "./schema.ts";
import type { Backend, ChangeEvent, ColType, FlatChange, Mutation, QueryId, RemoteQuery, ResultType, WireSchema, WireValue } from "./types.ts";
import { type ArrayView, type ChangePhase, FlatArrayView, type SingularArrayView, SingularView, type ViewChangeListener, type ViewTypes } from "./view.ts";

/** One query's SSR snapshot, keyed by its `viewKey` ({@link stableKey} of the AST): the
 *  pre-projected first-paint `rows` plus the `cvMin` watermark they reflect (SSR-DESIGN.md §6.2).
 *  Serializable as-is into the HTML — `rows` are already JSON values (json columns parsed). */
export interface DehydratedQuery {
  rows: unknown[];
  cvMin: number;
}

/** The whole dehydrated cache: every preloaded query's snapshot, keyed by `viewKey`. The server
 *  builds it with {@link Store.dehydrate}; the browser seeds it with {@link Store.hydrate}. */
export type DehydratedState = Record<string, DehydratedQuery>;

/** A single assembled (nested-by-name) row from `POST /query` (SSR-DESIGN.md §3.3): the cells
 *  under `cols`, each in-view relationship inlined by its alias (a nested array / object, or a
 *  scalar for a `countAs` aggregate). {@link Store.assembleSnapshot} converts these to the
 *  view's projected result shape. */
export interface AssembledNode {
  cols: Record<string, WireValue>;
  [rel: string]: unknown;
}

/** The write transaction handed to `store.write(tx => …)`. Rows are objects keyed by column;
 *  the Store positionalizes them (and stringifies json columns) before the backend sees them.
 *
 *  `add` takes an {@link InsertOf} row — a nullable column may be omitted (it is filled with `null`,
 *  design 206 §7). `remove`/`edit` take a full {@link RowOf} row: they identify an EXISTING row, so
 *  every column (nullable ones as their actual `T | null` value) must be present. */
export interface WriteTx<S extends ColsMap> {
  add<N extends keyof S & string>(table: N, row: InsertOf<S[N]>): void;
  remove<N extends keyof S & string>(table: N, row: RowOf<S[N]>): void;
  edit<N extends keyof S & string>(table: N, oldRow: RowOf<S[N]>, newRow: RowOf<S[N]>): void;
}

export interface CachedQueryView<Q extends Query<any, any, any>> {
  readonly view: ReturnType<Q["materialize"]>;
  /** Retain this query's named remote footprint and release it later. Ad-hoc local queries
   *  return a no-op release function. */
  retain(query: Q): () => void;
  destroy(): void;
}

export interface SyncQueryLease {
  readonly resultType: ResultType;
  subscribe(listener: () => void): () => void;
  release(): void;
}

interface SyncLeaseState {
  resultType: ResultType;
  listeners: Set<() => void>;
  released: boolean;
  seedKey?: string;
}

/** One live materialized view's read-only summary for a devtools pane (DEBUG-TOOLS-BROWSER-DESIGN
 *  §4.2 — "surface, not instrument"). All fields are already held by the {@link Store}; this is the
 *  single read-only accessor over the otherwise-private `views`/`asts` maps. */
export interface QueryInspect {
  /** The Store-assigned query id (also the backend's qid — the Store passes it straight through). */
  qid: QueryId;
  /** The query's AST (`Store.asts`), for the inspector's pretty-print / table-derivation. */
  ast: Ast;
  /** The view's SERVER-CHANNEL state (`unknown` while loading, `complete` once authoritative). */
  resultType: ResultType;
  /** Current materialized row count (`view.data.length`). */
  rowCount: number;
  /** A capped peek at the projected rows (reference-stable objects off the live view). */
  sample: readonly unknown[];
}

/** A frozen snapshot of the Store's live query state for a devtools pane ({@link Store.__inspect}). */
export interface StoreInspect {
  queries: QueryInspect[];
}

export class Store<S extends ColsMap> {
  /** Type-safe query entry: `store.query.issue.where.closed(false).materialize()`. */
  readonly query: QueryRoot<S>;

  private readonly schema: Schema<S>;
  private readonly backend: Backend;
  private nextId = 1;
  private readonly views = new Map<QueryId, FlatArrayView>();
  private readonly asts = new Map<QueryId, Ast>();
  private readonly syncLeases = new Map<QueryId, SyncLeaseState>();
  // SSR seeds (SSR-DESIGN.md §6), keyed by `viewKey`: a view materialized for one of these is
  // seeded for first paint; a seed is consumed (dropped) the moment its query's first live
  // SNAPSHOT lands — NOT its `hello` — so the seed bridges the `hello`→snapshot gap (the live
  // data arrives on the snapshot, a round-trip after the hello) and later mounts of a now-live
  // query don't re-seed stale SSR data.
  private readonly seeds = new Map<string, DehydratedQuery>();
  // Public per-query change subscribers ({@link subscribeChanges}). `undefined` until the first
  // subscription, so an app that never narrates (nor attaches devtools) pays one undefined-check per
  // routed event and nothing else — no global singleton, no hot-path instrumentation. Each listener
  // also receives the post-fold view for the qid (always the plural {@link ArrayView}, even for a
  // `.one()` query — the Store retains the list-shaped view, not the SingularView wrapper).
  private changeListeners?: Set<(qid: QueryId, ev: ChangeEvent, view?: ArrayView<unknown>) => void>;
  // How many active change subscribers asked for removed subtrees ({@link subscribeChanges} opts).
  // `> 0` ⇒ the view reconstructs each evicted subtree onto its `remove` op before fan-out. A plain
  // counter: the cost is paid only while at least one consumer wants it, and only on real evictions.
  private removedSubtreeWanted = 0;
  // Public per-query {@link ResultType} subscribers ({@link subscribeResultType}). `undefined` until
  // the first subscription. Devtools and any status-driven layer ride this instead of a private tap.
  private resultTypeListeners?: Set<(qid: QueryId, rt: ResultType) => void>;
  // Whether the backend drives a per-query resultType lifecycle (`onResultType`). When it does, a
  // REMOTE query is marked PENDING (`unknown`) at register (`registerMaterialized`) so its synchronous
  // pre-sync snapshot — the optimistic/wasm backend fetches local, not-yet-synced state INSIDE
  // `registerQuery` — can't be mistaken for the authoritative answer and retire the SSR seed. When it
  // doesn't, every view stays `complete` and the first snapshot IS authoritative (unchanged behavior).
  private readonly hasResultTypeLifecycle: boolean;
  // Cross-view-atomic notification (the `Backend.onCommitBoundary` contract): while the backend is
  // delivering one commit's coherent multi-query batch, fold every affected view but DEFER each
  // view's subscriber notification, collecting the changed views here; at the commit's `end` flush
  // them all. So when any view's subscriber runs, every sibling view touched by the same commit has
  // already folded — a callback that re-reads another view sees post-commit data. `> 0` ⇒ inside a
  // commit (defer); `0` ⇒ notify inline (the prior behavior, and what a backend with no commit
  // boundary always gets). A depth counter (not a bool) is robust to any future nesting.
  private commitDepth = 0;
  private readonly pendingFlush = new Set<FlatArrayView>();
  // The raw change-stream frames ({@link subscribeChanges}) buffered during a commit, delivered
  // together at the boundary alongside the view flush — so a change listener (narrator/devtools)
  // that re-reads ANY view also sees post-commit state, the same cross-view-atomic guarantee view
  // subscribers get. Only filled while a commit is open AND a change listener exists; otherwise it
  // stays empty and untouched (the no-narrator hot path is one undefined-check, as before).
  private readonly pendingChanges: Array<[QueryId, ChangeEvent]> = [];

  constructor(schema: Schema<S>, backend: Backend) {
    this.schema = schema;
    this.backend = backend;
    this.hasResultTypeLifecycle = typeof backend.onResultType === "function";
    this.backend.onEvent((qid, ev) => this.onEvent(qid, ev));
    // The in-process engine brackets each commit's multi-query delivery so every affected view
    // folds before any subscriber is notified (see `commitDepth`/`pendingFlush`). A backend with no
    // such boundary never enters deferred mode, so its views notify inline exactly as before.
    this.backend.onCommitBoundary?.((phase) => {
      if (phase === "begin") {
        this.commitDepth++;
      } else if (this.commitDepth > 0 && --this.commitDepth === 0) {
        this.flushCommit();
      }
    });
    // Route the backend's per-query lifecycle onto its view (`view.resultType`). Backends without a
    // lifecycle omit `onResultType`, leaving every view `complete`. A devtools tap mirrors the
    // transition (it never displaces this single handler).
    this.backend.onResultType?.((qid, rt) => {
      this.views.get(qid)?.setResultType(rt);
      const sync = this.syncLeases.get(qid);
      if (sync && sync.resultType !== rt) {
        sync.resultType = rt;
        if (rt === "complete" && sync.seedKey !== undefined) this.seeds.delete(sync.seedKey);
        for (const listener of sync.listeners) listener();
      }
      if (this.resultTypeListeners) for (const l of this.resultTypeListeners) l(qid, rt);
    });
    // `store.query` is the LOCAL builder (201-LOCAL-ONLY-TABLES-DESIGN.md §5): it scopes over
    // synced AND local-only tables, so a local query can join the two. Server/named queries use
    // the synced-only `newQueryBuilder`, which excludes local tables.
    this.query = queries(this.schema, (query) => this.materialize(query), { includeLocal: true }) as QueryRoot<S>;
  }

  /** Materialize any fluent query object. Named queries subscribe remotely by `(name,args)`;
   *  ad-hoc builder queries are local-only for local-first backends.
   *
   *  `opts.onChanges` binds a narrator to this view's DIFF stream ({@link ArrayView.onChanges}) — the
   *  per-view seam that replaces filtering the store-global {@link subscribeChanges} by `qid`. It is
   *  wired BEFORE the backend registers the query, so a synchronous backend's first `snapshot` (fired
   *  inside `registerQuery`, before this returns) is delivered too. */
  materialize<Q extends Query<any, any, any>>(
    query: Q,
    opts?: { onChanges?: ViewChangeListener },
  ): ReturnType<Q["materialize"]> {
    const remote = typeof query.name === "string" ? { name: query.name, args: query.args } : undefined;
    return this.registerMaterialized(query.ast(), remote, opts?.onChanges).view as ReturnType<Q["materialize"]>;
  }

  /** One-shot AUTHORITATIVE read: materialize `query`, wait until its result is server-authoritative
   *  ({@link ResultType} `"complete"`), read the data once, then destroy the view — resolving with the
   *  plain result rather than a live subscription. Rejects if the query enters the `"error"` state. Use
   *  it for exports, imports, undo snapshots — anywhere that wants the current answer as a value.
   *
   *  A synchronous local-first backend (wasm/replica) has already delivered the first snapshot inside
   *  {@link materialize}, so the view is `"complete"` on entry and this settles on the next microtask
   *  without ever attaching a listener; a remote backend settles when the first live snapshot lands.
   *  The query is NEVER left subscribed — the view is destroyed before the promise settles either way.
   *  (A remote query that never completes leaves the promise pending, exactly as a `resultType` poll
   *  would; race a timeout at the call site if you need one.) */
  readOnce<Q extends Query<any, any, any>>(query: Q): Promise<ReturnType<Q["materialize"]>["data"]> {
    type Data = ReturnType<Q["materialize"]>["data"];
    const view = this.materialize(query);
    return new Promise<Data>((resolve, reject) => {
      let settled = false;
      let detach: (() => void) | undefined;
      const settle = (): boolean => {
        const rt = view.resultType;
        if (rt !== "complete" && rt !== "error") return false;
        settled = true;
        detach?.();
        if (rt === "error") {
          view.destroy();
          reject(new Error("store.readOnce: query entered the error result state"));
        } else {
          const data = view.data as Data;
          view.destroy();
          resolve(data);
        }
        return true;
      };
      // A synchronous backend is already `complete` here — settle now, never attaching a listener.
      // (`subscribeResultType` never replays on attach, so the pre-check is what covers this case.)
      if (settle()) return;
      detach = this.subscribeResultType((qid) => {
        if (!settled && qid === view.qid) settle();
      });
    });
  }

  /** True when the backend can retain a remote named query independently from the local
   *  materialized AST view. React uses this to keep one local view per AST while still sending
   *  every mounted `(name,args)` lease through the backend. */
  canRetainRemoteQueries(): boolean {
    return (
      typeof this.backend.retainRemoteQuery === "function" &&
      typeof this.backend.releaseRemoteQuery === "function"
    );
  }

  /** Build one local AST view, with remote syncing retained separately through the returned
   *  handle. This is a lower-level API for UI bindings; ordinary app code should keep using
   *  `materialize(query)`. */
  createCachedQueryView<Q extends Query<any, any, any>>(query: Q): CachedQueryView<Q> {
    const { qid, view } = this.registerMaterialized(query.ast(), undefined);
    let destroyed = false;
    return {
      view: view as ReturnType<Q["materialize"]>,
      retain: (nextQuery: Q) => this.retainRemote(nextQuery, qid),
      destroy: () => {
        if (destroyed) return;
        destroyed = true;
        view.destroy();
      },
    };
  }

  /** Retain a named remote query purely for normalized/local-first coverage. This does not
   *  register or materialize the query AST locally, so React can keep server sync coverage alive
   *  without subscribing to the broad coverage result tree. */
  retainSyncQuery<Q extends Query<any, any, any>>(query: Q): SyncQueryLease {
    if (!this.canRetainRemoteQueries()) {
      throw new Error(
        "store.retainSyncQuery: this backend cannot retain a remote query without a local view.",
      );
    }
    const ast = query.ast();
    const remote = typeof query.name === "string" ? { name: query.name, args: query.args } : undefined;
    if (!remote) {
      throw new Error("store.retainSyncQuery: sync-only coverage requires a named query.");
    }
    const qid = this.nextId++;
    const state: SyncLeaseState = { resultType: "unknown", listeners: new Set(), released: false, seedKey: stableKey(ast) };
    this.syncLeases.set(qid, state);
    try {
      this.backend.retainRemoteQuery?.(qid, remote, qid, ast);
    } catch (e) {
      this.syncLeases.delete(qid);
      throw e;
    }
    return {
      get resultType() {
        return state.resultType;
      },
      subscribe: (listener: () => void) => {
        state.listeners.add(listener);
        listener();
        return () => {
          state.listeners.delete(listener);
        };
      },
      release: () => {
        if (state.released) return;
        state.released = true;
        this.syncLeases.delete(qid);
        this.backend.releaseRemoteQuery?.(qid);
      },
    };
  }

  /** Apply a batch of mutations (object rows → positional). Resolves when the backend has
   *  accepted them (local: applied; remote: sent). The resulting view updates flow back via
   *  the backend's event stream. */
  write(fn: (tx: WriteTx<S>) => void): Promise<void> {
    return Promise.resolve(this.backend.mutate(this.collectMutations(fn)));
  }

  /** Direct-commit write to LOCAL-only tables (`201-LOCAL-ONLY-TABLES-DESIGN.md` §6): the
   *  client-authoritative path for selection state, draft text, view prefs, scratch rows. It
   *  bypasses the optimistic pending stack entirely — a local table is untracked, so it never
   *  rebases, reverts, or waits on a server confirmation (it "moves on its own").
   *
   *  Rejects a synced/tracked table (M2): a direct write to one would be un-applied on the very
   *  next server rewind. Local writes also must NOT live inside a replayable mutator (M1) — the
   *  server runs the mutator from `args` alone and cannot see local tables; use this instead.
   *  Same keyed `WriteTx` shape as {@link write}. `async` so the no-seam / M2 guards surface as a
   *  REJECTED promise (the `Promise<void>` contract) rather than a synchronous throw that escapes a
   *  caller's `.catch` and crashes the event handler / render frame. */
  async writeLocal(fn: (tx: WriteTx<S>) => void): Promise<void> {
    if (!this.backend.writeLocal) {
      throw new Error(
        "store.writeLocal: this backend has no local-write seam (local-only tables need the wasm/optimistic backend).",
      );
    }
    const muts = this.collectMutations(fn);
    // Defense in depth (M2): a clear Store-level rejection of any non-local table, before the
    // backend's own chokepoint guard. Local writes are authoritative-for-themselves only.
    for (const m of muts) {
      if (!this.schema.tables[m.table]?.local) {
        throw new Error(
          `store.writeLocal: "${m.table}" is not a local-only table — use store.write or a named mutator (M2).`,
        );
      }
    }
    this.backend.writeLocal(muts);
  }

  /** Drain a keyed `WriteTx` callback into positional {@link Mutation}s (shared by
   *  {@link write} / {@link writeLocal}; json columns stringified, rows in column order). */
  private collectMutations(fn: (tx: WriteTx<S>) => void): Mutation[] {
    const muts: Mutation[] = [];
    const tx = {
      add: (t: string, row: Record<string, unknown>) =>
        muts.push({ op: "add", table: t, row: this.positionalize(t, row) }),
      remove: (t: string, row: Record<string, unknown>) =>
        muts.push({ op: "remove", table: t, row: this.positionalize(t, row) }),
      edit: (t: string, o: Record<string, unknown>, n: Record<string, unknown>) =>
        muts.push({ op: "edit", table: t, old: this.positionalize(t, o), new: this.positionalize(t, n) }),
    } as unknown as WriteTx<S>;
    fn(tx);
    return muts;
  }

  // --- SSR (SSR-DESIGN.md §6) ---------------------------------------------------

  /** Seed a query's first-paint snapshot from a `POST /query` response (server side): convert the
   *  assembled rows to the view's projected shape and stash them by `viewKey`. A view materialized
   *  for this AST (during the synchronous render) reads the seed; {@link dehydrate} serializes it. */
  seedAssembled(ast: Ast, rows: AssembledNode[], cvMin: number): void {
    this.seeds.set(stableKey(ast), { rows: this.assembleSnapshot(ast, rows), cvMin });
  }

  /** The dehydrated first-paint cache for every preloaded query — embed it in the HTML and pass it
   *  to {@link hydrate} in the browser (SSR-DESIGN.md §6.2). */
  dehydrate(): DehydratedState {
    return Object.fromEntries(this.seeds);
  }

  /** Seed the browser store from the server's {@link dehydrate} output (SSR-DESIGN.md §6.2): each
   *  view materialized for a hydrated AST shows these rows until its first live `hello` reconciles. */
  hydrate(state: DehydratedState): void {
    for (const [key, snap] of Object.entries(state)) this.seeds.set(key, snap);
  }

  /** A query's hydrated first-paint snapshot, by `viewKey` — what React's `getServerSnapshot`
   *  reads so an SSR render (and the matching client hydration pass) sees the seeded rows without
   *  opening a subscription. */
  seedSnapshot(viewKey: string): DehydratedQuery | undefined {
    return this.seeds.get(viewKey);
  }

  primaryKeyFor(table: string): readonly string[] {
    const meta = this.schema.tables[table];
    if (!meta) throw new Error(`unknown table: ${table}`);
    return meta.primaryKey;
  }

  /** Convert assembled (nested-by-name) rows (SSR-DESIGN.md §3.3) into the view's projected result
   *  shape: spread `cols` (parsing json columns), recurse into each relationship by its alias
   *  (plural → array, `.one()` → object/null, `countAs` → bare scalar). */
  assembleSnapshot(ast: Ast, rows: AssembledNode[]): unknown[] {
    return rows.map((row) => this.assembleNode(ast, row));
  }

  private assembleNode(ast: Ast, node: AssembledNode): Record<string, unknown> {
    const cols = this.columns(ast.table);
    const out: Record<string, unknown> = {};
    for (const [name, v] of Object.entries(node.cols ?? {})) {
      out[name] = cols[name]?.type === "json" && typeof v === "string" ? JSON.parse(v) : v;
    }
    for (const sub of ast.related ?? []) {
      const alias = sub.subquery.alias;
      if (alias === undefined || !(alias in node)) continue;
      const child = node[alias];
      if (Array.isArray(child)) {
        out[alias] = child.map((c) => this.assembleNode(sub.subquery, c as AssembledNode));
      } else if (child !== null && typeof child === "object") {
        out[alias] = this.assembleNode(sub.subquery, child as AssembledNode); // .one() singular
      } else {
        out[alias] = child; // a `countAs` scalar aggregate, or a null singular relationship
      }
    }
    return out;
  }

  // --- internals ---------------------------------------------------------------

  private registerMaterialized(
    ast: Ast,
    remote?: RemoteQuery,
    onChanges?: ViewChangeListener,
  ): { qid: QueryId; view: ArrayView<unknown> | SingularArrayView<unknown> } {
    const qid: QueryId = this.nextId++;
    this.asts.set(qid, ast);
    // Pre-create the view (PENDING) so `materialize` is synchronous for ANY backend — a remote
    // backend's `hello` arrives async (the view reads as `[]` until then). A synchronous backend
    // (wasm/replica) resets it during `registerQuery` below, so it returns already-hydrated. The
    // view carries its `qid` (exposed as `view.qid`) so a consumer can correlate it with the raw
    // change stream straight off `materialize(query).qid`.
    const view = this.views.get(qid) ?? this.views.set(qid, new FlatArrayView(undefined, undefined, qid)).get(qid)!;
    // Wire teardown: `destroy()` must unregister the query from the backend and drop our routing
    // entry. Otherwise the engine keeps emitting events for the destroyed query and the Store
    // routes them to the now-empty view — which throws on the next Child/remove ("parent not
    // found") and, because dispatch is a single loop, aborts delivery to sibling queries too.
    // This bites whenever a query is re-materialized (e.g. a changed limit/filter rebuilds it).
    const baseDestroy = view.destroy.bind(view);
    view.destroy = () => {
      if (this.views.delete(qid)) {
        this.asts.delete(qid);
        this.backend.unregisterQuery(qid);
      }
      baseDestroy();
    };
    // Bind the narrator BEFORE `registerQuery` fires (a synchronous backend dispatches this query's
    // first `hello`+`snapshot` inside it) so the initial snapshot reaches the change listener too.
    if (onChanges) view.onChanges(onChanges);
    // SSR first paint (SSR-DESIGN.md §6): seed the view — and, for a lifecycle-backed REMOTE query,
    // mark it PENDING (`unknown`) — BEFORE `registerQuery`, because a synchronous optimistic/wasm
    // backend fires this query's first `hello`+`snapshot` from LOCAL, not-yet-synced state INSIDE that
    // call. Doing both here first means (a) that pre-sync snapshot finds the seed already applied (so
    // `data` shows it, not an empty tree) and (b) the view already reads `unknown`, so `retireSeedIfLive`
    // KEEPS the seed through it — on the optimistic backend the real hydration point is the later
    // `catchUp` batch (which flips the query to `complete` FIRST, then folds the authoritative rows).
    // A LOCAL query is authoritative at register (its local snapshot IS the answer), so it keeps the
    // default `complete`; a lifecycle-LESS backend has no `onResultType`, so every view stays `complete`
    // and its first snapshot retires the seed exactly as before.
    const seed = this.seeds.get(stableKey(ast));
    if (seed) view.seed(seed.rows);
    if (remote !== undefined && this.hasResultTypeLifecycle) view.setResultType("unknown");
    // If the backend rejects the registration (E3: a remote query naming a local-only table), roll
    // back the per-qid state we just created — otherwise the view + ast entry leak (the caller never
    // gets a handle to `destroy()` them, since the throw aborts before we return).
    try {
      this.backend.registerQuery(qid, ast, remote);
    } catch (e) {
      this.views.delete(qid);
      this.asts.delete(qid);
      throw e;
    }
    // A top-level `.one()` (engine-capped to limit 1) unwraps at the result boundary.
    return { qid, view: ast.one ? new SingularView(view) : view };
  }

  private retainRemote<Q extends Query<any, any, any>>(query: Q, localQueryId: QueryId): () => void {
    const remote = typeof query.name === "string" ? { name: query.name, args: query.args } : undefined;
    if (!remote || !this.canRetainRemoteQueries()) return () => {};
    const qid = this.nextId++;
    this.backend.retainRemoteQuery?.(qid, remote, localQueryId, query.ast());
    let released = false;
    return () => {
      if (released) return;
      released = true;
      this.backend.releaseRemoteQuery?.(qid);
    };
  }

  private onEvent(qid: QueryId, ev: ChangeEvent): void {
    if (ev.type === "hello") {
      const ast = this.asts.get(qid);
      const types = ast ? this.viewTypes(ev.schema, ast) : undefined;
      // Reset the (pre-created or existing) view IN PLACE — first hello OR a re-hydrate (new
      // epoch) — so the materialized reference the caller holds survives a re-subscribe. The SSR
      // seed is KEPT across the reset and retired on the first `snapshot` below (the live data
      // lands then, not on the hello), so a seeded query bridges the gap instead of flashing empty.
      const view = this.views.get(qid) ?? this.views.set(qid, new FlatArrayView(undefined, undefined, qid)).get(qid)!;
      view.reset(ev.schema, types);
    } else if (ev.type === "snapshot") {
      // A snapshot is a hydration point — retire the SSR seed and fold, gated on the query being
      // AUTHORITATIVE. {@link foldHydration} handles the empty-fold case (a re-hydrate to nothing).
      this.foldHydration(qid, ev.adds, "snapshot");
    } else if (ev.catchUp) {
      // A `catchUp` batch is a query's initial hydration delivered as a delta — on the optimistic /
      // normalized backend THIS (not the earlier pre-sync snapshot) is the real hydration point,
      // arriving right after the query flips to `complete`. Retire the seed + fold, phased as a
      // `snapshot` so a narrator's "what CHANGED" default ignores the initial rows.
      this.foldHydration(qid, ev.events, "snapshot");
    } else {
      // A plain (non-catchUp) batch is a post-hydration delta — the seed is long gone by then, and
      // this is the incremental hot path, so it does no seed work at all.
      this.applyAndTrack(qid, ev.events, "batch");
    }
    // Fan the same post-fold frame out to subscribers (narration, devtools, …). Inside a commit
    // bracket the frames are BUFFERED and delivered together at the boundary (after every view has
    // folded), so a listener re-reading ANY view sees post-commit state — the same cross-view-atomic
    // guarantee view subscribers get; outside one they go inline (still after this view's own fold,
    // so the post-fold view passed here — and any re-read of it — reflects the post-apply state, and
    // an opted-in removed subtree the view just attached rides along on the `remove` op).
    // No-op (one undefined-check) when nothing is subscribed.
    if (this.changeListeners) {
      if (this.commitDepth > 0) this.pendingChanges.push([qid, ev]);
      else {
        const view = this.views.get(qid);
        for (const l of this.changeListeners) l(qid, ev, view);
      }
    }
  }

  /** Retire a view's SSR seed — from the view (so `data` switches from the seed to the maintained
   *  tree) AND from the seeds map (so no later mount re-seeds a now-live query) — but ONLY once the
   *  query is AUTHORITATIVE (`resultType === "complete"`). Called BEFORE the fold it accompanies, so
   *  that fold's notify already reflects the live tree with no empty gap. Idempotent.
   *
   *  The gate is the fix for the synchronous optimistic/wasm backend: it fires a query's FIRST snapshot
   *  from LOCAL, not-yet-synced state while the query is still `unknown` (`registerMaterialized` marks a
   *  lifecycle-backed remote view `unknown` up front for exactly this), then delivers the authoritative
   *  rows one event later as a `catchUp` batch — having already flipped the query to `complete`. So the
   *  seed survives the pre-sync snapshot (`unknown` ⇒ skip) and retires on the catch-up (`complete` ⇒
   *  retire). A lifecycle-LESS backend (pure wasm, the SSR one-shot, tests) is `complete` from creation,
   *  so its first snapshot retires the seed exactly as before this gate existed. */
  private retireSeedIfLive(qid: QueryId): boolean {
    const view = this.views.get(qid);
    if (!view || view.resultType !== "complete") return false;
    const retired = view.retireSeed(); // idempotent — false once the seed is already retired
    // Drop the map entry (so no later mount re-seeds a now-live query) only when we actually retired,
    // and only pay the `stableKey` hash while a seed is outstanding (the map is empty on a no-SSR app).
    if (retired && this.seeds.size > 0) {
      const ast = this.asts.get(qid);
      if (ast) this.seeds.delete(stableKey(ast));
    }
    return retired;
  }

  /** Retire the SSR seed (if authoritative) and fold the accompanying hydration delta — BEFORE the
   *  fold so its notify already reflects the live tree with no empty gap. The subtlety: a hydration
   *  can fold NOTHING — a 0-row authoritative result, or one whose rows are already present in `top`
   *  (a query whose result is fully covered by an already-hydrated sibling: the shared rows dedup to
   *  zero net base mutations). Then {@link FlatArrayView.applyChanges} notifies nothing, so the
   *  seed→tree switch would never reach subscribers and the view freezes on the stale seed. Guard
   *  against that: if the seed retired but the fold was a no-op, force the handoff notify (inline, or
   *  via the commit-boundary flush). Flash-safe — the forced notify only fires when there was nothing
   *  to fold, so `data` is already the correct live tree by then. */
  private foldHydration(qid: QueryId, events: FlatChange[], phase: ChangePhase): void {
    const retired = this.retireSeedIfLive(qid);
    const view = this.views.get(qid);
    if (!view) return;
    const deferring = this.commitDepth > 0;
    const changed = view.applyChanges(events, this.removedSubtreeWanted > 0, deferring, phase);
    if (deferring) {
      // Flush at the boundary if the fold changed the tree OR a retire is owed a notify (flush()
      // always notifies, even with no buffered segments).
      if (changed || retired) this.pendingFlush.add(view);
    } else if (retired && !changed) {
      view.notify(); // the fold notified nothing but the seed retired — land the handoff
    }
  }

  /** Fold a batch into its view, then notify now or — inside a commit bracket — defer the view's
   *  notification to the commit boundary, so all sibling views fold first (cross-view-atomic
   *  notification; see `commitDepth`). */
  private applyAndTrack(qid: QueryId, events: FlatChange[], phase: ChangePhase): void {
    const view = this.views.get(qid);
    if (!view) return;
    const deferring = this.commitDepth > 0;
    if (view.applyChanges(events, this.removedSubtreeWanted > 0, deferring, phase) && deferring) {
      this.pendingFlush.add(view);
    }
  }

  /** Deliver everything deferred during the just-ended commit, after every affected view has folded
   *  (cross-view-atomic notification): view subscribers first, then the raw change stream
   *  (narrators/devtools), each frame in arrival order. A throwing listener does not stop the others
   *  — the first error is re-raised only once the whole flush completes (mirroring the backend's
   *  per-query isolation). View subscribers run before change listeners, preserving the per-event
   *  order that held before coalescing (a view's subscribers fired before its change frame). */
  private flushCommit(): void {
    const views = this.pendingFlush.size > 0 ? [...this.pendingFlush] : [];
    if (views.length) this.pendingFlush.clear();
    const changes = this.pendingChanges.length > 0 ? this.pendingChanges.splice(0) : [];
    if (views.length === 0 && changes.length === 0) return;
    let firstError: unknown;
    let hasError = false;
    const note = (e: unknown): void => {
      if (!hasError) {
        hasError = true;
        firstError = e;
      }
    };
    for (const view of views) {
      try {
        view.flush();
      } catch (e) {
        note(e);
      }
    }
    if (this.changeListeners) {
      for (const [qid, ev] of changes) {
        const view = this.views.get(qid);
        for (const l of this.changeListeners) {
          try {
            l(qid, ev, view);
          } catch (e) {
            note(e);
          }
        }
      }
    }
    if (hasError) throw firstError;
  }

  /** Subscribe to the raw per-query {@link ChangeEvent} stream (hello / snapshot / batch) the Store
   *  routes to its views, tagged by `qid`. Fired AFTER the event is folded — and, for a commit that
   *  fans out to several queries (the in-process engine's `onCommitBoundary`), after EVERY view in
   *  that commit has folded — so a listener that re-reads ANY view (its own or a sibling) sees
   *  post-commit state, never a torn mid-commit one. Frames keep their arrival (engine-dispatch)
   *  order, one per affected query. This is the supported way to drive change-derived layers
   *  (e.g. {@link resolveChange} → @rindle/narrator, or a devtools pane) off a live store — attach
   *  BEFORE `materialize` to catch a synchronous backend's first `hello`+`snapshot`.
   *  The per-query view `WireSchema` rides the `hello` frame (also readable via `view.schema`).
   *
   *  The third listener arg is the post-fold {@link ArrayView} for this `qid` (so a template wanting
   *  list context — current `data`, `schema`, `resultType` — needn't look it up). It is ALWAYS the
   *  plural view, even for a top-level `.one()` query: the Store retains the list-shaped view, not the
   *  SingularView wrapper handed back from `materialize`. `undefined` only if the view is mid-teardown.
   *
   *  `opts.removedSubtree` enriches every `remove` op on this stream with the full removed subtree
   *  ({@link FlatOp.node}), so a consumer can resolve a removed row's nested subs exactly as on an
   *  `add` (a bare remove carries only the leaving row). It is reconstructed client-side from the
   *  view — no wire/engine cost — and paid only on real evictions while at least one subscriber asks.
   *
   *  Returns a detach function; multiple listeners may attach. */
  subscribeChanges(
    listener: (qid: QueryId, ev: ChangeEvent, view?: ArrayView<unknown>) => void,
    opts?: { removedSubtree?: boolean },
  ): () => void {
    (this.changeListeners ??= new Set()).add(listener);
    if (opts?.removedSubtree) this.removedSubtreeWanted++;
    return () => {
      if (this.changeListeners?.delete(listener) && opts?.removedSubtree) this.removedSubtreeWanted--;
    };
  }

  /** Subscribe to per-query {@link ResultType} transitions (the server-channel lifecycle the backend
   *  pushes — `unknown` → `complete`, etc.), tagged by `qid`. Fired only on a CHANGE (never replayed
   *  on attach; read `view.resultType` for the current value). The supported seam for a status-driven
   *  layer (a devtools pane). Returns a detach function; multiple listeners may attach. */
  subscribeResultType(listener: (qid: QueryId, rt: ResultType) => void): () => void {
    (this.resultTypeListeners ??= new Set()).add(listener);
    return () => {
      this.resultTypeListeners?.delete(listener);
    };
  }

  // --- dev-only introspection (DEBUG-TOOLS-BROWSER-DESIGN §2/§6.2) --------------
  // A single read-only accessor per the design's "surface, not instrument" rule. Only ever called
  // by `@rindle/devtools` (imported in dev); inert otherwise. The delta + resultType taps devtools
  // also needs are the SUPPORTED `subscribeChanges` / `subscribeResultType` seams above.

  /** A read-only snapshot of every live materialized view (DEBUG-TOOLS-BROWSER-DESIGN §4.2): its
   *  qid, AST, {@link ResultType}, row count, and a capped row sample. Built fresh on each call from
   *  the live `views`/`asts` maps — never cached, never mutating. `sampleRows` caps the per-query
   *  peek (default 50) so a large view doesn't bloat the snapshot. */
  __inspect(sampleRows = 50): StoreInspect {
    const queries: QueryInspect[] = [];
    for (const [qid, view] of this.views) {
      const ast = this.asts.get(qid);
      if (!ast) continue; // a view mid-teardown (ast already dropped) — skip
      const data = view.data;
      queries.push({
        qid,
        ast,
        resultType: view.resultType,
        rowCount: data.length,
        sample: sampleRows >= data.length ? data : data.slice(0, sampleRows),
      });
    }
    return { queries };
  }

  private columns(table: string): Record<string, { type: ColType }> {
    const meta = this.schema.tables[table];
    if (!meta) throw new Error(`unknown table: ${table}`);
    return meta.columns as unknown as Record<string, { type: ColType }>;
  }

  /** An object row → a positional cell array in the table's column order (json → string). */
  private positionalize(table: string, obj: Record<string, unknown>): WireValue[] {
    const cols = this.columns(table);
    return Object.keys(cols).map((name) => {
      const v = obj[name];
      if (cols[name].type === "json" && v != null && typeof v === "object") return JSON.stringify(v);
      return (v ?? null) as WireValue;
    });
  }

  /** The per-level column types parallel to the WireSchema, so the view parses json columns. */
  private viewTypes(ws: WireSchema, ast: Ast): ViewTypes {
    const cols = this.columns(ast.table);
    const columnTypes = ws.columns.map((name) => cols[name]?.type ?? "string");
    const rels: Record<number, ViewTypes> = {};
    for (const rel of ws.relationships) {
      if (!rel.child) continue;
      const sub = (ast.related ?? []).find((r) => r.subquery.alias === rel.name);
      if (sub) rels[rel.slot] = this.viewTypes(rel.child, sub.subquery);
    }
    return { columnTypes, rels };
  }
}
