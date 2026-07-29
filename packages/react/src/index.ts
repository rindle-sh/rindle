import {
  createContext,
  createElement,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  useSyncExternalStore,
} from "react";
import type { ReactNode } from "react";
import {
  createLocalFragmentRefForTable,
  fragmentAst,
  isFragment,
  isFragmentRelationship,
  localQueryReadAst,
  localFragmentReadAst,
  localRootFragmentRefsAst,
  OneShotBackend,
  queryFromAst,
  stableKey,
  Store,
  tableMeta,
} from "@rindle/client";
import type {
  AnyQuery,
  Ast,
  CachedQueryView,
  ColsMap,
  DehydratedState,
  Fragment,
  FragmentData,
  FragmentCoverage,
  FragmentRef,
  LitValue,
  LocalFragmentRef,
  NamedQuery,
  QueryLocalData,
  ResultType,
  Schema,
} from "@rindle/client";

type AnyView = ReturnType<AnyQuery["materialize"]>;
type AnyFragment = Fragment<any, any, any, any>;
type AnyNamedQuery = NamedQuery<any, readonly unknown[], AnyQuery>;

export type QueryData<Q extends AnyQuery> = ReturnType<Q["materialize"]>["data"];
export type RootData<Q extends AnyQuery> = QueryLocalData<Q>;
export type RootRefData<Q extends AnyQuery, F extends Fragment<any, any, any, any>> =
  QueryData<Q> extends readonly unknown[] ? readonly FragmentRef<F>[] : FragmentRef<F> | null;
export interface RootDetails {
  readonly status: ResultType;
}
export type RootResult<Q extends AnyQuery> = readonly [data: RootData<Q>, details: RootDetails];
export type RootRefResult<Q extends AnyQuery, F extends Fragment<any, any, any, any>> =
  readonly [data: RootRefData<Q, F>, details: RootDetails];

export type { ResultType } from "@rindle/client";
export type { Fragment, FragmentData, FragmentRef } from "@rindle/client";
export { fragmentKey } from "@rindle/client";

export interface RindleProps<S extends ColsMap = ColsMap> {
  store: Store<S>;
  children?: ReactNode;
}

interface QueryDescriptor {
  viewKey: string;
  leaseKey: string;
  one: boolean;
}

interface QueryLease {
  id: number;
  viewKey: string;
}

interface SyncLease {
  id: number;
  coverageKey: string;
}

interface MaterializedLease extends QueryLease {
  view: AnyView;
  remote: boolean;
}

interface SplitLease extends QueryLease {
  releaseRemote: () => void;
}

type CacheLease = MaterializedLease | SplitLease;

interface BaseCacheEntry {
  leases: CacheLease[];
  canonicalUnsubscribe: () => void;
  listeners: Set<() => void>;
}

interface SplitCacheEntry extends BaseCacheEntry {
  mode: "split";
  handle: CachedQueryView<AnyQuery>;
  leases: SplitLease[];
  pendingReleases: SplitLease[];
  releaseTimer: ReleaseTimer | undefined;
}

interface MaterializedCacheEntry extends BaseCacheEntry {
  mode: "materialized";
  leases: MaterializedLease[];
  canonical: MaterializedLease;
}

type CacheEntry = SplitCacheEntry | MaterializedCacheEntry;

const EMPTY_ARRAY: readonly never[] = Object.freeze([]);
// Keep just-released coverage alive briefly so a changed filter/limit can re-materialize from the
// local base synchronously while the replacement server lease is still streaming its first answer.
const REACT_CACHE_RELEASE_DELAY_MS = 2_000;

type ReleaseTimer = ReturnType<typeof setTimeout>;

interface QueryCacheOptions {
  releaseDelayMs?: number;
}

function setReleaseTimeout(fn: () => void, ms: number): ReleaseTimer {
  const timer = setTimeout(fn, ms);
  (timer as { unref?: () => void }).unref?.();
  return timer;
}

class RindleContextValue {
  readonly store: Store<ColsMap>;
  readonly cache: QueryCache;
  readonly syncCache: SyncQueryCache;

  constructor(store: Store<ColsMap>) {
    this.store = store;
    this.cache = new QueryCache(store, { releaseDelayMs: REACT_CACHE_RELEASE_DELAY_MS });
    this.syncCache = new SyncQueryCache(store, { releaseDelayMs: REACT_CACHE_RELEASE_DELAY_MS });
  }
}

const RindleContext = createContext<RindleContextValue | null>(null);

export function Rindle<S extends ColsMap>({ store, children }: RindleProps<S>) {
  const value = useMemo(() => new RindleContextValue(store as unknown as Store<ColsMap>), [store]);
  return createElement(RindleContext.Provider, { value }, children);
}

export const RindleProvider = Rindle;

export function useRindleStore<S extends ColsMap = ColsMap>(): Store<S> {
  return useRindleContext().store as unknown as Store<S>;
}

export interface RindleSSRProps<S extends ColsMap = ColsMap> {
  /** The app schema — used to build the transport-less seed {@link Store} that backs the server
   *  render and the matching client hydration pass. */
  schema: Schema<S>;
  /** The dehydrated first-paint cache from the route loader (`ServerStore.dehydrate()`), embedded in
   *  the HTML. Read on BOTH the server render and the client's first (hydration) render. */
  ssrState: DehydratedState;
  /** Boots the live (wasm-backed) client in the BROWSER — the app's `bootClient`. Called once, after
   *  hydration, and must resolve to the live optimistic store. Never invoked during the server
   *  render (SSR seeds are a first-paint concern only). */
  boot: () => Promise<{ store: Store<S> }>;
  children?: ReactNode;
}

/**
 * The SSR→SPA store handoff (SSR-DESIGN.md §6.1). Renders `<Rindle>` with a store that swaps from a
 * transport-less SSR seed to the live engine WITHOUT changing a single `useQuery` caller — bind the
 * app's `schema` + `bootClient` and drop it in above the tree:
 *
 *   - Server render + browser HYDRATION: a seed {@link Store} over a {@link OneShotBackend}, hydrated
 *     from `ssrState`. `useQuery` reads its seed via `getServerSnapshot`, so the server and the
 *     client's first render produce identical markup with NO engine on either side — first paint is
 *     the server-rendered data, never a "Starting…" splash that would break hydration.
 *   - After hydration: `boot()` starts the wasm IVM engine (browser only), its views are seeded from
 *     the SAME snapshot (so the swap shows the SSR rows with no flash), and the live `subscribe`
 *     reconciles — the page is now a live SPA.
 *
 * Framework-agnostic of the app: everything but `schema`/`boot`/`ssrState` is owned here (previously
 * hand-rolled per app as `src/RindleApp.tsx`).
 */
export function RindleSSR<S extends ColsMap>({ schema, ssrState, boot, children }: RindleSSRProps<S>) {
  // Built ONCE from the first-render snapshot (SSR seeds are an initial-load concern only — after
  // hydration the live store owns every read). Backs the server render AND the matching client
  // hydration pass — same seeds in, same markup out.
  const [seedStore] = useState(() => {
    const store = new Store(schema, new OneShotBackend());
    store.hydrate(ssrState);
    return store;
  });

  // The live wasm store — booted only in the browser, only after hydration.
  const [liveStore, setLiveStore] = useState<Store<S> | null>(null);
  // Pin `ssrState`/`boot` through refs so the boot effect runs EXACTLY once (empty deps) yet always
  // sees the latest values — a caller may pass an inline `boot`, which as a dep would re-boot the
  // client every render.
  const ssrStateRef = useRef(ssrState);
  ssrStateRef.current = ssrState;
  const bootRef = useRef(boot);
  bootRef.current = boot;
  useEffect(() => {
    let alive = true;
    void bootRef.current().then((app) => {
      if (!alive) return;
      app.store.hydrate(ssrStateRef.current); // seed the live views so the swap doesn't flash empty
      setLiveStore(app.store);
    });
    return () => {
      alive = false;
    };
  }, []);

  return createElement(Rindle<S>, { store: liveStore ?? seedStore }, children);
}

export function useQuery<Q extends AnyQuery>(query: Q): QueryData<Q> {
  const ctx = useRindleContext();
  const descriptor = useMemo(() => describeQuery(query), [query]);
  const queryRef = useRef(query);
  queryRef.current = query;

  const subscribe = useCallback(
    (onStoreChange: () => void) => {
      const lease = ctx.cache.retain(descriptor.viewKey, queryRef.current);
      const unsubscribe = ctx.cache.subscribe(descriptor.viewKey, onStoreChange);
      return () => {
        unsubscribe();
        ctx.cache.release(lease);
      };
    },
    [ctx.cache, descriptor.leaseKey, descriptor.viewKey],
  );

  const getSnapshot = useCallback(
    () => ctx.cache.snapshot(descriptor.viewKey, descriptor.one) as QueryData<Q>,
    [ctx.cache, descriptor.viewKey, descriptor.one],
  );

  // SSR (SSR-DESIGN.md §6): the server calls `getServerSnapshot` (never `subscribe`), so it must
  // surface the dehydrated/preloaded seed directly — not the live cache, which is never retained
  // on the server. The client's hydration pass reads the same seed, matching the SSR markup.
  const getServerSnapshot = useCallback(
    () => ctx.cache.serverSnapshot(descriptor.viewKey, descriptor.one) as QueryData<Q>,
    [ctx.cache, descriptor.viewKey, descriptor.one],
  );

  return useSyncExternalStore(subscribe, getSnapshot, getServerSnapshot);
}

/** The SERVER-CHANNEL state of a query's view (`@rindle/client` {@link ResultType}): `unknown` while
 *  it loads (not yet server-authoritative), `complete` once the server has answered. A pending
 *  optimistic mutation no longer moves this — that is a separate axis now (FOLDED-MUTATIONS-DESIGN
 *  §7); the `error` variant is reserved and currently unproduced. Shares the same cached/leased view
 *  as {@link useQuery} (so reading both for one query is one subscription), and re-renders only when
 *  the status changes. */
export function useQueryStatus(query: AnyQuery): ResultType {
  const ctx = useRindleContext();
  const descriptor = useMemo(() => describeQuery(query), [query]);
  const queryRef = useRef(query);
  queryRef.current = query;

  const subscribe = useCallback(
    (onStoreChange: () => void) => {
      const lease = ctx.cache.retain(descriptor.viewKey, queryRef.current);
      const unsubscribe = ctx.cache.subscribe(descriptor.viewKey, onStoreChange);
      return () => {
        unsubscribe();
        ctx.cache.release(lease);
      };
    },
    [ctx.cache, descriptor.leaseKey, descriptor.viewKey],
  );

  const getSnapshot = useCallback(() => ctx.cache.resultType(descriptor.viewKey), [ctx.cache, descriptor.viewKey]);

  // SSR: a seeded query is server-authoritative for first paint (`complete`); otherwise `unknown`.
  const getServerSnapshot = useCallback(
    () => ctx.cache.serverResultType(descriptor.viewKey),
    [ctx.cache, descriptor.viewKey],
  );

  return useSyncExternalStore(subscribe, getSnapshot, getServerSnapshot);
}

/** Retain a named server query for normalized/local-first sync coverage without subscribing React
 *  to that query's broad result tree. The returned value is lifecycle state only; it is `unknown`
 *  until the backend reports that the retained coverage has hydrated. */
export function useSyncQuery(query: AnyQuery): ResultType {
  const ctx = useRindleContext();
  const descriptor = useMemo(() => describeQuery(query), [query]);
  const queryRef = useRef(query);
  queryRef.current = query;

  const subscribe = useCallback(
    (onStoreChange: () => void) => {
      const lease = ctx.syncCache.retain(descriptor.leaseKey, queryRef.current);
      const unsubscribe = ctx.syncCache.subscribe(descriptor.leaseKey, onStoreChange);
      return () => {
        unsubscribe();
        ctx.syncCache.release(lease);
      };
    },
    [ctx.syncCache, descriptor.leaseKey],
  );

  const getSnapshot = useCallback(() => {
    const live = ctx.syncCache.resultType(descriptor.leaseKey);
    return live === "unknown" && ctx.cache.serverResultType(descriptor.viewKey) === "complete" ? "complete" : live;
  }, [ctx.cache, ctx.syncCache, descriptor.leaseKey, descriptor.viewKey]);

  const getServerSnapshot = useCallback(
    () => ctx.cache.serverResultType(descriptor.viewKey),
    [ctx.cache, descriptor.viewKey],
  );

  return useSyncExternalStore(subscribe, getSnapshot, getServerSnapshot);
}

/** Run a named root query and expose its local React-facing data. Fragment child relationships are
 *  refs, so child components can keep owning their own local reads. Passing a root fragment as the
 *  final argument switches the result to opaque root refs for that fragment. */
export function useRoot<Q extends AnyQuery>(query: Q): RootResult<Q>;
export function useRoot<Q extends AnyQuery, F extends AnyFragment>(
  query: Q,
  fragment: F,
): RootRefResult<Q, F>;
export function useRoot<Q extends AnyQuery>(
  query: NamedQuery<void, [], Q>,
): RootResult<Q>;
export function useRoot<Q extends AnyQuery, F extends AnyFragment>(
  query: NamedQuery<void, [], Q>,
  fragment: F,
): RootRefResult<Q, F>;
export function useRoot<Args, Ctx extends readonly unknown[], Q extends AnyQuery>(
  query: NamedQuery<Args, Ctx, Q>,
  args: Args,
  ...ctx: Ctx
): RootResult<Q>;
export function useRoot<Args, Ctx extends readonly unknown[], Q extends AnyQuery, F extends AnyFragment>(
  query: NamedQuery<Args, Ctx, Q>,
  args: Args,
  ...ctxAndFragment: [...ctx: Ctx, fragment: F]
): RootRefResult<Q, F>;
export function useRoot<Q extends AnyQuery, F extends AnyFragment>(
  queryOrNamed: Q | AnyNamedQuery,
  ...args: unknown[]
): RootResult<Q> | RootRefResult<Q, F> {
  const { query, fragment } = resolveRootQueryInput(queryOrNamed, args, "useRoot");
  const stableQuery = useStableQuery(query as Q);
  const status = useSyncQuery(stableQuery);
  const data = fragment === undefined
    ? useLocalRootQueryData(stableQuery)
    : useRootRefData(stableQuery, fragment as F);
  const details = useMemo<RootDetails>(() => ({ status }), [status]);
  return useMemo(
    () => [data, details] as const,
    [data, details],
  ) as RootResult<Q> | RootRefResult<Q, F>;
}

function resolveRootQueryInput(
  queryOrNamed: AnyQuery | AnyNamedQuery,
  args: readonly unknown[],
  hookName: string,
): { query: AnyQuery; fragment?: AnyFragment } {
  const last = args[args.length - 1];
  const fragment = isFragment(last) ? last as AnyFragment : undefined;
  const queryArgs = fragment === undefined ? args : args.slice(0, -1);
  if (isNamedQuery(queryOrNamed)) {
    const query = queryArgs.length === 0
      ? (queryOrNamed as unknown as () => AnyQuery)()
      : queryOrNamed(queryArgs[0], ...queryArgs.slice(1));
    return { query, fragment };
  }
  if (queryArgs.length !== 0) throw new Error(`${hookName}(): expected (query) or (query, fragment).`);
  return { query: queryOrNamed, fragment };
}

function isNamedQuery(v: unknown): v is AnyNamedQuery {
  return typeof v === "function"
    && typeof (v as Partial<AnyNamedQuery>).queryName === "string"
    && typeof (v as Partial<AnyNamedQuery>).resolve === "function";
}

function useStableQuery<Q extends AnyQuery>(query: Q): Q {
  const descriptor = describeQuery(query);
  const ref = useRef<{ leaseKey: string; query: Q } | undefined>(undefined);
  if (ref.current === undefined || ref.current.leaseKey !== descriptor.leaseKey) {
    ref.current = { leaseKey: descriptor.leaseKey, query };
  }
  return ref.current.query;
}

function useLocalRootQueryData<Q extends AnyQuery>(
  query: Q,
): RootData<Q> {
  const ctx = useRindleContext();
  const descriptor = useMemo(() => describeQuery(query), [query]);
  const ast = useMemo(
    () => localQueryReadAst(query, (table) => ctx.store.primaryKeyFor(table)),
    [ctx.store, query],
  );
  const localQuery = useMemo(() => queryFromAst(ast), [ast]);
  const localDescriptor = useMemo(() => describeQuery(localQuery), [localQuery]);
  const projection = useMemo(
    () => new LocalFragmentProjection(ast, { key: descriptor.leaseKey, query }, (table) => ctx.store.primaryKeyFor(table)),
    [ctx.store, ast, descriptor.leaseKey, query],
  );

  const subscribe = useCallback(
    (onStoreChange: () => void) => {
      const localLease = ctx.cache.retain(localDescriptor.viewKey, localQuery);
      const unsubscribeLocal = ctx.cache.subscribe(localDescriptor.viewKey, onStoreChange);
      return () => {
        unsubscribeLocal();
        ctx.cache.release(localLease);
      };
    },
    [ctx.cache, localDescriptor.viewKey, localQuery],
  );

  // Stale-while-revalidate: render whatever the local IVM view already holds (synced rows,
  // optimistic writes, partially-covered rows) rather than blanking to empty until the server
  // confirms coverage. `status` (from useSyncQuery) stays a SEPARATE signal callers can gate on;
  // the data itself never waits on the round-trip, so navigating to a view that's locally warm
  // shows rows immediately instead of flashing a loading state. (Eviction-on-release still drops
  // a cold view; a release TTL to keep views warm across nav is future work.)
  const getSnapshot = useCallback(() => {
    if (ctx.cache.serverResultType(descriptor.viewKey) === "complete") {
      return projectLocalRootSnapshot(ctx.cache.serverSnapshot(descriptor.viewKey, descriptor.one), descriptor.one, projection);
    }
    return projectLocalRootSnapshot(ctx.cache.snapshot(localDescriptor.viewKey, descriptor.one), descriptor.one, projection);
  }, [ctx.cache, descriptor.one, descriptor.viewKey, localDescriptor.viewKey, projection]);

  const getServerSnapshot = useCallback(
    () => projectLocalRootSnapshot(ctx.cache.serverSnapshot(descriptor.viewKey, descriptor.one), descriptor.one, projection),
    [ctx.cache, descriptor.one, descriptor.viewKey, projection],
  );

  return useSyncExternalStore(subscribe, getSnapshot, getServerSnapshot) as RootData<Q>;
}

function useRootRefData<Q extends AnyQuery, F extends Fragment<any, any, any, any>>(
  query: Q,
  fragment: F,
): RootRefData<Q, F> {
  const ctx = useRindleContext();
  const descriptor = useMemo(() => describeQuery(query), [query]);
  const ast = useMemo(
    () => localRootFragmentRefsAst(fragment, query, (table) => ctx.store.primaryKeyFor(table)),
    [ctx.store, fragment, query],
  );
  const localQuery = useMemo(() => queryFromAst(ast), [ast]);
  const localDescriptor = useMemo(() => describeQuery(localQuery), [localQuery]);
  const projection = useMemo(
    () => new RootRefProjection(fragment, { key: descriptor.leaseKey, query }, (table) => ctx.store.primaryKeyFor(table)),
    [ctx.store, descriptor.leaseKey, fragment, query],
  );

  const subscribe = useCallback(
    (onStoreChange: () => void) => {
      const localLease = ctx.cache.retain(localDescriptor.viewKey, localQuery);
      const unsubscribeLocal = ctx.cache.subscribe(localDescriptor.viewKey, onStoreChange);
      return () => {
        unsubscribeLocal();
        ctx.cache.release(localLease);
      };
    },
    [ctx.cache, localDescriptor.viewKey, localQuery],
  );

  // Stale-while-revalidate (see useLocalRootQueryData): the root rows render from the live local
  // view as soon as it holds anything; `status` is the separate axis for "server-authoritative yet".
  const getSnapshot = useCallback(() => {
    if (ctx.cache.serverResultType(descriptor.viewKey) === "complete") {
      return projectRootRefSnapshot(ctx.cache.serverSnapshot(descriptor.viewKey, descriptor.one), descriptor.one, projection);
    }
    const raw = ctx.cache.snapshot(localDescriptor.viewKey, descriptor.one);
    return descriptor.one ? projection.projectOne(raw) : projection.projectMany(raw);
  }, [ctx.cache, descriptor.one, descriptor.viewKey, localDescriptor.viewKey, projection]);

  const getServerSnapshot = useCallback(
    () => projectRootRefSnapshot(ctx.cache.serverSnapshot(descriptor.viewKey, descriptor.one), descriptor.one, projection),
    [ctx.cache, descriptor.one, descriptor.viewKey, projection],
  );

  return useSyncExternalStore(subscribe, getSnapshot, getServerSnapshot) as RootRefData<Q, F>;
}

/**
 * Read a {@link Fragment}'s local data from an opaque ref.
 *
 * The query boundary calls {@link useRoot} with a fragment argument to retain the full named
 * coverage query and receive root refs. Descendants call `useFragment` with those refs (or child
 * refs returned by a parent fragment read) to open narrow local-only reads for the fields their
 * fragment owns.
 *
 * `ref` is an opaque token created by {@link useRoot} or returned from another local fragment read.
 * The hook opens a narrow local-only query for this exact fragment and keeps the root coverage
 * lease retained while mounted. Passing a legacy projected data object is unsupported.
 */
export function useFragment<F extends Fragment<any, any, any, any>>(
  fragment: F,
  ref: FragmentRef<F> | null | undefined,
): FragmentData<F> | null {
  return useLocalFragment(fragment, ref);
}

/**
 * Render-prop sugar over {@link useFragment}: does the `null` check once. `from` is a fragment ref
 * (or null/undefined — an absent to-one relationship, an emptied `.one()`, or a row deleted out from
 * under a live read); when the row is present `children(data)` renders, otherwise `fallback` (default
 * nothing). Keeps the per-row subscription isolation — a child-only edit re-renders just this read.
 */
export function Frag<F extends AnyFragment>(
  { of, from, fallback = null, children }: {
    of: F;
    from: FragmentRef<F> | null | undefined;
    fallback?: ReactNode;
    children: (data: FragmentData<F>) => ReactNode;
  },
): ReactNode {
  const data = useFragment(of, from);
  return data == null ? fallback : children(data);
}

function useLocalFragment<F extends Fragment<any, any, any, any>>(
  fragment: F,
  ref: LocalFragmentRef<F> | null | undefined,
): FragmentData<F> | null {
  const ctx = useRindleContext();
  const coverage = ref?.coverage;
  const coverageDescriptor = useMemo(() => (coverage ? describeQuery(coverage.query) : undefined), [coverage]);
  const ast = useMemo(
    () => (ref ? localFragmentReadAst(fragment, ref, (table) => ctx.store.primaryKeyFor(table)) : undefined),
    [ctx.store, fragment, ref],
  );
  const query = useMemo(() => (ast ? queryFromAst(ast) : undefined), [ast]);
  const descriptor = useMemo(() => (query ? describeQuery(query) : undefined), [query]);
  const projection = useMemo(
    () => (coverage ? new LocalFragmentProjection(fragmentAst(fragment), coverage, (table) => ctx.store.primaryKeyFor(table)) : undefined),
    [ctx.store, coverage, fragment],
  );

  const subscribe = useCallback(
    (onStoreChange: () => void) => {
      if (!coverage || !descriptor || !query) return () => {};
      const syncLease = ctx.syncCache.retain(coverage.key, coverage.query);
      const localLease = ctx.cache.retain(descriptor.viewKey, query);
      const unsubscribeSync = ctx.syncCache.subscribe(coverage.key, onStoreChange);
      const unsubscribeLocal = ctx.cache.subscribe(descriptor.viewKey, onStoreChange);
      return () => {
        unsubscribeLocal();
        unsubscribeSync();
        ctx.cache.release(localLease);
        ctx.syncCache.release(syncLease);
      };
    },
    [ctx.cache, ctx.syncCache, coverage, descriptor, query],
  );

  // Stale-while-revalidate (see useLocalRootQueryData): project the fragment's local view as soon
  // as the row exists locally instead of returning null until its coverage is server-complete —
  // otherwise every nested fragment (UserBadge, TagChip, CommentCard, …) flashes empty for a
  // round-trip on each navigation. A field not yet synced simply reads absent, not "loading".
  const getSnapshot = useCallback(() => {
    if (!coverage || !descriptor || !projection) return null;
    if (coverageDescriptor && ctx.cache.serverResultType(coverageDescriptor.viewKey) === "complete") {
      return projectFragmentSeed(ctx, coverage.query.ast(), coverageDescriptor, ref, projection);
    }
    const data = projection.project(ctx.cache.snapshot(descriptor.viewKey, true));
    return data as FragmentData<F> | null;
  }, [ctx, coverage, coverageDescriptor, descriptor, projection, ref]);

  const getServerSnapshot = useCallback(
    () => projectFragmentSeed(ctx, coverage?.query.ast(), coverageDescriptor, ref, projection),
    [ctx, coverage, coverageDescriptor, projection, ref],
  );

  return useSyncExternalStore(subscribe, getSnapshot, getServerSnapshot);
}

function projectRootRefSnapshot(raw: unknown, one: boolean, projection: RootRefProjection): unknown {
  return one ? projection.projectOne(raw) : projection.projectMany(raw);
}

function projectLocalRootSnapshot(raw: unknown, one: boolean, projection: LocalFragmentProjection): unknown {
  return one ? projection.projectOne(raw) : projection.projectMany(raw);
}

function projectFragmentSeed<F extends Fragment<any, any, any, any>>(
  ctx: RindleContextValue,
  coverageAst: Ast | undefined,
  coverageDescriptor: QueryDescriptor | undefined,
  ref: LocalFragmentRef<F> | null | undefined,
  projection: LocalFragmentProjection | undefined,
): FragmentData<F> | null {
  if (!coverageAst || !coverageDescriptor || !ref || !projection) return null;
  const raw = ctx.cache.serverSnapshot(coverageDescriptor.viewKey, coverageDescriptor.one);
  const row = findFragmentSeedRow(coverageAst, raw, ref.table, ref.pk, (table) => ctx.store.primaryKeyFor(table));
  return projection.project(row) as FragmentData<F> | null;
}

function findFragmentSeedRow(
  ast: Ast,
  raw: unknown,
  table: string,
  pk: Readonly<Record<string, LitValue>>,
  primaryKeyFor: (table: string) => readonly string[],
): unknown {
  if (Array.isArray(raw)) {
    for (const row of raw) {
      const found = findFragmentSeedRowInObject(ast, row, table, pk, primaryKeyFor);
      if (found !== null) return found;
    }
    return null;
  }
  return findFragmentSeedRowInObject(ast, raw, table, pk, primaryKeyFor);
}

function findFragmentSeedRowInObject(
  ast: Ast,
  raw: unknown,
  table: string,
  pk: Readonly<Record<string, LitValue>>,
  primaryKeyFor: (table: string) => readonly string[],
): unknown {
  if (raw === null || raw === undefined || typeof raw !== "object") return null;
  const row = raw as Record<string, unknown>;
  if (ast.table === table && rowMatchesPk(row, pk, primaryKeyFor(table))) return row;
  for (const rel of ast.related ?? []) {
    if (rel.subquery.aggregate !== undefined) continue;
    const alias = rel.subquery.alias;
    if (alias === undefined) continue;
    const found = findFragmentSeedRow(rel.subquery, row[alias], table, pk, primaryKeyFor);
    if (found !== null) return found;
  }
  return null;
}

function rowMatchesPk(row: Record<string, unknown>, pk: Readonly<Record<string, LitValue>>, primaryKey: readonly string[]): boolean {
  for (const col of primaryKey) {
    if (!Object.prototype.hasOwnProperty.call(row, col)) return false;
    if (stableKey(row[col]) !== stableKey(pk[col])) return false;
  }
  return true;
}

class LocalFragmentProjection {
  private readonly manyCache = new WeakMap<object, readonly unknown[]>();
  private readonly cache = new WeakMap<object, unknown>();
  private readonly refCache = new Map<string, LocalFragmentRef>();
  private readonly ast: Ast;
  private readonly coverage: FragmentCoverage;
  private readonly primaryKeyFor: (table: string) => readonly string[];

  constructor(
    ast: Ast,
    coverage: FragmentCoverage,
    primaryKeyFor: (table: string) => readonly string[],
  ) {
    this.ast = ast;
    this.coverage = coverage;
    this.primaryKeyFor = primaryKeyFor;
  }

  projectOne(raw: unknown): unknown {
    return this.project(raw);
  }

  projectMany(raw: unknown): readonly unknown[] {
    if (!Array.isArray(raw)) return EMPTY_ARRAY;
    const cached = this.manyCache.get(raw);
    if (cached) return cached;
    const out = raw.map((row) => this.project(row));
    this.manyCache.set(raw, out);
    return out;
  }

  project(raw: unknown): unknown {
    if (raw === null || raw === undefined || typeof raw !== "object") return null;
    const cached = this.cache.get(raw);
    if (cached !== undefined) return cached;

    const out = this.projectRow(this.ast, raw);
    this.cache.set(raw, out);
    return out;
  }

  private projectRow(ast: Ast, raw: unknown): unknown {
    if (raw === null || raw === undefined || typeof raw !== "object") return null;
    const row = raw as Record<string, unknown>;
    const out: Record<string, unknown> = {};
    const relationshipAliases = new Set((ast.related ?? []).map((rel) => rel.subquery.alias).filter((a): a is string => a !== undefined));
    if (ast.select === undefined) {
      for (const [key, value] of Object.entries(row)) {
        if (!relationshipAliases.has(key)) out[key] = value;
      }
    } else {
      for (const col of ast.select) {
        if (Object.prototype.hasOwnProperty.call(row, col)) out[col] = row[col];
      }
    }

    for (const rel of ast.related ?? []) {
      const alias = rel.subquery.alias;
      if (alias === undefined) continue;
      const value = row[alias];
      if (rel.subquery.aggregate !== undefined) {
        out[alias] = value;
        continue;
      }
      out[alias] = isFragmentRelationship(rel) ? this.projectRelatedRefs(rel.subquery, value) : this.projectInline(rel.subquery, value);
    }

    return out;
  }

  private projectInline(childAst: Ast, value: unknown): unknown {
    if (Array.isArray(value)) return value.map((row) => this.projectRow(childAst, row));
    if (value === null || value === undefined) return value ?? null;
    return this.projectRow(childAst, value);
  }

  private projectRelatedRefs(childAst: Ast, value: unknown): unknown {
    if (Array.isArray(value)) return value.map((row) => this.refForRow(childAst.table, row));
    if (value === null || value === undefined) return value ?? null;
    return this.refForRow(childAst.table, value);
  }

  private refForRow(table: string, value: unknown): LocalFragmentRef {
    if (value === null || typeof value !== "object") {
      throw new Error(`useFragment(): cannot build a local fragment ref for "${table}" from a non-object row.`);
    }
    const row = value as Record<string, unknown>;
    const pk: Record<string, LitValue> = {};
    for (const col of this.primaryKeyFor(table)) {
      if (!Object.prototype.hasOwnProperty.call(row, col)) {
        throw new Error(`useFragment(): local relationship row for "${table}" is missing primary key column "${col}".`);
      }
      pk[col] = row[col] as LitValue;
    }
    const key = stableKey({ table, pk });
    const cached = this.refCache.get(key);
    if (cached) return cached;
    const ref = createLocalFragmentRefForTable(table, pk, this.coverage);
    this.refCache.set(key, ref);
    return ref;
  }
}

class RootRefProjection {
  private readonly cache = new WeakMap<object, readonly LocalFragmentRef[]>();
  private readonly rowCache = new WeakMap<object, LocalFragmentRef>();
  private readonly keyCache = new Map<string, LocalFragmentRef>();
  private readonly table: string;
  private readonly coverage: FragmentCoverage;
  private readonly primaryKeyFor: (table: string) => readonly string[];

  constructor(
    fragment: Fragment<any, any, any>,
    coverage: FragmentCoverage,
    primaryKeyFor: (table: string) => readonly string[],
  ) {
    this.table = tableMeta(fragment.table).name;
    this.coverage = coverage;
    this.primaryKeyFor = primaryKeyFor;
  }

  projectOne(raw: unknown): LocalFragmentRef | null {
    if (raw === null || raw === undefined) return null;
    return this.refForRow(raw);
  }

  projectMany(raw: unknown): readonly LocalFragmentRef[] {
    if (!Array.isArray(raw)) return EMPTY_ARRAY as readonly LocalFragmentRef[];
    const cached = this.cache.get(raw);
    if (cached) return cached;
    const refs = raw.map((row) => this.refForRow(row));
    this.cache.set(raw, refs);
    return refs;
  }

  private refForRow(value: unknown): LocalFragmentRef {
    if (value === null || typeof value !== "object") {
      throw new Error(`useRoot(): cannot build a fragment ref for "${this.table}" from a non-object row.`);
    }
    const cached = this.rowCache.get(value);
    if (cached) return cached;
    const row = value as Record<string, unknown>;
    const pk: Record<string, LitValue> = {};
    for (const col of this.primaryKeyFor(this.table)) {
      if (!Object.prototype.hasOwnProperty.call(row, col)) {
        throw new Error(`useRoot(): local root row for "${this.table}" is missing primary key column "${col}".`);
      }
      pk[col] = row[col] as LitValue;
    }
    const key = stableKey({ table: this.table, pk });
    const existing = this.keyCache.get(key);
    if (existing) {
      this.rowCache.set(value, existing);
      return existing;
    }
    const ref = createLocalFragmentRefForTable(this.table, pk, this.coverage);
    this.keyCache.set(key, ref);
    this.rowCache.set(value, ref);
    return ref;
  }
}

interface SyncCacheEntry {
  handle: SyncQueryHandle;
  leases: SyncLease[];
  listeners: Set<() => void>;
  unsubscribe: () => void;
  releaseTimer: ReleaseTimer | undefined;
}

interface SyncQueryHandle {
  readonly resultType: ResultType;
  subscribe(listener: () => void): () => void;
  release(): void;
}

export class SyncQueryCache {
  private readonly entries = new Map<string, SyncCacheEntry>();
  private nextLeaseId = 1;
  private readonly store: Store<ColsMap>;
  private readonly releaseDelayMs: number;

  constructor(store: Store<ColsMap>, opts: QueryCacheOptions = {}) {
    this.store = store;
    this.releaseDelayMs = opts.releaseDelayMs ?? 0;
  }

  retain(coverageKey: string, query: AnyQuery): SyncLease {
    let entry = this.entries.get(coverageKey);
    if (!entry) {
      const handle = this.createHandle(query);
      entry = {
        handle,
        leases: [],
        listeners: new Set(),
        unsubscribe: () => {},
        releaseTimer: undefined,
      };
      entry.unsubscribe = handle.subscribe(() => {
        for (const listener of entry!.listeners) listener();
      });
      this.entries.set(coverageKey, entry);
    } else if (entry.releaseTimer !== undefined) {
      clearTimeout(entry.releaseTimer);
      entry.releaseTimer = undefined;
    }
    const lease = { id: this.nextLeaseId++, coverageKey };
    entry.leases.push(lease);
    return lease;
  }

  release(lease: SyncLease): void {
    const entry = this.entries.get(lease.coverageKey);
    if (!entry) return;
    const index = entry.leases.findIndex((l) => l.id === lease.id);
    if (index < 0) return;
    entry.leases.splice(index, 1);
    if (entry.leases.length > 0) return;
    this.scheduleRelease(lease.coverageKey, entry);
  }

  subscribe(coverageKey: string, listener: () => void): () => void {
    const entry = this.entries.get(coverageKey);
    if (!entry) return () => {};
    entry.listeners.add(listener);
    listener();
    return () => {
      entry.listeners.delete(listener);
    };
  }

  resultType(coverageKey: string): ResultType {
    return this.entries.get(coverageKey)?.handle.resultType ?? "unknown";
  }

  size(): number {
    return this.entries.size;
  }

  private createHandle(query: AnyQuery): SyncQueryHandle {
    if (this.store.canRetainRemoteQueries()) return this.store.retainSyncQuery(query);
    const view = this.store.materialize(query) as AnyView;
    return {
      get resultType() {
        return view.resultType;
      },
      subscribe: (listener: () => void) => view.subscribe(listener),
      release: () => view.destroy(),
    };
  }

  private scheduleRelease(coverageKey: string, entry: SyncCacheEntry): void {
    if (this.releaseDelayMs <= 0) {
      this.finalizeRelease(coverageKey, entry);
      return;
    }
    entry.releaseTimer = setReleaseTimeout(() => this.finalizeRelease(coverageKey, entry), this.releaseDelayMs);
  }

  private finalizeRelease(coverageKey: string, entry: SyncCacheEntry): void {
    if (this.entries.get(coverageKey) !== entry || entry.leases.length > 0) return;
    entry.releaseTimer = undefined;
    entry.unsubscribe();
    entry.handle.release();
    this.entries.delete(coverageKey);
  }
}

export class QueryCache {
  private readonly entries = new Map<string, CacheEntry>();
  private nextLeaseId = 1;
  private readonly store: Store<ColsMap>;
  private readonly releaseDelayMs: number;

  constructor(store: Store<ColsMap>, opts: QueryCacheOptions = {}) {
    this.store = store;
    this.releaseDelayMs = opts.releaseDelayMs ?? 0;
  }

  retain<Q extends AnyQuery>(viewKey: string, query: Q): QueryLease {
    let entry = this.entries.get(viewKey);
    if (!entry) {
      entry = this.store.canRetainRemoteQueries()
        ? this.createSplitEntry(viewKey, query)
        : this.createMaterializedEntry(viewKey, query);
      this.entries.set(viewKey, entry);
      const lease = entry.leases[0];
      return { id: lease.id, viewKey };
    }
    if (entry.mode === "split") {
      const lease = this.createSplitLease(viewKey, entry.handle, query);
      entry.leases.push(lease);
      return { id: lease.id, viewKey };
    }
    const lease = this.createMaterializedLease(viewKey, query);
    entry.leases.push(lease);
    if (!entry.canonical.remote && lease.remote) this.setCanonical(entry, lease);
    return { id: lease.id, viewKey };
  }

  release(lease: QueryLease): void {
    const entry = this.entries.get(lease.viewKey);
    if (!entry) return;
    const index = entry.leases.findIndex((l) => l.id === lease.id);
    if (index < 0) return;
    if (entry.mode === "split") {
      const [released] = entry.leases.splice(index, 1);
      if (entry.leases.length > 0 || this.releaseDelayMs <= 0) {
        released.releaseRemote();
      } else {
        entry.pendingReleases.push(released);
        this.scheduleSplitRelease(lease.viewKey, entry);
      }
      if (entry.leases.length > 0) return;
      if (this.releaseDelayMs <= 0) {
        entry.canonicalUnsubscribe();
        entry.handle.destroy();
        this.entries.delete(lease.viewKey);
      }
      return;
    }
    const [released] = entry.leases.splice(index, 1);
    if (entry.canonical.id === released.id) {
      entry.canonicalUnsubscribe();
      entry.canonicalUnsubscribe = () => {};
    }
    released.view.destroy();
    if (entry.leases.length === 0) {
      this.entries.delete(lease.viewKey);
      return;
    }
    if (entry.canonical.id === released.id) this.setCanonical(entry, this.chooseCanonical(entry.leases));
  }

  subscribe(viewKey: string, listener: () => void): () => void {
    const entry = this.entries.get(viewKey);
    if (!entry) return () => {};
    entry.listeners.add(listener);
    listener();
    return () => {
      entry.listeners.delete(listener);
    };
  }

  snapshot(viewKey: string, one: boolean): unknown {
    const entry = this.entries.get(viewKey);
    if (entry) return entry.mode === "split" ? entry.handle.view.data : entry.canonical.view.data;
    return one ? null : EMPTY_ARRAY;
  }

  /** A query's current {@link ResultType} (from its view), or `unknown` before it is retained. */
  resultType(viewKey: string): ResultType {
    const entry = this.entries.get(viewKey);
    if (!entry) return "unknown";
    return entry.mode === "split" ? entry.handle.view.resultType : entry.canonical.view.resultType;
  }

  /** The SSR/hydration snapshot for `viewKey` — the store's preloaded/dehydrated seed, read
   *  WITHOUT retaining (the server never opens a subscription). Falls back to empty so a
   *  non-preloaded query renders like an unhydrated one. */
  serverSnapshot(viewKey: string, one: boolean): unknown {
    const seed = this.store.seedSnapshot(viewKey);
    if (!seed) return one ? null : EMPTY_ARRAY;
    return one ? (seed.rows[0] ?? null) : seed.rows;
  }

  /** The SSR {@link ResultType}: `complete` when a seed exists (server-authoritative first paint),
   *  else `unknown`. */
  serverResultType(viewKey: string): ResultType {
    return this.store.seedSnapshot(viewKey) ? "complete" : "unknown";
  }

  size(): number {
    return this.entries.size;
  }

  private createSplitEntry<Q extends AnyQuery>(viewKey: string, query: Q): SplitCacheEntry {
    const handle = this.store.createCachedQueryView(query) as unknown as CachedQueryView<AnyQuery>;
    const entry: SplitCacheEntry = {
      mode: "split",
      handle,
      leases: [],
      pendingReleases: [],
      releaseTimer: undefined,
      canonicalUnsubscribe: () => {},
      listeners: new Set(),
    };
    entry.leases.push(this.createSplitLease(viewKey, handle, query));
    entry.canonicalUnsubscribe = handle.view.subscribe(() => {
      for (const listener of entry.listeners) listener();
    });
    return entry;
  }

  private createSplitLease<Q extends AnyQuery>(
    viewKey: string,
    handle: CachedQueryView<AnyQuery>,
    query: Q,
  ): SplitLease {
    return { id: this.nextLeaseId++, viewKey, releaseRemote: handle.retain(query) };
  }

  private createMaterializedEntry<Q extends AnyQuery>(viewKey: string, query: Q): MaterializedCacheEntry {
    const lease = this.createMaterializedLease(viewKey, query);
    const entry: MaterializedCacheEntry = {
      mode: "materialized",
      leases: [lease],
      canonical: lease,
      canonicalUnsubscribe: () => {},
      listeners: new Set(),
    };
    this.setCanonical(entry, lease);
    return entry;
  }

  private createMaterializedLease<Q extends AnyQuery>(viewKey: string, query: Q): MaterializedLease {
    return {
      id: this.nextLeaseId++,
      viewKey,
      view: this.store.materialize(query) as AnyView,
      remote: typeof query.name === "string",
    };
  }

  private chooseCanonical(leases: MaterializedLease[]): MaterializedLease {
    return leases.find((lease) => lease.remote) ?? leases[0];
  }

  private setCanonical(entry: MaterializedCacheEntry, lease: MaterializedLease): void {
    entry.canonicalUnsubscribe();
    entry.canonical = lease;
    entry.canonicalUnsubscribe = lease.view.subscribe(() => {
      for (const listener of entry.listeners) listener();
    });
  }

  private scheduleSplitRelease(viewKey: string, entry: SplitCacheEntry): void {
    if (entry.releaseTimer !== undefined) clearTimeout(entry.releaseTimer);
    entry.releaseTimer = setReleaseTimeout(() => this.finalizeSplitRelease(viewKey, entry), this.releaseDelayMs);
  }

  private finalizeSplitRelease(viewKey: string, entry: SplitCacheEntry): void {
    if (this.entries.get(viewKey) !== entry) return;
    entry.releaseTimer = undefined;
    const pending = entry.pendingReleases.splice(0);
    for (const lease of pending) lease.releaseRemote();
    if (entry.leases.length > 0) return;
    entry.canonicalUnsubscribe();
    entry.handle.destroy();
    this.entries.delete(viewKey);
  }
}

export function queryCacheKey(query: AnyQuery): string {
  return describeQuery(query).viewKey;
}

function useRindleContext(): RindleContextValue {
  const ctx = useContext(RindleContext);
  if (!ctx) throw new Error("Rindle context is missing. Wrap this tree in <Rindle store={store}>.");
  return ctx;
}

function describeQuery(query: AnyQuery): QueryDescriptor {
  const ast = query.ast();
  const remote = typeof query.name === "string" ? { name: query.name, args: query.args } : null;
  return {
    // `viewKey` MUST match the SSR seed key (`@rindle/client`'s `stableKey(ast)`) so
    // `getServerSnapshot` finds the dehydrated entry — one canonical serializer, shared.
    viewKey: stableKey(ast),
    leaseKey: stableKey({ ast, remote }),
    one: ast.one === true,
  };
}
