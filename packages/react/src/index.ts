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

// The LM stream plane's client half (LM-STREAM-CHECKPOINT-DESIGN.md): the durable side arrives through
// an ordinary query, `useStreamedText` adds the live tail and merges the two. The pure reassembly
// helpers come from `@rindle/client` and are re-exported so a component needs ONE import.
export {
  DEFAULT_STREAM_ENDPOINT,
  eventSourceTransport,
  streamSubscribeUrl,
  useStreamedText,
} from "./stream.ts";
export type { StreamTransport, UseStreamedTextInput, UseStreamedTextOptions } from "./stream.ts";
export { assembleDurableText, spliceStreamText } from "@rindle/client";
export type { StreamFrame, StreamStatus } from "@rindle/client";

export interface RindleProps<S extends ColsMap = ColsMap> {
  store: Store<S>;
  /** Default grace window (ms) for every query in this tree — how long a view + its server lease are
   *  kept warm after the last subscriber unmounts. Defaults to 2s; see {@link QueryReleaseOptions}
   *  for why, and for the per-call-site override. Treat as a constant: changing it rebuilds the
   *  caches and tears down every live view. */
  releaseDelayMs?: number;
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
  /** Resolved grace window for THIS lease (see {@link QueryReleaseOptions}). */
  releaseDelayMs: number;
}

interface MaterializedLease extends QueryLease {
  view: AnyView;
  remote: boolean;
}

interface SplitLease extends QueryLease {
  releaseRemote: () => void;
  /** Resolved grace window for THIS lease (see {@link QueryReleaseOptions}). */
  releaseDelayMs: number;
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
  /** Absolute monotonic ms this entry's warm window expires at — the latest `release time + that
   *  lease's delay` asked for by ANY lease on this entry (see {@link QueryReleaseOptions}). Written
   *  only in `release`, so the clock starts when a subscriber LEAVES. Monotone, and it expires on its
   *  own, which is what makes a later lease inherit only the residue of an older window. */
  releaseDeadline: number;
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
// Overridable per tree (`<Rindle releaseDelayMs>`) and per call site ({@link QueryReleaseOptions}).
const REACT_CACHE_RELEASE_DELAY_MS = 2_000;

type ReleaseTimer = ReturnType<typeof setTimeout>;

interface QueryCacheOptions {
  releaseDelayMs?: number;
}

/**
 * Per-call-site override for how long a query is kept warm after its LAST subscriber unmounts.
 *
 * The default (2s, or whatever `<Rindle releaseDelayMs>` sets) exists so a changed filter/limit can
 * re-materialize from the still-warm local base while the replacement server lease streams its first
 * answer — it's what keeps navigation from flashing empty. That grace window is wrong for queries you
 * KNOW you will never come back to, the canonical case being typeahead search: every keystroke is a
 * distinct query, so a 2s window leaves one dead view + server subscription open per character typed.
 * Pass `0` there to tear down on unmount:
 *
 * ```tsx
 * const results = useQuery(searchIssues(term), { releaseDelayMs: 0 });
 * ```
 *
 * Treat the value as a constant per call site — changing it re-leases the query (drops the old lease
 * and takes a fresh one), which is wasted work if it changes every render.
 *
 * The rule for a query several components share with DIFFERENT delays is a DEADLINE, not a duration:
 * every release stamps `now + that lease's delay`, and the query stays warm until the latest deadline
 * any of its leases asked for (max-wins over what REMAINS, matching the SSR preload TTL rule in
 * `@rindle/client`'s `ssr.ts`). Two consequences worth internalizing:
 *
 *   - The clock starts when a subscriber LEAVES, never when it arrives — a mounted reader is never
 *     timed out, however long it stays.
 *   - A deadline expires on its own, so a later lease inherits at most the RESIDUE of an older window,
 *     never a fresh copy of it. Unmount a 2s reader, remount a `releaseDelayMs: 0` one 1.9s later and
 *     drop it: teardown lands at the original 2s mark, not 1.9s past it.
 *
 * Only meaningful against a backend that can retain remote queries. A local-only store (the SSR seed
 * over `OneShotBackend`, or a store with no remote leg) always tears its views down on release, so
 * there is no window to shorten.
 */
export interface QueryReleaseOptions {
  /** ms to keep this query warm after the last subscriber unmounts. `0` = release immediately.
   *  Defaults to the provider's `releaseDelayMs` (2s). */
  releaseDelayMs?: number;
}

/** Monotonic clock for release deadlines. Deliberately NOT `Date.now()`: a wall-clock step backwards
 *  would extend every live warm window, and a step forwards would truncate them. */
function nowMs(): number {
  return performance.now();
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

  constructor(store: Store<ColsMap>, releaseDelayMs: number) {
    this.store = store;
    this.cache = new QueryCache(store, { releaseDelayMs });
    this.syncCache = new SyncQueryCache(store, { releaseDelayMs });
  }
}

const RindleContext = createContext<RindleContextValue | null>(null);

export function Rindle<S extends ColsMap>({ store, releaseDelayMs, children }: RindleProps<S>) {
  const delay = releaseDelayMs ?? REACT_CACHE_RELEASE_DELAY_MS;
  // `releaseDelayMs` is tree CONFIG, not state: changing it rebuilds the caches exactly like swapping
  // `store` does, tearing down every live view. Pass a constant; use the per-hook option to vary it.
  const value = useMemo(
    () => new RindleContextValue(store as unknown as Store<ColsMap>, delay),
    [store, delay],
  );
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

export function useQuery<Q extends AnyQuery>(query: Q, opts?: QueryReleaseOptions): QueryData<Q> {
  const ctx = useRindleContext();
  const descriptor = useMemo(() => describeQuery(query), [query]);
  const queryRef = useRef(query);
  queryRef.current = query;
  const releaseDelayMs = opts?.releaseDelayMs;

  const subscribe = useCallback(
    (onStoreChange: () => void) => {
      const lease = ctx.cache.retain(descriptor.viewKey, queryRef.current, releaseDelayMs);
      const unsubscribe = ctx.cache.subscribe(descriptor.viewKey, onStoreChange);
      return () => {
        unsubscribe();
        ctx.cache.release(lease);
      };
    },
    [ctx.cache, descriptor.leaseKey, descriptor.viewKey, releaseDelayMs],
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
export function useQueryStatus(query: AnyQuery, opts?: QueryReleaseOptions): ResultType {
  const ctx = useRindleContext();
  const descriptor = useMemo(() => describeQuery(query), [query]);
  const queryRef = useRef(query);
  queryRef.current = query;
  const releaseDelayMs = opts?.releaseDelayMs;

  const subscribe = useCallback(
    (onStoreChange: () => void) => {
      const lease = ctx.cache.retain(descriptor.viewKey, queryRef.current, releaseDelayMs);
      const unsubscribe = ctx.cache.subscribe(descriptor.viewKey, onStoreChange);
      return () => {
        unsubscribe();
        ctx.cache.release(lease);
      };
    },
    [ctx.cache, descriptor.leaseKey, descriptor.viewKey, releaseDelayMs],
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
export function useSyncQuery(query: AnyQuery, opts?: QueryReleaseOptions): ResultType {
  const ctx = useRindleContext();
  const descriptor = useMemo(() => describeQuery(query), [query]);
  const queryRef = useRef(query);
  queryRef.current = query;
  const releaseDelayMs = opts?.releaseDelayMs;

  const subscribe = useCallback(
    (onStoreChange: () => void) => {
      const lease = ctx.syncCache.retain(descriptor.leaseKey, queryRef.current, releaseDelayMs);
      const unsubscribe = ctx.syncCache.subscribe(descriptor.leaseKey, onStoreChange);
      return () => {
        unsubscribe();
        ctx.syncCache.release(lease);
      };
    },
    [ctx.syncCache, descriptor.leaseKey, releaseDelayMs],
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
  opts?: QueryReleaseOptions,
): FragmentData<F> | null {
  return useLocalFragment(fragment, ref, opts);
}

/**
 * Render-prop sugar over {@link useFragment}: does the `null` check once. `from` is a fragment ref
 * (or null/undefined — an absent to-one relationship, an emptied `.one()`, or a row deleted out from
 * under a live read); when the row is present `children(data)` renders, otherwise `fallback` (default
 * nothing). Keeps the per-row subscription isolation — a child-only edit re-renders just this read.
 */
export function Frag<F extends AnyFragment>(
  { of, from, fallback = null, releaseDelayMs, children }: {
    of: F;
    from: FragmentRef<F> | null | undefined;
    fallback?: ReactNode;
    /** Per-call-site grace window — see {@link QueryReleaseOptions}. */
    releaseDelayMs?: number;
    children: (data: FragmentData<F>) => ReactNode;
  },
): ReactNode {
  const data = useFragment(of, from, { releaseDelayMs });
  return data == null ? fallback : children(data);
}

function useLocalFragment<F extends Fragment<any, any, any, any>>(
  fragment: F,
  ref: LocalFragmentRef<F> | null | undefined,
  opts?: QueryReleaseOptions,
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

  const releaseDelayMs = opts?.releaseDelayMs;
  const subscribe = useCallback(
    (onStoreChange: () => void) => {
      if (!coverage || !descriptor || !query) return () => {};
      const syncLease = ctx.syncCache.retain(coverage.key, coverage.query, releaseDelayMs);
      const localLease = ctx.cache.retain(descriptor.viewKey, query, releaseDelayMs);
      const unsubscribeSync = ctx.syncCache.subscribe(coverage.key, onStoreChange);
      const unsubscribeLocal = ctx.cache.subscribe(descriptor.viewKey, onStoreChange);
      return () => {
        unsubscribeLocal();
        unsubscribeSync();
        ctx.cache.release(localLease);
        ctx.syncCache.release(syncLease);
      };
    },
    [ctx.cache, ctx.syncCache, coverage, descriptor, query, releaseDelayMs],
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
  /** Absolute monotonic ms this coverage's warm window expires at — same deadline rule as
   *  {@link SplitCacheEntry.releaseDeadline}, so both caches implement the one contract documented on
   *  {@link QueryReleaseOptions}. */
  releaseDeadline: number;
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
  private readonly defaultReleaseDelayMs: number;

  constructor(store: Store<ColsMap>, opts: QueryCacheOptions = {}) {
    this.store = store;
    this.defaultReleaseDelayMs = Math.max(0, opts.releaseDelayMs ?? 0);
  }

  /** `releaseDelayMs` overrides the cache default for THIS lease only (see
   *  {@link QueryReleaseOptions}) — `0` asks for no warm window of its own, though an unexpired
   *  deadline from an earlier lease on this coverage still applies. */
  retain(coverageKey: string, query: AnyQuery, releaseDelayMs?: number): SyncLease {
    let entry = this.entries.get(coverageKey);
    if (!entry) {
      const handle = this.createHandle(query);
      entry = {
        handle,
        leases: [],
        listeners: new Set(),
        unsubscribe: () => {},
        releaseTimer: undefined,
        releaseDeadline: 0,
      };
      entry.unsubscribe = handle.subscribe(() => {
        for (const listener of entry!.listeners) listener();
      });
      this.entries.set(coverageKey, entry);
    } else if (entry.releaseTimer !== undefined) {
      // Cancel the pending teardown — but NOT `releaseDeadline`. The timer is stale (it was armed for
      // an entry that is live again); the deadline is an outstanding claim that must survive, or a
      // remount would silently refresh a window that should only ever decay.
      clearTimeout(entry.releaseTimer);
      entry.releaseTimer = undefined;
    }
    const lease = { id: this.nextLeaseId++, coverageKey, releaseDelayMs: this.resolveDelay(releaseDelayMs) };
    entry.leases.push(lease);
    return lease;
  }

  release(lease: SyncLease): void {
    const entry = this.entries.get(lease.coverageKey);
    if (!entry) return;
    const index = entry.leases.findIndex((l) => l.id === lease.id);
    if (index < 0) return;
    // Read the delay off the STORED lease, not the caller's handle — a caller can't widen its window
    // after the fact by mutating the object `retain` handed back.
    const [released] = entry.leases.splice(index, 1);
    // Stamp the deadline for EVERY release, not just the last one: a sibling that is still mounted
    // must not discard the window this lease just asked for, or the result would depend on unmount
    // order. Recording it can never cause a premature teardown — only the last-out branch below arms
    // a timer.
    entry.releaseDeadline = Math.max(entry.releaseDeadline, nowMs() + released.releaseDelayMs);
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

  private resolveDelay(releaseDelayMs: number | undefined): number {
    return Math.max(0, releaseDelayMs ?? this.defaultReleaseDelayMs);
  }

  /** Arm (or re-arm) the teardown for `entry.releaseDeadline`. Because the deadline is an ABSOLUTE
   *  instant, re-arming is idempotent — a later release recomputes the same wake-up time instead of
   *  restarting the window. */
  private scheduleRelease(coverageKey: string, entry: SyncCacheEntry): void {
    if (entry.releaseTimer !== undefined) {
      clearTimeout(entry.releaseTimer);
      entry.releaseTimer = undefined;
    }
    const remaining = entry.releaseDeadline - nowMs();
    if (remaining <= 0) {
      this.finalizeRelease(coverageKey, entry);
      return;
    }
    entry.releaseTimer = setReleaseTimeout(() => this.finalizeRelease(coverageKey, entry), remaining);
  }

  private finalizeRelease(coverageKey: string, entry: SyncCacheEntry): void {
    // `entry.leases.length > 0` is the guard that makes a live subscriber safe from a stale timer: a
    // remount inside the window revives the entry, and the timer armed before it may still fire. Do
    // not "simplify" this away.
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
  private readonly defaultReleaseDelayMs: number;

  constructor(store: Store<ColsMap>, opts: QueryCacheOptions = {}) {
    this.store = store;
    this.defaultReleaseDelayMs = Math.max(0, opts.releaseDelayMs ?? 0);
  }

  /** `releaseDelayMs` overrides the cache default for THIS lease only (see
   *  {@link QueryReleaseOptions}). Ignored for a local-only store, whose views are always torn down
   *  on release. */
  retain<Q extends AnyQuery>(viewKey: string, query: Q, releaseDelayMs?: number): QueryLease {
    let entry = this.entries.get(viewKey);
    if (!entry) {
      entry = this.store.canRetainRemoteQueries()
        ? this.createSplitEntry(viewKey, query, releaseDelayMs)
        : this.createMaterializedEntry(viewKey, query);
      this.entries.set(viewKey, entry);
      const lease = entry.leases[0];
      return { id: lease.id, viewKey };
    }
    if (entry.mode === "split") {
      // Take the new server lease FIRST, then hand the deferred ones back: `handle.retain` mints a
      // distinct remote qid per call, so overlapping them keeps this query's coverage continuously
      // live and the backend never re-subscribes.
      const lease = this.createSplitLease(viewKey, entry.handle, query, releaseDelayMs);
      entry.leases.push(lease);
      this.flushPendingReleases(entry);
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
      // Stamp the deadline for EVERY release, not just the last one: a sibling that is still mounted
      // must not discard the window this lease just asked for, or the result would depend on unmount
      // order. Recording it can never cause a premature teardown — only the last-out branch below
      // arms a timer.
      entry.releaseDeadline = Math.max(entry.releaseDeadline, nowMs() + released.releaseDelayMs);
      // A non-last lease never needs its remote lease held: the entry (and its warm local view)
      // outlives it, and the surviving leases keep the query subscribed.
      if (entry.leases.length > 0) {
        released.releaseRemote();
        return;
      }
      // Last one out. Its remote lease is what keeps the warm view fed until the deadline, so it is
      // deferred. `scheduleSplitRelease` collapses an already-expired deadline into a synchronous
      // teardown, so both paths funnel through one place.
      entry.pendingReleases.push(released);
      this.scheduleSplitRelease(lease.viewKey, entry);
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

  private createSplitEntry<Q extends AnyQuery>(
    viewKey: string,
    query: Q,
    releaseDelayMs?: number,
  ): SplitCacheEntry {
    const handle = this.store.createCachedQueryView(query) as unknown as CachedQueryView<AnyQuery>;
    const entry: SplitCacheEntry = {
      mode: "split",
      handle,
      leases: [],
      pendingReleases: [],
      releaseTimer: undefined,
      releaseDeadline: 0,
      canonicalUnsubscribe: () => {},
      listeners: new Set(),
    };
    entry.leases.push(this.createSplitLease(viewKey, handle, query, releaseDelayMs));
    entry.canonicalUnsubscribe = handle.view.subscribe(() => {
      for (const listener of entry.listeners) listener();
    });
    return entry;
  }

  private createSplitLease<Q extends AnyQuery>(
    viewKey: string,
    handle: CachedQueryView<AnyQuery>,
    query: Q,
    releaseDelayMs?: number,
  ): SplitLease {
    return {
      id: this.nextLeaseId++,
      viewKey,
      releaseRemote: handle.retain(query),
      releaseDelayMs: this.resolveDelay(releaseDelayMs),
    };
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

  private resolveDelay(releaseDelayMs: number | undefined): number {
    return Math.max(0, releaseDelayMs ?? this.defaultReleaseDelayMs);
  }

  /** Hand back every deferred remote lease and cancel the pending teardown, WITHOUT touching
   *  `entry.releaseDeadline`. Called when a retain revives the entry: the new lease covers the query,
   *  so the deferred ones are redundant, and the timer armed for an idle entry is stale. The deadline
   *  is not — it is an outstanding claim, and dropping it here would let a remount silently refresh a
   *  window that must only ever decay. */
  private flushPendingReleases(entry: SplitCacheEntry): void {
    if (entry.releaseTimer !== undefined) {
      clearTimeout(entry.releaseTimer);
      entry.releaseTimer = undefined;
    }
    for (const stale of entry.pendingReleases.splice(0)) stale.releaseRemote();
  }

  /** Arm (or re-arm) the teardown for `entry.releaseDeadline`. Because the deadline is an ABSOLUTE
   *  instant, re-arming is idempotent — a later release recomputes the same wake-up time instead of
   *  restarting the window. */
  private scheduleSplitRelease(viewKey: string, entry: SplitCacheEntry): void {
    if (entry.releaseTimer !== undefined) {
      clearTimeout(entry.releaseTimer);
      entry.releaseTimer = undefined;
    }
    const remaining = entry.releaseDeadline - nowMs();
    if (remaining <= 0) {
      this.finalizeSplitRelease(viewKey, entry);
      return;
    }
    entry.releaseTimer = setReleaseTimeout(() => this.finalizeSplitRelease(viewKey, entry), remaining);
  }

  private finalizeSplitRelease(viewKey: string, entry: SplitCacheEntry): void {
    if (this.entries.get(viewKey) !== entry) return;
    entry.releaseTimer = undefined;
    const pending = entry.pendingReleases.splice(0);
    for (const lease of pending) lease.releaseRemote();
    // The guard that makes a live subscriber safe from a stale timer: a remount inside the window
    // revives the entry, and the timer armed before it may still fire. Do not "simplify" this away.
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
