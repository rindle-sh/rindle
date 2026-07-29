// SSR client integration (SSR-DESIGN.md §6). Server-side render is synchronous, so the data
// must already be in the cache when the render reads it. The seam is the BACKEND, not the hook:
//
//   - Browser: the normal ws-backed Store — retain → lease → subscribe → live.
//   - Server : a Store over a one-shot REST backend ({@link OneShotBackend}) that never streams.
//              The route loader `preload`s each query (`POST /query`), which seeds the view for an
//              instant correct first paint; `dehydrate` serializes those seeds into the HTML; the
//              browser `store.hydrate(...)`s them and its live `subscribe` reconciles (§5).
//
// `useQuery` is byte-for-byte identical in both — only the injected backend (and whether a live
// subscribe ever happens) differs.

import { assertNoLocalTables, type Query } from "./query.ts";
import type { ColsMap, Schema } from "./schema.ts";
import { type AssembledNode, type DehydratedState, Store } from "./store.ts";
import type { Backend, ChangeEvent, Mutation, QueryId, RemoteQuery } from "./types.ts";

/** A `POST /query` response, as far as the server Store needs it (SSR-DESIGN.md §3.3): the
 *  assembled rows plus the `cvMin` baseline they reflect. (Matches `@rindle/daemon-client`'s
 *  `QueryOnceOutput`, but `@rindle/client` stays dependency-free — inject the call.) */
export interface OneShotResult {
  rows: AssembledNode[];
  cvMin?: number;
}

/** The one-shot read the server Store calls to preload a query. Two topologies inject different
 *  fns (SSR-DESIGN.md §6):
 *
 *   - **Direct to the daemon** (the trusted tier holds the daemon token): use `ast` —
 *     `(i) => daemon.query(i)`.
 *   - **Through the application's API tier** (the authority resolves names → ASTs, the loader never
 *     sends a raw AST): use `name`/`args` — `(i) => fetch('/api/rindle/read', {name: i.name, args: i.args})`.
 *
 *  `ast` is always present (the Store seeds the local view by its `viewKey`); `name`/`args` are
 *  present when the preloaded query came from `defineQuery`. The Store applies no policy/auth —
 *  that lives upstream (the daemon, or the API tier's `authorizeQuery`). */
export type OneShotQueryFn = (
  input: { ast: unknown; name?: string; args?: unknown; visibilityKey?: string; ttlMs?: number },
) => Promise<OneShotResult>;

/**
 * A no-op live backend for SSR (SSR-DESIGN.md §6.1): it opens no transport and never streams.
 * `registerQuery`/`mutate` are inert and no `ChangeEvent` is ever pushed, so every view stays
 * PENDING — reading its SSR {@link Store.seedAssembled seed} — and, lacking `onResultType`,
 * reports `complete` (a backend with no server lifecycle leaves every view authoritative).
 */
export class OneShotBackend implements Backend {
  registerQuery(_qid: QueryId, _ast: unknown, _remote?: RemoteQuery): void {}
  unregisterQuery(_qid: QueryId): void {}
  mutate(_mutations: Mutation[]): Promise<void> {
    return Promise.reject(new Error("the SSR one-shot backend is read-only; mutate on the browser store"));
  }
  onEvent(_handler: (queryId: QueryId, event: ChangeEvent) => void): void {}
}

export interface ServerStoreOptions {
  /** Performs the one-shot `POST /query` read (e.g. a `@rindle/daemon-client` instance's `query`). */
  query: OneShotQueryFn;
  /** Optional RLS/visibility dedup key forwarded to every preload (SSR-DESIGN.md §3.2). The daemon
   *  scopes the materialization's dedup `QueryKey` by it, so the same AST under two visibility keys
   *  never shares one pipeline; absent ⇒ the daemon's configured default. */
  visibilityKey?: string;
  /** Optional idle TTL (ms) the warm pipeline is left at, forwarded to every preload (SSR-DESIGN.md
   *  §3.4). The TTL is NOT part of the dedup key, so a shared materialization keeps the LONGEST TTL
   *  any caller requested (max-wins) — `ttlMs` can extend a query's warm-handoff window, never
   *  shrink it; absent ⇒ the daemon's default idle TTL. */
  ttlMs?: number;
}

/**
 * The server-side Store wrapper (SSR-DESIGN.md §6.2). Wraps a {@link Store} over a
 * {@link OneShotBackend} and adds the loader-phase `preload` plus `dehydrate`. Pass `.store` to
 * the React `<Rindle>` provider for the synchronous render; return `.dehydrate()` from the loader.
 */
export class ServerStore<S extends ColsMap> {
  readonly store: Store<S>;
  private readonly schema: Schema<S>;
  private readonly opts: ServerStoreOptions;

  constructor(schema: Schema<S>, opts: ServerStoreOptions) {
    this.store = new Store(schema, new OneShotBackend());
    this.schema = schema;
    this.opts = opts;
  }

  /** Run the one-shot read for `query` and seed its first-paint snapshot (SSR-DESIGN.md §6.2).
   *  Call once per query in the route loader, before the synchronous render. */
  async preload(query: Query<any, any, any>): Promise<void> {
    const ast = query.ast();
    // E3 backstop: a local-only table must never be forwarded to the daemon. The `OneShotBackend`'s
    // `registerQuery` is a no-op (no engine-side E3 check runs on the SSR path), and a query built
    // from the LOCAL builder permits local tables — so the AST is re-checked here regardless of how
    // it was built (201-LOCAL-ONLY-TABLES-DESIGN.md E3).
    assertNoLocalTables(ast, this.schema);
    // Forward the AST (a direct-to-daemon backend reads it) plus the named identity when this came
    // from `defineQuery` (an API-tier backend resolves `(name, args)` → AST itself, never trusting
    // a client AST). The seed is keyed by the AST's `viewKey` either way, so the browser's
    // `getServerSnapshot` finds it (SSR-DESIGN.md §6.2).
    const named = typeof query.name === "string" ? { name: query.name, args: query.args } : undefined;
    const result = await this.opts.query({
      ast,
      ...named,
      visibilityKey: this.opts.visibilityKey,
      ttlMs: this.opts.ttlMs,
    });
    this.store.seedAssembled(ast, result.rows, result.cvMin ?? 0);
  }

  /** The dehydrated first-paint cache for every preloaded query — embed in the HTML, then
   *  `store.hydrate(...)` it in the browser. */
  dehydrate(): DehydratedState {
    return this.store.dehydrate();
  }

  /**
   * Loader-phase convenience over {@link preload} + {@link dehydrate}: preload EVERY query (reads run
   * concurrently) and return the dehydrated first-paint cache. Composition keeps this to one read per
   * composed root query — no request waterfall (SSR-DESIGN.md §6.2).
   *
   * A failed read degrades to NO seed for that one query — `onError` fires and the browser's live
   * engine fills it in right after hydration — rather than rejecting the whole batch (which would trip
   * the route's error boundary). The seed is a first-paint optimization; the live `subscribe` is the
   * source of truth, so a missing seed never affects correctness. Without `onError` a failed read is
   * swallowed silently — pass one to log.
   */
  async preloadAll(
    queries: Array<Query<any, any, any>>,
    opts: { onError?: (query: Query<any, any, any>, err: unknown) => void } = {},
  ): Promise<DehydratedState> {
    await Promise.all(
      queries.map((query) => this.preload(query).catch((err) => opts.onError?.(query, err))),
    );
    return this.dehydrate();
  }
}

/** Construct a {@link ServerStore} — the one-shot REST Store for server-side rendering. */
export function createServerStore<S extends ColsMap>(
  schema: Schema<S>,
  opts: ServerStoreOptions,
): ServerStore<S> {
  return new ServerStore(schema, opts);
}
