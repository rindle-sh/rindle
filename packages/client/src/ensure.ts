// Query readiness for route loaders and intent prefetching. `ResultType` remains the
// server-authority axis; this module only decides when a caller has enough data to continue.

import { stableKey } from "./key.ts";
import type { AnyQuery } from "./query.ts";
import type { ColsMap } from "./schema.ts";
import type { Store } from "./store.ts";
import type { ResultType } from "./types.ts";

/** When an {@link QueryEnsureCache.ensure} call may resolve.
 *
 * - `complete` waits for the server-authoritative result (the default).
 * - `present` resolves as soon as the local view contains a result, while the remote retain keeps
 *   revalidating in the background. An authoritative empty result also resolves it, so a real
 *   not-found query never waits forever.
 *
 * `present` deliberately does not add a `partial` {@link ResultType}: a locally useful answer and
 * server authority are independent facts. While it resolves early, the view's result type remains
 * `unknown` until the server says otherwise. */
export type EnsureQueryUntil = "complete" | "present";

export interface EnsureQueryOptions {
  /** Readiness policy. Defaults to `complete`. */
  until?: EnsureQueryUntil;
  /** Cancel this caller's wait. The shared query may stay retained for another waiter/prefetch. */
  signal?: AbortSignal;
}

export interface QueryEnsureCacheOptions {
  /** Keep a completed preload alive for this long so the destination can adopt its rows. */
  releaseDelayMs?: number;
  /** Bound retained preloads. In-flight waits are never evicted. */
  maxEntries?: number;
}

/** The structural surface consumed by framework adapters such as `@rindle/tanstack`. */
export interface QueryEnsurer {
  ensure(query: AnyQuery, options?: EnsureQueryOptions): Promise<void>;
}

interface EnsureView {
  readonly data: unknown;
  readonly resultType: ResultType;
  subscribe(listener: () => void): () => void;
  destroy(): void;
}

interface EnsureWaiter {
  until: EnsureQueryUntil;
  resolve: () => void;
  reject: (reason: Error) => void;
  signal?: AbortSignal;
  onAbort?: () => void;
}

interface EnsureEntry {
  key: string;
  view: EnsureView;
  waiters: Set<EnsureWaiter>;
  unsubscribe: () => void;
  releaseTimer?: ReturnType<typeof setTimeout>;
  lastUsedAt: number;
}

const DEFAULT_RELEASE_DELAY_MS = 30_000;
const DEFAULT_MAX_ENTRIES = 32;

/**
 * Deduplicates route/intent preloads and holds their live view through the navigation handoff.
 *
 * The cache materializes the real named query rather than retaining sync coverage alone: that is
 * what makes `until: "present"` observable for overlapping queries already satisfied by local
 * normalized rows. Once the server marks the query complete, the entry is kept briefly for a
 * destination component to take its own retain, then released automatically.
 */
export class QueryEnsureCache<S extends ColsMap> implements QueryEnsurer {
  private readonly store: Store<S>;
  private readonly releaseDelayMs: number;
  private readonly maxEntries: number;
  private readonly entries = new Map<string, EnsureEntry>();
  private closed = false;

  constructor(store: Store<S>, options: QueryEnsureCacheOptions = {}) {
    this.store = store;
    this.releaseDelayMs = Math.max(0, options.releaseDelayMs ?? DEFAULT_RELEASE_DELAY_MS);
    this.maxEntries = Math.max(1, options.maxEntries ?? DEFAULT_MAX_ENTRIES);
  }

  /**
   * Ensure a named query is retained, resolving according to `options.until`.
   *
   * Concurrent calls for the same `(name, args, AST)` share one materialized view and one remote
   * subscription. A `present` call can resolve from local rows while a concurrent `complete` call
   * continues waiting for server authority.
   */
  async ensure<Q extends AnyQuery>(query: Q, options: EnsureQueryOptions = {}): Promise<void> {
    if (this.closed) throw new Error("QueryEnsureCache.ensure: cache is closed.");
    if (options.signal?.aborted) throw abortError();
    if (typeof query.name !== "string") {
      throw new Error("QueryEnsureCache.ensure: preloading requires a named query.");
    }

    const key = stableKey({ ast: query.ast(), remote: { name: query.name, args: query.args } });
    let entry = this.entries.get(key);
    if (!entry) entry = this.createEntry(key, query);
    this.touch(entry);

    const until = options.until ?? "complete";
    if (entry.view.resultType === "error") {
      const error = this.queryError();
      this.dispose(entry, error);
      throw error;
    }
    if (this.isReady(entry, until)) {
      if (entry.view.resultType === "complete") this.scheduleRelease(entry);
      this.trim();
      return;
    }

    const ready = new Promise<void>((resolve, reject) => {
      const waiter: EnsureWaiter = { until, resolve, reject, signal: options.signal };
      if (options.signal) {
        waiter.onAbort = () => {
          if (!entry!.waiters.delete(waiter)) return;
          reject(abortError());
        };
        options.signal.addEventListener("abort", waiter.onAbort, { once: true });
      }
      entry!.waiters.add(waiter);
    });
    this.trim();
    return ready;
  }

  /** Release every retained preload and reject outstanding waits. Idempotent. */
  close(): void {
    if (this.closed) return;
    this.closed = true;
    const error = new Error("QueryEnsureCache: closed before the query became ready.");
    for (const entry of [...this.entries.values()]) this.dispose(entry, error);
  }

  /** Number of retained query entries (primarily useful for tests/devtools). */
  size(): number {
    return this.entries.size;
  }

  private createEntry(key: string, query: AnyQuery): EnsureEntry {
    const view = this.store.materialize(query) as unknown as EnsureView;
    const entry: EnsureEntry = {
      key,
      view,
      waiters: new Set(),
      unsubscribe: () => {},
      lastUsedAt: Date.now(),
    };
    this.entries.set(key, entry);
    entry.unsubscribe = view.subscribe(() => this.inspect(entry));
    return entry;
  }

  private inspect(entry: EnsureEntry): void {
    if (this.entries.get(entry.key) !== entry) return;
    if (entry.view.resultType === "error") {
      this.dispose(entry, this.queryError());
      return;
    }

    for (const waiter of [...entry.waiters]) {
      if (!this.isReady(entry, waiter.until)) continue;
      entry.waiters.delete(waiter);
      this.detachAbort(waiter);
      waiter.resolve();
    }

    // A present waiter may have continued while authority was still unknown. Keep the retain alive
    // through that revalidation; only a completed query starts the handoff/expiry clock.
    if (entry.view.resultType === "complete" && entry.waiters.size === 0) {
      this.scheduleRelease(entry);
      this.trim();
    }
  }

  private isReady(entry: EnsureEntry, until: EnsureQueryUntil): boolean {
    if (entry.view.resultType === "complete") return true;
    return until === "present" && hasLocalResult(entry.view.data);
  }

  private touch(entry: EnsureEntry): void {
    entry.lastUsedAt = Date.now();
    if (entry.releaseTimer !== undefined) {
      clearTimeout(entry.releaseTimer);
      entry.releaseTimer = undefined;
    }
  }

  private scheduleRelease(entry: EnsureEntry): void {
    if (this.entries.get(entry.key) !== entry || entry.waiters.size > 0) return;
    if (entry.releaseTimer !== undefined) clearTimeout(entry.releaseTimer);
    // Even a configured zero delay crosses a task boundary. Some local-first backends publish
    // `complete` immediately before folding the authoritative catch-up batch; synchronous teardown
    // here would unregister the view in the middle of that delivery.
    entry.releaseTimer = setTimeout(() => this.dispose(entry), this.releaseDelayMs);
    (entry.releaseTimer as { unref?: () => void }).unref?.();
  }

  private trim(): void {
    if (this.entries.size <= this.maxEntries) return;
    const idle = [...this.entries.values()]
      .filter((entry) => entry.waiters.size === 0)
      .sort((left, right) => left.lastUsedAt - right.lastUsedAt);
    while (this.entries.size > this.maxEntries && idle.length > 0) this.dispose(idle.shift()!);
  }

  private dispose(entry: EnsureEntry, error?: Error): void {
    if (this.entries.get(entry.key) !== entry) return;
    this.entries.delete(entry.key);
    if (entry.releaseTimer !== undefined) clearTimeout(entry.releaseTimer);
    entry.unsubscribe();
    entry.view.destroy();
    if (error) {
      for (const waiter of entry.waiters) {
        this.detachAbort(waiter);
        waiter.reject(error);
      }
    }
    entry.waiters.clear();
  }

  private queryError(): Error {
    return new Error("QueryEnsureCache.ensure: query entered the error result state.");
  }

  private detachAbort(waiter: EnsureWaiter): void {
    if (waiter.signal && waiter.onAbort) waiter.signal.removeEventListener("abort", waiter.onAbort);
  }
}

/** A plural query is locally present when it has at least one row; a `.one()` query when non-null. */
function hasLocalResult(data: unknown): boolean {
  return Array.isArray(data) ? data.length > 0 : data !== null && data !== undefined;
}

function abortError(): Error {
  const error = new Error("QueryEnsureCache.ensure: wait was aborted.");
  error.name = "AbortError";
  return error;
}
