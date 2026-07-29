// @rindle/tanstack — the route-level bridge between TanStack Router and Rindle. Routes declare
// data intent; this adapter owns SSR/client branching, loader dehydration, navigation readiness,
// stale re-entry behavior, cancellation, and the SSR-seed → live-store provider handoff.

import { createElement, useMemo, type ReactNode } from "react";
import { useMatches } from "@tanstack/react-router";
import { stableKey } from "@rindle/client";
import type {
  AnyQuery,
  ColsMap,
  DehydratedState,
  EnsureQueryUntil,
  QueryEnsurer,
  Schema,
  Store,
} from "@rindle/client";
import { RindleSSR } from "@rindle/react";

/** The loader-data field serialized by TanStack Start and consumed by {@link Provider}. */
export const RINDLE_LOADER_DATA_KEY = "rindle" as const;

/** The stable subset of TanStack's loader context exposed to route query factories. TanStack passes
 *  a richer object at runtime; keeping this structural avoids coupling the adapter to a generated
 *  router's route-tree generics while still contextually typing the common fields. */
export interface RindleLoaderContext {
  abortController: AbortController;
  preload: boolean;
  params: Record<string, string>;
  deps: Record<string, unknown>;
  context: unknown;
  location: unknown;
  cause: "preload" | "enter" | "stay";
}

export interface RindleLoaderData {
  rindle: DehydratedState;
}

export interface RindleTanStackClient<S extends ColsMap> extends QueryEnsurer {
  store: Store<S>;
}

export interface RindleTanStackOptions<S extends ColsMap> {
  schema: Schema<S>;
  /** Browser-only lazy client boot. The adapter calls it at most once and shares that client
   *  between route loaders and the provider. It is never called during SSR. */
  boot: () => Promise<RindleTanStackClient<S>>;
  /** Server-side named-query preload. A dynamic implementation keeps server authority code out of
   *  the browser bundle; the full loader context is forwarded for request-scoped auth/policy. */
  preload: (
    queries: readonly AnyQuery[],
    context: RindleLoaderContext,
  ) => Promise<DehydratedState>;
}

type QueryList = AnyQuery | readonly AnyQuery[];

export interface RindleRouteLoaderOptions<Context extends RindleLoaderContext = RindleLoaderContext> {
  /** Query (or queries) that blocks client route entry and is always included in the SSR seed. */
  query?: (context: Context) => QueryList;
  /** Additional queries needed for SSR first paint but not worth blocking client navigation on. */
  ssr?: (context: Context) => QueryList;
  /** Client readiness policy for `query`. Defaults to local-first `present`; set `complete` when
   *  route entry must wait for a server-authoritative result. */
  until?: EnsureQueryUntil;
}

export interface RindleRouteLoader<Context extends RindleLoaderContext = RindleLoaderContext> {
  handler(context: Context): Promise<RindleLoaderData>;
  /** Re-entry must not commit a stale match while its Rindle query is being warmed. */
  staleReloadMode: "blocking";
}

export interface RindleProviderProps {
  children?: ReactNode;
}

export interface RindleTanStackIntegration<S extends ColsMap> {
  loader<Context extends RindleLoaderContext = RindleLoaderContext>(
    options: RindleRouteLoaderOptions<Context>,
  ): RindleRouteLoader<Context>;
  Provider(props: RindleProviderProps): ReturnType<typeof createElement>;
}

/**
 * Bind one Rindle client/server pair to TanStack Router. The returned `loader` is used by routes;
 * `Provider` replaces the app's manual `useMatches()` dehydration merge plus `<RindleSSR>` glue.
 */
export function createRindleTanStack<S extends ColsMap>(
  options: RindleTanStackOptions<S>,
): RindleTanStackIntegration<S> {
  let bootPromise: Promise<RindleTanStackClient<S>> | undefined;
  const boot = (): Promise<RindleTanStackClient<S>> => {
    // Both TanStack loaders and <RindleSSR> can be the first browser caller. Cache the in-flight
    // result so they always retain and render through the same live store.
    bootPromise ??= Promise.resolve().then(() => options.boot());
    return bootPromise;
  };

  const loader = <Context extends RindleLoaderContext = RindleLoaderContext>(
    route: RindleRouteLoaderOptions<Context>,
  ): RindleRouteLoader<Context> => ({
    staleReloadMode: "blocking",
    handler: async (context: Context): Promise<RindleLoaderData> => {
      if (!route.query && !route.ssr) {
        throw new Error("rindle.loader: expected at least one query or ssr query.");
      }
      const primary = route.query ? queryList(route.query(context)) : [];
      if (typeof window === "undefined") {
        const extras = route.ssr ? queryList(route.ssr(context)) : [];
        return { rindle: await options.preload(uniqueQueries([...primary, ...extras]), context) };
      }

      // An SSR-only declaration contributes to the first document render, but it should be a no-op
      // when TanStack invokes the same loader during browser navigation.
      if (primary.length === 0) return { rindle: {} };

      const client = await boot();
      await Promise.all(
        primary.map((query) =>
          client.ensure(query, {
            until: route.until ?? "present",
            signal: context.abortController.signal,
          }),
        ),
      );
      return { rindle: {} };
    },
  });

  function Provider({ children }: RindleProviderProps): ReturnType<typeof createElement> {
    const matches = useMatches() as readonly { loaderData?: unknown }[];
    const ssrState = useMemo(
      () => mergeRindleLoaderData(matches.map((match) => match.loaderData)),
      [matches],
    );
    return createElement(RindleSSR<S>, {
      schema: options.schema,
      ssrState,
      boot,
      children,
    });
  }

  return { loader, Provider };
}

/** Merge the serialized Rindle slice from every matched route. Exported for custom providers/tests. */
export function mergeRindleLoaderData(loaderData: readonly unknown[]): DehydratedState {
  const merged: DehydratedState = {};
  for (const data of loaderData) {
    if (data === null || typeof data !== "object") continue;
    const slice = (data as Partial<RindleLoaderData>)[RINDLE_LOADER_DATA_KEY];
    // Later matched routes intentionally win when two slices contain the same query key.
    if (slice) Object.assign(merged, slice);
  }
  return merged;
}

function queryList(value: QueryList): readonly AnyQuery[] {
  return Array.isArray(value) ? value : [value as AnyQuery];
}

function uniqueQueries(queries: readonly AnyQuery[]): readonly AnyQuery[] {
  const seen = new Set<string>();
  return queries.filter((query) => {
    const remote = typeof query.name === "string" ? { name: query.name, args: query.args } : undefined;
    const key = stableKey({ ast: query.ast(), remote });
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}
