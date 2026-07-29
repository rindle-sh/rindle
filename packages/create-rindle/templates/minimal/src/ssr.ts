// SSR preload — the server-side counterpart to the in-browser engine. On the first visit the route
// loader runs HERE (the server) and reads each first-paint query ONCE through the app authority
// (server/app-api.ts) — the SAME `createAppApi` factory the /api/rindle server route uses, called
// IN-PROCESS (no HTTP hop) because SSR and the API now share one server. The authority resolves
// `(name, args)` → AST itself; the loader never ships a raw AST. It returns the dehydrated snapshot
// for the HTML, `rindle.Provider` hydrates it for an instant correct first paint, then the wasm engine
// boots and the live `subscribe` reconciles.
//
// This is a STRICTLY server-side module: it builds the daemon client and reads `process.env`, so it is
// imported ONLY by src/rindle-tanstack.ts behind an `import.meta.env.SSR` guard, keeping it out of the
// client bundle. It never imports the engine.

import { createServerStore, type DehydratedState, type OneShotQueryFn, type OneShotResult, type Query } from "@rindle/client";
import type { ApiContext } from "@rindle/api-server";

import { createAppApi, resolveRindle } from "../server/app-api.ts";
import type { User } from "../server/app-api.ts";
import { schema } from "../shared/app-def.ts";

// SSR reads are PUBLIC (the authority's `authorizeQuery` allows anonymous), so the server reads the
// first-paint views under no principal. The browser's real per-tab handle takes over on the live
// subscribe after hydration.
const SSR_USER: User = undefined;

/** The one-shot read the server Store calls per preloaded query: resolve the NAMED query through the
 *  authority IN-PROCESS — the same path as the HTTP /api/rindle/read, minus the network round trip —
 *  and hand back the assembled current view. */
const readInProcess: OneShotQueryFn = async ({ name, args }): Promise<OneShotResult> => {
  const api = createAppApi(resolveRindle(process.env));
  const context: ApiContext<User> = { user: SSR_USER, request: undefined };
  return (await api.handleReadJson({ name, args }, context)) as OneShotResult;
};

/** Preload the given NAMED queries through the authority and return the dehydrated first-paint cache to
 *  embed in the HTML. Called by the integration's server-only preload hook. `preloadAll` degrades a
 *  failed read to no seed for that query (the live engine fills it in after hydration) rather than
 *  breaking the whole page. */
export async function preloadRindle(queries: Array<Query<any, any, any>>): Promise<DehydratedState> {
  return createServerStore(schema, { query: readInProcess }).preloadAll(queries, {
    onError: (_query, err) =>
      console.error("[ssr] preload failed; rendering this query without its first-paint seed:", err instanceof Error ? err.message : err),
  });
}
