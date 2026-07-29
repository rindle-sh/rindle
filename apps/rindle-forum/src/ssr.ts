// SSR preload — the server-side counterpart to the in-browser engine (SSR-DESIGN.md §6). On the first
// visit the route loader runs HERE (the server), reads each first-paint query ONCE through the API
// tier's /api/rindle/read (the authority resolves `(name, args)` → AST itself — the loader never ships
// a raw AST), and returns the dehydrated, assembled snapshot for the HTML. The browser hydrates it
// (src/RindleApp.tsx) for an instant correct first paint, then the wasm engine boots and the live
// `subscribe` reconciles → SPA.
//
// This is a STRICTLY server-side module path: it dials the API server over HTTP with an absolute URL
// (the dev Vite `/api` proxy is browser-only) and never imports the engine.

import { createServerStore, type DehydratedState, type OneShotQueryFn, type OneShotResult, type Query } from "@rindle/client";

import { schema } from "../shared/app-def.ts";

// The API tier's absolute base URL as seen from the SSR runtime. Dev dials the API server directly
// (7700, via `process.env` which exists there). The deployed Cloudflare Worker SSRs and serves
// /api/rindle/read in the SAME isolate, so it calls {@link configureSsrApiBase} once with its origin.
let apiBase = (typeof process !== "undefined" && process.env?.SSR_API_URL) || "http://127.0.0.1:7700";

/** Point SSR loader reads at `url` (the API tier's absolute origin). Call once from the server entry
 *  when `process.env` isn't available (e.g. the Cloudflare Worker, from its env/origin). */
export function configureSsrApiBase(url: string): void {
  apiBase = url;
}

// SSR here is viewer-INDEPENDENT: forum reads are public, so the server reads the first-paint views
// under a fixed identity. The browser's real per-tab handle (rindle-client.ts) takes over on the live
// subscribe after hydration.
const SSR_USER = "ssr";

/** The one-shot read the server Store calls per preloaded query: POST the NAMED query to the
 *  authority, which resolves it to an AST and hands back the assembled current view. */
const readViaApi: OneShotQueryFn = async ({ name, args }): Promise<OneShotResult> => {
  const res = await fetch(`${apiBase}/api/rindle/read`, {
    method: "POST",
    headers: { "content-type": "application/json", "x-forum-user": SSR_USER },
    body: JSON.stringify({ name, args }),
  });
  if (!res.ok) {
    throw new Error(`rindle SSR read failed (${res.status}): ${await res.text().catch(() => res.statusText)}`);
  }
  return (await res.json()) as OneShotResult;
};

/** Preload the given NAMED queries through the API tier and return the dehydrated first-paint cache to
 *  embed in the HTML. Call from a route loader (server only). Composition keeps this to one
 *  /api/rindle/read per composed root query — no request waterfall (SSR-DESIGN.md §6.2). `preloadAll`
 *  degrades a failed read to no seed for that query (the live engine fills it in after hydration)
 *  rather than breaking the whole page. */
export async function preloadRindle(queries: Array<Query<any, any, any>>): Promise<DehydratedState> {
  return createServerStore(schema, { query: readViaApi }).preloadAll(queries, {
    onError: (_query, err) =>
      console.error("[ssr] preload failed; rendering this query without its first-paint seed:", err instanceof Error ? err.message : err),
  });
}
