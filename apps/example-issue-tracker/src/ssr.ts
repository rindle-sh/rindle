// SSR preload — the server-side counterpart to the in-browser engine (SSR-DESIGN.md §6). On the
// first visit the route loader runs HERE (the server), reads each first-paint query ONCE through
// the API tier's /api/rindle/read (the authority resolves `(name, args)` → AST itself — the loader
// never ships a raw AST), and returns the dehydrated, assembled snapshot for the HTML. The browser
// hydrates it (src/RindleApp.tsx) for an instant correct first paint, then the wasm engine boots
// and the live `subscribe` reconciles → SPA.
//
// The host entry (src/server.ts / worker.ts) installs an IN-PROCESS reader via
// {@link configureSsrReader}, so the first-paint read calls the app authority DIRECTLY (same code
// the /api routes run) and the daemon answers it — no HTTP hop back to this server's own origin. A
// self-origin loopback works in local workerd but FAILS on Cloudflare's edge (a Worker can't fetch
// its own public hostname), which silently yielded an empty seed + a first-paint flash. The HTTP
// {@link readViaApi} below remains the fallback for a deployment that runs the authority as a
// SEPARATE origin and wires it with {@link configureSsrApiBase}.
//
// This is a STRICTLY server-side module path: it never imports the engine.

import { createServerStore, type DehydratedState, type OneShotQueryFn, type OneShotResult, type Query } from "@rindle/client";

import { schema } from "../shared/app-def.ts";

// Optional absolute API base for deployments that keep the authority on a separate origin. Normal
// Start/Worker hosts install an in-process reader below, so this HTTP fallback is rarely used.
let apiBase = (typeof process !== "undefined" && process.env?.SSR_API_URL) || "http://127.0.0.1:7700";

/** Point SSR loader reads at `url` (the API tier's absolute origin). Call once from the server entry
 *  when `process.env` isn't available (e.g. the Cloudflare Worker, from its env/origin). */
export function configureSsrApiBase(url: string): void {
  apiBase = url;
}

// SSR here is viewer-INDEPENDENT: issues are global (no per-row RLS), so the server reads the
// first-paint window under a fixed identity. The browser's real per-tab user (rindle-client.ts
// `currentUser`) takes over on the live subscribe after hydration. A deployment with per-viewer
// visibility would instead resolve the session here and forward it (and scope the read's subject).
export const SSR_USER = "ssr";

/** The one-shot read the server Store calls per preloaded query: POST the NAMED query to the
 *  authority, which resolves it to an AST and hands back the assembled current view. */
const readViaApi: OneShotQueryFn = async ({ name, args }): Promise<OneShotResult> => {
  const res = await fetch(`${apiBase}/api/rindle/read`, {
    method: "POST",
    headers: { "content-type": "application/json", "x-user": SSR_USER },
    body: JSON.stringify({ name, args }),
  });
  if (!res.ok) {
    throw new Error(`rindle SSR read failed (${res.status}): ${await res.text().catch(() => res.statusText)}`);
  }
  return (await res.json()) as OneShotResult;
};

// The reader the host installs (src/server.ts / worker.ts): resolves each named query through the
// app authority IN-PROCESS, so the SSR seed never depends on a self-origin HTTP loopback. Unset ⇒
// {@link readViaApi} (the separate-origin HTTP fallback) is used.
let ssrReader: OneShotQueryFn | undefined;

/** Install the in-process SSR first-paint reader (the host entry builds it from its daemon
 *  bindings). Call once on the server before the synchronous render; it supersedes
 *  {@link configureSsrApiBase}'s HTTP path. */
export function configureSsrReader(fn: OneShotQueryFn): void {
  ssrReader = fn;
}

/** Preload the given NAMED queries through the authority and return the dehydrated first-paint cache
 *  to embed in the HTML. Call from a route loader (server only). Composition keeps this to one read
 *  per composed root query — no request waterfall (SSR-DESIGN.md §6.2). `preloadAll` degrades a failed
 *  read to no seed for that query (rather than tripping the route's error boundary) — which is what
 *  keeps the page loading during a deploy bootstrap (before the daemon/DAEMON_TOKEN is wired the read
 *  503s) or on any transient blip; the browser's live engine fills the query in right after hydration. */
export async function preloadRindle(queries: Array<Query<any, any, any>>): Promise<DehydratedState> {
  return createServerStore(schema, { query: ssrReader ?? readViaApi }).preloadAll(queries, {
    onError: (_query, err) =>
      console.error("[ssr] preload failed; rendering this query without its first-paint seed:", err instanceof Error ? err.message : err),
  });
}
