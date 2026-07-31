// The whole client wire-up is ONE call — but DEFERRED to the browser. The optimistic engine is wasm
// (an in-process IVM engine), so it must never be constructed during the SSR/prerender shell pass.
// `bootClient()` lazily imports the engine + optimistic glue the first time it runs on the client and
// memoizes the promise; `app` is a live binding assigned once boot resolves, so the components that
// fire mutations (`app.mutate.*`) read the ready client at call time.
//
// Queries materialize through the API server (opaque lease back, subscribed on the daemon's public
// ws); mutations flush through the client-side queue as confirmed in-order batches; rejections surface
// via onRejected.

import type { MutationEnvelope } from "@rindle/client";

import wasmUrl from "rindle-wasm-bin?url";

import { handleToDisplayName, mutators, schema } from "../shared/app-def.ts";
import { getCloudToken } from "./cloud-auth.ts";

// The precise client type — including the typed `mutate.*` surface — is INFERRED from the concrete
// `createRindleClient({ schema, mutators, … })` call in `bootClientInner`. It is only ever CALLED on
// the client, so the engine never loads during the SSR/prerender shell pass.
type RindleApp = Awaited<ReturnType<typeof bootClientInner>>;
type RejectionHandler = (envelope: MutationEnvelope, reason: string) => void;

/** The placeholder identity used before this browser's real persisted handle is known — i.e. during
 *  SSR (no `localStorage`) and the first hydration render (which must byte-match the server). Reads are
 *  public, so first paint needs no real user; the browser adopts its handle post-hydration. */
export const SSR_USER = "ssr";

/** The dev "login": a handle persisted per browser. A real app puts a headwaters JWT in `api.headers`
 *  instead (RINDLE-FORUM-DESIGN.md §3). SSR-safe: returns {@link SSR_USER} when there is no
 *  `localStorage` (the server render) so it never throws in the SSR pass. */
export function currentHandle(): string {
  if (typeof localStorage === "undefined") return SSR_USER;
  let handle = localStorage.getItem("rindle-forum-user");
  if (!handle) {
    handle = `user-${Math.random().toString(36).slice(2, 7)}`;
    localStorage.setItem("rindle-forum-user", handle);
  }
  return handle;
}

export function setCurrentHandle(handle: string): void {
  if (typeof localStorage === "undefined") return;
  localStorage.setItem("rindle-forum-user", handle);
}

/** This browser's display name, derived from its handle (the dev provider does the same server-side). */
export function currentDisplayName(): string {
  return handleToDisplayName(currentHandle());
}

let rejectionHandler: RejectionHandler = () => {};
export function onRejection(handler: RejectionHandler): () => void {
  rejectionHandler = handler;
  return () => {
    if (rejectionHandler === handler) rejectionHandler = () => {};
  };
}

/** The live optimistic client — assigned once {@link bootClient} resolves. Components import this and
 *  call `app.mutate.*` inside event handlers, by which point boot has completed (the provider in
 *  src/RindleApp.tsx gates the whole tree on it). */
export let app: RindleApp;

/** The fleet edge's ws URL. No fixed-port fallback: the local fleet's ports are allocated PER
 *  PROJECT, so `ws://127.0.0.1:7650` is no longer this app's edge — it may well be a DIFFERENT
 *  project's, and the browser leg carries no project identity to be fenced on. `pnpm dev` sets
 *  `VITE_FLEET_WS` from the rendered rindle.json bindings; a deploy sets it at build time. */
function fleetWs(): string {
  const url = import.meta.env.VITE_FLEET_WS;
  if (!url) {
    throw new Error(
      "VITE_FLEET_WS is not set — run the app through `pnpm dev`, which injects the fleet edge URL " +
        "from rindle.json (`rindle render` prints it).",
    );
  }
  return url;
}

/** Dynamically imports the wasm engine + optimistic glue (so the SSR/prerender shell never evaluates
 *  them) and constructs the optimistic client. */
async function bootClientInner() {
  const [{ createRindleClient }, { initWasm }] = await Promise.all([
    import("@rindle/optimistic"),
    import("@rindle/wasm"),
  ]);
  await initWasm(wasmUrl);
  return createRindleClient({
    schema,
    mutators,
    api: {
      url: "", // same-origin: Vite proxies /api/* to the API server
      // Identity per request: a headwaters JWT (Bearer) when signed into Rindle Cloud, else the dev
      // handle header. The server's AuthProvider (dev vs. oidc) reads whichever it's configured for.
      headers: (): Record<string, string> => {
        const token = getCloudToken();
        return token ? { Authorization: `Bearer ${token}` } : { "x-forum-user": currentHandle() };
      },
    },
    // The fleet edge is stable for one or many followers. Direct follower access is an explicit
    // test/debug bypass only.
    daemon: import.meta.env.VITE_DAEMON_WS
      ? { wsUrl: import.meta.env.VITE_DAEMON_WS }
      : { wsUrl: fleetWs(), affinity: true },
    dev: { resetOnMutationGap: import.meta.env.DEV },
    onRejected: (envelope, reason) => rejectionHandler(envelope, reason),
  });
}

let bootPromise: Promise<RindleApp> | undefined;

/** Construct the optimistic client in the browser (idempotent / memoized). */
export function bootClient(): Promise<RindleApp> {
  if (!bootPromise) {
    bootPromise = bootClientInner().then((ready) => {
      app = ready;
      return ready;
    });
  }
  return bootPromise;
}
