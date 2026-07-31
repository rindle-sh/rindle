// The whole client wire-up is ONE call — but DEFERRED to the browser. The optimistic engine is
// wasm (an in-process IVM engine), so it must never be constructed during the SSR/prerender shell
// pass. `bootClient()` lazily imports the engine + optimistic glue the first time it runs on the
// client and memoizes the promise; `app` is a live binding assigned once boot resolves, so the
// components that fire mutations (`app.mutate.*`) read the ready client at call time.
//
// Queries materialize through the API tier (opaque lease back, subscribed on the daemon's public ws);
// mutations flush through the client-side queue as confirmed in-order batches; rejections surface via
// onRejected.

import type { MutationEnvelope } from "@rindle/client";

import wasmUrl from "rindle-wasm-bin?url";

import { mutators, schema } from "../shared/app-def.ts";

// The precise client type — including the typed `mutate.*` surface — is INFERRED from the concrete
// `createRindleClient({ schema, mutators, … })` call in `bootClientInner` (the generic signature
// alone would erase the mutator argument types to `never`). `bootClientInner` is only ever CALLED on
// the client, so the engine never loads during the SSR/prerender shell pass.
type RindleApp = Awaited<ReturnType<typeof bootClientInner>>;
type RejectionHandler = (envelope: MutationEnvelope, reason: string) => void;

/** The placeholder identity used before this browser's real persisted user is known — i.e. during
 *  SSR (no `localStorage`) and the first hydration render (which must byte-match the server). SSR
 *  data is viewer-global, so first paint needs no real user; the browser adopts it post-hydration. */
export const SSR_USER = "ssr";

/** The demo's "login": a display name persisted per browser. A real app would put a
 *  session/JWT in `api.headers` instead. SSR-safe: returns {@link SSR_USER} when there is no
 *  `localStorage` (the server render) so it never throws in the SSR pass. */
export function currentUser(): string {
  if (typeof localStorage === "undefined") return SSR_USER;
  let user = localStorage.getItem("issue-tracker-user");
  if (!user) {
    user = `user-${Math.random().toString(36).slice(2, 7)}`;
    localStorage.setItem("issue-tracker-user", user);
  }
  return user;
}

export function setCurrentUser(user: string): void {
  if (typeof localStorage === "undefined") return;
  localStorage.setItem("issue-tracker-user", user);
}

let rejectionHandler: RejectionHandler = () => {};
export function onRejection(handler: RejectionHandler): () => void {
  rejectionHandler = handler;
  return () => {
    if (rejectionHandler === handler) rejectionHandler = () => {};
  };
}

/** The live optimistic client — assigned once {@link bootClient} resolves. Components import this
 *  and call `app.mutate.*` inside event handlers, by which point boot has completed (the provider
 *  in src/RindleApp.tsx gates the whole tree on it). */
export let app: RindleApp;

/** The fleet edge's ws URL. No fixed-port fallback: the local fleet's ports are allocated PER
 *  PROJECT, so `ws://127.0.0.1:7650` is no longer this app's edge — it may be a DIFFERENT project's,
 *  and the browser leg carries no project identity to be fenced on. `pnpm dev` sets `VITE_FLEET_WS`
 *  from the rendered rindle.json bindings; a deploy sets it at build time. */
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
 *  them) and constructs the optimistic client. Its inferred return type is the precise client type. */
async function bootClientInner() {
  const [{ createRindleClient }, { initWasm }] = await Promise.all([
    import("@rindle/optimistic"),
    import("@rindle/wasm"),
  ]);
  // Point the engine at the Vite-served wasm binary BEFORE the client's own (idempotent)
  // initWasm call resolves the default path.
  await initWasm(wasmUrl);
  return createRindleClient({
    schema,
    mutators,
    // The acting principal for a shared mutator's `ctx.user` — the local user the optimistic
    // prediction writes under (the API server injects its OWN authenticated user for the
    // authoritative run). Re-read per invoke, matching the `x-user` header the mutation ships with.
    user: () => currentUser(),
    api: {
      url: "", // same-origin: TanStack Start serves /api/rindle/* server routes
      headers: () => ({ "x-user": currentUser() }),
    },
    // A fleet of one and a wider fleet share the same edge binding, so scaling followers does not
    // change browser configuration. VITE_DAEMON_WS remains only as an explicit test/debug bypass.
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
