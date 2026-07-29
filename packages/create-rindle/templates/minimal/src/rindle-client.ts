// The whole client wire-up is ONE call — but DEFERRED to the browser. The optimistic engine is wasm
// (an in-process IVM engine), so it must never be constructed during the SSR/prerender shell pass.
// `bootClient()` lazily imports the engine + optimistic glue the first time it runs on the client and
// memoizes the promise; `app` is a live binding assigned once boot resolves, so the components that
// fire mutations (`app.mutate.*`) read the ready client at call time.
//
// Queries materialize through the API server (subscribed on the daemon's public ws); mutations flush
// through the client-side queue as confirmed in-order batches; rejections surface via onRejected.

import type { MutationEnvelope } from "@rindle/client";

import wasmUrl from "rindle-wasm-bin?url";

import { mutators, normalizeSubject, schema } from "../shared/app-def.ts";

// The precise client type — including the typed `mutate.*` surface — is INFERRED from the concrete
// `createRindleClient({ schema, mutators, … })` call in `bootClientInner`.
type RindleApp = Awaited<ReturnType<typeof bootClientInner>>;
type RejectionHandler = (envelope: MutationEnvelope, reason: string) => void;

/** The placeholder identity used before this browser's real persisted handle is known — i.e. during
 *  SSR (no `localStorage`) and the first hydration render (which must byte-match the server). */
export const SSR_USER = "ssr";

/** The dev "login": a handle persisted per browser. A real app puts a verified token in `api.headers`
 *  instead. SSR-safe: returns {@link SSR_USER} when there is no `localStorage` (the server render). */
export function currentHandle(): string {
  if (typeof localStorage === "undefined") return SSR_USER;
  let handle = localStorage.getItem("rindle-user");
  if (!handle) {
    handle = `user-${Math.random().toString(36).slice(2, 7)}`;
    localStorage.setItem("rindle-user", handle);
  }
  return handle;
}

export function setCurrentHandle(handle: string): void {
  if (typeof localStorage === "undefined") return;
  localStorage.setItem("rindle-user", handle);
}

let rejectionHandler: RejectionHandler = () => {};
export function onRejection(handler: RejectionHandler): () => void {
  rejectionHandler = handler;
  return () => {
    if (rejectionHandler === handler) rejectionHandler = () => {};
  };
}

/** The live optimistic client — assigned once {@link bootClient} resolves. `rindle.Provider` and all
 *  route loaders share this boot; components use the live binding for `app.mutate.*` event handlers. */
export let app: RindleApp;

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
    // The acting principal a mutator sees as ctx.user — the prediction's author. The server injects
    // its OWN verified identity for the authoritative run (server/app-api.ts sharedCtx).
    user: () => normalizeSubject(currentHandle()),
    api: {
      url: "", // same-origin: /api/rindle/* is a Start server route on this same server
      // Identity per request: the dev handle header. A real app sends a verified token instead.
      headers: (): Record<string, string> => ({
        "x-rindle-user": currentHandle(),
      }),
    },
    // No browser topology config: the first query lease returns the public WebSocket endpoint plus
    // a fresh placement ticket, and the optimistic client opens the correctly pinned socket lazily.
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
      // Dev-only: register this client with the in-browser devtools pane (mutation timeline, queries
      // inspector, delta stream). `import.meta.env.DEV` is a static `false` in a production build, so
      // this dynamic import of @rindle/devtools is tree-shaken out — devtools never ship to users.
      if (import.meta.env.DEV) {
        void import("@rindle/devtools").then(({ attachDevtools }) => attachDevtools(ready));
      }
      return ready;
    });
  }
  return bootPromise;
}
