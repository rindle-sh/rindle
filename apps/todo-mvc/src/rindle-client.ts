import wasmUrl from "rindle-wasm-bin?url";

import { mutators, schema } from "../shared/app-def.ts";

type RindleTodoApp = Awaited<ReturnType<typeof bootClientInner>>;

export let app: RindleTodoApp;

async function bootClientInner() {
  const [{ createRindleClient }, { initWasm }] = await Promise.all([
    import("@rindle/optimistic"),
    import("@rindle/wasm"),
  ]);
  await initWasm(wasmUrl);
  return createRindleClient({
    schema,
    mutators,
    api: { url: "" },
    // The first query lease returns the WebSocket endpoint and placement ticket, so the browser
    // needs only its same-origin /api/rindle/* routes.
    dev: { resetOnMutationGap: import.meta.env.DEV },
    onRejected: (_envelope, reason) => {
      console.error("[rindle] mutation rejected:", reason);
    },
  });
}

let bootPromise: Promise<RindleTodoApp> | undefined;

export function bootClient(): Promise<RindleTodoApp> {
  if (!bootPromise) {
    bootPromise = bootClientInner().then((ready) => {
      app = ready;
      if (import.meta.env.DEV) {
        void import("@rindle/devtools").then(({ attachDevtools }) => attachDevtools(ready));
      }
      return ready;
    });
  }
  return bootPromise;
}
