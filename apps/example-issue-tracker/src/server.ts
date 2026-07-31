// The Start SSR server entry — TanStack Start auto-detects `src/server.ts` as the `server.entry`.
// `/api/rindle/{query,read,mutate}` is served by TanStack server routes in `src/routes/api.rindle.*`;
// everything else server-renders through createStartHandler + defaultStreamHandler.
//
// Worker deploys install Cloudflare `env` bindings in worker.ts before invoking this handler. The
// wasm engine is never imported here — SSR renders the view from the one-shot seed, never the engine
// (SSR-DESIGN.md §6).
//
// Daemon control-plane config comes from process.env in Node. On Cloudflare, worker.ts installs
// bindings into the API/SSR modules before calling this handler.

import { createStartHandler, defaultStreamHandler } from "@tanstack/react-start/server";
import type { OneShotQueryFn, OneShotResult } from "@rindle/client";

import { createIssueApi, requiredEnv } from "../server/app-api.ts";
import type { IssueApiOptions } from "../server/app-api.ts";
import { configureSsrReader, SSR_USER } from "./ssr.ts";

/** The Start SSR handler — server-renders the matched route to a stream (`(request, opts) => Response`). */
export const ssrHandler = createStartHandler(defaultStreamHandler);

const DAEMON_TOKEN = process.env.RINDLE_DAEMON_TOKEN ?? process.env.DAEMON_TOKEN ?? "dev-daemon-token";
const REPLICATOR_URL = process.env.RINDLE_REPLICATOR_URL ?? process.env.REPLICATOR_ORIGIN;
const REPLICATOR_TOKEN = process.env.RINDLE_REPLICATOR_TOKEN ?? process.env.WRITE_TOKEN;
// The master's PUBLIC SQL credential — where authoritative mutations execute. Distinct from
// WRITE_TOKEN by construction (the replicator refuses an equal pair at startup).
const DATABASE_TOKEN = process.env.RINDLE_DATABASE_TOKEN ?? process.env.SQL_TOKEN;

/** Build the in-process SSR first-paint reader: resolve each named query through the SAME app
 *  authority the /api routes use, with the daemon answering DIRECTLY — no HTTP hop back to this
 *  server's own origin. (That self-loopback works in local workerd but FAILS on Cloudflare's edge,
 *  silently emptying the seed and flashing "Loading issues…" on first paint.) Reused by worker.ts
 *  with the Worker's `env` daemon bindings. */
export function makeSsrReader(cfg: IssueApiOptions): OneShotQueryFn {
  const api = createIssueApi(cfg);
  return async ({ name, args }) => {
    const out = await api.handleReadJson({ name, args }, { user: SSR_USER });
    return { rows: out.rows, cvMin: out.cvMin } as OneShotResult;
  };
}

// The node host's SSR reader (daemon config from process.env). Built on FIRST USE, not at module
// scope: `worker.ts` imports this module, and on Cloudflare there is no `process` to read — the
// Worker supplies its own reader from `env` bindings instead. Resolving the URL eagerly here would
// throw at Worker startup.
let nodeSsrReaderMemo: OneShotQueryFn | undefined;
const nodeSsrReader: OneShotQueryFn = (req) => {
  nodeSsrReaderMemo ??= makeSsrReader({
    daemonUrl: requiredEnv("RINDLE_DAEMON_URL", "DAEMON_ORIGIN"),
    daemonToken: DAEMON_TOKEN,
    replicatorUrl: REPLICATOR_URL,
    replicatorToken: REPLICATOR_TOKEN,
    databaseToken: DATABASE_TOKEN,
  });
  return nodeSsrReaderMemo(req);
};

/** Route API requests to the authority, server-render everything else (`(request) => Response`). */
async function handler(request: Request): Promise<Response> {
  // SSR loader first-paint reads run IN-PROCESS through the authority — no self-origin loopback.
  configureSsrReader(nodeSsrReader);
  return ssrHandler(request);
}

// The Start `server.entry` shape: TanStack Start invokes `module.default.fetch(request)` — the dev
// server (start-plugin-core dev-server-plugin), the preview server, and the built server bundle ALL
// call `.fetch`, so the default export MUST be `{ fetch }`, never a bare function (a bare function
// has no `.fetch`, which 500s every SSR request).
export default { fetch: handler };
