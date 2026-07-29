// The Cloudflare Worker entry — installs Worker bindings for TanStack Start's SSR/API routes and
// adds the daily reset cron (Cloudflare Cron Triggers invoke `scheduled`, which a Web request
// handler can't express). Three jobs in one isolate:
//
//   browser ──► <worker>/api/rindle/{query,read,mutate} ──► TanStack Start API route ──bearer HTTP──► DAEMON_ORIGIN
//   browser ──► <worker>/<page route>                   ──► TanStack Start SSR (ssrHandler)
//   browser ──► <worker>/<static asset>                 ──► env.ASSETS (the prebuilt client bundle)
//   browser ──► wss://<app edge>/…                       ──lease──► data-plane edge (DIRECT, not here)
//
// Unlike Node Start routes (which read daemon config from process.env), the Worker gets it on `env`.
// The control-plane bearer token (`DAEMON_TOKEN`) is a Worker SECRET — daemon↔Worker only, never the
// browser.
//
// SSR first-paint reads run IN-PROCESS through the authority (the daemon answers directly), NOT via
// a fetch back to this Worker's own origin — a self-loopback works in local workerd but fails on
// Cloudflare's edge, which silently emptied the SSR seed (see the `fetch` handler below).
//
// BUILD: `CF=1 vite build` (via @cloudflare/vite-plugin) bundles THIS entry for workerd in the `ssr`
// environment, so the Start SSR manifest is baked in and the worker runs on Workers. Verified
// end-to-end on local workerd (`vite preview`/`wrangler dev`) in test/ssr-workerd.e2e.ts: SSR pages,
// the in-process first-paint read, and the assets binding all serve correctly.

import { ssrHandler, makeSsrReader } from "./src/server.ts";
import { configureIssueApi } from "./server/app-api.ts";
import { configureSsrReader } from "./src/ssr.ts";
import { reset } from "./server/reset.ts";

interface Env {
  /** Cloudflare static-assets binding — the prebuilt client bundle (wrangler.jsonc `assets`). */
  ASSETS: { fetch(request: Request): Promise<Response> };
  /** The FOLLOWER `rindled`'s control-plane base URL — the READ leg (design 214). A plain var;
   *  bearer-gated, see DAEMON_TOKEN. */
  DAEMON_ORIGIN: string;
  /** Shared follower control-plane bearer token (a SECRET; follower ↔ this Worker only). */
  DAEMON_TOKEN: string;
  /** The `rindle-replicator` write-master's ingress base URL — the WRITE leg (design 214). Every
   *  mutation (API writes) and the reset cron target this; the follower has no write plane. */
  REPLICATOR_ORIGIN: string;
  /** Bearer token for the master's write ingress (a SECRET). */
  WRITE_TOKEN: string;
  /** The master's PUBLIC /v1/sql/* bearer — where authoritative mutations execute. Must differ
   *  from WRITE_TOKEN (the replicator refuses an equal pair at startup). */
  SQL_TOKEN: string;
  /** This deployment's public origin. NO LONGER used for SSR (first-paint reads are in-process now);
   *  retained for compatibility with existing `wrangler.jsonc` vars and any future absolute-URL need. */
  PUBLIC_ORIGIN?: string;
  /** Reset/seed corpus size (optional override; defaults to server/seed.ts SEED_COUNT). */
  SEED_COUNT?: string;
}

/** Minimal shape of the Worker `scheduled` execution context (avoids a workers-types dep). */
interface ExecutionContext {
  waitUntil(promise: Promise<unknown>): void;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    // One topology (design 214): reads → the follower (DAEMON_ORIGIN), writes + mutation sessions →
    // the replicator write-master (REPLICATOR_ORIGIN) via SplitDaemonClient inside createIssueApi.
    const daemon = {
      daemonUrl: env.DAEMON_ORIGIN,
      daemonToken: env.DAEMON_TOKEN,
      replicatorUrl: env.REPLICATOR_ORIGIN,
      replicatorToken: env.WRITE_TOKEN,
      databaseToken: env.SQL_TOKEN,
    };
    configureIssueApi(daemon);
    // SSR loader first-paint reads run IN-PROCESS through the authority with the Worker's daemon
    // bindings — NOT a fetch back to this Worker's own origin. A Worker can't reliably fetch its own
    // public hostname on Cloudflare's edge (it works in local workerd), so the old loopback returned
    // an empty seed and the page flashed "Loading issues…". The authority dials the data plane
    // directly here, exactly as the /api branch above does. (`env` daemon config is a deployment
    // constant, so the module-level reader install is race-free across concurrent requests.)
    configureSsrReader(makeSsrReader(daemon));
    return ssrHandler(request);
  },

  // Shared-sandbox reset (wrangler.jsonc `triggers.crons`): snap the table back to the pristine
  // baseline (and seed a fresh deployment on the first fire). The reset is a WRITE, so it targets
  // the replicator write-master (design 214), NOT the read-only follower. `waitUntil` keeps it
  // alive past return.
  async scheduled(_event: unknown, env: Env, ctx: ExecutionContext): Promise<void> {
    if (!env.REPLICATOR_ORIGIN || !env.WRITE_TOKEN) {
      console.error("[reset] skipped — replicator origin/write token not configured");
      return;
    }
    ctx.waitUntil(
      reset({
        url: env.REPLICATOR_ORIGIN,
        token: env.WRITE_TOKEN,
        count: env.SEED_COUNT ? Number(env.SEED_COUNT) : undefined,
      }).then(
        (r) => console.log(`[reset] sandbox reset to ${r.count} issues (cv ${r.cv})`),
        (err) => console.error("[reset] failed:", (err as Error)?.message ?? err),
      ),
    );
  },
};
