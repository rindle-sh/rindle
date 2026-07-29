// The Cloudflare Worker entry — wraps the shared SSR+API handler (src/server.ts) for Workers. Three
// jobs in one isolate:
//
//   browser ──► <worker>/api/rindle/{query,read,mutate} ──bearer HTTP──► DAEMON_ORIGIN (control plane)
//   browser ──► <worker>/<page route>                   ──► TanStack Start SSR (ssrHandler)
//   browser ──► <worker>/<static asset>                 ──► env.ASSETS (the prebuilt client bundle)
//   browser ──► wss://<daemon>/…                          ──lease──► daemon public ws (DIRECT, not here)
//
// Unlike Node (src/server.ts reads daemon config from process.env), the Worker gets it on `env`, so we
// inject it into handleApi here. The control-plane bearer token (`DAEMON_TOKEN`) is a Worker SECRET.
//
// One topology (design 214): reads go to the follower (DAEMON_ORIGIN) and writes to the replicator
// write-master (REPLICATOR_ORIGIN); the split itself lives in app-api.ts's daemon construction — here
// we just forward the env bindings.

import { ssrHandler, handleApi } from "./src/server.ts";
import { DEFAULT_RINDLE_API_ROUTES } from "@rindle/api-server";
import { configureSsrApiBase } from "./src/ssr.ts";
import { selectAuth } from "./server/select-auth.ts";

interface Env {
  /** Cloudflare static-assets binding — the prebuilt client bundle (wrangler.jsonc `assets`). */
  ASSETS: { fetch(request: Request): Promise<Response> };
  /** The read FOLLOWER's control-plane base URL (a plain var; bearer-gated, see DAEMON_TOKEN). Reads. */
  DAEMON_ORIGIN: string;
  /** Shared control-plane bearer token (a SECRET; follower ↔ this Worker only). */
  DAEMON_TOKEN: string;
  /** The `rindle-replicator` write-master origin (design 214). Writes route here (via
   *  SplitDaemonClient); reads stay on DAEMON_ORIGIN (the follower). A live deployment always sets it —
   *  the follower's write plane 404s. */
  REPLICATOR_ORIGIN: string;
  /** Bearer token for the replicator write-master (a SECRET); defaults to DAEMON_TOKEN. */
  REPLICATOR_TOKEN?: string;
  /** This deployment's public origin — where SSR loader reads loop back to /api/rindle/read. */
  PUBLIC_ORIGIN?: string;

  // ── identity (RINDLE-FORUM-DESIGN §3.3) ──────────────────────────────────────────────────────
  /** "dev" (the OSS header provider) or "oidc" (verify a headwaters JWT). Default: "dev". */
  FORUM_AUTH?: string;
  /** headwaters origin — the JWT `iss` and JWKS host (required when FORUM_AUTH=oidc). */
  HEADWATERS_ISSUER?: string;
  /** Required `aud`; defaults to the issuer (matches headwaters' JWT_AUDIENCE default). */
  HEADWATERS_AUDIENCE?: string;
  /** Override the JWKS URL; defaults to `${HEADWATERS_ISSUER}/api/auth/jwks`. */
  HEADWATERS_JWKS_URL?: string;
}

const ROUTES = DEFAULT_RINDLE_API_ROUTES;
const isApiRoute = (pathname: string): boolean =>
  pathname === ROUTES.query || pathname === ROUTES.read || pathname === ROUTES.mutate;

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    configureSsrApiBase(env.PUBLIC_ORIGIN ?? url.origin);
    if (isApiRoute(url.pathname)) {
      // The Worker reads identity config off `env` (not process.env), so it selects the provider here
      // and threads it through. Defaults to the dev header provider unless FORUM_AUTH=oidc.
      const auth = selectAuth(env as unknown as Record<string, string | undefined>);
      // One topology: writes split off to the replicator write-master; reads and the control plane
      // stay on DAEMON_ORIGIN (the follower). REPLICATOR_ORIGIN is set on every live deployment.
      const writeDaemon = {
        url: env.REPLICATOR_ORIGIN,
        token: env.REPLICATOR_TOKEN ?? env.DAEMON_TOKEN,
      };
      return handleApi(request, {
        daemonUrl: env.DAEMON_ORIGIN,
        daemonToken: env.DAEMON_TOKEN,
        writeDaemon,
        auth,
      });
    }
    return ssrHandler(request);
  },
};
