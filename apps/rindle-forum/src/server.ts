// The SSR + API server entry — TanStack Start auto-detects `src/server.ts` as the `server.entry`. It
// is ONE Web-standard request handler with two jobs:
//
//   - /api/rindle/{query,read,mutate} → the app authority (server/app-api.ts): `read` is the one-shot
//     SSR loader read, `query` mints a live lease, `mutate` runs the mutators.
//   - everything else → server-render the app (createStartHandler + defaultStreamHandler).
//
// The route loader's first-paint read loops back to THIS handler's /api/rindle/read same-origin. In
// DEV the separate Node API server (server/api.ts, :7700) handles /api/* — Vite proxies to it before
// this handler runs; this API branch is the deployed Worker's single-server path. Either way reads go
// to the follower and writes to the replicator write-master (server/app-api.ts). The wasm engine is
// never imported here.

import { createStartHandler, defaultStreamHandler } from "@tanstack/react-start/server";
import { DEFAULT_RINDLE_API_ROUTES } from "@rindle/api-server";
import type { ApiContext } from "@rindle/api-server";

import { createForumApi, httpErrorOf, resolveForumDaemon } from "../server/app-api.ts";
import type { ForumDaemonTarget, User } from "../server/app-api.ts";
import type { AuthProvider } from "../shared/auth.ts";
import { selectAuth } from "../server/select-auth.ts";
import { configureSsrApiBase } from "./ssr.ts";

const ROUTES = DEFAULT_RINDLE_API_ROUTES;

/** The Start SSR handler — server-renders the matched route to a stream. */
export const ssrHandler = createStartHandler(defaultStreamHandler);

const DEFAULT_DAEMON = resolveForumDaemon(process.env, {
  daemonUrl: process.env.DAEMON_ORIGIN ?? "http://127.0.0.1:7600",
  daemonToken: process.env.DAEMON_TOKEN ?? "dev-daemon-token",
});

// The identity provider, chosen once from the environment (dev header vs. headwaters OIDC, §3.3). The
// Worker passes its own provider through `cfg.auth` since it reads config off `env`, not process.env.
const DEFAULT_AUTH = selectAuth();

const json = (body: unknown, status = 200): Response =>
  new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });

const isApiRoute = (pathname: string): boolean =>
  pathname === ROUTES.query || pathname === ROUTES.read || pathname === ROUTES.mutate;

/** Handle one `/api/rindle/*` request through the app authority. Exported so worker.ts can run it with
 *  Cloudflare's `env` bindings (Node reads `process.env` via the defaults above). */
export async function handleApi(
  request: Request,
  cfg: {
    daemonUrl: string;
    daemonToken: string;
    /** The `rindle-replicator` write-master: writes route here, reads stay on the follower (§214). */
    writeDaemon: ForumDaemonTarget;
    auth?: AuthProvider;
  } = DEFAULT_DAEMON,
): Promise<Response> {
  if (!cfg.daemonUrl || !cfg.daemonToken || !cfg.writeDaemon?.url) {
    return json({ error: "follower and replicator origins/tokens are not configured" }, 503);
  }
  try {
    const api = createForumApi({
      daemonUrl: cfg.daemonUrl,
      daemonToken: cfg.daemonToken,
      writeDaemon: cfg.writeDaemon,
    });
    const body = await request.json().catch(() => ({}));
    // Resolve the principal via the AuthProvider (dev: x-forum-user header; prod: a headwaters JWT).
    const user: User = (await (cfg.auth ?? DEFAULT_AUTH).verify(request)) ?? undefined;
    const context: ApiContext<User> = { user, request };
    const pathname = new URL(request.url).pathname;
    const out =
      pathname === api.routes.query
        ? await api.handleQueryJson(body, context)
        : pathname === api.routes.read
          ? await api.handleReadJson(body, context)
          : await api.handleMutateJson(body, context);
    return json(out);
  } catch (err) {
    const { status, message } = httpErrorOf(err);
    return json({ error: message }, status);
  }
}

/** Route API requests to the authority, server-render everything else. */
async function handler(request: Request): Promise<Response> {
  const url = new URL(request.url);
  configureSsrApiBase(url.origin); // SSR loader reads dial back here, same origin
  if (isApiRoute(url.pathname)) return handleApi(request);
  return ssrHandler(request);
}

// The Start `server.entry` shape: TanStack Start invokes `module.default.fetch(request)`.
export default { fetch: handler };
