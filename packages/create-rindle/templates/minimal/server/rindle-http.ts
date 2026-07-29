// The HTTP host for the app authority: it adapts a Web `Request` to the runtime-agnostic factory in
// app-api.ts and dispatches the three Rindle endpoints. The TanStack Start server routes in
// src/routes/api.rindle.*.tsx import this from INSIDE their handlers (a dynamic import), so this module
// and its daemon/SQL deps never reach the client bundle. SSR reads skip this and call the authority
// in-process instead (src/ssr.ts) — no network hop.

import type { ApiContext } from "@rindle/api-server";

import { createAppApi, httpErrorOf, resolveRindle } from "./app-api.ts";
import type { User } from "./app-api.ts";
import { devAuth } from "./auth-dev.ts";

/** Which Rindle endpoint a request targets. */
export type RindleRouteKind = "query" | "read" | "mutate";

/** Run one `/api/rindle/{query,read,mutate}` request through the authority. Stateless: the authority is
 *  built per request (cheap), so this is safe in a fresh-per-request serverless host too. */
export async function handleRindleJson(kind: RindleRouteKind, request: Request): Promise<Response> {
  try {
    const api = createAppApi(resolveRindle(process.env));
    const body = await request.json().catch(() => ({}));
    const context: ApiContext<User> = {
      user: (await devAuth.verify(request)) ?? undefined,
      request,
    };
    const out =
      kind === "query"
        ? await api.handleQueryJson(body, context)
        : kind === "read"
          ? await api.handleReadJson(body, context)
          : await api.handleMutateJson(body, context);
    return Response.json(out);
  } catch (err) {
    const { status, message } = httpErrorOf(err);
    return Response.json({ error: message }, { status });
  }
}
