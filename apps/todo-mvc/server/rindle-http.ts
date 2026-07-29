import type { ApiContext } from "@rindle/api-server";

import { createTodoApi, httpErrorOf, resolveRindle } from "./app-api.ts";
import type { User } from "./app-api.ts";

export type RindleRouteKind = "query" | "read" | "mutate";

export async function handleRindleJson(kind: RindleRouteKind, request: Request): Promise<Response> {
  try {
    const api = createTodoApi(resolveRindle(process.env));
    const body = await request.json().catch(() => ({}));
    const context: ApiContext<User> = { user: undefined, request };
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
