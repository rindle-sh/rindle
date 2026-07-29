import { createServerStore, type DehydratedState, type OneShotQueryFn, type OneShotResult, type Query } from "@rindle/client";
import type { ApiContext } from "@rindle/api-server";

import { createTodoApi, resolveRindle } from "../server/app-api.ts";
import type { User } from "../server/app-api.ts";
import { schema } from "../shared/app-def.ts";

const readInProcess: OneShotQueryFn = async ({ name, args }): Promise<OneShotResult> => {
  const api = createTodoApi(resolveRindle(process.env));
  const context: ApiContext<User> = { user: undefined, request: undefined };
  return (await api.handleReadJson({ name, args }, context)) as OneShotResult;
};

export async function preloadRindle(queries: Array<Query<any, any, any>>): Promise<DehydratedState> {
  return createServerStore(schema, { query: readInProcess }).preloadAll(queries, {
    onError: (_query, err) => console.error("[ssr] preload failed:", err instanceof Error ? err.message : err),
  });
}
