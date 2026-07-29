import {
  createRindleApiServer,
  defineApiMutators,
  registerQueries,
} from "@rindle/api-server";
import type { ApiMutators, RindleApiServer, SqlMutationTx } from "@rindle/api-server";

import {
  DEFAULT_LIST_ID,
  createTodoArgs,
  deleteTodoArgs,
  normalizeColor,
  normalizeTitle,
  setColorArgs,
  toggleTodoArgs,
} from "../shared/app-def.ts";
import { todoListQuery } from "../src/components/TodoList.queries.ts";

export type User = undefined;

const apiQueries = registerQueries<User>([todoListQuery]);

const apiMutators = defineApiMutators<User, ApiMutators<User>>({
  createTodo: (tx: SqlMutationTx, raw: unknown) => {
    const a = createTodoArgs.parse(raw);
    const title = normalizeTitle(a.title);
    if (!title) return;
    tx.exec(
      `INSERT INTO todo (id, listId, title, completed, temperature, createdAt, updatedAt, color)
       SELECT ?, ?, ?, 0, 0, ?, ?, 0.58 WHERE EXISTS (SELECT 1 FROM todo_list WHERE id = ?)`,
      [a.id, DEFAULT_LIST_ID, title, a.createdAt, a.createdAt, DEFAULT_LIST_ID],
    );
  },
  toggleTodo: (tx: SqlMutationTx, raw: unknown) => {
    const a = toggleTodoArgs.parse(raw);
    tx.exec("UPDATE todo SET completed = ?, updatedAt = ? WHERE id = ?", [
      a.completed ? 1 : 0,
      a.updatedAt,
      a.id,
    ]);
  },
  setColor: (tx: SqlMutationTx, raw: unknown) => {
    const a = setColorArgs.parse(raw);
    tx.exec("UPDATE todo SET color = ?, updatedAt = ? WHERE id = ?", [
      normalizeColor(a.color),
      a.updatedAt,
      a.id,
    ]);
  },
  deleteTodo: (tx: SqlMutationTx, raw: unknown) => {
    const a = deleteTodoArgs.parse(raw);
    tx.exec("DELETE FROM todo WHERE id = ?", [a.id]);
  },
});

export interface TodoApiOptions {
  /** The unified fleet ingress for reads, SQL writes, migrations, and subscriptions. */
  url: string;
  /** The server-only public SQL bearer. Never returned to the browser. */
  token: string;
  /** Optional distinct public WebSocket ingress; normally derived from {@link url}. */
  wsUrl?: string;
}

export function createTodoApi(opts: TodoApiOptions): RindleApiServer<User> {
  return createRindleApiServer<User>({
    rindle: { url: opts.url, token: opts.token, wsUrl: opts.wsUrl },
    queries: apiQueries,
    mutators: apiMutators,
    authorizeQuery: () => true,
    authorizeMutation: () => true,
  });
}

export function resolveRindle(env: Record<string, string | undefined>): TodoApiOptions {
  const url = env.RINDLE_URL;
  const token = env.RINDLE_DATABASE_TOKEN;
  if (!url || !token) {
    throw new Error(
      "RINDLE_URL + RINDLE_DATABASE_TOKEN are required — start the app with `rindle dev -- …`",
    );
  }
  const wsUrl = env.RINDLE_WS_URL;
  return { url, token, ...(wsUrl ? { wsUrl } : {}) };
}

export function httpErrorOf(err: unknown): { status: number; message: string } {
  const status = typeof err === "object" && err !== null ? (err as { status?: unknown }).status : undefined;
  return {
    status: typeof status === "number" ? status : 500,
    message: String(err instanceof Error ? err.message : err),
  };
}
