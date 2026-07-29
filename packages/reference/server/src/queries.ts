import type { Ast, Query } from "@rindle/client";

export type ServerQueryResult = Ast | Query<any, any, any>;
export type ServerQuery<Ctx, Args> = (ctx: Ctx, args: Args) => ServerQueryResult;
export type ServerQueries<Ctx> = Record<string, ServerQuery<Ctx, any>>;

export interface RunQueryInput<Ctx, Connection> {
  name: string;
  query: ServerQuery<Ctx, any>;
  args: unknown;
  connection: Connection;
}

export type RunQuery<Ctx, Connection> = (input: RunQueryInput<Ctx, Connection>) => ServerQueryResult;

export function defineServerQueries<Q extends ServerQueries<any>>(queries: Q): Q {
  return queries;
}

export function queryResultToAst(result: ServerQueryResult): Ast {
  if (result && typeof result === "object" && "ast" in result && typeof result.ast === "function") {
    return result.ast();
  }
  return result as Ast;
}

export function resolveNamedQuery<Ctx, Connection>(
  opts: { queries?: ServerQueries<Ctx>; runQuery?: RunQuery<Ctx, Connection> },
  name: string,
  args: unknown,
  connection: Connection,
): Ast {
  const query = opts.queries?.[name];
  if (!query) throw new Error(`unknown query: ${name}`);
  const result = opts.runQuery
    ? opts.runQuery({ name, query, args, connection })
    : query({} as Ctx, args as never);
  return queryResultToAst(result);
}
