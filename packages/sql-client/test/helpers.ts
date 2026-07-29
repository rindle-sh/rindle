import type { Fetch, WireSqlValue } from "../src/index.ts";

export interface FetchCall {
  url: string;
  init: RequestInit;
  body: Record<string, unknown> | undefined;
}

export function response(body: unknown, status = 200, headers?: HeadersInit): Response {
  return new Response(body === undefined ? undefined : JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json", ...Object.fromEntries(new Headers(headers).entries()) },
  });
}

export function wireStatementResult(
  columns: Array<{ name: string; decltype: string | null }> = [],
  rows: WireSqlValue[][] = [],
  overrides: Partial<Record<"rows_affected" | "last_insert_rowid" | "rows_read" | "rows_written", unknown>> = {},
): Record<string, unknown> {
  return {
    columns,
    rows,
    rows_affected: overrides.rows_affected ?? 0,
    last_insert_rowid: overrides.last_insert_rowid ?? null,
    rows_read: overrides.rows_read ?? null,
    rows_written: overrides.rows_written ?? null,
  };
}

export function executeResponse(
  cursor: string | null = null,
  result = wireStatementResult(),
): Record<string, unknown> {
  return {
    result,
    commit_cursor: cursor,
    routing: { served_by: "master", applied_lag_ms: null, fence_fallback: false },
  };
}

export function scriptedFetch(
  handler: (call: FetchCall, index: number) => Response | Promise<Response>,
): { fetch: Fetch; calls: FetchCall[] } {
  const calls: FetchCall[] = [];
  const fetch: Fetch = async (input, init = {}) => {
    const rawBody = typeof init.body === "string" && init.body !== "" ? (JSON.parse(init.body) as Record<string, unknown>) : undefined;
    const call = { url: String(input), init, body: rawBody };
    calls.push(call);
    return handler(call, calls.length - 1);
  };
  return { fetch, calls };
}

