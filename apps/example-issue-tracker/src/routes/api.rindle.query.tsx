import { createFileRoute } from "@tanstack/react-router";

// The live-query lease endpoint. The authority is reached through a dynamic import inside the
// handler, keeping the daemon client and SQL-only server code out of the browser bundle.
export const Route = createFileRoute("/api/rindle/query")({
  server: {
    handlers: {
      POST: async ({ request }) => {
        const { createIssueApiFromEnv, httpErrorOf } = await import("../../server/app-api.ts");
        try {
          const out = await createIssueApiFromEnv().handleQueryJson(await request.json().catch(() => ({})), {
            user: request.headers.get("x-user") ?? undefined,
            request,
          });
          return Response.json(out);
        } catch (err) {
          const { status, message } = httpErrorOf(err);
          return Response.json({ error: message }, { status });
        }
      },
    },
  },
});
