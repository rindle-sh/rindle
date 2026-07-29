import { createFileRoute } from "@tanstack/react-router";

// The one-shot read endpoint. SSR normally calls the authority in-process through src/ssr.ts, but
// this keeps HTTP parity for browser live setup, tests, and non-Start clients.
export const Route = createFileRoute("/api/rindle/read")({
  server: {
    handlers: {
      POST: async ({ request }) => {
        const { createIssueApiFromEnv, httpErrorOf } = await import("../../server/app-api.ts");
        try {
          const out = await createIssueApiFromEnv().handleReadJson(await request.json().catch(() => ({})), {
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
