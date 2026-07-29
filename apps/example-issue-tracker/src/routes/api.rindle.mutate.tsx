import { createFileRoute } from "@tanstack/react-router";

// The authoritative mutation endpoint. The client's optimistic queue flushes here; the authority
// validates args, enforces policy, and runs approved SQL against the daemon.
export const Route = createFileRoute("/api/rindle/mutate")({
  server: {
    handlers: {
      POST: async ({ request }) => {
        const { createIssueApiFromEnv, httpErrorOf } = await import("../../server/app-api.ts");
        try {
          const out = await createIssueApiFromEnv().handleMutateJson(await request.json().catch(() => ({})), {
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
