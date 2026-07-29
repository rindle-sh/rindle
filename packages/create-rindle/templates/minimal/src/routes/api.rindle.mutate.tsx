import { createFileRoute } from "@tanstack/react-router";

// The authoritative mutation endpoint (`/api/rindle/mutate`), mounted as a TanStack Start server route.
// The client's optimistic writes flush here; the authority validates args, enforces policy, and runs
// the approved SQL against the daemon. The dynamic import keeps that server-only code out of the client
// bundle.
export const Route = createFileRoute("/api/rindle/mutate")({
  server: {
    handlers: {
      POST: async ({ request }) => {
        const { handleRindleJson } = await import("../../server/rindle-http.ts");
        return handleRindleJson("mutate", request);
      },
    },
  },
});
