import { createFileRoute } from "@tanstack/react-router";

// The live-query lease endpoint, mounted as a TanStack Start server route (`/api/rindle/query`). The
// authority is reached through a dynamic import INSIDE the handler, so Start strips it — and all its
// server-only deps (the daemon client, SQL) — from the client bundle entirely. The browser hits this
// to mint a lease, then opens its subscription straight to the daemon ws.
export const Route = createFileRoute("/api/rindle/query")({
  server: {
    handlers: {
      POST: async ({ request }) => {
        const { handleRindleJson } = await import("../../server/rindle-http.ts");
        return handleRindleJson("query", request);
      },
    },
  },
});
