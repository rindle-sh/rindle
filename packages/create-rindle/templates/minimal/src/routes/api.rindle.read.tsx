import { createFileRoute } from "@tanstack/react-router";

// The one-shot read endpoint (`/api/rindle/read`), mounted as a TanStack Start server route. Kept for
// non-TanStack/Worker clients and HTTP parity — this app's own SSR reads call the authority IN-PROCESS
// (src/ssr.ts), skipping the network hop. The dynamic import keeps the server-only authority out of the
// client bundle.
export const Route = createFileRoute("/api/rindle/read")({
  server: {
    handlers: {
      POST: async ({ request }) => {
        const { handleRindleJson } = await import("../../server/rindle-http.ts");
        return handleRindleJson("read", request);
      },
    },
  },
});
