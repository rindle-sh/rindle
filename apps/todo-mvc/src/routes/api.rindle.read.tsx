import { createFileRoute } from "@tanstack/react-router";

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
