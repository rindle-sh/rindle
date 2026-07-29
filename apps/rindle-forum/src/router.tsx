// The TanStack Router instance. The route tree is generated from src/routes/* by the Start/router
// plugin into routeTree.gen.ts (run `pnpm generate-routes` to refresh it outside a dev/build).

import { createRouter as createTanStackRouter } from "@tanstack/react-router";

import { routeTree } from "./routeTree.gen";

export function getRouter() {
  return createTanStackRouter({
    routeTree,
    scrollRestoration: true,
    defaultPreload: "intent",
  });
}

declare module "@tanstack/react-router" {
  interface Register {
    router: ReturnType<typeof getRouter>;
  }
}
