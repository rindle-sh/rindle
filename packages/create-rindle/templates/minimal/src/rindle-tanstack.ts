// The one TanStack Router/Start binding for this app. Routes declare their Rindle queries through
// `rindle.loader(...)`; the root mounts `rindle.Provider` to merge SSR slices and hand the seeded
// store off to this same live browser client after hydration.

import { createRindleTanStack } from "@rindle/tanstack";

import { schema } from "../shared/app-def.ts";
import { bootClient } from "./rindle-client.ts";

export const rindle = createRindleTanStack({
  schema,
  boot: bootClient,
  preload: async (queries) => {
    // The adapter only calls preload on the server. Keep the static Vite guard as well so ssr.ts —
    // which builds the daemon client and reads server env — is eliminated from the browser build.
    if (!import.meta.env.SSR) return {};
    const { preloadRindle } = await import("./ssr.ts");
    return preloadRindle([...queries]);
  },
});
