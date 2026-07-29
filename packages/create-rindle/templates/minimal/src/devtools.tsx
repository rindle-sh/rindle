// The in-browser Rindle devtools pane (mutation timeline / queries inspector / delta stream), mounted
// ONLY in development. `import.meta.env.DEV` is a STATIC `false` in a production build, so Vite/Rollup
// fold the ternary to `null` and dead-code-eliminate the dynamic `import("@rindle/react-devtools")` —
// neither @rindle/react-devtools nor @rindle/devtools end up in the prod bundle. The client is attached
// to the pane in src/rindle-client.ts (also dev-gated); this file is just the UI mount.

import { lazy, Suspense, useEffect, useState } from "react";

// In production this is `null` (and the import is tree-shaken); in dev it lazily loads the panel chunk,
// so the devtools code is fetched only when the app actually runs in development.
const Panel = import.meta.env.DEV
  ? lazy(() => import("@rindle/react-devtools").then((m) => ({ default: m.RindleDevtools })))
  : null;

export function DevTools() {
  // Client-only: don't render (or fetch) the pane during the SSR/prerender shell pass — it has no value
  // there. Mounting after the first client effect also keeps the server and hydration markup identical
  // (both render nothing here), so there is never a hydration mismatch.
  const [mounted, setMounted] = useState(false);
  useEffect(() => setMounted(true), []);
  if (!Panel || !mounted) return null;
  return (
    <Suspense fallback={null}>
      <Panel />
    </Suspense>
  );
}
