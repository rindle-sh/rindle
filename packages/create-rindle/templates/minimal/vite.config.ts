import { createRequire } from "node:module";
import { dirname, resolve } from "node:path";

import { tanstackStart } from "@tanstack/react-start/plugin/vite";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";
// Opt-in: TanStack's devtools panel (router/store inspectors). Uncomment the plugin below to enable it
// — but KEEP `consolePiping: { enabled: false }` (see the plugins array). Needs `@tanstack/devtools-vite`.
// import { devtools } from "@tanstack/devtools-vite";

// The in-process wasm IVM engine ships prebuilt inside the @rindle/wasm package (pkg/rindle_bg.wasm).
// Alias `rindle-wasm-bin?url` to that file so the client can `import url from "rindle-wasm-bin?url"`
// without relying on a package subpath export. We resolve it off the package's main entry so it works
// wherever the package manager installed it.
const require = createRequire(import.meta.url);
const wasmBin = resolve(dirname(require.resolve("@rindle/wasm")), "..", "pkg", "rindle_bg.wasm");

export default defineConfig({
  build: { target: "es2022" },
  resolve: {
    alias: [
      // Regex find so the `?url` suffix survives the rewrite in both dev and build.
      { find: /^rindle-wasm-bin/, replacement: wasmBin },
    ],
  },
  // Keep the @rindle/* graph bundled through the SSR/prerender pass (the wasm engine itself is a
  // client-only dynamic import in src/rindle-client.ts, so it never loads in the shell pass).
  ssr: { noExternal: [/^@rindle\//] },
  // No `server.proxy`: /api/rindle/* are Start server routes (src/routes/api.rindle.*.tsx) served by
  // this same dev server. Browser subscriptions discover the fleet edge from their query lease.
  plugins: [
    // If you add TanStack's devtools panel, DISABLE console piping. The piping cross-forwards console
    // between the SSR server and the browser; a repeated log (a React warning, a Rindle fetch-retry)
    // echoes back and forth and snowballs into a multi-GB HMR-websocket payload that OOM-kills
    // `vite dev`. Disabling it keeps the panel + source injection, just not the console bridge.
    // devtools({ consolePiping: { enabled: false } }),
    tanstackStart(),
    react(),
  ],
});
