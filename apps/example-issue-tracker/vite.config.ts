import { fileURLToPath } from "node:url";

import { cloudflare } from "@cloudflare/vite-plugin";
import { tanstackStart } from "@tanstack/react-start/plugin/vite";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// CF=1 turns on the Cloudflare target: `vite build` emits a workerd Worker bundle from worker.ts
// (the deploy artifact) and `vite preview` runs it in miniflare/workerd. It is OFF by default so the
// existing node paths are untouched — `pnpm dev` keeps the node SSR dev server, and the plain
// `vite build` the smoke test runs keeps emitting the node bundle.
const cloudflareTarget = !!process.env.CF;

// The prebuilt wasm engine binary (packages/wasm/build.sh) — aliased so the client can
// `import url from "rindle-wasm-bin?url"` without a package subpath export. This app lives under
// apps/, so the sibling @rindle/* libraries are two levels up under ../../packages.
const wasmBin = fileURLToPath(new URL("../../packages/wasm/pkg/rindle_bg.wasm", import.meta.url));
const roomToken = fileURLToPath(new URL("../../packages/room/src/token.ts", import.meta.url));
// The sibling `@rindle/*` workspace sources. Aliasing straight to `src/index.ts` (the same TS the
// `@rindle/source` export condition points at) compiles them from source in EVERY Vite environment —
// including the Start prerender/server pass, where the export-condition resolution doesn't apply —
// so dev never runs against a stale dist build and the shell pass resolves them cleanly.
const src = (p: string) => fileURLToPath(new URL(`../../packages/${p}/src/index.ts`, import.meta.url));

export default defineConfig({
  // src/client.ts top-level-awaits the wasm init + client wire-up.
  build: { target: "es2022" },
  resolve: {
    // Compile the workspace packages from their TypeScript sources (same condition the
    // node test suites use), so dev never runs against a stale dist build.
    conditions: ["@rindle/source"],
    alias: [
      // Regex find so the `?url` suffix survives the rewrite in both dev and build.
      { find: /^rindle-wasm-bin/, replacement: wasmBin },
      { find: /^@rindle\/client$/, replacement: src("client") },
      { find: /^@rindle\/react$/, replacement: src("react") },
      { find: /^@rindle\/optimistic$/, replacement: src("optimistic") },
      { find: /^@rindle\/normalized$/, replacement: src("normalized") },
      { find: /^@rindle\/remote$/, replacement: src("remote") },
      { find: /^@rindle\/wasm$/, replacement: src("wasm") },
      // @rindle/api-server loads this export lazily for room leases. The explicit api-server
      // source alias bypasses package export-condition resolution, so its source subpath must
      // stay in the same graph too.
      { find: /^@rindle\/room\/token$/, replacement: roomToken },
      // The SSR server entry and /api server routes pull these into the Vite graph (the client-only
      // SPA never did), and the server pass doesn't apply the `@rindle/source` export condition — so
      // they need explicit src aliases like the rest.
      { find: /^@rindle\/api-server$/, replacement: src("api-server") },
      { find: /^@rindle\/daemon-client$/, replacement: src("daemon-client") },
      // Pulled in transitively by @rindle/api-server (the daemon-backend mutator read compiler).
      { find: /^@rindle\/query-compiler$/, replacement: src("query-compiler") },
      // Pulled in transitively by @rindle/api-server (the 222 SQL plane client).
      { find: /^@rindle\/sql-client$/, replacement: src("sql-client") },
    ],
  },
  // The wasm engine never loads in the shell pass — it's a client-only dynamic import (see
  // src/client.ts `bootClient`); the aliases above keep the rest of the `@rindle/*` graph bundled
  // from source rather than externalized as published dist.
  ssr: { noExternal: [/^@rindle\//] },
  plugins: [
    // The Cloudflare plugin (gated by CF=1) builds worker.ts in the `ssr` vite environment for
    // workerd and serves it via miniflare in `vite preview`/`dev`. It MUST come first. Off by default
    // so the node dev/build paths are unaffected (see `cloudflareTarget` above).
    ...(cloudflareTarget ? [cloudflare({ viteEnvironment: { name: "ssr" } })] : []),
    // TanStack Start in SSR mode (the default). First visit is SERVER-RENDERED: the route loader
    // reads each first-paint query ONCE through the API tier's /api/rindle/read (SSR-DESIGN.md §6),
    // the assembled rows are inlined into the HTML, and the browser hydrates them for an instant
    // correct first paint. The in-browser wasm IVM engine is still client-only — it boots AFTER
    // hydration (src/RindleApp.tsx) and the live `subscribe` reconciles, converting the page to a
    // live SPA. So SSR renders the *view*, never the engine. File-based routes live in src/routes,
    // including `/api/rindle/*` server routes; worker.ts adds Cloudflare env + cron glue.
    tanstackStart(),
    react(),
  ],
});
