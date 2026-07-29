import { fileURLToPath } from "node:url";

import { cloudflare } from "@cloudflare/vite-plugin";
import { tanstackStart } from "@tanstack/react-start/plugin/vite";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// CF=1 turns on the Cloudflare target: `vite build` emits a workerd Worker bundle from worker.ts and
// `vite preview` runs it in miniflare/workerd. OFF by default so the node paths are untouched —
// `pnpm dev` (the Rust-daemon harness in server/dev.ts) keeps its node SSR dev server.
const cloudflareTarget = !!process.env.CF;

// The prebuilt wasm engine binary (packages/wasm/build.sh) — aliased so the client can
// `import url from "rindle-wasm-bin?url"` without a package subpath export. This app lives under
// apps/, so the sibling @rindle/* libraries are two levels up under ../../packages.
const wasmBin = fileURLToPath(new URL("../../packages/wasm/pkg/rindle_bg.wasm", import.meta.url));
const roomToken = fileURLToPath(new URL("../../packages/room/src/token.ts", import.meta.url));
// The sibling `@rindle/*` workspace sources. Aliasing straight to `src/index.ts` (the same TS the
// `@rindle/source` export condition points at) compiles them from source in EVERY Vite environment —
// including the Start prerender/server pass — so dev never runs against a stale dist build.
const src = (p: string) => fileURLToPath(new URL(`../../packages/${p}/src/index.ts`, import.meta.url));

export default defineConfig({
  build: { target: "es2022" },
  resolve: {
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
      // @rindle/api-server loads this export lazily for room leases. Keep the subpath on source
      // when the package itself is source-aliased below.
      { find: /^@rindle\/room\/token$/, replacement: roomToken },
      // The SSR+API server entry (src/server.ts) pulls these into the Vite graph; the server pass
      // doesn't apply the `@rindle/source` export condition, so they need explicit src aliases.
      { find: /^@rindle\/api-server$/, replacement: src("api-server") },
      { find: /^@rindle\/daemon-client$/, replacement: src("daemon-client") },
      // Pulled in transitively by @rindle/api-server (the daemon-backend mutator read compiler).
      { find: /^@rindle\/query-compiler$/, replacement: src("query-compiler") },
      // Pulled in transitively by @rindle/api-server (the 222 SQL plane client).
      { find: /^@rindle\/sql-client$/, replacement: src("sql-client") },
    ],
  },
  // The wasm engine never loads in the shell pass — it's a client-only dynamic import (rindle-client.ts
  // `bootClient`); the aliases above keep the rest of the `@rindle/*` graph bundled from source.
  ssr: { noExternal: [/^@rindle\//] },
  server: {
    proxy: {
      // The app's API server (serverless tier). The daemon ws is connected to directly — websockets
      // carry no CORS, and the public plane only accepts opaque leases anyway.
      "/api": "http://127.0.0.1:7700",
    },
  },
  plugins: [
    ...(cloudflareTarget ? [cloudflare({ viteEnvironment: { name: "ssr" } })] : []),
    tanstackStart(),
    react(),
  ],
});
