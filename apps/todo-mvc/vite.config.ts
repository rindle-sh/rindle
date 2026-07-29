import { createRequire } from "node:module";

import { tanstackStart } from "@tanstack/react-start/plugin/vite";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

const require = createRequire(import.meta.url);
// Resolve the prebuilt wasm binary via its package subpath export. Don't route through the
// package's main entry (`require.resolve("@rindle/wasm")` → dist/index.js): in the workspace the
// CI `js` lane only runs `build:wasm` (which emits pkg/), never `build` (which emits dist/), so
// the main entry doesn't exist at vite-config load time and resolution would throw.
const wasmBin = require.resolve("@rindle/wasm/pkg/rindle_bg.wasm");

export default defineConfig({
  build: { target: "es2022" },
  resolve: {
    conditions: ["@rindle/source"],
    alias: [{ find: /^rindle-wasm-bin/, replacement: wasmBin }],
  },
  ssr: {
    noExternal: [/^@rindle\//],
    resolve: { conditions: ["@rindle/source"] },
  },
  plugins: [tanstackStart(), react()],
});
