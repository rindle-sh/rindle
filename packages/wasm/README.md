# @rindle/wasm — local (in-process) IVM backend

The WASM backend for [`@rindle/client`](../client): the Rust IVM engine (a port of Zero's ZQL
dataflow engine) compiled to WebAssembly, running queries **in-process**. It re-exports
`@rindle/client`, so a local app can import everything from here.

```ts
import { table, string, number, boolean, createSchema, createWasmStore } from "@rindle/wasm";

const issue = table("issue")
  .columns({ id: number(), title: string(), closed: boolean() })
  .primaryKey("id");
const schema = createSchema({ tables: [issue] });

const store = await createWasmStore(schema); // inits the wasm + wires the local backend

const view = store.query.issue.where.closed(false).materialize();
view.subscribe((data) => render(data)); // typed, reference-stable, always == a fresh query
await store.write((tx) => tx.add("issue", { id: 1, title: "hi", closed: false }));
```

`createWasmStore(schema)` calls `initWasm()` for you. If you build a `WasmBackend` directly,
`await initWasm()` first — and pass `initWasm(moduleOrPath)` to supply the wasm yourself (a
`WebAssembly.Module`, URL, or bytes).

## Docs

Full docs — the standalone browser engine, query surface, and the change model:
**[rindle.sh/docs/wasm-client](https://rindle.sh/docs/wasm-client)** · markdown mirror:
[`wasm-client.md`](https://rindle.sh/docs/wasm-client.md) · query shapes:
[supported-queries-ts](https://rindle.sh/docs/supported-queries-ts) · for agents:
[llms.txt](https://rindle.sh/llms.txt)

## Building & publishing

Two build steps. The Rust → wasm artifact (`pkg/`), needed once (and whenever the engine changes):

```sh
pnpm --filter @rindle/wasm build:wasm   # or: packages/wasm/build.sh [dev|release|wasm-release]
```

Requires the Rust toolchain + `wasm-bindgen` (`cargo install wasm-bindgen-cli`). The output is a
`--target web` ESM bundle that runs in browsers, bundlers, and Node.

Then the TypeScript → `dist/` compile that produces the published, runnable `.js` + `.d.ts`:

```sh
pnpm run build                       # builds @rindle/client then @rindle/wasm dist/ (run from the repo root)
```

The published package ships `dist/` (compiled) + `pkg/` (the wasm). In-repo the test suite resolves
the TS source directly (via the `@rindle/source` export condition), so `pnpm test` needs no build.

### Browser smoke test

`pnpm test:browser` builds `dist/`, then loads the package in **headless Chrome** (taking the real
browser `fetch` + `WebAssembly.instantiateStreaming` init path) and runs a query. Needs system
Chrome/Chromium (`CHROME_BIN` to override); skipped with a message when none is installed.
