# @rindle/devtools

Framework-agnostic, **dev-only** in-browser devtools core for [Rindle](https://github.com/rindle-sh/rindle).
It attaches to a running client over the read-only seams the client already exposes and maintains a
live read-model of three things (`DEBUG-TOOLS-BROWSER-DESIGN.md` §4):

- **Mutation timeline** — the fork/rebase optimistic loop made visible: each mutation `invoke → pending
  → confirmed/dropped`, with a heuristic **snap-back** highlight when a prediction diverged from the
  authoritative server result. This is the thing no refetch-based devtools can show.
- **Queries inspector** — every live materialized view: its AST, `resultType`, row count + a sample,
  and whether a pending mutation touches its tables (the pending axis).
- **Delta stream** — authoritative server deltas when the backend can expose them, falling back to
  the local per-query view stream for purely local/in-process backends.

This package has **no DOM assumptions** — pair it with a panel (`@rindle/react-devtools`) or read the
model yourself. It surfaces state the client already holds; it adds no hot-path instrumentation.

Full docs — wiring, the SSR-safe mounting pattern, and production tree-shaking:
**[rindle.sh/docs/devtools](https://rindle.sh/docs/devtools)** · markdown mirror:
[`devtools.md`](https://rindle.sh/docs/devtools.md) · for agents:
[llms.txt](https://rindle.sh/llms.txt)

## Usage

Attach in development only, so the pane and its deps tree-shake out of production:

```ts
if (import.meta.env.DEV) {
  const { attachDevtools } = await import("@rindle/devtools");
  attachDevtools(app); // app from createRindleClient({ ... }) — anything with { store, backend }
}
```

`attachDevtools` registers the instance on `globalThis.__RINDLE_DEVTOOLS__` (the discovery pattern a
panel uses) and returns a `DevtoolsCore`:

```ts
const core = attachDevtools(app);
core.subscribe(() => {
  const { timeline, queries, deltas } = core.getState();
  // …render however you like
});
core.detach(); // unwind: stop the poll, drop the tap, deregister
```

A panel that mounts before the app can discover it lazily:

```ts
import { getDevtoolsHub, getDevtoolsCore } from "@rindle/devtools";
getDevtoolsHub().subscribe(() => {
  const core = getDevtoolsCore();
  // …a client just attached
});
```

## How it stays zero-cost in production

The core reads `Store.__inspect()` / `Store.__attachDevtools()`, the optimistic backend's
`__inspect()`, and an optional backend server-delta tap — small read-only/additive methods. Nothing
here runs unless you import this package and call `attachDevtools`, which you do only in a dev build.
The wasm engine is untouched.

See the design doc for the full picture and the deferred phases (normalized-cache view, consistency
check, engine internals).
