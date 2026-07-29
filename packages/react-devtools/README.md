# @rindle/react-devtools

A floating, **dev-only** React panel for the [Rindle](https://github.com/rindle-sh/rindle) in-browser
devtools — the UI over [`@rindle/devtools`](../devtools). It renders the mutation **timeline** (the
fork/rebase loop, with the snap-back highlight), the **queries** inspector, and the live **delta**
stream (`DEBUG-TOOLS-BROWSER-DESIGN.md` §4).

Pure React + inline styles — no CSS import, no build step beyond `tsc`.

Full docs — wiring, the SSR-safe mounting pattern, and production tree-shaking:
**[rindle.sh/docs/devtools](https://rindle.sh/docs/devtools)** · markdown mirror:
[`devtools.md`](https://rindle.sh/docs/devtools.md) · for agents:
[llms.txt](https://rindle.sh/llms.txt)

## Usage

Mount the panel once near your app root, and attach a client — both behind a dev flag so they
tree-shake out of production:

```tsx
import { RindleDevtools } from "@rindle/react-devtools";

function Root() {
  return (
    <>
      <App />
      {import.meta.env.DEV && <RindleDevtools />}
    </>
  );
}
```

```ts
// somewhere during dev bootstrap, after createRindleClient:
if (import.meta.env.DEV) {
  const { attachDevtools } = await import("@rindle/devtools");
  attachDevtools(app);
}
```

The panel auto-discovers the most recently attached client through the global hub. It starts as a
small `🌊 Rindle` launcher in the bottom-right; click to open.

### Props

| Prop          | Type           | Default | Description |
| ------------- | -------------- | ------- | ----------- |
| `core`        | `DevtoolsCore` | —       | Bind to a specific core instead of auto-discovering. |
| `defaultOpen` | `boolean`      | `false` | Open the panel on first mount. |

You can also drive the core directly (`attachDevtools` is re-exported here for convenience). The data
logic lives in `@rindle/devtools`; this package is only the rendering.
