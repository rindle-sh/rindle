# @rindle/client — backend-agnostic flat-change client core

The shared core for the flat-change client: a **typed schema + query builder**, the
**`ArrayView`** (folds a flat change stream into a live, materialized tree), the **comparator**,
the **`Store`**, and the **`Backend`** seam. It talks to no engine directly — pair it with a
backend such as [`@rindle/wasm`](../wasm) (local, in-process) or a remote (network) backend.

```ts
import { table, string, number, createSchema, gt } from "@rindle/client";

const issue = table("issue")
  .columns({ id: number(), title: string(), priority: number() })
  .primaryKey("id");
const schema = createSchema({ tables: [issue] }); // pass to `new Store(schema, backend)`

const view = store.query.issue.where.priority(gt(3)).orderBy("id", "asc").materialize();
view.subscribe(render); // typed, reference-stable, always == a fresh query
```

Everything an app builds on lives here:

- the **query builder** (`where` proxies, `or`/`and`/`exists`, `sub`, `orderBy`/`limit`/`start`,
  `.one()`, `countAs`/`count()`/`groupBy`/`having`, `select`);
- **named queries & fragments** (`defineQuery`, `defineFragment`, `defineRelationships`/`rel`) —
  the co-located, waterfall-free composition surface;
- the **isomorphic-mutator vocabulary** (`defineMutators` → `shared(args, gen)`, `IsoTx`,
  `MutationGen`, `MutatorCtx`) — one generator body per write, run on both tiers;
- **local-only tables** (`table(name, { local: true })` + `extendSchema` + `store.writeLocal`) —
  client-authoritative state that never syncs or rebases.

## Docs

- **[Supported query shapes](https://rindle.sh/docs/supported-queries-ts)** — the honest matrix of
  what the builder can lower, and the build-time rejections.
- **[Compose the UI with fragments](https://rindle.sh/docs/fragments)** — `defineQuery` /
  `defineFragment` / `defineRelationships` in full.
- **[Isomorphic mutators](https://rindle.sh/docs/mutators)** — the write contract: the op
  vocabulary, reads, `ctx.user`, the determinism rules.
- **[The browser client](https://rindle.sh/docs/client)** — this core behind the optimistic synced
  store, including local-only tables.
- **[Schema & migrations](https://rindle.sh/docs/schema)** — why the schema is generated from SQL,
  and the one allowed hand-edit.

Markdown mirrors live at `https://rindle.sh/docs/<slug>.md`; for agents:
[llms.txt](https://rindle.sh/llms.txt).

## Build

`pnpm run build` (from the repo root) compiles `src/` → `dist/` (`.js` + `.d.ts`) — what the package
publishes. In-repo, tooling resolves the TS source directly via the `@rindle/source` export condition,
so `pnpm test` runs without a build.
