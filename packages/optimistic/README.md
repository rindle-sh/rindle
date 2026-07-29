# @rindle/optimistic

The browser store for a synced Rindle app: optimistic writes over the local-first wasm
engine. `createRindleClient` boots the wasm engine, opens the ws subscription to the
daemon, resolves query leases through your API server, and runs the mutation queue.

The local IVM engine **is** the rebase function: it keeps an authoritative fork per base
table and reconciles every server batch by **rewinding** to the authoritative state and
**re-invoking** the still-pending mutators against it — no inverse mutations, no rollback
code. A rejected write snaps back on its own.

```ts
import { createRindleClient } from "@rindle/optimistic";
import { mutators, schema } from "../shared/app-def.ts"; // isomorphic mutators + generated schema

export const app = await createRindleClient({
  schema,
  mutators,                                  // one generator body per mutator, run on both tiers
  user: () => currentUser(),                 // the acting principal a mutator sees as ctx.user
  api: { url: "" },                          // your API server (named queries + mutations)
  // No topology config: the first query lease returns the ws endpoint + placement ticket.
});

app.mutate.createIssue({ id, title: "ship it", createdAt: Date.now() }); // applies NOW, rebases on confirm
```

For flicker-free client navigation, retain a named query before committing the route:

```ts
await app.ensure(postQuery(slug), { until: "present" });
```

The default `until: "complete"` waits for a server-authoritative result. `"present"` returns as
soon as the local view has a row (including one supplied by another query's footprint), while the
same retain continues revalidating in the background; an authoritative empty result also releases
the wait. This does not add a `partial` `ResultType`—the query remains `unknown` until the server
answers. TanStack Router/Start apps can bind this once through `@rindle/tanstack`:

```ts
loader: rindle.loader({
  query: ({ params }) => postQuery(params.slug),
  until: "present",
});
```

Also in the box: **folded mutations** (collapse a slider drag into one last-value-wins
server write), **local-only tables** (`{ local: true }` — client state that never syncs
or rebases, written with `store.writeLocal`), the per-query **`ResultType`** axis
(loading vs. server-answered) and the separate **pending** axis (is an unconfirmed write
touching this query?).

## Docs

Full docs — wiring, optimistic writes & rebase, folded writes, local-only tables, and the
loading/pending signals: **[rindle.sh/docs/client](https://rindle.sh/docs/client)** ·
markdown mirror: [`client.md`](https://rindle.sh/docs/client.md) · the mutator contract:
[rindle.sh/docs/mutators](https://rindle.sh/docs/mutators) · for agents:
[llms.txt](https://rindle.sh/llms.txt)
