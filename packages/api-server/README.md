# @rindle/api-server

Framework-neutral helpers for your app's **authority tier** in a Rindle deployment.
Stateless and transport-agnostic: it authenticates the caller, resolves **named queries**
to approved ASTs, and drives the **same isomorphic mutators** the browser predicted —
rendering their logical ops to SQL and sending authoritative mutation transactions through
`@rindle/sql-client`, while named queries, materializations, and rooms use the daemon control plane.
The write-master only ever sees approved ASTs and approved write/rejection records.

```ts
import {
  createRindleApiServer,
  defineApiMutators,
  registerQueries,
  scoped,
  sharedApiMutators,
} from "@rindle/api-server";
import { issuesPageQuery } from "./src/IssueList.queries.ts";
import { mutators, schema } from "./shared/app-def.ts";

// The authenticated principal a shared mutator body sees as ctx.user (never a client arg):
const sharedCtx = (ctx) => {
  if (!ctx.user) throw new Error("unauthenticated");
  return { user: ctx.user };
};

const api = createRindleApiServer({
  // One ingress: reads/materialization + SQL writes. Query leases also derive its public ws URL.
  rindle: { url: process.env.RINDLE_URL!, token: process.env.RINDLE_DATABASE_TOKEN! },
  schema,                                             // drives the SQL renderer for the yielded ops
  queries: registerQueries([issuesPageQuery]),        // the co-located defineQuery values, listed
  mutators: sharedApiMutators(mutators, sharedCtx),   // the SAME bodies the browser predicts
  authorizeQuery: ({ user }) => Boolean(user),
  authorizeMutation: ({ user }) => Boolean(user),
});
// You own the HTTP: mount api.handleQueryJson / handleReadJson / handleMutateJson
// on api.routes ({ query: "/api/rindle/query", read: "/api/rindle/read", mutate: "/api/rindle/mutate" }).
```

The query lease returned to the browser includes the public `wsEndpoint` and the follower's opaque
affinity ticket. `wsEndpoint` is derived from `rindle.url`; pass `rindle.wsUrl` when HTTP and
WebSocket ingress differ. This lets `createRindleClient` discover subscriptions from its first
same-origin lease instead of relying on an application-authored runtime-config route.

The API server constructs and owns both clients in this normal setup, so the application imports
only `@rindle/api-server`. Server-only mutator overrides can
drop to raw SQL without constructing another client:

```ts
const serverMutators = defineApiMutators({
  revise: async (tx, { id }) => {
    await tx.sql.execute("update issue set revision = revision + 1 where id = ?", [id]);
    const [row] = await tx.sql.query<{ revision: number }>(
      "select revision from issue where id = ?",
      [id],
    ); // same mutation transaction; reads its own writes
    if (!row) throw new Error("issue disappeared");
  },

  importOnce: scoped(async (scope, { key }) => {
    await scope.sql.execute("insert into import_log (key) values (?) on conflict do nothing", [key]);
    await scope.transact((tx) => tx.sql.execute("insert into issue (id) values (?)", [key]));
  }),
});
```

`scope.sql` is deliberately outside the mutator transaction: its calls commit independently and can
repeat when an envelope is retried, so outside writes need their own unique/idempotency key. Call
`api.close()` during shutdown to abort work on the internally-created SQL client. Import
`createSqlClient` from `@rindle/sql-client` only for standalone SQL, migrations, ORM integration, or
other advanced work that is not part of this API-server path.

## Docs

Full docs — named queries & server-only divergence, driving the shared mutators, the
two rejection shapes, pinned queries & the one-shot read, and bring-your-own-HTTP:
**[rindle.sh/docs/api-server](https://rindle.sh/docs/api-server)** · markdown mirror:
[`api-server.md`](https://rindle.sh/docs/api-server.md) · the mutator contract:
[rindle.sh/docs/mutators](https://rindle.sh/docs/mutators) · for agents:
[llms.txt](https://rindle.sh/llms.txt)
