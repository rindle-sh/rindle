<!-- GENERATED FILE — do not edit.
     Source: product-page/src/content/docs/api-server.md (https://rindle.sh/docs/api-server)
     and:    product-page/src/content/docs/daemon.md (https://rindle.sh/docs/daemon)
     Regenerate: node product-page/scripts/gen-skill.mjs -->

# The API server

> Your app's authority — a stateless, serverless-shaped tier that authenticates the caller, resolves named queries to ASTs, drives the same isomorphic mutators the browser predicted into SQL, and keeps pinned queries warm for one-shot reads.

The API server is **your app's authority**. It is stateless and
serverless-shaped — every request could be a fresh lambda — and it sits between the
untrusted browser and the [data tier's private control plane](https://rindle.sh/docs/daemon#two-planes).
It does four things:

1. **Authenticates** the caller (your session/JWT, however you do it).
2. **Resolves named queries** to real query ASTs — validating args, and adding any
   tenancy/auth filters the client can't be trusted to apply.
3. **Drives the [same mutators](https://rindle.sh/docs/mutators)** the browser
   predicted — one shared body per name, its logical ops rendered to approved SQL
   under this tier's authority.
4. **Talks to the data tier** through one application-facing Rindle ingress — query leases,
   materializations, rooms, and authoritative mutation SQL are routed to the right internal tier.

It holds no data and no live state. Applications normally import only
[`@rindle/api-server`](#the-server); it owns the request handlers and its internal transports.
Import the lower-level clients directly only for an advanced split/custom deployment or standalone
SQL outside this server.

## Named queries → ASTs

A query is defined **once**, with `defineQuery`, co-located with the component that
reads it (a React-free `*.queries.ts` module — see
[Compose the UI with fragments](https://rindle.sh/docs/fragments)). The same value is callable on the
client (it stamps its result with the wire identity, so a subscription always
**syncs**) and registerable on the server. `registerQueries` turns the list of those
co-located queries into the server's query surface — it re-runs each query's
**validator** on the UNTRUSTED wire args and builds the authoritative AST, so client
and server resolve a **byte-identical** query and a malformed client can't smuggle a
garbage value in:

```ts
import { registerQueries } from "@rindle/api-server";
import { issuesPageQuery, myIssuesQuery } from "../src/components/IssueListItem.queries.ts";
import { issueDetailQuery } from "../src/components/IssueDetail.queries.ts";
import { recentCommentsQuery } from "../src/components/ActivityFeed.queries.ts";
import { usersQuery } from "../src/components/UserBadge.queries.ts";

const apiQueries = registerQueries<User>([
  issuesPageQuery,
  myIssuesQuery,
  issueDetailQuery,
  recentCommentsQuery,
  usersQuery,
]);
```

Each `defineQuery` carries its own arg validation and AST construction next to the
fragment and component it feeds, so the server's query surface is just *the list of
queries the app defines* — no second place to restate the shape, and no chance of
the two tiers drifting.

A **context-scoped** query — "my issues", scoped to the authenticated principal —
declares a second `ctx` parameter on its `build`. The client passes its session ctx
at the callsite; the server injects its AUTHORITATIVE `ApiContext` (`{ user, request }`),
which `registerQueries` forwards as the query's ctx. Context is **off-wire**: the
wire still carries only `{ name, args }`, so a client can never spoof the identity —
there is no owner arg to tamper with, and the server re-derives it from its trusted
session:

```ts
// src/components/IssueListItem.queries.ts — defined ONCE, runs on both tiers
export const myIssuesQuery = defineQuery(
  "myIssues",
  validateMyIssuesArgs,                              // → { limit }; the OWNER is NOT here
  ({ limit }: MyIssuesArgs, ctx: { user: string | undefined }) =>
    q.issue.where.ownerId(ctx.user ?? "").orderBy("createdAt", "desc").limit(limit),
);

// client: passes its session user as ctx — myIssuesQuery({ limit: 20 }, { user })
// server: registerQueries forwards its authoritative ApiContext as ctx; both tiers
//         build the same AST, and the wire carries only { name: "myIssues", args: { limit } }
```

When the server must **diverge** from the client — add a tenancy/auth filter the
client can't see — reach for `defineApiQueries`, which maps a name directly to a
`(ctx, args)` resolver. Register a server-specific query under the same name and it
wins:

```ts
import { defineApiQueries, registerQueries } from "@rindle/api-server";
import type { ApiContext, ApiQueries } from "@rindle/api-server";
import { issuesPageQuery } from "../src/components/IssueListItem.queries.ts";

// Same NAME as the client's issuesPage, but the authority narrows it further — the
// client only ever sends { name, args }, so it never sees this extra filter.
const serverOnly = defineApiQueries<User, ApiQueries<User>>({
  issuesPage: (ctx: ApiContext<User>, args: unknown) =>
    issuesPageQuery.resolve(args).where.ownerId(requireUser(ctx.user)),
});

// Both are plain { name → resolver } maps, so a later entry wins on a name clash:
const apiQueries = { ...registerQueries<User>([issuesPageQuery, /* … */]), ...serverOnly };
```

`issuesPageQuery.resolve(args)` re-runs the same validator and builds the canonical
`Query`; from there the server is free to add what the client can't be trusted to.
`ctx` is an `ApiContext<User>` — `{ user, request }`, where `user` is whatever your
auth produced.

There is no separate "scope key": if two requests resolve to the same canonical
AST, the daemon may dedupe them, because the result is the same. If tenant or user
visibility matters, **encode it into the AST** — a context-scoped query or a
server-side filter like the ones above.

## Driving the shared mutators

The server does **not** re-write each mutator. Your mutators are
[isomorphic](https://rindle.sh/docs/mutators) — one generator body per name,
carrying its own arg schema via `shared(args, gen)` — and `sharedApiMutators`
auto-drives the whole registry: for each mutator it parses the **untrusted** wire
args through the mutator's `.args`, injects this tier's **authenticated** principal
as `ctx.user`, and drives the same body the client predicted, rendering every
yielded op to dialect SQL. The statements run in **one transaction** on the write-master:

```ts
import { sharedApiMutators } from "@rindle/api-server";
import type { MutationContext } from "@rindle/api-server";
import type { MutatorCtx } from "@rindle/client";
import { mutators } from "../shared/app-def.ts";

/** The MutatorCtx a shared body sees on the server: the AUTHENTICATED principal
 *  (throw if absent — a rejection). Never a client-supplied author arg. */
function sharedCtx(ctx: MutationContext<User>): MutatorCtx {
  return { user: requireUser(ctx.user) };
}

const apiMutators = sharedApiMutators(mutators, sharedCtx);
```

That triad — parse, inject, drive — is the whole server side of most mutators, and
it guarantees every shared name has a server implementation by construction.
`MutationContext<User>` is `{ user, envelope, daemon, request }` — it carries the
authenticated `user`, the mutation `envelope` (`{ clientID, mid, name, args }`), and
the `daemon` client if you need it directly.

### Server-only authority

Write an explicit entry **only** for authority the client must *not* predict, and
let it override the auto-wrapped default by key (spread first, override wins):

- **A server-only policy guard** — a check the client deliberately doesn't run, so
  the rejection path stays exercised end to end. Parse the args, run the guard, then
  drive the same shared body with `runSharedMutation`.
- **Relational SQL a keyed op can't express** — an owner-gated cascade, a dedup by a
  non-pk column. `tx.sql` is the transaction-bound raw escape hatch; the client predicts
  the plain shared op and snaps back if the authority disagrees.

```ts
import { defineApiMutators, runSharedMutation, sharedApiMutators } from "@rindle/api-server";
import type { ApiMutator, ApiMutators, MutationContext, ServerMutationTx, SharedMutatorWithArgs } from "@rindle/api-server";
import { issueIdArgs, mutators } from "../shared/app-def.ts";

const apiMutators = defineApiMutators<User, ApiMutators<User>>({
  ...sharedApiMutators(mutators, sharedCtx),

  // (a) a policy layered onto the shared body: guard, then drive the SAME body.
  createIssue: withTitleGuard(mutators.createIssue),

  // (b) the raw-SQL escape hatch: ownership enforced IN the SQL, so a non-owner's
  //     delete is accepted-but-no-op and the optimistic delete snaps back on its own.
  deleteIssue: async (tx: ServerMutationTx, raw: unknown, ctx: MutationContext<User>) => {
    const { id } = issueIdArgs.parse(raw);
    await tx.sql.execute("DELETE FROM issue WHERE id = ? AND ownerId = ?", [id, requireUser(ctx.user)]);
  },
});

/** Wrap a shared mutator with a server-only policy that runs BEFORE any write. */
function withTitleGuard<A extends { title: string }>(gen: SharedMutatorWithArgs<A>): ApiMutator<User, unknown> {
  return (tx, raw, ctx) => {
    const a = gen.args.parse(raw);
    if (/\bspam\b/i.test(a.title)) throw new Error("the word 'spam' is not allowed"); // throw → reject
    return runSharedMutation(gen, a, sharedCtx(ctx), tx);
  };
}
```

`ServerMutationTx` carries both surfaces: the logical
`tx.insert / update / upsert / insertIgnore / delete / row` (rendered to dialect
SQL) *and* raw `tx.sql.execute / batch / query`. Raw queries run against the same
open transaction and therefore read their own writes. The older synchronous
`tx.exec(sql, params)` remains as a compatibility shorthand for queuing a raw
write. These server-only methods are intentionally absent from the shared/browser
transaction: what stays explicit is exactly your authority surface — everything
else is the one shared body.

There are **two rejection shapes**, and both make the client's optimistic write
disappear correctly:

- **Hard reject** — the mutator body (or a guard / arg parse) **throws**. The API
  server calls the write-master's `/reject-mutation`; the client's `onRejected` fires and
  the optimistic row snaps back. (In the example, a title containing the word "spam"
  throws.)
- **Accepted-but-no-op** — the run legitimately changes nothing
  (the `... AND ownerId = ?` guard above, or a shared body whose read-guard
  `return`s early). The write is accepted; when the empty
  authoritative result syncs in, the optimistic change rebases away on its own.

### Work outside the mutation transaction

Wrap a server-only override in `scoped` when it must do work before or after the one
authoritative transaction. `scope.transact` supplies the transaction-bound
`ServerMutationTx`; `scope.sql` is the corresponding raw-SQL escape hatch
**outside** that boundary:

```ts
import { scoped } from "@rindle/api-server";

const importIssue = scoped(async (scope, args: { key: string; issueId: string }) => {
  const [staged] = await scope.sql.query<{ payload: string }>(
    "select payload from staged_import where key = ?",
    [args.key],
  );
  if (!staged) throw new Error("unknown staged import");

  await scope.sql.execute("insert into import_attempt (key) values (?) on conflict do nothing", [args.key]);
  await scope.transact((tx) =>
    tx.sql.execute("insert into issue (id) values (?)", [args.issueId]),
  );
});
```

Every `scope.sql` call commits independently. It can remain visible if
`scope.transact` later rejects or fails, and it may run again when the client retries
an envelope. Give outside writes their own unique/idempotency key (the example's
unique `import_attempt.key`); domain writes normally belong on the `tx.sql` inside
`scope.transact`. A database failure from either facade remains an infrastructure
failure, so the API server does not advance `lmid` and the client can retry.

## The server

`createRindleApiServer` ties the queries, mutators, database connection, daemon client, and your authorizers
together. A Rindle is both layers at once — the SQL database below and the sync/IVM control plane
above — served by one ingress, so the simplest configuration is the unified connection: one URL,
one key.

```ts
import { createRindleApiServer } from "@rindle/api-server";
import { schema } from "../shared/app-def.ts";

const api = createRindleApiServer<User>({
  rindle: {},         // one URL, one key — resolved from RINDLE_URL + RINDLE_DATABASE_TOKEN
                      // (both exported by `rindle dev`); or pass { url, token } explicitly
  schema,             // drives the dialect-SQL renderer for the mutators' logical ops
  queries: apiQueries,
  mutators: apiMutators,
  authorizeQuery: ({ user }) => typeof user === "string" && user.length > 0,
  authorizeMutation: ({ user }) => typeof user === "string" && user.length > 0,
});
```

`rindle` derives both trusted transports against that single origin: the `database` layer for
authoritative mutations through the HCTree master's SQL facade, and the `daemon` layer for query
leases, materialization, and rooms. Both send the same server-only bearer. The fleet edge routes
each request to the right process; applications do not manage separate master/follower URLs or
credentials.

The query-lease response also carries the public `wsEndpoint` and the follower's opaque affinity
ticket. The optimistic browser client opens that endpoint lazily, so the app needs no separate
runtime-config route or browser WebSocket variable. Pass `rindle.wsUrl` only when the public
WebSocket ingress cannot be derived from the HTTP URL.

Explicit `daemon`, `database`, `sql`, and `backend` options remain advanced escape hatches for a
custom self-hosted router, injected test transport, or deliberately split trust domains. An
explicit field wins over the corresponding derived transport. Normal local and Rindle Cloud apps
should use `rindle: {}` or `rindle: { url, token }` and should not mint a second public URL or token.

> When `database` is present (configured explicitly or derived from `rindle`), the API server
> constructs, owns, and uses its SQL client as the default
> mutation backend; call `api.close()` during shutdown. An explicit `backend` still wins. Omitting
> both `rindle` and
> `database` preserves the legacy `daemonBackend(daemon)` behavior. The advanced `sql` option accepts
> an already-created, caller-owned `SqlSession`, but normal applications do not need to import a second
> package. Ordinary SQL calls never
> synthesize a mutation id: only the explicit mutation facade receives the envelope's
> `{ clientId, mid }`, and `lmid` remains server-owned output.

> The `daemon` option accepts any `RindleDaemonClient`. Keep a write-master leg when using room or
> lifecycle features that mutate control-plane state; a follower-only client is sufficient for an
> app that uses only named-query reads/materializations alongside the SQL mutation path.

> The `schema` option is what lets the server render a `yield tx.insert(...)`
> into dialect-correct SQL. `postgresBackend(...)` remains a low-level library adapter,
> but Postgres relay/source deployment is not part of the current supported topology;
> synced deployments send authoritative mutations to the HCTree write-master.

`authorizeQuery` / `authorizeMutation` run before a query or mutation resolves; return
`false` to forbid (a `403`). They receive the full request, not just the user —
`{ user, name, args, context }` for a query and `{ user, envelope, context }` for a
mutation — so you can gate on the query name, its args, or the mutation envelope, then
destructure what you need. The server exposes `api.routes` (`{ query, read, mutate }`,
defaulting to `/api/rindle/query`, `/api/rindle/read`, `/api/rindle/mutate` — the routes
the client posts to) and a JSON handler for each.

## Bring your own HTTP

`@rindle/api-server` is **transport-agnostic**. It gives you `handleQueryJson(body,
ctx)` and `handleMutateJson(body, ctx)`; you own the HTTP. That is what makes it
serverless-shaped — wire it into `node:http`, a Cloudflare Worker, a Lambda,
anything:

```ts
import { createServer } from "node:http";

createServer((req, res) => {
  void (async () => {
    const body = JSON.parse(await readBody(req));
    const ctx = { user: req.headers["x-user"], request: req }; // verify a JWT here in prod
    try {
      const out =
        req.url === api.routes.query  ? await api.handleQueryJson(body, ctx)  :
        req.url === api.routes.read   ? await api.handleReadJson(body, ctx)   : // optional one-shot read
        req.url === api.routes.mutate ? await api.handleMutateJson(body, ctx) :
        notFound();
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify(out));
    } catch (err) {
      res.writeHead(statusOf(err), { "content-type": "application/json" });
      res.end(JSON.stringify({ error: String(err) }));
    }
  })();
}).listen(7700);

// The three helpers are yours (they're plumbing, not Rindle API):
function readBody(req: import("node:http").IncomingMessage): Promise<string> {
  return new Promise((resolve, reject) => {
    let body = "";
    req.on("data", (chunk) => (body += chunk));
    req.on("end", () => resolve(body));
    req.on("error", reject);
  });
}
function notFound(): never { throw Object.assign(new Error("not found"), { status: 404 }); }
function statusOf(err: unknown): number {
  return typeof (err as { status?: number })?.status === "number" ? (err as { status: number }).status : 500;
}
```

`handleQueryJson` returns a lease (`{ leaseToken, materializationId, … }`) the client
uses to open its ws subscription; `handleReadJson` resolves the same named query **once**
and returns the rows — no subscription, for [SSR](https://rindle.sh/docs/ssr) or a preload;
`handleMutateJson` accepts one envelope or an in-order batch (`{ envelopes: [...] }`)
and returns the per-mutation outcome. Batched envelopes run strictly sequentially; a
**policy rejection still advances the write-master's mutation id**, so later envelopes stay
contiguous and keep applying. A mutation transport *error* fails the whole batch: the client
retries it and the write-master's per-mutation dedup absorbs the already-applied prefix.

On a Web-standard runtime (a Cloudflare Worker, Deno, TanStack Start, Hono, a
Lambda behind a `Request` adapter) the same three handlers wire to
`Request`/`Response` just as directly:

```ts
export async function handleRindle(kind: "query" | "read" | "mutate", request: Request): Promise<Response> {
  try {
    const body = await request.json().catch(() => ({}));
    const ctx = { user: await verifyUser(request), request };
    const out =
      kind === "query"  ? await api.handleQueryJson(body, ctx)  :
      kind === "read"   ? await api.handleReadJson(body, ctx)   :
                          await api.handleMutateJson(body, ctx);
    return Response.json(out);
  } catch (err) {
    const status = (err as { status?: number })?.status ?? 500;
    return Response.json({ error: String(err instanceof Error ? err.message : err) }, { status });
  }
}
```

## Pinned queries & the one-shot read

A materialized view **is a cache** — one the engine keeps exact on every write.
This is the server-side cache pattern: pin the hot queries so they stay warm with
zero subscribers, then serve them with a one-shot read instead of a subscription.
It's how one daemon holds a public page — a forum's topic list, a storefront, a
leaderboard — with no TTLs and no invalidation code.

### Pinning

A normal materialization lives while someone holds a lease. `pinnedQueries` names
the queries to keep regardless: `assertPins()` materializes each with a **pinned
policy** that survives zero subscribers, so the first request of the day reads a
result that is already maintained — as warm as the thousandth:

```ts
const api = createRindleApiServer<User>({
  rindle: {}, // RINDLE_URL + RINDLE_DATABASE_TOKEN
  queries: apiQueries,
  pinnedQueries: [{ name: "topicsPage", args: { limit: 50 } }],
});

await api.assertPins(); // materialize once — warm before the first request
```

Three properties to lean on:

- **Idempotent.** The follower dedupes by canonical query, so a re-assert reuses the
  existing materialization — call it at startup and after a follower boot-id change.
- **Viewer-independent.** Pins are shared by every reader, so they resolve under
  the `pinUser` option (default `undefined`), never a per-viewer identity — don't
  pin a context-scoped query like `myIssues`.
- **Cheap to hold.** A pin costs O(change) maintenance per write, once, shared by
  every viewer — versus O(table) × readers for re-running the query per request.
  "Stale" isn't a reachable state: the contract is *view-after-write == fresh-query*.

### The one-shot read

`handleReadJson` parses `{ name, args }` and runs `readQuery`: the **same authority
path as a lease** — named-query resolution, `authorizeQuery`, context injection —
but the daemon serializes the current view **once** and returns assembled rows.
No subscriber is registered, so a dropped page render leaks nothing; an unpinned
query's pipeline self-reclaims after the idle TTL (`readIdleTtlMs`) unless the
browser's follow-up subscribe lands first. Against a **pinned** query it is a pure
read of the warm materialization — the endpoint a public page or edge function
calls (the `api.routes.read` branch in the HTTP example above).

The response is `{ rows, cvMin, queryKey }` — assembled, nested-by-name rows plus
the watermark [SSR](https://rindle.sh/docs/ssr) uses to hand a preloaded page off to a live client.
Follower placement stays in the affinity ticket; the response never swaps the
client to a per-follower WebSocket endpoint.

### Across a fleet

A single follower materializes each pin once. In a fleet, provide the operator-side
`pinFanout` hook and `assertPins()` pushes every pin to **all live followers** — a
per-viewer affinity ticket still routes each lease to exactly one. See
[Deploying & scaling](https://rindle.sh/docs/deploy). Managed Rindle Cloud does not expose a
multi-follower plan yet.

## Talking to Rindle

Normal applications configure `rindle` and let the API server own both clients.
`@rindle/daemon-client` is the lower-level typed control client;
`HttpRindleDaemonClient` and `SplitDaemonClient` remain useful for custom self-hosted
routing, boot-id hooks, control-plane features, and compatibility.

For ordinary **bulk, out-of-band writes** like seeding, use the SQL client against the
same unified ingress:

```ts
import { createSqlClient } from "@rindle/sql-client";

const sql = createSqlClient({
  url: process.env.RINDLE_URL!,
  authToken: process.env.RINDLE_DATABASE_TOKEN!,
});
await sql.batch([
  { sql: "INSERT INTO issue (...) VALUES (?, ?, ...)", args: [/* … */] },
]);
```

(Batch multi-row `INSERT`s at ≤100 rows per statement — SQLite caps a statement
at 999 bound parameters.)

`HttpRindleDaemonClient` also surfaces the **boot id**. An advanced explicit client can
pass `onBootId` and call `api.assertPins()` whenever its follower restarts; see the
[daemon's recovery hook](https://rindle.sh/docs/daemon#restart-recovery-the-boot-id).

## Scope

The API tier is **your** code — these packages give you the request handlers and the
typed daemon client, not a framework. Auth, rate limiting, and multi-tenancy live
here, in front of the Rindle ingress the browser can never reach directly.

## Next steps

- [Run the daemon](https://rindle.sh/docs/daemon) — the control plane this tier calls and the ws
  plane the client subscribes to.
- [Isomorphic mutators](https://rindle.sh/docs/mutators) — the shared bodies this tier drives: the
  op vocabulary, reads, and the determinism rules.
- [The browser client](https://rindle.sh/docs/client) — the tier that predicts these same mutator
  bodies optimistically.
- [Server rendering](https://rindle.sh/docs/ssr) — one-shot reads through this same authority for
  first paint.
- [Streaming LLM responses](https://rindle.sh/docs/llm-streams) — the `streams:` plane on this
  server: live SSE tokens checkpointed into your own tables.
- [Deploying & scaling](https://rindle.sh/docs/deploy) — follower affinity and pin fan-out across a
  self-hosted read fleet.
- [Synced-app quickstart](https://rindle.sh/docs/synced-app-quickstart) — the API tier wired end to end
  with the browser client and data tier.
- [Supported queries](https://rindle.sh/docs/supported-queries-ts) — the query shapes your resolvers can
  return.
- [Connect your app to Rindle Cloud](https://rindle.sh/docs/cloud-connect) — the same `rindle` wiring
  with managed `RINDLE_URL` and `RINDLE_DATABASE_TOKEN` values.

# Run the daemon (rindled)

> rindled — the always-on server you run like Postgres: the config, the two network planes, boot-id restart recovery, and the multi-threaded Cluster engine underneath.

**`rindled`** is the read-serving tier of the [Rindle data tier](https://rindle.sh/docs/deploy) — the
always-up server you run like Postgres. It is a **read-follower**: it holds a SQLite
replica of the data and the live IVM pipelines, derives the incremental delta after
every write it receives from the [`rindle-replicator`](https://rindle.sh/docs/deploy) write-master, and
streams normalized, cv-stamped updates to every subscriber. Writes never enter a
follower — they go to the write-master; a follower has **no write ingress at all**. The
browser subscribes to a follower's public WebSocket; your
[API server](https://rindle.sh/docs/api-server) reads and materializes over its private HTTP control
plane (and sends writes to the write-master).

See [the architecture](https://rindle.sh/docs/architecture) for how the three tiers fit and
[deploying & scaling](https://rindle.sh/docs/deploy) for the write-master that feeds it; this page is
about running the follower daemon itself.

## The binary

`rindled` lives in the `rindle-server` crate and ships as a prebuilt, per-platform
binary with [`@rindle/cli`](https://rindle.sh/docs/rindle-cli). For local dev you rarely invoke it
directly — `rindle dev` renders your [`rindle.ncl`](https://rindle.sh/docs/rindle-cli), supervises
the master, follower, and `rindle-dev-edge`, and runs your application with unified
bindings. Use `rindle up` when you deliberately want only that data fleet.
It's the first thing the [synced-app quickstart](https://rindle.sh/docs/synced-app-quickstart) does.
This page is the follower daemon in full — the config, the two planes, restart
recovery, and the engine underneath — for when you run it yourself.

To run a follower directly (in production, or under your own supervisor), point a
release, container, or otherwise supervised `rindled` binary at a JSON config:

```bash
rindled --config follower.json
# …or, with a Rindle source license (rindle-server is a commercial crate):
cargo build -p rindle-server --bin rindled --release
./target/release/rindled --config follower.json
```

The config declares the follower's replica file, the two ports, an optional auth
token, the worker count, and the **change source** it tails — the write-master's
fan-out stream:

```json
{
  "db": "follower.db",
  "httpPort": 7600,
  "wsPort": 7601,
  "authToken": "dev-daemon-token",
  "nWorkers": 4,
  "sources": [
    { "kind": "replicator", "name": "rindle-master", "url": "ws://127.0.0.1:7610/subscribe" }
  ]
}
```

- **`db`** — the follower's file-backed wal2 SQLite **replica** (defaults to
  `rindle.db`); the write-master owns the authoritative copy.
- **`httpPort` / `wsPort`** — the control and subscription ports. `0` binds an
  ephemeral port (handy in tests; the chosen port comes back in the readiness
  signal).
- **`authToken`** — the follower's low-level bearer on both planes. Normal applications
  do not configure a second credential: the fleet edge accepts the one
  `RINDLE_DATABASE_TOKEN` and routes trusted calls internally.
- **`nWorkers`** — IVM worker threads in the underlying `Cluster` (defaults to 2).
- **`defaultLeaseTtlMs`** — how long a materialization lease lives without renewal.
- **`sources`** — exactly one `kind:"replicator"` change source: the write-master's
  fan-out WebSocket (`ws://…:7610/subscribe`). The follower dials out, tails the
  totally-ordered change log, and applies it. The **schema** arrives the same way —
  DDL replicates from the master — so there's no table list here: new tables are
  auto-discovered as the master's [migrations](https://rindle.sh/docs/schema) flow through, and
  `rindle schema gen` regenerates the client schema from the follower's `/schema`.

On a successful start `rindled` prints exactly **one** line of JSON to stdout, so a
supervisor or test runner can wait on it:

```json
{"ready":true,"httpPort":7600,"wsPort":7601}
```

## Two planes

A follower exposes two network surfaces, kept separate so the untrusted browser plane
and the trusted server-to-server plane never share a door:

| Plane | Port | Who connects | Carries |
| --- | --- | --- | --- |
| **Public WebSocket** | `wsPort` | browser clients | the normalized protocol — `init`, `subscribe` / `unsubscribe`, and cv-stamped snapshot + delta frames out |
| **Private HTTP control** | `httpPort` | your API server | **reads only** — `/materialize` (mint a query lease), `/execute-sql-read` (a raw read), `/dematerialize`, `/schema`, `/version` |

The **write** endpoints — `/execute-sql-txn`, `/migrate`, `/mutate-session/*`,
`/reject-mutation`, `/apply-row-change-txn` — live on the
[`rindle-replicator`](https://rindle.sh/docs/deploy) write-master, **not** on a follower. Point writes
at a follower and it refuses them with a fail-closed error that names the master, so a
misrouted write can't silently vanish.

Both planes require the bearer `authToken` when one is set. These ports are the
operator-level topology, not application configuration. The fleet edge exposes one
`RINDLE_URL`; [`createRindleApiServer`](https://rindle.sh/docs/api-server) sends the same server-only
`RINDLE_DATABASE_TOKEN` and the edge routes reads here and writes to the master.
Browsers never speak the control plane directly. The low-level
[`@rindle/daemon-client`](https://rindle.sh/docs/api-server#talking-to-the-daemon) package remains the
typed client for custom self-hosted routing and supervisor integrations.

## Restart recovery: the boot id

`rindled` keeps **no durable materialization state** — on restart it has the data
(it's file-backed) but no live queries or pins. So it stamps every control-plane
response with a **boot id** header that changes when it restarts. The
`HttpRindleDaemonClient` surfaces it via `onBootId`, and your API server re-asserts
its pinned queries when it fires:

```ts
const daemon = new HttpRindleDaemonClient({
  baseUrl: process.env.RINDLE_URL!,
  headers: { authorization: `Bearer ${process.env.RINDLE_DATABASE_TOKEN!}` },
  onBootId: () => api.assertPins().catch(console.error), // re-warm after a restart
});
```

The hook rides responses you already make, so there's no polling — the next
control-plane call after a restart re-establishes the warm set.

## Under the hood: the Cluster

`rindled` runs the multi-threaded **`Cluster`** engine from `rindle-replica`. Where
the single-thread [`Db`](https://rindle.sh/docs/replica-and-views) advances every query on one
thread, `Cluster` shards queries across a pool of IVM worker threads behind a single
writer/coordinator. On a follower the transactions it applies arrive over the
replication stream from the write-master rather than from a client, but the engine
mechanics are identical. The per-transaction handshake keeps the parallelism correct:

1. `write` opens a `BEGIN CONCURRENT` on the writer; the preupdate hook captures the
   row deltas as SQL runs.
2. `commit` fans the captured batch to every worker and waits for each to pin its
   own pre-commit snapshot (the barrier).
3. The writer commits durably while the workers derive their queries' deltas
   **concurrently** under snapshot isolation.
4. Each worker emits its affected queries' deltas, then a progress marker so the
   drain layer knows the transaction is fully delivered.

A query lives on exactly one worker, so **per-query event order is preserved**. If a
worker faults during derivation, that query is torn down (and the pool respawns the
worker) rather than corrupting the stream. The contract is unchanged from the
single-thread path: *view-after-write == fresh-query*. See
[crates](https://rindle.sh/docs/crates#going-multi-threaded-cluster) for the `Cluster` API.

## The query planner

The daemon runs the cost-based join-flip
[planner](https://rindle.sh/docs/how-it-works#query-planner) — it annotates each flippable `EXISTS`
with a `flip` decision before lowering, picking the cheaper drive side from a
real-SQLite cost model. It is **result-preserving** (only the work changes, never
the rows) and is **on by default** (`Cluster::open` enables it); opting out is
in-process only today, via `Cluster::open_with_planning(path, n, false)`, not yet
through the config file.

## Scope

`rindled` is the productionizing read server, but it is young. The replica's schema
constraints apply — plain tables (no triggers / generated columns), numbers within
±(2⁵³−1); see [replica and views](https://rindle.sh/docs/replica-and-views). The database bearer is
server-wide; finer-grained authz lives in your [API tier](https://rindle.sh/docs/api-server).

This page runs **one** follower. To scale reads across **N** affinity-placed followers fed by the
write-master, see
[deploying & scaling](https://rindle.sh/docs/deploy), which lays out the whole deployment menu and what
you can run yourself versus have us run for you on
[Rindle Cloud](https://cloud.rindle.sh).

## Next steps

- [The API server](https://rindle.sh/docs/api-server) — the tier that drives the control plane.
- [The browser client](https://rindle.sh/docs/client) — what subscribes to the ws plane.
- [`@rindle/cli`](https://rindle.sh/docs/rindle-cli) — the local supervisor, migration, and schema-gen
  toolchain that ships the daemon binary for JS/TS projects.
- [Synced-app quickstart](https://rindle.sh/docs/synced-app-quickstart) — `rindled` booted and wired to
  both other tiers.
- [Crates & API map](https://rindle.sh/docs/crates) — `rindle-replica` (`Db` / `Cluster`),
  `rindle-server`, and `rindle-planner`.
- [Deploying & scaling](https://rindle.sh/docs/deploy) — from this one node to a read-scaled fleet,
  and what runs it for you on Rindle Cloud.
- [Connect your app to Rindle Cloud](https://rindle.sh/docs/cloud-connect) — the managed counterpart
  of this page's internal planes: one application URL and token.
