# @rindle/server — WebSocket server over the replica engine

A Node WebSocket server that serves the [`@rindle/remote`](../../remote) protocol from the
SQLite-backed [`@rindle/replica`](../../replica) engine. It composes existing pieces
(WASM-CLIENT-DESIGN.md §2.5) — a `ReplicaBackend` (one replica db + its clean change stream), a
`Publisher` per subscription (epoch/seq/`schemaFp` framing), and `ws` routing — so a
[`@rindle/remote`](../../remote) client drives the same `Store` / `ArrayView` it would over a local
backend.

## Quick start

```ts
import { createSchema, newQueryBuilder, number, string, table } from "@rindle/client";
import { createReplicaServer, defineServerQueries } from "@rindle/server";

const issue = table("issue").columns({ id: number(), title: string() }).primaryKey("id");
const schema = createSchema({ tables: [issue] });
const q = newQueryBuilder(schema);
const queries = defineServerQueries({
  issueByID: (_ctx, args: { id: number }) => q.issue.where.id(args.id),
});

const server = await createReplicaServer({ schema, queries, port: 3000 });
// … clients connect via @rindle/remote at ws://localhost:3000 …
await server.close();
```

## How it works

One replica (one SQLite db) is shared by all connections, so a mutation from any client updates
every subscribed query; each derived change is framed by that subscription's `Publisher` and
routed to its socket. Remote subscriptions carry only `{ name, args }`; the server resolves
`name` in its query registry and builds the authoritative AST locally. Queries are multiplexed
per connection, tagged by the client's `queryId`. A re-subscribe (the client's gap recovery)
tears the old query down and re-registers it under a **new epoch**, so the client rejects any
in-flight stale-epoch batch.

`createReplicaServer` resolves once listening; with `port: 0` (or omitted) it binds an ephemeral
port, available as `server.port`.

## The normalized route

`createNormalizedServer({ schema, queries })` is the local-first sibling of `createReplicaServer`
(NORMALIZED-CHANGES-DESIGN.md §6). Each subscription gets the native engine's per-query
**normalized** footprint stream (`NormalizeFold` + `NormalizedPublisher`, already epoch/seq/fp-
stamped), relayed verbatim over ws as `nhello`/`nbatch`. Like the flat server it shares one
replica across connections and re-registers a query under a new epoch on gap recovery. A single
server is one mode — flat or normalized, not both.

```ts
import { createNormalizedServer } from "@rindle/server";

const server = await createNormalizedServer({ schema, queries, port: 3000 });
```

## The optimistic route

`createOptimisticServer({ schema, registry, queries })` serves the optimistic-writes protocol
(OPTIMISTIC-WRITES-DESIGN.md §8) from the cluster-backed `@rindle/replica` engine (`ClusterDb`),
deriving queries in parallel across IVM worker threads: clients subscribe in normalized mode and
get **`cv`-stamped** frames; named-mutator envelopes (`pushMutation`) run the **server registry**
mutator in one transaction with the co-transactional `lmid` upsert — dup mids skip idempotently,
a throw rolls effects back and still commits lmid-only (the durable record that the mid was
processed). No-reject semantics: a failed mutator is logged server-side only and carries no
protocol rejection signal — the client's own lmid release snaps the optimistic prediction back.
Delivery is eager (the client buffers by `cv`); after each commit, exactly the connections whose
data changed or whose own lmid advanced get one progress frame `{cvMin, lmid}` (the §8.4 poke
rule — foreign writes cost idle connections nothing). `cvMin` is computed per-worker from the
slowest worker's position, so a slow worker holds back only clients with a query on it and data
frames always precede the releasing progress frame.

```ts
import { createOptimisticServer } from "@rindle/server";

const server = await createOptimisticServer({
  schema,
  registry: {
    createIssue: (txn, args) =>
      txn.exec("INSERT INTO issue (id, title, score) VALUES (?, ?, ?)", [args.id, args.title, args.score]),
  },
  queries,
});
```

## Scope

Single shared replica, in-memory subscription routing — a reference server, not a clustered
deployment. The replica's schema constraints apply (plain tables; numbers within ±(2^53−1) — see
the `rindle-replica` crate docs). Auth, backpressure, and horizontal scale are out of scope here.
