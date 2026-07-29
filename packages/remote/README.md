# @rindle/remote — network backend for @rindle/client

A `Backend` that drives the **same** `Store` / `ArrayView` as the local backends
([`@rindle/wasm`](../wasm), [`@rindle/replica`](../replica)) — but over a network transport to a
server (e.g. [`@rindle/server`](../reference/server)). The core never sees the wire protocol: the
`RemoteBackend` owns the epoch/seq/gap handshake and hands only clean `hello`/`snapshot`/
`batch` changes up to the `ArrayView` (WASM-CLIENT-DESIGN.md §2.2). Re-exports `@rindle/client`.

## Quick start

```ts
import {
  createRemoteStore,
  createSchema,
  defineQuery,
  newQueryBuilder,
  number,
  string,
  table,
} from "@rindle/remote";

const issue = table("issue").columns({ id: number(), title: string() }).primaryKey("id");
const schema = createSchema({ tables: [issue] });
const q = newQueryBuilder(schema);
const issueQueries = {
  issueByID: defineQuery("issueByID", (args: { id: number }) => q.issue.where.id(args.id)),
};

const store = createRemoteStore(schema, "ws://localhost:3000");
const view = store.materialize(issueQueries.issueByID({ id: 1 }));
view.subscribe(render);             // fires `[]` immediately, then the server's snapshot
await store.write((tx) => tx.add("issue", { id: 1, title: "x" })); // sent up; batches stream back
```

Reads are **eventually consistent**: `materialize()` returns immediately with a pending view
(`[]` / `null`) and fills in when the server's `hello` + snapshot arrive; writes are sent and
the resulting changes stream back (the §2.3 write asymmetry — same API, async timing). The
default transport uses the global `WebSocket` (Node 22+ and browsers — zero dependency); pass a
custom `Transport` to `createRemoteStore` for ws/sse/http of your choosing.

Remote subscriptions are named by default: the client sends only `{ name, args }`. The local query
function builds the client view AST; the embedded server resolves the same name in its own query
registry.

For daemon/serverless deployments, pass `resolveSubscribe`. It can call your API server to
authenticate the user, validate args, materialize the query on `rindle-server`, and return the
opaque lease the browser should present to the daemon:

```ts
const store = createRemoteStore(schema, daemonTransport, {
  resolveSubscribe: async ({ remote }) => {
    const res = await fetch("/api/rindle/query-lease", {
      method: "POST",
      body: JSON.stringify(remote), // { name, args }
    });
    return { leaseToken: (await res.json()).leaseToken };
  },
  sendMutation: async (mutations) => {
    await fetch("/api/rindle/write", { method: "POST", body: JSON.stringify({ mutations }) });
  },
});
```

## The protocol (for servers)

`@rindle/remote` also exports the wire contract a server frames with — a port of
`src/flat_protocol.rs` + `src/wire_schema.rs`:

- **`schemaFp(wireSchema)`** — the FNV-1a 64 content fingerprint (16-char hex), byte-for-byte
  the engine's `schema_fp` (checked against Rust-dumped vectors in `test/`).
- **`COMPARATOR_VERSION`** — the comparator algorithm-contract version.
- **`Publisher`** (server) — stamps batches with `epoch`/`seq`/`schemaFp`; snapshot is seq 0,
  increments seq 1, 2, …; an empty transaction emits no batch (so a gap = a lost batch).
- **`Subscriber`** (client) — validates a frame stream (comparator at `hello`; per batch: epoch,
  fingerprint, strict in-order seq) and emits clean `ChangeEvent`s. Does **not** fold — the
  core `ArrayView` does (the §2.2 split that lets one view serve both local and remote).

> Site docs — the backend seam and the change vocabulary this package speaks:
> [rindle.sh/docs/backends](https://rindle.sh/docs/backends) ·
> [rindle.sh/docs/change-model](https://rindle.sh/docs/change-model) · for agents:
> [llms.txt](https://rindle.sh/llms.txt)

On a gap / drift the `RemoteBackend` re-hydrates internally: it re-subscribes, the server
re-registers under a **new epoch** and replies with a fresh snapshot, and the `ArrayView` resets
in place — the materialized view reference the caller holds survives.

Chunked snapshots (for very large hydrates) are a deferred follow-up; today the snapshot is one
seq-0 batch.

## The optimistic source

For the optimistic-writes path (OPTIMISTIC-WRITES-DESIGN.md §8), `RemoteOptimisticSource`
implements `@rindle/client`'s `OptimisticSource` over the same transport: the normalized
subscription stream with **`cv`-stamped** frames, plus `init` (the connection's stable
clientID), `pushMutation` (named-mutator envelopes up), and connection-level **progress
frames** `{cvMin, lmid, rejected}` down. Plug it into `@rindle/optimistic`'s
`createOptimisticStore` against a [`@rindle/server`](../reference/server) `createOptimisticServer`:

```ts
import { createRemoteOptimisticSource } from "@rindle/remote";
import { createOptimisticStore } from "@rindle/optimistic";

const source = createRemoteOptimisticSource("ws://localhost:3000", clientID);
const { store, backend, mutate } = createOptimisticStore(schema, source, clientRegistry, { clientID });
```

In daemon/serverless mode, use the same `resolveSubscribe` hook for query leases and override
`pushMutation` so custom mutators go to your API server. The API server remains the authority and
forwards an approved transaction to `rindle-server`; progress frames still arrive on the daemon
stream:

```ts
const source = createRemoteOptimisticSource(daemonTransport, clientID, {
  resolveSubscribe: async ({ remote }) => {
    const lease = await api.createQueryLease(remote);
    return { leaseToken: lease.token };
  },
  pushMutation: (envelope) => api.runMutator(envelope),
});
```

Validation is the ordinary normalized subscriber; on a gap it re-subscribes and the server's
fresh `cv`-stamped snapshot (released by its progress frame) re-hydrates — still-pending
mutations survive, re-invoked against the recovered base.
