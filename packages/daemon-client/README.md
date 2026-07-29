# @rindle/daemon-client

Low-level private client for a single Rindle node. It speaks the control contract of the
file-backed Rust nodes — a `rindle-server` **follower** (reads/subscriptions) or a
`rindle-replicator` **write-master** (writes). One client points at one node; in a synced
app `@rindle/api-server`'s `SplitDaemonClient` wraps two of these to route reads and writes
to the right node. (The former in-process JS daemon, `createRindleDaemon`, has been retired;
the contract now lives in `packages/reference/server/test/daemon-conformance.mjs`.)

It is intentionally small: it knows the control/write request shapes and can POST JSON to a
node, but it does not resolve named queries, authenticate users, or run application mutator
policy. Use `@rindle/api-server` for that higher-level app boundary.

Full docs — the control plane, seeding, and the boot-id re-warm hook:
**[rindle.sh/docs/api-server](https://rindle.sh/docs/api-server#talking-to-the-daemon)** ·
the daemon itself: [rindle.sh/docs/daemon](https://rindle.sh/docs/daemon) · for agents:
[llms.txt](https://rindle.sh/llms.txt)

```ts
import { HttpRindleDaemonClient } from "@rindle/daemon-client";

const authz = { authorization: `Bearer ${process.env.RINDLE_DAEMON_TOKEN}` };

// Reads/subscriptions → a follower.
const follower = new HttpRindleDaemonClient({ baseUrl: "https://follower.internal", headers: authz });
const lease = await follower.materialize({
  ast,
  mode: "normalized",
  policy: { kind: "whileSubscribed" },
});

// Writes → the write-master (a follower has no write ingress).
const master = new HttpRindleDaemonClient({ baseUrl: "https://write-master.internal", headers: authz });
await master.executeSqlTxn({
  clientID: "client-1",
  mid: 1,
  statements: [{ sql: "INSERT INTO issue (id, title) VALUES (?, ?)", params: [1, "first"] }],
});
```
