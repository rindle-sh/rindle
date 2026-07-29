# @rindle/replica — native (SQLite-backed) local backend

A native Node backend for [`@rindle/client`](../client): the SQLite-backed
`rindle-replica` live-query engine (real `BEGIN CONCURRENT` CDC), compiled
to a [napi-rs](https://napi.rs) addon and run **in-process**. It speaks the same `Backend`
seam as [`@rindle/wasm`](../wasm) — the **same** `Store` / `ArrayView` / query builder drive it,
only the engine differs (native SQLite vs the wasm memory engine). Re-exports `@rindle/client`.

## Quick start

```ts
import { table, string, number, createSchema, createReplicaStore } from "@rindle/replica";

const issue = table("issue").columns({ id: number(), title: string() }).primaryKey("id");
const schema = createSchema({ tables: [issue] });

const store = createReplicaStore(schema); // synchronous — the native addon loads on import
const view = store.query.issue.where.closed(false).orderBy("id", "asc").materialize();
view.subscribe(render);
await store.write((tx) => tx.add("issue", { id: 1, title: "x" }));
```

No async init (unlike `@rindle/wasm`): the native addon loads synchronously when the module is
imported. Each `createReplicaStore` / `ReplicaBackend` owns its own replica — a temporary
`wal2` SQLite database, removed when the handle is collected.

## How it works

`ReplicaBackend` constructs a native `Db` (`rindle-replica`), runs `CREATE TABLE` DDL from the
typed schema + registers each table, and translates the Store's positional `add`/`remove`/
`edit` mutations into SQL through the controlled writer. Each commit derives every affected
query's incremental change against a pre-commit `BEGIN CONCURRENT` snapshot and emits the
**same bare, JS-shaped flat changes** the wasm `Db` does — so one `ArrayView` folds either
backend's stream identically (verified byte-for-byte in `test/e2e.mjs`).

## Building

Two steps, like `@rindle/wasm`. The native addon (the `.node` + its JS loader):

```sh
pnpm --filter @rindle/replica build:native        # release; build:native:debug for dev
```

Requires the Rust toolchain + `@napi-rs/cli` (a devDependency). Then the TypeScript → `dist/`
compile that produces the published `.js` + `.d.ts`:

```sh
pnpm run build                                  # from the repo root — builds client, wasm, replica
```

The published package ships `dist/` (compiled TS) + the native `index.js` loader + the
platform `.node`. In-repo, tests resolve the TS source via the `@rindle/source` export condition,
so `pnpm test` needs no TS build (but does need the native addon + the wasm pkg oracle built).

## Scope

The replica owns its schema shape: registered tables must be **plain** (no triggers, generated
columns, or FK cascades), and numbers stay within ±(2^53−1) — see the `rindle-replica` crate docs.
