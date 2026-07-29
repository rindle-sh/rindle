# @rindle/sql-client

The fetch-native client for **Rindle SQL** — the plain-SQL face of a Rindle deployment. Send SQL
over HTTP and get rows back, with typed interactive transactions, session read-your-writes, and a
Drizzle adapter. It runs anywhere with WinterCG `fetch` and has no runtime dependencies.

Full docs — consistency modes, transaction/retry semantics, the Drizzle seam and the declared v1
value bounds: **[rindle.sh/docs/sql-client](https://rindle.sh/docs/sql-client)** ·
the schema subset: [rindle.sh/docs/schema](https://rindle.sh/docs/schema) · for agents:
[llms.txt](https://rindle.sh/llms.txt)

```ts
import { createSqlClient } from "@rindle/sql-client";

const sql = createSqlClient({
  url: process.env.RINDLE_REPLICATOR_URL ?? "http://127.0.0.1:7611",
  authToken: process.env.RINDLE_DATABASE_TOKEN!,
});

const inserted = await sql.execute({
  sql: "insert into user(name) values (?) returning id",
  args: ["Ada"],
});

await sql.withTransaction(async (tx) => {
  await tx.execute({ sql: "update account set balance = balance - ? where id = ?", args: [10, 1] });
  await tx.execute({ sql: "update account set balance = balance + ? where id = ?", args: [10, 2] });
});
```

`authToken` is a trusted, database-wide server credential — do not expose it to browsers or
unrelated tenants.

## Optimistic mutation transactions

`@rindle/api-server` uses this transport internally for authoritative synced-app mutations. In the
normal full-featured setup, give the API server database configuration rather than constructing a
second client yourself. It chooses the one-request mutation path for pure writes, lazily opens an
interactive transaction at the first mutator read, and processes business rejections as lmid-only
commits:

```ts
const api = createRindleApiServer({
  daemon, // named queries, materializations, and rooms
  database: { url: process.env.RINDLE_REPLICATOR_URL ?? "http://127.0.0.1:7611", authToken: process.env.RINDLE_DATABASE_TOKEN! },
  schema,
  queries,
  mutators,
});
```

Server-only mutator code reaches the bound client through `tx.sql` (inside the mutation transaction)
or `scope.sql` (explicitly outside it). Use `createSqlClient` directly when ordinary SQL is the main
operation — a script, migration, ORM, admin task, or service code unrelated to the mutator path. An
already-created `SqlSession` can still be injected into the API server's advanced `sql` option for
testing or custom session ownership.

The low-level methods are `executeMutation`, `beginMutation`, and `rejectMutation`. They accept the
browser envelope's `{ clientId, mid }`; **`lmid` is never an input**. It is server-owned state
returned in the `MutationReceipt` after the effects and watermark commit atomically. Keep this
surface in a trusted authority tier—the browser still sends only a mutator name and args to your API.
