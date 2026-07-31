<!-- GENERATED FILE — do not edit.
     Source: product-page/src/content/docs/schema.md (https://rindle.sh/docs/schema)
     Regenerate: node product-page/scripts/gen-skill.mjs -->

# Schema & migrations

> SQL is the source of truth. Evolve schema with additive or destructive DDL, evolve data with captured DML migrations, and generate the typed query schema with the rindle CLI.

Rindle's data model is **tables with typed columns**, and **the SQL schema is the source
of truth**. The authoritative write master runs HCTree; [`rindled`](https://rindle.sh/docs/daemon)
followers maintain WAL2 SQLite copies for incremental views. You define tables in
ordinary SQL and evolve their schema and data with ordered migrations. Followers
introspect the replicated shape and maintain your queries against it.

The TypeScript schema you may have seen — `table("issue").columns({…}).primaryKey("id")` —
is **not** where your data model lives. It is a **generated, typed facade** for the
[query builder](https://rindle.sh/docs/supported-queries-ts): it gives the builder its column types,
drives the comparator, and parses `json` columns on read. You generate it *from* the SQL
with one command, so the client and the database can never drift. This page is that loop.

> **Writing Rust?** The embedded [`rindle-replica`](https://rindle.sh/docs/quickstart) path is already
> SQL-first — you run your own `CREATE TABLE` and build queries with the `rindle::table`
> AST builder, no generated TypeScript in sight. The migrate + `schema gen` loop below is
> for the daemon and its JS/TS clients.

## Migrations

One toolchain does all of it: the **`rindle` CLI**, shipped beside the daemon. (Rust:
installed with `rindled`. JS/TS: `npm i -D @rindle/cli`, then `npx rindle …`; see
[`@rindle/cli`](https://rindle.sh/docs/rindle-cli) for the toolchain reference.)

```bash
rindle init
rindle dev --migrate --gen src/schema.gen.ts -- vite dev
```

`rindle init` writes a loopback dev topology and a `migrations/` folder. `rindle dev`
renders that topology, supervises the master, follower, and `rindle-dev-edge`, applies
and watches migrations, regenerates the TypeScript schema, and runs your app with
`RINDLE_URL` plus `RINDLE_DATABASE_TOKEN`.

The topology itself remains a small input record:

```text
# rindle.ncl — the one topology (design 214): a write-master + follower(s)
{
  profile = "replicated",
  app = "my-app",
  followers = 1,   # 1 = the colocated pair, both processes on one box
}
```

Use `rindle up` only when you want the fleet without an app process. There's no table
list anywhere — tables come from migrations and are auto-discovered on followers as the
master's DDL replicates.

Every non-empty migration file must be one of two pure kinds:

- **DDL** — schema statements such as `CREATE`, `ALTER`, and `DROP`.
- **DML** — data writes such as `INSERT`, `UPDATE`, and `DELETE`.

DDL and DML cannot appear in the same file. Keep their zero-padded filenames ordered when
a backfill depends on an earlier schema change.

### 1 · Author a schema migration

```bash
rindle migrate create init     # creates migrations/0001_init.sql
```

A schema migration is **ordinary SQL DDL** — one statement per `;`. Every table needs
a single declared **primary key** (the engine indexes on it). Declare a column's *kind*
with its type name — including `BOOLEAN` and `JSON` (more below). Two habits pay off:
use `IF NOT EXISTS` so a re-run is safe, and add an index for each direction your joins
and windowed `orderBy`s traverse. (Column *order* matters too — the engine reads it back
with `PRAGMA table_info`, so append new columns rather than reordering.)

```sql
-- migrations/0001_init.sql
CREATE TABLE IF NOT EXISTS issue (
  id        TEXT PRIMARY KEY,
  title     TEXT    NOT NULL,
  closed    BOOLEAN NOT NULL DEFAULT 0,    -- declared BOOLEAN  → boolean()
  labels    JSON    NOT NULL DEFAULT '[]', -- declared JSON     → json()
  priority  INTEGER NOT NULL DEFAULT 0,
  createdAt REAL    NOT NULL
);
CREATE INDEX IF NOT EXISTS issue_created ON issue (createdAt DESC, id);  -- the paginated window

CREATE TABLE IF NOT EXISTS comment (
  id      TEXT PRIMARY KEY,
  issueId TEXT NOT NULL,
  body    TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS comment_issue ON comment (issueId);           -- the issue → comments join
```

### 2 · Apply it

```bash
rindle migrate apply           # POSTs each *.sql to the daemon, in order, idempotently
```

The CLI classifies and checksums every file before sending the ordered batch. DDL still
rejects `RENAME`, column type changes, and raw `blob`; destructive statements print a
loud notice first — see [evolving your schema](#evolving-your-schema). The write master
commits each schema migration in order and replicates it to every follower. New tables are
**auto-discovered** — you don't list them anywhere — and followers reshape without a
manual restart:

```text
[migrate] applying 1 migration(s) from migrations/ → <write master>
  [applied] 0001_init  schemaVersion=0001_init
[migrate] done — 1 newly applied, 0 already present
[migrate] schema committed on the write-master; followers apply DDL over replication — no manual restart needed.
```

`rindle migrate apply` is safe to re-run. The master binds each id to its kind and
content checksum; an exact match reports `present`, while edited content or reusing a
DDL id for DML fails instead of silently adopting it. `rindle migrate status` verifies
the local kinds and checksums against the applied journals.

If an applied file was changed cosmetically and reverting it is no longer practical, the first
line of the file can explicitly accept the checksum that actually ran:

```sql
-- OVERRIDE_HASH: 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
CREATE TABLE issue (...);
```

Copy the applied hash shown by `rindle migrate status`, then review the change before adding the
directive. The directive is excluded from the SQL and the current checksum. The master accepts it
only when it exactly names that migration id's stored hash; it never re-executes the edited SQL,
changes the stored history, or permits a DDL/DML kind mismatch. A fresh database applies the
current file normally, so use this escape hatch only when the old and current SQL are operationally
equivalent.

### Data migrations

Put seeds and bounded backfills in pure-DML files after any DDL they depend on:

```sql
-- migrations/0002_add_color.sql (DDL)
ALTER TABLE issue ADD COLUMN color TEXT;
```

```sql
-- migrations/0003_backfill_color.sql (DML)
UPDATE issue SET color = 'red' WHERE priority >= 8;
UPDATE issue SET color = 'blue' WHERE color IS NULL;
```

The master evaluates a data migration exactly once in one transaction. It captures the
concrete inserted, updated, deleted, cascaded, and conflict-resolved rows and ships those
deltas to followers; followers never execute the migration SQL. This also makes values
from `random()` or time functions converge exactly. An apply-once marker commits with the
row changes, including when the DML affects zero rows.

Data migrations accept statements classified as writes: `INSERT`, `UPDATE`, `DELETE`,
and write CTEs. Reads, PRAGMAs, and explicit transaction control are rejected. A DML file
does not change `schemaVersion` or reshape followers.

One data migration can capture at most **8,191 user-row changes** (including cascades)
and **64 MiB** of encoded row data. The marker consumes the final change in HCTree's
8,192-change transaction budget. Crossing either limit rolls the file back; split a large
backfill into explicitly key-ranged, separately numbered files.

Mixed DDL+DML table rebuilds are not atomic in v1. Express an additive change and its
backfill as two ordered files. The public `SqlClient.migrate()` surface remains DDL-only;
deploy data migrations with `rindle migrate apply`, locally or with `--cloud`.

### 3 · Generate the typed schema

```bash
rindle schema gen --out src/schema.gen.ts
```

This reads the follower's introspected schema (`GET /schema`) and emits the
`@rindle/client` definition — one `const` per table, sorted by name, plus the
`createSchema` aggregate:

```ts
// Generated by `rindle schema gen` from the daemon's introspected schema (GET /schema).
// Do not edit by hand — re-run the generator after each migration.
import { boolean, createSchema, json, number, string, table } from "@rindle/client";

export const comment = table("comment")
  .columns({
    id: string(),
    issueId: string(),
    body: string(),
  })
  .primaryKey("id");

export const issue = table("issue")
  .columns({
    id: string(),
    title: string(),
    closed: boolean(),   // ← from the declared BOOLEAN
    labels: json(),      // ← from the declared JSON
    priority: number(),
    createdAt: number(),
  })
  .primaryKey("id");

export const schema = createSchema({ tables: [comment, issue] });
```

That's the whole loop: **edit SQL → `migrate apply` → `schema gen`.** Re-run the last two
after every schema change.

### Adding local-only client tables to a generated schema

Do **not** hand-edit the generated file for browser-only tables such as drafts, selections,
or view preferences. Define those tables in a separate module and extend the generated schema:

```ts
// src/schema.local.ts
import { extendSchema, string, table } from "@rindle/client";
import { schema as generatedSchema } from "./schema.gen.ts";

export const selection = table("selection", { local: true })
  .columns({ id: string(), issueId: string() })
  .primaryKey("id");

export const clientSchema = extendSchema(generatedSchema, { tables: [selection] });
```

Use `clientSchema` in the browser. Keep using the generated `schema` for your API server and
any daemon-facing named-query registry. `extendSchema` accepts only `{ local: true }` tables,
which keeps real synced tables SQL-first and generated from daemon introspection.

## Column types: arbitrary type names

SQLite has only five storage classes, but it stores the **full declared type name verbatim**
and never restricts what you write. Rindle reads that declared name back, so you get the
column *kind* you meant — not just a coarse affinity:

| You declare | Generates | Notes |
| --- | --- | --- |
| `TEXT` · `VARCHAR(n)` · `CHAR` · `CLOB` | `string()` | TEXT affinity |
| `INTEGER` · `REAL` · `NUMERIC` · `DECIMAL` · … | `number()` | numbers are `f64` |
| `BOOLEAN` · `BOOL` | `boolean()` | recovered from the declared name |
| `JSON` · `JSONB` | `json()` | recovered from the declared name; stored as TEXT |
| `BLOB` | `string()` | no blob type yet — store bytes as base64 `TEXT` |

So the SQLite "type limitation" is a non-issue: **declare `BOOLEAN` or `JSON` and the
generated schema is `boolean()` / `json()`.** This matches what the engine already does
internally — a `BOOLEAN` column compares as a boolean, a `JSON` column is parsed on read —
so the generated types agree with runtime behavior.

The one thing a declared name *can't* carry is a refinement **within** a kind — the element
type of `json<T>()`, or a string/number literal union. Those you layer on by hand after
generating (the generated file is yours to re-annotate, then re-apply after each regen):

```ts
import { json, type Col } from "@rindle/client";

labels: json<string[]>(),                          // refine the JSON shape
status: string() as Col<"todo" | "doing" | "done">, // refine a string to a literal union
```

A bare `INTEGER` you *intend* as a boolean stays `number()` — the name carried no intent.
Declare it `BOOLEAN` to recover it. Declaring a column exactly `BIGINT` (or `INT8`) opts it
into the **exact int64 plane**: it generates `int64()` (TS `bigint`), the full i64 range
round-trips exactly through SQL reads/writes and replication, and — until the browser
bigint lane ships — live IVM queries that touch the column are refused at registration
(project it away with `select` to query the rest of the table). Every other integer
spelling (`INTEGER`, `UNSIGNED BIGINT`, …) stays the safe-range `number()` plane. (Raw
`blob` is still refused at apply time — there is no blob column type yet.)

## What the generated schema is for

The schema is **purely for the typed query builder** — it is never consulted for
correctness. Concretely it gives you:

- **Typed queries and rows.** `schema` types `store.query.<table>` and the rows you read
  back, so `where`/`orderBy`/`select` are checked against real columns and a result is
  `{ id: string; closed: boolean; labels: string[] }`, not `any`.
- **The comparator.** Each column's kind drives ordering (strings bytewise, numbers by
  total order, booleans as 0/1) so a client sorts a view exactly as the engine does.
- **`json` parsing.** `json` columns arrive as text on the wire and are parsed to objects
  once, on read.

What it is **not**: it carries **no relationships**. Query correlations
(`issue.id → comment.issueId`) live in your [named queries and fragments](https://rindle.sh/docs/fragments),
not in the schema — which is why plain SQL introspection (columns + PK) is enough to
generate it. And because the daemon validates a client's schema fingerprint on subscribe,
a stale generated schema is **rejected and re-fetched**, never silently wrong: regenerating
after a migration is a convenience, not a correctness burden.

Import it wherever you build queries — the [synced client](https://rindle.sh/docs/client) and
[API server](https://rindle.sh/docs/api-server) share the one value:

```ts
import { createRindleClient } from "@rindle/optimistic";
import { schema } from "./schema.gen.ts";          // generated
import { mutators } from "./mutators.ts";          // hand-written (your app logic)

export const app = await createRindleClient({ schema, mutators, /* … */ });
```

## Evolving your schema

Migrations cover both directions of schema change:

- **Additive** — `CREATE TABLE`, `ADD COLUMN`, `CREATE INDEX`.
- **Destructive** — `DROP TABLE`, `ALTER TABLE … DROP COLUMN`, `DROP INDEX`. A drop deletes
  the schema (and its data) on the write-master and **every follower**; `rindle migrate
  apply` prints a `[destructive]` notice per statement before sending anything. There is no
  flag to set — the reviewed migration file is the consent — and point-in-time restore
  (the backup plane) is the undo.

Still rejected: `RENAME` (expand instead: add the new column/table, move writes in your
app, then drop the old one), column type changes, and raw `blob` columns.

Each applied **DDL** file advances the write-master's `schemaVersion`, which namespaces
live-query results: an old-schema client can't attach to a new-shape view, so on the
follower's reshape it simply re-leases against the new version. A DML file advances the
ordered write cursor but leaves `schemaVersion` unchanged. After any schema change,
**re-run `rindle schema gen`** and ship the regenerated schema with your client.

Migrations are the one way to shape the schema: `rindle migrate apply` sends your DDL to the
write-master, which replicates it to every follower. There's no inline table list to
maintain.

### Dropping safely: contract like you expand

Order a removal the same way you order an addition, just reversed:

1. **Ship the app without the doomed table/column first** — remove it from named queries,
   fragments, mutators, and room declarations. A query that still names it after the drop
   fails cleanly (that one query errors; nothing else is affected) — visible, not corrupt —
   but there's no reason to ship that.
2. **Apply the drop migration.** Followers reshape and clients re-lease + re-hydrate
   automatically.
3. **Regenerate** (`rindle schema gen`) so the typed schema no longer mentions it.

Two SQLite rules worth knowing: you can't drop a **primary-key** column, and you can't drop
an **indexed** column directly — drop the index first, in the same migration:

```sql
-- migrations/0007_remove_priority.sql
DROP INDEX IF EXISTS issue_priority;
ALTER TABLE issue DROP COLUMN priority;
```

If you declared **foreign keys**, drop in dependency order. The write-master enforces
`foreign_keys = ON`, and dropping a table that other tables still reference is refused —
whether or not either table holds rows — with an error naming the cause and the fix. Drop the
referencing tables first — the order composes in one migration:

```sql
-- migrations/0008_remove_comments.sql
DROP TABLE comment;   -- references issue(id)
DROP TABLE issue;
```

Dropping a referenced table while **keeping** a table that points at it is not supported:
SQLite cannot drop a foreign-key constraint in place, and the dangling reference would break
the surviving table's writes. This holds even if you recreate the referenced table under the
same name with a different key — `DROP TABLE parent; CREATE TABLE parent (…)` that no longer
carries the referenced column is refused too (a same-shape rebuild is fine). To keep that data,
expand-contract it — create a replacement table without the foreign key in a DDL migration, move
the rows with a bounded DML migration and move application writes, then drop both old tables.

One constraint for embedded/Rust deployments that pass a declared table list to the
write-master (`TableSpec`): declared tables are **pinned** — the declaration re-creates
them at every boot, so a migration that drops one is refused. Remove the table from the
declaration (redeploy), then apply the drop. Apps built on the migration-first flow above
declare nothing and never see this.

## The one hand-authored case: standalone wasm

[`@rindle/wasm`](https://rindle.sh/docs/wasm-client) run **with no server** has no SQLite underneath — it
maintains queries over in-memory rows you push with `tx.add(…)`. There is no database to
introspect, so for that standalone playground you write the same `table(…).columns({…})`
schema **by hand**. The moment a daemon backs it (a [synced app](https://rindle.sh/docs/architecture)), the
schema becomes a generated artifact again — the source of truth moves back to SQL.

## Next steps

- [Run the daemon](https://rindle.sh/docs/daemon) — the read-follower that serves `/schema` (the
  write-master serves `/migrate`).
- [The browser client](https://rindle.sh/docs/client) — imports the generated `schema` to run live, optimistic
  queries.
- [Supported query shapes](https://rindle.sh/docs/supported-queries-ts) — what the typed builder can lower.
- [Reactive queries in the browser](https://rindle.sh/docs/wasm-client) — the standalone engine, where you
  author the schema by hand.
